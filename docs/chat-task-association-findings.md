# Findings: associating ClickUp chat messages with tasks

**Date:** 2026-06-13
**Status:** Decided — do **not** build on the native message↔task "relationship"; use task comments.
**Tested against:** live ClickUp sandbox (workspace "Orbsoft", `team_id 90141305256`, token `CLICKUP_API_TOKEN_SANDBOX`).

## Question

Can `clickup-yaml-sync` programmatically (a) associate a channel message with a
task — the "Add relationship" action available on a chat message's `…` menu in
the UI — and (b) write future messages to an associated task?

## TL;DR / decision

The native message↔task link **cannot be read back or reproduced through the
public API**, on any channel type. Therefore it is **unusable for an automated
sync** (you cannot sync or verify a link you cannot read). The only viable
mechanism is **task comments**:

- **New messages** → `POST /api/v2/task/{task_id}/comment` (the per-task chat thread; fully round-trips: readable + writable).
- **Existing backlog** → copy message content into the task as a comment with attribution + a backlink, tracked by a `message_id → task_id` map in YAML for idempotency.
- The native "Add relationship" stays a **manual UI gesture** only — never scripted.

## What was tested

Two API "shapes" exist and must not be confused:

| Concern | API |
|---|---|
| Channel chats | **Chat API v3** (experimental) — `…/chat/channels`, `…/chat/channels/{id}/messages`, `…/chat/messages/{id}/replies` |
| Per-task chat | **Comments API v2** — `…/task/{id}/comment` (the task "chat" *is* its comment thread; there is no separate object) |

The "Add relationship" UI feature maps to three `triaged_*` fields on the
**create-message** endpoint: `triaged_action` (1 or 2), `triaged_object_id`,
`triaged_object_type`.

## Results (empirical, live sandbox)

| Behavior | Result |
|---|---|
| `POST …/messages` with `triaged_action`(1 or 2) + `triaged_object_id` + `triaged_object_type:"task"` | ✅ **200, echoes the fields back** |
| Does `action=1` (or 2) **create** a task? | ❌ No — no task created; both just (claim to) tag the message |
| **Read the link back** via `GET …/messages` | ❌ **`triaged_*` stripped** — message object is only `content, date, date_assigned, id, links, parent_channel, replies_count, resolved, type, user_id` |
| `GET` a single message | ❌ **405 Method Not Allowed** (no single-message GET endpoint) |
| Does the **task** show the linked message? (`GET /api/v2/task/{id}`) | ❌ No — `linked_tasks: []`, no chat/message/relationship field |
| **PATCH** an existing message to add the link (the screenshot's action) | ❌ **HTTP 500 Internal Server Error** |
| **Does a list-scoped channel change any of the above?** | ❌ **No** — identical behavior on a workspace channel (`parent.type 7`) and a list-scoped/canonical channel (`parent.type 6`, `is_canonical_channel: true`) |
| **Does the triage-write at least show a relationship in the UI?** (verified by screenshot 2026-06-13) | ❌ **No** — a message posted with `triaged_*` renders as **plain text, no relationship chip**, and the target task shows `Relationships: Empty`. The triage-write is a **true no-op/placebo**: accepted and echoed, but produces nothing in API *or* UI. |

### Interpretation

- The triage write is **accepted-but-blind**: you can write it, but no public
  endpoint returns it. A link you cannot read is not syncable.
- **List-scoping is irrelevant** (a hypothesis we explicitly tested and
  disproved — the "Channel" tab being a List's canonical chat view made no
  difference).
- Retroactive linking of an **existing** message (exactly the UI's "Add
  relationship") has **no working public endpoint** — `PATCH` 500s.
- The UI "Add relationship" therefore runs on an **internal endpoint not
  mirrored in the public API**. We chose not to reverse-engineer it: even if
  captured, it is almost certainly UI-session/CSRF-gated, not usable from a
  `pk_` token, so it cannot be the basis of an automated tool.

## The association that DOES work: a task link embedded in the message body

Tested 2026-06-13 against the standing sandbox channel, with UI screenshots:

- **Manual "Add relationship"** (UI `…` → Add relationship on a message) → the
  relationship is created in the UI but is **invisible to the public API** — the
  message `GET` returns no relationship/`triaged_*` field, and the task shows
  `Relationships: Empty`-equivalent in the API. So even human-created links can't
  be read by the tool. **Dead end for sync, both directions.**
- **Embedding/mentioning a task in a message.** In the UI (type and pick a task)
  this renders as a **task chip** with a status badge that opens the task
  *in-app* (side panel). Key limitations found via API + screenshots:
  - **Read:** a human-created embed *is* readable — its message `content` serializes to a markdown link `[https://app.clickup.com/t/<task_id>](…)`, so you can parse `app\.clickup\.com/t/(\w+)` and **detect** task references humans put in messages. ✅
  - **Write:** `POST …/messages` with a task link in `content` persists and round-trips ✅ — **BUT it always renders as a plain hyperlink that opens a NEW window, never a chip.** Tested both short (`/t/<id>`) and full (`/t/<team>/<id>`) URL forms; neither upgrades to a chip.
  - **The native chip is UI-only.** It's an internal entity-reference token the UI task-picker inserts; the chip-ness is lost when the API reads the message (serialized to a plain markdown link), and cannot be produced by writing to the `content` field. The Chat API documents no embed/mention syntax — only `content_format: text/md|text/plain` and an undocumented `post_data` object.

Net: the message↔task association is **functionally** scriptable (parseable task
link, both read and write) but **not cosmetically** (no chip, API-written links
open a new window). It is content-level text, not a first-class ClickUp relation.
For a sync tool that cares about the mapping (not the chip), this is sufficient.

### Revised recommendation

For associating a message with a task, two viable mechanisms (use both):
1. **Task comments** (`POST /api/v2/task/{id}/comment`) — to put the conversation *on the task* (readable in the task's chat/comment thread).
2. **Embedded task link in the message** (`app.clickup.com/t/<id>` in message `content`) — to point *from the channel message to the task*, parseable for the `message_id → task_id` mapping.

Avoid entirely: the native structured "relationship" / `triaged_*` triage path —
it is a no-op via the API (create) and unreadable (manual or otherwise).

## Why a registered ClickUp App does NOT help

An OAuth App authenticates against the **same public API** — it unlocks no extra
endpoints. It would only matter for acting on behalf of other users
(per-user attribution at scale, requiring every user to OAuth-authorize) or
marketplace distribution. Neither applies to this internal tool. Stick with a
personal token (`pk_`).

## Design for clickup-yaml-sync

A new module (not a config flip — the tool currently hardcodes `api/v2` and
touches no chat/comments):

- **v2 comments client** — read/write `…/task/{id}/comment`.
- **(optional) v3 chat-read client** — read channel messages for the backfill sweep.
- **YAML mapping** — `channel_id → task_id`, an imported `message_id` set, and a sweep watermark. Fits the existing YAML-as-source-of-truth model.

Attribution caveat: comments are authored by the **token's user**, not the
original sender — bake `[Author, date]` into the comment text. Rate limit: v2
token ≈ 100 req/min, so the backfill loops with throttling.

## What was confirmed vs not

- Channel **exists and is functional** — ✅ proven by message **write + read-back
  round-trip** (`POST …/messages` → `GET …/messages` returns the message with
  content intact). This is the same read-back that revealed `triaged_*` is
  stripped.
- Channel **creation + list binding** — ✅ proven via API read-back
  (`GET …/chat/channels` shows the channel with `parent.id` = the list id,
  `type 6`, `is_canonical_channel: true`).
- Channel's **UI surfacing** (that it appears as the list's "Channel" tab) —
  ✅ confirmed via logged-in browser screenshots (the API-created list-scoped
  channel shows in the sidebar under "Channels" and as the list's Channel tab,
  with the API-posted message visible).

## Reproduction

All via `curl` with `CLICKUP_API_TOKEN_SANDBOX` (see `~/bin/clickup-sandbox.env`):

1. `POST /api/v2/team/{team}/space` → `POST /api/v2/space/{space}/list` → `POST /api/v2/list/{list}/task`.
2. `POST /api/v3/workspaces/{team}/chat/channels/location` with body `{"location":{"id":"<list_id>","type":"list"}}` → list-scoped channel.
3. `POST /api/v3/workspaces/{team}/chat/channels/{ch}/messages` with `triaged_action`/`triaged_object_id`/`triaged_object_type:"task"`.
4. `GET /api/v3/workspaces/{team}/chat/channels/{ch}/messages` → observe `triaged_*` absent.
5. `GET /api/v2/task/{task}` → observe `linked_tasks: []`.
6. `PATCH /api/v3/workspaces/{team}/chat/messages/{msg}` with `triaged_*` → 500.

## Complete avenue inventory (every path tried)

| Avenue | Tried | Result |
|---|---|---|
| v3 triage on message create (`triaged_*`) | ✅ | accepted, echoed — **no-op** (stripped on read, no UI effect) |
| v3 `PATCH` to link an existing message | ✅ | **HTTP 500** |
| v3 list-scoped channel (the "active channel" hypothesis) | ✅ | identical no-op to a workspace channel |
| v3 task-link embedded in message `content` | ✅ | **parseable link** ✅, but renders plain (new window), not a chip |
| Manual UI "Add relationship" → read via API | ✅ | **invisible** (no field on message GET; task `Relationships: Empty`) |
| Manual UI task **chip** embed → read via API | ✅ | serializes to a plain markdown task link (chip-ness lost) |
| v2 `Add Task Link` | n/a | **task↔task only**, not message↔task |
| Internal `frontdoor` chip endpoint | ✅ (network capture) | session/cookie-auth on `frontdoor-prod-*.clickup.com`, **not `pk_`-callable** |

### Why v3 doesn't change the answer

All chat tests above ran against the **public v3** API
(`api.clickup.com/api/v3/.../chat/...`). The **complete** v3 chat message surface
is: `POST /chat/message`, `PATCH /chat/message/{id}`, `DELETE`,
reactions (`GET/POST/DELETE`), replies (`GET/POST`), `GET /chat/message/{id}/tagged_users`
(@user mentions only), `GET /chat/messages`. **There is no v3 (or v2) endpoint
for triaging a message, linking a message to a task, or a message
"relationship."** The native chip is created/served only via ClickUp's internal
`frontdoor` host under session auth — confirmed by network capture (the web app
talks to `frontdoor-prod-us-east-2-2.clickup.com`, never `api.clickup.com`). It
is unreachable by any public endpoint or a personal token.

### If the internal chip endpoint is ever revisited

It would require replaying a **`frontdoor`** request under a **browser session
token** (not `pk_`) — i.e. not a supported integration path. To capture it, use a
**direct Playwright script** (not the PW MCP): read creds from
`pass clickup/orbsoft-sandbox-login` inside the process (keeps the secret out of
any transcript), persist auth with `storageState`, filter Network to `frontdoor`
Fetch/XHR, and capture the `POST` fired when a task mention is inserted + sent.
The MCP path is unsuitable — every typed value becomes a tool-call argument
(secret leakage) and it cannot reuse a saved session cleanly. **Not recommended**:
a `frontdoor`/session-auth call is not usable from an automated `pk_`-token tool.

## Standing sandbox (left up for reference)

Not torn down — available for further poking. Orbsoft workspace (`team 90141305256`):
- Space `90146054238` "Triage Test (standing)"
- List `901417260037` "Triage List" → list-scoped channel `6-901417260037-8`
- Tasks: `86baecbxr` ("Relationship target task"), `86baecc1k` ("manual task")
- Token: `~/bin/clickup-sandbox.env` → `CLICKUP_API_TOKEN_SANDBOX`
