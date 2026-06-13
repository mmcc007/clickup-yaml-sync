# Spec: status-note comment push (YAML → ClickUp task comments)

**Status:** 🗄️ SHELVED (2026-06-13) — not being built.
**Why shelved:** this is a one-way *publish*, not sync, and there is **no machine
source** generating status notes today. Human-authored notes in YAML have worse
ergonomics than just commenting in ClickUp, so the feature would add idempotency/
attribution machinery for negative value. The design below is sound and ready to
pick up unchanged **if/when a machine source appears** — e.g. CI/build status, a
per-task summary generator, or (highest value) the sync tool posting its own
change-log of what a `push` did. Revisit only then; if built, name it honestly as
`publish`/`--changelog`, not "sync."
**Depends on / context:** `docs/chat-task-association-findings.md` (why this — not
chat-message sync — is the only message-shaped feature worth building).

## Goal

Let the tool post **structured status notes** authored in YAML onto a ClickUp
task as **comments**, one-way, idempotently, as a bot user. This is the
machine-authored "status trail" feature — NOT chat, NOT bidirectional.

## Non-goals (explicit)

- ❌ Not chat-message sync, and not channel messages — task comments only.
- ❌ Not bidirectional — human comments are never read into YAML.
- ❌ Not the native message↔task "relationship"/chip — proven unreachable by the public API (see findings doc).
- ❌ No editing or deleting of comments authored by humans (or, in v1, even our own — append-only).

## YAML schema (additive to a story)

```yaml
stories:
  - name: "Implement RAG retrieval"
    status_notes:                       # optional; append-only log
      - text: "Build started — branch feat/rag"
      - text: "Deployed to staging, smoke tests green"
        notify: false                   # default false
      - id: deploy-prod                 # optional explicit id (else derived)
        text: "Promoted to prod"
```

- `status_notes` is an ordered list. Each entry:
  - `text` (required) — the comment body. **Plain text only via `comment_text`** (see Formatting below — `comment_text` does NOT render markdown; `**bold**` stays literal). For rich notes, use `blocks` instead.
  - `blocks` (optional) — a structured `comment[]` array for rich formatting (bold/lists/@mentions/links). Mutually informative with `text`; if present, posted via the `comment` field instead of `comment_text`.
  - `id` (optional) — stable identity. **Default `id = sha1(text or serialized blocks)[:12]`.**
  - `notify` (optional, default `false`) — maps to ClickUp `notify_all`.
- Add to `schema.yaml` alongside the existing story fields.

## Formatting capabilities (verified empirically + docs, 2026-06-13)

`comment_text` is **not** rich — markdown is stored/shown literally (only bare URLs
auto-link). All real formatting requires the structured `comment[]` block array:

| Capability | How |
|---|---|
| bold / italic / inline code | `{"text":"x","attributes":{"bold":true}}` (`italic`, `code`) |
| code block, bullet/ordered/checklist/toggle list | `attributes."code-block"` / `attributes.list` |
| **user @mention** (renders a user chip + notifies) | `{"type":"tag","user":{"id":<userId>}}` |
| hyperlink (styled label) | `{"text":"label","attributes":{"link":"<url>"}}` |
| emoji | `{"type":"emoticon","emoticon":{"code":"1f60a"}}` |
| **task chip / embed** | ❌ **not supported** — no task-mention block type exists; reference a task only as a plain hyperlink (`attributes.link` to `app.clickup.com/t/<team>/<id>`) |

Implication: status notes can be bold-headed, listed, @owner-pinged, and linked —
but a task reference is a plain link, never a chip. For most status notes,
`comment_text` (plain + a trailing link) is enough; use `blocks` only when bold/
lists/@mentions add real value.

## Comment marker & identity

Every pushed comment ends with a marker line the tool uses to recognize its own
output and to dedupe:

```
<text>

⟦custatus:<id>⟧
```

- `⟦custatus:<id>⟧` — unobtrusive, on its own trailing line, machine-parseable
  with `⟦custatus:([0-9a-z-]+)⟧`.
- `<id>` is the note's `id` (explicit, or `sha1(text)[:12]`).
- The marker in ClickUp is the **authoritative** record of what's posted; a local
  cache (below) is only an optimization.

## Push algorithm (per task with `status_notes`)

1. `GET /api/v2/task/{task_id}/comment` → existing comments.
2. Parse `⟦custatus:<id>⟧` markers → set `posted_ids`.
3. For each YAML note in order, compute its `id`:
   - if `id ∈ posted_ids` → **skip** (already posted).
   - else → `POST /api/v2/task/{task_id}/comment`, `notify_all = note.notify`:
     - **text mode:** `comment_text = text + "\n\n⟦custatus:" + id + "⟧"`.
     - **block mode:** post `comment = [...note.blocks, {"text":"\n⟦custatus:"+id+"⟧"}]` (marker appended as a trailing plain-text block).
   - Marker parsing on read scans both `comment_text` and the flattened `comment[]` block text, so either mode dedupes correctly.
4. Record posted ids in a local cache (see Idempotency) for fast dry-runs.

Add-only: notes removed from YAML are **not** deleted from ClickUp (immutable
history). This intentionally diverges from the tag-reconciliation pattern, which
removes stale managed tags.

## Coexistence with other posters (the load-bearing requirement)

- **Human comments** (no marker) → ignored entirely; never read, edited, or deleted. They interleave chronologically in the same thread.
- **Ordering** → flat thread, interleaved by post time. Status notes are not grouped/pinned (no pin API).
- **Human deletes a bot note** → marker gone → re-posted next run (self-healing; documented surprise). A future `tombstones:` list in YAML could suppress re-post if needed — out of scope for v1.
- **Human strips the marker by editing a bot comment** → would re-post (duplicate). Mitigations: marker on its own trailing line + the local id cache as a second dedupe signal.
- The reconciliation **reads existing comments only to find our markers** — it does not interpret human content.

## Attribution (important — comments post as the token's user)

The ClickUp `POST comment` endpoint has **no author-override** (fields are only
`comment_text`, `assignee`, `group_assignee`, `notify_all`). Authorship = the
`pk_` token's user. With a personal token, every status note shows the **human
owner's name + avatar** in their comment history. The only way to change the
author is a different user's token. Options, in preference order:

1. **Dedicated bot/service member + its own `pk_` token (recommended).** Comments
   read e.g. "Sync Bot", cleanly separated from humans. **Cost: a paid ClickUp
   seat** (billed per member). Guests are cheaper but have limited/uncertain
   comment permissions — verify on the plan before relying on a guest.
2. **Personal token + text prefix (no extra cost).** Keep the human token but
   prefix each note, e.g. `🤖 [auto] <text>`, so it's visibly automated. Author/
   avatar is still the human; the prefix carries the signal.
3. **Accept author = the human** if the human conceptually owns the notes.

Make the attribution strategy a **config choice**: `comment_author_token` (the
token to post with) + `comment_prefix` (e.g. `🤖 [auto] `, default empty). If no
bot token is set, default to applying `comment_prefix` so notes are never
silently indistinguishable from human comments.

## Idempotency & failure handling

- **Source of truth = ClickUp markers** (step 2). A run is safe to repeat: a
  failed/partial POST simply isn't marked, so it retries next run. No duplicates
  as long as the marker survives.
- **Local cache (optional optimization):** persist `task_id → {posted_ids}` in a
  sync-state file so `diff` can preview without a GET per task. The cache is
  advisory; the ClickUp marker always wins on conflict.
- **POST failure** → exponential backoff; on final failure, log and continue to
  next note/task (do not abort the whole push).
- **Rate limit** (v2 ≈ 100 req/min) → throttle: 1 GET + N POSTs per task; sleep
  between batches.

## CLI integration

- **`diff`** → show, per task, `status_notes: N to post` (dry-run preview using the GET or cache).
- **`push`** → after task upsert, run the comment reconciliation for tasks that have `status_notes`. Gate behind a config flag `push_status_notes: true` (default on) so it's opt-out.
- **No new top-level command** — comments are part of a task's push lifecycle, consistent with how tags/assignees already ride `push`.

## Edge cases

- Task missing/archived → skip with warning.
- Duplicate `id` within one task's `status_notes` → validation error before push.
- `text` exceeding ClickUp comment length limit → error with the offending note (do not silent-truncate).
- Empty/whitespace `text` → validation error.
- Comment GET pagination → follow if the task has many comments (ClickUp paginates `start`/`start_id`).
- Marker collision with user-typed `⟦custatus:…⟧` → astronomically unlikely; accept.

## Reuse from existing code

- Auth header + base URL: reuse `clickup.py`'s existing v2 request path (`CLICKUP_BASE`).
- Reconciliation shape: model on the existing **tag managed-set** logic, but **add-only** (no stale-removal).
- Hashing: stdlib `hashlib.sha1`.

## Test plan (target ≥80% on new code)

**Unit**
- `note_id`: explicit id passthrough; derived `sha1` determinism; collision validation.
- marker render/parse round-trip; parse ignores non-marker comments; parse tolerant of extra whitespace/markdown.
- reconciliation: posts only missing ids; skips present; add-only (removed YAML note not deleted); duplicate-id rejected; oversized/empty text rejected.

**Integration (mocked ClickUp HTTP)**
- GET returns mix of human + bot comments → only missing notes posted, humans untouched.
- POST failure mid-batch → later run completes the rest, no dupes.
- pagination over a long comment list.
- `notify` flag maps to `notify_all`.

**End-to-end (sandbox, `CLICKUP_API_TOKEN_SANDBOX`)**
- Push 2 notes to the standing task `86baecbxr`; re-run → no duplicates.
- Manually add a human comment in the UI → re-run → human comment untouched, still no dupes.
- `diff` shows correct pending count before push.
