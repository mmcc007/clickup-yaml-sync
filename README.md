# clickup-yaml-sync

Bidirectional sync between local YAML project files and ClickUp. Manage your project as structured YAML, sync to ClickUp, pull status updates back, and resolve conflicts — including LLM-assisted merging.

## Concept

Epics live **only in YAML** as organizational groupings. Stories become flat top-level tasks in ClickUp, tagged with their epic name. This keeps ClickUp clean while preserving full project structure locally.

```
YAML                          ClickUp
──────────────────────        ────────────────────────
epics:                        Tasks (flat list):
  - name: Core Pipeline   →     "Implement RAG retrieval" [tag: Core Pipeline]
    stories:              →     "Add confidence scoring"  [tag: Core Pipeline]
  - name: API Gateway     →     "POST /api/generate"      [tag: API Gateway]
    stories:              →     "Feedback endpoints"      [tag: API Gateway]
```

## Commands

| Command | Description |
|---------|-------------|
| `push` | Push YAML → ClickUp (create new, update changed) |
| `pull` | Pull ClickUp → YAML (update statuses, detect new tasks) |
| `diff` | Show differences without making any changes |
| `sync` | Full bidirectional sync with conflict resolution |
| `merge` | Like sync but uses GPT-4o-mini to propose merged values |
| `status` | Offline summary table (no API calls) |

### Which command should I use? → `sync` (default)

**`sync` is the safe default and the recommended command for routine work.** It is the only command with **conflict detection**: it reads a base snapshot, auto-resolves changes that happened on only one side, and **stops on a true conflict** (both sides changed the same field) under the default `--on-conflict stop` — making zero changes until you resolve it. Inspect first with `sync --dry-run`.

`push` and `pull` are **blunt, one-directional overwrites with no conflict detection** — keep them for deliberate "force one side to win" cases (or first-time bootstrap), not routine use:

| | Direction | Conflict detection | Safety net | Equivalent via sync |
|---|---|:--:|---|---|
| **`sync`** | both | **yes** (3-way, stops on conflict) | base snapshot | — |
| **`push`** | YAML → ClickUp | none — clobbers UI edits | auto ClickUp-state backup | `sync --on-conflict local` |
| **`pull`** | ClickUp → YAML | none — clobbers local YAML edits | auto YAML-file backup | `sync --on-conflict remote` |

Both `push` and `pull` print a one-time warning banner describing what they'll overwrite, and both auto-create a backup first (disable with `--no-backup`). Prefer the `sync` equivalents — they do the same thing but auto-resolve the non-conflicting changes and stop before clobbering a genuine collision.

> **⚠️ Runtime — don't let a 2-minute shell timeout kill a sync mid-run.** A full `sync`/`push` re-issues an Epic-dropdown update for **every** task in the list, so on a board of dozens of tasks a single run can take **several minutes** — longer than a default 120 s command timeout (e.g. an agent's Bash tool). If the run is killed during the **create** phase, tasks already created in ClickUp may not have had their `clickup_id` written back to the YAML yet → a naive retry **creates duplicates**.
>
> - **Run it detached / in the background** (an agent's `run_in_background`, or `nohup … &` / a tmux pane), or set the shell timeout to **≥ 10 minutes**. Never run it under the default 2-minute cap.
> - **If a run *was* interrupted:** check `git status` on the YAML. If it's unmodified and the new tasks are still `clickup_id: null`, the run died *before* creating anything — safe to re-run. If the YAML gained ids (or `git status` is dirty), the create phase started — re-run carefully and verify no duplicates were created in ClickUp.
> - Always preview with `sync --dry-run` first (read-only, fast enough).

## Multi-Tag, Milestones, and Custom Dropdowns

The tool maintains an additive multi-tag set per story. Sources merge in
this order (deduped case-insensitively):

1. The epic's `name` (always present — preserves the legacy single-tag behavior).
2. Explicit `tags: [...]` on the story.
3. The lowercased `milestone_label` slug (e.g., `M1` → `m1`).

Pre-existing tags on the ClickUp task that are **not** in the YAML's
"managed tag universe" (every epic name + every milestone slug + every
explicit tag in YAML) are **preserved** on push — that means tags added
manually in the ClickUp UI survive.

Tags that **are** in the managed universe but not on the story being
pushed are treated as stale and removed — that's how an epic or milestone
reassignment in YAML actually takes effect.

## Assignees (bidirectional)

Each story can carry an `assignees` list of human-readable strings — emails
(preferred) or ClickUp usernames:

```yaml
stories:
  - name: Example task
    clickup_id: 860000001
    points: 2
    status: backlog
    assignees:
      - alice@example.com
```

Resolution is automatic: `push` looks up the list's member roster and converts
each string to the numeric ClickUp user id the API needs; `pull` writes the
remote assignees (as emails) back into the YAML. Names not on the roster are
**warned and skipped**, never fatal.

Semantics mirror tags' coexistence with the ClickUp UI:

| YAML on the story | Push behaviour |
|---|---|
| no `assignees` key | **Unmanaged** — ClickUp assignees left untouched (UI assignments preserved) |
| `assignees: []` | **Clear** — all assignees removed |
| `assignees: [a, b]` | **Authoritative** — ClickUp reconciled to exactly this set |

`pull` always reads remote assignees back — so if someone reassigns a task in
the ClickUp UI, the next `pull`/`sync` reflects it in YAML. Under `sync`, the
`--conflict` strategy applies at the whole-set level (`local` = YAML wins,
`remote` = ClickUp wins, `ask` = one prompt per task when they diverge).

## Dependencies (waiting_on edges)

Each story can declare a `depends_on` list — the ClickUp ids of tasks it
**waits on** (a "waiting on" dependency). Targets are referenced by the same id
stored in another story's `clickup_id`:

```yaml
stories:
  - name: Ingestion adapter
    clickup_id: 860000002
    points: 3
    status: backlog
    depends_on:
      - 860000003   # waits on the design-doc sign-off
```

Only the **waiting_on** direction is modeled; ClickUp maintains the mirrored
"blocking" edge on the other task automatically. Semantics mirror assignees:

| YAML on the story | Push behaviour |
|---|---|
| no `depends_on` key | **Unmanaged** — ClickUp dependencies left untouched (UI-added edges preserved) |
| `depends_on: []` | **Clear** — the task's waiting_on edges removed |
| `depends_on: [id, …]` | **Authoritative** — ClickUp reconciled to exactly this set |

Edges are reconciled in a **second pass**, after the create/update pass, so a
task created in the same run already has its `clickup_id` and can be referenced.
A target that doesn't exist yet (no `clickup_id`) can't be referenced — declare
the edge once both tasks exist and it resolves on the next run. `pull` reads
remote waiting_on edges back into `depends_on`; `diff` shows mismatches. Requires
the **Dependencies ClickApp** enabled on the Space.

`push`, `sync`, and `merge` all run the same second pass, so adding a dependency
needs nothing beyond your normal command — including `sync --dry-run → sync`.
Edges are YAML-authoritative (the table above) and are **not** base-tracked 3-way
fields: declaring `depends_on` in YAML is *applied*, never surfaced as a sync
conflict. `pull`/`diff` round-trip them too.

## Relations (non-blocking "linked tasks")

Each story can also declare a `related` list — ClickUp's non-blocking
"linked tasks" (the relate/link feature, `POST /task/{id}/link/{links_to}`).
Unlike `depends_on`, a link implies **no ordering or blocking** — it's a plain
association. Targets are referenced by `clickup_id`, exactly like `depends_on`:

```yaml
stories:
  - name: Ingestion adapter
    clickup_id: 860000002
    related:
      - 860000004   # see-also: a related spike
```

A link is **non-directional** — ClickUp records it on both endpoints — so the
reader collapses each link to "the other end," and the relation round-trips
identically regardless of which task created it. Semantics mirror `depends_on`:

| YAML on the story | Behaviour |
|---|---|
| no `related` key | **Unmanaged** — ClickUp links left untouched (UI-added links preserved) |
| `related: []` | **Clear** — the task's links removed |
| `related: [id, …]` | **Authoritative** — ClickUp reconciled to exactly this set |

Reconciled by the same second pass as `depends_on` (`push`/`sync`/`merge`), read
back by `pull`, and shown by `diff`.

> **Symmetric-link footgun.** A link lives on *both* endpoints. If you manage
> `related` on **both** tasks of a pair and only one lists the other, the
> authoritative reconcile will **delete** the link (the side that doesn't list it
> wins its own `related: []`/`[other]`). To avoid surprise: declare the link on
> **both** ends, or run `pull` first (it symmetrizes — writing the reciprocal
> `related` onto both stories), then edit. `sync`/`merge` print a **loud warning
> before removing any edge** (dependency or relation) so a destructive deletion is
> never silent; `push` removes silently by its authoritative-overwrite contract.

## Descriptions (markdown / task-mention safe)

Descriptions are read as `markdown_description` and written as `markdown_content`,
so an **embedded task-mention tile survives the round-trip** as a `[label](url)`
link. (ClickUp's plain `description`/`text_content` flattens a mention to
whitespace — silently dropping the reference — which is why the markdown form is
used.) ClickUp is **not** storing two separate fields: a description is stored
once and rendered both ways, so existing tasks need no migration — every task
already returns a valid `markdown_description`.

The markdown rendering applies two transformations that the tool normalizes on
read so they never show as spurious diffs:

- **Backslash-escaped punctuation** — `Article_type` comes back as
  `Article\_type`; unescaped back to the authored text.
- **Auto-linkified bare URLs/emails/domains** — `alice@example.com` comes back
  as `[alice@example.com](mailto:alice@example.com)`; *self-referential* links
  (label == url, ignoring scheme) are collapsed to the bare text. A genuine
  mention or labeled link — where the label differs from the url — is preserved.

The `Points · Milestone · Sprint` meta-header behaves exactly as before; it's
prepended on push and stripped on pull regardless of markdown.

### Pushing a custom dropdown field (e.g., a PM-curated "Epic" field)

If ClickUp has a single-select dropdown custom field that mirrors the epic
axis (some teams prefer this to tags), declare the mapping in the YAML
`project:` block and push will set it on every story automatically:

```yaml
project:
  name: My Project
  clickup_list_id: '900000000000'
  epic_dropdown_field_id: '00000000-0000-0000-0000-000000000000'
  epic_dropdown_options:
    'Epic One': '11111111-1111-1111-1111-111111111111'
    'Epic Two': '22222222-2222-2222-2222-222222222222'
    'Epic Three': '33333333-3333-3333-3333-333333333333'
    'Epic Four': '44444444-4444-4444-4444-444444444444'
```

If either field is missing, the dropdown push is skipped silently — old
YAML files keep working unchanged. Per-story override via
`epic_dropdown_value: "<epic name>"` on a single story.

## Backups (push and pull)

Each command backs up **the side it's about to overwrite**, and `--no-backup`
disables whichever applies:

- **`push`** snapshots the current **ClickUp state** to a YAML file. To recover
  from a bad push, **`push` that backup file back up** (`push <backup-file>`) —
  it restores ClickUp to the snapshot. (Not `pull` — pull reads ClickUp's live
  API, never a backup file.) Runs by default for any non-empty list; new empty
  sandbox lists skip it.
- **`pull`** copies your **YAML file** (the thing pull overwrites) to
  `.clickup-sync/yaml-backup-<stem>-<iso>.yaml` beside the project — the same
  gitignored sidecar that holds the base snapshot. To recover from a bad pull,
  copy that backup back over your YAML. If the backup can't be written, pull
  **aborts** rather than overwrite unprotected (rerun with `--no-backup` to
  proceed without one). Runs by default.
- `--backup-to /path` overrides the location for either; `--backup-to` with no
  value uses the default. `sync`/`merge` accept `--backup-to` but don't
  auto-backup — pass it to opt in.

## 3-way merge (sync)

`sync` is a **3-way merge** whenever a base snapshot exists. The base — the
field values as of the last successful reconcile — lives in a sidecar file per
list at `<yaml-dir>/.clickup-sync/base-<list_id>.json`, written automatically
by every `push`, `pull`, and `sync`. With a base, `sync` can tell *which* side
changed a field instead of treating every difference as a conflict:

| Field changed since base | Action |
|---|---|
| only in YAML | push to ClickUp (auto) |
| only in ClickUp | pull into YAML (auto) |
| both sides, same value | nothing |
| both sides, **different** values | true conflict → `--on-conflict` |

True conflicts are handled by `--on-conflict`:

| `--on-conflict` | Behaviour |
|---|---|
| `stop` (default) | abort the whole sync, change nothing, print the conflict list |
| `local` | YAML wins the conflicting fields |
| `remote` | ClickUp wins the conflicting fields |

`stop` is fail-safe — no silent data loss. With **no base** (e.g. the first
run), `sync` falls back to the legacy 2-way reconcile below and writes a base
for next time.

**Scope:** 3-way covers the scalar synced fields (name, status, description,
priority, milestone, due_date, start_date). Assignees and tags are not
base-tracked — assignee divergence is surfaced as a conflict (so `stop` catches
it; `local`/`remote` applies that direction). Keep `status_map` 1:1 — a
many-to-one mapping makes the pull-side reverse-lookup ambiguous.

**Dates** are date-only (`YYYY-MM-DD`), pushed at noon UTC. Stable for
workspaces UTC−12…UTC+11; a date set in the **ClickUp UI** of a UTC+12-or-east
workspace may pull back one calendar day earlier (stable, never oscillates).
YAML-originated dates are exact in every timezone.

## Conflict Strategies (legacy 2-way — only when no base exists yet)

Used with `sync --conflict`:

| Strategy | Behaviour |
|----------|-----------|
| `local` | YAML wins all differences (equivalent to push) |
| `remote` | ClickUp wins all differences (equivalent to pull) |
| `ask` | Interactive per-field prompt (default) |
| `merge` | LLM proposes a merged value; you confirm each |

## Field coverage & limitations

Which task fields the sync actually handles, and in which direction. **If a
field is not listed as supported, the tool ignores it — it is neither pushed
nor pulled, and a value set in the ClickUp UI will be invisible to the YAML
(and vice-versa).**

| Field | Push (YAML→ClickUp) | Pull (ClickUp→YAML) |
|-------|:--:|:--:|
| name | ✅ | ✅ |
| status (via `status_map`) | ✅ | ✅ |
| description (with `Points/Milestone/Sprint` meta header) | ✅ | ✅ |
| priority (`1`=urgent, `2`=high, `3`=normal, `4`=low, `null`=none) | ✅ | ✅ |
| milestone (`custom_item_id`) | ✅ | ✅ |
| due_date / start_date (`YYYY-MM-DD`, date-only) | ✅ | ✅ |
| assignees | ✅ | ✅ |
| `depends_on` (waiting_on edges) | ✅ (push/sync/merge; UI-added edges preserved) | ✅ (pull/diff) |
| `related` (non-blocking linked tasks) | ✅ (push/sync/merge; UI-added links preserved) | ✅ (pull/diff) |
| tags | ✅ (additive; UI-added tags preserved) | ⚠️ epic placement only — UI tag *edits* are **not** pulled into YAML |
| Epic dropdown (one configured custom field) | ✅ | ❌ |

**Not implemented at all (silently ignored both directions):**

- **`time_estimate`** — not synced.
- **Native ClickUp `points` field** — not synced. The YAML `points` value is
  shown only as `Points: N` text in the description. Native points also require
  the **Sprint Points ClickApp** enabled on the list (a `PUT {points: N}`
  otherwise 400s with `ITEM_225`), so it stays out of scope until that's on.
- **Arbitrary custom fields** — only the single configured Epic dropdown is
  handled. Other custom fields (any other single-select or text field on the
  board) are not read or written.

Caveat on `priority`: a YAML value only reaches the board on a `push`/`sync`
that runs *after* the value is set — setting it in YAML and never pushing
leaves the board unchanged (this is data drift, not a bug). Likewise, push
cannot reliably *clear* a board priority back to none.

## Tags & milestones (read this — two "milestone" concepts)

There are **two unrelated things both called "milestone"**:

| YAML | Becomes in ClickUp | Use |
|---|---|---|
| `milestone_label: M1` | a **tag** `m1` (lowercased) | group tasks into a milestone (a tag set, e.g. all `m2` tasks) |
| `milestone: true` | a **native Milestone task** (`custom_item_id=1`, the ◆ diamond) | a single milestone marker task (can carry its own due date) |

They're independent: a task can be tagged `m2` *and/or* be a ◆ milestone. To give a milestone a **due date on the board**, either date every member task or add one `milestone: true` marker task (tag it `milestone_label` too so it sits in the group). Native milestone creation requires the **Milestones ClickApp** enabled on the Space (sandbox-verified); native `points` similarly requires the **Sprint Points ClickApp** (else `PUT` 400s `ITEM_225`).

### Tag sync is push-authoritative, NOT pull-tracked (the limit)

Sandbox-verified behavior:

- **Push (YAML→ClickUp):** the tag set on a story = epic name (if `push_epic_tag`) + `milestone_label` slug (`m1`) + `sprint_target` slug (`s2`) + explicit `tags:[]`. Additive. Tags in the **managed universe** (all epic names + all milestone slugs + all explicit tags across the YAML) that are *not* on a story are stripped as stale — that's how a milestone/sprint reassignment takes effect. Tags **outside** the managed universe (added in the ClickUp UI) are **preserved**.
- **Pull (ClickUp→YAML): tags are read only for epic placement.** A tag added or removed **in the ClickUp UI does NOT come back into the YAML** — and on the next push a managed tag missing from YAML is reverted. Tags are also **not** part of the 3-way merge (no conflict detection on tags).
- **Rule of thumb:** manage `m`/`s`/milestone tags **from the YAML**; don't edit them in the ClickUp UI expecting them to round-trip. Ad-hoc UI tags outside the managed set are safe.

## Setup

```bash
# Install dependency
pip install pyyaml

# Configure credentials
cp .env.example clickup.env
# Edit clickup.env with your ClickUp API token
# Optionally add OPENAI_API_KEY for 'merge' command
```

## YAML File Structure

```yaml
project:
  name: My Project
  clickup_list_id: '901414096256'
  last_synced: '2026-02-08T23:59:01+00:00'

status_map:
  done: done
  in_progress: current sprint
  upcoming_sprint: upcoming sprint
  backlog: backlog

epics:
  - number: 1
    name: Foundation
    status: done
    points: 21
    priority: 3
    stories:
      - name: As a developer, I want X so that Y
        clickup_id: 86b8f2a23   # populated after first push
        points: 3
        status: done
        description: 'Optional description'
        priority: 3
```

## Usage

```bash
# Check what's out of sync (no changes made)
python3 clickup.py diff project.yaml

# Push local changes to ClickUp
python3 clickup.py push project.yaml

# Pull ClickUp status updates into YAML
python3 clickup.py pull project.yaml

# Full bidirectional sync, ask on each conflict
python3 clickup.py sync project.yaml --conflict ask

# Full sync, YAML wins all conflicts
python3 clickup.py sync project.yaml --conflict local

# LLM-assisted merge
python3 clickup.py merge project.yaml

# Offline status summary
python3 clickup.py status project.yaml

# Dry run (show what would happen)
python3 clickup.py push project.yaml --dry-run
```

## Roadmap

- **Subtasks.** The ClickUp API supports them (create a task with a
  `parent: <task_id>` field; read via `?include_subtasks=true`). Model them
  either as a `parent:` reference on a story or as a nested `subtasks:` list,
  reconciled like the create/update pass. Today stories are flat top-level tasks
  only.

## Credentials

Each secret resolves in a fixed precedence — **environment variable → [`pass`](https://www.passwordstore.org/) → legacy `~/bin/*.env` file** — so the canonical store is `pass`, while existing `.env` setups keep working untouched.

| Secret | Env var | `pass` entry | `.env` fallback |
|---|---|---|---|
| ClickUp token (production) | `CLICKUP_API_TOKEN` | `clickup/api-token` | `~/bin/clickup.env` |
| ClickUp token (sandbox, with `--sandbox`) | `CLICKUP_API_TOKEN_SANDBOX` | `clickup/sandbox-api-token` | `~/bin/clickup-sandbox.env` |
| OpenAI key (optional, for `merge`) | `OPENAI_API_KEY` | `clickup/openai-key` | `~/bin/clickup.env` |

```bash
# Recommended: store in pass
pass insert clickup/api-token
pass insert clickup/sandbox-api-token   # only if you use --sandbox
```

Pass `--sandbox` on any command to target the sandbox ClickUp account instead of production (selects the sandbox token from whichever source above resolves first). Get a ClickUp API token at: **Settings → Apps → API Token**.

## Logging

All operations are logged to `~/tmp/clickup_sync.log` (debug level) and stdout (info level).

## Requirements

- Python 3.10+
- `pyyaml` (`pip install pyyaml`)
- `openai` API key (optional, only for `merge` command)
