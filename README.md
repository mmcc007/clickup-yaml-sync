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
  - name: Slack workspace export
    clickup_id: 86ba84bqa
    points: 2
    status: backlog
    assignees:
      - kathy@e-m-marketing.com
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

### Pushing a custom dropdown field (e.g., a PM-curated "Epic" field)

If ClickUp has a single-select dropdown custom field that mirrors the epic
axis (some teams prefer this to tags), declare the mapping in the YAML
`project:` block and push will set it on every story automatically:

```yaml
project:
  name: EM Marketing OS
  clickup_list_id: '901416587639'
  epic_dropdown_field_id: 'eb53bc71-c0c7-40ed-93cb-7f0b993900e6'
  epic_dropdown_options:
    'Relationship Signal Automation': 'f110326a-b184-44bd-8880-ddd503d3e8c9'
    'Magnit Monitoring System': '85bf365b-d601-40a2-be5a-c5f1ba8ef280'
    'Infrastructure + CRM': 'de5b8e45-e45e-482e-9c05-5764e2d2b8f9'
    'Kickoff / Access': '12d207f1-78c9-466f-9078-9ca726cd907e'
```

If either field is missing, the dropdown push is skipped silently — old
YAML files keep working unchanged. Per-story override via
`epic_dropdown_value: "<epic name>"` on a single story.

## Backup-before-push

Before any modifying call, `push` snapshots the current ClickUp state to a
YAML file you can `pull --conflict remote` from if a push goes sideways.

- `push` runs the backup by **default** for any non-empty list. New empty
  sandbox lists skip the backup.
- `push --no-backup` disables it.
- `push --backup-to /path/to/file.yaml` writes to a specific path.
- `push --backup-to` (no value) uses
  `~/tmp/clickup-backup-<list_id>-<iso>.yaml`.
- `sync` and `merge` accept the same `--backup-to` flag but **don't**
  auto-backup — pass the flag explicitly to opt in.

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
| tags | ✅ (additive; UI-added tags preserved) | ⚠️ epic placement only — UI tag *edits* are **not** pulled into YAML |
| Epic dropdown (one configured custom field) | ✅ | ❌ |

**Not implemented at all (silently ignored both directions):**

- **`time_estimate`** — not synced.
- **Native ClickUp `points` field** — not synced. The YAML `points` value is
  shown only as `Points: N` text in the description. Native points also require
  the **Sprint Points ClickApp** enabled on the list (a `PUT {points: N}`
  otherwise 400s with `ITEM_225`), so it stays out of scope until that's on.
- **Arbitrary custom fields** — only the single configured Epic dropdown is
  handled. Other custom fields (e.g. the **A–D deliverable-group field** on the
  EM Marketing board) are not read or written.

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

## Credentials

The script loads credentials from `~/bin/clickup.env` by default, then falls back to environment variables.

```bash
# ~/bin/clickup.env
CLICKUP_API_TOKEN=pk_your_token_here
OPENAI_API_KEY=sk_your_key_here  # optional
```

Get your ClickUp API token at: **Settings → Apps → API Token**

## Logging

All operations are logged to `~/tmp/clickup_sync.log` (debug level) and stdout (info level).

## Requirements

- Python 3.10+
- `pyyaml` (`pip install pyyaml`)
- `openai` API key (optional, only for `merge` command)
