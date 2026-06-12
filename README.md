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

## Conflict Strategies

Used with `sync --conflict`:

| Strategy | Behaviour |
|----------|-----------|
| `local` | YAML wins all conflicts (equivalent to push) |
| `remote` | ClickUp wins all conflicts (equivalent to pull) |
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
| assignees | ✅ | ✅ |
| tags | ✅ (additive; UI-added tags preserved) | ⚠️ epic placement only — UI tag *edits* are **not** pulled into YAML |
| Epic dropdown (one configured custom field) | ✅ | ❌ |

**Not implemented at all (silently ignored both directions):**

- **`due_date` / `start_date`** — no deadline sync.
- **`time_estimate`** — not synced.
- **Native ClickUp `points` field** — the YAML `points` value is shown only as
  `Points: N` text in the description; it does not populate ClickUp's Sprint
  Points field.
- **Arbitrary custom fields** — only the single configured Epic dropdown is
  handled. Other custom fields (e.g. the **A–D deliverable-group field** on the
  EM Marketing board) are not read or written.

Caveat on `priority`: a YAML value only reaches the board on a `push`/`sync`
that runs *after* the value is set — setting it in YAML and never pushing
leaves the board unchanged (this is data drift, not a bug). Likewise, push
cannot reliably *clear* a board priority back to none.

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
