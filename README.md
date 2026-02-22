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

## Conflict Strategies

Used with `sync --conflict`:

| Strategy | Behaviour |
|----------|-----------|
| `local` | YAML wins all conflicts (equivalent to push) |
| `remote` | ClickUp wins all conflicts (equivalent to pull) |
| `ask` | Interactive per-field prompt (default) |
| `merge` | LLM proposes a merged value; you confirm each |

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
