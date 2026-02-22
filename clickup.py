#!/usr/bin/env python3
"""clickup.py -- Bidirectional sync between YAML project files and ClickUp.

Commands:
  push    Push local YAML state to ClickUp (create new, update changed)
  pull    Pull ClickUp state into local YAML (update statuses, descriptions, detect new tasks)
  diff    Show differences between YAML and ClickUp (no changes made)
  sync    Full bidirectional sync with per-conflict resolution strategy
  merge   LLM-assisted conflict resolution (pull + push with intelligent merging)
  status  Show summary of project state from YAML (offline, no API calls)

Conflict strategies (--conflict flag for sync):
  local    YAML wins all conflicts (same as push)
  remote   ClickUp wins all conflicts (same as pull)
  ask      Prompt per conflict: local / remote / merge / skip (default)
  merge    Use LLM to propose merged value, confirm each
"""

import argparse
import copy
import json
import logging
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

try:
    import yaml
except ImportError:
    print("ERROR: PyYAML is required. Install with: pip install pyyaml", file=sys.stderr)
    sys.exit(1)


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOG_PATH = Path.home() / "tmp" / "clickup_sync.log"


def setup_logging() -> logging.Logger:
    logger = logging.getLogger("clickup_sync")
    logger.setLevel(logging.DEBUG)

    file_handler = logging.FileHandler(LOG_PATH, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    )
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(console_handler)

    return logger


log = setup_logging()


# ---------------------------------------------------------------------------
# Environment & config
# ---------------------------------------------------------------------------


def load_env_file(path: str) -> None:
    """Load key=value pairs from a file into os.environ (simple .env loader)."""
    env_path = Path(path)
    if not env_path.exists():
        return
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                # Handle export KEY=VALUE and KEY=VALUE
                if line.startswith("export "):
                    line = line[7:]
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip().strip("\"'")
                if key and key not in os.environ:
                    os.environ[key] = value


def get_clickup_token() -> str:
    load_env_file(str(Path.home() / "bin" / "clickup.env"))
    token = os.environ.get("CLICKUP_API_TOKEN", "")
    if not token:
        log.error("CLICKUP_API_TOKEN not set. Export it or add to ~/bin/clickup.env")
        sys.exit(1)
    return token


def get_openai_key() -> str:
    load_env_file(str(Path.home() / "bin" / "clickup.env"))
    key = os.environ.get("OPENAI_API_KEY", "")
    if not key:
        log.error("OPENAI_API_KEY not set. Export it or add to ~/bin/clickup.env")
        sys.exit(1)
    return key


# ---------------------------------------------------------------------------
# YAML loading / saving
# ---------------------------------------------------------------------------


def load_yaml(path: str) -> dict:
    with open(path, "r") as f:
        data = yaml.safe_load(f)
    if not data or "epics" not in data:
        log.error("Invalid YAML: missing 'epics' key")
        sys.exit(1)
    return data


def save_yaml(data: dict, path: str) -> None:
    data = copy.deepcopy(data)
    data["project"]["last_synced"] = datetime.now(timezone.utc).isoformat()
    with open(path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, allow_unicode=True, sort_keys=False, width=120)
    log.info(f"YAML saved to {path}")


# ---------------------------------------------------------------------------
# ClickUp API client
# ---------------------------------------------------------------------------

CLICKUP_BASE = "https://api.clickup.com/api/v2"
RATE_LIMIT_SLEEP = 0.5


def _api_request(
    method: str,
    url: str,
    token: str,
    data: Optional[dict] = None,
    retries: int = 1,
) -> dict:
    """Make an HTTP request to ClickUp API with rate limiting and retry."""
    headers = {
        "Content-Type": "application/json",
        "Authorization": token,
    }
    body = json.dumps(data).encode("utf-8") if data else None
    req = urllib.request.Request(url, data=body, headers=headers, method=method)

    for attempt in range(retries + 1):
        try:
            time.sleep(RATE_LIMIT_SLEEP)
            with urllib.request.urlopen(req, timeout=30) as resp:
                resp_body = resp.read().decode("utf-8")
                if resp_body:
                    return json.loads(resp_body)
                return {}
        except urllib.error.HTTPError as e:
            resp_body = e.read().decode("utf-8", errors="replace")
            if e.code == 429 and attempt < retries:
                wait = 5
                log.warning(f"Rate limited (429), waiting {wait}s before retry...")
                time.sleep(wait)
                continue
            log.error(f"HTTP {e.code}: {resp_body}")
            raise
        except urllib.error.URLError as e:
            log.error(f"URL error: {e.reason}")
            raise


def clickup_create_task(token: str, list_id: str, task_data: dict) -> dict:
    url = f"{CLICKUP_BASE}/list/{list_id}/task"
    return _api_request("POST", url, token, task_data)


def clickup_get_task(token: str, task_id: str) -> dict:
    url = f"{CLICKUP_BASE}/task/{task_id}"
    return _api_request("GET", url, token)


def clickup_update_task(token: str, task_id: str, task_data: dict) -> dict:
    url = f"{CLICKUP_BASE}/task/{task_id}"
    return _api_request("PUT", url, token, task_data)


def clickup_add_tag(token: str, task_id: str, tag_name: str) -> dict:
    url = f"{CLICKUP_BASE}/task/{task_id}/tag/{urllib.parse.quote(tag_name)}"
    return _api_request("POST", url, token)


def clickup_remove_tag(token: str, task_id: str, tag_name: str) -> dict:
    url = f"{CLICKUP_BASE}/task/{task_id}/tag/{urllib.parse.quote(tag_name)}"
    return _api_request("DELETE", url, token)


def clickup_list_tasks(token: str, list_id: str, page: int = 0) -> list[dict]:
    """Fetch all tasks from a ClickUp list (with pagination and subtasks)."""
    all_tasks: list[dict] = []
    while True:
        url = f"{CLICKUP_BASE}/list/{list_id}/task?subtasks=true&include_closed=true&page={page}"
        resp = _api_request("GET", url, token)
        tasks = resp.get("tasks", [])
        if not tasks:
            break
        all_tasks.extend(tasks)
        if resp.get("last_page", True):
            break
        page += 1
    return all_tasks


# ---------------------------------------------------------------------------
# OpenAI merge client
# ---------------------------------------------------------------------------


def openai_merge(
    api_key: str,
    yaml_value: str,
    clickup_value: str,
    task_name: str,
    field_name: str,
) -> str:
    """Ask GPT-4o-mini to merge two conflicting field values."""
    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    prompt = (
        f"You are merging two versions of a task field. "
        f"Produce the best merged result that preserves information from both sides.\n\n"
        f"Task: {task_name}\n"
        f"Field: {field_name}\n"
        f"Local (YAML) value:\n{yaml_value}\n\n"
        f"Remote (ClickUp) value:\n{clickup_value}\n\n"
        f"Return ONLY the merged value as a JSON object: {{\"merged\": \"...\"}}"
    )
    body = json.dumps({
        "model": "gpt-4o-mini",
        "messages": [{"role": "user", "content": prompt}],
        "response_format": {"type": "json_object"},
        "temperature": 0.2,
    }).encode("utf-8")

    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=30) as resp:
        result = json.loads(resp.read().decode("utf-8"))

    content = result["choices"][0]["message"]["content"]
    parsed = json.loads(content)
    return parsed.get("merged", content)


# ---------------------------------------------------------------------------
# Status mapping helpers
# ---------------------------------------------------------------------------


def yaml_status_to_clickup(status: str, status_map: dict) -> str:
    """Map a YAML status key to a ClickUp status name."""
    return status_map.get(status, status)


def clickup_status_to_yaml(clickup_status: str, status_map: dict) -> str:
    """Map a ClickUp status name back to a YAML status key."""
    reverse = {v.lower(): k for k, v in status_map.items()}
    return reverse.get(clickup_status.lower(), clickup_status.lower().replace(" ", "_"))


def priority_to_clickup(priority: Optional[int]) -> Optional[int]:
    """ClickUp priority: 1=urgent, 2=high, 3=normal, 4=low. Same mapping."""
    return priority


def clickup_priority_to_yaml(priority_obj: Optional[dict]) -> Optional[int]:
    """Extract priority int from ClickUp priority object."""
    if priority_obj and "id" in priority_obj:
        return int(priority_obj["id"])
    return None


# ---------------------------------------------------------------------------
# Diff engine
# ---------------------------------------------------------------------------

SYNCED_FIELDS = ["name", "status", "description", "priority", "milestone"]

# ClickUp custom_item_id values
CUSTOM_ITEM_TASK = 0
CUSTOM_ITEM_MILESTONE = 1


def normalize_description(desc: Optional[str]) -> str:
    """Normalize description for comparison (strip trailing whitespace/newlines)."""
    if not desc:
        return ""
    return desc.strip()


def compare_task(
    yaml_task: dict,
    clickup_task: dict,
    status_map: dict,
    is_epic: bool = False,
) -> list[dict]:
    """Compare a YAML task/story against its ClickUp counterpart.
    Returns list of {field, yaml_value, clickup_value} dicts for differences.
    """
    diffs: list[dict] = []

    # Name
    yaml_name = yaml_task.get("name", "")
    cu_name = clickup_task.get("name", "")
    if yaml_name != cu_name:
        diffs.append({"field": "name", "yaml": yaml_name, "clickup": cu_name})

    # Status
    yaml_status = yaml_status_to_clickup(yaml_task.get("status", ""), status_map)
    cu_status = clickup_task.get("status", {}).get("status", "").lower()
    if yaml_status.lower() != cu_status:
        diffs.append({"field": "status", "yaml": yaml_status, "clickup": cu_status})

    # Description
    yaml_desc = normalize_description(yaml_task.get("description"))
    cu_desc = normalize_description(clickup_task.get("description", "") or "")
    if yaml_desc != cu_desc:
        diffs.append({"field": "description", "yaml": yaml_desc, "clickup": cu_desc})

    # Priority
    yaml_priority = yaml_task.get("priority")
    cu_priority = clickup_priority_to_yaml(clickup_task.get("priority"))
    if yaml_priority != cu_priority:
        diffs.append({"field": "priority", "yaml": yaml_priority, "clickup": cu_priority})

    # Milestone
    yaml_milestone = bool(yaml_task.get("milestone"))
    cu_milestone = _is_clickup_milestone(clickup_task)
    if yaml_milestone != cu_milestone:
        diffs.append({"field": "milestone", "yaml": yaml_milestone, "clickup": cu_milestone})

    return diffs


def _is_clickup_milestone(cu_task: dict) -> bool:
    """Check if a ClickUp task is a milestone (custom_item_id == 1)."""
    return cu_task.get("custom_item_id") == CUSTOM_ITEM_MILESTONE


# ---------------------------------------------------------------------------
# Build ClickUp task body from YAML
# ---------------------------------------------------------------------------


def build_task_body(
    yaml_task: dict,
    status_map: dict,
    tags: Optional[list[str]] = None,
    default_priority: Optional[int] = None,
) -> dict:
    """Build a ClickUp API request body from a YAML task/story dict."""
    body: dict[str, Any] = {
        "name": yaml_task["name"],
        "status": yaml_status_to_clickup(yaml_task.get("status", "backlog"), status_map),
        "description": yaml_task.get("description", ""),
    }
    priority = yaml_task.get("priority") or default_priority
    if priority is not None:
        body["priority"] = priority
    if yaml_task.get("milestone"):
        body["custom_item_id"] = CUSTOM_ITEM_MILESTONE
    if tags:
        body["tags"] = tags
    return body


# ---------------------------------------------------------------------------
# Index helpers
# ---------------------------------------------------------------------------


def build_story_id_index(data: dict) -> dict[str, tuple[int, int]]:
    """Build {clickup_id: (epic_idx, story_idx)} for stories only."""
    index: dict[str, tuple[int, int]] = {}
    for ei, epic in enumerate(data.get("epics", [])):
        for si, story in enumerate(epic.get("stories", [])):
            scid = story.get("clickup_id")
            if scid:
                index[scid] = (ei, si)
    return index


def build_epic_name_map(data: dict) -> dict[str, int]:
    """Build {epic_name_lower: epic_index} for placing stories by tag."""
    m: dict[str, int] = {}
    for ei, epic in enumerate(data.get("epics", [])):
        name = epic.get("name", "")
        if name:
            m[name.lower()] = ei
    return m


def _extract_epic_name_from_tags(cu_task: dict, epic_name_map: dict) -> Optional[str]:
    """Find which epic a ClickUp task belongs to by matching tags to epic names."""
    for tag in cu_task.get("tags", []):
        tag_name = tag.get("name", "") if isinstance(tag, dict) else str(tag)
        tag_lower = tag_name.lower()
        if tag_lower in epic_name_map:
            return tag_lower
    return None


def _epic_tag(epic: dict) -> str:
    """Return the ClickUp tag name for an epic — the epic's name."""
    return epic.get("name", f"E{epic.get('number', 0)}")


def _has_tag(cu_task: dict, tag_name: str) -> bool:
    """Check if a ClickUp task has a specific tag (case-insensitive)."""
    for tag in cu_task.get("tags", []):
        name = tag.get("name", "") if isinstance(tag, dict) else str(tag)
        if name.lower() == tag_name.lower():
            return True
    return False


def _sync_tags(token: str, task_id: str, cu_task: dict, desired_tag: str) -> None:
    """Replace epic tags on a task with the desired tag.
    Removes old epic-pattern tags (E<number>) and any stale epic name tags."""
    current_tags = cu_task.get("tags", [])
    for tag in current_tags:
        name = tag.get("name", "") if isinstance(tag, dict) else str(tag)
        # Remove old E<number> pattern tags
        if name.upper().startswith("E") and name[1:].isdigit():
            try:
                clickup_remove_tag(token, task_id, name)
                log.info(f"    Removed old tag '{name}'")
            except Exception:
                pass
    # Add desired tag if not already present
    if not _has_tag(cu_task, desired_tag):
        try:
            clickup_add_tag(token, task_id, desired_tag)
        except Exception as e:
            log.warning(f"    Failed to add tag '{desired_tag}': {e}")


def _all_yaml_story_ids(data: dict) -> set[str]:
    """Collect all clickup_ids from stories across all epics."""
    ids: set[str] = set()
    for epic in data.get("epics", []):
        for story in epic.get("stories", []):
            cid = story.get("clickup_id")
            if cid:
                ids.add(cid)
    return ids


# ---------------------------------------------------------------------------
# Push command (flat stories with epic tag)
# ---------------------------------------------------------------------------


def cmd_push(data: dict, yaml_path: str, dry_run: bool = False) -> dict:
    """Push stories to ClickUp as flat top-level tasks with an epic tag.
    Epics exist only in YAML — they are NOT created in ClickUp."""
    token = get_clickup_token()
    list_id = data["project"]["clickup_list_id"]
    status_map = data.get("status_map", {})
    stats = {"created": 0, "updated": 0, "unchanged": 0, "errors": 0}

    for epic in data["epics"]:
        epic_name = epic["name"]
        tag = _epic_tag(epic)
        epic_priority = epic.get("priority")
        log.info(f"--- Epic {epic.get('number', '?')}: {epic_name} [{tag}] ---")

        for story in epic.get("stories", []):
            story_name = story["name"]
            if not story.get("clickup_id"):
                # CREATE story as top-level task with epic tag
                body = build_task_body(story, status_map, tags=[tag],
                                       default_priority=epic_priority)
                if dry_run:
                    log.info(f"  [DRY RUN] Would create: {story_name} [{tag}]")
                    stats["created"] += 1
                else:
                    try:
                        resp = clickup_create_task(token, list_id, body)
                        story["clickup_id"] = resp["id"]
                        story["task_id"] = resp.get("custom_id")
                        # Inherit epic priority into story YAML
                        if "priority" not in story and epic_priority is not None:
                            story["priority"] = epic_priority
                        save_yaml(data, yaml_path)  # incremental save
                        log.info(f"  Created: {story_name} -> {resp['id']} "
                                 f"({resp.get('custom_id')}) [{tag}]")
                        stats["created"] += 1
                    except Exception as e:
                        log.error(f"  Failed to create {story_name}: {e}")
                        stats["errors"] += 1
            else:
                # UPDATE story if changed
                try:
                    cu_task = clickup_get_task(token, story["clickup_id"])
                    _sync_metadata(story, cu_task)
                    # Inherit epic priority if story has none
                    if "priority" not in story and epic_priority is not None:
                        story["priority"] = epic_priority
                    diffs = compare_task(story, cu_task, status_map)
                    if diffs:
                        update_body = build_task_body(story, status_map,
                                                      default_priority=epic_priority)
                        if dry_run:
                            for d in diffs:
                                log.info(f"  [DRY RUN] Would update {story_name} "
                                         f"field '{d['field']}': "
                                         f"'{d['clickup']}' -> '{d['yaml']}'")
                        else:
                            clickup_update_task(token, story["clickup_id"], update_body)
                            for d in diffs:
                                log.info(f"  Updated {story_name} "
                                         f"field '{d['field']}': "
                                         f"'{d['clickup']}' -> '{d['yaml']}'")
                        stats["updated"] += 1
                    else:
                        stats["unchanged"] += 1
                except Exception as e:
                    log.error(f"  Failed to update {story_name}: {e}")
                    stats["errors"] += 1

    if not dry_run:
        save_yaml(data, yaml_path)

    log.info(f"\nPush complete: {stats['created']} created, {stats['updated']} updated, "
             f"{stats['unchanged']} unchanged, {stats['errors']} errors")
    return stats


# ---------------------------------------------------------------------------
# Pull command (flat stories matched by clickup_id or epic tag)
# ---------------------------------------------------------------------------


def cmd_pull(data: dict, yaml_path: str, dry_run: bool = False) -> dict:
    """Pull ClickUp tasks into YAML. Tasks are matched by clickup_id.
    New tasks are placed by their epic tag (E1, E9, etc.) or into _orphans."""
    token = get_clickup_token()
    list_id = data["project"]["clickup_list_id"]
    status_map = data.get("status_map", {})
    stats = {"updated": 0, "new": 0, "archived": 0, "unchanged": 0}

    log.info("Fetching all tasks from ClickUp...")
    cu_tasks = clickup_list_tasks(token, list_id)
    log.info(f"Fetched {len(cu_tasks)} tasks from ClickUp")

    story_index = build_story_id_index(data)
    epic_name_map = build_epic_name_map(data)
    seen_cu_ids: set[str] = set()

    for cu_task in cu_tasks:
        cu_id = cu_task["id"]
        seen_cu_ids.add(cu_id)

        if cu_id in story_index:
            # Known story — update fields
            ei, si = story_index[cu_id]
            story = data["epics"][ei]["stories"][si]
            if not dry_run:
                _sync_metadata(story, cu_task)
            diffs = compare_task(story, cu_task, status_map)
            if diffs:
                if not dry_run:
                    _apply_clickup_to_yaml(story, cu_task, status_map)
                for d in diffs:
                    log.info(f"  Updated '{story['name']}' "
                             f"field '{d['field']}': '{d['yaml']}' -> '{d['clickup']}'")
                stats["updated"] += 1
            else:
                stats["unchanged"] += 1
        else:
            # New task from ClickUp — place by epic tag
            new_story = _clickup_task_to_yaml_story(cu_task, status_map)
            epic_key = _extract_epic_name_from_tags(cu_task, epic_name_map)
            if epic_key is not None:
                target_ei = epic_name_map[epic_key]
                if not dry_run:
                    data["epics"][target_ei].setdefault("stories", []).append(new_story)
                log.info(f"  New story from ClickUp: '{cu_task['name']}' "
                         f"-> epic '{data['epics'][target_ei]['name']}'")
            else:
                orphan = _get_or_create_orphan_epic(data)
                if not dry_run:
                    orphan.setdefault("stories", []).append(new_story)
                log.info(f"  Orphan from ClickUp: '{cu_task['name']}' ({cu_id})")
            stats["new"] += 1

    # Detect archived stories (in YAML but not in ClickUp)
    for epic in data["epics"]:
        for story in epic.get("stories", []):
            scu_id = story.get("clickup_id")
            if scu_id and scu_id not in seen_cu_ids:
                if not dry_run:
                    story["archived_in_clickup"] = True
                log.info(f"  Archived: '{story['name']}' ({scu_id})")
                stats["archived"] += 1

    if not dry_run:
        save_yaml(data, yaml_path)

    log.info(f"\nPull complete: {stats['updated']} updated, {stats['new']} new, "
             f"{stats['archived']} archived, {stats['unchanged']} unchanged")
    return stats


def _apply_clickup_to_yaml(yaml_task: dict, cu_task: dict, status_map: dict) -> None:
    """Update a YAML task dict with values from ClickUp."""
    yaml_task["name"] = cu_task.get("name", yaml_task.get("name", ""))
    cu_status = cu_task.get("status", {}).get("status", "")
    yaml_task["status"] = clickup_status_to_yaml(cu_status, status_map)
    yaml_task["description"] = cu_task.get("description") or ""
    cu_priority = clickup_priority_to_yaml(cu_task.get("priority"))
    if cu_priority is not None:
        yaml_task["priority"] = cu_priority
    yaml_task["milestone"] = _is_clickup_milestone(cu_task)
    _sync_metadata(yaml_task, cu_task)


def _sync_metadata(yaml_task: dict, cu_task: dict) -> None:
    """Sync non-diffable metadata (task_id) from ClickUp into YAML."""
    cu_custom_id = cu_task.get("custom_id")
    if cu_custom_id:
        yaml_task["task_id"] = cu_custom_id


def _clickup_task_to_yaml_story(cu_task: dict, status_map: dict) -> dict:
    """Convert a ClickUp task to a YAML story dict."""
    return {
        "name": cu_task.get("name", ""),
        "clickup_id": cu_task["id"],
        "task_id": cu_task.get("custom_id"),
        "points": 0,
        "status": clickup_status_to_yaml(
            cu_task.get("status", {}).get("status", ""), status_map
        ),
        "milestone": _is_clickup_milestone(cu_task),
        "description": cu_task.get("description") or "",
    }


def _get_or_create_orphan_epic(data: dict) -> dict:
    """Get or create an '_orphans' epic for unmatched ClickUp tasks."""
    for epic in data["epics"]:
        if epic.get("name") == "_orphans":
            return epic
    orphan = {
        "number": None,
        "name": "_orphans",
        "clickup_id": None,
        "status": "backlog",
        "points": 0,
        "priority": 4,
        "sprint": None,
        "description": "Tasks found in ClickUp with no matching epic in YAML.",
        "stories": [],
    }
    data["epics"].append(orphan)
    return orphan


# ---------------------------------------------------------------------------
# Diff command (stories only — epics are YAML-local)
# ---------------------------------------------------------------------------


def cmd_diff(data: dict) -> dict:
    token = get_clickup_token()
    list_id = data["project"]["clickup_list_id"]
    status_map = data.get("status_map", {})
    stats = {"need_push": 0, "need_pull": 0, "mismatches": 0, "synced": 0, "archived": 0}

    log.info("Fetching all tasks from ClickUp...")
    cu_tasks = clickup_list_tasks(token, list_id)
    cu_by_id = {t["id"]: t for t in cu_tasks}

    log.info(f"\n{'='*80}")
    log.info("DIFF REPORT")
    log.info(f"{'='*80}\n")

    yaml_ids: set[str] = set()

    for epic in data["epics"]:
        tag = _epic_tag(epic)
        has_stories = False

        for story in epic.get("stories", []):
            story_name = story["name"]
            scu_id = story.get("clickup_id")

            if not scu_id:
                if not has_stories:
                    log.info(f"[{tag}] {epic['name']}:")
                    has_stories = True
                log.info(f"  [PUSH NEEDED] '{story_name}'")
                stats["need_push"] += 1
            elif scu_id not in cu_by_id:
                if not has_stories:
                    log.info(f"[{tag}] {epic['name']}:")
                    has_stories = True
                log.info(f"  [ARCHIVED] '{story_name}' ({scu_id})")
                stats["archived"] += 1
            else:
                yaml_ids.add(scu_id)
                diffs = compare_task(story, cu_by_id[scu_id], status_map)
                if diffs:
                    if not has_stories:
                        log.info(f"[{tag}] {epic['name']}:")
                        has_stories = True
                    log.info(f"  [MISMATCH] '{story_name}':")
                    for d in diffs:
                        yaml_val = _truncate(str(d["yaml"]), 60)
                        cu_val = _truncate(str(d["clickup"]), 60)
                        log.info(f"    {d['field']}: YAML='{yaml_val}' "
                                 f"vs ClickUp='{cu_val}'")
                    stats["mismatches"] += 1
                else:
                    stats["synced"] += 1

    # ClickUp tasks not in YAML
    epic_name_map = build_epic_name_map(data)
    for cu_task in cu_tasks:
        if cu_task["id"] not in yaml_ids:
            epic_key = _extract_epic_name_from_tags(cu_task, epic_name_map)
            tag_label = f"[{data['epics'][epic_name_map[epic_key]]['name']}] " if epic_key else ""
            log.info(f"[PULL NEEDED] {tag_label}'{cu_task['name']}' ({cu_task['id']})")
            stats["need_pull"] += 1

    log.info(f"\n{'='*80}")
    log.info(f"Summary: {stats['need_push']} need push, {stats['need_pull']} need pull, "
             f"{stats['mismatches']} mismatches, {stats['archived']} archived, "
             f"{stats['synced']} synced")
    log.info(f"{'='*80}")
    return stats


def _truncate(s: str, max_len: int) -> str:
    s = s.replace("\n", "\\n")
    if len(s) > max_len:
        return s[:max_len - 3] + "..."
    return s


# ---------------------------------------------------------------------------
# Merge command (LLM-assisted)
# ---------------------------------------------------------------------------


def cmd_merge(data: dict, yaml_path: str) -> dict:
    token = get_clickup_token()
    openai_key = get_openai_key()
    list_id = data["project"]["clickup_list_id"]
    status_map = data.get("status_map", {})
    stats = {"merged": 0, "skipped": 0, "errors": 0}

    log.info("Fetching all tasks from ClickUp for merge...")
    cu_tasks = clickup_list_tasks(token, list_id)
    cu_by_id = {t["id"]: t for t in cu_tasks}

    all_items: list[tuple[dict, dict, str]] = []
    for epic in data["epics"]:
        tag = _epic_tag(epic)
        for story in epic.get("stories", []):
            scu_id = story.get("clickup_id")
            scu_task = cu_by_id.get(scu_id) if scu_id else None
            if scu_task:
                all_items.append((story, scu_task, tag))

    for yaml_task, cu_task, tag in all_items:
        diffs = compare_task(yaml_task, cu_task, status_map)
        if not diffs:
            continue

        task_name = yaml_task.get("name", "unknown")
        label = f"[{tag}]"
        log.info(f"\n--- {label} {task_name} ---")

        for d in diffs:
            field = d["field"]
            yaml_val = str(d["yaml"])
            cu_val = str(d["clickup"])

            log.info(f"  Conflict on '{field}':")
            log.info(f"    YAML:    {_truncate(yaml_val, 80)}")
            log.info(f"    ClickUp: {_truncate(cu_val, 80)}")

            try:
                merged = openai_merge(openai_key, yaml_val, cu_val, task_name, field)
                log.info(f"    LLM merged: {_truncate(str(merged), 80)}")

                choice = input(f"  Accept merge for {field}? [y/n/l(ocal)/r(emote)] ").strip().lower()
                if choice == "y":
                    _apply_merged_value(yaml_task, cu_task, field, merged, status_map, token)
                    stats["merged"] += 1
                elif choice == "l":
                    # Keep local (YAML) value, push to ClickUp
                    _push_field_to_clickup(yaml_task, cu_task, field, status_map, token)
                    log.info(f"    Kept local value, pushed to ClickUp")
                    stats["merged"] += 1
                elif choice == "r":
                    # Keep remote (ClickUp) value, update YAML
                    _pull_field_to_yaml(yaml_task, cu_task, field, status_map)
                    log.info(f"    Kept remote value, updated YAML")
                    stats["merged"] += 1
                else:
                    log.info(f"    Skipped")
                    stats["skipped"] += 1
            except Exception as e:
                log.error(f"    Merge failed: {e}")
                stats["errors"] += 1

    save_yaml(data, yaml_path)
    log.info(f"\nMerge complete: {stats['merged']} merged, {stats['skipped']} skipped, "
             f"{stats['errors']} errors")
    return stats


def _apply_merged_value(
    yaml_task: dict,
    cu_task: dict,
    field: str,
    merged_value: str,
    status_map: dict,
    token: str,
) -> None:
    """Apply a merged value to both YAML and ClickUp."""
    cu_id = yaml_task.get("clickup_id") or cu_task.get("id")
    if field == "name":
        yaml_task["name"] = merged_value
        if cu_id:
            clickup_update_task(token, cu_id, {"name": merged_value})
    elif field == "status":
        yaml_task["status"] = clickup_status_to_yaml(merged_value, status_map)
        if cu_id:
            clickup_update_task(token, cu_id, {"status": merged_value})
    elif field == "description":
        yaml_task["description"] = merged_value
        if cu_id:
            clickup_update_task(token, cu_id, {"description": merged_value})
    elif field == "priority":
        try:
            p = int(merged_value)
        except (ValueError, TypeError):
            p = 3
        yaml_task["priority"] = p
        if cu_id:
            clickup_update_task(token, cu_id, {"priority": p})
    elif field == "milestone":
        is_ms = str(merged_value).lower() in ("true", "1", "yes")
        yaml_task["milestone"] = is_ms
        if cu_id:
            cid = CUSTOM_ITEM_MILESTONE if is_ms else CUSTOM_ITEM_TASK
            clickup_update_task(token, cu_id, {"custom_item_id": cid})


def _push_field_to_clickup(
    yaml_task: dict, cu_task: dict, field: str, status_map: dict, token: str
) -> None:
    """Push a single field from YAML to ClickUp."""
    cu_id = yaml_task.get("clickup_id") or cu_task.get("id")
    if not cu_id:
        return
    if field == "name":
        clickup_update_task(token, cu_id, {"name": yaml_task["name"]})
    elif field == "status":
        clickup_update_task(token, cu_id, {
            "status": yaml_status_to_clickup(yaml_task.get("status", ""), status_map)
        })
    elif field == "description":
        clickup_update_task(token, cu_id, {"description": yaml_task.get("description", "")})
    elif field == "priority":
        clickup_update_task(token, cu_id, {"priority": yaml_task.get("priority", 3)})
    elif field == "milestone":
        cid = CUSTOM_ITEM_MILESTONE if yaml_task.get("milestone") else CUSTOM_ITEM_TASK
        clickup_update_task(token, cu_id, {"custom_item_id": cid})


def _pull_field_to_yaml(yaml_task: dict, cu_task: dict, field: str, status_map: dict) -> None:
    """Pull a single field from ClickUp into YAML."""
    if field == "name":
        yaml_task["name"] = cu_task.get("name", "")
    elif field == "status":
        cu_status = cu_task.get("status", {}).get("status", "")
        yaml_task["status"] = clickup_status_to_yaml(cu_status, status_map)
    elif field == "description":
        yaml_task["description"] = cu_task.get("description") or ""
    elif field == "priority":
        yaml_task["priority"] = clickup_priority_to_yaml(cu_task.get("priority"))
    elif field == "milestone":
        yaml_task["milestone"] = _is_clickup_milestone(cu_task)


# ---------------------------------------------------------------------------
# Sync command (bidirectional with conflict resolution)
# ---------------------------------------------------------------------------

CONFLICT_STRATEGIES = ("ask", "local", "remote", "merge")


def cmd_sync(data: dict, yaml_path: str, conflict: str = "ask", dry_run: bool = False) -> dict:
    """Full bidirectional sync with per-conflict resolution.
    Stories are flat top-level tasks with epic tags. Epics are YAML-only."""
    token = get_clickup_token()
    list_id = data["project"]["clickup_list_id"]
    status_map = data.get("status_map", {})
    openai_key = get_openai_key() if conflict == "merge" else None
    stats = {
        "created_in_clickup": 0,
        "created_in_yaml": 0,
        "resolved_local": 0,
        "resolved_remote": 0,
        "resolved_merge": 0,
        "skipped": 0,
        "unchanged": 0,
        "archived": 0,
        "errors": 0,
    }

    # Phase 1: Fetch
    log.info("Fetching all tasks from ClickUp...")
    cu_tasks = clickup_list_tasks(token, list_id)
    cu_by_id = {t["id"]: t for t in cu_tasks}
    log.info(f"Fetched {len(cu_tasks)} tasks")

    seen_cu_ids: set[str] = set(t["id"] for t in cu_tasks)
    all_yaml_ids: set[str] = _all_yaml_story_ids(data)

    # Phase 2 & 4: Walk YAML stories, create or reconcile
    for epic in data["epics"]:
        epic_name = epic["name"]
        tag = _epic_tag(epic)
        epic_priority = epic.get("priority")
        log.info(f"--- Epic {epic.get('number', '?')}: {epic_name} [{tag}] ---")

        for story in epic.get("stories", []):
            story_name = story["name"]

            if not story.get("clickup_id"):
                # Create in ClickUp
                body = build_task_body(story, status_map, tags=[tag],
                                       default_priority=epic_priority)
                if dry_run:
                    log.info(f"  [DRY RUN] Would create: {story_name} [{tag}]")
                else:
                    try:
                        resp = clickup_create_task(token, list_id, body)
                        story["clickup_id"] = resp["id"]
                        story["task_id"] = resp.get("custom_id")
                        log.info(f"  Created: {story_name} -> {resp['id']} "
                                 f"({resp.get('custom_id')}) [{tag}]")
                    except Exception as e:
                        log.error(f"  Failed to create {story_name}: {e}")
                        stats["errors"] += 1
                        continue
                stats["created_in_clickup"] += 1
            elif story["clickup_id"] in cu_by_id:
                cu_task = cu_by_id[story["clickup_id"]]
                _sync_metadata(story, cu_task)
                diffs = compare_task(story, cu_task, status_map)
                if diffs:
                    _resolve_conflicts(
                        story, cu_task, diffs, story_name, "Story",
                        conflict, status_map, token, openai_key, stats, dry_run,
                    )
                else:
                    stats["unchanged"] += 1

    # Phase 3: ClickUp tasks not in YAML -> create in YAML
    epic_name_map = build_epic_name_map(data)
    for cu_task in cu_tasks:
        if cu_task["id"] in all_yaml_ids:
            continue
        new_story = _clickup_task_to_yaml_story(cu_task, status_map)
        epic_key = _extract_epic_name_from_tags(cu_task, epic_name_map)
        if epic_key is not None:
            target_ei = epic_name_map[epic_key]
            if not dry_run:
                data["epics"][target_ei].setdefault("stories", []).append(new_story)
            log.info(f"  New from ClickUp: '{cu_task['name']}' -> epic '{data['epics'][target_ei]['name']}'")
        else:
            orphan = _get_or_create_orphan_epic(data)
            if not dry_run:
                orphan.setdefault("stories", []).append(new_story)
            log.info(f"  Orphan from ClickUp: '{cu_task['name']}'")
        stats["created_in_yaml"] += 1

    # Phase 5: Detect archived stories
    for epic in data["epics"]:
        for story in epic.get("stories", []):
            scu_id = story.get("clickup_id")
            if scu_id and scu_id not in seen_cu_ids:
                if not dry_run:
                    story["archived_in_clickup"] = True
                log.info(f"  Archived: '{story['name']}' ({scu_id})")
                stats["archived"] += 1

    if not dry_run:
        save_yaml(data, yaml_path)

    log.info(f"\nSync complete:")
    log.info(f"  Created in ClickUp: {stats['created_in_clickup']}")
    log.info(f"  Created in YAML:    {stats['created_in_yaml']}")
    log.info(f"  Resolved (local):   {stats['resolved_local']}")
    log.info(f"  Resolved (remote):  {stats['resolved_remote']}")
    log.info(f"  Resolved (merge):   {stats['resolved_merge']}")
    log.info(f"  Skipped:            {stats['skipped']}")
    log.info(f"  Unchanged:          {stats['unchanged']}")
    log.info(f"  Archived:           {stats['archived']}")
    log.info(f"  Errors:             {stats['errors']}")
    return stats


def _resolve_conflicts(
    yaml_task: dict,
    cu_task: dict,
    diffs: list[dict],
    task_name: str,
    label: str,
    conflict: str,
    status_map: dict,
    token: str,
    openai_key: Optional[str],
    stats: dict,
    dry_run: bool,
) -> None:
    """Resolve field-level conflicts between YAML and ClickUp for one task."""
    cu_id = yaml_task.get("clickup_id") or cu_task.get("id")

    for d in diffs:
        field = d["field"]
        yaml_val = d["yaml"]
        cu_val = d["clickup"]

        if conflict == "local":
            # YAML wins -> push to ClickUp
            if not dry_run and cu_id:
                _push_field_to_clickup(yaml_task, cu_task, field, status_map, token)
            log.info(f"  {label} '{task_name}' {field}: local wins "
                     f"('{_truncate(str(cu_val), 40)}' -> '{_truncate(str(yaml_val), 40)}')")
            stats["resolved_local"] += 1

        elif conflict == "remote":
            # ClickUp wins -> pull into YAML
            if not dry_run:
                _pull_field_to_yaml(yaml_task, cu_task, field, status_map)
            log.info(f"  {label} '{task_name}' {field}: remote wins "
                     f"('{_truncate(str(yaml_val), 40)}' -> '{_truncate(str(cu_val), 40)}')")
            stats["resolved_remote"] += 1

        elif conflict == "merge":
            # LLM merge
            if not openai_key:
                log.error(f"  No OPENAI_API_KEY for merge on {task_name}.{field}")
                stats["errors"] += 1
                continue
            try:
                merged = openai_merge(openai_key, str(yaml_val), str(cu_val), task_name, field)
                log.info(f"  {label} '{task_name}' {field}:")
                log.info(f"    Local:  {_truncate(str(yaml_val), 60)}")
                log.info(f"    Remote: {_truncate(str(cu_val), 60)}")
                log.info(f"    Merged: {_truncate(str(merged), 60)}")
                choice = input(f"    Accept merge? [y/n/l(ocal)/r(emote)] ").strip().lower()
                if choice == "y":
                    if not dry_run:
                        _apply_merged_value(yaml_task, cu_task, field, merged, status_map, token)
                    stats["resolved_merge"] += 1
                elif choice == "l":
                    if not dry_run and cu_id:
                        _push_field_to_clickup(yaml_task, cu_task, field, status_map, token)
                    stats["resolved_local"] += 1
                elif choice == "r":
                    if not dry_run:
                        _pull_field_to_yaml(yaml_task, cu_task, field, status_map)
                    stats["resolved_remote"] += 1
                else:
                    stats["skipped"] += 1
            except Exception as e:
                log.error(f"  LLM merge failed for {task_name}.{field}: {e}")
                stats["errors"] += 1

        else:
            # "ask" — interactive per-field
            log.info(f"\n  {label} '{task_name}' conflict on '{field}':")
            log.info(f"    [L]ocal (YAML):    {_truncate(str(yaml_val), 60)}")
            log.info(f"    [R]emote (ClickUp): {_truncate(str(cu_val), 60)}")
            prompt_parts = ["l(ocal)", "r(emote)"]
            if openai_key or os.environ.get("OPENAI_API_KEY"):
                prompt_parts.append("m(erge via LLM)")
            prompt_parts.append("s(kip)")
            choice = input(f"    Choose: [{'/'.join(prompt_parts)}] ").strip().lower()

            if choice == "l":
                if not dry_run and cu_id:
                    _push_field_to_clickup(yaml_task, cu_task, field, status_map, token)
                log.info(f"    -> local wins")
                stats["resolved_local"] += 1
            elif choice == "r":
                if not dry_run:
                    _pull_field_to_yaml(yaml_task, cu_task, field, status_map)
                log.info(f"    -> remote wins")
                stats["resolved_remote"] += 1
            elif choice == "m":
                try:
                    oai_key = openai_key or get_openai_key()
                    merged = openai_merge(oai_key, str(yaml_val), str(cu_val), task_name, field)
                    log.info(f"    LLM proposed: {_truncate(str(merged), 60)}")
                    confirm = input(f"    Accept? [y/n] ").strip().lower()
                    if confirm == "y":
                        if not dry_run:
                            _apply_merged_value(yaml_task, cu_task, field, merged, status_map, token)
                        stats["resolved_merge"] += 1
                    else:
                        stats["skipped"] += 1
                except Exception as e:
                    log.error(f"    LLM merge failed: {e}")
                    stats["errors"] += 1
            else:
                log.info(f"    -> skipped")
                stats["skipped"] += 1


# ---------------------------------------------------------------------------
# Status command (offline)
# ---------------------------------------------------------------------------


def cmd_status(data: dict) -> None:
    project = data["project"]
    status_map = data.get("status_map", {})

    print(f"\nProject: {project['name']}")
    print(f"ClickUp List: {project['clickup_list_id']}")
    print(f"Last Synced: {project.get('last_synced') or 'never'}")

    total_points = 0
    total_stories = 0
    done_points = 0
    synced_count = 0

    header = (f"{'#':>3} {'Task ID':<11} {'Epic':<40} {'Status':<16} "
              f"{'Stories':>7} {'Points':>6} {'Synced':>6} {'Sprint':>6} {'MS':>2}")
    print(f"\n{header}")
    print("-" * len(header))

    for epic in data["epics"]:
        num = epic.get("number", "?")
        task_id = epic.get("task_id") or "-"
        name = epic["name"][:39]
        status = epic.get("status", "?")
        stories = epic.get("stories", [])
        points = epic.get("points", 0)
        sprint = epic.get("sprint") or "-"
        n_stories = len(stories)
        synced_stories = sum(1 for s in stories if s.get("clickup_id"))
        synced_label = f"{synced_stories}/{n_stories}" if n_stories else "-"
        ms_count = sum(1 for s in stories if s.get("milestone"))
        ms_flag = str(ms_count) if ms_count else ""

        total_points += points
        total_stories += n_stories
        synced_count += synced_stories
        if status == "done":
            done_points += points

        print(f"{str(num):>3} {task_id:<11} {name:<40} {status:<16} "
              f"{n_stories:>7} {points:>6} {synced_label:>6} {str(sprint):>6} {ms_flag:>2}")

    total_items = total_stories
    print("-" * len(header))
    print(f"{'':>3} {'':>11} {'TOTAL':<40} {'':>16} "
          f"{total_stories:>7} {total_points:>6} {synced_count}/{total_items}  ")
    print(f"\nDone: {done_points}/{total_points} points "
          f"({done_points * 100 // total_points if total_points else 0}%)")

    # Status breakdown
    status_counts: dict[str, int] = {}
    for epic in data["epics"]:
        s = epic.get("status", "unknown")
        status_counts[s] = status_counts.get(s, 0) + 1
        for story in epic.get("stories", []):
            ss = story.get("status", "unknown")
            status_counts[ss] = status_counts.get(ss, 0) + 1

    print("\nStatus breakdown:")
    for s, count in sorted(status_counts.items()):
        print(f"  {s}: {count}")


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Bidirectional sync between YAML project files and ClickUp",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "command",
        choices=["push", "pull", "diff", "sync", "merge", "status"],
        help="Command to execute",
    )
    parser.add_argument(
        "yaml_file",
        help="Path to the YAML project file",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would happen without making changes (push/pull/sync)",
    )
    parser.add_argument(
        "--conflict",
        choices=CONFLICT_STRATEGIES,
        default="ask",
        help="Conflict resolution strategy for sync (default: ask)",
    )

    args = parser.parse_args()

    if not os.path.exists(args.yaml_file):
        log.error(f"YAML file not found: {args.yaml_file}")
        sys.exit(1)

    data = load_yaml(args.yaml_file)

    if args.command == "status":
        cmd_status(data)
    elif args.command == "push":
        cmd_push(data, args.yaml_file, dry_run=args.dry_run)
    elif args.command == "pull":
        cmd_pull(data, args.yaml_file, dry_run=args.dry_run)
    elif args.command == "diff":
        cmd_diff(data)
    elif args.command == "sync":
        cmd_sync(data, args.yaml_file, conflict=args.conflict, dry_run=args.dry_run)
    elif args.command == "merge":
        cmd_merge(data, args.yaml_file)


if __name__ == "__main__":
    main()
