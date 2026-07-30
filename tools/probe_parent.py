#!/usr/bin/env python3
"""Throwaway probe for issue #20: does ClickUp's API support parent/child well
enough for this tool to model hierarchy in YAML?

Answers the spike checklist empirically (response shapes, not docs):
  1. POST /list/{id}/task with `parent` -> real subtask? what comes back?
  2. GET /list/{id}/task?subtasks=true -> is the child returned with `parent`
     populated (i.e. readable back for 3-way base tracking)?
  2b. Same GET *without* subtasks=true -> does the child vanish?
  3. Can `parent` be CHANGED on PUT /task/{id} (re-parenting)? Unset to null?
  4. Do tags / custom fields still work on a subtask?

Sandbox only -- refuses to run without CLICKUP_SANDBOX, like
tools/capture_edge_fixture.py. Creates throwaway tasks and deletes them.

    python3 tools/probe_parent.py <sandbox_list_id> [<sandbox_list_id_2>]
"""

import json
import os
import sys
import urllib.parse
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

os.environ["CLICKUP_SANDBOX"] = "1"  # set before import: the token loader is lazy

import clickup  # noqa: E402

OUT = HERE.parent / "probe-parent-findings.json"

# Fields we care about when reporting a task's shape.
KEYS = ("id", "name", "parent", "top_level_parent", "subtasks", "linked_tasks")


def shape(task: dict) -> dict:
    """Just the hierarchy-relevant slice of a task body, plus key inventory."""
    out = {k: task.get(k) for k in KEYS if k in task}
    out["_all_keys"] = sorted(task.keys())
    out["_tags"] = [t.get("name") for t in task.get("tags") or []]
    out["_custom_field_names"] = [
        f.get("name") for f in task.get("custom_fields") or []
    ]
    return out


def raw_get(token: str, path: str) -> dict:
    return clickup._api_request("GET", f"{clickup.CLICKUP_BASE}{path}", token)


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    list_id = sys.argv[1]
    other_list_id = sys.argv[2] if len(sys.argv) > 2 else None
    token = clickup.get_clickup_token()

    findings: dict = {"list_id": list_id, "steps": {}}
    created: list[str] = []

    def record(step: str, value) -> None:
        findings["steps"][step] = value
        print(f"\n=== {step} ===")
        print(json.dumps(value, indent=2)[:3000])

    def attempt(step: str, fn):
        try:
            value = fn()
            record(step, value)
            return value
        except Exception as e:  # noqa: BLE001 - probe: record the failure verbatim
            record(step, {"ERROR": type(e).__name__, "message": str(e)})
            return None

    try:
        # --- 1. create a parent, then a child via `parent` on create ---------
        p = clickup.clickup_create_task(token, list_id, {"name": "PROBE-PARENT-1"})
        created.append(p["id"])
        record("1a_create_parent_response", shape(p))

        c = attempt(
            "1b_create_child_with_parent_response",
            lambda: shape(
                clickup.clickup_create_task(
                    token, list_id, {"name": "PROBE-CHILD-1", "parent": p["id"]}
                )
            ),
        )
        child_id = c.get("id") if c and "id" in c else None
        if child_id:
            created.append(child_id)

        # --- 2. read-back: list endpoint with and without subtasks ----------
        def list_shapes(with_subtasks: bool) -> dict:
            q = (
                "?include_closed=true&include_markdown_description=true"
                + ("&subtasks=true" if with_subtasks else "")
            )
            resp = raw_get(token, f"/list/{list_id}/task{q}")
            return {
                "task_count": len(resp.get("tasks", [])),
                "tasks": [
                    shape(t)
                    for t in resp.get("tasks", [])
                    if t["id"] in (p["id"], child_id)
                ],
                "child_present": any(t["id"] == child_id for t in resp.get("tasks", [])),
            }

        attempt("2a_list_with_subtasks_true", lambda: list_shapes(True))
        attempt("2b_list_without_subtasks_param", lambda: list_shapes(False))
        if child_id:
            attempt(
                "2c_get_single_child_task",
                lambda: shape(clickup.clickup_get_task(token, child_id)),
            )
            attempt(
                "2d_get_single_parent_task",
                lambda: shape(clickup.clickup_get_task(token, p["id"])),
            )

        # --- 3. re-parenting via PUT ----------------------------------------
        p2 = clickup.clickup_create_task(token, list_id, {"name": "PROBE-PARENT-2"})
        created.append(p2["id"])

        # 3a: flat task -> child, by PUT parent
        d = clickup.clickup_create_task(token, list_id, {"name": "PROBE-FLAT-1"})
        created.append(d["id"])
        attempt(
            "3a_put_parent_on_flat_task",
            lambda: shape(
                clickup.clickup_update_task(token, d["id"], {"parent": p["id"]})
            ),
        )
        attempt(
            "3a_readback",
            lambda: shape(clickup.clickup_get_task(token, d["id"])),
        )

        # 3b: move an existing child to a different parent
        if child_id:
            attempt(
                "3b_put_reparent_child_to_other_parent",
                lambda: shape(
                    clickup.clickup_update_task(token, child_id, {"parent": p2["id"]})
                ),
            )
            attempt(
                "3b_readback",
                lambda: shape(clickup.clickup_get_task(token, child_id)),
            )

            # 3c: unparent (promote back to top level) via null
            attempt(
                "3c_put_parent_null_unparent",
                lambda: shape(
                    clickup.clickup_update_task(token, child_id, {"parent": None})
                ),
            )
            attempt(
                "3c_readback",
                lambda: shape(clickup.clickup_get_task(token, child_id)),
            )

            # 3d: re-attach so later steps have a real subtask again
            attempt(
                "3d_put_parent_reattach",
                lambda: shape(
                    clickup.clickup_update_task(token, child_id, {"parent": p["id"]})
                ),
            )

        # 3e: parent in a DIFFERENT list (cross-list hierarchy)
        if other_list_id:
            x = clickup.clickup_create_task(
                token, other_list_id, {"name": "PROBE-OTHER-LIST-PARENT"}
            )
            created.append(x["id"])
            attempt(
                "3e_child_with_parent_in_other_list",
                lambda: shape(
                    clickup.clickup_create_task(
                        token, list_id, {"name": "PROBE-XLIST-CHILD", "parent": x["id"]}
                    )
                ),
            )

        # --- 4. tags + custom fields on a subtask ---------------------------
        if child_id:
            attempt(
                "4a_add_tag_to_subtask",
                lambda: clickup.clickup_add_tag(token, child_id, "probe-epic"),
            )
            attempt(
                "4b_subtask_readback_after_tag",
                lambda: shape(clickup.clickup_get_task(token, child_id)),
            )
            # Does the subtask carry the same custom-field inventory (i.e. can
            # the Epic dropdown be set on it)?
            attempt(
                "4c_list_accessible_custom_fields",
                lambda: [
                    {"id": f["id"], "name": f["name"], "type": f["type"]}
                    for f in raw_get(token, f"/list/{list_id}/field").get("fields", [])
                ],
            )
    finally:
        # --- cleanup ---------------------------------------------------------
        deleted, failed = [], []
        for tid in reversed(created):
            try:
                clickup._api_request(
                    "DELETE", f"{clickup.CLICKUP_BASE}/task/{tid}", token
                )
                deleted.append(tid)
            except Exception as e:  # noqa: BLE001
                failed.append({"id": tid, "error": str(e)})
        findings["cleanup"] = {"deleted": deleted, "failed": failed}
        print(f"\n=== cleanup ===\ndeleted={deleted}\nfailed={failed}")
        OUT.write_text(json.dumps(findings, indent=2) + "\n")
        print(f"\nwrote {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
