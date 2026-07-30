#!/usr/bin/env python3
"""Close the one gap the #20 spike left open: can the Epic dropdown be set on a
SUBTASK, and does this tool read the value back correctly?

The free sandbox workspace has no custom fields and the API cannot create one,
so this runs against a dedicated *test* workspace that does. That means it uses
the PRODUCTION token — so instead of the CLICKUP_SANDBOX gate the other probes
use, it refuses to touch any list outside ALLOWED_LISTS, and re-checks the
list's name before writing. The live board is in a different workspace entirely
and can never be reached by this script.

Also closes two smaller "not tested" items from the findings doc:
  - cross-list parenting (child in list A, parent in list B)
  - what happens to a child when its PARENT is deleted

    python3 tools/probe_subtask_custom_field.py

Creates throwaway tasks and deletes them. It never creates, edits or deletes a
custom field — only task values.
"""

import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

import clickup  # noqa: E402

# Hard allowlist: {list_id: expected list name}. A write to anything else, or to
# one of these whose name no longer matches, aborts before any call.
ALLOWED_LISTS = {
    "901417224622": "test list 1",
    "901417224774": "Test List 2",
}
PRIMARY = "901417224622"
SECONDARY = "901417224774"
OUT = HERE.parent / "probe-subtask-field-findings.json"


def assert_allowed(token: str, list_id: str) -> dict:
    if list_id not in ALLOWED_LISTS:
        raise SystemExit(f"REFUSING: list {list_id} is not in the allowlist")
    lst = clickup._api_request("GET", f"{clickup.CLICKUP_BASE}/list/{list_id}", token)
    if lst.get("name") != ALLOWED_LISTS[list_id]:
        raise SystemExit(
            f"REFUSING: list {list_id} is named {lst.get('name')!r}, expected "
            f"{ALLOWED_LISTS[list_id]!r} — the id may have been reused"
        )
    return lst


def dropdown_field(token: str, list_id: str) -> dict:
    fields = clickup._api_request(
        "GET", f"{clickup.CLICKUP_BASE}/list/{list_id}/field", token
    ).get("fields", [])
    for f in fields:
        if f.get("type") == "drop_down":
            return f
    raise SystemExit(f"no drop_down field on list {list_id}")


def main() -> int:
    token = clickup.get_clickup_token()
    assert_allowed(token, PRIMARY)
    assert_allowed(token, SECONDARY)

    field = dropdown_field(token, PRIMARY)
    options = (field.get("type_config") or {}).get("options") or []
    findings: dict = {
        "list": PRIMARY,
        "field": {"id": field["id"], "name": field["name"], "type": field["type"]},
        "options": [
            {"id": o.get("id"), "name": o.get("name"), "orderindex": o.get("orderindex")}
            for o in options
        ],
        "steps": {},
    }
    if not options:
        raise SystemExit("the drop_down field has no options to set")
    option = options[0]
    created: list[str] = []

    def record(step: str, value) -> None:
        findings["steps"][step] = value
        print(f"\n=== {step} ===")
        print(json.dumps(value, indent=1)[:2000])

    def attempt(step: str, fn):
        try:
            v = fn()
            record(step, v)
            return v
        except Exception as e:  # noqa: BLE001 - probe: record the failure verbatim
            record(step, {"ERROR": type(e).__name__, "message": str(e)})
            return None

    def cf_view(task: dict) -> dict:
        """What the tool itself sees: its reader's answer plus the raw entry."""
        raw = [
            {"id": f.get("id"), "name": f.get("name"), "value": f.get("value")}
            for f in task.get("custom_fields") or []
            if f.get("id") == field["id"]
        ]
        return {
            "id": task.get("id"),
            "parent": task.get("parent"),
            "raw_field_entry": raw,
            "tool_reads_option_id": clickup._current_dropdown_option_id(
                task, field["id"]
            ),
        }

    try:
        hub = clickup.clickup_create_task(token, PRIMARY, {"name": "CFPROBE-HUB"})
        created.append(hub["id"])
        child = clickup.clickup_create_task(
            token, PRIMARY, {"name": "CFPROBE-CHILD", "parent": hub["id"]}
        )
        created.append(child["id"])
        record("0_baseline", {"hub": hub["id"], "child": cf_view(child)})

        # --- 1. the gap: set the dropdown ON THE SUBTASK --------------------
        attempt(
            "1a_set_dropdown_on_subtask",
            lambda: clickup.clickup_set_custom_field(
                token, child["id"], field["id"], option["id"]
            ),
        )
        attempt(
            "1b_subtask_readback_single_task_endpoint",
            lambda: cf_view(clickup.clickup_get_task(token, child["id"])),
        )
        attempt(
            "1c_subtask_readback_list_endpoint",
            lambda: [
                cf_view(t)
                for t in clickup.clickup_list_tasks(token, PRIMARY)
                if t["id"] == child["id"]
            ],
        )
        # Does the tool consider it already-correct (the no-op skip from BUG #13)?
        attempt(
            "1d_no_op_skip_sees_current_value",
            lambda: {
                "expected_option_id": option["id"],
                "tool_reads": clickup._current_dropdown_option_id(
                    clickup.clickup_get_task(token, child["id"]), field["id"]
                ),
            },
        )

        # --- 2. cross-list parenting ---------------------------------------
        other_parent = clickup.clickup_create_task(
            token, SECONDARY, {"name": "CFPROBE-XLIST-HUB"}
        )
        created.append(other_parent["id"])
        xchild = attempt(
            "2a_create_child_with_parent_in_other_list",
            lambda: {
                k: v
                for k, v in clickup.clickup_create_task(
                    token, PRIMARY,
                    {"name": "CFPROBE-XLIST-CHILD", "parent": other_parent["id"]},
                ).items()
                if k in ("id", "parent", "top_level_parent")
            },
        )
        if xchild and xchild.get("id"):
            created.append(xchild["id"])
            attempt(
                "2b_which_list_did_it_land_in",
                lambda: {
                    "child_list": (
                        clickup.clickup_get_task(token, xchild["id"]).get("list") or {}
                    ).get("id"),
                    "parent_list": (
                        clickup.clickup_get_task(token, other_parent["id"]).get("list")
                        or {}
                    ).get("id"),
                },
            )

        # --- 3. delete the PARENT: does the child survive? ------------------
        doomed = clickup.clickup_create_task(token, PRIMARY, {"name": "CFPROBE-DOOMED-HUB"})
        orphan = clickup.clickup_create_task(
            token, PRIMARY, {"name": "CFPROBE-ORPHAN-CHILD", "parent": doomed["id"]}
        )
        created.append(orphan["id"])  # the hub is deleted by the test itself
        attempt(
            "3a_delete_parent",
            lambda: clickup._api_request(
                "DELETE", f"{clickup.CLICKUP_BASE}/task/{doomed['id']}", token
            ),
        )

        def child_after_parent_delete():
            try:
                t = clickup.clickup_get_task(token, orphan["id"])
                return {
                    "child_still_exists": True,
                    "parent": t.get("parent"),
                    "status": (t.get("status") or {}).get("status"),
                }
            except Exception as e:  # noqa: BLE001
                return {"child_still_exists": False, "error": str(e)}

        attempt("3b_child_after_parent_delete", child_after_parent_delete)
        attempt(
            "3c_child_visible_in_list_after_parent_delete",
            lambda: any(
                t["id"] == orphan["id"]
                for t in clickup.clickup_list_tasks(token, PRIMARY)
            ),
        )
    finally:
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
        print(f"wrote {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
