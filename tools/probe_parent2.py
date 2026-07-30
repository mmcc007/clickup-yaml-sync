#!/usr/bin/env python3
"""Second pass for issue #20 -- the gaps probe_parent.py left open.

  A. Unparenting: PUT parent=null silently no-ops. Try "" and a same-task PUT,
     and confirm nothing promotes a child back to top level.
  B. Nesting depth: can a subtask itself have a subtask (epics -> stories ->
     children)? Does the list endpoint return the grandchild, and what does
     top_level_parent hold at depth 3?
  C. Does a subtask live in the same list as its parent (i.e. does it inherit
     the list-scoped custom-field set)?
  D. Does GET /task/{parent} expose a subtasks array (include_subtasks=true)?
  E. Does setting a status on a subtask work like a normal task?

Sandbox only. Creates throwaway tasks and deletes them.

    python3 tools/probe_parent2.py <sandbox_list_id>
"""

import json
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

os.environ["CLICKUP_SANDBOX"] = "1"

import clickup  # noqa: E402

OUT = HERE.parent / "probe-parent-findings-2.json"


def shape(t: dict) -> dict:
    return {
        "id": t.get("id"),
        "name": t.get("name"),
        "parent": t.get("parent"),
        "top_level_parent": t.get("top_level_parent"),
        "list_id": (t.get("list") or {}).get("id"),
        "status": (t.get("status") or {}).get("status"),
        "subtasks": [s.get("id") for s in t.get("subtasks") or []],
        "_has_subtasks_key": "subtasks" in t,
    }


def main() -> int:
    if len(sys.argv) != 2:
        print(__doc__)
        return 2
    list_id = sys.argv[1]
    token = clickup.get_clickup_token()
    findings: dict = {"list_id": list_id, "steps": {}}
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
        except Exception as e:  # noqa: BLE001
            record(step, {"ERROR": type(e).__name__, "message": str(e)})
            return None

    def get(path: str) -> dict:
        return clickup._api_request("GET", f"{clickup.CLICKUP_BASE}{path}", token)

    try:
        p = clickup.clickup_create_task(token, list_id, {"name": "PROBE2-PARENT"})
        created.append(p["id"])
        c = clickup.clickup_create_task(
            token, list_id, {"name": "PROBE2-CHILD", "parent": p["id"]}
        )
        created.append(c["id"])
        record("0_baseline", {"parent": shape(p), "child": shape(c)})

        # --- A. unparent attempts -------------------------------------------
        attempt(
            "A1_put_parent_empty_string",
            lambda: shape(clickup.clickup_update_task(token, c["id"], {"parent": ""})),
        )
        attempt("A1_readback", lambda: shape(clickup.clickup_get_task(token, c["id"])))
        attempt(
            "A2_put_parent_null_again",
            lambda: shape(
                clickup.clickup_update_task(token, c["id"], {"parent": None})
            ),
        )
        attempt("A2_readback", lambda: shape(clickup.clickup_get_task(token, c["id"])))
        # A3: parent = itself (nonsense on purpose -- what error shape?)
        attempt(
            "A3_put_parent_self",
            lambda: shape(
                clickup.clickup_update_task(token, c["id"], {"parent": c["id"]})
            ),
        )
        attempt("A3_readback", lambda: shape(clickup.clickup_get_task(token, c["id"])))

        # --- B. nesting depth -----------------------------------------------
        g = attempt(
            "B1_create_grandchild_under_subtask",
            lambda: shape(
                clickup.clickup_create_task(
                    token, list_id, {"name": "PROBE2-GRANDCHILD", "parent": c["id"]}
                )
            ),
        )
        if g and g.get("id"):
            created.append(g["id"])
        attempt(
            "B2_list_with_subtasks_true_ids",
            lambda: {
                t["id"]: {
                    "name": t["name"],
                    "parent": t.get("parent"),
                    "top_level_parent": t.get("top_level_parent"),
                }
                for t in get(
                    f"/list/{list_id}/task?subtasks=true&include_closed=true"
                ).get("tasks", [])
                if t["name"].startswith("PROBE2-")
            },
        )

        # --- C/D. parent read shape -----------------------------------------
        attempt(
            "C_child_list_id_vs_parent",
            lambda: {
                "parent_list": shape(clickup.clickup_get_task(token, p["id"]))["list_id"],
                "child_list": shape(clickup.clickup_get_task(token, c["id"]))["list_id"],
            },
        )
        attempt(
            "D1_get_task_plain",
            lambda: shape(get(f"/task/{p['id']}")),
        )
        attempt(
            "D2_get_task_include_subtasks",
            lambda: shape(get(f"/task/{p['id']}?include_subtasks=true")),
        )

        # --- E. status on a subtask -----------------------------------------
        attempt(
            "E_set_status_on_subtask",
            lambda: shape(
                clickup.clickup_update_task(token, c["id"], {"status": "complete"})
            ),
        )
        attempt(
            "E_parent_status_after_child_complete",
            lambda: shape(clickup.clickup_get_task(token, p["id"])),
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
        print(f"\n=== cleanup ===\ndeleted={deleted} failed={failed}")
        OUT.write_text(json.dumps(findings, indent=2) + "\n")
        print(f"wrote {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
