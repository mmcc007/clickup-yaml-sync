#!/usr/bin/env python3
"""Recapture tests/fixtures/live_list_edges.json from the live ClickUp sandbox.

The edge tests in tests/test_sync.py replay a real GET /list/{id}/task body
rather than a hand-written mock, so that a shape change on ClickUp's side is
caught by the suite instead of silently invalidating every dependency and
relation test at once.

Run it only when the fixture needs refreshing (e.g. a suspected API change):

    python3 tools/capture_edge_fixture.py <sandbox_list_id>

It creates two throwaway tasks, wires one waiting-on dependency and one
non-blocking link between them, captures both the list and the single-task
responses, then deletes the tasks. Sandbox only -- it refuses to run without
CLICKUP_SANDBOX, so it can never touch a production board.
"""

import json
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

os.environ["CLICKUP_SANDBOX"] = "1"  # set before import: the token loader is lazy

import clickup  # noqa: E402

FIXTURE = HERE.parent / "tests" / "fixtures" / "live_list_edges.json"


def main() -> int:
    if len(sys.argv) != 2:
        print(__doc__)
        return 2
    list_id = sys.argv[1]
    token = clickup.get_clickup_token()

    a = clickup.clickup_create_task(token, list_id, {"name": "EDGE-A"})
    b = clickup.clickup_create_task(token, list_id, {"name": "EDGE-B"})
    aid, bid = a["id"], b["id"]
    try:
        clickup.clickup_add_dependency(token, aid, bid)  # A waits on B
        clickup.clickup_add_link(token, aid, bid)  # symmetric, non-blocking

        by_id = {t["id"]: t for t in clickup.clickup_list_tasks(token, list_id)}
        payload = {
            "_source": (
                "Captured live from the ClickUp sandbox by "
                "tools/capture_edge_fixture.py. Two throwaway tasks with one "
                "waiting-on dependency (A waits on B) and one non-blocking "
                "link. list_* is the body of GET /list/{id}/task"
                "?subtasks=true&include_closed=true"
                "&include_markdown_description=true -- exactly what "
                "clickup_list_tasks() returns. get_* is the single-task "
                "GET /task/{id}, kept so the suite can assert the two "
                "endpoints agree."
            ),
            "a_id": aid,
            "b_id": bid,
            "list_A": by_id[aid],
            "list_B": by_id[bid],
            "get_A": clickup.clickup_get_task(token, aid),
            "get_B": clickup.clickup_get_task(token, bid),
        }
        FIXTURE.write_text(json.dumps(payload, indent=2) + "\n")
        print(f"wrote {FIXTURE}")
    finally:
        # clickup.py has no delete helper by design -- the sync tool never
        # deletes tasks -- so issue the raw DELETE here rather than widening
        # the module's API for a test utility.
        for tid in (aid, bid):
            try:
                clickup._api_request(
                    "DELETE", f"{clickup.CLICKUP_BASE}/task/{tid}", token
                )
            except Exception as exc:  # cleanup must not mask a capture error
                print(f"WARNING: could not delete {tid}: {exc}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
