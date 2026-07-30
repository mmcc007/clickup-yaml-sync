# Parent/child (subtask) support — spike findings

**Issue:** #20. **Date:** 2026-07-29.
**Tested against:** live ClickUp sandbox (workspace "Orbsoft", `team_id 90141305256`,
Space `90146054238`, List `901417260037`), token `CLICKUP_API_TOKEN_SANDBOX`.
**Probes:** `tools/probe_parent.py`, `tools/probe_parent2.py` (both sandbox-gated,
create throwaway tasks and delete them). UI half via a direct Playwright script
(creds read from `pass` inside the process, never a tool argument).

Everything below is an observed response shape, not a doc claim.

## 1. `parent` on create — works

`POST /list/{id}/task` with `{"name": …, "parent": "<task_id>"}` returns a normal
task body with the edge already populated:

```json
{ "id": "86bb5nbqk", "name": "PROBE-CHILD-1",
  "parent": "86bb5nbqg", "top_level_parent": "86bb5nbqg" }
```

A top-level task has `"parent": null, "top_level_parent": null`.

## 2. Read-back — works, and `?subtasks=true` is load-bearing

`GET /list/{id}/task?subtasks=true&include_closed=true` (exactly what
`clickup_list_tasks()` sends, clickup.py:452) returns the child as a task in the
list with `parent` populated. So the edge is **readable back** — 3-way base
tracking is possible with no change to the fetch.

Dropping `subtasks=true` from the same query dropped the child from the response
(`task_count` 5 → 4, `child_present: false`). The tool always passes it, so this is
fine today, but it means the parent edge is invisible to any fetch that omits it.

`GET /task/{id}` agrees with the list shape. `GET /task/{parent}` does **not**
include a `subtasks` array by default; `?include_subtasks=true` adds
`"subtasks": [<child ids>]`. Not needed — children carry the edge themselves.

## 3. `parent` is MUTABLE on update — the invariant is safe

This was the main risk and it is not a problem:

| Operation | Result |
|---|---|
| `PUT /task/{flat}` `{"parent": P}` | ✅ becomes a subtask of P (200, edge in response and on read-back) |
| `PUT /task/{child}` `{"parent": P2}` | ✅ re-parented to P2 |
| `PUT /task/{child}` `{"parent": P}` again | ✅ moved back |
| `PUT /task/{child}` `{"parent": null}` | ⚠️ **HTTP 200, silent no-op** — still a child |
| `PUT /task/{child}` `{"parent": ""}` | ⚠️ **HTTP 200, silent no-op** — still a child |
| `PUT /task/{child}` `{"parent": <itself>}` | HTTP 400 |

**So: `parent` can be set and changed in place by `clickup_id`. No delete+recreate,
no collision with the in-place-update invariant.**

The one gap: **un-parenting is not reachable via the API.** Both `null` and `""`
return 200 and change nothing, so a `parent:` key removed from YAML cannot be
honoured — that has to be an explicit, loud error, not a silent divergence.
(Promoting a subtask back to top level is a UI-only operation.)

## 4. Nesting, list membership, tags, status

- **Depth ≥ 3 works.** A subtask of a subtask is accepted; the grandchild has
  `parent: <child>` and `top_level_parent: <root>`. The list endpoint returns all
  three, so `epics → stories → children` round-trips.
- **A subtask lives in its parent's list** (`list.id` identical), so it is in scope
  for the same list-scoped custom fields and the same sync run.
- **Tags work on a subtask**: `POST /task/{child}/tag/probe-epic` → 200, and the tag
  comes back on read. The epic *tag* half of the epic grouping is therefore fine.
- **Epic *dropdown* on a subtask: UNVERIFIED.** The sandbox list has no custom
  fields (`GET /list/{id}/field` → `[]`) and the API cannot create one. Inference
  only: custom fields are list-scoped and the subtask is in the same list, so the
  same field set should apply. Verify on a list that has the dropdown before relying
  on it.
- **No status rollup.** Setting a child to `complete` left the parent's status
  untouched. Rollup is a UI progress badge, not a field — consistent with rollup
  being a non-goal.

## 5. UI visibility — children are not lost, but they are hidden by default

Verified in the logged-in web app on the sandbox list (screenshots taken):

- The List view has a **`Subtasks` view control** with three modes:
  **Collapsed (default)**, **Expanded**, **Separate** ("use this to filter subtasks").
- Under the default **Collapsed**, the child rows are **not** rendered. The parent
  row gets a disclosure triangle and a subtask-count badge (`⑂ 2`) — the only
  on-screen evidence the children exist.
- Switching to **Expanded** renders both children as indented rows under the parent
  in the same status group. The mode is a per-view setting (a "Save view" button
  appears until saved).
- The status-group count stayed at the number of **top-level** tasks (4) in both
  modes — children do not inflate group counts.

So nothing vanishes permanently, but **a board left on the default view mode would
show a hub row with a badge instead of the child rows it shows today.** That is a
one-time per-view setting change, and it should be part of any rollout.

## 6. Not tested

- Cross-list parenting (child in list A, parent in list B). The tool syncs one list,
  so a parent is always same-list; probe skipped it.
- Cascade behaviour when a **parent is deleted** (children were deleted first). The
  tool never deletes tasks, but a UI delete of a hub could take children with it.
- The epic dropdown on a subtask (see §4).

## 7. What this costs in the reconciler

`parent` being mutable means it fits the **existing edge machinery** rather than
needing anything new on the create path. Shape, mirroring `depends_on`/`related`:

- `_cu_parent_id(cu_task)` → `cu_task.get("parent")`.
- `_sync_parent(token, task_id, cu_task, story, dry_run, warn_on_remove)` —
  unmanaged when the story has no `parent` key; PUTs only when desired ≠ current;
  refuses self-parent; **errors explicitly** when YAML wants no parent but ClickUp
  has one (§3).
- Hook both into `_reconcile_edges_pass` (clickup.py:1804), which already runs
  *after* the create pass (so a parent created this run has its id written back)
  and already has the `dry_run` / `PENDING_CREATE_ID` preview path — so #24's
  "reconciler wasn't dry-run-aware" failure mode is avoided by construction. No new
  edge class is needed for `sync --dry-run`.
- Pull side: `_parent_pull_target` / `_pull_parent` mirroring
  `_dependencies_pull_target`, and `_clickup_task_to_yaml_story` (clickup.py:2410)
  should emit `parent:`. Today a UI-created subtask is imported as a flat top-level
  story — pre-existing information loss that this fixes.
- Base tracking: `parent` *is* readable back, so it could join the 3-way
  `SYNCED_FIELDS`. Recommend it does **not** for v1 — match the edge convention
  (YAML-authoritative when the key is present), which is cheaper and consistent
  with `depends_on`/`related`.

**Open design decision — what does `parent:` reference?** If it must be a
`clickup_id` (like `depends_on`), a brand-new parent *and* child authored in the
same YAML cannot be linked on the first sync, because the parent's id does not exist
until that run creates it. Resolving `parent:` by **story name (falling back to id)**
at reconcile time removes that two-pass requirement, and the second pass runs after
creates so the id is always available by then.

Rough size: ~150–200 lines in `clickup.py` closely modelled on the existing
dependency/relation functions, plus schema docs and tests in the
`tests/fixtures/live_list_edges.json` replay style.
