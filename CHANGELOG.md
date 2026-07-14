# Changelog

## Unreleased

### Fixed

- **#14 (critical) — interrupted + retried sync no longer creates duplicate tasks.**
  - `sync` now flushes each new `clickup_id` back to the YAML file immediately
    after each create (matching `push`), so a run killed mid-create is resumable.
  - Both `sync` and `push` now dedupe before creating: a YAML story is matched
    against existing ClickUp tasks by `(name, epic-tag)` and **adopts** a match
    instead of creating a second copy — covering orphan tasks left by a prior
    killed run and pre-existing duplicates.
  - A task created (or adopted) during a run is no longer re-detected by the
    same run's reconcile pass as "new from ClickUp".
  - The sync summary now reports `Adopted existing: N` and flags
    `⚠️ DUPLICATES: N` when any stories share a `clickup_id`, instead of
    printing a clean `Created: N`.

- **#13 (perf) — full sync no longer re-PATCHes the Epic dropdown on every task.**
  - The current dropdown value is resolved from ClickUp's read shape (the
    selected option's *orderindex* integer, mapped back to the option UUID via
    `type_config.options`) before comparing to the target, so an already-correct
    dropdown is skipped. Previously the read value (orderindex) never equalled
    the target UUID, so every task got an unconditional dropdown write — the
    main reason a full sync ran for minutes and timed out.
