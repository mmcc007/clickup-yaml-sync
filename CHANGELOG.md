# Changelog

## Unreleased

### Fixed

- **Dependency/relation edges are now fully visible in `--dry-run`.**
  - A story the run would **create** no longer has its declared `depends_on` /
    `related` silently skipped in the preview. It has no `clickup_id` under
    `--dry-run` (creates don't happen), so the second pass used to skip it
    entirely and the edges first appeared in the real run — unpreviewed.
  - Edge log lines (preview and apply) now name the task they change —
    `'story name' (task id)` — instead of emitting bare id lists from a second
    pass that runs after the per-story output.
  - The inline scope comment above `DEP_TYPE_WAITING_ON` claimed dependencies
    were "NOT yet handled by `cmd_sync` / `cmd_merge`". They have been handled by
    both since sync/merge gained the second pass; the accurate half — that
    dependencies are excluded from the 3-way/base-snapshot machinery and are
    therefore *applied* rather than surfaced as conflicts — is kept. A test now
    pins `sync` (not just `push`) applying a declared `depends_on`, so the claim
    can't drift silently again.

### Added

- **Warning when the same target is declared as both `depends_on` and `related`.**
  ClickUp permits both edges on one pair and both are still applied as declared,
  but it is nearly always one intent written twice — and the two edges then have
  to be cleaned up separately.

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
