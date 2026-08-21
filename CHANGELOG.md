# Changelog

## Unreleased

### Added

- **The advisory lock is enforced inside the tool, across the whole transaction.**
  `clickup.py` previously had no concept of the lock protecting the files it
  writes. The lock existed only as a Claude Code `PreToolUse` hook, which can
  only see Claude Code tool calls — so it never covered this tool's own
  writeback (`clickup_id` flush, `last_synced`, pulled rows, the
  `.clickup-sync/` base snapshot) or a human/cron at a shell. Every writing
  command now holds a **file lock and a ClickUp-list lock** for the whole run:
  acquire → edit → sync → release.
  - Same lock path, JSON shape and TTL as the hook, and under Claude Code the
    same identity (`CLAUDE_CODE_SESSION_ID`), so a session's edit and its sync
    are one continuous hold rather than two mechanisms taking turns. A lock
    already held by our own session is adopted, and handed back on release.
  - A held lock waits **visibly** and then fails **loudly** — exit code `3`,
    never a silent no-op.
  - Crash-safe via the TTL; a heartbeat keeps a long run's locks fresh.
  - `--lock-timeout`, `--no-lock` / `CLICKUP_NO_LOCK`, `CLICKUP_LOCK_OWNER`.
  - See the README's **Locking** section. Add `.project-tasks.lock` to the
    consuming repo's `.gitignore`.

- **`with-lock <file> -- <command>`** — run any command (an editor, a script, a
  shell, a Claude session) inside the project's lock. `clickup.py` is now the
  only supported writer of a task file, and editing without syncing is a normal
  workflow, so the tool wraps whatever you would have used rather than
  reimplementing text editing. The child inherits `CLICKUP_LOCK_OWNER` so a
  nested `sync` joins the hold instead of deadlocking against its own parent.

- **CI.** The repo had a test suite and no workflow, so nothing could be run
  against a change. `.github/workflows/ci.yml` runs the suite on PRs and on
  pushes to `main`.

### Fixed

- **`save_yaml` is now atomic** (temp file + `os.replace`). `push`/`sync` flush
  the task file once per created task; a crash partway through the previous
  truncate-and-write left the project's task file truncated.


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
