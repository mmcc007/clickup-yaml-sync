# Changelog

## Unreleased

### Fixed

- **A failed dependency or linked-task edge is no longer easy to miss.** Two
  separate problems, both of which made a board silently end up without the
  structure its YAML declares:
  - **The API's reason never reached the caller.** `urllib`'s `HTTPError`
    stringifies to `HTTP Error 403: Forbidden`, and the response body was
    already consumed reading it for the log — so every
    `except Exception as e: log.warning(f"...: {e}")` printed the status and
    threw away ClickUp's own `err` message and `ECODE`. The real reason existed
    only on a separate ERROR line, correlated with the failure by adjacency.
    A new `ClickUpAPIError` carries status, body, `err` and `ECODE`, so any
    caller's message is now informative.
  - **Failures were per-edge warnings and nothing else.** On a 13-card board
    that is 13 warnings scrolling past while the run reports success. Edge
    failures are now collected and reported in an end-of-run block naming every
    edge that did not get created, with a once-per-run checklist of plausible
    causes (Dependencies ClickApp disabled on the Space, a guest-shared Space
    refusing access, a target in another Space or deleted). The checklist says
    explicitly that the API's own message is authoritative and that it is a
    checklist, not a diagnosis — presenting a guess as a diagnosis sends people
    down the wrong path with confidence.

  A failed edge still does not abort the run: the tasks themselves are fine, and
  the point is that the failure is *reported*, not that it becomes fatal.

### Added

- **Story-level `notes:` — YAML-only context that never reaches ClickUp.** The
  story-scale equivalent of `project.notes`. Never pushed, never overwritten by
  `pull`, invisible to every diff and conflict path (it has no remote
  counterpart, so it cannot conflict).

  A description that accumulates context helps whoever *writes* the card and
  harms whoever *reads* it — and on a client-visible board an over-long
  description spends the client's attention on our reasoning instead of their
  action. The material being cut is usually provenance (who said it on which
  call, which SOW clause, what was rejected), and its only previous homes were
  the card itself, where it hurts the reader, or a separate document, where it
  drifts away from what it describes.

  **`description` = what a reader needs to act. `notes` = why it is like that.**
  Acceptance criteria belong in the description; putting them in notes hides
  them from everyone working the board.

  **Authored, not derived** — `pull` writes nothing into it, by decision. A sync
  that appended there would make the field untrustworthy.

  No new machinery was needed: unknown story keys already survived every path,
  because push and pull work from explicit field lists and mutate story dicts in
  place. But that behaviour was *emergent*, and emergent behaviour breaks
  silently — so `YAML_ONLY_STORY_FIELDS` names the contract and a test class
  pins each property. The day someone rebuilds a story dict, CI fails instead of
  quietly deleting the material people kept precisely because it was too
  valuable to delete.

### Changed

- **`milestone_label` accepts a slug: `M<n>-<slug>`, with `n` unbounded.**
  `M1-infrastructure` pushes as the tag `m1-infrastructure`. The number carries
  the sequence, the slug carries the meaning — a bare `m1` is a handle that
  tells a reader nothing, which is why every project was bypassing the field and
  hand-rolling its own convention into `tags:`. Hand-rolled conventions diverge;
  the field makes the agreed shape the default.
  - **Bare `M0`–`M3` remain valid and unchanged**, so boards using them are
    undisturbed.
  - The old `M0`–`M3` enum capped a project at four milestones for no reason a
    five-milestone SOW would accept. The constraint is now a pattern.
  - Push and the milestone-date lint share **one** definition of a milestone
    slug (`MILESTONE_TAG_RE`). Two regexes for one convention would drift, and
    the drift would surface as a lint that quietly stops resolving what push
    emits.
  - New lint finding `milestone-label-malformed` for a value that is neither
    form — it pushes a tag nothing can resolve, so the card looks tagged and is
    silently unchecked. Flagged, not rejected.

### Added

- **`lint` — a milestone-date coherence check.** A card tagged `m<n>-<slug>`
  should be due on or before the due date of the milestone card carrying that
  tag; work needed for a gate cannot be due after it. ClickUp enforces nothing
  here (a milestone is a task *type* rendered as a diamond — it contains and
  groups nothing), so the tag is the only association that exists and a date
  check over it is the only way to catch an incoherent plan.
  - **Flags, never modifies.** A date is a human's decision.
  - **Never blocks.** Runs as an advisory tail on every command and does not
    affect the exit code. `lint --strict` is the opt-in for a non-zero exit.
  - **Missing data is silent** — an undated card is not a violation.
  - Also reports the resolution failures, which are often worth more than the
    date check: a tag pointing at no gate, two gates sharing a number, a
    same-number/different-slug typo (still date-checked, so a typo cannot
    silently disable the check), and an undated gate (once per gate).
  - **`lint_exceptions:`** accepts a finding per card by code, with a required
    **written reason** — a date set in the ClickUp UI arrives via `pull` as
    legitimate data. A bare `true` is rejected; accepted findings are still
    counted, so suppressed is not the same as gone.

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

- **Importing `clickup.py` no longer hard-crashes when `~/tmp` does not exist.**
  `setup_logging()` opened the debug log at import time with no directory
  creation and no fallback, so on a fresh checkout, in a container or on CI the
  whole tool failed to load with a `FileNotFoundError`. The directory is now
  created, and file logging degrades to console-only if it still cannot be
  written. (Found the moment this repo got CI.)

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
