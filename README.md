# clickup-yaml-sync

Bidirectional sync between local YAML project files and ClickUp. Manage your project as structured YAML, sync to ClickUp, pull status updates back, and resolve conflicts — including LLM-assisted merging.

## Concept

Epics live **only in YAML** as organizational groupings. Stories become flat top-level tasks in ClickUp, tagged with their epic name. This keeps ClickUp clean while preserving full project structure locally.

```
YAML                          ClickUp
──────────────────────        ────────────────────────
epics:                        Tasks (flat list):
  - name: Core Pipeline   →     "Implement RAG retrieval" [tag: Core Pipeline]
    stories:              →     "Add confidence scoring"  [tag: Core Pipeline]
  - name: API Gateway     →     "POST /api/generate"      [tag: API Gateway]
    stories:              →     "Feedback endpoints"      [tag: API Gateway]
```

## Commands

| Command | Description |
|---------|-------------|
| `push` | Push YAML → ClickUp (create new, update changed) |
| `pull` | Pull ClickUp → YAML (update statuses, detect new tasks) |
| `diff` | Show differences without making any changes |
| `sync` | Full bidirectional sync with conflict resolution |
| `merge` | Like sync but uses GPT-4o-mini to propose merged values |
| `status` | Offline summary table (no API calls) |
| `lint` | Report milestone-date incoherence — advisory, flags but never modifies |
| `with-lock` | Run any command (an editor, a script, a shell) while holding the project's lock — the supported way to hand-edit a task file |

### Which command should I use? → `sync` (default)

**`sync` is the safe default and the recommended command for routine work.** It is the only command with **conflict detection**: it reads a base snapshot, auto-resolves changes that happened on only one side, and **stops on a true conflict** (both sides changed the same field) under the default `--on-conflict stop` — making zero changes until you resolve it. Inspect first with `sync --dry-run`.

`push` and `pull` are **blunt, one-directional overwrites with no conflict detection** — keep them for deliberate "force one side to win" cases (or first-time bootstrap), not routine use:

| | Direction | Conflict detection | Safety net | Equivalent via sync |
|---|---|:--:|---|---|
| **`sync`** | both | **yes** (3-way, stops on conflict) | base snapshot | — |
| **`push`** | YAML → ClickUp | none — clobbers UI edits | auto ClickUp-state backup | `sync --on-conflict local` |
| **`pull`** | ClickUp → YAML | none — clobbers local YAML edits | auto YAML-file backup | `sync --on-conflict remote` |

Both `push` and `pull` print a one-time warning banner describing what they'll overwrite, and both auto-create a backup first (disable with `--no-backup`). Prefer the `sync` equivalents — they do the same thing but auto-resolve the non-conflicting changes and stop before clobbering a genuine collision.

> **⚠️ Runtime — a full sync can still run for minutes; prefer running it detached.** On a board of dozens of tasks a single `sync`/`push` can take a while. Two fixes (2026-06-30) make an interrupted run **safe to retry** without creating duplicates:
>
> - **Per-create writeback (BUG #14).** Each new task's `clickup_id` is now flushed to the YAML file **immediately** after it's created — so a run killed mid-create leaves a resumable YAML, never an orphan that a retry re-creates.
> - **Dedupe-before-create (BUG #14).** Before creating, the tool matches each YAML story against existing ClickUp tasks by `(name, epic-tag)`. If a match exists (e.g. an orphan from a prior killed run), it **adopts** that task's id instead of creating a duplicate, and never re-imports it as a "new from ClickUp" YAML row. The summary now reports `Adopted existing: N` and flags any `⚠️ DUPLICATES` (stories sharing a `clickup_id`).
> - **No-op Epic-dropdown skip (BUG #13).** The Epic dropdown is only PATCHed when the value actually changes — the current value is resolved from ClickUp's read shape (`value` = option *orderindex*, mapped back to the option id) so an already-correct dropdown is no longer re-written on every task. This is what made full syncs slow enough to hit the timeout in the first place.
>
> Still good practice: **run it detached / in the background** (an agent's `run_in_background`, or `nohup … &` / a tmux pane) for very large boards, and always preview with `sync --dry-run` first (read-only).
>
> - **If a run *was* interrupted (older versions, or to double-check):** check `git status` on the YAML and re-run — the dedupe pass adopts any orphan tasks. If you see `⚠️ DUPLICATES` in the summary, inspect the YAML and ClickUp for duplicate tasks.

## Milestone-date lint

**A card tagged to a milestone should be due on or before that milestone's own due
date.** If work has to be finished before a gate, a date after the gate is a
contradiction. `clickup.py lint <file>` reports those, and so does every other
command, as an advisory tail.

```bash
./clickup.py lint docs/project-tasks.yaml
```

### It is a lint, not a constraint — and that is deliberate

**ClickUp enforces nothing here.** A "milestone" is a task *type* on an ordinary
task, rendered as a diamond at a point in time. It contains nothing and groups
nothing; the only structural relationships ClickUp has are dependencies and subtasks.
So the `m<n>-<slug>` tag is the **only** association that exists between a card and
its gate, and a date check over that tag is the only way to catch an incoherent plan.

Three rules follow from that, and each is load-bearing:

1. **It flags; it never modifies.** A date is a human's decision.
2. **Missing data is silent.** Most cards have no due date. An undated card is not a
   violation and is never reported as one.
3. **It never blocks.** Every command's tail is advisory and the exit code is
   unaffected. A lint that stops a sync over a guideline gets bypassed, and a
   bypassed lint is worse than no lint. `lint --strict` is the opt-in for anyone who
   *does* want a non-zero exit (CI, a pre-merge gate).

It does **not** look at epics. Epics are a YAML-only grouping and a diamond is
deliberately not filed under a work epic, so no milestone relationship is inferred
from one.

### `milestone_label` — let the tool own the convention

You can hand-write the tag into `tags:`, but `milestone_label` is the field for it:

```yaml
- name: M1 - Infrastructure ready
  milestone: true
  milestone_label: M1-infrastructure     # pushes as the tag `m1-infrastructure`
  due_date: "2026-09-15"
```

**The number carries the sequence; the slug carries the meaning.** A bare `m1` is a
handle that tells a reader nothing, which is why the slug form exists. `n` is
unbounded — the field used to be an `M0`–`M3` enum, which capped a project at four
milestones for no reason a five-milestone SOW would accept.

Bare `M0`–`M3` remain valid and behave exactly as before, so existing boards are
undisturbed. A malformed value (`Milestone 1`) is reported by the lint as
`milestone-label-malformed` rather than rejected — it would push a tag nothing can
resolve, so the card would *look* tagged and be silently unchecked.

Use the field rather than hand-rolling the tag. While it emitted a bare `m1`, every
project bypassed it and invented its own convention — and hand-rolled conventions
diverge (`m1-infrastructure` here, `milestone-1` on the next board, no number on a
third). The field makes the agreed shape the default instead of a habit each project
has to remember.

> **One nuance on removal.** Bare `m0`–`m3` are permanently in the managed-tag
> universe, so deleting `milestone_label: M1` strips the `m1` tag on the next push
> even with no base snapshot. A *slugged* label relies on the base snapshot's
> recorded `managed_tags` for that, the same way explicit `tags:` entries always
> have. In practice the snapshot is written by every push/pull/sync, so this only
> bites on a first run against a board with no `.clickup-sync/` yet.

<details>
<summary><strong>Design note — why the enum's removal is a small regression, and the shape of the fix</strong> (investigated 2026-08-21, not built)</summary>

The `M0`–`M3` enum's real purpose was never readability — it was a **closed
vocabulary**, and a closed vocabulary is what buys reliable stale-tag removal. The
tool could say "these four slugs are always mine to strip" precisely because it knew
all of them in advance. Widening the field to `M<n>-<slug>` gains the readability and
loses that property, so this is a genuine (small) regression rather than a rough edge.

**The wrong way to get it back** is to declare the whole `m<n>[-slug]` namespace
permanently managed. That claims every `m`-prefixed tag on a live board — including
ones a person added in the ClickUp UI for their own reasons — and silently strips
them. The blast radius is worse than the bug.

**The better shape: derive the vocabulary from the milestone cards themselves.** The
gates are already identifiable — stories with `milestone: true` carrying an
`m<n>[-slug]` tag. That set *is* a closed vocabulary; it is computed from the file
rather than hardcoded; it grows and shrinks as milestones are added or removed; and
it never claims a tag no gate references. It recovers the enum's correctness property
without the enum's four-milestone cap.

Two things were checked before recommending it:

1. **Is the universe computed early enough to see the milestone cards?** Yes.
   `_collect_managed_tag_universe(data)` runs at the top of both `cmd_push` and
   `cmd_sync`, before any per-story reconcile, and walks every epic and story. Gates
   are ordinary stories, so there is no ordering problem.
2. **What happens on the run that deletes a gate?** Its slug leaves the derived
   vocabulary in the same run that removes it, so on its own the derived set cannot
   strip the now-orphaned tag. In `sync` this is already covered: `managed_universe`
   is unioned with the *previous* run's `managed_tags` from the base snapshot, which
   still names the deleted slug. So the derived set is **strictly additive over the
   base snapshot and never worse** — including the case where a project uses
   `milestone_label` but has no gate card at all, where it is simply empty.

**But that check surfaced a separate, pre-existing defect worth fixing first.**
`cmd_sync` unions the base snapshot's `managed_tags`; **`cmd_push` does not.** So a
tag removed from `tags:` (or a `milestone_label` removed) is stripped by `sync` and
**silently left behind by `push`** — nothing to do with milestones, and true today for
every board. Fixing that is one line and worth more than the milestone-vocabulary
work; it also changes tag-removal behaviour on live boards, so it wants a decision
rather than a quiet patch.

</details>

### How a card is tied to a gate

The gate is a story with `milestone: true` carrying an `m<n>` tag; a card points at it
with the same tag. The **number** carries the sequence, the **slug** carries the
meaning:

```yaml
epics:
  - name: Milestones
    stories:
      - name: M1 - Infrastructure ready
        milestone: true
        due_date: "2026-09-15"
        tags: [m1-infrastructure]

  - name: Delivery
    stories:
      - name: Stand up the ingest pipeline
        due_date: "2026-10-01"        # after the gate -> CONTRADICTION
        tags: [m1-infrastructure]
```

Both `tags:` and the `milestone_label` enum (`M0`–`M3`, which push lowercases into the
same tag namespace) are read.

### What it reports

| code | severity | meaning |
|---|---|---|
| `milestone-date` | contradiction | the card is due after its gate |
| `milestone-tag-unresolved` | warning | no gate carries that number — a typo, or the gate does not exist yet |
| `milestone-slug-mismatch` | warning | same number, different slug — likely a typo. Checked against that gate anyway, so a typo cannot silently disable the date check |
| `milestone-ambiguous` | warning | two gates share a number, so nothing can resolve to one of them |
| `milestone-gate-undated` | note | the gate has no due date, so no card tagged to it can be checked. Reported once per gate, not once per card |
| `milestone-label-malformed` | warning | `milestone_label` is not `M<n>` or `M<n>-<slug>`, so it pushes a tag nothing can resolve |

The resolution findings are often worth more than the date check itself: a typo'd tag
otherwise filters to nothing, and the plan looks clean **because nothing is being
checked**.

### Accepting a finding

A date set in the ClickUp UI arrives here via `pull` and is legitimate data, not
necessarily a mistake. A card can accept a finding by its code, **with a written
reason**:

```yaml
- name: Late but agreed
  due_date: "2026-11-01"
  tags: [m1-infrastructure]
  lint_exceptions:
    milestone-date: "Client moved the acceptance window; agreed with Eric 2026-08-20"
```

The value must be a **non-empty string**. A bare `true` is deliberately rejected — a
suppression flag with no rationale tells the next reader nothing, and by the time
anyone asks, whoever set it has moved on. Accepted findings are still **counted** in
the report, so suppressed is not the same as gone.

## Story `notes:` — context that stays out of ClickUp

A per-story field that lives in the YAML and **never reaches ClickUp**. Nothing
pushes it, `pull` never overwrites it, and it is invisible to every diff and conflict
path — it has no remote counterpart, so it cannot conflict. The story-scale
equivalent of `project.notes`.

```yaml
- name: Stand up the ingest pipeline
  description: |
    Ingest the nightly carrier feed into the staging table and alert on a
    failed run. Done when a failed run pages and a clean run does not.
  notes: |
    Threshold split routine vs adversarial per Eric on the 2026-08-14 kick-off.
    SOW clause 4b makes the acceptance window ten business days, which is why
    the gate is 2026-09-15. Considered a single combined check and rejected it —
    the two have different escalation paths.
```

### Which goes where — the line that keeps the board useful

| | `description` | `notes` |
|---|---|---|
| **Answers** | what a reader needs in order to **act** | **why** the card is the way it is |
| **Read by** | everyone working the board, including the client | whoever maintains the file |
| **Holds** | scope, acceptance criteria, definition of done | provenance: who said it, which clause, what was rejected |

**Do not let `notes` become a second description.** Acceptance criteria and
definition-of-done are description material; moving them into `notes` hides them from
everyone actually working the board, which makes it worse rather than better.

### Why the field exists

A description that accumulates context helps whoever **writes** the card and actively
harms whoever **reads** it. A card nobody reads has stopped being a coordination
artefact and become a filing cabinet — and on a client-visible board, an over-long
description spends the client's attention on our reasoning instead of their action.

The cut context is usually not worthless; it is provenance. Without somewhere adjacent
to keep it, the only options are on the card (where it hurts the reader) or in a
separate document (where it drifts away from the card it describes). `notes:` is the
third option.

### It is authored, not derived

**`pull` writes nothing into it, by decision.** A sync that appended to `notes` would
make the field untrustworthy — you could no longer tell what a human meant from what a
tool deposited. If a future change ever does need to record something there, it should
be a clearly delimited block, never mixed into hand-written prose.

> **Implementation note.** Unknown story keys already survive every code path, because
> push and pull work from explicit field lists and mutate story dicts in place rather
> than rebuilding them. `notes:` therefore needed no new machinery — but the behaviour
> was *emergent*, and emergent behaviour breaks silently. `YAML_ONLY_STORY_FIELDS`
> names the contract and a test class pins each property (never pushed, never pulled,
> never compared, never snapshotted, never in an API body, survives save/reload). The
> day someone rebuilds a story dict, CI fails instead of quietly deleting people's
> provenance.

## Working on this code: test the joins, not just the halves

**Both halves green, the join untested, the bug invisible.** That is the shape of
almost every defect this tool has shipped, and it is worth knowing before you add
anything.

The clearest example: `push` never removed a tag dropped from the YAML, for the whole
life of the feature, on every board. `_collect_managed_tag_universe` had tests.
`load_base_managed_tags` had tests. **What had no test was the wiring between them** —
and that is exactly where the two commands diverged, with the fix landing in `sync`
and not in `push`. Every unit was green the entire time.

The same shape recurs: a lock whose acquire and release were both correct while
nothing checked that the *tool* took it; a diagnostic that tested what it found rather
than whether it fired; a field that survived every code path by accident, with nothing
saying it was supposed to.

So when you change something here:

- **Ask what connects the pieces, and test that.** A test that two call sites produce
  the same answer is weak — it passes again the moment someone adds a third, subtly
  different one. Prefer one function and a test asserting the callers *use* it. That
  is why `managed_tag_universe_for()` exists and why a test checks neither command
  computes the universe itself.
- **Pin behaviour that currently works by accident.** `notes:` survived every path
  before anything declared it should; emergent behaviour breaks silently, and silence
  is the whole problem. Name the contract (`YAML_ONLY_STORY_FIELDS`) and test each
  property.
- **Share the definition, not the value.** Push and the milestone lint use one
  `MILESTONE_TAG_RE`. Two regexes for one convention drift, and the symptom would be a
  lint quietly failing to resolve what push emits — which reads as "nothing wrong".
- **Make failure loud.** A lock that silently skips, a sync that quietly does nothing,
  an edge that fails into scrollback: all of them look like success. Non-zero exits and
  end-of-run summaries exist for that reason.

## Running it: pin a copy (`clickup.py pin`)

```bash
./clickup.py pin          # writes ~/bin/clickup-<commit>.py and tells you how to use it
~/bin/clickup-<commit>.py sync docs/project-tasks.yaml
```

**Run boards through a pinned copy, not through a development checkout.** A pinned
copy cannot change under you when somebody merges, and it reports its own content
hash. `clickup.py` is a single file with no runtime dependency on anything else in
the repo (it does not read `schema.yaml`), so a copy at a known commit is a complete,
correct tool.

### Every run says what produced it

```bash
./clickup.py --version
# clickup.py /home/…/clickup.py | commit b451d96 (clean) | sha256 4c4a79014723
```

The same line is logged at the start of **every** run, before anything is touched.

**The hazard being closed is unattributable runs — not torn reads.** Python loads
this file fully at interpreter start and git replaces files by rename, so a running
process cannot have its code swapped. What actually goes wrong is subtler: on
2026-08-21 an operator prepared a client-board sync against one commit, the working
tree moved to another commit while they prepared, and the only reason anyone noticed
was an unrelated message. There was no record either way. Every corpus board is
invoked from a live development checkout, so that is the normal case, not an accident.

If the checkout moves *during* a writing run, the run says so on the way out and
names the commit that actually ran.

### A writing command will not run from a modified `clickup.py`

```
ERROR: Refusing to run 'sync': clickup.py has uncommitted changes, so these bytes
have never been tested and this is a writing command.
```

**Two different goals, and only one of them justifies a refusal.** *Attribution* is
solved completely by the content hash — even uncommitted code is fully identified.
What refusal buys is separate: **it stops untested code writing to a client's board.**

Three ways forward, and the message lists them easiest-first:

1. **`clickup.py pin`** — one argument-free command. This is the recommended path and
   it is deliberately easier than the bypass; a guard with a convenient bypass becomes
   decoration, because everyone types the flag and it then fires on nothing.
2. Commit the change and run again.
3. **`--allow-dirty`**, for deliberately testing an uncommitted change.

**The bypass marks a run; it never blinds one.** `--allow-dirty` still logs the exact
content hash and stamps the run — on stderr and in the log — as a bypass of untested
code. That is what makes marking it sufficient rather than a hole.

Only `push`, `pull`, `sync` and `merge` are refused. `status`, `diff`, `lint`,
`with-lock` and `pin` never are — reading the world can always be accounted for by
reading it again.

> **This is a behaviour change for anyone running from a working tree they edit.**
> If your checkout has local modifications to `clickup.py`, your next writing command
> stops until you pin, commit, or pass the flag. That is intended: the thing on the
> other side is an unattributable, untested write to a client's board.

The lock below protects the *task file* from concurrent writers; this protects the
question *which code did that*.

## Locking — the tool takes the flag for you

**`clickup.py` is the only supported writer of a task file.** Every writing command
takes an advisory lock before it reads the YAML, holds it for the whole run, and
releases it at the end. **You do not grab a flag by hand, and you must not** — a
hand-written lock carries an identity nothing else can match, which locks out its own
author.

If you want to *edit* rather than sync, that also goes through the tool. Editing
without syncing is a normal workflow — you stage cards, look at them, publish later —
so the tool wraps whatever you would have used anyway rather than reimplementing
text editing:

```bash
# Hand-edit inside the lock
./clickup.py with-lock docs/project-tasks.yaml -- $EDITOR docs/project-tasks.yaml

# A whole session of edits, then a sync, as one continuous hold
./clickup.py with-lock docs/project-tasks.yaml -- bash
```

### Why the lock is in the tool and not only in the hook

On this machine a Claude Code `PreToolUse` hook has long guarded hand-edits to
`project-tasks.yaml`. A hook can only see Claude Code tool calls, so it is
structurally blind to the writers that matter most here:

| writer of the task file | hook | tool |
|---|:--:|:--:|
| Claude via Edit / Write / MultiEdit | ✅ | — |
| Claude via Bash (`sed`, a heredoc) | ❌ | ✅ via `with-lock` |
| **`sync` / `push` / `pull` / `merge` writing back to the YAML** | ❌ | ✅ |
| a human or a cron at a shell | ❌ | ✅ |

Row three is the easy one to miss. **`sync` is a writer, not just a reader**: it
flushes each new task's `clickup_id` back to the YAML the moment the task is created,
stamps `project.last_synced`, and `pull` rewrites story rows wholesale. It also
mutates a second piece of shared state, the 3-way base snapshot under
`.clickup-sync/`. Two runs against one list can interleave a read-modify-write on
both.

So the critical section is **the whole transaction — acquire → edit → sync →
release** — not the individual write.

### What it locks, and where

| lock | path | protects |
|---|---|---|
| file | `<dir>/.<stem>.lock` — e.g. `docs/.project-tasks.lock` | the YAML and its `.clickup-sync/` base snapshot |
| list | `${XDG_CACHE_HOME:-~/.cache}/clickup-yaml-sync/list-<id>.lock` | the ClickUp list itself |

Both are taken, **file first and then list**, a fixed order that cannot deadlock. They
are not the same lock and one does not imply the other: **two different YAML files
pointed at one ClickUp list is a real configuration**, and file-level locking does
nothing for it.

**Add `.project-tasks.lock` (or `.*.lock` beside your task file) to the consuming
repo's `.gitignore`.** It is per-machine runtime state; committing it has already
caused confusion elsewhere.

### Interop with the Claude Code hook

The file lock is deliberately the *same* lock the hook uses — same path, same JSON
(`{session_id, ts, pid}`), same 5-minute TTL — and under Claude Code the **same
identity**. `CLAUDE_CODE_SESSION_ID` is exported into Bash tool calls and holds
exactly the session UUID the hook writes (verified against a live hook-written lock,
2026-08-21; note `CLAUDE_SESSION_ID`, without `CODE`, is a different and empty
variable).

That is what makes a session's edit and its sync **one continuous hold of one lock**
rather than two mechanisms taking turns on a file the other is writing. Concretely:

- A fresh lock already held by **our own session** is *adopted*, not waited on — so a
  session that just edited does not deadlock against itself for a full TTL.
- On release, an adopted lock is **handed back** rather than deleted, because that
  session may still be mid-edit-burst. A lock this tool created is deleted.
- The hook still earns its place: it catches a hand-edit made without the tool, and
  tells that session to wait.

Outside Claude Code (a plain shell, a cron), the tool mints a unique per-process
identity. `with-lock` exports `CLICKUP_LOCK_OWNER` to its child, so a nested
`clickup.py sync` **joins** the hold instead of blocking on its own parent.

### When someone else holds it

The run waits, visibly, then **fails loudly** — it never quietly skips the work:

```
Waiting for YAML file lock held by 852d7779 (held 12s) -- up to 120s...
ERROR: YAML file is locked by 852d7779 (held 132s, expires in 168s).
  lock file: /path/to/docs/.project-tasks.lock
  Waited 120s and gave up. Nothing was changed -- rerun when that holder is done.
  Raise the wait with --lock-timeout SECONDS. ...
```

Exit code **3**, distinct from `1`, so a caller can tell "busy, try again" from
"failed".

| flag / env | effect |
|---|---|
| `--lock-timeout SECONDS` | how long to wait before failing (default 120) |
| `--no-lock`, `CLICKUP_NO_LOCK=1` | skip locking entirely — the escape hatch. You are then responsible for knowing nothing else is writing |
| `CLICKUP_LOCK_OWNER=<id>` | pin the identity (tests; a cron that wants a stable name) |

**Which commands lock:** `push`, `pull`, `sync`, `merge`, `with-lock`. Not `status`
(offline) or `diff` (reads both sides, writes neither). **`--dry-run` does not lock**
either — it writes nothing, so making it queue behind a real run costs more than it
buys; the plan it prints is a preview, and the real run that follows locks properly.

### Crash safety

A process killed mid-sync leaves a lock that **expires** after the 5-minute TTL,
never one held forever. A long run refreshes its locks on a background heartbeat, so
a sync slower than the TTL does not go stale under its own feet. A corrupt or
unreadable lock file reads as *free* (the same choice the hook makes) — an advisory
lock that failed closed would wedge the tool with no way out but a manual delete.

One residual race, stated rather than hidden: this tool writes lock files
**atomically**, but the hook writes in place, so a reader can in principle catch a
hook write half-finished and read the file as free. The window is microseconds and
the mechanism is advisory by design; the durable arbiter is still git.

## Multi-Tag, Milestones, and Custom Dropdowns

The tool maintains an additive multi-tag set per story. Sources merge in
this order (deduped case-insensitively):

1. The epic's `name` (always present — preserves the legacy single-tag behavior).
2. Explicit `tags: [...]` on the story.
3. The lowercased `milestone_label` slug (e.g., `M1` → `m1`).

Pre-existing tags on the ClickUp task that are **not** in the YAML's
"managed tag universe" (every epic name + every milestone slug + every
explicit tag in YAML) are **preserved** on push — that means tags added
manually in the ClickUp UI survive.

Tags that **are** in the managed universe but not on the story being
pushed are treated as stale and removed — that's how an epic or milestone
reassignment in YAML actually takes effect.

## Assignees (bidirectional)

Each story can carry an `assignees` list of human-readable strings — emails
(preferred) or ClickUp usernames:

```yaml
stories:
  - name: Example task
    clickup_id: 860000001
    points: 2
    status: backlog
    assignees:
      - alice@example.com
```

Resolution is automatic: `push` looks up the list's member roster and converts
each string to the numeric ClickUp user id the API needs; `pull` writes the
remote assignees (as emails) back into the YAML. Names not on the roster are
**warned and skipped**, never fatal.

Semantics mirror tags' coexistence with the ClickUp UI:

| YAML on the story | Push behaviour |
|---|---|
| no `assignees` key | **Unmanaged** — ClickUp assignees left untouched (UI assignments preserved) |
| `assignees: []` | **Clear** — all assignees removed |
| `assignees: [a, b]` | **Authoritative** — ClickUp reconciled to exactly this set |

`pull` always reads remote assignees back — so if someone reassigns a task in
the ClickUp UI, the next `pull`/`sync` reflects it in YAML. Under `sync`, the
`--conflict` strategy applies at the whole-set level (`local` = YAML wins,
`remote` = ClickUp wins, `ask` = one prompt per task when they diverge).

## Dependencies (waiting_on edges)

Each story can declare a `depends_on` list — the tasks it **waits on** (a
"waiting on" dependency). Targets may be referenced **by story name** or by raw
`clickup_id`:

```yaml
stories:
  - name: Ingestion adapter
    depends_on:
      - Design doc sign-off      # a story in this file, by name — preferred
      - 860000003                # a raw id, for a target outside this YAML
```

**Prefer names.** An id does not exist until the run that creates it, so on a
brand-new board an id reference cannot be written at all — which used to force
two passes: one to create the tasks, a second to add the edges once the ids
existed. A name reference links two brand-new tasks on the **first** sync,
because the reconcile pass runs after creates. This is the same reasoning
`parent` has always used (see [Parent / child](#parent--child-subtasks)); it now
applies to all three reference fields.

Resolution is identical to `parent`'s: an explicit `clickup_id` in this file
wins, then a story name (case-insensitive, trimmed), then a bare token is
treated as a literal id for a target outside the file.

**It refuses rather than guesses**, and never applies part of an edge set:

| situation | behaviour |
|---|---|
| two stories share the name | error naming the count — reference it by `clickup_id` |
| a name matching no story | error — a value with whitespace was meant as a name, and pushing it as an id returns an opaque 400 instead of naming your typo |
| a name resolving to the story itself | skipped with a warning (a no-op, not a hole in the graph) |
| a name pending create, under `--dry-run` | reported as resolving after create |
| a name pending create, in a real run | error — the reconcile pass runs after creates, so this should be impossible |

The first two abort **the whole edge set for that story** and count as an error.
A half-applied dependency graph is worse than none, because it looks complete.

Only the **waiting_on** direction is modeled; ClickUp maintains the mirrored
"blocking" edge on the other task automatically. Semantics mirror assignees:

| YAML on the story | Push behaviour |
|---|---|
| no `depends_on` key | **Unmanaged** — ClickUp dependencies left untouched (UI-added edges preserved) |
| `depends_on: []` | **Clear** — the task's waiting_on edges removed |
| `depends_on: [id, …]` | **Authoritative** — ClickUp reconciled to exactly this set |

Edges are reconciled in a **second pass**, after the create/update pass, so a
task created in the same run already has its `clickup_id` and can be referenced
— which is why a **name** reference works on a first sync where an id could not.
`pull` reads
remote waiting_on edges back into `depends_on`; `diff` shows mismatches. Requires
the **Dependencies ClickApp** enabled on the Space.

`push`, `sync`, and `merge` all run the same second pass, so adding a dependency
needs nothing beyond your normal command — including `sync --dry-run → sync`.
Edges are YAML-authoritative (the table above) and are **not** base-tracked 3-way
fields: declaring `depends_on` in YAML is *applied*, never surfaced as a sync
conflict. `pull`/`diff` round-trip them too.

A `--dry-run` previews every edge change it would make — additions and removals,
named by story and id — including for a task the run would *create*, whose edges
land in the second pass after the create. Because `depends_on: []` **deletes real
blockers**, `sync`/`merge` also print a loud warning before any edge removal (see
the footgun note under Relations).

## Relations (non-blocking "linked tasks")

Each story can also declare a `related` list — ClickUp's non-blocking
"linked tasks" (the relate/link feature, `POST /task/{id}/link/{links_to}`).
Unlike `depends_on`, a link implies **no ordering or blocking** — it's a plain
association. Targets are referenced **by story name or `clickup_id`**, exactly
like `depends_on` — same resolution rules, same refusals:

```yaml
stories:
  - name: Ingestion adapter
    related:
      - Retrieval spike     # by name
      - 860000004           # or a raw id, for a target outside this YAML
```

> **Names make the symmetry footgun easier to reach.** A link is non-directional
> and ClickUp records it on both endpoints, so declaring it on *both* stories is
> redundant — and the union semantics below exist because two managed stories
> disagreeing about a mutual link used to make it oscillate every sync. Writing
> both sides by hand is now easier than it was, so it is worth restating:
> **declare a link on one endpoint.** The other end still round-trips.

A link is **non-directional** — ClickUp records it on both endpoints — so the
reader collapses each link to "the other end," and the relation round-trips
identically regardless of which task created it. Semantics mirror `depends_on`:

| YAML on the story | Behaviour |
|---|---|
| no `related` key | **Unmanaged** — ClickUp links left untouched (UI-added links preserved) |
| `related: []` | **Clear** — the task's links removed |
| `related: [id, …]` | **Authoritative** — ClickUp reconciled to exactly this set |

Reconciled by the same second pass as `depends_on` (`push`/`sync`/`merge`), read
back by `pull`, and shown by `diff`.

> **Symmetric-link footgun.** A link lives on *both* endpoints. If you manage
> `related` on **both** tasks of a pair and only one lists the other, the
> authoritative reconcile will **delete** the link (the side that doesn't list it
> wins its own `related: []`/`[other]`). To avoid surprise: declare the link on
> **both** ends, or run `pull` first (it symmetrizes — writing the reciprocal
> `related` onto both stories), then edit. `sync`/`merge` print a **loud warning
> before removing any edge** (dependency or relation) so a destructive deletion is
> never silent; `push` removes silently by its authoritative-overwrite contract.

## Parent / child (subtasks)

A story can declare a `parent` — that makes it a real ClickUp **subtask**, so the
`epics → stories` hierarchy in the YAML survives the round-trip instead of being
flattened into a hub card plus prose. **The reference is the parent story's `name`**,
resolved to its `clickup_id` at push time:

```yaml
epics:
  - name: Contact export
    stories:
      - name: Export contacts hub          # the parent
        clickup_id: 860000010
      - name: Assign owners
        clickup_id: 860000011
        parent: Export contacts hub        # by name, not by id
      - name: API write-back
        clickup_id: 860000012
        parent: Export contacts hub
```

Why names rather than ids (as `depends_on` uses): the reconcile pass runs **after**
creates, so a parent created in the same run already has its id written back — a
name reference therefore links a brand-new parent *and* child on the **first** sync,
where an id reference could not (the id doesn't exist until that run creates it). It
also reads as hierarchy in the file, which is the point.

Resolution rules:

- Matched **case-insensitively and trimmed** against story names in the same file.
- A raw `clickup_id` is also accepted — that's how you reference a parent that
  isn't a story in this file. It must still be **in the same list**: a parent in
  another list is refused (see the warning below).
- **Two stories sharing the referenced name is an error, not a guess.** Reference
  that parent by `clickup_id` instead.

| YAML on the story | Behaviour |
|---|---|
| no `parent` key | **Unmanaged** — ClickUp hierarchy untouched (a subtask nested in the UI survives) |
| `parent: <name or id>` | **Authoritative** — the task is moved under that parent, **in place** |
| `parent:` (empty) | **Top-level** — valid only if the task already *is* top-level (see below) |

Reconciled by the same second pass as `depends_on`/`related` (`push`/`sync`/`merge`),
read back by `pull`, shown by `diff`, and — like the other edges — **not
base-tracked**, so a declared parent is applied rather than raised as a conflict.

> **⚠️ There is no un-parent.** Sandbox-verified: `PUT /task/{id}` with
> `{"parent": null}` **or** `{"parent": ""}` returns **HTTP 200 and changes
> nothing**. So clearing a `parent:` on a story that *is* a subtask cannot be
> honoured — the tool **fails loudly** (counted as an error) rather than silently
> diverging. Promote the task to top level in the ClickUp UI, then pull. Setting
> and *changing* a parent both work fine, in place — never delete+recreate.

> **⚠️ Subtasks are hidden in the default List view.** ClickUp's List view has a
> `Subtasks` control with **Collapsed (default) / Expanded / Separate**. Under the
> default the child rows are **not rendered** — the parent row shows a disclosure
> triangle and a `⑂ N` badge instead. Switch the view to **Expanded** (or
> **Separate**) and save it, or a board that used to show flat rows will look like
> it lost them. Status-group counts stay at top-level tasks either way.

> **⚠️ A parent must be in the same list, and the tool enforces it.** ClickUp is
> asymmetric here: *creating* a task under a parent in another list is refused
> (`400 ITEM_137 "Parent not child of list"`), but a `PUT` **succeeds and silently
> moves the task into the parent's list** — verified live. Since this tool sets
> `parent` via `PUT`, an unchecked cross-list reference would relocate the story out
> of the synced list, and the next sync would report it as `archived_in_clickup`. So
> a parent outside the synced list is refused before the write (under `--dry-run`
> too); a mistyped id fails with a readable error rather than a bare 400.

> **⚠️ Deleting a parent deletes its children.** Verified live: deleting a hub took
> its subtask with it (`GET` on the child → 404, gone from the list). If you're
> moving off the hub-card-plus-`related` pattern, note the difference — deleting a
> hub card used to leave its siblings alone. The tool never deletes tasks, but a
> delete in the **UI** now destroys the children underneath.

Also verified against the live API: nesting works at least 3 deep, a subtask stays
in its parent's list, tags **and the Epic dropdown** apply to subtasks normally
(the dropdown reads back through the same `orderindex` shape the no-op-skip already
handles, so an already-correct value isn't re-written), and there is **no status
rollup** — completing every child leaves the parent's status alone (the rollup is a
UI progress badge, not a field). Full evidence:
[`docs/subtask-parent-findings.md`](docs/subtask-parent-findings.md).

## Descriptions (markdown / task-reference safe)

Descriptions are read as `markdown_description` and written as `markdown_content`,
so a task **reference** survives the round-trip as a `[label](url)` link. (ClickUp's
plain `description`/`text_content` flattens a task mention to whitespace — silently
dropping the reference — which is why the markdown form is used.) ClickUp is **not**
storing two separate fields: a description is stored once and rendered both ways, so
existing tasks need no migration — every task already returns a valid
`markdown_description`.

### Two kinds of "chip" — only one is API-reachable (verified 2026-07-15)

ClickUp has two distinct task-reference visuals. Do not conflate them — an earlier
version of this section wrongly claimed a description link "renders as a chip." It
does not.

- **Inline description chip** — the pill you get by typing `@` + a task title
  *inside the description prose* in the UI. It is a rich-text embed token that lives
  ONLY in ClickUp's internal editor format (the `frontdoor` API, cookie/session
  auth). **The public `pk_` API cannot create it, and every readable field destroys
  it:** `description`/`text_content` drop it entirely; `markdown_description`
  degrades it to plain title text + a self-link. Consequence: **a hand-added inline
  chip is flattened to a plain link the next time this tool pushes that task** — do
  not rely on one surviving a sync. (Empirically confirmed: bare short URL, full
  team-scoped URL, `<autolink>`, and labeled link were each written via
  `markdown_content` on both a new and an existing task — none rendered as a chip.)
- **Linked-task relationship chip** — the chip in the task's **Relationships /
  "Linked"** panel. This is first-class structured data (`linked_tasks`), is
  **fully API-writable**, and **this tool already creates it from the YAML
  `related:` / `depends_on:` fields.** This is the durable, sync-safe way to "chip"
  a task reference — prefer it whenever the goal is an actual relationship.

### Authoring convention — description task references are labeled links

**Hard rule (Maurice, 2026-07-14):** when a YAML `description` references another
ClickUp task, embed it as a **markdown link labeled with the task title or ID** —
never a bare task ID and never a raw URL as visible text:

```yaml
description: |
  Blocked by [Acquire foundation corpus](https://app.clickup.com/t/86baxxxxx)
  — see [`86baxxxxx`](https://app.clickup.com/t/86baxxxxx) for details.
```

This renders as a **plain clickable link** in the description (NOT an inline chip —
see above) and round-trips losslessly. The label matters for two reasons:
self-referential links (label == url) are deliberately collapsed to bare text on
read (so an unlabeled link will not survive), and a readable label beats a raw ID
for anyone skimming the task. When the goal is a real chip for the relationship,
declare it in `related:` / `depends_on:` — that produces a Relationships-panel chip
via the API. The same labeled-link convention already applies to task comments and
chat/board messages authored outside this tool; this section keeps synced
descriptions consistent with them.

User mentions in descriptions use ClickUp's native markdown form
`[@Name](#user_mention#<user_id>)` (observed in `markdown_description` reads;
user ids come from the workspace-members API).

The markdown rendering applies two transformations that the tool normalizes on
read so they never show as spurious diffs:

- **Backslash-escaped punctuation** — `Article_type` comes back as
  `Article\_type`; unescaped back to the authored text.
- **Auto-linkified bare URLs/emails/domains** — `alice@example.com` comes back
  as `[alice@example.com](mailto:alice@example.com)`; *self-referential* links
  (label == url, ignoring scheme) are collapsed to the bare text. A genuine
  mention or labeled link — where the label differs from the url — is preserved.

The `Points · Milestone · Sprint` meta-header behaves exactly as before; it's
prepended on push and stripped on pull regardless of markdown.

### Pushing a custom dropdown field (e.g., a PM-curated "Epic" field)

If ClickUp has a single-select dropdown custom field that mirrors the epic
axis (some teams prefer this to tags), declare the mapping in the YAML
`project:` block and push will set it on every story automatically:

```yaml
project:
  name: My Project
  clickup_list_id: '900000000000'
  epic_dropdown_field_id: '00000000-0000-0000-0000-000000000000'
  epic_dropdown_options:
    'Epic One': '11111111-1111-1111-1111-111111111111'
    'Epic Two': '22222222-2222-2222-2222-222222222222'
    'Epic Three': '33333333-3333-3333-3333-333333333333'
    'Epic Four': '44444444-4444-4444-4444-444444444444'
```

If either field is missing, the dropdown push is skipped silently — old
YAML files keep working unchanged. Per-story override via
`epic_dropdown_value: "<epic name>"` on a single story.

## Backups (push and pull)

Each command backs up **the side it's about to overwrite**, and `--no-backup`
disables whichever applies:

- **`push`** snapshots the current **ClickUp state** to a YAML file. To recover
  from a bad push, **`push` that backup file back up** (`push <backup-file>`) —
  it restores ClickUp to the snapshot. (Not `pull` — pull reads ClickUp's live
  API, never a backup file.) Runs by default for any non-empty list; new empty
  sandbox lists skip it.
- **`pull`** copies your **YAML file** (the thing pull overwrites) to
  `.clickup-sync/yaml-backup-<stem>-<iso>.yaml` beside the project — the same
  gitignored sidecar that holds the base snapshot. To recover from a bad pull,
  copy that backup back over your YAML. If the backup can't be written, pull
  **aborts** rather than overwrite unprotected (rerun with `--no-backup` to
  proceed without one). Runs by default.
- `--backup-to /path` overrides the location for either; `--backup-to` with no
  value uses the default. `sync`/`merge` accept `--backup-to` but don't
  auto-backup — pass it to opt in.

## 3-way merge (sync)

`sync` is a **3-way merge** whenever a base snapshot exists. The base — the
field values as of the last successful reconcile — lives in a sidecar file per
list at `<yaml-dir>/.clickup-sync/base-<list_id>.json`, written automatically
by every `push`, `pull`, and `sync`. With a base, `sync` can tell *which* side
changed a field instead of treating every difference as a conflict:

| Field changed since base | Action |
|---|---|
| only in YAML | push to ClickUp (auto) |
| only in ClickUp | pull into YAML (auto) |
| both sides, same value | nothing |
| both sides, **different** values | true conflict → `--on-conflict` |

True conflicts are handled by `--on-conflict`:

| `--on-conflict` | Behaviour |
|---|---|
| `stop` (default) | abort the whole sync, change nothing, print the conflict list |
| `local` | YAML wins the conflicting fields |
| `remote` | ClickUp wins the conflicting fields |

`stop` is fail-safe — no silent data loss. With **no base** (e.g. the first
run), `sync` falls back to the legacy 2-way reconcile below and writes a base
for next time.

**Scope:** 3-way covers the scalar synced fields (name, status, description,
priority, milestone, due_date, start_date). Assignees and tags are not
base-tracked — assignee divergence is surfaced as a conflict (so `stop` catches
it; `local`/`remote` applies that direction). Keep `status_map` 1:1 — a
many-to-one mapping makes the pull-side reverse-lookup ambiguous.

**Dates** are date-only (`YYYY-MM-DD`), pushed at noon UTC. Stable for
workspaces UTC−12…UTC+11; a date set in the **ClickUp UI** of a UTC+12-or-east
workspace may pull back one calendar day earlier (stable, never oscillates).
YAML-originated dates are exact in every timezone.

## Conflict Strategies (legacy 2-way — only when no base exists yet)

Used with `sync --conflict`:

| Strategy | Behaviour |
|----------|-----------|
| `local` | YAML wins all differences (equivalent to push) |
| `remote` | ClickUp wins all differences (equivalent to pull) |
| `ask` | Interactive per-field prompt (default) |
| `merge` | LLM proposes a merged value; you confirm each |

## Field coverage & limitations

Which task fields the sync actually handles, and in which direction. **If a
field is not listed as supported, the tool ignores it — it is neither pushed
nor pulled, and a value set in the ClickUp UI will be invisible to the YAML
(and vice-versa).**

| Field | Push (YAML→ClickUp) | Pull (ClickUp→YAML) |
|-------|:--:|:--:|
| name | ✅ | ✅ |
| status (via `status_map`) | ✅ | ✅ |
| description (with `Points/Milestone/Sprint` meta header) | ✅ | ✅ |
| priority (`1`=urgent, `2`=high, `3`=normal, `4`=low, `null`=none) | ✅ | ✅ |
| milestone (`custom_item_id`) | ✅ | ✅ |
| due_date / start_date (`YYYY-MM-DD`, date-only) | ✅ | ✅ |
| assignees | ✅ | ✅ |
| `depends_on` (waiting_on edges) | ✅ (push/sync/merge; UI-added edges preserved) | ✅ (pull/diff) |
| `related` (non-blocking linked tasks) | ✅ (push/sync/merge; UI-added links preserved) | ✅ (pull/diff) |
| `parent` (subtask hierarchy, by story name) | ✅ (push/sync/merge; set + re-parent in place, same list only, **cannot un-parent**) | ✅ (pull/diff) |
| tags | ✅ (additive; UI-added tags preserved) | ⚠️ epic placement only — UI tag *edits* are **not** pulled into YAML |
| Epic dropdown (one configured custom field) | ✅ | ❌ |

**Not implemented at all (silently ignored both directions):**

- **`time_estimate`** — not synced.
- **Native ClickUp `points` field** — not synced. The YAML `points` value is
  shown only as `Points: N` text in the description. Native points also require
  the **Sprint Points ClickApp** enabled on the list (a `PUT {points: N}`
  otherwise 400s with `ITEM_225`), so it stays out of scope until that's on.
- **Arbitrary custom fields** — only the single configured Epic dropdown is
  handled. Other custom fields (any other single-select or text field on the
  board) are not read or written.

Caveat on `priority`: a YAML value only reaches the board on a `push`/`sync`
that runs *after* the value is set — setting it in YAML and never pushing
leaves the board unchanged (this is data drift, not a bug). Likewise, push
cannot reliably *clear* a board priority back to none.

## Tags & milestones (read this — two "milestone" concepts)

There are **two unrelated things both called "milestone"**:

| YAML | Becomes in ClickUp | Use |
|---|---|---|
| `milestone_label: M1` | a **tag** `m1` (lowercased) | group tasks into a milestone (a tag set, e.g. all `m2` tasks) |
| `milestone: true` | a **native Milestone task** (`custom_item_id=1`, the ◆ diamond) | a single milestone marker task (can carry its own due date) |

They're independent: a task can be tagged `m2` *and/or* be a ◆ milestone. To give a milestone a **due date on the board**, either date every member task or add one `milestone: true` marker task (tag it `milestone_label` too so it sits in the group). Native milestone creation requires the **Milestones ClickApp** enabled on the Space (sandbox-verified); native `points` similarly requires the **Sprint Points ClickApp** (else `PUT` 400s `ITEM_225`).

### Tag sync is push-authoritative, NOT pull-tracked (the limit)

Sandbox-verified behavior:

- **Push (YAML→ClickUp):** the tag set on a story = epic name (if `push_epic_tag`) + `milestone_label` slug (`m1`) + `sprint_target` slug (`s2`) + explicit `tags:[]`. Additive. Tags in the **managed universe** (all epic names + all milestone slugs + all explicit tags across the YAML) that are *not* on a story are stripped as stale — that's how a milestone/sprint reassignment takes effect. Tags **outside** the managed universe (added in the ClickUp UI) are **preserved**.
- **Pull (ClickUp→YAML): tags are read only for epic placement.** A tag added or removed **in the ClickUp UI does NOT come back into the YAML** — and on the next push a managed tag missing from YAML is reverted. Tags are also **not** part of the 3-way merge (no conflict detection on tags).
- **Rule of thumb:** manage `m`/`s`/milestone tags **from the YAML**; don't edit them in the ClickUp UI expecting them to round-trip. Ad-hoc UI tags outside the managed set are safe.

## Setup

```bash
# Install dependency
pip install pyyaml

# Configure credentials
cp .env.example clickup.env
# Edit clickup.env with your ClickUp API token
# Optionally add OPENAI_API_KEY for 'merge' command
```

## YAML File Structure

```yaml
project:
  name: My Project
  clickup_list_id: '901414096256'
  last_synced: '2026-02-08T23:59:01+00:00'

status_map:
  done: done
  in_progress: current sprint
  upcoming_sprint: upcoming sprint
  backlog: backlog

epics:
  - number: 1
    name: Foundation
    status: done
    points: 21
    priority: 3
    stories:
      - name: As a developer, I want X so that Y
        clickup_id: 86b8f2a23   # populated after first push
        points: 3
        status: done
        description: 'Optional description'
        priority: 3
```

## Usage

```bash
# Check what's out of sync (no changes made)
python3 clickup.py diff project.yaml

# Push local changes to ClickUp
python3 clickup.py push project.yaml

# Pull ClickUp status updates into YAML
python3 clickup.py pull project.yaml

# Full bidirectional sync, ask on each conflict
python3 clickup.py sync project.yaml --conflict ask

# Full sync, YAML wins all conflicts
python3 clickup.py sync project.yaml --conflict local

# LLM-assisted merge
python3 clickup.py merge project.yaml

# Offline status summary
python3 clickup.py status project.yaml

# Check milestone-date coherence (advisory; never changes anything)
./clickup.py lint docs/project-tasks.yaml

# Hand-edit a task file inside the lock (the supported edit path)
./clickup.py with-lock docs/project-tasks.yaml -- $EDITOR docs/project-tasks.yaml

# Wait longer for a lock held by another session
./clickup.py sync docs/project-tasks.yaml --lock-timeout 300

# Dry run (show what would happen)
python3 clickup.py push project.yaml --dry-run
```

## Roadmap

- **Subtasks.** The ClickUp API supports them (create a task with a
  `parent: <task_id>` field; read via `?include_subtasks=true`). Model them
  either as a `parent:` reference on a story or as a nested `subtasks:` list,
  reconciled like the create/update pass. Today stories are flat top-level tasks
  only.

## Credentials

Each secret resolves in a fixed precedence — **environment variable → [`pass`](https://www.passwordstore.org/) → legacy `~/bin/*.env` file** — so the canonical store is `pass`, while existing `.env` setups keep working untouched.

| Secret | Env var | `pass` entry | `.env` fallback |
|---|---|---|---|
| ClickUp token (production) | `CLICKUP_API_TOKEN` | `clickup/api-token` | `~/bin/clickup.env` |
| ClickUp token (sandbox, with `--sandbox`) | `CLICKUP_API_TOKEN_SANDBOX` | `clickup/sandbox-api-token` | `~/bin/clickup-sandbox.env` |
| OpenAI key (optional, for `merge`) | `OPENAI_API_KEY` | `clickup/openai-key` | `~/bin/clickup.env` |

```bash
# Recommended: store in pass
pass insert clickup/api-token
pass insert clickup/sandbox-api-token   # only if you use --sandbox
```

Pass `--sandbox` on any command to target the sandbox ClickUp account instead of production (selects the sandbox token from whichever source above resolves first). Get a ClickUp API token at: **Settings → Apps → API Token**.

## Logging

All operations are logged to `~/tmp/clickup_sync.log` (debug level) and stdout (info level).

## Requirements

- Python 3.10+
- `pyyaml` (`pip install pyyaml`)
- `openai` API key (optional, only for `merge` command)
