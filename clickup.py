#!/usr/bin/env python3
"""clickup.py -- Bidirectional sync between YAML project files and ClickUp.

Commands:
  push    Push local YAML state to ClickUp (create new, update changed)
  pull    Pull ClickUp state into local YAML (update statuses, descriptions, detect new tasks)
  diff    Show differences between YAML and ClickUp (no changes made)
  sync    Full bidirectional sync with per-conflict resolution strategy
  merge   LLM-assisted conflict resolution (pull + push with intelligent merging)
  status  Show summary of project state from YAML (offline, no API calls)
  lint    Report milestone-date incoherence (advisory; flags, never modifies)
  pin     Write an immutable, pinned copy of this tool and print its path --
          the recommended way to run a board (it cannot change under you)
  with-lock  Run any command (an editor, a script, a shell) while holding the
             project's advisory lock -- the supported way to hand-edit a task
             file, since every write goes through this tool

Locking:
  Every writing command holds an advisory lock on the task file AND on the
  ClickUp list for the whole run, and releases it at the end. A second
  concurrent run waits, visibly, then fails loudly rather than doing nothing.
  The lock is the same file, format and TTL the Claude Code diamond-lock hook
  uses, and under Claude Code the same identity, so a session's edit and its
  sync are one continuous hold. --no-lock opts out; --lock-timeout tunes the
  wait.

Conflict strategies (--conflict flag for sync):
  local    YAML wins all conflicts (same as push)
  remote   ClickUp wins all conflicts (same as pull)
  ask      Prompt per conflict: local / remote / merge / skip (default)
  merge    Use LLM to propose merged value, confirm each
"""

import argparse
import copy
import hashlib
import json
import logging
import os
import re
import shutil
import socket
import subprocess
import sys
import threading
import time
import uuid
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone, date
from pathlib import Path
from typing import Any, Optional

try:
    import yaml
except ImportError:
    print("ERROR: PyYAML is required. Install with: pip install pyyaml", file=sys.stderr)
    sys.exit(1)


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOG_PATH = Path.home() / "tmp" / "clickup_sync.log"


def setup_logging() -> logging.Logger:
    logger = logging.getLogger("clickup_sync")
    logger.setLevel(logging.DEBUG)

    # The debug log is a convenience, not a dependency. Import-time used to
    # hard-crash on any box where ~/tmp did not exist (CI, a fresh checkout, a
    # container) -- and it crashed on *import*, so nothing in the tool ran at
    # all. Create the directory, and fall back to console-only if we still
    # cannot write there.
    try:
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(LOG_PATH, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        )
        logger.addHandler(file_handler)
    except OSError as e:
        print(f"WARNING: file logging disabled ({LOG_PATH}: {e})", file=sys.stderr)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(console_handler)

    return logger


log = setup_logging()


# ---------------------------------------------------------------------------
# Environment & config
# ---------------------------------------------------------------------------


# Credentials resolve in a fixed precedence (env var > pass > legacy env file)
# so the project can standardize on `pass` while existing ~/bin/*.env setups
# keep working untouched. `pass` is the canonical store; the env-file branch is
# a back-compat fallback only.


def _pass_get(key: str) -> Optional[str]:
    """Return the first line of ``pass show <key>``, or None if unavailable.

    None covers every "no secret here" case — pass not installed, the entry
    missing, or an empty body — so the caller falls through to the next source.
    """
    try:
        result = subprocess.run(
            ["pass", "show", key],
            capture_output=True,
            text=True,
            timeout=15,
        )
    except (FileNotFoundError, OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0:
        return None
    lines = result.stdout.splitlines()
    if not lines:
        return None
    first = lines[0].strip()
    return first or None


def _read_env_file_value(path: Path, key: str) -> Optional[str]:
    """Read a single ``KEY=value`` from a .env file without mutating os.environ.

    Handles the ``export`` prefix and quote stripping, and is side-effect-free
    so the file-fallback branch can't leak a value into the process environment
    and bleed across callers (or tests).
    """
    if not path.exists():
        return None
    try:
        contents = path.read_text()
    except OSError:
        return None  # unreadable (permissions, etc.) — fall through to caller
    for line in contents.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:]
        k, sep, value = line.partition("=")
        if sep and k.strip() == key:
            return value.strip().strip("\"'") or None
    return None


def _resolve_secret(
    env_var: str, pass_key: str, env_path: Path, label: str
) -> str:
    """Resolve a secret: explicit env var > ``pass`` > legacy env file.

    The env-file key is the same name as ``env_var``. Exits the process with a
    pointer to all three sources if none yields a value.
    """
    val = os.environ.get(env_var, "").strip()
    if val:
        return val
    val = _pass_get(pass_key)
    if val:
        return val
    val = _read_env_file_value(env_path, env_var)
    if val:
        return val
    log.error(
        f"{label} not set. Add it to pass ({pass_key}), export {env_var}, "
        f"or put it in {env_path}"
    )
    sys.exit(1)


def _sandbox_mode() -> bool:
    """True when CLI/env selects the sandbox ClickUp account (CLICKUP_SANDBOX)."""
    return bool(os.environ.get("CLICKUP_SANDBOX"))


def _prod_env_path() -> Path:
    return Path.home() / "bin" / "clickup.env"


def _sandbox_env_path() -> Path:
    return Path.home() / "bin" / "clickup-sandbox.env"


def get_clickup_token() -> str:
    if _sandbox_mode():
        return _resolve_secret(
            "CLICKUP_API_TOKEN_SANDBOX",
            "clickup/sandbox-api-token",
            _sandbox_env_path(),
            "ClickUp sandbox API token",
        )
    return _resolve_secret(
        "CLICKUP_API_TOKEN",
        "clickup/api-token",
        _prod_env_path(),
        "CLICKUP_API_TOKEN",
    )


def get_openai_key() -> str:
    return _resolve_secret(
        "OPENAI_API_KEY",
        "clickup/openai-key",
        _prod_env_path(),
        "OPENAI_API_KEY",
    )


# ---------------------------------------------------------------------------
# YAML loading / saving
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Run provenance: which code produced this run
# ---------------------------------------------------------------------------
#
# The hazard this closes is NOT that a running process has its code swapped --
# Python reads this single file fully at interpreter start and git replaces
# files by rename, so that does not happen. It is that **nobody can say which
# version ran**.
#
# Witnessed 2026-08-21: an operator prepared a client-board sync against one
# commit of this file, the working tree moved to another commit while they were
# preparing, and the only reason anyone noticed was an unrelated message. There
# was no record either way, before or after. Every corpus board is invoked from
# a live development checkout, so this is the normal case, not an accident.
#
# Two goals, and they are NOT the same one -- keeping them apart is what stops
# the mechanism drifting:
#
#   * ATTRIBUTION -- say exactly what ran. A content hash achieves this
#     completely, including for uncommitted code. Every run states its
#     provenance; `--version` reports the same.
#   * NOT WRITING UNTESTED CODE TO A CLIENT BOARD -- a separate goal, and the
#     only one that justifies a refusal. This is why a writing command stops on
#     a modified `clickup.py`: not because the run would be unattributable (the
#     hash handles that) but because nothing has tested those bytes.
#
# So the bypass MARKS a run; it never blinds one. `--allow-dirty` still records
# the full provenance and stamps the run as bypassed, prominently.
#
# The guard is only as good as the easy path around it: a guard with a
# convenient bypass becomes decoration, because everyone types the flag and it
# then fires on nothing. `clickup.py pin` therefore makes the SAFE path a single
# argument-free command -- easier than remembering a flag, which is the point.

TOOL_PATH = Path(__file__).resolve()
PROVENANCE_EXIT_CODE = 4  # distinct from 1 (failed) and 3 (lock busy)

# Commands that write to the YAML, to ClickUp, or to both. Only these are
# refused: reading the world can always be accounted for by re-reading it.
WRITING_COMMANDS = ("push", "pull", "sync", "merge")


def _git_out(*args: str) -> Optional[str]:
    """Run git in the tool's own directory; None on any failure.

    None means "not a git checkout, or git unavailable" -- both legitimate (an
    installed or pinned copy), never an error.
    """
    try:
        out = subprocess.run(
            ["git", "-C", str(TOOL_PATH.parent), *args],
            capture_output=True, text=True, timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return out.stdout.strip() if out.returncode == 0 else None


def _file_sha256(path: Path) -> Optional[str]:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:  # pragma: no cover - defensive
        return None


def tool_provenance() -> dict:
    """Where this specific `clickup.py` came from.

    ``dirty`` is about THIS FILE, not the whole working tree: an unrelated
    change elsewhere in the repo does not make a run untested, and refusing on
    it would block people for no safety gain.
    """
    sha256 = _file_sha256(TOOL_PATH)
    commit = _git_out("rev-parse", "HEAD")
    if commit is None:
        return {"path": str(TOOL_PATH), "sha256": sha256,
                "commit": None, "dirty": None, "in_git": False}
    status = _git_out("status", "--porcelain", "--", str(TOOL_PATH))
    return {"path": str(TOOL_PATH), "sha256": sha256,
            "commit": commit, "dirty": bool(status), "in_git": True}


def format_provenance(prov: dict, bypassed: bool = False) -> str:
    """One line naming what ran. The sha256 is always present, so even a
    bypassed run is fully attributable -- that is what makes marking it enough."""
    sha = (prov.get("sha256") or "?")[:12]
    mark = "  [!! --allow-dirty BYPASS: untested code !!]" if bypassed else ""
    if not prov.get("in_git"):
        return f"clickup.py {prov['path']} | sha256 {sha} | pinned copy (not a git checkout){mark}"
    state = "MODIFIED" if prov.get("dirty") else "clean"
    return (f"clickup.py {prov['path']} | commit {(prov.get('commit') or '?')[:7]} "
            f"({state}) | sha256 {sha}{mark}")


def pinned_copy_path(prov: dict) -> Path:
    stamp = (prov.get("commit") or prov.get("sha256") or "unknown")[:7]
    suffix = "-dirty" if prov.get("dirty") else ""
    return Path.home() / "bin" / f"clickup-{stamp}{suffix}.py"


def cmd_pin() -> int:
    """Write a pinned, immutable copy of this tool and print its path.

    The safe path has to be EASIER than the bypass or the guard is decoration.
    This is one argument-free command; the alternative is remembering a flag.
    """
    prov = tool_provenance()
    if prov.get("dirty"):
        log.error(
            "Refusing to pin a MODIFIED clickup.py: a pinned copy is meant to "
            "be a known, committed version. Commit first, then pin."
        )
        return PROVENANCE_EXIT_CODE
    dest = pinned_copy_path(prov)
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(TOOL_PATH, dest)
    dest.chmod(0o755)
    print(f"Pinned: {dest}")
    print(f"  {format_provenance(prov)}")
    print("\nRun boards through this copy from now on. It cannot change under "
          "you when someone merges, and it reports its own hash:")
    print(f"  {dest} sync <yaml-file>")
    return 0


def assert_attributable(prov: dict, command: str, allow_dirty: bool) -> None:
    """Refuse a writing command running from untested (uncommitted) code."""
    if command not in WRITING_COMMANDS or not prov.get("dirty"):
        return
    if allow_dirty:
        return  # marked, not blinded -- the caller stamps the run as bypassed
    dest = pinned_copy_path({**prov, "dirty": False})
    raise RuntimeError(
        "Refusing to run '" + command + "': clickup.py has uncommitted changes, "
        "so these bytes have never been tested and this is a writing command.\n"
        "  " + format_provenance(prov) + "\n"
        "  Three ways forward, easiest first:\n"
        "    1. Run a pinned copy instead of the development tree (recommended, "
        "and the reason this exists):\n"
        "         " + str(TOOL_PATH) + " pin\n"
        "       ...then invoke " + str(dest) + " instead. It cannot change under "
        "you when someone merges.\n"
        "    2. Commit the change and run again.\n"
        "    3. --allow-dirty, if you are deliberately testing an uncommitted "
        "change. The run still records its exact content hash and is stamped as "
        "a bypass -- it is marked, not hidden."
    )


def warn_if_head_moved(before: Optional[str], command: str) -> None:
    """Report a checkout that moved while the run was in flight.

    The run itself is unaffected -- this process loaded its code at start. What
    changed is that the tree no longer matches what ran, so anyone reading it
    afterwards to work out what happened reads the wrong thing. That is the
    exact event of 2026-08-21, and it previously produced no signal at all.
    """
    if not before or command not in WRITING_COMMANDS:
        return
    after = _git_out("rev-parse", "HEAD")
    if after and after != before:
        msg = ("NOTE: the checkout moved during this run (" + before[:7] + " -> "
               + after[:7] + "). This run used " + before[:7] + "; the files on "
               "disk no longer match it. Attribute this run to " + before[:7]
               + ", not to what is checked out now.")
        print("\n" + msg, file=sys.stderr, flush=True)
        log.warning(msg)


class _VersionAction(argparse.Action):
    """Prints provenance and exits, without requiring the yaml_file positional."""

    def __init__(self, option_strings, dest, **kwargs):
        super().__init__(option_strings, dest, nargs=0, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        print(format_provenance(tool_provenance()))
        parser.exit()


# ---------------------------------------------------------------------------
# YAML-only story fields
# ---------------------------------------------------------------------------
#
# Fields that live in the task file and NEVER reach ClickUp. They are authored,
# not derived: nothing pushes them, nothing pulls them, and no remote
# counterpart exists for them to conflict with.
#
# Why they exist. A card description accumulates context that helps whoever
# WRITES the card and actively harms whoever READS it -- provenance, mostly:
# who said a thing on a call, why a threshold is set where it is, which SOW
# clause an obligation comes from. On a client-visible board an over-long
# description also spends the client's attention on our reasoning instead of
# their action. Without somewhere adjacent to put that material the only
# choices are on the card (where it hurts the reader) or in a separate
# document (where it drifts away from the card it describes).
#
# This constant is documentation with teeth, not machinery: nothing reads it at
# runtime. Unknown story keys already survive every code path -- push and pull
# both work from an explicit field list and mutate story dicts in place rather
# than rebuilding them. That is a property worth NAMING and TESTING rather than
# relying on, because the day someone rebuilds a story dict or adds a field to
# `comparable_local`, this material disappears silently. It is exactly the
# material people put here because it was too valuable to delete.
YAML_ONLY_STORY_FIELDS = ("notes",)


def load_yaml(path: str) -> dict:
    with open(path, "r") as f:
        data = yaml.safe_load(f)
    if not data or "epics" not in data:
        log.error("Invalid YAML: missing 'epics' key")
        sys.exit(1)
    return data


def save_yaml(data: dict, path: str) -> None:
    data = copy.deepcopy(data)
    data["project"]["last_synced"] = datetime.now(timezone.utc).isoformat()
    # Atomic: the task file is the source of truth for a whole project, and
    # push/sync flush it once per created task. A crash partway through a plain
    # truncate-and-write would leave it truncated.
    tmp = Path(path).with_suffix(Path(path).suffix + f".tmp{os.getpid()}")
    try:
        with open(tmp, "w") as f:
            yaml.dump(data, f, default_flow_style=False, allow_unicode=True, sort_keys=False, width=120)
        os.replace(tmp, path)
    except Exception:
        try:
            tmp.unlink()
        except OSError:
            pass
        raise
    log.info(f"YAML saved to {path}")
    # Refresh the 3-way base snapshot so the next sync can tell who-changed-what.
    # Persisting here (the single chokepoint for push/pull/sync) keeps the base
    # fresh even under interleaved pull->edit->push workflows. Best-effort: a
    # base-write failure must never fail the YAML save.
    try:
        list_id = data.get("project", {}).get("clickup_list_id")
        if list_id:
            save_base_snapshot(
                base_snapshot_path(path, str(list_id)),
                data,
                data.get("status_map", {}),
            )
    except Exception as e:  # pragma: no cover - defensive
        log.warning(f"Could not write base snapshot: {e}")


# ---------------------------------------------------------------------------
# Advisory locking (interoperates with the Claude Code "diamond lock" hook)
# ---------------------------------------------------------------------------
#
# Why this lives in the tool and not only in a harness hook
# --------------------------------------------------------
# A PreToolUse hook can only see Claude Code tool calls, so it guards a
# hand-edit made with Edit/Write and nothing else. It is structurally blind to
# the two writers that matter most here:
#
#   * this tool itself -- ``sync``/``push``/``pull``/``merge`` are WRITERS of the
#     YAML (each new task's ``clickup_id`` is flushed back immediately on
#     create, ``last_synced`` is stamped, ``pull`` rewrites story rows), and
#     they also rewrite the 3-way base snapshot under ``.clickup-sync/``;
#   * a human or a cron invoking this tool from a plain shell.
#
# So the critical section is the whole transaction -- acquire, edit, sync,
# release -- not the individual write. Holding a lock only across the edit
# leaves the sync outside the protected span, which is precisely the gap this
# closes.
#
# Interop, not a second mechanism
# -------------------------------
# The lock file, its location, its JSON shape and its TTL are deliberately
# identical to the hook's, and under Claude Code this tool uses THE SAME
# IDENTITY the hook does, so a session's edit-then-sync is one continuous hold
# of one lock rather than two mechanisms taking turns on a file the other is
# writing. Verified 2026-08-21: the hook writes ``session_id`` from its payload,
# and ``$CLAUDE_CODE_SESSION_ID`` in a Bash tool call holds that same UUID.
# (``$CLAUDE_SESSION_ID`` -- no ``CODE`` -- is a different, empty variable.)
#
# Consequence, and it is the important one: when this tool finds a fresh lock
# already held by its OWN session, it adopts it instead of blocking, and on
# release it hands it BACK rather than deleting it -- the session may still be
# mid-edit-burst, and the hook must keep recognising the lock afterwards.
#
# Two locks, always taken in the same order
# -----------------------------------------
# A file lock protects the YAML; a list lock protects the ClickUp list. They are
# not the same thing -- two YAML files pointed at one list is a real
# configuration, and file-level locking does nothing for it -- so both are
# taken, file first and then list, a fixed global order that cannot deadlock.
#
# Crash safety comes from the TTL: a process killed mid-sync leaves a lock that
# expires, never one held forever. A long sync refreshes its locks on a
# background heartbeat so a slow run does not go stale under its own feet.

LOCK_TTL_MS = 5 * 60 * 1000            # must match the hook's LOCK_TTL_MS
LOCK_POLL_SECONDS = 2.0
LOCK_WAIT_DEFAULT_SECONDS = 120.0      # "the wait shouldn't be more than a minute"
LOCK_HEARTBEAT_SECONDS = 60.0
LOCK_EXIT_CODE = 3                     # distinct from 1, so a caller can tell "busy" from "failed"


class LockBusy(RuntimeError):
    """Another owner holds a fresh lock and did not release it in time."""


def lock_owner_id() -> str:
    """Identity to claim locks under.

    Order matters:

    1. ``CLICKUP_LOCK_OWNER`` -- explicit override, used by tests and by anyone
       who needs to pin an identity (a cron job that wants a stable name).
    2. ``CLAUDE_CODE_SESSION_ID`` -- under Claude Code this is exactly what the
       hook writes, which is what makes edit-then-sync a single hold.
    3. A unique per-process id for a plain shell. Deliberately unique rather
       than something like ``os.getpid()`` alone: a stable-looking id risks
       colliding with a real session's lock, and a colliding id would let this
       process silently steal a lock instead of waiting for it.
    """
    explicit = os.environ.get("CLICKUP_LOCK_OWNER", "").strip()
    if explicit:
        return explicit
    session = os.environ.get("CLAUDE_CODE_SESSION_ID", "").strip()
    if session:
        return session
    return f"shell-{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:8]}"


def lock_path_for_yaml(yaml_path: str) -> Path:
    """``<dir>/.<stem>.lock`` -- for ``project-tasks.yaml`` this is exactly the
    ``.project-tasks.lock`` the hook guards, and it generalises to any other
    task file without inventing a second convention."""
    p = Path(yaml_path).resolve()
    return p.parent / f".{p.stem}.lock"


def lock_path_for_list(list_id: str) -> Path:
    """Machine-global, because the whole point is to catch two *different* YAML
    files aimed at one ClickUp list. A path next to either file would not."""
    cache = os.environ.get("XDG_CACHE_HOME", "").strip() or str(Path.home() / ".cache")
    return Path(cache) / "clickup-yaml-sync" / f"list-{list_id}.lock"


def _read_lock(path: Path) -> Optional[dict]:
    """Absent, unreadable or malformed all mean 'free' -- same as the hook. An
    advisory lock that fails closed on a corrupt file would wedge the tool."""
    try:
        with open(path) as f:
            doc = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None
    return doc if isinstance(doc, dict) and doc.get("session_id") else None


def _write_lock(path: Path, owner: str) -> None:
    """Atomic, so a concurrent reader never sees a half-written lock and
    concludes the file is free. (The hook writes in place; that residual race is
    the hook's, and is noted in the README.)"""
    path.parent.mkdir(parents=True, exist_ok=True)
    doc = {"session_id": owner, "ts": int(time.time() * 1000), "pid": os.getpid()}
    tmp = path.with_suffix(path.suffix + f".tmp{os.getpid()}")
    try:
        with open(tmp, "w") as f:
            f.write(json.dumps(doc) + "\n")
        os.replace(tmp, path)
    except OSError:
        try:
            tmp.unlink()
        except OSError:
            pass
        raise


def _lock_age_ms(lock: dict) -> float:
    try:
        return time.time() * 1000 - float(lock.get("ts") or 0)
    except (TypeError, ValueError):
        return float("inf")


class AdvisoryLock:
    """One lock file. Not reentrant across processes; adoption of our own
    session's lock is what stands in for reentrancy here."""

    def __init__(self, path: Path, owner: str, label: str) -> None:
        self.path = path
        self.owner = owner
        self.label = label
        self.held = False
        # True when the lock already existed under OUR identity -- i.e. our own
        # Claude session took it while editing. We must hand it back on release,
        # not delete it, or we cut short a hold the session is still relying on.
        self.inherited = False

    def acquire(self, wait_seconds: float) -> None:
        deadline = time.time() + max(0.0, wait_seconds)
        announced = False
        while True:
            lock = _read_lock(self.path)
            if lock is None:
                break
            same_owner = str(lock.get("session_id")) == self.owner
            age = _lock_age_ms(lock)
            if same_owner:
                self.inherited = True
                log.info(
                    f"Lock ({self.label}) already held by this session "
                    f"({self.owner[:8]}) -- continuing the same hold."
                )
                break
            if age >= LOCK_TTL_MS:
                log.warning(
                    f"Taking over a stale lock ({self.label}) from "
                    f"{str(lock.get('session_id'))[:8]} -- last touched "
                    f"{age / 1000:.0f}s ago, TTL {LOCK_TTL_MS / 1000:.0f}s. "
                    f"{self.path}"
                )
                break
            if time.time() >= deadline:
                raise LockBusy(
                    f"{self.label} is locked by {str(lock.get('session_id'))[:8]} "
                    f"(held {age / 1000:.0f}s, expires in "
                    f"{max(0.0, (LOCK_TTL_MS - age) / 1000):.0f}s).\n"
                    f"  lock file: {self.path}\n"
                    f"  Waited {wait_seconds:.0f}s and gave up. Nothing was "
                    f"changed -- rerun when that holder is done.\n"
                    f"  Raise the wait with --lock-timeout SECONDS. If that "
                    f"holder crashed, the lock self-expires; --no-lock bypasses "
                    f"this check entirely (see README)."
                )
            if not announced:
                # Visible, once, on stderr as well as the log: a sync that
                # appears to hang must say why it is waiting.
                msg = (
                    f"Waiting for {self.label} lock held by "
                    f"{str(lock.get('session_id'))[:8]} (held {age / 1000:.0f}s) "
                    f"-- up to {wait_seconds:.0f}s..."
                )
                print(msg, file=sys.stderr, flush=True)
                log.info(msg)
                announced = True
            time.sleep(LOCK_POLL_SECONDS)

        _write_lock(self.path, self.owner)
        self.held = True
        log.info(f"Lock acquired ({self.label}) by {self.owner[:8]}: {self.path}")

    def refresh(self) -> None:
        if self.held:
            try:
                _write_lock(self.path, self.owner)
            except OSError as e:  # pragma: no cover - defensive
                log.warning(f"Could not refresh {self.label} lock: {e}")

    def release(self) -> None:
        if not self.held:
            return
        self.held = False
        current = _read_lock(self.path)
        if current is not None and str(current.get("session_id")) != self.owner:
            # Someone took over (our TTL lapsed). Not ours to delete.
            log.warning(
                f"Not releasing {self.label} lock: it now belongs to "
                f"{str(current.get('session_id'))[:8]}."
            )
            return
        if self.inherited:
            # Hand it back to the session that was already holding it, with a
            # fresh timestamp so the sync we just ran counts as activity.
            try:
                _write_lock(self.path, self.owner)
            except OSError:  # pragma: no cover - defensive
                pass
            log.info(f"Lock ({self.label}) returned to this session's own hold.")
            return
        try:
            self.path.unlink()
            log.info(f"Lock released ({self.label}): {self.path}")
        except FileNotFoundError:
            pass
        except OSError as e:  # pragma: no cover - defensive
            log.warning(f"Could not remove {self.label} lock {self.path}: {e}")


class SyncLock:
    """The file lock and (when a list id is configured) the list lock, held
    together for the whole run, refreshed on a heartbeat, released in reverse.

    Use as a context manager. ``acquire`` raises :class:`LockBusy`; the caller
    turns that into a loud, non-zero exit. It must never degrade into skipping
    the work quietly -- a sync that silently does nothing is the failure that
    hides for days.
    """

    def __init__(
        self,
        yaml_path: str,
        list_id: Optional[str],
        *,
        wait_seconds: float = LOCK_WAIT_DEFAULT_SECONDS,
        owner: Optional[str] = None,
    ) -> None:
        self.owner = owner or lock_owner_id()
        self.wait_seconds = wait_seconds
        self.locks = [AdvisoryLock(lock_path_for_yaml(yaml_path), self.owner, "YAML file")]
        if list_id:
            self.locks.append(
                AdvisoryLock(lock_path_for_list(str(list_id)), self.owner, f"ClickUp list {list_id}")
            )
        self._stop = threading.Event()
        self._heartbeat: Optional[threading.Thread] = None

    def __enter__(self) -> "SyncLock":
        acquired: list[AdvisoryLock] = []
        try:
            for lock in self.locks:  # fixed order: file, then list
                lock.acquire(self.wait_seconds)
                acquired.append(lock)
        except BaseException:
            for lock in reversed(acquired):
                lock.release()
            raise
        self._heartbeat = threading.Thread(target=self._beat, daemon=True)
        self._heartbeat.start()
        return self

    def __exit__(self, *exc: Any) -> None:
        self._stop.set()
        if self._heartbeat is not None:
            self._heartbeat.join(timeout=LOCK_POLL_SECONDS)
        for lock in reversed(self.locks):
            lock.release()

    def _beat(self) -> None:
        # A big board can take longer than the TTL. Without this, a slow sync
        # lets its own lock go stale and another session walks in behind it.
        while not self._stop.wait(LOCK_HEARTBEAT_SECONDS):
            for lock in self.locks:
                lock.refresh()


# ---------------------------------------------------------------------------
# 3-way base snapshot persistence (sidecar JSON per list)
# ---------------------------------------------------------------------------
#
# Stored next to the project YAML at .clickup-sync/base-<list_id>.json. Holds
# the comparable scalar fields of every story with a clickup_id, as of the
# last successful reconcile. Written by push/pull/sync; read by sync to drive
# the 3-way merge. Absent file => sync falls back to 2-way (and writes one).


class BaseSnapshotCorrupt(Exception):
    """A base snapshot file exists but cannot be parsed. Distinct from 'absent'
    so the caller can refuse to silently degrade to interactive 2-way."""


def base_snapshot_path(yaml_path: str, list_id: str) -> Path:
    return Path(yaml_path).resolve().parent / ".clickup-sync" / f"base-{list_id}.json"


def load_base_snapshot(path: Path) -> dict:
    """Return {clickup_id: {field: value}}.

    Absent file -> {} (a legitimate first run; caller falls back to 2-way).
    Present-but-unparseable -> raise BaseSnapshotCorrupt, so the caller does NOT
    silently lose the 3-way safety model (M3).
    """
    if not path.exists():
        return {}
    try:
        with open(path) as f:
            doc = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        raise BaseSnapshotCorrupt(f"{path}: {e}") from e
    tasks = doc.get("tasks", {})
    if not isinstance(tasks, dict):
        raise BaseSnapshotCorrupt(f"{path}: 'tasks' is not a mapping")
    return tasks


def load_base_managed_tags(path: Path) -> set[str]:
    """Return the managed-tag universe recorded in the base snapshot (lowercased).

    Empty set when the file is absent, unparseable, or predates the
    ``managed_tags`` key — so a first run or an old base simply falls back to the
    prior additive-only tag behavior instead of erroring.
    """
    if not path.exists():
        return set()
    try:
        with open(path) as f:
            doc = json.load(f)
    except (json.JSONDecodeError, OSError):
        return set()
    return {str(t).lower() for t in (doc.get("managed_tags") or [])}


def build_base_from_yaml(data: dict, status_map: dict) -> dict:
    """Comparable scalar snapshot for every story that has a clickup_id."""
    tasks: dict = {}
    for epic in data.get("epics", []):
        for story in epic.get("stories", []):
            cid = story.get("clickup_id")
            if cid:
                tasks[cid] = comparable_local(story, status_map)
    return tasks


def save_base_snapshot(path: Path, data: dict, status_map: dict) -> None:
    """Write the base snapshot from the (post-reconcile) YAML state, atomically."""
    tasks = build_base_from_yaml(data, status_map)
    doc = {
        "version": 1,
        "saved_at": datetime.now(timezone.utc).isoformat(),
        # Record the tag universe this tool managed as of this snapshot, so the
        # next sync can remove an epic/tag that was dropped from the YAML since
        # (otherwise dropped managed tags accumulate — they're not in the *current*
        # universe, so they'd look like untouched UI tags and be preserved).
        "managed_tags": sorted(_collect_managed_tag_universe(data)),
        "tasks": tasks,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        with open(tmp, "w") as f:
            json.dump(doc, f, indent=2, sort_keys=True)
        os.replace(tmp, path)
    except Exception:
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass
        raise
    log.info(f"Base snapshot saved to {path} ({len(tasks)} tasks)")


# ---------------------------------------------------------------------------
# ClickUp API client
# ---------------------------------------------------------------------------

CLICKUP_BASE = "https://api.clickup.com/api/v2"
RATE_LIMIT_SLEEP = 0.5


class ClickUpAPIError(RuntimeError):
    """A ClickUp API call that failed, carrying the response body.

    ``urllib``'s ``HTTPError`` stringifies to just ``HTTP Error 403: Forbidden``,
    and by the time a caller sees it the body has already been consumed reading
    it for the log. So every ``except Exception as e: log.warning(f"...: {e}")``
    in this file printed the status and threw away the only part that says WHY
    -- ClickUp's ``err`` message and its ``ECODE``. The real reason then existed
    only on a separate ERROR line, correlated with the failure by adjacency.
    """

    def __init__(self, status: int, body: str, method: str, url: str) -> None:
        self.status = status
        self.body = body
        self.method = method
        self.url = url
        self.ecode: Optional[str] = None
        self.err: Optional[str] = None
        try:
            doc = json.loads(body)
            if isinstance(doc, dict):
                self.ecode = doc.get("ECODE")
                self.err = doc.get("err")
        except (json.JSONDecodeError, TypeError):
            pass
        detail = self.err or (body[:200] if body else "(no body)")
        code = f" [{self.ecode}]" if self.ecode else ""
        super().__init__(f"HTTP {status}{code}: {detail}")


def _api_request(
    method: str,
    url: str,
    token: str,
    data: Optional[dict] = None,
    retries: int = 1,
) -> dict:
    """Make an HTTP request to ClickUp API with rate limiting and retry."""
    headers = {
        "Content-Type": "application/json",
        "Authorization": token,
    }
    body = json.dumps(data).encode("utf-8") if data else None
    req = urllib.request.Request(url, data=body, headers=headers, method=method)

    for attempt in range(retries + 1):
        try:
            time.sleep(RATE_LIMIT_SLEEP)
            with urllib.request.urlopen(req, timeout=30) as resp:
                resp_body = resp.read().decode("utf-8")
                if resp_body:
                    return json.loads(resp_body)
                return {}
        except urllib.error.HTTPError as e:
            resp_body = e.read().decode("utf-8", errors="replace")
            if e.code == 429 and attempt < retries:
                wait = 5
                log.warning(f"Rate limited (429), waiting {wait}s before retry...")
                time.sleep(wait)
                continue
            log.error(f"HTTP {e.code}: {resp_body}")
            raise ClickUpAPIError(e.code, resp_body, method, url) from e
        except urllib.error.URLError as e:
            log.error(f"URL error: {e.reason}")
            raise


def clickup_create_task(token: str, list_id: str, task_data: dict) -> dict:
    url = f"{CLICKUP_BASE}/list/{list_id}/task"
    return _api_request("POST", url, token, task_data)


def clickup_get_task(token: str, task_id: str) -> dict:
    url = f"{CLICKUP_BASE}/task/{task_id}?include_markdown_description=true"
    return _api_request("GET", url, token)


def clickup_update_task(token: str, task_id: str, task_data: dict) -> dict:
    url = f"{CLICKUP_BASE}/task/{task_id}"
    return _api_request("PUT", url, token, task_data)


def clickup_add_tag(token: str, task_id: str, tag_name: str) -> dict:
    url = f"{CLICKUP_BASE}/task/{task_id}/tag/{urllib.parse.quote(tag_name)}"
    return _api_request("POST", url, token)


def clickup_remove_tag(token: str, task_id: str, tag_name: str) -> dict:
    url = f"{CLICKUP_BASE}/task/{task_id}/tag/{urllib.parse.quote(tag_name)}"
    return _api_request("DELETE", url, token)


def clickup_add_dependency(token: str, task_id: str, depends_on: str) -> dict:
    """Add a waiting-on dependency: ``task_id`` waits on ``depends_on``."""
    url = f"{CLICKUP_BASE}/task/{task_id}/dependency"
    return _api_request("POST", url, token, {"depends_on": depends_on})


def clickup_remove_dependency(token: str, task_id: str, depends_on: str) -> dict:
    """Remove a waiting-on dependency between ``task_id`` and ``depends_on``."""
    url = (
        f"{CLICKUP_BASE}/task/{task_id}/dependency"
        f"?depends_on={urllib.parse.quote(depends_on)}"
    )
    return _api_request("DELETE", url, token)


def clickup_add_link(token: str, task_id: str, links_to: str) -> dict:
    """Add a non-blocking link ("linked task") between two tasks.

    The link is symmetric — ClickUp records it on both endpoints.
    """
    url = f"{CLICKUP_BASE}/task/{task_id}/link/{urllib.parse.quote(links_to)}"
    return _api_request("POST", url, token)


def clickup_remove_link(token: str, task_id: str, links_to: str) -> dict:
    """Remove a non-blocking link between ``task_id`` and ``links_to``."""
    url = f"{CLICKUP_BASE}/task/{task_id}/link/{urllib.parse.quote(links_to)}"
    return _api_request("DELETE", url, token)


def clickup_set_custom_field(
    token: str, task_id: str, field_id: str, value: Any
) -> dict:
    """Set a custom field value on a task.

    For dropdown fields, `value` is the option UUID string. ClickUp accepts
    the same endpoint for checkbox, text, number, etc. — caller picks the
    correct value shape.
    """
    url = f"{CLICKUP_BASE}/task/{task_id}/field/{field_id}"
    return _api_request("POST", url, token, {"value": value})


def clickup_get_list_members(token: str, list_id: str) -> list[dict]:
    """Fetch the members who have access to a list.

    Returns a list of ``{id, username, email, ...}`` dicts. Used to resolve
    YAML assignee strings (emails/usernames) to the numeric ClickUp user ids
    the task API expects.
    """
    url = f"{CLICKUP_BASE}/list/{list_id}/member"
    resp = _api_request("GET", url, token)
    return resp.get("members", [])


def clickup_list_tasks(token: str, list_id: str, page: int = 0) -> list[dict]:
    """Fetch all tasks from a ClickUp list (with pagination and subtasks)."""
    all_tasks: list[dict] = []
    while True:
        url = (
            f"{CLICKUP_BASE}/list/{list_id}/task"
            f"?subtasks=true&include_closed=true"
            f"&include_markdown_description=true&page={page}"
        )
        resp = _api_request("GET", url, token)
        tasks = resp.get("tasks", [])
        if not tasks:
            break
        all_tasks.extend(tasks)
        if resp.get("last_page", True):
            break
        page += 1
    return all_tasks


# ---------------------------------------------------------------------------
# OpenAI merge client
# ---------------------------------------------------------------------------


def openai_merge(
    api_key: str,
    yaml_value: str,
    clickup_value: str,
    task_name: str,
    field_name: str,
) -> str:
    """Ask GPT-4o-mini to merge two conflicting field values."""
    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    prompt = (
        f"You are merging two versions of a task field. "
        f"Produce the best merged result that preserves information from both sides.\n\n"
        f"Task: {task_name}\n"
        f"Field: {field_name}\n"
        f"Local (YAML) value:\n{yaml_value}\n\n"
        f"Remote (ClickUp) value:\n{clickup_value}\n\n"
        f"Return ONLY the merged value as a JSON object: {{\"merged\": \"...\"}}"
    )
    body = json.dumps({
        "model": "gpt-4o-mini",
        "messages": [{"role": "user", "content": prompt}],
        "response_format": {"type": "json_object"},
        "temperature": 0.2,
    }).encode("utf-8")

    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=30) as resp:
        result = json.loads(resp.read().decode("utf-8"))

    content = result["choices"][0]["message"]["content"]
    parsed = json.loads(content)
    return parsed.get("merged", content)


# ---------------------------------------------------------------------------
# Status mapping helpers
# ---------------------------------------------------------------------------


def yaml_status_to_clickup(status: str, status_map: dict) -> str:
    """Map a YAML status key to a ClickUp status name."""
    return status_map.get(status, status)


def clickup_status_to_yaml(clickup_status: str, status_map: dict) -> str:
    """Map a ClickUp status name back to a YAML status key."""
    reverse = {v.lower(): k for k, v in status_map.items()}
    return reverse.get(clickup_status.lower(), clickup_status.lower().replace(" ", "_"))


def priority_to_clickup(priority: Optional[int]) -> Optional[int]:
    """ClickUp priority: 1=urgent, 2=high, 3=normal, 4=low. Same mapping."""
    return priority


def clickup_priority_to_yaml(priority_obj: Optional[dict]) -> Optional[int]:
    """Extract priority int from ClickUp priority object."""
    if priority_obj and "id" in priority_obj:
        return int(priority_obj["id"])
    return None


def _norm_yaml_date(v: Any) -> Optional[str]:
    """Normalize a YAML date value to 'YYYY-MM-DD'. YAML auto-parses an
    unquoted 2026-06-20 into a date object, so accept str / date / datetime."""
    if v in (None, ""):
        return None
    if isinstance(v, datetime):
        return v.date().isoformat()
    if isinstance(v, date):
        return v.isoformat()
    return str(v).strip()[:10]


def yaml_date_to_clickup_ms(v: Any) -> Optional[int]:
    """'YYYY-MM-DD' -> epoch ms at 12:00 UTC. Noon (not midnight) keeps the
    calendar day stable when ClickUp normalizes a date-only value into the
    workspace timezone — verified in sandbox: midnight drifts a day, noon does
    not. Paired with due_date_time=false so ClickUp displays date-only.

    Stable for workspaces UTC-12..UTC+11. YAML-originated dates are exact in
    every tz; only a date set in the ClickUp UI of a UTC+12-or-east workspace
    may pull back one calendar day earlier (it does NOT oscillate)."""
    d = _norm_yaml_date(v)
    if not d:
        return None
    dt = datetime.strptime(d, "%Y-%m-%d").replace(hour=12, tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def clickup_ms_to_yaml_date(ms: Any) -> Optional[str]:
    """ClickUp epoch-ms (str/int/None) -> 'YYYY-MM-DD' (UTC calendar date).
    Comparing at date granularity absorbs ClickUp's sub-day time nudge so the
    value round-trips without a phantom diff."""
    if ms in (None, ""):
        return None
    return datetime.fromtimestamp(int(ms) / 1000, timezone.utc).date().isoformat()


# ---------------------------------------------------------------------------
# Diff engine
# ---------------------------------------------------------------------------

SYNCED_FIELDS = ["name", "status", "description", "priority", "milestone",
                 "due_date", "start_date"]

# ClickUp custom_item_id values
CUSTOM_ITEM_TASK = 0
CUSTOM_ITEM_MILESTONE = 1


def normalize_description(desc: Optional[str]) -> str:
    """Normalize description for comparison (strip trailing whitespace/newlines)."""
    if not desc:
        return ""
    return desc.strip()


# ClickUp escapes ASCII-punctuation markdown metacharacters in
# ``markdown_description`` (e.g. ``Article_type`` -> ``Article\_type``). We
# author/store the unescaped form in YAML, so reads must reverse it or every
# description with an underscore/asterisk would look like a spurious diff.
_MD_ESCAPE_RE = re.compile(r"\\([!-/:-@\[-`{-~])")

# ClickUp also auto-linkifies bare URLs/emails/domains in markdown, e.g.
# ``alice@example.com`` -> ``[alice@example.com](mailto:alice@example.com)``.
# We collapse such *self-referential* links (label == url, ignoring the
# mailto:/http(s):// scheme) back to bare text. A genuine mention or labeled
# link — where the label differs from the url — is left untouched.
_MD_AUTOLINK_RE = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")
_URL_SCHEMES = ("mailto:", "https://", "http://")


def _unescape_markdown(text: str) -> str:
    """Drop CommonMark backslash-escapes before ASCII punctuation.

    Leaves real syntax intact — a ``[label](url)`` link has no backslashes —
    while normalizing ``\\_`` -> ``_`` etc. back to the authored text.
    """
    return _MD_ESCAPE_RE.sub(r"\1", text)


def _strip_url_scheme(s: str) -> str:
    for pre in _URL_SCHEMES:
        if s.startswith(pre):
            return s[len(pre):]
    return s


def _collapse_self_links(text: str) -> str:
    """Reverse ClickUp's auto-linkification of bare URLs/emails/domains.

    Collapses ``[X](X)`` (modulo url scheme) to ``X``; preserves real links
    where the label and url differ (task mentions, labeled links).
    """
    def repl(m: "re.Match") -> str:
        label, url = m.group(1), m.group(2)
        if label == url or _strip_url_scheme(label) == _strip_url_scheme(url):
            return label
        return m.group(0)
    return _MD_AUTOLINK_RE.sub(repl, text)


def _cu_description(cu_task: dict) -> str:
    """A ClickUp task's description, preferring the markdown form.

    ClickUp's plain ``description``/``text_content`` flattens an embedded
    task-mention tile to whitespace (losing the reference), whereas
    ``markdown_description`` preserves it as a ``[label](url)`` link — and
    round-trips through ``markdown_content`` on write. The markdown form
    backslash-escapes punctuation, so we unescape it back to the authored text
    for a clean compare/pull. Falls back to the plain field when the markdown
    form wasn't requested or returned (e.g. an old cached task dict).
    """
    md = cu_task.get("markdown_description")
    if md is not None:
        return _collapse_self_links(_unescape_markdown(md))
    return cu_task.get("description") or ""


def _meta_prefix(story: dict) -> str:
    """A one-line 'Points/Milestone/Sprint' header, so those YAML fields are
    visible on the ClickUp card even without the Sprint-Points ClickApp.
    The structured fields stay source-of-truth; this is display only."""
    bits: list[str] = []
    if story.get("points"):
        bits.append(f"Points: {story['points']}")
    ms = story.get("milestone_label")
    if isinstance(ms, str) and ms.strip():
        bits.append(f"Milestone: {ms.strip()}")
    sp = story.get("sprint_target")
    if isinstance(sp, int) and sp > 0:
        bits.append(f"Sprint: s{sp}")
    return " · ".join(bits)


def description_with_meta(story: dict) -> str:
    """Story description with the meta header prepended (generated, not stored).

    Used identically by build_task_body (push) and compare_task (diff), so the
    header round-trips without ever showing as a spurious description diff.
    """
    meta = _meta_prefix(story)
    body = (story.get("description") or "").strip()
    if not meta:
        return body
    return f"{meta}\n\n{body}".strip() if body else meta


def strip_meta_prefix(raw: Optional[str], story: dict) -> str:
    """Inverse of description_with_meta: remove the leading Points/Milestone/
    Sprint header this tool would have prepended, so a description pulled from
    ClickUp doesn't re-accumulate the header on the next push (the H1 bug).

    Anchored to the EXACT header generated for *this* story — never a loose
    regex — so a user's own first line is left untouched unless it byte-matches
    the header we would have written.
    """
    raw = raw or ""
    meta = _meta_prefix(story)
    if not meta or not raw:
        return raw
    if raw == meta:
        return ""
    prefix = f"{meta}\n\n"
    if raw.startswith(prefix):
        return raw[len(prefix):]
    return raw


def compare_task(
    yaml_task: dict,
    clickup_task: dict,
    status_map: dict,
    is_epic: bool = False,
) -> list[dict]:
    """Compare a YAML task/story against its ClickUp counterpart.
    Returns list of {field, yaml_value, clickup_value} dicts for differences.
    """
    diffs: list[dict] = []

    # Name
    yaml_name = yaml_task.get("name", "")
    cu_name = clickup_task.get("name", "")
    if yaml_name != cu_name:
        diffs.append({"field": "name", "yaml": yaml_name, "clickup": cu_name})

    # Status
    yaml_status = yaml_status_to_clickup(yaml_task.get("status", ""), status_map)
    cu_status = clickup_task.get("status", {}).get("status", "").lower()
    if yaml_status.lower() != cu_status:
        diffs.append({"field": "status", "yaml": yaml_status, "clickup": cu_status})

    # Description
    yaml_desc = normalize_description(description_with_meta(yaml_task))
    cu_desc = normalize_description(_cu_description(clickup_task))
    if yaml_desc != cu_desc:
        diffs.append({"field": "description", "yaml": yaml_desc, "clickup": cu_desc})

    # Priority
    yaml_priority = yaml_task.get("priority")
    cu_priority = clickup_priority_to_yaml(clickup_task.get("priority"))
    if yaml_priority != cu_priority:
        diffs.append({"field": "priority", "yaml": yaml_priority, "clickup": cu_priority})

    # Milestone
    yaml_milestone = bool(yaml_task.get("milestone"))
    cu_milestone = _is_clickup_milestone(clickup_task)
    if yaml_milestone != cu_milestone:
        diffs.append({"field": "milestone", "yaml": yaml_milestone, "clickup": cu_milestone})

    # Dates — compared at YYYY-MM-DD granularity (see clickup_ms_to_yaml_date)
    for fld in ("due_date", "start_date"):
        yv = _norm_yaml_date(yaml_task.get(fld))
        cv = clickup_ms_to_yaml_date(clickup_task.get(fld))
        if yv != cv:
            diffs.append({"field": fld, "yaml": yv, "clickup": cv})

    return diffs


def _is_clickup_milestone(cu_task: dict) -> bool:
    """Check if a ClickUp task is a milestone (custom_item_id == 1)."""
    return cu_task.get("custom_item_id") == CUSTOM_ITEM_MILESTONE


# ---------------------------------------------------------------------------
# 3-way merge engine (base snapshot)
# ---------------------------------------------------------------------------
#
# The 2-way diff (YAML vs ClickUp) cannot tell WHICH side changed a field — a
# difference could be a local edit or a remote edit, so every difference looks
# like a conflict. A persisted *base* snapshot (the agreed field values as of
# the last successful reconcile) makes it a 3-way merge: a field that moved on
# only one side auto-resolves toward that side; a field that moved on BOTH
# sides to different values is a true conflict that no policy can resolve
# correctly without human judgement.
#
# Scope (v1): the scalar SYNCED_FIELDS only (name/status/description/priority/
# milestone). Assignees and tags keep their existing set-level reconcile.

_MISSING = object()


def comparable_local(story: dict, status_map: dict) -> dict:
    """Scalar fields of a YAML story in the same comparable form compare_task uses."""
    return {
        "name": story.get("name", ""),
        "status": yaml_status_to_clickup(story.get("status", ""), status_map).lower(),
        "description": normalize_description(description_with_meta(story)),
        "priority": story.get("priority"),
        "milestone": bool(story.get("milestone")),
        "due_date": _norm_yaml_date(story.get("due_date")),
        "start_date": _norm_yaml_date(story.get("start_date")),
    }


def comparable_remote(cu_task: dict, status_map: dict) -> dict:
    """Scalar fields of a ClickUp task in the same comparable form compare_task uses."""
    return {
        "name": cu_task.get("name", ""),
        "status": (cu_task.get("status", {}).get("status", "") or "").lower(),
        "description": normalize_description(_cu_description(cu_task)),
        "priority": clickup_priority_to_yaml(cu_task.get("priority")),
        "milestone": _is_clickup_milestone(cu_task),
        "due_date": clickup_ms_to_yaml_date(cu_task.get("due_date")),
        "start_date": clickup_ms_to_yaml_date(cu_task.get("start_date")),
    }


def classify_3way(base_v: Any, local_v: Any, remote_v: Any) -> str:
    """Classify one field's 3-way state.

    Returns:
      'none'     — local and remote already agree (nothing to do)
      'push'     — only the local side changed since base -> write to ClickUp
      'pull'     — only the remote side changed since base -> write to YAML
      'conflict' — both sides changed to different values, or base is unknown
    """
    if local_v == remote_v:
        return "none"
    changed_local = local_v != base_v
    changed_remote = remote_v != base_v
    if changed_local and not changed_remote:
        return "push"
    if changed_remote and not changed_local:
        return "pull"
    # Both moved (to different values, since local != remote), or base is
    # missing for this field -> cannot safely auto-resolve.
    return "conflict"


def three_way_plan(
    base_fields: dict,
    story: dict,
    cu_task: dict,
    status_map: dict,
    fields: Optional[list] = None,
) -> dict:
    """Per-field 3-way classification for one matched task.

    Returns {field: action} only for fields whose local/remote differ.
    ``base_fields`` is the snapshot for this task's clickup_id (may be ``{}``).
    """
    fields = fields or SYNCED_FIELDS
    loc = comparable_local(story, status_map)
    rem = comparable_remote(cu_task, status_map)
    plan: dict = {}
    for f in fields:
        action = classify_3way(base_fields.get(f, _MISSING), loc.get(f), rem.get(f))
        if action != "none":
            plan[f] = action
    return plan


# ---------------------------------------------------------------------------
# Build ClickUp task body from YAML
# ---------------------------------------------------------------------------


def build_task_body(
    yaml_task: dict,
    status_map: dict,
    tags: Optional[list[str]] = None,
    default_priority: Optional[int] = None,
    assignee_ids: Optional[list[int]] = None,
) -> dict:
    """Build a ClickUp API request body from a YAML task/story dict.

    ``assignee_ids`` are added only on the create path — ClickUp's create
    endpoint takes a flat id array, whereas updates use the ``{add, rem}``
    shape reconciled separately in ``_sync_assignees``.
    """
    body: dict[str, Any] = {
        "name": yaml_task["name"],
        "status": yaml_status_to_clickup(yaml_task.get("status", "backlog"), status_map),
        "markdown_content": description_with_meta(yaml_task),
    }
    priority = yaml_task.get("priority") or default_priority
    if priority is not None:
        body["priority"] = priority
    if yaml_task.get("milestone"):
        body["custom_item_id"] = CUSTOM_ITEM_MILESTONE
    dd = yaml_date_to_clickup_ms(yaml_task.get("due_date"))
    if dd is not None:
        body["due_date"] = dd
        body["due_date_time"] = False
    sd = yaml_date_to_clickup_ms(yaml_task.get("start_date"))
    if sd is not None:
        body["start_date"] = sd
        body["start_date_time"] = False
    if tags:
        body["tags"] = tags
    if assignee_ids:
        body["assignees"] = assignee_ids
    return body


# ---------------------------------------------------------------------------
# Index helpers
# ---------------------------------------------------------------------------


def build_story_id_index(data: dict) -> dict[str, tuple[int, int]]:
    """Build {clickup_id: (epic_idx, story_idx)} for stories only."""
    index: dict[str, tuple[int, int]] = {}
    for ei, epic in enumerate(data.get("epics", [])):
        for si, story in enumerate(epic.get("stories", [])):
            scid = story.get("clickup_id")
            if scid:
                index[scid] = (ei, si)
    return index


def build_epic_name_map(data: dict) -> dict[str, int]:
    """Build {epic_name_lower: epic_index} for placing stories by tag."""
    m: dict[str, int] = {}
    for ei, epic in enumerate(data.get("epics", [])):
        name = epic.get("name", "")
        if name:
            m[name.lower()] = ei
    return m


def _extract_epic_name_from_tags(cu_task: dict, epic_name_map: dict) -> Optional[str]:
    """Find which epic a ClickUp task belongs to by matching tags to epic names."""
    for tag in cu_task.get("tags", []):
        tag_name = tag.get("name", "") if isinstance(tag, dict) else str(tag)
        tag_lower = tag_name.lower()
        if tag_lower in epic_name_map:
            return tag_lower
    return None


def _epic_tag(epic: dict) -> str:
    """Return the ClickUp tag name for an epic — the epic's name."""
    return epic.get("name", f"E{epic.get('number', 0)}")


def _has_tag(cu_task: dict, tag_name: str) -> bool:
    """Check if a ClickUp task has a specific tag (case-insensitive)."""
    for tag in cu_task.get("tags", []):
        name = tag.get("name", "") if isinstance(tag, dict) else str(tag)
        if name.lower() == tag_name.lower():
            return True
    return False


def _current_tag_names(cu_task: dict) -> list[str]:
    """Extract the list of tag names currently on a ClickUp task."""
    names: list[str] = []
    for tag in cu_task.get("tags", []):
        name = tag.get("name", "") if isinstance(tag, dict) else str(tag)
        if name:
            names.append(name)
    return names


def _sync_tags(
    token: str,
    task_id: str,
    cu_task: dict,
    desired_tags: list[str],
    managed_known_tags: Optional[set[str]] = None,
) -> None:
    """Reconcile a ClickUp task's tags to match ``desired_tags`` additively.

    Semantics (changed 2026-06-01 — multi-tag push):

    - All tags in ``desired_tags`` are added if missing.
    - Pre-existing ClickUp tags NOT in ``desired_tags`` are PRESERVED by
      default — we don't strip tags a human added in the ClickUp UI.
    - ``managed_known_tags`` (optional) names the universe of tags this tool
      manages (e.g. every epic name + every milestone slug in the YAML). A
      pre-existing tag that's IN this universe but NOT in ``desired_tags``
      for THIS story IS treated as stale and removed. That's how an epic
      reassignment in YAML removes the old epic tag.
    - The legacy ``E<number>`` epic-pattern tags (E1, E2, …) are always
      stripped — leftover from the pre-multi-tag schema.
    """
    desired_lower = {t.lower() for t in desired_tags}
    managed_lower = {t.lower() for t in (managed_known_tags or set())}

    current_names = _current_tag_names(cu_task)

    # Remove stale tags
    for name in current_names:
        name_lower = name.lower()
        if name_lower in desired_lower:
            continue
        is_legacy_epic = (
            name.upper().startswith("E")
            and len(name) > 1
            and name[1:].isdigit()
        )
        is_managed_stale = name_lower in managed_lower
        if is_legacy_epic or is_managed_stale:
            try:
                clickup_remove_tag(token, task_id, name)
                log.info(f"    Removed stale tag '{name}'")
            except Exception as e:
                log.warning(f"    Failed to remove tag '{name}': {e}")

    # Add desired tags
    current_lower = {n.lower() for n in current_names}
    for tag in desired_tags:
        if tag.lower() in current_lower:
            continue
        try:
            clickup_add_tag(token, task_id, tag)
            log.info(f"    Added tag '{tag}'")
        except Exception as e:
            log.warning(f"    Failed to add tag '{tag}': {e}")


# ---------------------------------------------------------------------------
# Tag + milestone + custom-field resolution
# ---------------------------------------------------------------------------


# `m1`, `m2-system`, `m3-acceptance`. The NUMBER carries the sequence and the
# SLUG carries the meaning. One definition, shared by tag emission and the
# milestone-date lint -- two regexes for one convention would drift, and the
# drift would show up as a lint that quietly stops resolving what push emits.
#
# The number is unbounded on purpose. The field used to be an `M0`-`M3` enum,
# which capped a project at four milestones for no reason a five-milestone SOW
# would accept.
MILESTONE_TAG_RE = re.compile(r"^m(\d+)(?:-([a-z0-9][a-z0-9-]*))?$", re.IGNORECASE)

# Bare slugs kept permanently in the managed-tag universe for backward
# compatibility: removing a `milestone_label: M1` from a story must still strip
# its `m1` tag on the next push even with no base snapshot to compare against.
# Slugged labels rely on the base snapshot's recorded `managed_tags` for that,
# the same way explicit `tags:` entries always have -- see the README.
VALID_MILESTONE_LABELS = ("M0", "M1", "M2", "M3")


def _story_desired_tags(
    story: dict, epic: dict, push_epic_tag: bool = True
) -> list[str]:
    """Compute the desired tag set for one story (multi-tag, additive).

    Order of precedence (deduped, case-insensitive, original case preserved):
      1. epic name (only when ``push_epic_tag`` — set project.push_epic_tag:
         false to let a curated Epic dropdown own the workstream axis instead)
      2. story.tags[] from YAML
      3. lowercase milestone slug from story.milestone_label
         (``M1`` -> ``m1``; ``M1-infrastructure`` -> ``m1-infrastructure``)
      4. lowercase sprint slug from story.sprint_target (1 -> ``s1``)
    """
    out: list[str] = []
    seen: set[str] = set()

    def _add(tag: Optional[str]) -> None:
        if not tag:
            return
        key = tag.lower()
        if key in seen:
            return
        seen.add(key)
        out.append(tag)

    if push_epic_tag:
        _add(_epic_tag(epic))
    for t in story.get("tags") or []:
        if isinstance(t, str):
            _add(t)
    ms = story.get("milestone_label")
    if isinstance(ms, str) and ms.strip():
        _add(ms.strip().lower())
    sprint = story.get("sprint_target")
    if isinstance(sprint, int) and sprint > 0:
        _add(f"s{sprint}")
    return out


def managed_tag_universe_for(data: dict, yaml_path: str, list_id: str) -> set[str]:
    """The full managed-tag universe for a run: what the YAML declares NOW,
    unioned with what this tool managed as of the last base snapshot.

    Both halves are required and the second is the easy one to forget. Without
    it a tag DROPPED from the YAML since the last run is no longer in the
    current universe, so it looks like an untouched UI tag and is preserved
    forever -- the reconcile can only remove what it knows it owns.

    This exists as one function because it previously existed as two call sites
    and only one of them had the union: ``cmd_sync`` did, ``cmd_push`` did not,
    so `push` silently never removed a dropped tag. Both halves were unit-tested;
    the WIRING between them was not, which is exactly how they diverged. One
    function means the next fix cannot land in only one of them.
    """
    universe = _collect_managed_tag_universe(data)
    universe |= load_base_managed_tags(base_snapshot_path(yaml_path, str(list_id)))
    return universe


def _collect_managed_tag_universe(data: dict) -> set[str]:
    """All tags this tool considers under its management — used to decide
    which pre-existing ClickUp tags are stale vs untouched UI additions."""
    universe: set[str] = set()
    for epic in data.get("epics", []) or []:
        ename = epic.get("name")
        if ename:
            universe.add(ename.lower())
        for story in epic.get("stories", []) or []:
            for t in story.get("tags") or []:
                if isinstance(t, str) and t:
                    universe.add(t.lower())
            ms = story.get("milestone_label")
            if isinstance(ms, str) and ms.strip():
                universe.add(ms.strip().lower())
    # Always treat lowercased milestone slugs as managed even if no story
    # currently uses them — so removing a milestone_label from YAML strips
    # the tag on next push.
    for ms in VALID_MILESTONE_LABELS:
        universe.add(ms.lower())
    # Sprint slugs are also always managed — so removing sprint_target from
    # a story strips the s<N> tag on next push. We add the slugs for any
    # sprint number actually referenced in the YAML (either by stories or
    # by the project's sprint registry).
    for epic in data.get("epics", []) or []:
        for story in epic.get("stories", []) or []:
            sprint = story.get("sprint_target")
            if isinstance(sprint, int) and sprint > 0:
                universe.add(f"s{sprint}")
    for sp in data.get("sprints", []) or []:
        n = sp.get("number")
        if isinstance(n, int) and n > 0:
            universe.add(f"s{n}")
    return universe


def _epic_dropdown_value_for(story: dict, epic: dict, project_cfg: dict) -> Optional[tuple[str, str]]:
    """Resolve (field_id, option_id) for the Epic custom dropdown, if any.

    Returns None when the YAML doesn't configure the dropdown, when the epic
    name has no matching option, or when the field id is missing.
    """
    field_id = project_cfg.get("epic_dropdown_field_id")
    options = project_cfg.get("epic_dropdown_options") or {}
    if not field_id or not options:
        return None
    # Story-level override wins; otherwise infer from epic.
    epic_name = story.get("epic_dropdown_value") or epic.get("name")
    if not epic_name:
        return None
    # Case-insensitive option lookup so YAML capitalization is forgiving.
    options_lower = {k.lower(): v for k, v in options.items()}
    option_id = options_lower.get(epic_name.lower())
    if not option_id:
        return None
    return (field_id, option_id)


def _current_custom_field_value(cu_task: dict, field_id: str) -> Optional[str]:
    """Read the raw current value of a custom field off a ClickUp task payload."""
    for f in cu_task.get("custom_fields", []) or []:
        if f.get("id") == field_id:
            return f.get("value")
    return None


def _current_dropdown_option_id(cu_task: dict, field_id: str) -> Optional[str]:
    """Resolve a drop_down field's current value to its option UUID.

    ClickUp's *write* endpoint (``clickup_set_custom_field``) takes the option
    UUID, but a *read* (GET task) returns the selected drop_down value as the
    option's **orderindex integer**, not the UUID — with the id↔orderindex map
    carried in ``type_config.options``. Comparing the raw read value against the
    target UUID therefore never matches, so the dropdown was re-PATCHed on every
    task of every sync (BUG #13). Normalize the read value to the option UUID so
    an already-correct value is recognised and skipped.

    Handles both shapes defensively: some payloads/fixtures already carry the
    UUID directly, in which case it is returned unchanged.
    """
    for f in cu_task.get("custom_fields", []) or []:
        if f.get("id") != field_id:
            continue
        raw = f.get("value")
        if raw is None:
            return None
        options = (f.get("type_config") or {}).get("options") or []
        # Direct UUID match (payload already gives the option id).
        for opt in options:
            if str(opt.get("id")) == str(raw):
                return str(opt.get("id"))
        # Orderindex match (ClickUp's real GET shape: value = orderindex int).
        for opt in options:
            if str(opt.get("orderindex")) == str(raw):
                return str(opt.get("id"))
        # No options metadata to resolve against — compare the raw value as-is.
        return str(raw)
    return None


def _push_epic_dropdown_if_needed(
    token: str,
    task_id: str,
    cu_task: dict,
    story: dict,
    epic: dict,
    project_cfg: dict,
    dry_run: bool,
) -> bool:
    """Push the Epic dropdown option to ClickUp if YAML differs. Returns True
    if a write was attempted (or would have been, in dry-run). Silently noop
    when the YAML doesn't define the dropdown."""
    resolved = _epic_dropdown_value_for(story, epic, project_cfg)
    if not resolved:
        return False
    field_id, option_id = resolved
    current = _current_dropdown_option_id(cu_task, field_id)
    if current == option_id:
        return False
    if dry_run:
        log.info(
            f"    [DRY RUN] Would set Epic dropdown {field_id} -> {option_id}"
        )
        return True
    try:
        clickup_set_custom_field(token, task_id, field_id, option_id)
        log.info(f"    Set Epic dropdown ({field_id}) -> {option_id}")
        return True
    except Exception as e:
        log.warning(f"    Failed to set Epic dropdown: {e}")
        return False


def _all_yaml_story_ids(data: dict) -> set[str]:
    """Collect all clickup_ids from stories across all epics."""
    ids: set[str] = set()
    for epic in data.get("epics", []):
        for story in epic.get("stories", []):
            cid = story.get("clickup_id")
            if cid:
                ids.add(cid)
    return ids


def _create_dedupe_key(name: str, epic_tag: Optional[str]) -> tuple[str, str]:
    """Stable (name, epic-tag) key for matching a YAML story against tasks that
    already exist in ClickUp. Case/whitespace-insensitive."""
    return ((name or "").strip().lower(), (epic_tag or "").strip().lower())


def _build_create_dedupe_index(cu_tasks: list[dict]) -> dict[tuple[str, str], list[str]]:
    """Index existing ClickUp tasks by (normalized name, normalized tag) so a
    create can detect a task that already exists — e.g. an orphan left by a
    prior run whose ``clickup_id`` writeback was lost to a mid-run kill (BUG
    #14). Each task is indexed under every one of its tags, plus a name-only
    bucket (tag="") used when epic tagging is disabled.
    """
    index: dict[tuple[str, str], list[str]] = {}
    for t in cu_tasks:
        name = (t.get("name") or "").strip().lower()
        tags = [
            (tag.get("name", "") if isinstance(tag, dict) else str(tag)).strip().lower()
            for tag in (t.get("tags") or [])
        ]
        keys = {(name, tag) for tag in tags}
        keys.add((name, ""))  # name-only fallback bucket
        for k in keys:
            index.setdefault(k, []).append(t["id"])
    return index


def _match_existing_cu_task(
    dedupe_index: dict[tuple[str, str], list[str]],
    story_name: str,
    epic_tag: Optional[str],
    push_epic_tag: bool,
) -> Optional[str]:
    """Return the id of an existing ClickUp task matching this story, or None.

    Prefers a (name, epic-tag) match; falls back to a name-only match only when
    epic tagging is off (no tag axis to disambiguate on). Returns the first id
    when several match (duplicates already exist) — caller flags the surplus."""
    ids = dedupe_index.get(_create_dedupe_key(story_name, epic_tag))
    if not ids and not push_epic_tag:
        ids = dedupe_index.get(_create_dedupe_key(story_name, ""))
    return ids[0] if ids else None


def _count_yaml_duplicate_ids(data: dict) -> int:
    """Count surplus stories that share a clickup_id (a duplicate-row symptom).

    Returns the number of stories beyond the first for each repeated id — 0 when
    every clickup_id is unique."""
    seen: dict[str, int] = {}
    for epic in data.get("epics", []):
        for story in epic.get("stories", []):
            cid = story.get("clickup_id")
            if cid:
                seen[cid] = seen.get(cid, 0) + 1
    return sum(n - 1 for n in seen.values() if n > 1)


# ---------------------------------------------------------------------------
# Assignee resolution + reconcile (set-valued, modelled on tags)
# ---------------------------------------------------------------------------
#
# YAML stores assignees as human-readable strings (emails preferred, usernames
# accepted). ClickUp's task API speaks numeric user ids, so push resolves
# string -> id via the list's member roster; pull writes the readable string
# back. Like tags, assignees coexist with the ClickUp UI:
#
#   - story has no ``assignees`` key  -> UNMANAGED: ClickUp assignees untouched
#     on push (UI-set assignees are preserved).
#   - story has ``assignees: []``     -> MANAGED, empty: push clears assignees.
#   - story has ``assignees: [...]``  -> MANAGED: ClickUp reconciled to match.
#
# Pull always reads the remote assignees back into YAML (the "someone changed
# it in the ClickUp UI" path).


def _build_assignee_resolver(members: list[dict]) -> dict[str, int]:
    """Map lowercased {email, username, str(id)} -> ClickUp user id."""
    resolver: dict[str, int] = {}
    for m in members or []:
        uid = m.get("id")
        if uid is None:
            continue
        for key in (m.get("email"), m.get("username"), str(uid)):
            if key:
                resolver[str(key).strip().lower()] = int(uid)
    return resolver


def _resolve_assignee_ids(
    assignees: Optional[list], resolver: dict[str, int]
) -> tuple[list[int], list[str]]:
    """Resolve YAML assignee strings to ClickUp ids (deduped, order-preserving).

    Returns ``(ids, unresolved)`` — unresolved strings are names/emails not
    found in the list roster; callers warn and skip them rather than failing.
    """
    ids: list[int] = []
    unresolved: list[str] = []
    seen: set[int] = set()
    for a in assignees or []:
        key = str(a).strip().lower()
        if not key:
            continue
        uid = resolver.get(key)
        if uid is None:
            unresolved.append(str(a))
            continue
        if uid not in seen:
            seen.add(uid)
            ids.append(uid)
    return ids, unresolved


def _cu_assignee_ids(cu_task: dict) -> set[int]:
    """The set of numeric assignee ids currently on a ClickUp task."""
    out: set[int] = set()
    for a in cu_task.get("assignees", []) or []:
        uid = a.get("id") if isinstance(a, dict) else None
        if uid is not None:
            out.add(int(uid))
    return out


def _cu_assignee_keys(cu_task: dict) -> list[str]:
    """Readable, stable assignee identifiers for YAML.

    Prefers email, then username, then the stringified id — sorted
    case-insensitively so the YAML serialisation is stable across pulls.
    """
    keys: list[str] = []
    for a in cu_task.get("assignees", []) or []:
        if not isinstance(a, dict):
            continue
        key = a.get("email") or a.get("username")
        if not key and a.get("id") is not None:
            key = str(a["id"])
        if key:
            keys.append(key)
    return sorted(keys, key=str.lower)


def _assignee_keyset(items: Optional[list]) -> set[str]:
    """Lowercased set of assignee strings, for equality checks."""
    return {str(x).strip().lower() for x in (items or []) if str(x).strip()}


def _sync_assignees(
    token: str,
    task_id: str,
    cu_task: dict,
    story: dict,
    resolver: dict[str, int],
    dry_run: bool = False,
) -> bool:
    """Reconcile a task's ClickUp assignees to match the story's YAML.

    No-op (preserves UI assignees) when the story has no ``assignees`` key.
    Returns True if a change was made — or would be, under ``dry_run``.
    """
    if "assignees" not in story:
        return False
    desired_ids, unresolved = _resolve_assignee_ids(story.get("assignees"), resolver)
    for u in unresolved:
        log.warning(f"    Assignee not found in list members, skipping: '{u}'")
    desired = set(desired_ids)
    current = _cu_assignee_ids(cu_task)
    add = sorted(desired - current)
    rem = sorted(current - desired)
    if not add and not rem:
        return False
    if dry_run:
        log.info(f"    [DRY RUN] Would reconcile assignees: +{add} -{rem}")
        return True
    try:
        clickup_update_task(token, task_id, {"assignees": {"add": add, "rem": rem}})
        log.info(f"    Updated assignees: +{add} -{rem}")
        return True
    except Exception as e:
        log.warning(f"    Failed to update assignees: {e}")
        return False


def _assignees_pull_target(story: dict, cu_task: dict) -> Optional[list[str]]:
    """The assignee list a pull would write, or None if no change is needed.

    Pure (no mutation) so both the dry-run preview and the real pull share one
    change-detection rule. Returns the ClickUp keys when they differ from the
    story; ``None`` when already equal or when both sides are empty (which would
    only litter YAML with ``assignees: []``).
    """
    cu_keys = _cu_assignee_keys(cu_task)
    if not cu_keys and "assignees" not in story:
        return None
    if _assignee_keyset(story.get("assignees")) == _assignee_keyset(cu_keys):
        return None
    return cu_keys


def _pull_assignees(story: dict, cu_task: dict) -> bool:
    """Read ClickUp assignees back into the YAML story. Returns True if changed.

    Writes the ``assignees`` key when ClickUp has any, or when the story
    already declares it (so a remote *removal* becomes an explicit empty list).
    Leaves the key absent when both sides are empty — no YAML litter.
    """
    target = _assignees_pull_target(story, cu_task)
    if target is None:
        return False
    story["assignees"] = target
    return True


def _assignees_differ(story: dict, cu_task: dict, resolver: dict[str, int]) -> bool:
    """Whether the managed YAML assignees diverge from ClickUp's.

    An unmanaged story (no key) "differs" only if ClickUp has assignees to
    capture — so ``sync ask`` can offer to pull them into YAML.
    """
    if "assignees" not in story:
        return bool(_cu_assignee_ids(cu_task))
    desired_ids, _ = _resolve_assignee_ids(story.get("assignees"), resolver)
    return set(desired_ids) != _cu_assignee_ids(cu_task)


# ---------------------------------------------------------------------------
# Dependencies (waiting_on edges) — relationship-reconcile, mirrors assignees
# ---------------------------------------------------------------------------
#
# Semantics (mirror assignees — YAML-authoritative when the key is present):
#   - story has no ``depends_on`` key  -> UNMANAGED: ClickUp edges untouched
#                                          (a UI-added dependency survives).
#   - story has ``depends_on: []``     -> MANAGED, empty: push clears the
#                                          task's waiting_on edges.
#   - story has ``depends_on: [...]``  -> MANAGED: ClickUp reconciled to match
#                                          exactly (add missing, remove extra).
#
# Targets are referenced by ClickUp id (the same id stored in ``clickup_id`` /
# indexed by ``build_story_id_index``). A target not yet created (no clickup_id)
# cannot be referenced — declare the edge once both tasks exist and it resolves
# on the next push. Only the waiting_on direction (ClickUp type 1) is modeled;
# ClickUp maintains the mirrored "blocking" edge automatically.
#
# Scope: reconciled by ``cmd_push``, ``cmd_sync`` and ``cmd_merge`` — all three
# run the same relationship-reconcile second pass (``_reconcile_edges_pass``) —
# read back by ``cmd_pull``, and shown in ``cmd_diff``.
#
# Dependencies remain deliberately excluded from the 3-way/base-snapshot
# machinery: they are NOT base-tracked, so sync APPLIES a declared ``depends_on``
# rather than surfacing it as a conflict. That exclusion is also why sync/merge
# warn before every edge removal (``_warn_edge_removal``) — with no base there is
# nothing to distinguish a deliberate clear from an accidental one.

DEP_TYPE_WAITING_ON = 1  # ClickUp dependency ``type``: task_id waits on depends_on

# Stand-in id for a story the run would create: under ``--dry-run`` no task is
# created, so there is no ClickUp id to name in the edge preview.
PENDING_CREATE_ID = "pending create"


def _cu_waiting_on_ids(cu_task: dict) -> set[str]:
    """The set of task ids THIS task waits on, read from ClickUp's edges.

    ClickUp returns the same edge on both endpoints; we keep only the edges
    where this task is the dependent (``task_id`` == our id) and the type is
    waiting_on, so the mirrored blocking edges on other tasks are ignored.
    """
    self_id = cu_task.get("id")
    out: set[str] = set()
    for dep in cu_task.get("dependencies") or []:
        if (
            dep.get("type") == DEP_TYPE_WAITING_ON
            and dep.get("task_id") == self_id
            and dep.get("depends_on")
        ):
            out.add(str(dep["depends_on"]))
    return out


def _resolve_dependency_ids(
    raw: Optional[list], self_id: Optional[str]
) -> tuple[list[str], list[str]]:
    """Normalize a YAML ``depends_on`` list to ``(desired_ids, unresolved)``.

    Drops blanks, dedups (order-preserving), and refuses a self-dependency.
    Rejected entries come back as ``unresolved`` so the caller can warn.
    """
    desired: list[str] = []
    unresolved: list[str] = []
    seen: set[str] = set()
    for entry in raw or []:
        s = str(entry).strip()
        if not s:
            continue
        if s == self_id:
            unresolved.append(s)
            continue
        if s in seen:
            continue
        seen.add(s)
        desired.append(s)
    return desired, unresolved


# Edge failures are per-edge and easy to lose: a 13-card board can emit 13
# warnings that scroll past while the run reports success. These accumulate them
# so the end of the run can say so plainly -- the same principle as the lock
# failing loudly rather than doing nothing quietly.
_EDGE_FAILURES: list[str] = []
_EDGE_HINT_SHOWN = False

# Printed ONCE per run on the first edge failure. Deliberately phrased as things
# to check rather than as a diagnosis: these are the plausible causes on this
# account, not a reading of the error. The error itself is in the message above
# it, and it is authoritative.
#
# STILL UNEXERCISED AGAINST A REAL FAILURE (as of 2026-08-21). This was written
# expecting the guest-shared BREC space to refuse edge writes -- it did not. The
# Dependencies ClickApp turned out to be ENABLED there, and all 9 dependencies
# and 2 relations applied with no error. So the one live guest-shared client
# board we have does NOT reproduce the case this hint describes, and the
# INSUFFICIENT_ACCESS line in particular remains a plausible cause nobody has
# seen here. That is the reason it is a checklist rather than a diagnosis: if a
# real failure ever arrives, put its actual ECODE in here and narrow it.
EDGE_FAILURE_HINT = (
    "    Edge operations (dependencies / linked tasks) can fail for reasons the "
    "task itself is fine for. Worth checking:\n"
    "      - the Dependencies ClickApp may be disabled on this Space\n"
    "      - a guest-shared Space can refuse reads/writes the token can make "
    "elsewhere (INSUFFICIENT_ACCESS)\n"
    "      - the target task may be in a different Space or already deleted\n"
    "    The API's own message on the line above is authoritative; this is a "
    "checklist, not a diagnosis."
)


def _record_edge_failure(kind: str, label: str, target: str, err: Exception) -> None:
    """Log one edge failure, show the checklist once, and remember it for the
    end-of-run summary."""
    global _EDGE_HINT_SHOWN
    log.warning(f"    Failed to {kind} on {target} for {label}: {err}")
    _EDGE_FAILURES.append(f"{kind}: {label} -> {target}: {err}")
    if not _EDGE_HINT_SHOWN:
        _EDGE_HINT_SHOWN = True
        log.warning(EDGE_FAILURE_HINT)


def report_edge_failures() -> None:
    """Surface accumulated edge failures at the end of a run.

    Without this the run prints its normal success summary and the failures are
    somewhere in the scrollback -- so a board that silently has none of its
    declared dependencies looks like a board that synced fine.
    """
    if not _EDGE_FAILURES:
        return
    print("\n" + "=" * 68)
    print(f"EDGE OPERATIONS FAILED: {len(_EDGE_FAILURES)}")
    print("=" * 68)
    print("The tasks themselves synced. These relationships did NOT get created,")
    print("so the board is missing structure the YAML declares:")
    for f in _EDGE_FAILURES:
        print(f"  - {f}")
    print("\n" + EDGE_FAILURE_HINT.replace("    ", "  "))
    print("=" * 68)


def _edge_label(story: dict, task_id: str) -> str:
    """``'story name' (task id)`` — so an edge log line says which task it means.

    Edge lines are emitted from a second pass that runs after the per-story
    output, so without this they read as bare id lists with no way to tell which
    task a preview belongs to.
    """
    return f"'{story.get('name', '?')}' ({task_id})"


def _warn_edge_removal(kind: str, task_id: str, rem: list[str], dry_run: bool) -> None:
    """Loudly flag a destructive edge removal (mirrors the assignee warning).

    Edges are YAML-authoritative but NOT base-tracked, so a removal here deletes
    a remote edge that may have been added in the ClickUp UI or declared by the
    peer task — surface it instead of silently destroying it. push omits this
    (its overwrite contract is documented); sync/merge pass warn_on_remove=True.
    """
    prefix = "[DRY RUN] Would remove" if dry_run else "Removing"
    log.warning(
        f"    {prefix} {len(rem)} {kind} edge(s) {rem} on {task_id} — YAML is "
        f"authoritative and these aren't base-tracked; a UI-added or peer-declared "
        f"edge will be deleted."
    )


def _sync_dependencies(
    token: str,
    task_id: str,
    cu_task: dict,
    story: dict,
    dry_run: bool = False,
    warn_on_remove: bool = False,
) -> bool:
    """Reconcile a task's waiting_on edges to match the story's ``depends_on``.

    No-op (preserves UI edges) when the story has no ``depends_on`` key.
    Returns True if a change was made — or would be, under ``dry_run``.
    ``warn_on_remove`` loudly flags edge deletions (set by sync/merge, not push).
    """
    if "depends_on" not in story:
        return False
    desired_ids, unresolved = _resolve_dependency_ids(story.get("depends_on"), task_id)
    for u in unresolved:
        log.warning(f"    Invalid depends_on target, skipping: '{u}'")
    desired = set(desired_ids)
    current = _cu_waiting_on_ids(cu_task)
    add = sorted(desired - current)
    rem = sorted(current - desired)
    if not add and not rem:
        return False
    label = _edge_label(story, task_id)
    if rem and warn_on_remove:
        _warn_edge_removal("dependency", task_id, rem, dry_run)
    if dry_run:
        log.info(
            f"    [DRY RUN] Would reconcile dependencies on {label}: +{add} -{rem}"
        )
        return True
    changed = False
    for dep_id in add:
        try:
            clickup_add_dependency(token, task_id, dep_id)
            log.info(f"    Added dependency: {label} waits on {dep_id}")
            changed = True
        except Exception as e:
            _record_edge_failure("add dependency", label, dep_id, e)
    for dep_id in rem:
        try:
            clickup_remove_dependency(token, task_id, dep_id)
            log.info(f"    Removed dependency: {label} no longer waits on {dep_id}")
            changed = True
        except Exception as e:
            _record_edge_failure("remove dependency", label, dep_id, e)
    return changed


def _dependencies_pull_target(story: dict, cu_task: dict) -> Optional[list[str]]:
    """The ``depends_on`` list a pull would write, or None if no change needed.

    Pure (no mutation) so the dry-run preview and the real pull share one rule.
    Returns the sorted ClickUp ids when they differ from the story; ``None``
    when already equal, or when both sides are empty (avoids YAML litter).
    """
    cu_ids = _cu_waiting_on_ids(cu_task)
    if not cu_ids and "depends_on" not in story:
        return None
    current = {
        str(x).strip() for x in (story.get("depends_on") or []) if str(x).strip()
    }
    if current == cu_ids:
        return None
    return sorted(cu_ids)


def _pull_dependencies(story: dict, cu_task: dict) -> bool:
    """Read ClickUp waiting_on edges back into the YAML story. True if changed."""
    target = _dependencies_pull_target(story, cu_task)
    if target is None:
        return False
    story["depends_on"] = target
    return True


# ---------------------------------------------------------------------------
# Relations (non-blocking "linked tasks") — relationship-reconcile
# ---------------------------------------------------------------------------
#
# Semantics mirror ``depends_on`` (and assignees) exactly — YAML-authoritative
# when the ``related`` key is present:
#   - no ``related`` key   -> UNMANAGED: ClickUp links untouched (UI links survive)
#   - ``related: []``      -> MANAGED, empty: clears the task's links
#   - ``related: [id, …]`` -> MANAGED: reconciled to match exactly
#
# Unlike ``depends_on`` (a directional waiting_on edge), a link is NON-directional
# — ClickUp records the same link on both endpoints. We therefore collapse each
# linked_tasks entry to "the other end" (the id that isn't this task), so the
# relation round-trips identically regardless of which task created it.


def _cu_linked_ids(cu_task: dict) -> set[str]:
    """The set of task ids THIS task is linked to, read from ``linked_tasks``.

    Each entry carries two ids (``task_id`` and ``link_id``) for the two
    endpoints; the relation is symmetric, so we keep whichever id is not this
    task's own. Self-referential or malformed entries are ignored.
    """
    self_id = cu_task.get("id")
    out: set[str] = set()
    for link in cu_task.get("linked_tasks") or []:
        a = link.get("task_id")
        b = link.get("link_id")
        other = None
        if a and a != self_id:
            other = a
        elif b and b != self_id:
            other = b
        if other:
            out.add(str(other))
    return out


def _build_declared_relations(data: dict) -> dict[str, set[str]]:
    """Map each relation-managed story's ``clickup_id`` -> the ids it declares in
    ``related``. A story is relation-managed only with BOTH a ``clickup_id`` and a
    ``related`` key. Lets ``_sync_relations`` preserve a symmetric link declared by
    EITHER endpoint (union semantics) instead of deleting the reciprocal that this
    side happens not to list.
    """
    declared: dict[str, set[str]] = {}
    for epic in data.get("epics", []):
        for story in epic.get("stories", []):
            cid = story.get("clickup_id")
            if not cid or "related" not in story:
                continue
            ids = {
                str(x).strip()
                for x in (story.get("related") or [])
                if str(x).strip()
            }
            ids.discard(str(cid))
            declared[str(cid)] = ids
    return declared


def _sync_relations(
    token: str,
    task_id: str,
    cu_task: dict,
    story: dict,
    dry_run: bool = False,
    warn_on_remove: bool = False,
    peer_declared: Optional[dict[str, set[str]]] = None,
) -> bool:
    """Reconcile a task's links to match the story's ``related`` list.

    No-op (preserves UI links) when the story has no ``related`` key.
    Returns True if a change was made — or would be, under ``dry_run``.
    ``warn_on_remove`` loudly flags link deletions (set by sync/merge, not push).

    Links are symmetric, so a link declared by EITHER endpoint is authoritative.
    ``peer_declared`` (id -> that managed peer's declared ``related`` set, from
    ``_build_declared_relations``) lets us keep an edge whose peer still declares
    it rather than deleting it because *this* side didn't list the reciprocal —
    the H1 footgun, where two managed tasks disagree about a mutual link and it
    oscillates every sync. Absent (push, or a bare unit call) behaviour is
    unchanged: ``related`` remains authoritative for this task alone.
    """
    if "related" not in story:
        return False
    # Same normalizer as depends_on: drops blanks, dedups, refuses self-link.
    desired_ids, unresolved = _resolve_dependency_ids(story.get("related"), task_id)
    for u in unresolved:
        log.warning(f"    Invalid related target, skipping: '{u}'")
    desired = set(desired_ids)
    current = _cu_linked_ids(cu_task)
    add = sorted(desired - current)
    rem = sorted(current - desired)
    # Union semantics: never delete a symmetric link a managed peer still
    # declares — that edge is authoritative from the peer's end, so deleting it
    # here only oscillates (the peer re-adds it next sync). Absent the map
    # (push / bare unit call) this is a no-op and legacy behaviour stands.
    if peer_declared:
        rem = [r for r in rem if str(task_id) not in peer_declared.get(r, ())]
    if not add and not rem:
        return False
    label = _edge_label(story, task_id)
    if rem and warn_on_remove:
        _warn_edge_removal("relation", task_id, rem, dry_run)
    if dry_run:
        log.info(f"    [DRY RUN] Would reconcile relations on {label}: +{add} -{rem}")
        return True
    changed = False
    for rid in add:
        try:
            clickup_add_link(token, task_id, rid)
            log.info(f"    Added relation: {label} linked to {rid}")
            changed = True
        except Exception as e:
            _record_edge_failure("add relation", label, rid, e)
    for rid in rem:
        try:
            clickup_remove_link(token, task_id, rid)
            log.info(f"    Removed relation: {label} no longer linked to {rid}")
            changed = True
        except Exception as e:
            _record_edge_failure("remove relation", label, rid, e)
    return changed


def _relations_pull_target(story: dict, cu_task: dict) -> Optional[list[str]]:
    """The ``related`` list a pull would write, or None if no change needed.

    Pure (no mutation), so the dry-run preview and the real pull share one rule.
    """
    cu_ids = _cu_linked_ids(cu_task)
    if not cu_ids and "related" not in story:
        return None
    current = {
        str(x).strip() for x in (story.get("related") or []) if str(x).strip()
    }
    if current == cu_ids:
        return None
    return sorted(cu_ids)


def _pull_relations(story: dict, cu_task: dict) -> bool:
    """Read ClickUp links back into the YAML story. True if changed."""
    target = _relations_pull_target(story, cu_task)
    if target is None:
        return False
    story["related"] = target
    return True


# ---------------------------------------------------------------------------
# Parent (subtask hierarchy) — reconciled as an edge, referenced by story NAME
# ---------------------------------------------------------------------------
#
# Semantics mirror ``depends_on``/``related`` — YAML-authoritative when the key
# is present:
#   - no ``parent`` key      -> UNMANAGED: ClickUp hierarchy untouched (a
#                               subtask nested in the UI survives).
#   - ``parent: <ref>``      -> MANAGED: the task is moved under that parent.
#   - ``parent:`` (empty)    -> MANAGED, top-level. Satisfiable only when the
#                               task already HAS no remote parent — see below.
#
# ``<ref>`` is normally the parent story's ``name`` as written in this YAML,
# resolved to a ``clickup_id`` at reconcile time (``_resolve_parent_id``). Names
# are matched case-insensitively and trimmed. A raw ``clickup_id`` is also
# accepted, which is what makes a parent living outside this file (or pulled
# back from ClickUp) expressible.
#
# Why names and not ids (as ``depends_on`` uses): the reconcile pass runs AFTER
# the create pass, so a parent created in the same run already has its id
# written back — a name reference therefore links a brand-new parent and child
# on the FIRST sync, where an id reference could not (the id does not exist
# until that run creates it). It also reads as hierarchy in the file, which is
# the point of the feature (#20).
#
# Sandbox-verified constraints (docs/subtask-parent-findings.md):
#   - ``parent`` is settable AND changeable via ``PUT /task/{id}`` — a re-parent
#     is an in-place update, never delete+recreate, so the
#     in-place-by-``clickup_id`` invariant holds.
#   - There is NO un-parent: ``PUT {"parent": null}`` and ``{"parent": ""}``
#     both return 200 and change nothing. So a declared-empty ``parent`` on a
#     task that IS a subtask cannot be honoured — that raises rather than
#     silently diverging. Promoting a child back to top level is a UI action.
#
# Like the other edges, ``parent`` is deliberately NOT base-tracked: sync
# APPLIES a declared parent rather than surfacing it as a 3-way conflict.
#
# A newly created child therefore costs one extra ``PUT`` (create, then parent in
# the second pass) instead of passing ``parent`` on create. That is deliberate:
# one code path handles create and re-parent alike, and it is what lets a
# name reference point at a parent created later in the same run.

# Sentinel for "no pull needed" — the pull target itself can legitimately be
# ``None`` (remote task was promoted to top level), so ``None`` cannot mean
# "nothing to do" the way it does for the list-valued edges.
_NO_PARENT_CHANGE = object()


class ParentPendingCreate(ValueError):
    """The named parent story exists in YAML but has no ``clickup_id`` yet.

    Distinguished from other resolution failures because it means opposite
    things per caller: under ``--dry-run`` it is the *expected* state for a
    parent the real run will create (preview it), whereas in a real run — where
    the create pass has already written ids back — it means that create failed.
    """


def _build_parent_context(
    data: dict, cu_by_id: Optional[dict] = None
) -> dict:
    """Indexes ``parent`` resolution needs, built once per run.

    ``names``    -- ``name_lower -> [story, …]`` (a list, so a duplicated story
                    name is detectable and can be refused instead of guessed at).
    ``name_by_id`` -- ``clickup_id -> name``, for writing a pulled parent back as
                    a readable name instead of an opaque id.
    ``ids``      -- every ``clickup_id`` in the file, so an explicit id reference
                    is recognised as such before the name lookup runs.
    ``list_id``  -- the list being synced, so a parent in a DIFFERENT list can be
                    refused before ClickUp silently relocates the child.
    ``cu_by_id`` -- tasks already fetched from that list, so the membership check
                    usually costs no extra API call.
    """
    names: dict[str, list[dict]] = {}
    name_by_id: dict[str, str] = {}
    ids: set[str] = set()
    for epic in data.get("epics", []):
        for story in epic.get("stories", []):
            name = str(story.get("name", "")).strip()
            if name:
                names.setdefault(name.lower(), []).append(story)
            cid = story.get("clickup_id")
            if cid:
                ids.add(str(cid))
                if name:
                    name_by_id[str(cid)] = name
    return {
        "names": names,
        "name_by_id": name_by_id,
        "ids": ids,
        "list_id": str((data.get("project") or {}).get("clickup_list_id") or ""),
        "cu_by_id": cu_by_id or {},
    }


def _parent_display_ref(cu_parent_id: str, parent_ctx: dict) -> str:
    """How to WRITE a remote parent id into YAML: its story name, or the id.

    The name is only used when it is unambiguous in this file — a name shared by
    two stories is exactly what ``_resolve_parent_id`` refuses, so writing it
    would produce YAML that the next push rejects. Falls back to the id, which
    always resolves.
    """
    cu_parent_id = str(cu_parent_id)
    name = parent_ctx.get("name_by_id", {}).get(cu_parent_id)
    if not name:
        return cu_parent_id
    if len(parent_ctx.get("names", {}).get(name.lower(), [])) > 1:
        return cu_parent_id
    return name


def _parent_ref(story: dict) -> Optional[str]:
    """The story's declared ``parent`` reference, trimmed; ``None`` when empty."""
    raw = story.get("parent")
    if raw is None:
        return None
    ref = str(raw).strip()
    return ref or None


def _resolve_parent_id(ref: str, parent_ctx: dict, self_id: Optional[str]) -> str:
    """Resolve a ``parent`` reference to a ClickUp task id.

    Precedence: an explicit ``clickup_id`` of a story in this file, then a story
    NAME, then a bare token treated as a literal id (a parent outside this YAML).

    Raises ``ValueError`` — never guesses — when the reference is ambiguous,
    unknown, self-referential, or names a story that has no id yet. The caller
    turns that into a counted error: a parent that silently failed to apply is
    exactly the drift this feature exists to remove.
    """
    ref = str(ref).strip()
    self_id = str(self_id) if self_id else None

    def _check_self(target: str) -> str:
        if self_id and target == self_id:
            raise ValueError(f"a task cannot be its own parent (parent: '{ref}')")
        return target

    if ref in parent_ctx.get("ids", ()):
        return _check_self(ref)

    candidates = parent_ctx.get("names", {}).get(ref.lower(), [])
    if len(candidates) > 1:
        raise ValueError(
            f"parent: '{ref}' is ambiguous — {len(candidates)} stories share that "
            f"name; reference the parent by its clickup_id instead"
        )
    if candidates:
        cid = candidates[0].get("clickup_id")
        if not cid:
            raise ParentPendingCreate(
                f"parent story '{ref}' has no clickup_id yet"
            )
        return _check_self(str(cid))

    if not ref or any(c.isspace() for c in ref):
        raise ValueError(f"parent: '{ref}' — no story named that in this YAML")
    # No name match and no whitespace: a literal id for a parent outside this
    # file. Push it as given; ClickUp rejects a bad id loudly.
    return _check_self(ref)


def _assert_parent_in_same_list(
    token: str, parent_id: str, parent_ctx: dict, label: str
) -> None:
    """Refuse a parent that lives in a different ClickUp list.

    Verified live: ``PUT /task/{id} {"parent": <task in another list>}`` returns
    200 and **relocates the child into the parent's list**, where the next sync
    sees it as ``archived_in_clickup`` — i.e. the story silently leaves the board.
    (ClickUp's create endpoint refuses the same pairing outright: 400 ITEM_137
    "Parent not child of list".) So membership is checked before the PUT.

    Free when the parent is one of the tasks already fetched from this list;
    otherwise one GET, which also turns a bad id into a clear error instead of an
    opaque 400.
    """
    target_list = parent_ctx.get("list_id")
    if not target_list:
        return  # no list in context (bare unit call) — nothing to compare against
    if parent_id in parent_ctx.get("ids", ()):
        return  # a story in THIS yaml: synced to this list by construction. If it
        # were moved out in the UI, sync reports it as archived, not as a parent bug.
    if parent_id in (parent_ctx.get("cu_by_id") or {}):
        return  # fetched from this list, so it is in it (a subtask shares its
        # parent's list, so this holds for nested tasks too)
    try:
        parent_task = clickup_get_task(token, parent_id)
    except Exception as e:
        raise ValueError(
            f"{label}: parent task {parent_id} could not be read ({e}) — check "
            f"the id"
        ) from e
    parent_list = str((parent_task.get("list") or {}).get("id") or "")
    if parent_list and parent_list != str(target_list):
        raise ValueError(
            f"{label}: parent task {parent_id} is in a different list "
            f"({parent_list}, not {target_list}). ClickUp would MOVE this task "
            f"into that list rather than reject it, and the next sync would see "
            f"it as archived. Pick a parent in the list being synced."
        )


def _sync_parent(
    token: str,
    task_id: str,
    cu_task: dict,
    story: dict,
    parent_ctx: dict,
    dry_run: bool = False,
    warn_on_remove: bool = False,
) -> bool:
    """Reconcile a task's ``parent`` edge to match the story's ``parent``.

    No-op (preserves a UI-built hierarchy) when the story has no ``parent`` key.
    Returns True if a change was made — or would be, under ``dry_run``.
    Raises ``ValueError`` when the declared parent cannot be applied.
    """
    if "parent" not in story:
        return False
    current = cu_task.get("parent")
    current = str(current) if current else None
    ref = _parent_ref(story)
    label = _edge_label(story, task_id)

    if ref is None:
        # Declared top-level. Already true -> nothing to do. Otherwise refuse:
        # the API has no un-parent (verified — PUT parent:null is a silent 200).
        if current is None:
            return False
        raise ValueError(
            f"{label} declares no parent but ClickUp has it nested under "
            f"{current}; the API cannot un-parent a task (PUT parent:null is a "
            f"silent no-op). Promote it in the ClickUp UI, or restore the "
            f"`parent:` value in YAML."
        )

    desired = _resolve_parent_id(ref, parent_ctx, task_id)
    if desired == current:
        return False
    # Checked under dry-run too: the whole point of the preview is to surface
    # what the real run would do, and this one would relocate the task.
    _assert_parent_in_same_list(token, desired, parent_ctx, label)
    if current and warn_on_remove:
        log.warning(
            f"    {'[DRY RUN] Would move' if dry_run else 'Moving'} {label} from "
            f"parent {current} to {desired} — parent is YAML-authoritative and "
            f"not base-tracked, so a hierarchy change made in the UI is lost."
        )
    if dry_run:
        log.info(
            f"    [DRY RUN] Would set parent on {label}: "
            f"{current or '(top level)'} -> {desired} ('{ref}')"
        )
        return True
    clickup_update_task(token, task_id, {"parent": desired})
    log.info(f"    Set parent: {label} -> {desired} ('{ref}')")
    return True


def _parent_pull_target(story: dict, cu_task: dict, parent_ctx: dict) -> Any:
    """The ``parent`` value a pull would write, or ``_NO_PARENT_CHANGE``.

    Pure (no mutation) so the dry-run preview and the real pull share one rule.
    Prefers the parent story's NAME (the authoring form) and falls back to the
    raw id when the parent isn't a story in this YAML.
    """
    cu_parent = cu_task.get("parent")
    cu_parent = str(cu_parent) if cu_parent else None
    if cu_parent is None:
        # Remote is top-level: only a story that CLAIMS a parent needs writing.
        if _parent_ref(story) is None:
            return _NO_PARENT_CHANGE
        return None
    # Already pointing at this parent (by name or id)? Leave the wording alone.
    ref = _parent_ref(story)
    if ref is not None:
        try:
            if _resolve_parent_id(ref, parent_ctx, story.get("clickup_id")) == cu_parent:
                return _NO_PARENT_CHANGE
        except ValueError:
            pass  # unresolvable locally — the remote value is the truth, write it
    return _parent_display_ref(cu_parent, parent_ctx)


def _pull_parent(story: dict, cu_task: dict, parent_ctx: dict) -> bool:
    """Read ClickUp's ``parent`` edge back into the YAML story. True if changed."""
    target = _parent_pull_target(story, cu_task, parent_ctx)
    if target is _NO_PARENT_CHANGE:
        return False
    story["parent"] = target
    return True


def _declared_edge_ids(story: dict, key: str) -> set[str]:
    """The non-blank ids a story declares under ``key`` (``depends_on``/``related``)."""
    return {str(x).strip() for x in (story.get(key) or []) if str(x).strip()}


def _warn_edge_target_overlap(story: dict, task_id: str) -> None:
    """Flag a target declared as BOTH a dependency and a relation.

    ClickUp permits both edges on one pair and we apply exactly what the YAML
    says, so this is a warning and not an error. But it is nearly always one
    intent written twice — typically a ``related`` entry added because the
    ``depends_on`` was believed not to have taken effect — and the two edges then
    have to be found and cleaned up separately.
    """
    both = sorted(
        _declared_edge_ids(story, "depends_on") & _declared_edge_ids(story, "related")
    )
    if not both:
        return
    log.warning(
        f"    {_edge_label(story, task_id)} declares {both} as both a dependency "
        f"and a relation — ClickUp will hold two separate edges on the same pair. "
        f"Drop the `related` entry unless the extra link is deliberate."
    )


def _preview_pending_create_edges(
    token: str, story: dict, parent_ctx: Optional[dict] = None
) -> None:
    """Preview the edges a story that would be CREATED this run declares.

    Under ``--dry-run`` no task is created, so the story still has no
    ``clickup_id`` and the reconcile below skips it — the edges would then first
    appear in the real run, having never been previewed. Reconcile against a
    synthetic edge-free task so additions still surface; a task that does not
    exist yet has no edges to remove, so nothing destructive is hidden here.

    A declared ``parent`` may itself point at a story pending create (no id yet),
    which ``_resolve_parent_id`` refuses — under dry-run that is expected, not an
    error, so it is reported as pending rather than raised.
    """
    cu_task = {
        "id": PENDING_CREATE_ID, "dependencies": [], "linked_tasks": [], "parent": None,
    }
    _sync_dependencies(token, PENDING_CREATE_ID, cu_task, story, dry_run=True)
    _sync_relations(token, PENDING_CREATE_ID, cu_task, story, dry_run=True)
    if "parent" in story:
        try:
            _sync_parent(
                token, PENDING_CREATE_ID, cu_task, story, parent_ctx or {}, dry_run=True
            )
        except ParentPendingCreate:
            # The parent is being created by this same run, so it has no id to
            # name yet. Expected under dry-run — preview it rather than error.
            log.info(
                f"    [DRY RUN] Would set parent on "
                f"{_edge_label(story, PENDING_CREATE_ID)} -> "
                f"'{_parent_ref(story)}' (id assigned when it is created)"
            )
        except ValueError as e:
            log.error(
                f"    [DRY RUN] Parent on "
                f"{_edge_label(story, PENDING_CREATE_ID)} cannot be applied: {e}"
            )


def _reconcile_edges_pass(
    token: str,
    data: dict,
    cu_by_id: dict,
    stats: dict,
    dry_run: bool,
    warn_on_remove: bool = False,
) -> None:
    """Reconcile dependency, relation and parent edges for every managed story.

    The relationship-reconcile **second pass**, shared by ``cmd_push``,
    ``cmd_sync``, and ``cmd_merge``. Runs after the create/update pass so a task
    created this run already has its ``clickup_id`` written back (edge targets
    reference tasks by id and must resolve). A task created this run isn't in
    ``cu_by_id`` (fetched before creates), so it starts from a synthetic
    edge-free task. Under ``dry_run`` no create happened, so a pending-create
    story has no id at all — its edges are previewed against a synthetic task
    rather than skipped, since the real run WILL apply them. Edges are
    YAML-authoritative when the key is present;
    failures bump ``stats['errors']``. ``warn_on_remove`` loudly flags edge
    deletions — push leaves it False (authoritative overwrite is its contract);
    sync/merge pass True so a destructive removal is never silent.
    """
    declared_related = _build_declared_relations(data)
    # Built once: `parent` references stories by NAME, so resolution needs the
    # whole-file view (and the ids the create pass just wrote back). cu_by_id
    # makes the same-list check free for any parent already fetched.
    parent_ctx = _build_parent_context(data, cu_by_id)
    for epic in data["epics"]:
        for story in epic.get("stories", []):
            cid = story.get("clickup_id")
            if (
                "depends_on" not in story
                and "related" not in story
                and "parent" not in story
            ):
                continue
            _warn_edge_target_overlap(story, cid or PENDING_CREATE_ID)
            if not cid:
                # No id yet. In a real run the create pass has already written
                # one back, so this is the dry-run case: preview the edges
                # instead of going silent about them (they WOULD be applied).
                if dry_run:
                    _preview_pending_create_edges(token, story, parent_ctx)
                continue
            cu_task = cu_by_id.get(cid) or {
                "id": cid, "dependencies": [], "linked_tasks": [], "parent": None
            }
            try:
                _sync_dependencies(
                    token, cid, cu_task, story,
                    dry_run=dry_run, warn_on_remove=warn_on_remove,
                )
            except Exception as e:
                log.error(
                    f"  Failed to reconcile dependencies for {story['name']}: {e}"
                )
                stats["errors"] += 1
            try:
                _sync_relations(
                    token, cid, cu_task, story,
                    dry_run=dry_run, warn_on_remove=warn_on_remove,
                    peer_declared=declared_related,
                )
            except Exception as e:
                log.error(
                    f"  Failed to reconcile relations for {story['name']}: {e}"
                )
                stats["errors"] += 1
            try:
                _sync_parent(
                    token, cid, cu_task, story, parent_ctx,
                    dry_run=dry_run, warn_on_remove=warn_on_remove,
                )
            except Exception as e:
                log.error(f"  Failed to reconcile parent for {story['name']}: {e}")
                stats["errors"] += 1


# ---------------------------------------------------------------------------
# Backup-before-push
# ---------------------------------------------------------------------------


# Sentinel indicating ``--backup-to`` was passed with no explicit path —
# resolve to the default location at write time.
BACKUP_DEFAULT_SENTINEL = "__DEFAULT__"


def _default_backup_path(list_id: str) -> Path:
    """Default backup location: ``~/tmp/clickup-backup-<list_id>-<iso>.yaml``."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup_dir = Path.home() / "tmp"
    backup_dir.mkdir(parents=True, exist_ok=True)
    return backup_dir / f"clickup-backup-{list_id}-{ts}.yaml"


def _build_backup_snapshot(data: dict, cu_tasks: list[dict]) -> dict:
    """Convert a bulk-fetched ClickUp task list into a YAML-compatible snapshot.

    The snapshot mirrors the input YAML's ``project`` + ``status_map`` blocks
    so a bad push can be undone by ``push``-ing this file back up (YAML→ClickUp
    restores the snapshot). NOT via ``pull`` — pull's source is ClickUp's live
    API, never a backup file.
    """
    status_map = data.get("status_map", {}) or {}
    epic_name_map = build_epic_name_map(data)
    epic_buckets: dict[int, list[dict]] = {ei: [] for ei in range(len(data.get("epics", [])))}
    orphans: list[dict] = []
    for cu_task in cu_tasks:
        story = _clickup_task_to_yaml_story(cu_task, status_map)
        epic_key = _extract_epic_name_from_tags(cu_task, epic_name_map)
        if epic_key is not None:
            epic_buckets[epic_name_map[epic_key]].append(story)
        else:
            orphans.append(story)

    snapshot_epics: list[dict] = []
    for ei, epic in enumerate(data.get("epics", []) or []):
        snapshot_epics.append({
            "number": epic.get("number"),
            "name": epic.get("name"),
            "status": epic.get("status", "backlog"),
            "points": epic.get("points", 0),
            "stories": epic_buckets[ei],
        })
    if orphans:
        snapshot_epics.append({
            "number": None,
            "name": "_orphans",
            "status": "backlog",
            "points": 0,
            "description": "Tasks fetched at backup time with no matching epic.",
            "stories": orphans,
        })

    return {
        "project": {
            "name": (data.get("project", {}) or {}).get("name", "backup"),
            "clickup_list_id": (data.get("project", {}) or {}).get("clickup_list_id", ""),
            "backed_up_at": datetime.now(timezone.utc).isoformat(),
        },
        "status_map": status_map,
        "epics": snapshot_epics,
    }


def _resolve_backup_path(
    backup_path: Optional[str],
    backup_default: bool,
    list_id: str,
    cu_tasks: list[dict],
) -> Optional[Path]:
    """Pick the final backup destination Path, or None if we should skip.

    Skip semantics:
      - explicit None and ``backup_default`` False -> skip (caller opted out)
      - explicit None and ``backup_default`` True but list is empty -> skip
        (nothing to back up — a brand-new sandbox list)
    """
    if backup_path is None:
        if not backup_default:
            return None
        if not cu_tasks:
            log.info("Skipping backup — list is empty (no remote state to lose).")
            return None
        return _default_backup_path(list_id)
    if backup_path == BACKUP_DEFAULT_SENTINEL:
        return _default_backup_path(list_id)
    return Path(backup_path)


def _maybe_write_backup(
    data: dict,
    cu_tasks: list[dict],
    list_id: str,
    backup_path: Optional[str],
    backup_default: bool,
) -> Optional[Path]:
    """Write a snapshot YAML of current ClickUp state before mutating it.

    Returns the path written, or None if no backup was produced.
    """
    target = _resolve_backup_path(backup_path, backup_default, list_id, cu_tasks)
    if target is None:
        return None
    snapshot = _build_backup_snapshot(data, cu_tasks)
    target.parent.mkdir(parents=True, exist_ok=True)
    with open(target, "w") as f:
        yaml.dump(snapshot, f, default_flow_style=False, allow_unicode=True,
                  sort_keys=False, width=120)
    log.info(f"Backup written: {target}")
    return target


def _backup_yaml_file(yaml_path: str, dest: Optional[str] = None) -> Optional[Path]:
    """Copy the YAML file to a timestamped backup before pull overwrites it.

    pull is the only mutating command that rewrites the *local* file (remote
    wins), so its safety net is a copy of the YAML — not a snapshot of ClickUp
    (which pull never touches). Returns the backup path, or None if the source
    doesn't exist yet.

    Default destination is the project-local ``.clickup-sync/`` sidecar (the same
    gitignored dir that holds the base snapshot) — NOT ``~/tmp``: backups keyed
    only by file *stem* collide when a corpus has several identically-named files
    (e.g. multiple ``project-tasks.yaml``). Co-locating beside the source is
    collision-free and travels with the project. ``dest`` (from ``--backup-to``)
    overrides with an explicit path.
    """
    src = Path(yaml_path)
    if not src.exists():
        return None
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    if dest:
        dst = Path(dest)
    else:
        dst = src.resolve().parent / ".clickup-sync" / f"yaml-backup-{src.stem}-{ts}.yaml"
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    log.info(f"YAML backup written: {dst}")
    return dst


def _warn_one_directional(
    command: str,
    overwrites: str,
    backup_desc: str,
    *,
    dry_run: bool,
    will_backup: bool,
) -> None:
    """Loud startup banner: this command is a blunt one-way overwrite.

    push/pull have NO conflict detection — unlike sync, which reads the base
    snapshot, auto-resolves one-sided changes, and STOPS on a true conflict.
    Non-blocking (so scripted use still works), just makes the bluntness visible.
    The backup line is derived from the resolved run state (``dry_run`` /
    ``will_backup``) so it never claims a backup that won't actually be written.
    """
    if dry_run:
        note = "(dry run — no changes will be written.)"
    elif will_backup:
        note = f"A backup of {backup_desc} is written first (--no-backup to skip)."
    else:
        note = ("--no-backup: NO backup will be written — an unwanted overwrite "
                "is unrecoverable.")
    bar = "=" * 74
    log.warning(bar)
    log.warning(f"  `{command}` is a one-directional overwrite with NO conflict detection.")
    log.warning(f"  It overwrites {overwrites}.")
    log.warning(f"  {note}")
    log.warning("  For conflict-aware reconciliation that auto-resolves one-sided")
    log.warning("  changes and STOPS on a true conflict, use `sync` instead.")
    log.warning(bar)


# ---------------------------------------------------------------------------
# Push command (flat stories with epic tag)
# ---------------------------------------------------------------------------


def cmd_push(
    data: dict,
    yaml_path: str,
    dry_run: bool = False,
    backup_path: Optional[str] = None,
    backup_default: bool = True,
) -> dict:
    """Push stories to ClickUp as flat top-level tasks with an epic tag.
    Epics exist only in YAML — they are NOT created in ClickUp.
    Uses a single bulk fetch to build an in-memory index, then only
    makes PUT calls for stories that actually differ."""
    token = get_clickup_token()
    list_id = data["project"]["clickup_list_id"]
    status_map = data.get("status_map", {})
    project_cfg = data.get("project", {})
    stats = {"created": 0, "updated": 0, "unchanged": 0, "errors": 0}

    _warn_one_directional(
        "push",
        "ClickUp from YAML for every managed field — a change made in the "
        "ClickUp UI since your last sync will be lost",
        "current ClickUp state, restorable by running `push <backup-file>`",
        dry_run=dry_run,
        will_backup=backup_default,
    )

    # Bulk fetch all ClickUp tasks once — avoids N individual GET calls
    log.info("Fetching all tasks from ClickUp...")
    cu_tasks = clickup_list_tasks(token, list_id)
    cu_by_id = {t["id"]: t for t in cu_tasks}
    log.info(f"Fetched {len(cu_tasks)} tasks")

    # Backup-before-push: snapshot remote state so a bad push can be undone by
    # `push`-ing the backup file back up (YAML→ClickUp restores the snapshot).
    # NOT `pull` — pull reads ClickUp's live API, never a backup file.
    if not dry_run:
        _maybe_write_backup(
            data=data,
            cu_tasks=cu_tasks,
            list_id=list_id,
            backup_path=backup_path,
            backup_default=backup_default,
        )

    managed_universe = managed_tag_universe_for(data, yaml_path, list_id)
    # Dedupe index — adopt an existing ClickUp task rather than create a
    # duplicate when a prior run was interrupted before id writeback (BUG #14).
    dedupe_index = _build_create_dedupe_index(cu_tasks)
    push_epic_tag = project_cfg.get("push_epic_tag", True)
    assignee_resolver = _build_assignee_resolver(
        clickup_get_list_members(token, list_id)
    )

    for epic in data["epics"]:
        tag = _epic_tag(epic)
        epic_priority = epic.get("priority")

        for story in epic.get("stories", []):
            story_name = story["name"]
            desired_tags = _story_desired_tags(
                story, epic, push_epic_tag=project_cfg.get("push_epic_tag", True)
            )
            if not story.get("clickup_id"):
                # Dedupe-before-create: adopt a matching existing task instead of
                # making a second copy (interrupted-retry safety, BUG #14).
                existing_id = _match_existing_cu_task(
                    dedupe_index, story_name, tag, push_epic_tag
                )
                if existing_id is not None and not dry_run:
                    story["clickup_id"] = existing_id
                    story["task_id"] = cu_by_id.get(existing_id, {}).get("custom_id")
                    save_yaml(data, yaml_path)  # crash-safe per-task flush
                    log.warning(
                        f"  Adopted existing ClickUp task for '{story_name}' -> "
                        f"{existing_id} (skipped duplicate create)"
                    )
                    stats.setdefault("adopted", 0)
                    stats["adopted"] += 1
                    continue
                # CREATE story as top-level task with all desired tags
                create_assignee_ids: list[int] = []
                if "assignees" in story:
                    create_assignee_ids, unresolved = _resolve_assignee_ids(
                        story.get("assignees"), assignee_resolver
                    )
                    for u in unresolved:
                        log.warning(f"  Assignee not found, skipping: '{u}'")
                body = build_task_body(
                    story, status_map, tags=desired_tags,
                    default_priority=epic_priority,
                    assignee_ids=create_assignee_ids,
                )
                if dry_run:
                    log.info(
                        f"  [DRY RUN] Would create: {story_name} "
                        f"tags={desired_tags}"
                    )
                    stats["created"] += 1
                else:
                    try:
                        resp = clickup_create_task(token, list_id, body)
                        story["clickup_id"] = resp["id"]
                        story["task_id"] = resp.get("custom_id")
                        # Register the new id so a later same-name story this run
                        # adopts it instead of creating a second copy.
                        dedupe_index.setdefault(
                            _create_dedupe_key(story_name, tag), []
                        ).append(resp["id"])
                        if "priority" not in story and epic_priority is not None:
                            story["priority"] = epic_priority
                        # Push Epic dropdown if YAML configures it.
                        _push_epic_dropdown_if_needed(
                            token, resp["id"], resp, story, epic, project_cfg, dry_run
                        )
                        save_yaml(data, yaml_path)  # incremental save
                        log.info(
                            f"  Created: {story_name} -> {resp['id']} "
                            f"({resp.get('custom_id')}) tags={desired_tags}"
                        )
                        stats["created"] += 1
                    except Exception as e:
                        log.error(f"  Failed to create {story_name}: {e}")
                        stats["errors"] += 1
            else:
                # UPDATE story if changed — compare against in-memory index
                cu_task = cu_by_id.get(story["clickup_id"])
                if cu_task is None:
                    log.warning(f"  Story not found in ClickUp: {story_name} ({story['clickup_id']})")
                    stats["errors"] += 1
                    continue
                _sync_metadata(story, cu_task)
                if "priority" not in story and epic_priority is not None:
                    story["priority"] = epic_priority
                diffs = compare_task(story, cu_task, status_map)
                # Field-level diff drives the PUT; tags + custom-field are
                # reconciled separately (their endpoints are different).
                if diffs:
                    update_body = build_task_body(story, status_map,
                                                  default_priority=epic_priority)
                    if dry_run:
                        for d in diffs:
                            log.info(f"  [DRY RUN] Would update {story_name} "
                                     f"field '{d['field']}': "
                                     f"'{d['clickup']}' -> '{d['yaml']}'")
                    else:
                        try:
                            clickup_update_task(token, story["clickup_id"], update_body)
                            for d in diffs:
                                log.info(f"  Updated {story_name} "
                                         f"field '{d['field']}': "
                                         f"'{d['clickup']}' -> '{d['yaml']}'")
                        except Exception as e:
                            log.error(f"  Failed to update {story_name}: {e}")
                            stats["errors"] += 1
                            continue
                    stats["updated"] += 1
                else:
                    stats["unchanged"] += 1
                # Reconcile tags (multi-tag, additive) on every update path
                if dry_run:
                    current = _current_tag_names(cu_task)
                    if set(t.lower() for t in current) != set(
                        t.lower() for t in desired_tags
                    ):
                        log.info(
                            f"  [DRY RUN] Would reconcile tags on {story_name}: "
                            f"{current} -> {desired_tags}"
                        )
                else:
                    _sync_tags(
                        token,
                        story["clickup_id"],
                        cu_task,
                        desired_tags,
                        managed_known_tags=managed_universe,
                    )
                # Push Epic dropdown if YAML configures it and value differs
                _push_epic_dropdown_if_needed(
                    token, story["clickup_id"], cu_task, story, epic, project_cfg, dry_run
                )
                # Reconcile assignees (YAML-authoritative when the key is present)
                _sync_assignees(
                    token, story["clickup_id"], cu_task, story,
                    assignee_resolver, dry_run=dry_run,
                )

    # Second pass: reconcile relationship edges (dependencies + relations).
    _reconcile_edges_pass(token, data, cu_by_id, stats, dry_run)

    if not dry_run:
        save_yaml(data, yaml_path)

    log.info(f"\nPush complete: {stats['created']} created, {stats['updated']} updated, "
             f"{stats['unchanged']} unchanged, {stats['errors']} errors")
    return stats


# ---------------------------------------------------------------------------
# Pull command (flat stories matched by clickup_id or epic tag)
# ---------------------------------------------------------------------------


def cmd_pull(
    data: dict,
    yaml_path: str,
    dry_run: bool = False,
    backup_default: bool = True,
    backup_path: Optional[str] = None,
) -> dict:
    """Pull ClickUp tasks into YAML. Tasks are matched by clickup_id.
    New tasks are placed by their epic tag (E1, E9, etc.) or into _orphans.
    ``backup_path`` (from --backup-to) overrides the default backup location."""
    token = get_clickup_token()
    list_id = data["project"]["clickup_list_id"]
    status_map = data.get("status_map", {})
    stats = {"updated": 0, "new": 0, "archived": 0, "unchanged": 0}

    _warn_one_directional(
        "pull",
        "your YAML file from ClickUp (remote wins) — uncommitted local YAML "
        "edits will be lost",
        "your YAML file (restore by copying the backup back over it)",
        dry_run=dry_run,
        will_backup=backup_default,
    )

    # Backup-before-pull: pull rewrites the local file, so copy it first. Fail
    # CLOSED — if the safety copy can't be written, abort before any overwrite
    # rather than proceed unprotected (rerun with --no-backup to opt out).
    if not dry_run and backup_default:
        explicit = backup_path if backup_path not in (None, BACKUP_DEFAULT_SENTINEL) else None
        try:
            _backup_yaml_file(yaml_path, dest=explicit)
        except OSError as e:
            log.error(
                f"Pre-pull YAML backup failed ({e}); aborting to avoid an "
                f"unrecoverable overwrite. Rerun with --no-backup to pull without one."
            )
            sys.exit(1)

    log.info("Fetching all tasks from ClickUp...")
    cu_tasks = clickup_list_tasks(token, list_id)
    log.info(f"Fetched {len(cu_tasks)} tasks from ClickUp")

    story_index = build_story_id_index(data)
    epic_name_map = build_epic_name_map(data)
    parent_ctx = _build_parent_context(data)
    seen_cu_ids: set[str] = set()

    for cu_task in cu_tasks:
        cu_id = cu_task["id"]
        seen_cu_ids.add(cu_id)

        if cu_id in story_index:
            # Known story — update fields
            ei, si = story_index[cu_id]
            story = data["epics"][ei]["stories"][si]
            if not dry_run:
                _sync_metadata(story, cu_task)
            diffs = compare_task(story, cu_task, status_map)
            if diffs:
                if not dry_run:
                    _apply_clickup_to_yaml(story, cu_task, status_map)
                for d in diffs:
                    log.info(f"  Updated '{story['name']}' "
                             f"field '{d['field']}': '{d['yaml']}' -> '{d['clickup']}'")
                stats["updated"] += 1
            else:
                stats["unchanged"] += 1
            # Assignees aren't a compare_task field; reconcile directly so a
            # UI-only assignee change still round-trips into YAML.
            if dry_run:
                target = _assignees_pull_target(story, cu_task)
                if target is not None:
                    log.info(f"  [DRY RUN] Would pull assignees for "
                             f"'{story['name']}': "
                             f"{story.get('assignees', '(none)')} -> {target}")
            elif _pull_assignees(story, cu_task):
                log.info(f"  Pulled assignees for '{story['name']}': "
                         f"{story.get('assignees')}")
            # Dependencies are likewise not a compare_task field — reconcile
            # them directly so a UI-set waiting_on edge round-trips into YAML.
            if dry_run:
                dtarget = _dependencies_pull_target(story, cu_task)
                if dtarget is not None:
                    log.info(f"  [DRY RUN] Would pull dependencies for "
                             f"'{story['name']}': "
                             f"{story.get('depends_on', '(none)')} -> {dtarget}")
            elif _pull_dependencies(story, cu_task):
                log.info(f"  Pulled dependencies for '{story['name']}': "
                         f"{story.get('depends_on')}")
            # Relations (linked tasks) are also not a compare_task field —
            # reconcile directly so a UI-set link round-trips into YAML.
            if dry_run:
                rtarget = _relations_pull_target(story, cu_task)
                if rtarget is not None:
                    log.info(f"  [DRY RUN] Would pull relations for "
                             f"'{story['name']}': "
                             f"{story.get('related', '(none)')} -> {rtarget}")
            elif _pull_relations(story, cu_task):
                log.info(f"  Pulled relations for '{story['name']}': "
                         f"{story.get('related')}")
            # Parent (subtask hierarchy) — likewise not a compare_task field, so
            # a nesting change made in the UI round-trips into YAML here.
            if dry_run:
                ptarget = _parent_pull_target(story, cu_task, parent_ctx)
                if ptarget is not _NO_PARENT_CHANGE:
                    log.info(f"  [DRY RUN] Would pull parent for "
                             f"'{story['name']}': "
                             f"{story.get('parent', '(unmanaged)')} -> "
                             f"{ptarget or '(top level)'}")
            elif _pull_parent(story, cu_task, parent_ctx):
                log.info(f"  Pulled parent for '{story['name']}': "
                         f"{story.get('parent') or '(top level)'}")
        else:
            # New task from ClickUp — place by epic tag
            new_story = _clickup_task_to_yaml_story(cu_task, status_map, parent_ctx)
            epic_key = _extract_epic_name_from_tags(cu_task, epic_name_map)
            if epic_key is not None:
                target_ei = epic_name_map[epic_key]
                if not dry_run:
                    data["epics"][target_ei].setdefault("stories", []).append(new_story)
                log.info(f"  New story from ClickUp: '{cu_task['name']}' "
                         f"-> epic '{data['epics'][target_ei]['name']}'")
            else:
                orphan = _get_or_create_orphan_epic(data)
                if not dry_run:
                    orphan.setdefault("stories", []).append(new_story)
                log.info(f"  Orphan from ClickUp: '{cu_task['name']}' ({cu_id})")
            stats["new"] += 1

    # Detect archived stories (in YAML but not in ClickUp)
    for epic in data["epics"]:
        for story in epic.get("stories", []):
            scu_id = story.get("clickup_id")
            if scu_id and scu_id not in seen_cu_ids:
                if not dry_run:
                    story["archived_in_clickup"] = True
                log.info(f"  Archived: '{story['name']}' ({scu_id})")
                stats["archived"] += 1

    if not dry_run:
        save_yaml(data, yaml_path)

    log.info(f"\nPull complete: {stats['updated']} updated, {stats['new']} new, "
             f"{stats['archived']} archived, {stats['unchanged']} unchanged")
    return stats


def _apply_clickup_to_yaml(yaml_task: dict, cu_task: dict, status_map: dict) -> None:
    """Update a YAML task dict with values from ClickUp."""
    yaml_task["name"] = cu_task.get("name", yaml_task.get("name", ""))
    cu_status = cu_task.get("status", {}).get("status", "")
    yaml_task["status"] = clickup_status_to_yaml(cu_status, status_map)
    yaml_task["description"] = strip_meta_prefix(_cu_description(cu_task), yaml_task)
    cu_priority = clickup_priority_to_yaml(cu_task.get("priority"))
    if cu_priority is not None:
        yaml_task["priority"] = cu_priority
    yaml_task["milestone"] = _is_clickup_milestone(cu_task)
    for _fld in ("due_date", "start_date"):
        _cv = clickup_ms_to_yaml_date(cu_task.get(_fld))
        if _cv is not None or _fld in yaml_task:
            yaml_task[_fld] = _cv
    _sync_metadata(yaml_task, cu_task)


def _sync_metadata(yaml_task: dict, cu_task: dict) -> None:
    """Sync non-diffable metadata (task_id) from ClickUp into YAML."""
    cu_custom_id = cu_task.get("custom_id")
    if cu_custom_id:
        yaml_task["task_id"] = cu_custom_id


def _clickup_task_to_yaml_story(
    cu_task: dict, status_map: dict, parent_ctx: Optional[dict] = None
) -> dict:
    """Convert a ClickUp task to a YAML story dict.

    ``parent_ctx`` (from ``_build_parent_context``) lets a subtask's ``parent``
    be written as the parent story's readable NAME; without it the raw id is
    used, which pushes back identically. Before this, a subtask imported from
    ClickUp lost its hierarchy and landed as a flat top-level story.
    """
    story = {
        "name": cu_task.get("name", ""),
        "clickup_id": cu_task["id"],
        "task_id": cu_task.get("custom_id"),
        "points": 0,
        "status": clickup_status_to_yaml(
            cu_task.get("status", {}).get("status", ""), status_map
        ),
        "milestone": _is_clickup_milestone(cu_task),
        "description": _cu_description(cu_task),
    }
    cu_keys = _cu_assignee_keys(cu_task)
    if cu_keys:
        story["assignees"] = cu_keys
    dep_ids = _cu_waiting_on_ids(cu_task)
    if dep_ids:
        story["depends_on"] = sorted(dep_ids)
    rel_ids = _cu_linked_ids(cu_task)
    if rel_ids:
        story["related"] = sorted(rel_ids)
    cu_parent = cu_task.get("parent")
    if cu_parent:
        story["parent"] = _parent_display_ref(str(cu_parent), parent_ctx or {})
    return story


def _get_or_create_orphan_epic(data: dict) -> dict:
    """Get or create an '_orphans' epic for unmatched ClickUp tasks."""
    for epic in data["epics"]:
        if epic.get("name") == "_orphans":
            return epic
    orphan = {
        "number": None,
        "name": "_orphans",
        "clickup_id": None,
        "status": "backlog",
        "points": 0,
        "priority": 4,
        "sprint": None,
        "description": "Tasks found in ClickUp with no matching epic in YAML.",
        "stories": [],
    }
    data["epics"].append(orphan)
    return orphan


# ---------------------------------------------------------------------------
# Diff command (stories only — epics are YAML-local)
# ---------------------------------------------------------------------------


def cmd_diff(data: dict) -> dict:
    token = get_clickup_token()
    list_id = data["project"]["clickup_list_id"]
    status_map = data.get("status_map", {})
    stats = {"need_push": 0, "need_pull": 0, "mismatches": 0, "synced": 0, "archived": 0}

    log.info("Fetching all tasks from ClickUp...")
    cu_tasks = clickup_list_tasks(token, list_id)
    cu_by_id = {t["id"]: t for t in cu_tasks}
    assignee_resolver = _build_assignee_resolver(
        clickup_get_list_members(token, list_id)
    )
    parent_ctx = _build_parent_context(data)

    log.info(f"\n{'='*80}")
    log.info("DIFF REPORT")
    log.info(f"{'='*80}\n")

    yaml_ids: set[str] = set()

    for epic in data["epics"]:
        tag = _epic_tag(epic)
        has_stories = False

        for story in epic.get("stories", []):
            story_name = story["name"]
            scu_id = story.get("clickup_id")

            if not scu_id:
                if not has_stories:
                    log.info(f"[{tag}] {epic['name']}:")
                    has_stories = True
                log.info(f"  [PUSH NEEDED] '{story_name}'")
                stats["need_push"] += 1
            elif scu_id not in cu_by_id:
                if not has_stories:
                    log.info(f"[{tag}] {epic['name']}:")
                    has_stories = True
                log.info(f"  [ARCHIVED] '{story_name}' ({scu_id})")
                stats["archived"] += 1
            else:
                yaml_ids.add(scu_id)
                cu_task = cu_by_id[scu_id]
                diffs = compare_task(story, cu_task, status_map)
                a_diff = _assignees_differ(story, cu_task, assignee_resolver)
                dep_target = _dependencies_pull_target(story, cu_task)
                d_diff = dep_target is not None
                rel_target = _relations_pull_target(story, cu_task)
                r_diff = rel_target is not None
                p_target = _parent_pull_target(story, cu_task, parent_ctx)
                p_diff = p_target is not _NO_PARENT_CHANGE
                if diffs or a_diff or d_diff or r_diff or p_diff:
                    if not has_stories:
                        log.info(f"[{tag}] {epic['name']}:")
                        has_stories = True
                    log.info(f"  [MISMATCH] '{story_name}':")
                    for d in diffs:
                        yaml_val = _truncate(str(d["yaml"]), 60)
                        cu_val = _truncate(str(d["clickup"]), 60)
                        log.info(f"    {d['field']}: YAML='{yaml_val}' "
                                 f"vs ClickUp='{cu_val}'")
                    if a_diff:
                        y = story.get("assignees", "(unmanaged)")
                        r = _cu_assignee_keys(cu_task)
                        log.info(f"    assignees: YAML='{y}' vs ClickUp='{r}'")
                    if d_diff:
                        y = story.get("depends_on", "(unmanaged)")
                        r = sorted(_cu_waiting_on_ids(cu_task))
                        log.info(f"    depends_on: YAML='{y}' vs ClickUp='{r}'")
                    if r_diff:
                        y = story.get("related", "(unmanaged)")
                        r = sorted(_cu_linked_ids(cu_task))
                        log.info(f"    related: YAML='{y}' vs ClickUp='{r}'")
                    if p_diff:
                        y = story.get("parent", "(unmanaged)")
                        cu_p = cu_task.get("parent")
                        # YAML names its parent, ClickUp only knows the id —
                        # show both sides in the same terms where we can.
                        r = (
                            f"{parent_ctx['name_by_id'].get(str(cu_p), str(cu_p))} "
                            f"({cu_p})" if cu_p else "(top level)"
                        )
                        log.info(f"    parent: YAML='{y}' vs ClickUp='{r}'")
                    stats["mismatches"] += 1
                else:
                    stats["synced"] += 1

    # ClickUp tasks not in YAML
    epic_name_map = build_epic_name_map(data)
    for cu_task in cu_tasks:
        if cu_task["id"] not in yaml_ids:
            epic_key = _extract_epic_name_from_tags(cu_task, epic_name_map)
            tag_label = f"[{data['epics'][epic_name_map[epic_key]]['name']}] " if epic_key else ""
            log.info(f"[PULL NEEDED] {tag_label}'{cu_task['name']}' ({cu_task['id']})")
            stats["need_pull"] += 1

    log.info(f"\n{'='*80}")
    log.info(f"Summary: {stats['need_push']} need push, {stats['need_pull']} need pull, "
             f"{stats['mismatches']} mismatches, {stats['archived']} archived, "
             f"{stats['synced']} synced")
    log.info(f"{'='*80}")
    return stats


def _truncate(s: str, max_len: int) -> str:
    s = s.replace("\n", "\\n")
    if len(s) > max_len:
        return s[:max_len - 3] + "..."
    return s


# ---------------------------------------------------------------------------
# Merge command (LLM-assisted)
# ---------------------------------------------------------------------------


def cmd_merge(
    data: dict,
    yaml_path: str,
    backup_path: Optional[str] = None,
    backup_default: bool = False,
) -> dict:
    token = get_clickup_token()
    openai_key = get_openai_key()
    list_id = data["project"]["clickup_list_id"]
    status_map = data.get("status_map", {})
    stats = {"merged": 0, "skipped": 0, "errors": 0}

    log.info("Fetching all tasks from ClickUp for merge...")
    cu_tasks = clickup_list_tasks(token, list_id)
    cu_by_id = {t["id"]: t for t in cu_tasks}

    _maybe_write_backup(
        data=data,
        cu_tasks=cu_tasks,
        list_id=list_id,
        backup_path=backup_path,
        backup_default=backup_default,
    )

    all_items: list[tuple[dict, dict, str]] = []
    for epic in data["epics"]:
        tag = _epic_tag(epic)
        for story in epic.get("stories", []):
            scu_id = story.get("clickup_id")
            scu_task = cu_by_id.get(scu_id) if scu_id else None
            if scu_task:
                all_items.append((story, scu_task, tag))

    for yaml_task, cu_task, tag in all_items:
        diffs = compare_task(yaml_task, cu_task, status_map)
        if not diffs:
            continue

        task_name = yaml_task.get("name", "unknown")
        label = f"[{tag}]"
        log.info(f"\n--- {label} {task_name} ---")

        for d in diffs:
            field = d["field"]
            yaml_val = str(d["yaml"])
            cu_val = str(d["clickup"])

            log.info(f"  Conflict on '{field}':")
            log.info(f"    YAML:    {_truncate(yaml_val, 80)}")
            log.info(f"    ClickUp: {_truncate(cu_val, 80)}")

            try:
                merged = openai_merge(openai_key, yaml_val, cu_val, task_name, field)
                log.info(f"    LLM merged: {_truncate(str(merged), 80)}")

                choice = input(f"  Accept merge for {field}? [y/n/l(ocal)/r(emote)] ").strip().lower()
                if choice == "y":
                    _apply_merged_value(yaml_task, cu_task, field, merged, status_map, token)
                    stats["merged"] += 1
                elif choice == "l":
                    # Keep local (YAML) value, push to ClickUp
                    _push_field_to_clickup(yaml_task, cu_task, field, status_map, token)
                    log.info(f"    Kept local value, pushed to ClickUp")
                    stats["merged"] += 1
                elif choice == "r":
                    # Keep remote (ClickUp) value, update YAML
                    _pull_field_to_yaml(yaml_task, cu_task, field, status_map)
                    log.info(f"    Kept remote value, updated YAML")
                    stats["merged"] += 1
                else:
                    log.info(f"    Skipped")
                    stats["skipped"] += 1
            except Exception as e:
                log.error(f"    Merge failed: {e}")
                stats["errors"] += 1

    # Relationship-reconcile second pass (dependencies + relations), same as
    # push/sync — so merge also applies YAML-authoritative link edges. Interactive
    # command, so warn before any destructive edge removal.
    _reconcile_edges_pass(token, data, cu_by_id, stats, dry_run=False, warn_on_remove=True)

    save_yaml(data, yaml_path)
    log.info(f"\nMerge complete: {stats['merged']} merged, {stats['skipped']} skipped, "
             f"{stats['errors']} errors")
    return stats


def _apply_merged_value(
    yaml_task: dict,
    cu_task: dict,
    field: str,
    merged_value: str,
    status_map: dict,
    token: str,
) -> None:
    """Apply a merged value to both YAML and ClickUp."""
    cu_id = yaml_task.get("clickup_id") or cu_task.get("id")
    if field == "name":
        yaml_task["name"] = merged_value
        if cu_id:
            clickup_update_task(token, cu_id, {"name": merged_value})
    elif field == "status":
        yaml_task["status"] = clickup_status_to_yaml(merged_value, status_map)
        if cu_id:
            clickup_update_task(token, cu_id, {"status": merged_value})
    elif field == "description":
        yaml_task["description"] = merged_value
        if cu_id:
            clickup_update_task(token, cu_id, {"markdown_content": merged_value})
    elif field == "priority":
        try:
            p = int(merged_value)
        except (ValueError, TypeError):
            p = 3
        yaml_task["priority"] = p
        if cu_id:
            clickup_update_task(token, cu_id, {"priority": p})
    elif field == "milestone":
        is_ms = str(merged_value).lower() in ("true", "1", "yes")
        yaml_task["milestone"] = is_ms
        if cu_id:
            cid = CUSTOM_ITEM_MILESTONE if is_ms else CUSTOM_ITEM_TASK
            clickup_update_task(token, cu_id, {"custom_item_id": cid})
    elif field in ("due_date", "start_date"):
        d = _norm_yaml_date(merged_value)
        yaml_task[field] = d
        if cu_id:
            clickup_update_task(token, cu_id, {
                field: yaml_date_to_clickup_ms(d),
                f"{field}_time": False,
            })


def _push_field_to_clickup(
    yaml_task: dict, cu_task: dict, field: str, status_map: dict, token: str
) -> None:
    """Push a single field from YAML to ClickUp."""
    cu_id = yaml_task.get("clickup_id") or cu_task.get("id")
    if not cu_id:
        return
    if field == "name":
        clickup_update_task(token, cu_id, {"name": yaml_task["name"]})
    elif field == "status":
        clickup_update_task(token, cu_id, {
            "status": yaml_status_to_clickup(yaml_task.get("status", ""), status_map)
        })
    elif field == "description":
        clickup_update_task(token, cu_id, {"markdown_content": description_with_meta(yaml_task)})
    elif field == "priority":
        clickup_update_task(token, cu_id, {"priority": yaml_task.get("priority", 3)})
    elif field == "milestone":
        cid = CUSTOM_ITEM_MILESTONE if yaml_task.get("milestone") else CUSTOM_ITEM_TASK
        clickup_update_task(token, cu_id, {"custom_item_id": cid})
    elif field == "due_date":
        clickup_update_task(token, cu_id, {
            "due_date": yaml_date_to_clickup_ms(yaml_task.get("due_date")),
            "due_date_time": False,
        })
    elif field == "start_date":
        clickup_update_task(token, cu_id, {
            "start_date": yaml_date_to_clickup_ms(yaml_task.get("start_date")),
            "start_date_time": False,
        })


def _pull_field_to_yaml(yaml_task: dict, cu_task: dict, field: str, status_map: dict) -> None:
    """Pull a single field from ClickUp into YAML."""
    if field == "name":
        yaml_task["name"] = cu_task.get("name", "")
    elif field == "status":
        cu_status = cu_task.get("status", {}).get("status", "")
        yaml_task["status"] = clickup_status_to_yaml(cu_status, status_map)
    elif field == "description":
        yaml_task["description"] = strip_meta_prefix(_cu_description(cu_task), yaml_task)
    elif field == "priority":
        yaml_task["priority"] = clickup_priority_to_yaml(cu_task.get("priority"))
    elif field == "milestone":
        yaml_task["milestone"] = _is_clickup_milestone(cu_task)
    elif field == "due_date":
        yaml_task["due_date"] = clickup_ms_to_yaml_date(cu_task.get("due_date"))
    elif field == "start_date":
        yaml_task["start_date"] = clickup_ms_to_yaml_date(cu_task.get("start_date"))


# ---------------------------------------------------------------------------
# Sync command (bidirectional with conflict resolution)
# ---------------------------------------------------------------------------

CONFLICT_STRATEGIES = ("ask", "local", "remote", "merge")


def cmd_sync(
    data: dict,
    yaml_path: str,
    conflict: str = "ask",
    on_conflict: str = "stop",
    dry_run: bool = False,
    backup_path: Optional[str] = None,
    backup_default: bool = False,
) -> dict:
    """Full bidirectional sync.

    When a base snapshot exists (written by a prior push/pull/sync), this is a
    3-way merge: one-sided scalar-field changes auto-resolve toward the side
    that moved; true conflicts (both sides changed the same field) are handled
    per ``on_conflict`` (stop|local|remote). When no base exists yet, it falls
    back to the legacy 2-way reconcile driven by ``conflict`` and writes a base
    for next time. Stories are flat top-level tasks with epic tags."""
    token = get_clickup_token()
    list_id = data["project"]["clickup_list_id"]
    status_map = data.get("status_map", {})
    openai_key = get_openai_key() if conflict == "merge" else None
    stats = {
        "created_in_clickup": 0,
        "created_in_yaml": 0,
        "adopted": 0,
        "duplicates": 0,
        "resolved_local": 0,
        "resolved_remote": 0,
        "resolved_merge": 0,
        "conflicts": 0,
        "skipped": 0,
        "unchanged": 0,
        "archived": 0,
        "errors": 0,
    }

    # Phase 1: Fetch
    log.info("Fetching all tasks from ClickUp...")
    cu_tasks = clickup_list_tasks(token, list_id)
    cu_by_id = {t["id"]: t for t in cu_tasks}
    log.info(f"Fetched {len(cu_tasks)} tasks")

    # Backup-before-sync (opt-in via --backup-to). Default off for sync
    # because the user-facing semantic of sync is "reconcile", not "overwrite".
    if not dry_run:
        _maybe_write_backup(
            data=data,
            cu_tasks=cu_tasks,
            list_id=list_id,
            backup_path=backup_path,
            backup_default=backup_default,
        )

    seen_cu_ids: set[str] = set(t["id"] for t in cu_tasks)
    all_yaml_ids: set[str] = _all_yaml_story_ids(data)
    project_cfg = data.get("project", {})
    managed_universe = managed_tag_universe_for(data, yaml_path, list_id)
    # Dedupe index: lets a create detect a task that already exists in ClickUp
    # (e.g. an orphan from a prior run killed before its id was written back),
    # so a retry adopts it instead of creating a duplicate (BUG #14).
    dedupe_index = _build_create_dedupe_index(cu_tasks)
    push_epic_tag = project_cfg.get("push_epic_tag", True)
    assignee_resolver = _build_assignee_resolver(
        clickup_get_list_members(token, list_id)
    )

    # 3-way base: when present, drives auto-resolution; when absent, fall back
    # to legacy 2-way and write a base at the end for next time.
    try:
        base = load_base_snapshot(base_snapshot_path(yaml_path, list_id))
    except BaseSnapshotCorrupt as e:
        log.error(f"Base snapshot is unreadable: {e}")
        log.error(
            "Refusing to sync — falling back here would silently switch to "
            "interactive 2-way and lose the 3-way safety model. Delete the base "
            "file to re-establish it (a `pull` or `push` rewrites it), then sync again."
        )
        stats["errors"] += 1
        return stats
    base_exists = bool(base)
    if base_exists:
        log.info(f"3-way mode: base snapshot has {len(base)} task(s).")
        conflicts = _collect_3way_conflicts(
            data, cu_by_id, base, status_map, assignee_resolver
        )
        if conflicts and on_conflict == "stop":
            log.info(f"\n{'='*80}")
            log.info(f"SYNC ABORTED — {len(conflicts)} true conflict(s); NO changes made.")
            log.info(f"{'='*80}")
            for c in conflicts:
                log.info(f"  [CONFLICT] '{c['task']}' {c['field']}:")
                log.info(f"      local:  {_truncate(str(c['local']), 60)}")
                log.info(f"      remote: {_truncate(str(c['remote']), 60)}")
            log.info(
                "Resolve each (set the YAML to the intended value, or re-run with "
                "--on-conflict local|remote), then sync again."
            )
            stats["conflicts"] = len(conflicts)
            return stats
    else:
        log.info("No base snapshot yet — 2-way sync this run; base written for next time.")

    # Phase 2 & 4: Walk YAML stories, create or reconcile
    for epic in data["epics"]:
        epic_name = epic["name"]
        tag = _epic_tag(epic)
        epic_priority = epic.get("priority")
        log.info(f"--- Epic {epic.get('number', '?')}: {epic_name} [{tag}] ---")

        for story in epic.get("stories", []):
            story_name = story["name"]
            desired_tags = _story_desired_tags(
                story, epic, push_epic_tag=project_cfg.get("push_epic_tag", True)
            )

            if not story.get("clickup_id"):
                # Dedupe-before-create: if a task with this (name, epic-tag)
                # already exists in ClickUp, adopt it instead of creating a
                # duplicate. Covers the interrupted-retry case (BUG #14) where a
                # prior run created the task but died before the id was written
                # back to YAML — and any pre-existing duplicate already present.
                existing_id = _match_existing_cu_task(
                    dedupe_index, story_name, tag, push_epic_tag
                )
                if existing_id is not None:
                    if not dry_run:
                        story["clickup_id"] = existing_id
                        story["task_id"] = cu_by_id.get(existing_id, {}).get("custom_id")
                        seen_cu_ids.add(existing_id)
                        all_yaml_ids.add(existing_id)  # don't re-import in Phase 3
                        save_yaml(data, yaml_path)  # crash-safe per-task flush
                    log.warning(
                        f"  Adopted existing ClickUp task for '{story_name}' -> "
                        f"{existing_id} (skipped duplicate create)"
                    )
                    stats["adopted"] += 1
                    continue
                # Create in ClickUp
                create_assignee_ids: list[int] = []
                if "assignees" in story:
                    create_assignee_ids, unresolved = _resolve_assignee_ids(
                        story.get("assignees"), assignee_resolver
                    )
                    for u in unresolved:
                        log.warning(f"  Assignee not found, skipping: '{u}'")
                body = build_task_body(
                    story, status_map, tags=desired_tags,
                    default_priority=epic_priority,
                    assignee_ids=create_assignee_ids,
                )
                if dry_run:
                    log.info(
                        f"  [DRY RUN] Would create: {story_name} tags={desired_tags}"
                    )
                else:
                    try:
                        resp = clickup_create_task(token, list_id, body)
                        story["clickup_id"] = resp["id"]
                        story["task_id"] = resp.get("custom_id")
                        # A task created during this run now exists in ClickUp;
                        # record its id so Phase 5 archive-detection doesn't
                        # false-flag it as archived (it was absent from the
                        # pre-run fetch that seeded seen_cu_ids), and so Phase 3
                        # never re-imports it as "new from ClickUp".
                        seen_cu_ids.add(resp["id"])
                        all_yaml_ids.add(resp["id"])
                        # Register the new id so a later same-name story this run
                        # adopts it rather than creating a second copy.
                        dedupe_index.setdefault(
                            _create_dedupe_key(story_name, tag), []
                        ).append(resp["id"])
                        # Flush the id to disk IMMEDIATELY — a run killed mid-loop
                        # must leave a resumable YAML, never an orphan that a
                        # retry re-creates (BUG #14).
                        save_yaml(data, yaml_path)
                        _push_epic_dropdown_if_needed(
                            token, resp["id"], resp, story, epic, project_cfg, dry_run
                        )
                        log.info(
                            f"  Created: {story_name} -> {resp['id']} "
                            f"({resp.get('custom_id')}) tags={desired_tags}"
                        )
                    except Exception as e:
                        log.error(f"  Failed to create {story_name}: {e}")
                        stats["errors"] += 1
                        continue
                stats["created_in_clickup"] += 1
            elif story["clickup_id"] in cu_by_id:
                cu_task = cu_by_id[story["clickup_id"]]
                _sync_metadata(story, cu_task)
                diffs = compare_task(story, cu_task, status_map)
                if diffs:
                    if base_exists:
                        _resolve_conflicts_3way(
                            story, cu_task, base.get(story["clickup_id"], {}),
                            story_name, on_conflict, status_map, token, stats, dry_run,
                        )
                    else:
                        _resolve_conflicts(
                            story, cu_task, diffs, story_name, "Story",
                            conflict, status_map, token, openai_key, stats, dry_run,
                        )
                else:
                    stats["unchanged"] += 1
                # Reconcile multi-tag + Epic dropdown on every existing story.
                if not dry_run:
                    _sync_tags(
                        token,
                        story["clickup_id"],
                        cu_task,
                        desired_tags,
                        managed_known_tags=managed_universe,
                    )
                _push_epic_dropdown_if_needed(
                    token, story["clickup_id"], cu_task, story, epic, project_cfg, dry_run
                )
                # Reconcile assignees per the conflict strategy (set-level):
                # local -> YAML wins, remote -> ClickUp wins, ask/merge ->
                # prompt once when they diverge.
                if not dry_run:
                    # In 3-way mode, assignees aren't base-tracked; the pre-pass
                    # has already flagged any divergence as a conflict (so under
                    # 'stop' we never reach here with a difference). Under
                    # local/remote we apply that direction, but warn loudly since
                    # it can overwrite the side that didn't actually change (M1).
                    assignee_strategy = on_conflict if base_exists else conflict
                    if base_exists and _assignees_differ(story, cu_task, assignee_resolver):
                        log.warning(
                            f"  Assignees on '{story_name}' differ and are NOT base-tracked; "
                            f"applying --on-conflict={on_conflict} may overwrite the unchanged "
                            f"side. local={story.get('assignees', '(unmanaged)')} "
                            f"remote={_cu_assignee_keys(cu_task)}"
                        )
                    _reconcile_assignees_sync(
                        token, story, cu_task, assignee_resolver,
                        assignee_strategy, story_name, stats,
                    )

    # Phase 2.5: relationship-reconcile second pass (dependencies + relations).
    # Mirrors cmd_push — runs after the create/update pass so tasks created this
    # run already have their clickup_id written back. Edges are YAML-authoritative
    # when the key is present (same managed/clear/authoritative rule as push); they
    # are NOT base-tracked 3-way fields, so adding `depends_on`/`related` in YAML
    # is applied here rather than surfaced as a conflict — which is exactly what
    # makes the `sync --dry-run -> sync` workflow cover link edges.
    _reconcile_edges_pass(token, data, cu_by_id, stats, dry_run, warn_on_remove=True)

    # Phase 3: ClickUp tasks not in YAML -> create in YAML
    epic_name_map = build_epic_name_map(data)
    # Rebuilt here, after phase 2 wrote back the ids of tasks created this run,
    # so an imported subtask can name its parent instead of citing its id.
    import_parent_ctx = _build_parent_context(data)
    for cu_task in cu_tasks:
        if cu_task["id"] in all_yaml_ids:
            continue
        new_story = _clickup_task_to_yaml_story(cu_task, status_map, import_parent_ctx)
        epic_key = _extract_epic_name_from_tags(cu_task, epic_name_map)
        if epic_key is not None:
            target_ei = epic_name_map[epic_key]
            if not dry_run:
                data["epics"][target_ei].setdefault("stories", []).append(new_story)
            log.info(f"  New from ClickUp: '{cu_task['name']}' -> epic '{data['epics'][target_ei]['name']}'")
        else:
            orphan = _get_or_create_orphan_epic(data)
            if not dry_run:
                orphan.setdefault("stories", []).append(new_story)
            log.info(f"  Orphan from ClickUp: '{cu_task['name']}'")
        stats["created_in_yaml"] += 1

    # Phase 5: Detect archived stories
    for epic in data["epics"]:
        for story in epic.get("stories", []):
            scu_id = story.get("clickup_id")
            if scu_id and scu_id not in seen_cu_ids:
                if not dry_run:
                    story["archived_in_clickup"] = True
                log.info(f"  Archived: '{story['name']}' ({scu_id})")
                stats["archived"] += 1

    if not dry_run:
        save_yaml(data, yaml_path)

    # Duplicate-detection guard: surface any clickup_id that ended up on more
    # than one story (the BUG #14 symptom) instead of printing a clean summary.
    stats["duplicates"] = _count_yaml_duplicate_ids(data)

    log.info(f"\nSync complete:")
    log.info(f"  Created in ClickUp: {stats['created_in_clickup']}")
    log.info(f"  Created in YAML:    {stats['created_in_yaml']}")
    if stats["adopted"]:
        log.info(f"  Adopted existing:   {stats['adopted']}")
    log.info(f"  Resolved (local):   {stats['resolved_local']}")
    log.info(f"  Resolved (remote):  {stats['resolved_remote']}")
    log.info(f"  Resolved (merge):   {stats['resolved_merge']}")
    log.info(f"  Conflicts:          {stats['conflicts']}")
    log.info(f"  Skipped:            {stats['skipped']}")
    log.info(f"  Unchanged:          {stats['unchanged']}")
    log.info(f"  Archived:           {stats['archived']}")
    log.info(f"  Errors:             {stats['errors']}")
    if stats["duplicates"]:
        log.warning(
            f"  ⚠️  DUPLICATES:      {stats['duplicates']} story row(s) share a "
            f"clickup_id — inspect the YAML and ClickUp for duplicate tasks."
        )
    return stats


def _resolve_conflicts(
    yaml_task: dict,
    cu_task: dict,
    diffs: list[dict],
    task_name: str,
    label: str,
    conflict: str,
    status_map: dict,
    token: str,
    openai_key: Optional[str],
    stats: dict,
    dry_run: bool,
) -> None:
    """Resolve field-level conflicts between YAML and ClickUp for one task."""
    cu_id = yaml_task.get("clickup_id") or cu_task.get("id")

    for d in diffs:
        field = d["field"]
        yaml_val = d["yaml"]
        cu_val = d["clickup"]

        if conflict == "local":
            # YAML wins -> push to ClickUp
            if not dry_run and cu_id:
                _push_field_to_clickup(yaml_task, cu_task, field, status_map, token)
            log.info(f"  {label} '{task_name}' {field}: local wins "
                     f"('{_truncate(str(cu_val), 40)}' -> '{_truncate(str(yaml_val), 40)}')")
            stats["resolved_local"] += 1

        elif conflict == "remote":
            # ClickUp wins -> pull into YAML
            if not dry_run:
                _pull_field_to_yaml(yaml_task, cu_task, field, status_map)
            log.info(f"  {label} '{task_name}' {field}: remote wins "
                     f"('{_truncate(str(yaml_val), 40)}' -> '{_truncate(str(cu_val), 40)}')")
            stats["resolved_remote"] += 1

        elif conflict == "merge":
            # LLM merge
            if not openai_key:
                log.error(f"  No OPENAI_API_KEY for merge on {task_name}.{field}")
                stats["errors"] += 1
                continue
            try:
                merged = openai_merge(openai_key, str(yaml_val), str(cu_val), task_name, field)
                log.info(f"  {label} '{task_name}' {field}:")
                log.info(f"    Local:  {_truncate(str(yaml_val), 60)}")
                log.info(f"    Remote: {_truncate(str(cu_val), 60)}")
                log.info(f"    Merged: {_truncate(str(merged), 60)}")
                choice = input(f"    Accept merge? [y/n/l(ocal)/r(emote)] ").strip().lower()
                if choice == "y":
                    if not dry_run:
                        _apply_merged_value(yaml_task, cu_task, field, merged, status_map, token)
                    stats["resolved_merge"] += 1
                elif choice == "l":
                    if not dry_run and cu_id:
                        _push_field_to_clickup(yaml_task, cu_task, field, status_map, token)
                    stats["resolved_local"] += 1
                elif choice == "r":
                    if not dry_run:
                        _pull_field_to_yaml(yaml_task, cu_task, field, status_map)
                    stats["resolved_remote"] += 1
                else:
                    stats["skipped"] += 1
            except Exception as e:
                log.error(f"  LLM merge failed for {task_name}.{field}: {e}")
                stats["errors"] += 1

        else:
            # "ask" — interactive per-field
            log.info(f"\n  {label} '{task_name}' conflict on '{field}':")
            log.info(f"    [L]ocal (YAML):    {_truncate(str(yaml_val), 60)}")
            log.info(f"    [R]emote (ClickUp): {_truncate(str(cu_val), 60)}")
            prompt_parts = ["l(ocal)", "r(emote)"]
            if openai_key or os.environ.get("OPENAI_API_KEY"):
                prompt_parts.append("m(erge via LLM)")
            prompt_parts.append("s(kip)")
            choice = input(f"    Choose: [{'/'.join(prompt_parts)}] ").strip().lower()

            if choice == "l":
                if not dry_run and cu_id:
                    _push_field_to_clickup(yaml_task, cu_task, field, status_map, token)
                log.info(f"    -> local wins")
                stats["resolved_local"] += 1
            elif choice == "r":
                if not dry_run:
                    _pull_field_to_yaml(yaml_task, cu_task, field, status_map)
                log.info(f"    -> remote wins")
                stats["resolved_remote"] += 1
            elif choice == "m":
                try:
                    oai_key = openai_key or get_openai_key()
                    merged = openai_merge(oai_key, str(yaml_val), str(cu_val), task_name, field)
                    log.info(f"    LLM proposed: {_truncate(str(merged), 60)}")
                    confirm = input(f"    Accept? [y/n] ").strip().lower()
                    if confirm == "y":
                        if not dry_run:
                            _apply_merged_value(yaml_task, cu_task, field, merged, status_map, token)
                        stats["resolved_merge"] += 1
                    else:
                        stats["skipped"] += 1
                except Exception as e:
                    log.error(f"    LLM merge failed: {e}")
                    stats["errors"] += 1
            else:
                log.info(f"    -> skipped")
                stats["skipped"] += 1


def _collect_3way_conflicts(
    data: dict,
    cu_by_id: dict,
    base: dict,
    status_map: dict,
    assignee_resolver: dict,
) -> list[dict]:
    """Read-only pre-pass: list true conflicts across all matched stories.

    A true conflict is a scalar field that changed on BOTH sides since base
    (three_way_plan -> 'conflict'), or an assignee-set divergence (assignees
    aren't base-tracked, so any difference is surfaced rather than silently
    resolved). Used to abort before any mutation when on_conflict='stop'.
    """
    conflicts: list[dict] = []
    for epic in data.get("epics", []):
        for story in epic.get("stories", []):
            cid = story.get("clickup_id")
            if not cid or cid not in cu_by_id:
                continue
            cu_task = cu_by_id[cid]
            base_task = base.get(cid, {})
            plan = three_way_plan(base_task, story, cu_task, status_map)
            loc = comparable_local(story, status_map)
            rem = comparable_remote(cu_task, status_map)
            for field, action in plan.items():
                if action == "conflict":
                    conflicts.append({
                        "task": story.get("name", "?"),
                        "field": field,
                        "local": loc.get(field),
                        "remote": rem.get(field),
                    })
            if _assignees_differ(story, cu_task, assignee_resolver):
                conflicts.append({
                    "task": story.get("name", "?"),
                    "field": "assignees",
                    "local": story.get("assignees", "(unmanaged)"),
                    "remote": _cu_assignee_keys(cu_task),
                })
    return conflicts


def _resolve_conflicts_3way(
    yaml_task: dict,
    cu_task: dict,
    base_task: dict,
    task_name: str,
    on_conflict: str,
    status_map: dict,
    token: str,
    stats: dict,
    dry_run: bool,
) -> None:
    """Apply 3-way directional resolution for one task's scalar fields.

    One-sided change -> auto push/pull; true conflict -> on_conflict policy
    (local=push, remote=pull). 'stop' conflicts never reach here because the
    pre-pass aborts the whole sync first.
    """
    cu_id = yaml_task.get("clickup_id") or cu_task.get("id")
    plan = three_way_plan(base_task, yaml_task, cu_task, status_map)
    for field, action in plan.items():
        if action == "conflict":
            action = {"local": "push", "remote": "pull"}.get(on_conflict)
            if action is None:
                log.info(f"  [conflict] '{task_name}' {field}: skipped (no policy)")
                stats["conflicts"] += 1
                stats["skipped"] += 1
                continue
        if action == "push":
            if not dry_run and cu_id:
                _push_field_to_clickup(yaml_task, cu_task, field, status_map, token)
            log.info(f"  3way '{task_name}' {field}: push (local wins)")
            stats["resolved_local"] += 1
        elif action == "pull":
            if not dry_run:
                _pull_field_to_yaml(yaml_task, cu_task, field, status_map)
            log.info(f"  3way '{task_name}' {field}: pull (remote wins)")
            stats["resolved_remote"] += 1


def _reconcile_assignees_sync(
    token: str,
    story: dict,
    cu_task: dict,
    resolver: dict[str, int],
    conflict: str,
    task_name: str,
    stats: dict,
) -> None:
    """Reconcile assignees during ``sync``, honoring the conflict strategy.

    Assignees are a set, not a scalar, so resolution is at the whole-set level
    (one decision per task) rather than per ClickUp user.
    """
    if not _assignees_differ(story, cu_task, resolver):
        return

    if conflict == "local":
        if _sync_assignees(token, story["clickup_id"], cu_task, story, resolver):
            log.info(f"  Assignees on '{task_name}': local wins")
            stats["resolved_local"] += 1
    elif conflict == "remote":
        if _pull_assignees(story, cu_task):
            log.info(f"  Assignees on '{task_name}': remote wins")
            stats["resolved_remote"] += 1
    else:  # "ask" / "merge" — prompt once at the set level
        y = story.get("assignees", "(unmanaged)")
        r = _cu_assignee_keys(cu_task)
        log.info(f"\n  Assignees differ on '{task_name}':")
        log.info(f"    [L]ocal (YAML):     {y}")
        log.info(f"    [R]emote (ClickUp): {r}")
        choice = input("    Choose: [l(ocal)/r(emote)/s(kip)] ").strip().lower()
        if choice == "l":
            if _sync_assignees(token, story["clickup_id"], cu_task, story, resolver):
                stats["resolved_local"] += 1
        elif choice == "r":
            if _pull_assignees(story, cu_task):
                stats["resolved_remote"] += 1
        else:
            stats["skipped"] += 1


# ---------------------------------------------------------------------------
# Status command (offline)
# ---------------------------------------------------------------------------


def cmd_status(data: dict) -> None:
    project = data["project"]
    status_map = data.get("status_map", {})

    print(f"\nProject: {project['name']}")
    print(f"ClickUp List: {project['clickup_list_id']}")
    print(f"Last Synced: {project.get('last_synced') or 'never'}")

    total_points = 0
    total_stories = 0
    done_points = 0
    synced_count = 0

    header = (f"{'#':>3} {'Task ID':<11} {'Epic':<40} {'Status':<16} "
              f"{'Stories':>7} {'Points':>6} {'Synced':>6} {'Sprint':>6} {'MS':>2}")
    print(f"\n{header}")
    print("-" * len(header))

    for epic in data["epics"]:
        num = epic.get("number", "?")
        task_id = epic.get("task_id") or "-"
        name = epic["name"][:39]
        status = epic.get("status", "?")
        stories = epic.get("stories", [])
        points = epic.get("points", 0)
        sprint = epic.get("sprint") or "-"
        n_stories = len(stories)
        synced_stories = sum(1 for s in stories if s.get("clickup_id"))
        synced_label = f"{synced_stories}/{n_stories}" if n_stories else "-"
        ms_count = sum(1 for s in stories if s.get("milestone"))
        ms_flag = str(ms_count) if ms_count else ""

        total_points += points
        total_stories += n_stories
        synced_count += synced_stories
        if status == "done":
            done_points += points

        print(f"{str(num):>3} {task_id:<11} {name:<40} {status:<16} "
              f"{n_stories:>7} {points:>6} {synced_label:>6} {str(sprint):>6} {ms_flag:>2}")

    total_items = total_stories
    print("-" * len(header))
    print(f"{'':>3} {'':>11} {'TOTAL':<40} {'':>16} "
          f"{total_stories:>7} {total_points:>6} {synced_count}/{total_items}  ")
    print(f"\nDone: {done_points}/{total_points} points "
          f"({done_points * 100 // total_points if total_points else 0}%)")

    # Status breakdown
    status_counts: dict[str, int] = {}
    for epic in data["epics"]:
        s = epic.get("status", "unknown")
        status_counts[s] = status_counts.get(s, 0) + 1
        for story in epic.get("stories", []):
            ss = story.get("status", "unknown")
            status_counts[ss] = status_counts.get(ss, 0) + 1

    print("\nStatus breakdown:")
    for s, count in sorted(status_counts.items()):
        print(f"  {s}: {count}")


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Milestone-date lint
# ---------------------------------------------------------------------------
#
# A card tagged to a milestone should be due on or before that milestone's own
# due date. If the work has to be finished before a gate, a date after the gate
# is a contradiction.
#
# This is a LINT, not a constraint, and the distinction is deliberate. ClickUp
# enforces nothing here: a "milestone" is a task *type* on an ordinary task,
# rendered as a diamond at a point in time. It contains nothing and groups
# nothing -- the only structural relationships ClickUp has are dependencies and
# subtasks. So the slug tag is the ONLY association that exists between a card
# and its gate, and a date check over that tag is the only way to catch an
# incoherent plan.
#
# Three rules it follows, and each exists for a reason:
#
#   1. It flags; it never modifies. A date is a human's decision.
#   2. Findings can be accepted per-card, with a written reason. A date set in
#      the ClickUp UI arrives here via `pull` and is legitimate data, not
#      necessarily a mistake -- so there has to be a way to say "yes, we know"
#      that does not nag forever and does not lose why.
#   3. Missing data is silent. Most cards have no due date at all; an undated
#      card is not a violation and must not be reported as one.
#
# It is never a blocker. A lint that stops a sync over a guideline gets
# bypassed, and a bypassed lint is worse than no lint. `lint --strict` is the
# opt-in for anyone who does want a non-zero exit (CI, a pre-merge gate).
#
# Note it does NOT look at epics. Epics are a YAML-only grouping and a diamond
# is deliberately not filed under a work epic, so no milestone relationship can
# be inferred from one.

# Codes are stable: they are what a card's `lint_exceptions:` mapping is keyed
# on, so renaming one silently un-suppresses every acceptance that used it.
LINT_DATE_AFTER_GATE = "milestone-date"
LINT_TAG_UNRESOLVED = "milestone-tag-unresolved"
LINT_SLUG_MISMATCH = "milestone-slug-mismatch"
LINT_AMBIGUOUS = "milestone-ambiguous"
LINT_GATE_UNDATED = "milestone-gate-undated"
LINT_LABEL_MALFORMED = "milestone-label-malformed"

LINT_CODES = (
    LINT_DATE_AFTER_GATE,
    LINT_TAG_UNRESOLVED,
    LINT_SLUG_MISMATCH,
    LINT_AMBIGUOUS,
    LINT_GATE_UNDATED,
    LINT_LABEL_MALFORMED,
)


def _milestone_refs(story: dict) -> list[tuple[int, Optional[str], str]]:
    """Milestone references on a story, as ``(number, slug, original_tag)``.

    Reads both ``tags:`` and ``milestone_label`` -- push lowercases the label
    into the same tag namespace, so ``milestone_label: M1-infrastructure`` and
    ``tags: [m1-infrastructure]`` are the same reference by different routes.
    One story may carry more than one.
    """
    seen: set[str] = set()
    refs: list[tuple[int, Optional[str], str]] = []
    candidates = [t for t in (story.get("tags") or []) if isinstance(t, str)]
    label = story.get("milestone_label")
    if isinstance(label, str) and label.strip():
        candidates.append(label.strip())
    for raw in candidates:
        tag = raw.strip().lower()
        if tag in seen:
            continue
        m = MILESTONE_TAG_RE.match(tag)
        if m:
            seen.add(tag)
            refs.append((int(m.group(1)), m.group(2), tag))
    return refs


def _lint_exception(story: dict, code: str) -> Optional[str]:
    """The written reason this card accepts ``code``, or None.

    A reason string, deliberately, not a boolean: a bare suppression flag tells
    the next reader nothing about why the contradiction is fine, and by the time
    anyone asks, whoever set it has moved on. An empty or non-string value is
    treated as NOT an exception -- silently accepting `true` would let the
    self-documenting requirement rot away one card at a time.
    """
    block = story.get("lint_exceptions")
    if not isinstance(block, dict):
        return None
    reason = block.get(code)
    if isinstance(reason, str) and reason.strip():
        return reason.strip()
    return None


def _lint_finding(code: str, severity: str, epic: dict, story: dict, message: str) -> dict:
    return {
        "code": code,
        "severity": severity,
        "epic": epic.get("name", "?"),
        "story": story.get("name", "?"),
        "clickup_id": story.get("clickup_id"),
        "message": message,
    }


def _milestone_index(data: dict) -> tuple[dict, list[dict]]:
    """Map every milestone card's number -> the cards claiming it, and report
    the diamonds that carry no milestone tag at all (they cannot be referenced,
    so nothing can ever resolve to them)."""
    by_number: dict[int, list[dict]] = {}
    untagged: list[dict] = []
    for epic in data.get("epics", []) or []:
        for story in epic.get("stories", []) or []:
            if not story.get("milestone"):
                continue
            refs = _milestone_refs(story)
            if not refs:
                untagged.append({"epic": epic, "story": story})
                continue
            for number, slug, tag in refs:
                by_number.setdefault(number, []).append(
                    {"epic": epic, "story": story, "slug": slug, "tag": tag}
                )
    return by_number, untagged


def lint_milestone_dates(data: dict) -> dict:
    """Return ``{"findings": [...], "accepted": [...]}``.

    ``accepted`` holds findings a card explicitly accepted with a reason. They
    are kept rather than dropped so the report can say how many there are
    without listing them -- suppressed is not the same as gone.
    """
    findings: list[dict] = []
    accepted: list[dict] = []

    def _record(code: str, severity: str, epic: dict, story: dict, message: str) -> None:
        finding = _lint_finding(code, severity, epic, story, message)
        reason = _lint_exception(story, code)
        if reason:
            finding["accepted_because"] = reason
            accepted.append(finding)
        else:
            findings.append(finding)

    # A malformed label is worth catching at the source: it pushes a garbage tag
    # to ClickUp and then resolves to no gate, so the card looks tagged and is
    # silently unchecked.
    for epic in data.get("epics", []) or []:
        for story in epic.get("stories", []) or []:
            label = story.get("milestone_label")
            if isinstance(label, str) and label.strip() and not MILESTONE_TAG_RE.match(label.strip()):
                _record(
                    LINT_LABEL_MALFORMED, "warning", epic, story,
                    f"has milestone_label '{label.strip()}', which is not of the "
                    f"form M<n> or M<n>-<slug> (e.g. 'M1', 'M1-infrastructure'). "
                    f"It would push as a tag nothing can resolve.",
                )

    by_number, untagged = _milestone_index(data)

    for entry in untagged:
        _record(
            LINT_TAG_UNRESOLVED, "warning", entry["epic"], entry["story"],
            "is a milestone but carries no m<n> tag, so no card can be tied to "
            "it. Add a tag like 'm1-infrastructure'.",
        )

    # A gate with no date silently disables the check for every card pointing at
    # it -- reported once per gate, not once per card, and not as a violation.
    for number, holders in sorted(by_number.items()):
        for holder in holders:
            if _norm_yaml_date(holder["story"].get("due_date")) is None:
                _record(
                    LINT_GATE_UNDATED, "info", holder["epic"], holder["story"],
                    f"is the m{number} gate but has no due_date, so no card "
                    f"tagged m{number} can be date-checked against it.",
                )

    for number, holders in sorted(by_number.items()):
        if len(holders) > 1:
            names = ", ".join(sorted(f"'{h['story'].get('name', '?')}'" for h in holders))
            for holder in holders:
                _record(
                    LINT_AMBIGUOUS, "warning", holder["epic"], holder["story"],
                    f"shares milestone number m{number} with another milestone "
                    f"card ({names}). A card tagged m{number} cannot be resolved "
                    f"to one gate.",
                )

    for epic in data.get("epics", []) or []:
        for story in epic.get("stories", []) or []:
            if story.get("milestone"):
                continue  # a gate is not checked against itself
            for number, slug, tag in _milestone_refs(story):
                holders = by_number.get(number) or []
                if not holders:
                    _record(
                        LINT_TAG_UNRESOLVED, "warning", epic, story,
                        f"is tagged '{tag}' but no milestone card carries m{number}. "
                        f"Typo, or the gate has not been created yet.",
                    )
                    continue
                if len(holders) > 1:
                    continue  # already reported as ambiguous on the gates
                gate = holders[0]
                if slug and gate["slug"] and slug != gate["slug"]:
                    _record(
                        LINT_SLUG_MISMATCH, "warning", epic, story,
                        f"is tagged '{tag}' but the m{number} gate "
                        f"'{gate['story'].get('name', '?')}' is tagged "
                        f"'{gate['tag']}'. Same number, different slug -- likely "
                        f"a typo. Checked against that gate anyway.",
                    )
                gate_due = _norm_yaml_date(gate["story"].get("due_date"))
                story_due = _norm_yaml_date(story.get("due_date"))
                # Missing data is silent, on either side. Most cards have no due
                # date, and that is not a contradiction.
                if gate_due is None or story_due is None:
                    continue
                if story_due > gate_due:
                    _record(
                        LINT_DATE_AFTER_GATE, "error", epic, story,
                        f"is due {story_due}, after the m{number} gate "
                        f"'{gate['story'].get('name', '?')}' on {gate_due}. Work "
                        f"needed for a gate cannot be due after it.",
                    )

    return {"findings": findings, "accepted": accepted}


def print_lint_report(result: dict, *, only_if_findings: bool = True) -> None:
    """Print the lint report. Advisory tone throughout: this reports a
    guideline, and phrasing it as a failure invites someone to route around it."""
    findings = result.get("findings") or []
    accepted = result.get("accepted") or []
    if not findings and only_if_findings:
        if accepted:
            print(
                f"\nMilestone-date lint: clean "
                f"({len(accepted)} accepted exception(s))."
            )
        return
    if not findings:
        print("\nMilestone-date lint: clean.")
        return

    order = {"error": 0, "warning": 1, "info": 2}
    print("\n" + "=" * 68)
    print("MILESTONE-DATE LINT (advisory -- nothing was changed)")
    print("=" * 68)
    for f in sorted(findings, key=lambda f: (order.get(f["severity"], 9), f["epic"], f["story"])):
        label = {"error": "CONTRADICTION", "warning": "WARNING", "info": "note"}[f["severity"]]
        print(f"  [{label}] {f['epic']} / '{f['story']}'")
        print(f"      {f['message']}")
        print(f"      (accept with lint_exceptions: {{{f['code']}: \"<why>\"}})")
    if accepted:
        print(f"\n  {len(accepted)} finding(s) accepted on the card with a written reason.")
    print("=" * 68)


def cmd_lint(data: dict, strict: bool = False) -> int:
    result = lint_milestone_dates(data)
    print_lint_report(result, only_if_findings=False)
    if strict and result["findings"]:
        # Opt-in only. The default stays zero because this is a guideline, and a
        # guideline that breaks builds gets deleted rather than obeyed.
        return 1
    return 0


# ---------------------------------------------------------------------------
# with-lock: run any command inside the project's lock
# ---------------------------------------------------------------------------


def cmd_with_lock(argv: list[str]) -> int:
    """Run ``argv`` while holding this project's locks, then release them.

    This is the edit path. `clickup.py` is the only supported writer of a task
    file, but "writer" does not have to mean "editor": editing without syncing
    is a normal workflow -- you stage cards, look at them, and publish later --
    so the tool wraps whatever you would have used anyway ($EDITOR, a script, a
    shell, a Claude session) instead of reimplementing text editing inside a
    sync tool.

        clickup.py with-lock docs/project-tasks.yaml -- $EDITOR docs/project-tasks.yaml
        clickup.py with-lock docs/project-tasks.yaml -- ./restructure.sh

    The child inherits ``CLICKUP_LOCK_OWNER``, so a nested ``clickup.py sync``
    joins this hold rather than blocking on it. Without that a plain shell would
    deadlock against itself: outside Claude Code each process mints its own
    unique owner id, and the nested run would see a fresh lock it does not
    recognise. Under Claude Code the identity is the session id either way, so
    the hook firing on an edit inside the wrapper also sees its own lock and
    allows it.

    Returns the child's exit code, so the wrapper is transparent to callers.
    """
    env = dict(os.environ)
    env["CLICKUP_LOCK_OWNER"] = os.environ.get("CLICKUP_LOCK_OWNER") or lock_owner_id()
    try:
        proc = subprocess.run(argv, env=env)
    except FileNotFoundError:
        log.error(f"with-lock: command not found: {argv[0]}")
        return 127
    except PermissionError:
        log.error(f"with-lock: not executable: {argv[0]}")
        return 126
    return proc.returncode


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Bidirectional sync between YAML project files and ClickUp",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "command",
        choices=["push", "pull", "diff", "sync", "merge", "status", "lint",
                 "with-lock", "pin"],
        help="Command to execute",
    )
    parser.add_argument(
        "yaml_file",
        nargs="?",
        help="Path to the YAML project file (not needed for 'pin')",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would happen without making changes (push/pull/sync)",
    )
    parser.add_argument(
        "--conflict",
        choices=CONFLICT_STRATEGIES,
        default="ask",
        help="Legacy 2-way strategy for sync when no base snapshot exists yet "
             "(default: ask)",
    )
    parser.add_argument(
        "--on-conflict",
        choices=("stop", "local", "remote"),
        default="stop",
        help="3-way true-conflict policy for sync when a base exists "
             "(stop=abort & report, local=YAML wins, remote=ClickUp wins; "
             "default: stop)",
    )
    parser.add_argument(
        "--backup-to",
        nargs="?",
        const=BACKUP_DEFAULT_SENTINEL,
        default=None,
        help=(
            "Override the backup location written before a modifying run. For "
            "push/sync/merge this is the ClickUp-state snapshot "
            "(~/tmp/clickup-backup-<list_id>-<iso>.yaml by default); for pull it "
            "is the YAML-file copy (.clickup-sync/yaml-backup-<stem>-<iso>.yaml by "
            "default). Pass a path, or omit the value for the default. "
            "Auto-on for non-empty lists; --no-backup opts out."
        ),
    )
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="Disable the automatic backup-before-push (ClickUp snapshot) and "
             "backup-before-pull (YAML-file copy).",
    )
    parser.add_argument(
        "--version",
        action=_VersionAction,
        help="Print which clickup.py this is (path, commit, clean/modified, "
             "content hash) and exit, so a run can be accounted for afterwards.",
    )
    parser.add_argument(
        "--allow-dirty",
        action="store_true",
        help="Run a writing command from an uncommitted clickup.py. The run is "
             "still fully recorded -- its exact content hash is logged and it is "
             "stamped as a bypass. Marked, not hidden. Prefer 'clickup.py pin'.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="lint only: exit non-zero when the milestone-date lint has "
             "findings. Off by default -- the lint reports a guideline, and a "
             "guideline that breaks builds gets routed around rather than "
             "obeyed. Opt in for CI or a pre-merge gate.",
    )
    parser.add_argument(
        "--no-lock",
        action="store_true",
        help="Skip the advisory lock entirely. The escape hatch, for when a "
             "lock is known-wrong and waiting it out is not an option; you are "
             "then responsible for knowing nothing else is writing. Also "
             "settable as CLICKUP_NO_LOCK=1.",
    )
    parser.add_argument(
        "--lock-timeout",
        type=float,
        default=LOCK_WAIT_DEFAULT_SECONDS,
        metavar="SECONDS",
        help=f"How long to wait for a held lock before failing loudly "
             f"(default: {LOCK_WAIT_DEFAULT_SECONDS:.0f}s). Never skips the "
             f"work silently -- a timeout is a non-zero exit.",
    )
    parser.add_argument(
        "--sandbox",
        action="store_true",
        help="Use the sandbox ClickUp account (token from pass "
             "clickup/sandbox-api-token, env CLICKUP_API_TOKEN_SANDBOX, or "
             "~/bin/clickup-sandbox.env) instead of production.",
    )

    # Split the with-lock payload off BEFORE argparse sees it. argparse's
    # REMAINDER would swallow trailing options too, breaking the long-standing
    # `clickup.py sync <file> --dry-run` form.
    argv = sys.argv[1:]
    rest: list[str] = []
    if "--" in argv:
        cut = argv.index("--")
        argv, rest = argv[:cut], argv[cut + 1:]

    args = parser.parse_args(argv)

    if args.sandbox:
        os.environ["CLICKUP_SANDBOX"] = "1"

    if args.command == "pin":
        sys.exit(cmd_pin())

    if not args.yaml_file:
        parser.error(f"{args.command} needs a YAML file")
    if not os.path.exists(args.yaml_file):
        log.error(f"YAML file not found: {args.yaml_file}")
        sys.exit(1)

    # Provenance is stated on EVERY run, before anything is touched, so the log
    # of any run says what produced it.
    prov = tool_provenance()
    bypassed = bool(args.allow_dirty and prov.get("dirty")
                    and args.command in WRITING_COMMANDS)
    log.info(format_provenance(prov, bypassed=bypassed))
    if bypassed:
        # Prominent and separate from the info line: a bypassed run must be
        # obvious to anyone scanning output, not just present in a log.
        print(
            "\n!! --allow-dirty: running UNTESTED, uncommitted code. "
            f"sha256 {(prov.get('sha256') or '?')[:12]} !!\n",
            file=sys.stderr, flush=True,
        )
    try:
        assert_attributable(prov, args.command, args.allow_dirty)
    except RuntimeError as e:
        print(f"ERROR: {e}", file=sys.stderr, flush=True)
        log.error(str(e))
        sys.exit(PROVENANCE_EXIT_CODE)
    head_before = prov.get("commit")

    if args.command == "with-lock":
        if not rest:
            parser.error(
                "with-lock needs a command to run: "
                "clickup.py with-lock <file> -- <command> [args...]"
            )
    elif rest:
        parser.error(
            f"only with-lock takes a '--' command; got: {' '.join(rest)}"
        )

    # Which commands need the lock, and why the others do not:
    #   status  -- offline, reads the YAML and prints. No writes anywhere.
    #   diff    -- reads YAML and ClickUp, writes neither.
    # Everything else writes the YAML (clickup_id flush, last_synced stamp,
    # pulled story rows) and the .clickup-sync/ base snapshot, and with-lock
    # exists precisely to hold the lock for someone else.
    needs_lock = args.command not in ("status", "diff", "lint")
    if args.dry_run and args.command != "with-lock":
        # A dry run writes nothing, so making it queue behind (or block) a real
        # run costs more than it buys. It can therefore read a file another
        # process is mid-way through rewriting -- the plan it prints is a
        # preview, and the real run that follows takes the lock properly.
        needs_lock = False
        log.info("Dry run: not taking the lock (nothing will be written).")
    if os.environ.get("CLICKUP_NO_LOCK", "").strip() not in ("", "0"):
        args.no_lock = True
    if args.no_lock and needs_lock:
        needs_lock = False
        log.warning(
            "--no-lock: running without the advisory lock. Nothing is stopping "
            "another session from writing this file at the same time."
        )

    def _run() -> int:
        return _dispatch(args, rest, head_before)

    if not needs_lock:
        sys.exit(_run())

    # The list id is read cheaply and separately from load_yaml, because the
    # lock has to be held BEFORE the file is read -- reading first would mean
    # acting on a snapshot taken outside the protected span.
    list_id = _peek_list_id(args.yaml_file)
    try:
        with SyncLock(args.yaml_file, list_id, wait_seconds=args.lock_timeout):
            sys.exit(_run())
    except LockBusy as e:
        # Loud and non-zero. Never a quiet no-op: a sync that silently does
        # nothing is the failure mode that hides for days.
        print(f"ERROR: {e}", file=sys.stderr, flush=True)
        log.error(f"Lock busy, aborting: {e}")
        sys.exit(LOCK_EXIT_CODE)


def _peek_list_id(yaml_path: str) -> Optional[str]:
    """Read just ``project.clickup_list_id``, tolerating anything unreadable --
    a malformed file is load_yaml's error to report, not the locker's."""
    try:
        with open(yaml_path) as f:
            doc = yaml.safe_load(f)
        list_id = (doc or {}).get("project", {}).get("clickup_list_id")
        return str(list_id) if list_id else None
    except Exception:
        return None


def _dispatch(args: argparse.Namespace, rest: list[str], head_before: Optional[str] = None) -> int:
    if args.command == "with-lock":
        return cmd_with_lock(rest)

    data = load_yaml(args.yaml_file)

    if args.command == "lint":
        return cmd_lint(data, strict=args.strict)

    if args.command == "status":
        cmd_status(data)
    elif args.command == "push":
        cmd_push(
            data,
            args.yaml_file,
            dry_run=args.dry_run,
            backup_path=args.backup_to,
            backup_default=not args.no_backup,
        )
    elif args.command == "pull":
        cmd_pull(
            data,
            args.yaml_file,
            dry_run=args.dry_run,
            backup_default=not args.no_backup,
            backup_path=args.backup_to,
        )
    elif args.command == "diff":
        cmd_diff(data)
    elif args.command == "sync":
        cmd_sync(
            data,
            args.yaml_file,
            conflict=args.conflict,
            on_conflict=args.on_conflict,
            dry_run=args.dry_run,
            backup_path=args.backup_to,
            backup_default=False,
        )
    elif args.command == "merge":
        cmd_merge(
            data,
            args.yaml_file,
            backup_path=args.backup_to,
            backup_default=False,
        )

    # Advisory tail on every command, run against the POST-run state so it
    # reflects what the YAML actually says now (a pull can bring back a UI date
    # that contradicts a gate -- that is exactly the case worth seeing). Never
    # changes the exit code: this reports a guideline, not a failure. Wrapped
    # because a lint defect must not fail an otherwise-good sync.
    # Edge failures first: a board missing its declared dependencies is a
    # structural problem, and it must not be buried under a lint report.
    report_edge_failures()
    warn_if_head_moved(head_before, args.command)
    try:
        print_lint_report(lint_milestone_dates(data))
    except Exception as e:  # pragma: no cover - defensive
        log.warning(f"Milestone-date lint could not run: {e}")
    return 0


if __name__ == "__main__":
    main()
