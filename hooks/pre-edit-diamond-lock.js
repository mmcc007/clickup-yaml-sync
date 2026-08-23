#!/usr/bin/env node
/**
 * project-tasks.yaml edit lock (PreToolUse - Edit / Write / MultiEdit)
 *
 * Advisory mutex that stops two concurrent Claude sessions from editing a
 * project task file at the same time — the lost-update hazard that bit us on
 * 2026-07-10 (two sessions rewrote the same task entries; only manual vigilance
 * avoided a clobber).
 *
 * THIS HOOK IS THE BACKSTOP, NOT THE PRIMARY MECHANISM (since 2026-08-21).
 * `clickup.py` (the clickup-yaml-sync tool that owns this file format) now
 * takes the SAME lock itself — same path, same JSON, same TTL, and under Claude
 * Code the same session_id — across a whole acquire → edit → sync → release
 * transaction. That covers the writers a hook structurally cannot see: the
 * tool's own writeback, a Bash `sed`, a cron, a human at a shell.
 *
 * What remains for this hook is the case the tool cannot see either: an
 * Edit/Write made WITHOUT going through the tool. Both are needed.
 *
 * NOBODY SHOULD EVER HAND-WRITE THIS LOCK FILE. A hand-written lock carries an
 * identity nothing else can match, so it locks out its own author.
 *
 * "Diamond" here is EM Marketing's nickname for the TASK FILE. It has nothing
 * to do with ClickUp milestone diamonds, which are a different thing entirely
 * and now appear on real boards — hence the renamed title above.
 *
 * Behaviour:
 *   - Guards ONLY files whose basename is `project-tasks.yaml`; anything else
 *     passes straight through (exit 0), so this hook is invisible elsewhere.
 *   - Records every lock it claims in a per-session registry so the companion
 *     Stop hook can release ALL of them, not just one hardcoded path.
 *   - Lock lives next to the target: `<dir>/.project-tasks.lock`, holding JSON
 *     { session_id, ts, pid }.
 *   - If a *different* session holds a *fresh* lock (age < TTL) -> BLOCK (exit 2)
 *     and tell this session to wait. Otherwise acquire/refresh it (exit 0).
 *   - The lock is refreshed on every edit, so an actively-editing session keeps
 *     it, and it auto-expires TTL after the last edit — a session that moved on
 *     frees the file without a clean exit. The companion Stop hook
 *     (stop-diamond-lock-release.js) releases it immediately on session end.
 *
 * TTL is deliberately short (5m): long enough to cover think-time between a
 * burst of related edits, short enough that a finished session frees the file
 * fast. Tune LOCK_TTL_MS if your edit bursts routinely span longer gaps.
 *
 * This is ADVISORY (a tiny TOCTOU window remains) — pair it with the
 * worktree-per-session discipline (git as the real arbiter) for belt + braces.
 *
 * Exit codes: 0 = allow, 2 = block (Claude sees stderr and skips the tool call).
 */
'use strict';

const fs = require('fs');
const os = require('os');
const path = require('path');

const GUARDED_BASENAME = 'project-tasks.yaml';
const LOCK_BASENAME = '.project-tasks.lock';
const LOCK_TTL_MS = 5 * 60 * 1000;
const MAX_STDIN = 1024 * 1024;

// Per-session registry of the lock paths this session has claimed. The Stop
// hook reads it to release every one of them on session end.
//
// Before this existed, the Stop hook released a single HARDCODED path (EM
// Marketing's), so every OTHER project's lock waited out the full 5-minute TTL
// after a session finished — blocking a sibling session for five minutes on a
// file nobody was editing. That cost became real when a second client board
// got a task file.
//
// Overridable for tests.
const REGISTRY_DIR =
  process.env.DIAMOND_LOCK_REGISTRY_DIR ||
  path.join(os.homedir(), '.cache', 'claude-diamond-locks');

function registryPath(sessionId) {
  // Session ids are UUIDs; sanitise anyway so a hostile value cannot escape the
  // directory.
  const safe = String(sessionId).replace(/[^A-Za-z0-9._-]/g, '_').slice(0, 128);
  return path.join(REGISTRY_DIR, `${safe}.json`);
}

function recordClaim(sessionId, lockPath) {
  // Best-effort: failing to record must never block an edit. The TTL remains
  // the backstop if this does not persist.
  try {
    const file = registryPath(sessionId);
    let claimed = [];
    try {
      const parsed = JSON.parse(fs.readFileSync(file, 'utf8'));
      if (Array.isArray(parsed)) claimed = parsed;
    } catch { /* absent or unreadable -> start fresh */ }
    if (!claimed.includes(lockPath)) {
      claimed.push(lockPath);
      fs.mkdirSync(REGISTRY_DIR, { recursive: true });
      fs.writeFileSync(file, JSON.stringify(claimed) + '\n');
    }
  } catch { /* ignore */ }
}

/**
 * Exportable run() for in-process execution via run-with-flags.js
 * (mirrors doc-file-warning.js). Returns { exitCode, stderr? }.
 */
function run(inputOrRaw, _options = {}) {
  let input;
  try {
    input = typeof inputOrRaw === 'string'
      ? (inputOrRaw.trim() ? JSON.parse(inputOrRaw) : {})
      : (inputOrRaw || {});
  } catch {
    return { exitCode: 0 };
  }

  const filePath = String(input?.tool_input?.file_path || '');
  if (!filePath) return { exitCode: 0 };

  const normalized = filePath.replace(/\\/g, '/');
  if (path.basename(normalized) !== GUARDED_BASENAME) return { exitCode: 0 };

  const sessionId = String(input?.session_id || `pid-${process.ppid}`);
  const lockPath = path.join(path.dirname(filePath), LOCK_BASENAME);
  const now = Date.now();

  let lock = null;
  try {
    lock = JSON.parse(fs.readFileSync(lockPath, 'utf8'));
  } catch { /* no lock, or unreadable/invalid -> treat as free */ }

  const heldByOther = lock && lock.session_id && String(lock.session_id) !== sessionId;
  const ageMs = lock ? now - (Number(lock.ts) || 0) : Infinity;
  const fresh = ageMs < LOCK_TTL_MS;

  if (heldByOther && fresh) {
    const ageMin = Math.max(0, Math.round(ageMs / 60000));
    const expiresMin = Math.max(0, Math.ceil((LOCK_TTL_MS - ageMs) / 60000));
    return {
      exitCode: 2,
      stderr:
        '[Hook] BLOCKED: another Claude session is editing the diamond file (project-tasks.yaml).\n' +
        `[Hook]   held by session ${String(lock.session_id).slice(0, 8)} · last edit ${ageMin}m ago · TTL 5m\n` +
        '[Hook]   Concurrent edits to this file cause lost updates (see 2026-07-10).\n' +
        '[Hook]   Wait for that session to finish + commit, then retry.\n' +
        `[Hook]   If it crashed, the lock self-expires in ~${expiresMin}m, or delete: ${lockPath}`,
    };
  }

  // Free / stale / our own -> acquire or refresh. Best-effort: never block edits
  // just because the lock could not be written.
  try {
    fs.writeFileSync(
      lockPath,
      JSON.stringify({ session_id: sessionId, ts: now, pid: process.ppid }) + '\n'
    );
    recordClaim(sessionId, lockPath);
  } catch { /* ignore */ }

  return { exitCode: 0 };
}

module.exports = { run };

// Stdin fallback for spawnSync execution.
let data = '';
process.stdin.setEncoding('utf8');
process.stdin.on('data', c => {
  if (data.length < MAX_STDIN) data += c.substring(0, MAX_STDIN - data.length);
});
process.stdin.on('end', () => {
  const result = run(data);
  if (result.stderr) process.stderr.write(result.stderr + '\n');
  if (result.exitCode === 2) process.exit(2);
  process.stdout.write(data);
});
