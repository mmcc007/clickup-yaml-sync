#!/usr/bin/env node
/**
 * project-tasks.yaml lock release (Stop hook)
 *
 * Companion to pre-edit-diamond-lock.js. On session end, release EVERY lock
 * this session claimed, so those files are immediately free instead of waiting
 * out the 5-minute TTL.
 *
 * It used to release ONE HARDCODED PATH (EM Marketing's), so every other
 * project's lock sat for the full TTL after its session finished — blocking a
 * sibling for five minutes on a file nobody was editing. That was harmless
 * while only one board had a task file and stopped being harmless the moment a
 * second one did. The pre-edit hook now records each lock it claims in a
 * per-session registry, and this reads it.
 *
 * A lock is only removed if it still names THIS session: if it expired and
 * another session took it over, deleting it would strip a lock off a run that
 * is actively using it.
 *
 * "Diamond" is EM Marketing's nickname for the TASK FILE, not a ClickUp
 * milestone diamond — unrelated things, hence the renamed title.
 *
 * Exit code always 0 — never blocks session end.
 */
'use strict';

const fs = require('fs');
const os = require('os');
const path = require('path');

// Where pre-edit-diamond-lock.js records the locks each session claims.
const REGISTRY_DIR =
  process.env.DIAMOND_LOCK_REGISTRY_DIR ||
  path.join(os.homedir(), '.cache', 'claude-diamond-locks');

// Legacy single path, still released for sessions that predate the registry
// (their claims were never recorded, so the registry is empty for them).
// Overridable via DIAMOND_LOCK_PATH (used by tests).
const LEGACY_LOCK =
  process.env.DIAMOND_LOCK_PATH ||
  '/home/mmcc/dev/github.com/e-m-marketing/e-m-marketing-os/docs/.project-tasks.lock';
const MAX_STDIN = 1024 * 1024;

function registryPath(sessionId) {
  const safe = String(sessionId).replace(/[^A-Za-z0-9._-]/g, '_').slice(0, 128);
  return path.join(REGISTRY_DIR, `${safe}.json`);
}

function claimedLocks(sessionId) {
  try {
    const parsed = JSON.parse(fs.readFileSync(registryPath(sessionId), 'utf8'));
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function releaseIfOurs(lockPath, sessionId) {
  try {
    const lock = JSON.parse(fs.readFileSync(lockPath, 'utf8'));
    if (lock && String(lock.session_id) === sessionId) {
      fs.unlinkSync(lockPath);
    }
  } catch { /* absent, unreadable, or not ours -> nothing to release */ }
}

function run(inputOrRaw, _options = {}) {
  let input;
  try {
    input = typeof inputOrRaw === 'string'
      ? (inputOrRaw.trim() ? JSON.parse(inputOrRaw) : {})
      : (inputOrRaw || {});
  } catch {
    return { exitCode: 0 };
  }

  const sessionId = String(input?.session_id || `pid-${process.ppid}`);

  const paths = new Set(claimedLocks(sessionId));
  paths.add(LEGACY_LOCK);
  for (const lockPath of paths) {
    releaseIfOurs(lockPath, sessionId);
  }

  // The registry has served its purpose; leaving it would accumulate one file
  // per session forever.
  try {
    fs.unlinkSync(registryPath(sessionId));
  } catch { /* never existed, or already gone */ }

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
  run(data);
  process.stdout.write(data);
});
