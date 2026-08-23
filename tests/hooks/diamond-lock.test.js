#!/usr/bin/env node
'use strict';

/**
 * Tests for the project-tasks.yaml lock hooks.
 *
 * These hooks had ZERO tests while guarding live client task files, and were
 * untracked in git as well — which is how one of them kept a hardcoded path
 * long after a second project needed it. Written before changing them so the
 * change is characterised rather than hoped at.
 *
 * They now live in THIS repo, which is the fix for the cause rather than the
 * symptom: they encode this tool's file format and its lock protocol, so being
 * versioned separately from it — and unreachable by its CI — is what let them
 * drift. Run by the `hooks` job in .github/workflows/ci.yml.
 */

const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { spawnSync } = require('child_process');

const HOOKS = path.join(__dirname, '..', '..', 'hooks');
const PRE = path.join(HOOKS, 'pre-edit-diamond-lock.js');
const STOP = path.join(HOOKS, 'stop-diamond-lock-release.js');

let passed = 0;
let failed = 0;

function test(name, fn) {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'diamond-lock-'));
  try {
    fn(tmp);
    console.log(`  ✓ ${name}`);
    passed++;
  } catch (err) {
    console.log(`  ✗ ${name}`);
    console.log(`    Error: ${err.message}`);
    failed++;
  } finally {
    fs.rmSync(tmp, { recursive: true, force: true });
  }
}

function run(script, input, env) {
  const result = spawnSync('node', [script], {
    encoding: 'utf8',
    input: JSON.stringify(input),
    timeout: 10000,
    env: { ...process.env, ...env },
  });
  return { code: result.status || 0, stderr: result.stderr || '' };
}

function taskFile(dir) {
  const docs = path.join(dir, 'docs');
  fs.mkdirSync(docs, { recursive: true });
  const f = path.join(docs, 'project-tasks.yaml');
  fs.writeFileSync(f, 'epics: []\n');
  return f;
}

function lockFor(file) {
  return path.join(path.dirname(file), '.project-tasks.lock');
}

function env(dir) {
  return {
    DIAMOND_LOCK_REGISTRY_DIR: path.join(dir, 'registry'),
    // Point the legacy path somewhere harmless so tests never touch the real one.
    DIAMOND_LOCK_PATH: path.join(dir, 'nonexistent-legacy.lock'),
  };
}

console.log('\n=== Testing the project-tasks.yaml lock hooks ===\n');

// --- pre-edit hook: the guard itself -------------------------------------

test('claims the lock on a project-tasks.yaml edit', (dir) => {
  const f = taskFile(dir);
  const r = run(PRE, { session_id: 'sess-A', tool_input: { file_path: f } }, env(dir));
  assert.strictEqual(r.code, 0);
  assert.strictEqual(JSON.parse(fs.readFileSync(lockFor(f), 'utf8')).session_id, 'sess-A');
});

test('ignores any other file entirely', (dir) => {
  const other = path.join(dir, 'README.md');
  fs.writeFileSync(other, 'hi');
  const r = run(PRE, { session_id: 'sess-A', tool_input: { file_path: other } }, env(dir));
  assert.strictEqual(r.code, 0);
  assert.ok(!fs.existsSync(path.join(dir, '.project-tasks.lock')));
});

test('blocks a DIFFERENT session holding a fresh lock', (dir) => {
  const f = taskFile(dir);
  run(PRE, { session_id: 'sess-A', tool_input: { file_path: f } }, env(dir));
  const r = run(PRE, { session_id: 'sess-B', tool_input: { file_path: f } }, env(dir));
  assert.strictEqual(r.code, 2, 'a second session must be blocked');
  assert.ok(/BLOCKED/.test(r.stderr));
});

test('allows the SAME session to keep editing', (dir) => {
  // The self-deadlock case: a session must not lock itself out of its own file.
  const f = taskFile(dir);
  run(PRE, { session_id: 'sess-A', tool_input: { file_path: f } }, env(dir));
  const r = run(PRE, { session_id: 'sess-A', tool_input: { file_path: f } }, env(dir));
  assert.strictEqual(r.code, 0);
});

test('takes over a stale lock', (dir) => {
  const f = taskFile(dir);
  fs.writeFileSync(lockFor(f), JSON.stringify({
    session_id: 'dead-session', ts: Date.now() - 6 * 60 * 1000, pid: 1,
  }));
  const r = run(PRE, { session_id: 'sess-B', tool_input: { file_path: f } }, env(dir));
  assert.strictEqual(r.code, 0);
  assert.strictEqual(JSON.parse(fs.readFileSync(lockFor(f), 'utf8')).session_id, 'sess-B');
});

test('a corrupt lock file reads as free rather than wedging the file', (dir) => {
  const f = taskFile(dir);
  fs.writeFileSync(lockFor(f), '{not json');
  assert.strictEqual(run(PRE, { session_id: 'sess-A', tool_input: { file_path: f } }, env(dir)).code, 0);
});

// --- the registry: what makes release work for ANY project ---------------

test('records each claimed lock in a per-session registry', (dir) => {
  const f = taskFile(dir);
  run(PRE, { session_id: 'sess-A', tool_input: { file_path: f } }, env(dir));
  const reg = path.join(dir, 'registry', 'sess-A.json');
  assert.deepStrictEqual(JSON.parse(fs.readFileSync(reg, 'utf8')), [lockFor(f)]);
});

test('records locks for MORE THAN ONE project in one session', (dir) => {
  // The whole point: a session touching two boards must free both on exit.
  const a = taskFile(path.join(dir, 'board-a'));
  const b = taskFile(path.join(dir, 'board-b'));
  run(PRE, { session_id: 'sess-A', tool_input: { file_path: a } }, env(dir));
  run(PRE, { session_id: 'sess-A', tool_input: { file_path: b } }, env(dir));
  const claimed = JSON.parse(fs.readFileSync(path.join(dir, 'registry', 'sess-A.json'), 'utf8'));
  assert.deepStrictEqual(claimed.sort(), [lockFor(a), lockFor(b)].sort());
});

test('does not record the same lock twice', (dir) => {
  const f = taskFile(dir);
  run(PRE, { session_id: 'sess-A', tool_input: { file_path: f } }, env(dir));
  run(PRE, { session_id: 'sess-A', tool_input: { file_path: f } }, env(dir));
  assert.strictEqual(
    JSON.parse(fs.readFileSync(path.join(dir, 'registry', 'sess-A.json'), 'utf8')).length, 1);
});

// --- Stop hook: release ---------------------------------------------------

test('releases a NON-EM project lock on session end', (dir) => {
  // The bug this fixes. Previously only one hardcoded path was released, so
  // every other project waited out the full 5-minute TTL with nobody editing.
  const f = taskFile(dir);
  run(PRE, { session_id: 'sess-A', tool_input: { file_path: f } }, env(dir));
  assert.ok(fs.existsSync(lockFor(f)));
  run(STOP, { session_id: 'sess-A' }, env(dir));
  assert.ok(!fs.existsSync(lockFor(f)), 'the lock must be freed immediately, not after the TTL');
});

test('releases every project a session claimed, not just the first', (dir) => {
  const a = taskFile(path.join(dir, 'board-a'));
  const b = taskFile(path.join(dir, 'board-b'));
  run(PRE, { session_id: 'sess-A', tool_input: { file_path: a } }, env(dir));
  run(PRE, { session_id: 'sess-A', tool_input: { file_path: b } }, env(dir));
  run(STOP, { session_id: 'sess-A' }, env(dir));
  assert.ok(!fs.existsSync(lockFor(a)));
  assert.ok(!fs.existsSync(lockFor(b)));
});

test('never releases a lock another session now holds', (dir) => {
  // Our TTL lapsed and someone else took over: deleting would strip a lock off
  // a run actively using it.
  const f = taskFile(dir);
  run(PRE, { session_id: 'sess-A', tool_input: { file_path: f } }, env(dir));
  fs.writeFileSync(lockFor(f), JSON.stringify({ session_id: 'sess-B', ts: Date.now(), pid: 2 }));
  run(STOP, { session_id: 'sess-A' }, env(dir));
  assert.strictEqual(JSON.parse(fs.readFileSync(lockFor(f), 'utf8')).session_id, 'sess-B');
});

test('cleans up its own registry file', (dir) => {
  const f = taskFile(dir);
  run(PRE, { session_id: 'sess-A', tool_input: { file_path: f } }, env(dir));
  run(STOP, { session_id: 'sess-A' }, env(dir));
  assert.ok(!fs.existsSync(path.join(dir, 'registry', 'sess-A.json')),
    'one file per session would otherwise accumulate forever');
});

test('a session that claimed nothing exits cleanly', (dir) => {
  assert.strictEqual(run(STOP, { session_id: 'sess-NOBODY' }, env(dir)).code, 0);
});

test('still releases the legacy hardcoded path for pre-registry sessions', (dir) => {
  // Sessions that started before the registry existed recorded nothing, so the
  // single known path is still tried.
  const legacy = path.join(dir, 'legacy.lock');
  fs.writeFileSync(legacy, JSON.stringify({ session_id: 'sess-A', ts: Date.now(), pid: 1 }));
  run(STOP, { session_id: 'sess-A' }, {
    DIAMOND_LOCK_REGISTRY_DIR: path.join(dir, 'registry'),
    DIAMOND_LOCK_PATH: legacy,
  });
  assert.ok(!fs.existsSync(legacy));
});

test('the Stop hook never blocks session end', (dir) => {
  assert.strictEqual(run(STOP, { session_id: 'sess-A' }, env(dir)).code, 0);
  assert.strictEqual(run(STOP, {}, env(dir)).code, 0);
});

console.log(`\n${passed} passed, ${failed} failed\n`);
process.exit(failed === 0 ? 0 : 1);
