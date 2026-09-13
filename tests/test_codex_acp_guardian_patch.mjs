import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { cpSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { spawnSync } from 'node:child_process';
import { dirname, join } from 'node:path';
import { tmpdir } from 'node:os';
import {
  DIRECTOR_SERVER,
  GuardianDenialStore,
  directorPayloadDigest
} from '../scripts/codex-acp-guardian-helpers.mjs';

const root = new URL('..', import.meta.url).pathname;
const bundlePath = join(root, 'node_modules/@agentclientprotocol/codex-acp/dist/index.js');
const patchScript = join(root, 'scripts/patch-codex-acp-guardian.mjs');
const rootRequire = createRequire(import.meta.url);
const adapterRequire = createRequire(bundlePath);

{
  const expectedVersion = '0.154.0';
  const rootPackage = JSON.parse(readFileSync(join(root, 'package.json'), 'utf8'));
  const codexPackage = JSON.parse(readFileSync(join(root, 'node_modules/@openai/codex/package.json'), 'utf8'));
  const rootCodexPath = rootRequire.resolve('@openai/codex/bin/codex.js');
  const adapterCodexPath = adapterRequire.resolve('@openai/codex/bin/codex.js');
  assert.equal(rootPackage.dependencies['@openai/codex'], expectedVersion);
  assert.equal(codexPackage.version, expectedVersion);
  assert.equal(adapterCodexPath, rootCodexPath, 'the ACP adapter must resolve the checked-in root Codex runtime');
  const version = spawnSync(process.execPath, [adapterCodexPath, '--version'], { encoding: 'utf8' });
  assert.equal(version.status, 0, version.stderr);
  assert.match(version.stdout, new RegExp(`codex-cli ${expectedVersion.replaceAll('.', '\\.')}`));
}

function hash(value) {
  return createHash('sha256').update(value, 'utf8').digest('hex');
}

function started(itemId = 'item-1') {
  return {
    id: itemId,
    type: 'mcpToolCall',
    server: DIRECTOR_SERVER,
    tool: 'publish_reply',
    arguments: {
      authority: 'authority-1',
      text: 'I’ll take it 🚏',
      conversation_title: '🚏 Commute plan',
      conversation_emoji: '🚏',
      conversation_preview: 'I’ll take it 🚏',
      responsibility_id: null,
      execution_fence: null
    }
  };
}

function completed(overrides = {}) {
  return {
    threadId: 'thread-1',
    turnId: 'turn-1',
    reviewId: 'review-1',
    targetItemId: 'item-1',
    startedAtMs: 10,
    completedAtMs: 20,
    review: { status: 'denied', riskLevel: 'medium', rationale: 'explicit approval required' },
    action: { type: 'mcpToolCall', server: DIRECTOR_SERVER, toolName: 'publish_reply' },
    ...overrides
  };
}

{
  const store = new GuardianDenialStore({ now: () => 100 });
  assert.equal(store.captureStarted('thread-1', started()), true);
  const candidate = store.captureCompleted(completed());
  assert.deepEqual(Object.keys(candidate).sort(), ['authority', 'fingerprint', 'payloadDigest', 'replyDisplay', 'reviewId', 'sessionId', 'turnId']);
  assert.equal(candidate.authority, 'authority-1');
  assert.deepEqual(candidate.replyDisplay, {
    text: 'I’ll take it 🚏',
    conversation_title: '🚏 Commute plan',
    conversation_emoji: '🚏',
    conversation_preview: 'I’ll take it 🚏'
  });
  assert.equal('authority' in candidate.replyDisplay, false);
  assert.equal('event' in candidate.replyDisplay, false);
  assert.equal(candidate.payloadDigest, '064ff2c24304dca084190f0259f8439bc3ccc0f658c57493c78cdebe7e248d6b');
  const calls = [];
  await assert.rejects(
    () => store.approve({ ...candidate, fingerprint: '0'.repeat(64) }, async () => {}),
    /unknown Director guardian approval/
  );
  await assert.doesNotReject(() => store.approve(candidate, async (request) => calls.push(request)));
  assert.deepEqual(calls, [{
    threadId: 'thread-1',
    event: {
      id: 'review-1', target_item_id: 'item-1', turn_id: 'turn-1', started_at_ms: 10,
      completed_at_ms: 20, status: 'denied', risk_level: 'medium', rationale: 'explicit approval required',
      action: { type: 'mcp_tool_call', server: DIRECTOR_SERVER, tool_name: 'publish_reply' }
    }
  }]);
  assert.equal(store.captureCompleted(completed()), null, 'duplicate completion cannot recreate consumed approval');
  await assert.rejects(() => store.approve(candidate, async () => {}), /unknown Director guardian approval/);
}


{
  const store = new GuardianDenialStore({ now: () => 100 });
  const item = started();
  delete item.arguments.conversation_title;
  delete item.arguments.conversation_emoji;
  delete item.arguments.conversation_preview;
  store.captureStarted('thread-1', item);
  const candidate = store.captureCompleted(completed());
  assert.deepEqual(candidate.replyDisplay, {
    text: 'I’ll take it 🚏',
    conversation_title: null,
    conversation_emoji: null,
    conversation_preview: null
  });
  assert.equal('authority' in candidate.replyDisplay, false);
}

for (const event of [
  completed({ action: { type: 'mcpToolCall', server: 'other-server', toolName: 'publish_reply' } }),
  completed({ action: { type: 'mcpToolCall', server: DIRECTOR_SERVER, toolName: 'other_tool' } }),
  completed({ targetItemId: 'missing-item' })
]) {
  const store = new GuardianDenialStore({ now: () => 100 });
  store.captureStarted('thread-1', started());
  assert.equal(store.captureCompleted(event), null);
}

{
  const store = new GuardianDenialStore({ now: () => 100 });
  const wrongServer = { ...started(), server: 'unrelated-server' };
  assert.equal(store.captureStarted('thread-1', wrongServer), false);
  assert.equal(store.captureCompleted(completed()), null);
}

assert.equal(
  directorPayloadDigest(started().arguments),
  '064ff2c24304dca084190f0259f8439bc3ccc0f658c57493c78cdebe7e248d6b'
);
assert.equal(
  hash('{"conversation_emoji":"\\ud83d\\ude8f","conversation_preview":"I\\u2019ll take it \\ud83d\\ude8f","conversation_title":"\\ud83d\\ude8f Commute plan","execution_fence":null,"responsibility_id":null,"text":"I\\u2019ll take it \\ud83d\\ude8f"}'),
  directorPayloadDigest(started().arguments),
  'matches Python json.dumps(sort_keys=True, separators=(\',\', \':\')) with ensure_ascii=True'
);

const result = spawnSync(process.execPath, [patchScript], { cwd: root, encoding: 'utf8' });
assert.equal(result.status, 0, result.stderr);
const patched = readFileSync(bundlePath, 'utf8');
for (const marker of ['DIRECTOR_GUARDIAN_DENIAL_EXTENSION_V1', '_director/approve_guardian_denied_action', 'thread/approveGuardianDeniedAction', 'directorApproval', 'GuardianDenialStore']) {
  assert.ok(patched.includes(marker));
}
assert.equal(spawnSync(process.execPath, ['--check', bundlePath], { encoding: 'utf8' }).status, 0);

const fixture = mkdtempSync(join(tmpdir(), 'director-acp-drift-'));
try {
  cpSync(join(root, 'scripts'), join(fixture, 'scripts'), { recursive: true });
  const fixtureBundle = join(fixture, 'node_modules/@agentclientprotocol/codex-acp/dist/index.js');
  mkdirSync(dirname(fixtureBundle), { recursive: true });
  cpSync(bundlePath, fixtureBundle, { recursive: false, force: true });
  writeFileSync(fixtureBundle, `${readFileSync(fixtureBundle, 'utf8')}\n// drift\n`);
  const drift = spawnSync(process.execPath, [join(fixture, 'scripts/patch-codex-acp-guardian.mjs')], { cwd: fixture, encoding: 'utf8' });
  assert.notEqual(drift.status, 0);
  assert.match(drift.stderr, /unexpected patched codex-acp bundle hash/);
} finally {
  rmSync(fixture, { recursive: true, force: true });
}
