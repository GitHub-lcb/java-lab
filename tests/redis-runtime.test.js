import test from 'node:test';
import assert from 'node:assert/strict';
import { createSessionId, executeCommand, resetSession, runtimeStatus } from '../src/redisRuntime.js';
import { realLabCatalog } from '../src/realLabCatalog.js';
import { labs } from '../src/catalog.js';

test('Creates and reuses a valid isolated session id', () => {
  const values = new Map();
  const storage = { getItem: key => values.get(key) ?? null, setItem: (key, value) => values.set(key, value) };
  const crypto = { randomUUID: () => '12345678-1234-1234-1234-123456789abc' };
  assert.equal(createSessionId(storage, crypto), '12345678-1234-1234-1234-123456789abc');
  assert.equal(createSessionId(storage, { randomUUID: () => 'different-id' }), '12345678-1234-1234-1234-123456789abc');
});

test('Replaces malformed stored session ids', () => {
  const storage = { value: 'bad id', getItem() { return this.value; }, setItem(key, value) { this.value = value; } };
  assert.equal(createSessionId(storage, { randomUUID: () => '87654321-safe-session' }), '87654321-safe-session');
});

test('Executes plain-text command through the Java gateway', async () => {
  const calls = [];
  const fetcher = async (url, options) => { calls.push({ url, options }); return { ok: true, json: async () => ({ ok: true, result: 'PONG' }) }; };
  const result = await executeCommand(fetcher, 'http://127.0.0.1:8787', 'abc12345', 'PING');
  assert.equal(result.result, 'PONG');
  assert.equal(calls[0].options.headers['X-Lab-Session'], 'abc12345');
  assert.equal(calls[0].options.body, 'PING');
});

test('Rejects blank commands without calling the gateway', async () => {
  let calls = 0;
  await assert.rejects(() => executeCommand(async () => { calls++; }, '', 'abc12345', '   '), /command/i);
  assert.equal(calls, 0);
});

test('Converts unavailable gateway into an explicit offline status', async () => {
  assert.deepEqual(await runtimeStatus(async () => { throw new Error('ECONNREFUSED'); }, 'http://127.0.0.1:8787'), { java: false, redis: false, message: 'Java 网关未启动' });
});

test('Reset sends only the isolated session identifier', async () => {
  const calls = [];
  const fetcher = async (url, options) => { calls.push({ url, options }); return { ok: true, json: async () => ({ ok: true, deleted: 3 }) }; };
  const result = await resetSession(fetcher, 'http://127.0.0.1:8787', 'abc12345');
  assert.equal(result.deleted, 3);
  assert.equal(calls[0].options.headers['X-Lab-Session'], 'abc12345');
  assert.equal(calls[0].options.body, undefined);
});

test('Every Redis lesson has a guided real command lab', () => {
  const redisLabs = labs.filter(lab => lab.group === 'Redis 专题');
  assert.equal(Object.keys(realLabCatalog).length, redisLabs.length);
  for (const lab of redisLabs) {
    const guide = realLabCatalog[lab.id];
    assert.ok(guide?.objective, lab.id);
    assert.ok(guide.commands.length >= 3, lab.id);
  }
});
