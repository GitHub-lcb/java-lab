import test from 'node:test';
import assert from 'node:assert/strict';
import { simulate, javaHash, bucketIndex } from '../src/models.js';

test('Java string hash matches String.hashCode', () => {
  assert.equal(javaHash('hello'), 99162322);
  assert.equal(javaHash(''), 0);
  assert.equal(javaHash('Aa'), javaHash('BB'));
});
test('HashMap spreads the high bits and uses a power-of-two mask', () => {
  assert.equal(bucketIndex('hello', 16), 11);
});
test('Redis cache-aside gets repeated keys from cache', () => {
  const run = simulate('redis', { capacity: 3, requests: 6, cache: true, key: 'user:1001' });
  assert.equal(run.at(-1)?.metrics.hits, 3);
  assert.equal(run.at(-1)?.metrics.misses, 3);
});
test('Disabled cache always queries the database', () => {
  const run = simulate('redis', { capacity: 3, requests: 6, cache: false, key: 'user:1001' });
  assert.equal(run.at(-1)?.metrics.hits, 0);
  assert.equal(run.at(-1)?.metrics.misses, 6);
});
test('LRU eviction changes the hit rate', () => {
  assert.ok(simulate('redis', { capacity: 1, requests: 6, cache: true, key: 'k' }).at(-1)?.metrics.hits < 3);
});
test('ThreadPoolExecutor admits core workers, queue, max workers, then rejects', () => {
  const end = simulate('threadpool', { core: 2, max: 4, queue: 2, tasks: 8, policy: 'abort' }).at(-1);
  assert.equal(end?.metrics.workers, 4);
  assert.equal(end?.metrics.queued, 2);
  assert.equal(end?.metrics.rejected, 2);
});
test('CallerRunsPolicy does not count caller tasks as rejected', () => {
  const end = simulate('threadpool', { core: 1, max: 2, queue: 1, tasks: 5, policy: 'caller' }).at(-1);
  assert.equal(end?.metrics.caller, 2);
  assert.equal(end?.metrics.rejected, 0);
});
test('HashMap doubles capacity only when size exceeds threshold', () => {
  const run = simulate('hashmap', { capacity: 4, factor: 0.75, entries: 4 });
  assert.equal(run.at(-1)?.metrics.capacity, 8);
  assert.equal(run.at(-1)?.metrics.size, 4);
});
test('Kafka assigns each partition to exactly one active group consumer', () => {
  const end = simulate('kafka', { partitions: 3, consumers: 5, messages: 8 }).at(-1);
  assert.deepEqual(end?.assignments, [0, 1, 2]);
  assert.equal(end?.metrics.idle, 2);
  assert.equal(end?.metrics.consumed, 8);
});
test('MySQL indexed and full scans find the same row with different reads', () => {
  const index = simulate('mysql', { indexed: true, target: 42 }).at(-1);
  const scan = simulate('mysql', { indexed: false, target: 42 }).at(-1);
  assert.equal(index?.metrics.found, true);
  assert.equal(scan?.metrics.found, true);
  assert.ok(index?.metrics.reads < scan?.metrics.reads);
});
test('JVM generational model reclaims unreachable allocations', () => {
  const end = simulate('jvm', { eden: 4, allocations: 10, survival: 50 }).at(-1);
  assert.ok(end?.metrics.collections >= 2);
  assert.ok(end?.metrics.reclaimed > 0);
});
test('Unknown lab fails explicitly', () => {
  assert.throws(() => simulate('missing', {}), /Unknown lab/);
});
test('Every Redis frame reports a rate consistent with its displayed counts', () => {
  for (const frame of simulate('redis', { capacity: 3, requests: 6, cache: true, key: 'k' })) {
    const m = frame.metrics;
    assert.equal(m.rate, m.requests ? Math.round(m.hits / m.requests * 100) : 0);
  }
});
test('Kafka commits the next offset per partition and activates the offset node', () => {
  const run = simulate('kafka', { partitions: 3, consumers: 2, messages: 8 });
  assert.deepEqual(run.at(-1).offsets, [3, 3, 2]);
  assert.equal(run.at(-1).active, 'offset');
  assert.equal(run.filter(frame => frame.active === 'offset').length, 8);
});
