import test from 'node:test';
import assert from 'node:assert/strict';
import { simulate } from '../src/models.js';

const scenarios = {
  'threadlocal-leak': ['request', 'pool-leak', 'pool-remove'],
  'blocking-queue': ['abq', 'lbq', 'backpressure'],
  'cas-atomic': ['spin', 'aba', 'adder'],
  'completable-future': ['chain', 'error', 'pool'],
};
for (const [lab, list] of Object.entries(scenarios)) {
  for (const scenario of list) {
    const run = simulate(lab, { scenario });
    assert.ok(run.length > 5, `${lab}/${scenario} should advance beyond the intro frame`);
    assert.equal(run.at(-1).message.includes('运行结束'), true, `${lab}/${scenario} should end with a summary`);
  }
}

test('ThreadLocal scenario ①: independent request threads keep private copies and die clean', () => {
  const end = simulate('threadlocal-leak', { scenario: 'request' }).at(-1);
  assert.deepEqual({ ...end?.metrics }, { requests: 2, reads: 2, dirty: 0, leaks: 0, removes: 0 });
  assert.equal(end?.items.threads.length, 2);
  assert.equal(end?.items.threads.every(t => t.st === 'done'), true);
  assert.equal(end?.items.heap.length, 0);
});
test('ThreadLocal scenario ②: pooled worker without remove() leaves residue value that the next request reads', () => {
  const end = simulate('threadlocal-leak', { scenario: 'pool-leak' }).at(-1);
  assert.deepEqual({ ...end?.metrics }, { requests: 2, reads: 2, dirty: 1, leaks: 1, removes: 0 });
  assert.equal(end?.items.threads.find(t => t.id === 'req#1')?.st, 'residue');
  assert.equal(end?.items.threads.find(t => t.id === 'req#2')?.st, 'dirty');
  assert.equal(end?.items.threads.find(t => t.id === 'req#2')?.ctx, 'U1');
  assert.deepEqual(end?.items.heap, ['U1']);
});
test('ThreadLocal scenario ③: finally remove() cures both dirty reads and value leaks', () => {
  const end = simulate('threadlocal-leak', { scenario: 'pool-remove' }).at(-1);
  assert.deepEqual({ ...end?.metrics }, { requests: 2, reads: 2, dirty: 0, leaks: 0, removes: 1 });
  assert.equal(end?.items.threads.every(t => t.ctx === '—'), true);
  assert.equal(end?.items.removed, true);
  assert.equal(end?.items.heap.length, 0);
});

test('Blocking queue scenario ①: full ABQ blocks two puts until take signals notFull', () => {
  const end = simulate('blocking-queue', { scenario: 'abq' }).at(-1);
  assert.deepEqual({ ...end?.metrics }, { puts: 5, takes: 5, blocks: 2, wakes: 2, peak: 3 });
  assert.equal(end?.items.q.length, 0);
  assert.equal(end?.items.waiting.length, 0);
});
test('Blocking queue scenario ②: dual locks let put and take run without mutual blocking', () => {
  const end = simulate('blocking-queue', { scenario: 'lbq' }).at(-1);
  assert.deepEqual({ ...end?.metrics }, { puts: 5, takes: 5, blocks: 0, wakes: 0, peak: 2 });
});
test('Blocking queue scenario ③: bounded queue passes slow-consumer pressure back to the producer', () => {
  const end = simulate('blocking-queue', { scenario: 'backpressure' }).at(-1);
  assert.deepEqual({ ...end?.metrics }, { puts: 5, takes: 5, blocks: 3, wakes: 3, peak: 2 });
  assert.equal(end?.items.waiting.length, 0);
});

test('CAS scenario ①: failed compareAndSet spins by rereading the slot then retries', () => {
  const end = simulate('cas-atomic', { scenario: 'spin' }).at(-1);
  assert.deepEqual({ ...end?.metrics }, { ops: 3, ok: 2, spins: 1, detect: 0, spread: 0 });
  assert.equal(end?.items.slot, 2);
});
test('CAS scenario ②: unstamped CAS passes the ABA spoof, the stamp version intercepts once then retries', () => {
  const end = simulate('cas-atomic', { scenario: 'aba' }).at(-1);
  assert.deepEqual({ ...end?.metrics }, { ops: 7, ok: 6, spins: 0, detect: 1, spread: 0 });
  assert.equal(end?.items.slot, 30);
  assert.equal(end?.items.stamp, 3);
  assert.equal(end?.items.blocked, false);
  assert.equal(simulate('cas-atomic', { scenario: 'aba' }).some(frame => frame.items.blocked), true);
});
test('CAS scenario ③: LongAdder diverts colliding threads into Cells and sum() reconciles with base', () => {
  const end = simulate('cas-atomic', { scenario: 'adder' }).at(-1);
  assert.deepEqual({ ...end?.metrics }, { ops: 13, ok: 8, spins: 0, detect: 0, spread: 5 });
  assert.equal(end?.items.base, 3);
  assert.equal(end?.items.sum, 8);
  assert.deepEqual(end?.items.cells.sort((a, b) => a.i - b.i), [{ i: 0, n: 2 }, { i: 1, n: 2 }, { i: 2, n: 1 }]);
});

test('CompletableFuture scenario ①: dependency graph finishes six nodes in three clock layers', () => {
  const end = simulate('completable-future', { scenario: 'chain' }).at(-1);
  assert.deepEqual({ ...end?.metrics }, { stages: 6, steps: 3, errors: 0, fallbacks: 0, skipped: 0, queued: 0 });
  assert.equal(end?.items.value.t, 'F ✓ 全链终值');
});
test('CompletableFuture scenario ②: exception propagates, dependents skip, exceptionally takes over at the tail', () => {
  const end = simulate('completable-future', { scenario: 'error' }).at(-1);
  assert.deepEqual({ ...end?.metrics }, { stages: 4, steps: 2, errors: 1, fallbacks: 1, skipped: 2, queued: 0 });
  assert.equal(end?.items.jobs.find(j => j.id === 'B')?.st, 'error');
  assert.equal(end?.items.value.t, 'fallback · 兜底值');
});
test('CompletableFuture scenario ③: mixed pool queues four compute tasks, isolated pools clear them', () => {
  const run = simulate('completable-future', { scenario: 'pool' });
  assert.equal(run.some(frame => frame.metrics.queued === 4), true);
  const end = run.at(-1);
  assert.equal(end?.metrics.queued, 0);
  assert.equal(end?.metrics.stages, 12);
  assert.equal(end?.metrics.steps, 5);
  assert.match(end?.items.value.t, /隔离池墙钟 2 层/);
  assert.equal(end?.items.phase.txt, '幕 2 · 隔离池（两池并行）');
});
