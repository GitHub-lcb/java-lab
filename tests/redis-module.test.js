import test from 'node:test';
import assert from 'node:assert/strict';
import { simulate } from '../src/models.js';
import { labs } from '../src/catalog.js';
import { gradeQuiz, isChallengeComplete, isMastered, matchesRequirement, recordChallengeRun } from '../src/curriculum.js';

test('Redis data structure experiment shows Set deduplication while List preserves entries', () => {
  const setRun = simulate('redis-types', { type: 'set', items: 8, duplicateEvery: 2 }).at(-1);
  const listRun = simulate('redis-types', { type: 'list', items: 8, duplicateEvery: 2 }).at(-1);
  assert.ok(setRun.metrics.stored < listRun.metrics.stored);
  assert.equal(listRun.metrics.stored, 8);
});

test('noeviction rejects writes after maxmemory while allkeys-lru admits them by eviction', () => {
  const noEviction = simulate('redis-expiry', { keys: 8, memory: 4, ttl: 3, jitter: false, policy: 'noeviction' }).at(-1);
  const lru = simulate('redis-expiry', { keys: 8, memory: 4, ttl: 3, jitter: false, policy: 'allkeys-lru' }).at(-1);
  assert.equal(noEviction.metrics.denied, 4);
  assert.equal(lru.metrics.denied, 0);
  assert.equal(lru.metrics.evicted, 4);
});

test('Bloom filter blocks unique cache penetration before the database', () => {
  const none = simulate('redis-penetration', { requests: 12, distinct: 12, bloom: false, nullCache: false }).at(-1);
  const bloom = simulate('redis-penetration', { requests: 12, distinct: 12, bloom: true, nullCache: false }).at(-1);
  assert.equal(none.metrics.dbQueries, 12);
  assert.equal(bloom.metrics.dbQueries, 0);
  assert.equal(bloom.metrics.blocked, 12);
});

test('Null caching only helps repeated nonexistent keys', () => {
  const repeated = simulate('redis-penetration', { requests: 12, distinct: 2, bloom: false, nullCache: true }).at(-1);
  assert.equal(repeated.metrics.dbQueries, 2);
  assert.equal(repeated.metrics.nullHits, 10);
});

test('Mutex and logical expiration collapse a hot-key rebuild to one database query', () => {
  const none = simulate('redis-breakdown', { concurrent: 20, strategy: 'none' }).at(-1);
  const mutex = simulate('redis-breakdown', { concurrent: 20, strategy: 'mutex' }).at(-1);
  const logical = simulate('redis-breakdown', { concurrent: 20, strategy: 'logical' }).at(-1);
  assert.equal(none.metrics.dbQueries, 20);
  assert.equal(mutex.metrics.dbQueries, 1);
  assert.equal(logical.metrics.dbQueries, 1);
  assert.equal(logical.metrics.stale, 20);
});

test('TTL jitter lowers avalanche peak database load', () => {
  const sameTime = simulate('redis-avalanche', { keys: 24, window: 6, jitter: false, dbCapacity: 8, rateLimit: false }).at(-1);
  const spread = simulate('redis-avalanche', { keys: 24, window: 6, jitter: true, dbCapacity: 8, rateLimit: false }).at(-1);
  assert.equal(sameTime.metrics.peakDb, 24);
  assert.equal(spread.metrics.peakDb, 4);
});

test('Rate limiting caps database load and records rejected requests', () => {
  const run = simulate('redis-avalanche', { keys: 24, window: 3, jitter: false, dbCapacity: 8, rateLimit: true }).at(-1);
  assert.equal(run.metrics.peakDb, 8);
  assert.equal(run.metrics.rejected, 16);
});

test('Delete-first can leave stale cache while database-first deletion converges', () => {
  const unsafe = simulate('redis-consistency', { strategy: 'deleteFirst', concurrentRead: true }).at(-1);
  const cacheAside = simulate('redis-consistency', { strategy: 'dbFirst', concurrentRead: true }).at(-1);
  const doubleDelete = simulate('redis-consistency', { strategy: 'doubleDelete', concurrentRead: true }).at(-1);
  assert.equal(unsafe.metrics.consistent, '否');
  assert.equal(cacheAside.metrics.consistent, '是');
  assert.equal(doubleDelete.metrics.consistent, '是');
});

test('Atomic inventory strategies prevent oversell while naive GET and SET does not', () => {
  const naive = simulate('redis-atomic', { strategy: 'getSet', clients: 8, stock: 5, demand: 1 }).at(-1);
  const watched = simulate('redis-atomic', { strategy: 'watch', clients: 8, stock: 5, demand: 1 }).at(-1);
  const lua = simulate('redis-atomic', { strategy: 'lua', clients: 8, stock: 5, demand: 1 }).at(-1);
  assert.equal(naive.metrics.oversold, 3);
  assert.equal(watched.metrics.oversold, 0);
  assert.equal(lua.metrics.oversold, 0);
  assert.ok(watched.metrics.retries > 0);
  assert.equal(lua.metrics.retries, 0);
});

test('Lock lease shorter than work creates overlap unless watchdog renews it', () => {
  const expired = simulate('redis-lock', { clients: 3, lease: 3, work: 8, watchdog: false, safeUnlock: true }).at(-1);
  const renewed = simulate('redis-lock', { clients: 3, lease: 3, work: 8, watchdog: true, safeUnlock: true }).at(-1);
  assert.equal(expired.metrics.overlaps, 1);
  assert.equal(renewed.metrics.overlaps, 0);
  assert.ok(renewed.metrics.renewals >= 2);
});

test('Token comparison prevents an expired owner from deleting the new owner lock', () => {
  const unsafe = simulate('redis-lock', { clients: 2, lease: 2, work: 5, watchdog: false, safeUnlock: false }).at(-1);
  const safe = simulate('redis-lock', { clients: 2, lease: 2, work: 5, watchdog: false, safeUnlock: true }).at(-1);
  assert.equal(unsafe.metrics.unsafeUnlocks, 1);
  assert.equal(safe.metrics.unsafeUnlocks, 0);
});

test('AOF always persists every acknowledged write while periodic modes have a loss window', () => {
  const always = simulate('redis-persistence', { mode: 'aofAlways', writes: 11 }).at(-1);
  const everySecond = simulate('redis-persistence', { mode: 'aofEverysec', writes: 11 }).at(-1);
  const rdb = simulate('redis-persistence', { mode: 'rdb', writes: 11 }).at(-1);
  assert.equal(always.metrics.lost, 0);
  assert.equal(everySecond.metrics.lost, 1);
  assert.equal(rdb.metrics.lost, 5);
});

test('Replica promotion exposes asynchronous replication loss window', () => {
  const run = simulate('redis-ha', { mode: 'sentinel', replicas: 2, writes: 10, lag: 2 }).at(-1);
  assert.equal(run.metrics.promoted, 1);
  assert.equal(run.metrics.lost, 2);
  assert.equal(run.metrics.available, '恢复');
});

test('Operations experiment identifies hot keys and big keys independently', () => {
  const run = simulate('redis-ops', { requests: 100, hotRatio: 80, valueKb: 1024 }).at(-1);
  assert.equal(run.metrics.hotKeys, 1);
  assert.equal(run.metrics.bigKeys, 1);
});

test('Protected Redis capstone lowers peak load and resolves modeled risks', () => {
  const baseline = simulate('redis-capstone', { missing: 12, concurrent: 24, dbCapacity: 8, bloom: false, rebuild: 'none', jitter: false, safeWrite: false }).at(-1);
  const protectedRun = simulate('redis-capstone', { missing: 12, concurrent: 24, dbCapacity: 8, bloom: true, rebuild: 'mutex', jitter: true, safeWrite: true }).at(-1);
  assert.ok(protectedRun.metrics.dbQueries < baseline.metrics.dbQueries);
  assert.ok(protectedRun.metrics.peakDb < baseline.metrics.peakDb);
  assert.equal(baseline.metrics.risks, 4);
  assert.equal(protectedRun.metrics.risks, 0);
});

test('Every Redis curriculum lesson has goals, a challenge, two checks and official reference', () => {
  const lessons = labs.filter(lab => lab.group === 'Redis 专题');
  assert.equal(lessons.length, 13);
  for (const lesson of lessons) {
    assert.ok(lesson.goals.length >= 2, lesson.id);
    assert.ok(lesson.challenge?.task, lesson.id);
    assert.ok(lesson.challenge.requirements.length >= 2, lesson.id);
    assert.equal(lesson.quiz.length, 2, lesson.id);
    assert.match(lesson.source, /^https:\/\/(redis\.io|developer\.redislabs\.com)/, lesson.id);
  }
});

test('Quiz requires every answer and reports explanations', () => {
  const quiz = [
    { prompt: 'q1', choices: ['a', 'b'], answer: 1, explanation: 'because b' },
    { prompt: 'q2', choices: ['c', 'd'], answer: 0, explanation: 'because c' },
  ];
  const result = gradeQuiz(quiz, { 0: 1, 1: 0 });
  assert.equal(result.passed, true);
  assert.equal(result.score, 2);
  assert.deepEqual(result.details.map(item => item.explanation), ['because b', 'because c']);
  assert.equal(gradeQuiz(quiz, { 0: 1 }).passed, false);
});

test('Redis lesson mastery requires both experiment completion and quiz pass', () => {
  const lesson = { id: 'redis-types', quiz: [{ answer: 0 }], challenge: { requirements: [{ id: 'list' }, { id: 'set' }] } };
  assert.equal(isMastered(lesson, ['redis-types'], []), false);
  assert.equal(isMastered(lesson, [], ['redis-types']), false);
  assert.equal(isMastered(lesson, ['redis-types'], ['redis-types'], { 'redis-types': { list: {} } }), false);
  assert.equal(isMastered(lesson, ['redis-types'], ['redis-types'], { 'redis-types': { list: {}, set: {} } }), true);
});

test('Challenge requirements match selected parameters and keep result evidence', () => {
  const challenge = { requirements: [
    { id: 'small', when: { capacity: 1, requests: 6 } },
    { id: 'normal', when: { capacity: 3, requests: 6 } },
  ] };
  assert.equal(matchesRequirement(challenge.requirements[0], { capacity: 1, requests: 6, cache: true }), true);
  assert.equal(matchesRequirement(challenge.requirements[0], { capacity: 3, requests: 6 }), false);
  let history = recordChallengeRun(challenge, {}, { capacity: 1, requests: 6, cache: true }, { misses: 6 });
  assert.equal(isChallengeComplete(challenge, history), false);
  assert.equal(history.small.metrics.misses, 6);
  history = recordChallengeRun(challenge, history, { capacity: 3, requests: 6 }, { misses: 3 });
  assert.equal(isChallengeComplete(challenge, history), true);
});
