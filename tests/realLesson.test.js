import test from 'node:test';
import assert from 'node:assert/strict';
import { matchesExpected, advanceLesson, createRun, lessonSession } from '../src/realLesson.js';

const guide = { steps: [
  { id: 'init', command: 'SET stock 2', expected: { kind: 'equals', value: 'OK' } },
  { id: 'first', command: 'DECR stock', expected: { kind: 'equals', value: 1 } },
  { id: 'second', command: 'DECR stock', expected: { kind: 'equals', value: 0 } },
] };
const reply = (command, result, ok = true) => ({ command, result, ok });
test('A repeated command must execute once for each distinct step', () => {
  let run = advanceLesson(guide, createRun(), reply('SET stock 2', 'OK'));
  run = advanceLesson(guide, run, reply('DECR stock', 1));
  assert.equal(run.index, 2);
  assert.equal(run.complete, false);
  run = advanceLesson(guide, run, reply('DECR stock', 0));
  assert.equal(run.complete, true);
  assert.equal(run.evidence.length, 3);
});
test('Out of order commands and wrong return values do not advance a lesson', () => {
  const initial = createRun();
  assert.equal(advanceLesson(guide, initial, reply('DECR stock', 0)).index, 0);
  assert.equal(advanceLesson(guide, initial, reply('SET stock 2', null)).index, 0);
  assert.equal(advanceLesson(guide, initial, reply('SET stock 2', 'OK', false)).index, 0);
});
test('Null, empty string and integer zero have different meanings', () => {
  assert.equal(matchesExpected({ kind: 'equals', value: null }, null), true);
  for (const value of ['', 0, undefined, 'null']) assert.equal(matchesExpected({ kind: 'equals', value: null }, value), false);
  assert.equal(matchesExpected({ kind: 'equals', value: 0 }, '0'), false);
});
test('Set membership ignores ordering but list order is checked', () => {
  const unordered = { kind: 'set', value: ['java', 'redis'] };
  assert.equal(matchesExpected(unordered, ['redis', 'java']), true);
  assert.equal(matchesExpected(unordered, ['java', 'java']), false);
  assert.equal(matchesExpected({ kind: 'equals', value: ['java', 'redis'] }, ['redis', 'java']), false);
});
test('TTL bounds reject expired and persistent keys', () => {
  const ttl = { kind: 'integerRange', min: 1, max: 120 };
  assert.equal(matchesExpected(ttl, 115), true);
  for (const value of [-2, -1, 0, 121, NaN, '60']) assert.equal(matchesExpected(ttl, value), false);
});
test('Lesson sessions isolate courses and fit the Java gateway limits', () => {
  const id = '12345678-1234-1234-1234-123456789abc';
  assert.notEqual(lessonSession(id, 'redis'), lessonSession(id, 'redis-lock'));
  assert.match(lessonSession(id, 'redis-persistence'), /^[A-Za-z0-9_-]{8,64}$/);
  assert.throws(() => lessonSession('bad id', 'redis'));
});
test('A restarted run does not carry previous step evidence', () => {
  let run = advanceLesson(guide, createRun(), reply('SET stock 2', 'OK'));
  assert.equal(run.evidence.length, 1);
  run = createRun();
  assert.equal(run.index, 0);
  assert.deepEqual(run.evidence, []);
});
