import test from 'node:test';
import assert from 'node:assert/strict';
import { simulate } from '../src/models.js';

test('Function Calling single round trip executes one tool and answers from its result', () => {
  const run = simulate('function-calling', { scenario: 'single' });
  const end = run.at(-1);
  assert.deepEqual({ ...end?.metrics }, { rounds: 2, calls: 1, executed: 1, retries: 0, blocked: 0 });
  assert.equal(end?.items.calls.length, 1);
  assert.equal(end?.items.calls[0].st, 'ok');
  assert.equal(end?.items.calls[0].res, '晴 · 22°C · 无雨');
  assert.match(end?.items.answer, /不用带伞/);
});
test('Function Calling parallel calls reject a malformed schema then self-correct', () => {
  const run = simulate('function-calling', { scenario: 'multi' });
  const end = run.at(-1);
  assert.deepEqual({ ...end?.metrics }, { rounds: 3, calls: 3, executed: 2, retries: 1, blocked: 0 });
  const states = end?.items.calls.map(c => c.st);
  assert.deepEqual(states, ['ok', 'bad', 'ok']);
  assert.match(end?.items.calls[1].full, /明天/);
  assert.equal(end?.items.calls[1].res, null);
  assert.match(end?.items.answer, /北京明天晴 22°C、上海多云 26°C/);
});
test('Function Calling allowlist blocks a hallucinated tool and converges', () => {
  const run = simulate('function-calling', { scenario: 'guard' });
  const end = run.at(-1);
  assert.deepEqual({ ...end?.metrics }, { rounds: 5, calls: 4, executed: 3, retries: 0, blocked: 1 });
  const blocked = end?.items.calls.find(c => c.st === 'block');
  assert.equal(blocked?.name, 'forecast_humidity');
  assert.match(end?.items.answer, /暂无湿度数据接口/);
});
test('Function Calling answer chip appears only after tool results were backfilled', () => {
  const run = simulate('function-calling', { scenario: 'single' });
  const beforeAnswer = run.findIndex(f => f.items.answer);
  assert.ok(beforeAnswer > 0);
  assert.equal(run[beforeAnswer - 1].items.calls[0].st, 'ok');
  assert.equal(run[beforeAnswer].items.answer.includes('22°C'), true);
});
