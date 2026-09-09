import test from 'node:test';
import assert from 'node:assert/strict';
import { simulate } from '../src/models.js';

const scenarios = {
  'mysql-sharding': ['shard', 'rehash', 'ring'],
  'mysql-explain': ['plan', 'fail', 'force'],
  'es-query': ['bm25', 'query', 'rank'],
};
for (const [lab, list] of Object.entries(scenarios)) {
  for (const scenario of list) {
    const run = simulate(lab, { scenario });
    assert.ok(run.length > 5, `${lab}/${scenario} should advance beyond the intro frame`);
    assert.equal(run.at(-1).message.includes('运行结束'), true, `${lab}/${scenario} should end with a summary`);
  }
}

test('MySQL sharding scenario ①: eight orders route to 8 tables, shard-key queries locate, no-key query broadcasts', () => {
  const end = simulate('mysql-sharding', { scenario: 'shard' }).at(-1);
  assert.deepEqual({ ...end?.metrics }, { writes: 8, locates: 2, broadcast: 1, total: 8, moves: 0, nodes: 0 });
  assert.equal(end?.items.tables.reduce((n, t) => n + t.keys.length, 0), 8);
  assert.deepEqual(end?.items.tables[0].keys, ['o8']);
  assert.deepEqual(end?.items.tables[3].keys, ['o3']);
  assert.equal(end?.items.last?.kind, 'broadcast');
});
test('MySQL sharding scenario ②: 4→8 rehash retires all old tables and replays every row (100%)', () => {
  const end = simulate('mysql-sharding', { scenario: 'rehash' }).at(-1);
  assert.deepEqual({ ...end?.metrics }, { writes: 8, locates: 0, broadcast: 0, total: 8, moves: 8, nodes: 0 });
  assert.equal(end?.items.oldTables.length, 4);
  assert.equal(end?.items.oldTables.every(t => t.retired === true), true);
  assert.equal(end?.items.tables.reduce((n, t) => n + t.keys.length, 0), 8);
  assert.deepEqual(end?.items.tables[4].keys, ['o4']);
  assert.deepEqual(end?.items.tables[0].keys, ['o8']);
  assert.match(end?.message, /100%/);
});
test('MySQL sharding scenario ③: adding N5 to the hash ring moves only the two keys on its arc', () => {
  const end = simulate('mysql-sharding', { scenario: 'ring' }).at(-1);
  assert.deepEqual({ ...end?.metrics }, { writes: 0, locates: 0, broadcast: 0, total: 16, moves: 2, nodes: 5 });
  assert.deepEqual(end?.items.moved.map(m => m.k), ['o1', 'o2']);
  const n5 = end?.items.ring.find(n => n.id === 'N5');
  assert.equal(n5?.born, true);
  assert.deepEqual(n5?.keys, ['o1', 'o2']);
  const n2 = end?.items.ring.find(n => n.id === 'N2');
  assert.deepEqual(n2?.keys, ['o3', 'o4']);
  assert.match(end?.message, /12\.5%/);
});

test('MySQL explain scenario ①: three plans contrast const/ref/ALL with rows 1 vs 12 vs 10000', () => {
  const end = simulate('mysql-explain', { scenario: 'plan' }).at(-1);
  assert.deepEqual({ ...end?.metrics }, { plans: 3, const: 1, ref: 1, all: 1, failed: 0, degraded: 0, filesorts: 0, corrected: 0 });
  assert.deepEqual(end?.items.plans.map(p => p.rows), [1, 12, 10000]);
  assert.deepEqual(end?.items.plans.map(p => p.type), ['const', 'ref', 'ALL']);
});
test('MySQL explain scenario ②: expression wrap and implicit cast fail to ALL, missing leading column degrades to index', () => {
  const end = simulate('mysql-explain', { scenario: 'fail' }).at(-1);
  assert.deepEqual({ ...end?.metrics }, { plans: 3, const: 0, ref: 0, all: 2, failed: 2, degraded: 1, filesorts: 0, corrected: 0 });
  const types = end?.items.plans.map(p => p.type);
  assert.equal(types.filter(t => t === 'ALL').length, 2);
  assert.equal(types.filter(t => t === 'index').length, 1);
  assert.equal(end?.items.plans[2].st, 'warn');
});
test('MySQL explain scenario ③: FORCE INDEX swaps the filesorting plan for an early-stop range scan', () => {
  const end = simulate('mysql-explain', { scenario: 'force' }).at(-1);
  assert.deepEqual({ ...end?.metrics }, { plans: 2, const: 0, ref: 1, all: 0, failed: 0, degraded: 0, filesorts: 1, corrected: 1 });
  assert.equal(end?.items.plans[0].key, 'idx_user');
  assert.equal(end?.items.plans[1].key, 'idx_created');
  assert.equal(end?.items.plans[1].rows, 20);
});

test('ES query scenario ①: BM25 scores length normalization, term saturation and rare-word IDF weighting', () => {
  const end = simulate('es-query', { scenario: 'bm25' }).at(-1);
  assert.deepEqual({ ...end?.metrics }, { docs: 4, scored: 4, hits: 3, saturated: 1, rare: 1, queries: 2, matchHits: 0, termHits: 0, phraseHits: 0, boosted: 0, rescored: 0 });
  assert.equal(end?.items.chips.filter(c => c.st === 'warn').length, 1);
  assert.match(end?.items.cur[0].t, /3\.9/);
});
test('ES query scenario ②: match tokenizes with OR, term is exact-case, phrase requires adjacency and order', () => {
  const end = simulate('es-query', { scenario: 'query' }).at(-1);
  assert.deepEqual({ ...end?.metrics }, { docs: 3, scored: 0, hits: 0, saturated: 0, rare: 0, queries: 3, matchHits: 2, termHits: 0, phraseHits: 1, boosted: 0, rescored: 0 });
  assert.equal(end?.items.results.length, 3);
  assert.equal(end?.items.results[0].st, 'ok');
  assert.equal(end?.items.results[1].st, 'bad');
});
test('ES query scenario ③: title boost tops d1, function_score click-rate lifts d3 above d2', () => {
  const end = simulate('es-query', { scenario: 'rank' }).at(-1);
  assert.deepEqual({ ...end?.metrics }, { docs: 3, scored: 0, hits: 0, saturated: 0, rare: 0, queries: 3, matchHits: 0, termHits: 0, phraseHits: 0, boosted: 1, rescored: 1 });
  assert.match(end?.items.docs[0].t, /5\.00/);
  assert.match(end?.items.docs[2].t, /2\.35/);
  assert.equal(end?.items.chips.length, 2);
});
