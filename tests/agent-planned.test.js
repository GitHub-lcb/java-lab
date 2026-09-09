import test from 'node:test';
import assert from 'node:assert/strict';
import { simulate } from '../src/models.js';
import { labs } from '../src/catalog.js';

const scenarios = {
  'rag-chain': ['recall', 'truncate', 'cite'],
  'llm-streaming': ['sse', 'interrupt', 'compare'],
  'mcp-protocol': ['disc', 'call', 'scope'],
  'agent-memory': ['stm', 'ltm', 'fix'],
  'multi-agent': ['plan', 'retry', 'cmp'],
  'llm-inference': ['prefill', 'kv', 'long'],
  'embedding-vector': ['vec', 'nn', 'hybrid'],
};
for (const [lab, list] of Object.entries(scenarios)) {
  for (const scenario of list) {
    const run = simulate(lab, { scenario });
    assert.ok(run.length > 5, `${lab}/${scenario} should advance beyond the intro frame`);
    assert.equal(run.at(-1).message.includes('运行结束'), true, `${lab}/${scenario} should end with a summary`);
  }
}

test('RAG scenario ①: query embedding scores the doc chunks and top-2 recalls only the two cache-breakdown blocks', () => {
  const end = simulate('rag-chain', { scenario: 'recall' }).at(-1);
  assert.deepEqual({ ...end?.metrics }, { queries: 1, recalls: 2, chunks: 2, cited: 2, halls: 0 });
  assert.deepEqual(end?.items.ctx, ['c3', 'c4']);
  assert.deepEqual(end?.items.cites, ['c3', 'c4']);
  assert.equal(end?.items.halls.length, 0);
  assert.ok(end?.items.answer.short.includes('互斥锁'));
});
test('RAG scenario ②: head truncation drops the mid-document hit blocks and the model fabricates, then rerank re-opens the window', () => {
  const run = simulate('rag-chain', { scenario: 'truncate' });
  const end = run.at(-1);
  assert.deepEqual({ ...end?.metrics }, { queries: 1, recalls: 4, chunks: 5, cited: 2, halls: 1 });
  assert.deepEqual(end?.items.ctx, ['b5', 'b6', 'b7']);
  assert.deepEqual(end?.items.cites, ['b6', 'b7']);
  assert.equal(end?.items.halls.length, 1);
  const naiveWindow = run.find(f => f.items.ctx.length === 2 && f.items.ctx[0] === 'b1');
  assert.deepEqual(naiveWindow.items.ctx, ['b1', 'b2']);
  assert.ok(end.items.answer.short.includes('1/N'));
});
test('RAG scenario ③: the retrieval-free baseline fabricates a fake citation while grounded generation rejects the low-score hits', () => {
  const end = simulate('rag-chain', { scenario: 'cite' }).at(-1);
  assert.deepEqual({ ...end?.metrics }, { queries: 2, recalls: 3, chunks: 0, cited: 0, halls: 1 });
  assert.equal(end?.items.halls.length, 1);
  assert.match(end?.items.halls[0], /假引用/);
  assert.deepEqual(end?.items.ctx, []);
  assert.deepEqual(end?.items.cites, []);
  assert.ok(end?.items.answer.short.includes('未收录'));
});

test('Streaming scenario ①: the generation loop flushes four SSE packets and the client renders the text cumulatively', () => {
  const end = simulate('llm-streaming', { scenario: 'sse' }).at(-1);
  assert.deepEqual({ ...end?.metrics }, { tokens: 13, sse: 4, ttft: 8, cancels: 0, resumes: 0 });
  assert.equal(end?.items.typed, '全场八折会员九五折先到先得');
  assert.deepEqual(end?.items.ev.map(e => e.txt), ['全场八折', '会员', '九五折', '先到先得']);
  assert.deepEqual(end?.items.ev.map(e => e.at), ['+8', '+9', '+10', '+11']);
  assert.equal(end?.items.chips.length, 0);
});
test('Streaming scenario ②: the disconnect drops the unsent buffer, then a Last-Event-ID=6 reconnect resumes from the breakpoint', () => {
  const run = simulate('llm-streaming', { scenario: 'interrupt' });
  const end = run.at(-1);
  assert.deepEqual({ ...end?.metrics }, { tokens: 19, sse: 4, ttft: 8, cancels: 1, resumes: 1 });
  assert.equal(end?.items.typed, '春夏出游季全场八折会员九五折满三件包邮');
  const cancelFrame = run.find(f => f.items.chips.length === 1);
  assert.match(cancelFrame?.items.chips[0].t, /7-12/);
  const reconnect = run.find(f => /Last-Event-ID: 6/.test(f.message));
  assert.equal(reconnect?.active, 'client');
  const mid = run.find(f => f.items.typed === '春夏出游季全');
  assert.equal(mid?.items.ev.length, 1);
});
test('Streaming scenario ③: the same text twice — full mode waits 13 ticks for the whole body while stream mode starts reading at t+3', () => {
  const end = simulate('llm-streaming', { scenario: 'compare' }).at(-1);
  assert.deepEqual({ ...end?.metrics }, { tokens: 24, sse: 5, ttft: 3, cancels: 0, resumes: 0 });
  assert.equal(end?.items.typed, '全场八折会员九五折满三件');
  assert.equal(end?.items.chips.length, 2);
  assert.match(end?.items.chips[0].t, /t\+13/);
  assert.match(end?.items.chips[1].t, /t\+3/);
  assert.deepEqual(end?.items.ev.map(e => e.at), ['+3', '+4', '+5', '+6']);
});

test('MCP scenario ①: the client discovers three calc tools and a single authorized call executes cleanly', () => {
  const end = simulate('mcp-protocol', { scenario: 'disc' }).at(-1);
  assert.deepEqual({ ...end?.metrics }, { lists: 1, calls: 1, ok: 1, errs: 0, blocks: 0 });
  assert.deepEqual(end?.items.tools.map(t => t.name), ['calc.add', 'calc.sqrt', 'calc.divide']);
  assert.equal(end?.items.reqs.length, 1);
  assert.equal(end?.items.reqs[0].st, 'ok');
  assert.equal(end?.items.reqs[0].res, '42');
});
test('MCP scenario ②: three in-flight calls stay correlated by id while a business failure arrives as isError instead of a protocol error', () => {
  const run = simulate('mcp-protocol', { scenario: 'call' });
  const end = run.at(-1);
  assert.deepEqual({ ...end?.metrics }, { lists: 1, calls: 3, ok: 2, errs: 1, blocks: 0 });
  assert.equal(end?.items.reqs.at(-1).res, '21');
  const failed = end?.items.reqs.find(r => r.st === 'err');
  assert.equal(failed?.tool, 'calc.divide');
  assert.equal(failed?.res, '除数不能为零');
  assert.match(failed?.note, /isError/);
  const isErrorFrame = run.find(f => /isError/.test(f.message) && f.items.flash?.st === 'warn');
  assert.equal(isErrorFrame?.items.flash.title.includes('协议层仍是成功响应'), true);
});
test('MCP scenario ③: scope checks block shell.run as unauthorized and /etc/hosts as out-of-bounds, and the agent works inside docs/ only', () => {
  const run = simulate('mcp-protocol', { scenario: 'scope' });
  const end = run.at(-1);
  assert.deepEqual({ ...end?.metrics }, { lists: 1, calls: 3, ok: 1, errs: 0, blocks: 2 });
  assert.equal(end?.items.reqs.filter(r => r.st === 'block').length, 2);
  assert.ok(end?.items.reqs.some(r => r.st === 'block' && r.res.includes('未授权')));
  assert.ok(end?.items.reqs.some(r => r.st === 'block' && r.res.includes('越界')));
  const policyFrames = run.filter(f => f.active === 'policy');
  assert.ok(policyFrames.length >= 2, 'scope scenario should visit the policy node twice');
  assert.ok(end?.message.includes('docs/ 内作业'));
});

test('Memory scenario ①: the fixed 4-message window rolls out m1-m3 in order, the budget constraint goes with m2, and the compressor pins it as a summary', () => {
  const run = simulate('agent-memory', { scenario: 'stm' });
  const end = run.at(-1);
  assert.deepEqual({ ...end?.metrics }, { msgs: 7, drop: 3, facts: 1, hit: 0, fix: 0 });
  assert.deepEqual(end?.items.msgs.filter(m => m.st === 'out').map(m => m.n), [1, 2, 3]);
  assert.deepEqual(end?.items.msgs.filter(m => m.st === 'in').map(m => m.n), [4, 5, 6, 7]);
  assert.ok(end?.items.sum.txt.includes('预算 ≤8000'));
  const amnesia = run.find(f => f.items.flash?.st === 'pink' && /失忆时刻/.test(f.items.flash.t));
  assert.ok(amnesia, 'the budget eviction should be flagged as the amnesia moment');
  const recovery = run.find(f => /摘要行里记着/.test(f.message));
  assert.ok(recovery, 'the model should re-answer m7 from the pinned summary');
});
test('Memory scenario ②: long-term recall answers from seeded facts, a missing entry is admitted rather than fabricated, and a fresh fact is queryable immediately', () => {
  const run = simulate('agent-memory', { scenario: 'ltm' });
  const end = run.at(-1);
  assert.deepEqual({ ...end?.metrics }, { msgs: 4, drop: 0, facts: 3, hit: 3, fix: 0 });
  const rows = end?.items.facts.map(f => `${f.k} = ${f.v}`);
  assert.deepEqual(rows, ['应用端口 = dev 9000', '预热任务书 = docs/warmup.md · 张伟', '网关超时 = 800ms']);
  assert.equal(end?.items.facts.at(-1).extra, '来源：本次会话');
  const miss = run.find(f => f.items.flash?.st === 'warn' && /告警阈值/.test(f.items.flash.t));
  assert.ok(miss, 'the missing 告警阈值 query should flash a no-record warning');
  const honest = run.find(f => /没记录过/.test(f.message));
  assert.ok(honest, 'the model should admit the gap instead of inventing a threshold');
  const hits = new Set(run.filter(f => f.items.flash?.st === 'ok').map(f => f.items.flash.t).filter(t => /命中/.test(t)));
  assert.equal(hits.size, 3, 'two seeded recalls plus the self-check should all hit');
});
test('Memory scenario ③: a user correction overwrites the stale QPS value, the superseded release note is forgotten, and only the current version answers', () => {
  const run = simulate('agent-memory', { scenario: 'fix' });
  const end = run.at(-1);
  assert.deepEqual({ ...end?.metrics }, { msgs: 4, drop: 1, facts: 2, hit: 1, fix: 1 });
  const cap = end?.items.facts.find(f => f.k === '容量上限');
  assert.equal(cap?.st, 'fix');
  assert.match(cap?.v, /2000/);
  const releases = end?.items.facts.filter(f => f.k === '最近发版');
  assert.equal(releases.length, 1, 'only the re-recorded release survives the forget');
  assert.match(releases[0]?.v, /9\/8/);
  assert.ok(!run.some(f => f.items.facts.filter(r => r.k === '最近发版').length > 1), 'no frame should hold two release versions at once');
  const forgetFlash = run.find(f => f.items.flash?.st === 'pink' && /遗忘清理/.test(f.items.flash.t));
  assert.ok(forgetFlash, 'the expiry cleanup should flash pink');
});

test('Orchestration scenario ①: the planner decomposes the mission, dispatches three contract-bound executors in parallel, and the writer assembles once all deps are ready', () => {
  const run = simulate('multi-agent', { scenario: 'plan' });
  const end = run.at(-1);
  assert.deepEqual({ ...end?.metrics }, { tasks: 3, retries: 0, degrades: 0, deps: 3, tick: 7 });
  assert.ok(end?.items.jobs.every(j => j.st === 'ok'), 'all three jobs should finish green');
  assert.deepEqual(end?.items.jobs.map(j => j.res), ['0.42%', '218ms', '892/1000']);
  assert.ok(end?.items.report.delivered && end?.items.report.gaps === 0);
  const ready3 = run.find(f => /依赖就绪 3\/3/.test(f.message));
  assert.ok(ready3, 'the third delivery should announce all deps ready');
  const assemble = run.find(f => f.active === 'writer' && /汇总启动/.test(f.message));
  assert.equal(assemble?.items.report, null, 'writer waits for deps before assembling');
  assert.equal(run.filter(f => f.active === 'planner').length >= 3, true);
});
test('Orchestration scenario ②: 503s get one retry each, T2 recovers while T3 is degraded to a snapshot, and the gap is flagged in the delivered report', () => {
  const run = simulate('multi-agent', { scenario: 'retry' });
  const end = run.at(-1);
  assert.deepEqual({ ...end?.metrics }, { tasks: 3, retries: 2, degrades: 1, deps: 3, tick: 12 });
  const t3 = end?.items.jobs.find(j => j.id === 'T3');
  assert.equal(t3?.st, 'deg');
  assert.match(t3?.res, /842\/1000/);
  const t2 = end?.items.jobs.find(j => j.id === 'T2');
  assert.equal(t2?.mark, '重试后成功');
  assert.equal(end?.items.report.gaps, 1);
  const failFrame = run.find(f => /T3 首轮失败/.test(f.message));
  assert.ok(failFrame);
  const degradeFrame = run.find(f => f.items.flash?.st === 'warn' && /降级/.test(f.items.flash.t));
  assert.ok(degradeFrame, 'degradation should flash a warning');
  const isolation = run.find(f => /T1 的数据全程没被/.test(f.message));
  assert.ok(isolation, 'the report delivery should call out the isolation');
  const retryNode = run.filter(f => f.active === 'retry');
  assert.ok(retryNode.length >= 2, 'the retry/degrade policy node should be visited twice');
});
test('Orchestration scenario ③: the same mission runs serial (Σ subtasks = 12 ticks) then parallel (max = 4 + assembly = 7 ticks) for a combined 19', () => {
  const run = simulate('multi-agent', { scenario: 'cmp' });
  const end = run.at(-1);
  assert.deepEqual({ ...end?.metrics }, { tasks: 6, retries: 0, degrades: 0, deps: 6, tick: 19 });
  assert.deepEqual(end?.items.chips.map(c => c.t), ['串行轮 · 12 ticks（3+4+2+3 依次相加）', '并行轮 · 7 ticks（max 4 + 汇总 3）']);
  const serialReport = run.find(f => f.items.round === 'serial' && f.items.report?.delivered);
  assert.ok(serialReport, 'the serial round should deliver a report before the parallel round starts');
  const maxLine = run.find(f => /max\(3,4,2\)=4/.test(f.message));
  assert.ok(maxLine, 'the slowest subtask should be identified as the parallel bottleneck');
  assert.equal(run.at(-1).items.jobs.length, 3);
});

test('Inference scenario ①: prefill encodes the 10-token prompt in one parallel pass, then decode emits 8 tokens one at a time to finish the ad copy', () => {
  const run = simulate('llm-inference', { scenario: 'prefill' });
  const end = run.at(-1);
  assert.deepEqual({ ...end?.metrics }, { tok: 18, pre: 10, dec: 8, dup: 0, cache: 0 });
  assert.equal(end?.items.prompt.length, 10);
  assert.equal(end?.items.gen.filter(g => g.st !== 'wait').length, 8);
  assert.equal(end?.items.assem, '全场五折起，先到先得，闭眼入');
  assert.equal(end?.items.phase.txt, '生成完毕 · 8/8 词元');
  const parallel = run.find(f => /预填充（1 跳，并行）/.test(f.message));
  assert.ok(parallel && parallel?.items.phase.txt.includes('预填 10 词元'));
  const lastStep = run.find(f => f.items.flash?.t.includes('已见 17 词元'));
  assert.ok(lastStep, 'the final decode step should attend to all 17 historical tokens');
});
test('Inference scenario ②: round A recomputes the whole history every step (8 recomputes), round B with KV cache appends only, ending at 18 cached tokens and zero recompute', () => {
  const run = simulate('llm-inference', { scenario: 'kv' });
  const end = run.at(-1);
  assert.deepEqual({ ...end?.metrics }, { tok: 36, pre: 20, dec: 16, dup: 8, cache: 18 });
  assert.equal(end?.items.chips.length, 2);
  assert.match(end?.items.chips[0].t, /轮 A 账本：8 步全量重算/);
  assert.match(end?.items.chips[1].t, /轮 B 账本：零重算/);
  const firstRecompute = run.find(f => f.metrics.dup === 1 && f.items.flash?.t.includes('重算 +1'));
  assert.ok(firstRecompute, 'round A step 1 should recompute the 10 prompt tokens once');
  const cacheOn = run.find(f => /轮 B · 开 KV Cache/.test(f.message));
  assert.equal(cacheOn?.metrics.cache, 10);
  const midB = run.find(f => f.metrics.cache === 14);
  assert.equal(midB?.metrics.dup, 8, 'once the cache opens the recompute counter must freeze');
  const zeroLedger = run.find(f => f.items.chips.length === 2 && f.items.chips[1].st === 'ok' && /零重算/.test(f.items.chips[1].t));
  assert.equal(zeroLedger?.metrics.cache, 18, 'round B ledger should land only after its last decode step');
  assert.equal(end?.items.assem, '全场五折起，先到先得，闭眼入');
  assert.ok(end?.message.includes('轮 A 重算 8 次'));
});
test('Inference scenario ③: an 8000-token annual report prefills in one pass, the full KV cache costs 4.2GB, and the sliding window evicts 6000 tokens for a 4x bandwidth speedup', () => {
  const run = simulate('llm-inference', { scenario: 'long' });
  const end = run.at(-1);
  assert.deepEqual({ ...end?.metrics }, { tok: 8032, pre: 8000, dec: 32, dup: 0, cache: 2000 });
  assert.equal(end?.items.phase.txt, '生成完毕 · 32 步');
  assert.equal(end?.items.assem, '本基金全年净值上涨8.2%科技与半导体持仓超配制造业板块出现回撤建议关注三季度调仓');
  const fullCache = run.find(f => f.metrics.cache === 8000 && f.items.flash?.t.includes('4.2GB'));
  assert.ok(fullCache, 'the full KV cache frame should quote the 4.2GB ledger');
  const slide = run.find(f => f.items.flash?.t.includes('逐出 6000 词元'));
  assert.equal(slide?.metrics.cache, 2000);
  assert.equal(slide?.items.chips.length, 1);
  assert.ok(end?.message.includes('约 4× 提速'));
  const speedup = run.find(f => f.items.flash?.t.includes('4× 提速'));
  assert.equal(speedup?.metrics.cache, 2000, 'the speedup frame must come after the window slides');
});

test('Embedding scenario ①: semantically rewritten orders cluster above 0.90 despite zero shared words, while the same word 苹果 splits by context', () => {
  const run = simulate('embedding-vector', { scenario: 'vec' });
  const end = run.at(-1);
  assert.deepEqual({ ...end?.metrics }, { vecs: 6, sims: 8, hops: 0, hits: 2, fuse: 0 });
  assert.equal(run[0].metrics.vecs, 4, 'the four corpus rows should be embedded at ingest');
  const q1 = run.find(f => f.items.rows.some(r => r.id === 'C1' && r.sc === 0.93) && f.items.rows.some(r => r.id === 'C2' && r.sc === 0.90));
  assert.ok(q1, 'q1 should score C1/C2 above 0.90 despite different words');
  const cluster = run.find(f => f.items.flash?.t.includes('同义聚拢'));
  assert.ok(cluster && cluster.items.rows.find(r => r.id === 'C1').st === 'hit');
  const context = run.find(f => f.items.flash?.t.includes('同词异义'));
  assert.equal(context?.items.rows.find(r => r.id === 'C4').sc, 0.88);
  assert.equal(context?.items.rows.find(r => r.id === 'C3').sc, 0.30, 'same surface word must not imply near vectors');
  assert.ok(end?.message.includes('同词异义'));
});
test('Embedding scenario ②: brute force pays 10000 scorings for exact top-3, HNSW navigates 96 nodes and misses D803, efSearch 200 walks 402 and aligns with brute', () => {
  const run = simulate('embedding-vector', { scenario: 'nn' });
  const end = run.at(-1);
  assert.deepEqual({ ...end?.metrics }, { vecs: 1, sims: 200, hops: 402, hits: 3, fuse: 0 });
  const brute = run.find(f => f.metrics.sims === 10000 && f.metrics.hits === 3);
  assert.deepEqual(brute?.items.rows.map(r => r.id), ['D7', 'D52', 'D803']);
  const navOnly = run.find(f => f.metrics.hops === 96 && f.metrics.sims === 0);
  assert.ok(navOnly, 'the HNSW walk should precede its rerank');
  const approx = run.find(f => f.metrics.sims === 20 && f.metrics.hits === 2);
  assert.deepEqual(approx?.items.rows.map(r => r.id), ['D7', 'D52', 'D801']);
  assert.equal(approx?.items.rows.find(r => r.id === 'D801').st, 'near');
  const miss = run.find(f => f.items.flash?.t.includes('D803 → D801'));
  assert.ok(miss, 'the missed recall should flash as the approximation warning');
  const aligned = run.find(f => f.metrics.hops === 402 && f.metrics.sims === 200 && f.metrics.hits === 3);
  assert.deepEqual(aligned?.items.rows.map(r => r.id), ['D7', 'D52', 'D803']);
  assert.equal(end?.items.chips.length, 3, 'each retrieval method should leave a ledger chip');
  assert.ok(end?.message.includes('efSearch 200 跳转 402'));
});
test('Embedding scenario ③: pure vector search ranks the 24999 over-budget flagship first, then the price filter drops D5/D4 and in-set rerank hits D1 at 7999', () => {
  const run = simulate('embedding-vector', { scenario: 'hybrid' });
  const end = run.at(-1);
  assert.deepEqual({ ...end?.metrics }, { vecs: 6, sims: 8, hops: 0, hits: 3, fuse: 1 });
  const vecRound = run.find(f => f.metrics.sims === 5 && f.items.rows.find(r => r.id === 'D5')?.sc === 0.90);
  assert.equal(vecRound?.items.rows.find(r => r.id === 'D5').st, 'top');
  assert.ok(run.find(f => f.items.flash?.t.includes('两条超预算')), 'the pure-vector top-3 must be flagged as over budget');
  const filtered = run.find(f => f.metrics.fuse === 1 && f.items.rows.find(r => r.id === 'D5')?.st === 'drop');
  assert.equal(filtered?.items.rows.find(r => r.id === 'D4').st, 'drop');
  const final = run.at(-1);
  assert.equal(final.items.rows.find(r => r.id === 'D1').st, 'hit');
  assert.equal(final.items.chips.length, 1);
  assert.ok(end?.message.includes('相似度定序，约束定界'));
});

test('Every Agent group live lesson keeps a complete catalog contract', () => {
  const lessons = labs.filter(lab => lab.group === 'Agent 智能体');
  assert.equal(lessons.length, 8);
  for (const lesson of lessons) {
    assert.ok(lesson.number, lesson.id);
    assert.equal(lesson.scenarios, undefined, lesson.id);
    assert.ok(lesson.nodes.length >= 5, lesson.id);
    assert.ok(lesson.edges.length >= 5, lesson.id);
    assert.ok(lesson.code.length >= 8, lesson.id);
    assert.ok(lesson.theory.length >= 3, lesson.id);
    assert.ok(lesson.goals.length >= 3, lesson.id);
    assert.ok(lesson.challenge.task.length > 80, lesson.id);
    assert.ok(lesson.challenge.hint.length > 40, lesson.id);
    assert.ok(lesson.challenge.success.length > 40, lesson.id);
    assert.equal(lesson.challenge.requirements.length, 3, lesson.id);
    assert.equal(lesson.quiz.length, 4, lesson.id);
    assert.match(lesson.source, /^https:\/\//, lesson.id);
  }
});
