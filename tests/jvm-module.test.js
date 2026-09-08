import test from 'node:test';
import assert from 'node:assert/strict';
import { labs } from '../src/catalog.js';
import { simulate } from '../src/models.js';
import { jvmProbeCatalog } from '../src/jvmProbeCatalog.js';
import { fetchJvmProbe } from '../src/jvmRuntime.js';
import { jvmDeepDives } from '../src/jvmDeepDives.js';
import { getLearningModule, jvmStages } from '../src/assessmentCatalog.js';

test('JVM foundation milestone contains six complete lessons', () => {
  const lessons = labs.filter(lab => lab.module === 'JVM' && lab.milestone === 'foundation');
  assert.equal(lessons.length, 6);
  for (const lesson of lessons) {
    assert.equal(lesson.module, 'JVM', lesson.id);
    assert.ok(lesson.phase, lesson.id);
    assert.ok(lesson.goals.length >= 3, lesson.id);
    assert.ok(lesson.challenge.requirements.length >= 2, lesson.id);
    assert.equal(lesson.quiz.length, 2, lesson.id);
    assert.match(lesson.source, /^https:\/\/(docs\.oracle\.com|openjdk\.org)/, lesson.id);
  }
});

test('Bytecode interpreter keeps operands on the stack and returns the correct sum', () => {
  const run = simulate('jvm-bytecode', { left: 7, right: 5 });
  assert.ok(run.some(frame => frame.instruction === 'iadd'));
  assert.equal(run.at(-1).metrics.result, 12);
  assert.equal(run.at(-1).metrics.stackDepth, 0);
  assert.equal(run.at(-1).items.length, 0);
});

test('Parent delegation loads JDK classes at bootstrap and application classes in the app loader', () => {
  const jdk = simulate('jvm-classloading', { target: 'java.lang.String', custom: true }).at(-1);
  const application = simulate('jvm-classloading', { target: 'com.example.OrderService', custom: true }).at(-1);
  assert.equal(jdk.metrics.owner, 'Bootstrap');
  assert.equal(application.metrics.owner, 'Application');
  assert.equal(jdk.metrics.defined, 1);
  assert.equal(application.metrics.initialized, 1);
});

test('Java stacks are thread-private while heap objects are shared', () => {
  const one = simulate('jvm-memory', { threads: 1, depth: 4, objects: 6, classes: 3 }).at(-1);
  const three = simulate('jvm-memory', { threads: 3, depth: 4, objects: 6, classes: 3 }).at(-1);
  assert.equal(one.metrics.stackFrames, 4);
  assert.equal(three.metrics.stackFrames, 12);
  assert.equal(one.metrics.heapObjects, three.metrics.heapObjects);
  assert.equal(one.metrics.loadedClasses, three.metrics.loadedClasses);
});

test('TLAB moves small object allocations off the shared slow path', () => {
  const tlab = simulate('jvm-allocation', { objects: 12, size: 8, tlab: true, tlabCapacity: 32 }).at(-1);
  const shared = simulate('jvm-allocation', { objects: 12, size: 8, tlab: false, tlabCapacity: 32 }).at(-1);
  assert.ok(tlab.metrics.fastPath > 0);
  assert.ok(tlab.metrics.slowPath < shared.metrics.slowPath);
  assert.equal(tlab.metrics.allocated, 12);
});

test('Reachability analysis retains transitive objects and reclaims unreachable cycles', () => {
  const end = simulate('jvm-roots', { objects: 8, roots: 2, cycle: true }).at(-1);
  assert.equal(end.metrics.objects, 8);
  assert.equal(end.metrics.reachable, 4);
  assert.equal(end.metrics.reclaimed, 4);
  assert.equal(end.metrics.cyclesReclaimed, 1);
});

test('Generational collection remains part of the JVM foundation path', () => {
  const end = simulate('jvm', { eden: 4, allocations: 10, survival: 50 }).at(-1);
  assert.ok(end.metrics.collections >= 2);
  assert.ok(end.metrics.reclaimed > 0);
});

test('Every JVM foundation lesson maps to one safe real JDK probe', () => {
  const lessons = labs.filter(lab => lab.group === 'JVM 专题');
  assert.equal(Object.keys(jvmProbeCatalog).length, lessons.length);
  for (const lesson of lessons) {
    const probe = jvmProbeCatalog[lesson.id];
    assert.ok(probe?.probe, lesson.id);
    assert.ok(probe.evidence.length >= 2, lesson.id);
    assert.ok(probe.boundary, lesson.id);
  }
});

test('JVM probe client calls only catalogued probe endpoint and returns evidence', async () => {
  const calls = [];
  const fetcher = async url => { calls.push(url); return { ok: true, json: async () => ({ ok: true, probe: 'runtime', data: { javaVersion: '1.8' } }) }; };
  const result = await fetchJvmProbe(fetcher, 'http://127.0.0.1:8787', 'runtime');
  assert.equal(result.data.javaVersion, '1.8');
  assert.equal(calls[0], 'http://127.0.0.1:8787/api/jvm?probe=runtime');
});

test('JVM probe client surfaces gateway failures', async () => {
  const fetcher = async () => ({ ok: false, status: 400, json: async () => ({ ok: false, error: 'Unknown JVM probe' }) });
  await assert.rejects(() => fetchJvmProbe(fetcher, 'http://127.0.0.1:8787', 'shell'), /Unknown JVM probe/);
});

test('Every JVM lesson has a complete deep-dive chapter with official references', () => {
  const lessons = labs.filter(lab => lab.module === 'JVM');
  assert.deepEqual(Object.keys(jvmDeepDives).sort(), lessons.map(lesson => lesson.id).sort());
  for (const lesson of lessons) {
    const chapter = jvmDeepDives[lesson.id];
    assert.ok(chapter.question, lesson.id);
    assert.ok(chapter.scenario, lesson.id);
    assert.ok(chapter.chain.length >= 3, lesson.id);
    assert.equal(chapter.headers.length, 3, lesson.id);
    assert.ok(chapter.rows.length >= 3, lesson.id);
    assert.ok(chapter.misconception, lesson.id);
    assert.ok(chapter.transfer, lesson.id);
    assert.ok(chapter.answer, lesson.id);
    assert.ok(chapter.links.length >= 2, lesson.id);
    for (const [, url] of chapter.links) assert.match(url, /^https:\/\/(docs\.oracle\.com|openjdk\.org)/, lesson.id);
  }
});

test('JVM learning center has four complete stages linked to JVM lessons', () => {
  const lessonIds = new Set(labs.filter(lab => lab.module === 'JVM').map(lab => lab.id));
  assert.equal(jvmStages.length, 4);
  for (const stage of jvmStages) {
    assert.equal(stage.questions.length, 5, stage.id);
    assert.equal(stage.passScore, 4, stage.id);
    for (const question of stage.questions) {
      assert.ok(lessonIds.has(question.lessonId), question.id);
      assert.equal(question.choices.length, 3, question.id);
      assert.ok(question.explanation, question.id);
    }
  }
  const module = getLearningModule('JVM');
  assert.equal(module.name, 'JVM');
  assert.equal(module.stages, jvmStages);
});

test('Collector comparison separates pause work, parallel workers and concurrent work', () => {
  const workload = { youngMb: 128, livePercent: 50, gcThreads: 4 };
  const serial = simulate('jvm-collectors', { ...workload, collector: 'serial' }).at(-1);
  const parallel = simulate('jvm-collectors', { ...workload, collector: 'parallel' }).at(-1);
  const g1 = simulate('jvm-collectors', { ...workload, collector: 'g1' }).at(-1);
  assert.equal(serial.metrics.reclaimedMb, 64);
  assert.equal(parallel.metrics.reclaimedMb, serial.metrics.reclaimedMb);
  assert.equal(g1.metrics.reclaimedMb, serial.metrics.reclaimedMb);
  assert.equal(serial.metrics.workers, 1);
  assert.equal(parallel.metrics.workers, 4);
  assert.ok(parallel.metrics.pauseWork < serial.metrics.pauseWork);
  assert.ok(g1.metrics.concurrentWork > 0);
});

test('Collector lesson observes the current JVM without claiming a comparison benchmark', () => {
  const lesson = labs.find(lab => lab.id === 'jvm-collectors');
  assert.equal(lesson.milestone, 'diagnostics');
  assert.equal(jvmProbeCatalog[lesson.id].probe, 'gc');
  assert.match(jvmProbeCatalog[lesson.id].boundary, /不.*基准/);
});
