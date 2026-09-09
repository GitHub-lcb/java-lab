import test from 'node:test';
import assert from 'node:assert/strict';
import { simulate } from '../src/models.js';

const scenarios = {
  'zk-lock': ['ephemeral', 'queue', 'watch'],
  'seata-tx': ['success', 'rollback', 'tcc'],
  zab: ['broadcast', 'crash', 'recovery'],
};
for (const [lab, list] of Object.entries(scenarios)) {
  for (const scenario of list) {
    const run = simulate(lab, { scenario });
    assert.ok(run.length > 5, `${lab}/${scenario} should advance beyond the intro frame`);
    assert.equal(run.at(-1).message.includes('运行结束'), true, `${lab}/${scenario} should end with a summary`);
  }
}

test('ZK lock scenario ①: A creates the ephemeral node and holds; B watches, is woken by delete, then the session timeout auto-cleans', () => {
  const run = simulate('zk-lock', { scenario: 'ephemeral' });
  const end = run.at(-1);
  assert.deepEqual({ ...end?.metrics }, { acquires: 2, releases: 2, queued: 0, woken: 1 });
  assert.equal(end?.items.holder, null);
  assert.deepEqual(end?.items.waiters, []);
  const withWaiter = run.find(f => f.items.holder?.id === 'A' && f.items.waiters.length === 1);
  assert.equal(withWaiter?.items.waiters[0].id, 'B');
  assert.equal(withWaiter?.items.waiters[0].txt, 'watch /lock · NodeDeleted');
});
test('ZK lock scenario ②: sequential tickets queue fair waiters and each release wakes exactly the next one', () => {
  const run = simulate('zk-lock', { scenario: 'queue' });
  const end = run.at(-1);
  assert.deepEqual({ ...end?.metrics }, { acquires: 3, releases: 3, queued: 2, woken: 2 });
  assert.equal(end?.items.holder, null);
  assert.deepEqual(end?.items.waiters, []);
  assert.deepEqual(end?.items.queue, []);
  const allThree = run.find(f => f.items.holder?.id === 'A' && f.items.queue.length === 3);
  assert.deepEqual(allThree?.items.queue.map(q => q.id), ['c-0000', 'c-0001', 'c-0002']);
  assert.equal(allThree?.items.queue[0].st, 'hold');
  assert.equal(allThree?.items.waiters.length, 2);
});
test('ZK lock scenario ③: releasing a herd-watched lock wakes all three, then precise predecessor watches wake one at a time', () => {
  const end = simulate('zk-lock', { scenario: 'watch' }).at(-1);
  assert.deepEqual({ ...end?.metrics }, { acquires: 4, releases: 3, queued: 2, woken: 5 });
  assert.equal(end?.items.holder.id, 'Z');
  assert.equal(end?.items.holder.st, 'hold');
  assert.deepEqual(end?.items.waiters, []);
  assert.deepEqual(end?.items.queue.map(q => q.id), ['c-0002']);
  assert.equal(end?.items.queue[0].owner, 'Z');
  assert.equal(end?.items.queue[0].st, 'hold');
  assert.match(end?.message, /惊群/);
});

test('Seata scenario ①: three AT branches register with undo_log, then phase two only deletes those logs', () => {
  const end = simulate('seata-tx', { scenario: 'success' }).at(-1);
  assert.deepEqual({ ...end?.metrics }, { globals: 1, branches: 3, commits: 3, rollbacks: 0, undos: 0 });
  assert.equal(end?.items.xid, 'TX-001');
  assert.deepEqual(end?.items.branches.map(b => b.txt), ['提交完成', '提交完成', '提交完成']);
  assert.ok(end?.items.branches.every(b => b.note === 'undo_log 已删除'));
  assert.deepEqual(end?.items.chips, []);
});
test('Seata scenario ②: the unregistered account failure rolls back the two registered branches via undo_log reverse SQL', () => {
  const run = simulate('seata-tx', { scenario: 'rollback' });
  const end = run.at(-1);
  assert.deepEqual({ ...end?.metrics }, { globals: 1, branches: 2, commits: 0, rollbacks: 2, undos: 2 });
  assert.equal(end?.items.xid, 'TX-002');
  assert.equal(end?.items.branches.length, 2);
  assert.ok(end?.items.branches.every(b => !b.id.includes('账户')));
  assert.deepEqual(end?.items.branches.map(b => b.txt), ['已回滚', '已回滚']);
  assert.deepEqual(end?.items.chips.map(c => c.t), ['订单 #9002 恢复', '库存 98→100']);
  assert.ok(end?.items.chips.every(c => c.st === 'ok'));
});
test('Seata scenario ③: TCC confirms three Try branches, then a failed Try cancels the two reserved ones without undo_log', () => {
  const end = simulate('seata-tx', { scenario: 'tcc' }).at(-1);
  assert.deepEqual({ ...end?.metrics }, { globals: 2, branches: 5, commits: 3, rollbacks: 2, undos: 0 });
  assert.equal(end?.items.xid, 'TX-004');
  const byId = id => end?.items.branches.find(b => b.id === id);
  assert.equal(byId('rm1 · 订单')?.txt, 'Cancel 解冻');
  assert.equal(byId('rm2 · 库存')?.txt, 'Cancel 解冻');
  assert.equal(byId('rm3 · 账户')?.txt, 'Try ✗');
  assert.equal(byId('rm3 · 账户')?.st, 'bad');
  assert.equal(end?.items.chips.filter(c => c.st === 'ok').length, 3);
  assert.equal(end?.items.chips.filter(c => c.st === 'bad').length, 2);
  assert.equal(end?.items.branches.every(b => !b.note.includes('undo')), true);
});

test('ZAB scenario ①: every proposal is broadcast to both followers and committed after a quorum of ACKs', () => {
  const end = simulate('zab', { scenario: 'broadcast' }).at(-1);
  assert.deepEqual({ ...end?.metrics }, { writes: 3, acks: 6, commits: 3, elections: 0, discarded: 0, synced: 0 });
  const roles = Object.fromEntries(end?.items.roles.map(r => [r.id, r.txt]));
  assert.equal(roles.A, 'A · e1 Leader');
  assert.equal(roles.B, 'B · e1 Follower');
  assert.deepEqual(end?.items.chips.map(c => c.t), ['T1 (e1,1)', 'T2 (e1,2)', 'T3 (e1,3)']);
});
test('ZAB scenario ②: T2 with a single ACK is discarded after the election, and the e2 leader commits T3 with one follower vote', () => {
  const end = simulate('zab', { scenario: 'crash' }).at(-1);
  assert.deepEqual({ ...end?.metrics }, { writes: 3, acks: 4, commits: 2, elections: 1, discarded: 1, synced: 0 });
  const byId = id => end?.items.roles.find(r => r.id === id);
  assert.equal(byId('A')?.st, 'down');
  assert.equal(byId('B')?.txt, 'B · e2 Leader');
  assert.equal(byId('C')?.txt, 'C · e1 Follower');
  assert.deepEqual(end?.items.chips.map(c => c.t), ['T1 (e1,1)', 'T3 (e2,1)']);
});
test('ZAB scenario ③: equal logs elect B by myid, and the returning old leader catches up as a follower', () => {
  const end = simulate('zab', { scenario: 'recovery' }).at(-1);
  assert.deepEqual({ ...end?.metrics }, { writes: 3, acks: 4, commits: 3, elections: 1, discarded: 0, synced: 1 });
  const byId = id => end?.items.roles.find(r => r.id === id);
  assert.equal(byId('A')?.txt, 'A · e2 Follower');
  assert.equal(byId('A')?.st, 'ok');
  assert.equal(byId('B')?.txt, 'B · e2 Leader');
  assert.deepEqual(end?.items.chips.map(c => c.t), ['T1 (e1,1)', 'T2 (e2,1)', 'T3 (e2,2)']);
});
