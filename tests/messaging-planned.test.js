import test from 'node:test';
import assert from 'node:assert/strict';
import { simulate } from '../src/models.js';

const scenarios = {
  'kafka-consumer': ['assign', 'rebalance', 'coop'],
  'kafka-storage': ['log', 'zerocopy', 'roll'],
  'rabbitmq-cluster': ['topology', 'failover', 'quorum'],
  'rocketmq-dledger': ['master-slave', 'raft', 'return'],
};
for (const [lab, list] of Object.entries(scenarios)) {
  for (const scenario of list) {
    const run = simulate(lab, { scenario });
    assert.ok(run.length > 5, `${lab}/${scenario} should advance beyond the intro frame`);
    assert.equal(run.at(-1).message.includes('运行结束'), true, `${lab}/${scenario} should end with a summary`);
  }
}

test('Kafka consumer scenario ①: 4 members join, range assigns 6 partitions, group-unique consumption', () => {
  const end = simulate('kafka-consumer', { scenario: 'assign' }).at(-1);
  assert.deepEqual({ ...end?.metrics }, { joins: 4, assigned: 6, consumed: 6, rebalances: 0, revoked: 0, handed: 0 });
  assert.equal(end?.items.members.length, 4);
  assert.equal(end?.items.members.every(m => m.st === 'ok'), true);
});
test('Kafka consumer scenario ②: C3 death triggers eager rebalance — revoke all 6, re-assign 12', () => {
  const end = simulate('kafka-consumer', { scenario: 'rebalance' }).at(-1);
  assert.deepEqual({ ...end?.metrics }, { joins: 4, assigned: 12, consumed: 8, rebalances: 1, revoked: 6, handed: 0 });
  assert.equal(end?.items.members.find(m => m.id === 'C3').st, 'dead');
});
test('Kafka consumer scenario ③: cooperative handover revokes only 2 partitions', () => {
  const end = simulate('kafka-consumer', { scenario: 'coop' }).at(-1);
  assert.deepEqual({ ...end?.metrics }, { joins: 4, assigned: 6, consumed: 6, rebalances: 1, revoked: 2, handed: 2 });
  assert.equal(end?.items.members.find(m => m.id === 'C2').st, 'left');
});

test('Kafka storage scenario ①: 4 sequential appends + 2 binary searches via sparse index', () => {
  const end = simulate('kafka-storage', { scenario: 'log' }).at(-1);
  assert.deepEqual({ ...end?.metrics }, { writes: 4, found: 2, copied: 0, direct: 0, rolled: 0, deleted: 0, freed: 0 });
  assert.equal(end?.items.segs.length, 1);
  assert.equal(end?.items.segs[0].st, 'ok');
});
test('Kafka storage scenario ②: traditional path 4 copies vs sendfile 2 direct sends', () => {
  const end = simulate('kafka-storage', { scenario: 'zerocopy' }).at(-1);
  assert.deepEqual({ ...end?.metrics }, { writes: 1, found: 0, copied: 4, direct: 2, rolled: 0, deleted: 0, freed: 0 });
});
test('Kafka storage scenario ③: S0 rolls to S1 then expires as a whole segment freeing 4 messages', () => {
  const end = simulate('kafka-storage', { scenario: 'roll' }).at(-1);
  assert.deepEqual({ ...end?.metrics }, { writes: 8, found: 0, copied: 0, direct: 0, rolled: 1, deleted: 1, freed: 4 });
  assert.equal(end?.items.segs[0].st, 'gone');
  assert.equal(end?.items.segs[1].st, 'ok');
});

test('RabbitMQ cluster scenario ①: cross-node routing delivers to host nodes, 2 writes + 2 consumes', () => {
  const end = simulate('rabbitmq-cluster', { scenario: 'topology' }).at(-1);
  assert.deepEqual({ ...end?.metrics }, { routed: 2, written: 2, consumed: 2, unavailable: 0, promoted: 0, acks: 0 });
  assert.equal(end?.items.brokers.length, 3);
  assert.equal(end?.items.brokers.every(b => b.st === 'ok'), true);
});
test('RabbitMQ cluster scenario ②: B2 down — plain queue unavailable, mirrored queue promoted on B3', () => {
  const end = simulate('rabbitmq-cluster', { scenario: 'failover' }).at(-1);
  assert.deepEqual({ ...end?.metrics }, { routed: 0, written: 3, consumed: 3, unavailable: 1, promoted: 1, acks: 0 });
  assert.equal(end?.items.queues.find(q => q.id === 'Q普通').st, 'down');
  assert.equal(end?.items.brokers.find(b => b.id === 'B2').st, 'down');
});
test('RabbitMQ cluster scenario ③: quorum queue survives B1 down — 4 writes × 2 acks = 8 majority acks', () => {
  const end = simulate('rabbitmq-cluster', { scenario: 'quorum' }).at(-1);
  assert.deepEqual({ ...end?.metrics }, { routed: 0, written: 4, consumed: 4, unavailable: 0, promoted: 0, acks: 8 });
});

test('RocketMQ DLedger scenario ①: async master-slave loses the 2 in-flight writes after A down', () => {
  const end = simulate('rocketmq-dledger', { scenario: 'master-slave' }).at(-1);
  assert.deepEqual({ ...end?.metrics }, { writes: 6, synced: 4, lost: 2, failovers: 1, elected: 0, replayed: 0 });
  assert.equal(end?.items.roles.find(r => r.id === 'B').role, '新主');
});
test('RocketMQ DLedger scenario ②: raft majority commits 5/5 with zero loss after A down', () => {
  const end = simulate('rocketmq-dledger', { scenario: 'raft' }).at(-1);
  assert.deepEqual({ ...end?.metrics }, { writes: 5, synced: 5, lost: 0, failovers: 0, elected: 1, replayed: 0 });
  assert.equal(end?.items.roles.find(r => r.id === 'A').st, 'down');
});
test('RocketMQ DLedger scenario ③: old leader A returns and catches up 2 log entries as follower', () => {
  const end = simulate('rocketmq-dledger', { scenario: 'return' }).at(-1);
  assert.deepEqual({ ...end?.metrics }, { writes: 4, synced: 4, lost: 0, failovers: 0, elected: 0, replayed: 2 });
  assert.equal(end?.items.roles.find(r => r.id === 'A').role, 'follower');
});
