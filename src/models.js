import { LRUCache } from 'lru-cache';
import { bisectRight } from 'd3-array';
import { redisRunners } from './redisModels.js';
import { jvmRunners } from './jvmModels.js';
import { agentRunners } from './agentModels.js';

export function javaHash(key) {
  let hash = 0;
  for (let i = 0; i < key.length; i++) hash = (Math.imul(hash, 31) + key.charCodeAt(i)) | 0;
  return hash;
}
export function bucketIndex(key, capacity) {
  const hash = javaHash(key);
  return (hash ^ (hash >>> 16)) & (capacity - 1);
}
const snapshot = (frames, state) => frames.push(structuredClone(state));

function redis(p) {
  const cache = new LRUCache({ max: p.capacity });
  const frames = [];
  const metrics = { requests: 0, hits: 0, misses: 0, rate: 0 };
  const keys = [p.key, `${p.key}:2`, p.key, `${p.key}:3`, p.key, `${p.key}:2`];
  const emit = (active, message, code, key) => {
    metrics.rate = metrics.requests ? Math.round(metrics.hits / metrics.requests * 100) : 0;
    snapshot(frames, { active, message, code, key, metrics, items: [...cache.keys()] });
  };
  emit('client', '实验就绪，等待请求进入。', 44 , p.key);
  for (let i = 0; i < p.requests; i++) {
    const key = keys[i % keys.length];
    metrics.requests++;
    emit('app', `请求 #${i + 1}：读取 ${key}`, 51 , key);
    if (p.cache && cache.get(key)) {
      metrics.hits++;
      emit('cache', `HIT · ${key} 已存在，直接返回缓存数据。`, 55 , key);
    } else {
      metrics.misses++;
      emit('db', `${p.cache ? 'MISS' : 'BYPASS'} · 从 MySQL 读取 ${key}。`, 59 , key);
      if (p.cache) {
        cache.set(key, { id: key });
        emit('cache', `SET · 写入 ${key}，缓存占用 ${cache.size}/${p.capacity}。`, 71 , key);
      }
    }
    emit('client', `请求 #${i + 1} 完成，返回用户数据。`, 74 , key);
  }
  return frames;
}
function threadpool(p) {
  const frames = [];
  const metrics = { submitted: 0, workers: 0, queued: 0, rejected: 0, caller: 0 };
  const items = [];
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items });
  emit('submit', '线程池就绪，当前工作线程数为 0。', 40 );
  for (let i = 1; i <= p.tasks; i++) {
    metrics.submitted++;
    if (metrics.workers < p.core) {
      metrics.workers++;
      items.push({ label: `task-${i}`, state: 'worker' });
      emit('workers', `task-${i}：创建核心线程执行。`, 4 );
    } else if (metrics.queued < p.queue) {
      metrics.queued++;
      items.push({ label: `task-${i}`, state: 'queue' });
      emit('queue', `task-${i}：核心线程忙碌，进入有界队列。`, 5 );
    } else if (metrics.workers < p.max) {
      metrics.workers++;
      items.push({ label: `task-${i}`, state: 'worker' });
      emit('workers', `task-${i}：队列已满，创建非核心线程。`, 6 );
    } else {
      const caller = p.policy === 'caller';
      metrics[caller ? 'caller' : 'rejected']++;
      items.push({ label: `task-${i}`, state: caller ? 'caller' : 'rejected' });
      emit('reject', `task-${i}：${caller ? '由提交任务的调用线程执行。' : '触发 AbortPolicy，任务被拒绝。'}`, 7 );
    }
  }
  return frames;
}
function hashmap(p) {
  const frames = [];
  const items = [];
  const metrics = { capacity: p.capacity, size: 0, threshold: Math.floor(p.capacity * p.factor), resizes: 0 };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items });
  emit('key', '空 HashMap 已初始化。', 30 );
  const keys = ['Aa', 'BB', 'java', 'redis', 'spring', 'kafka', 'mysql', 'jvm', 'lock', 'thread', 'cache', 'queue'];
  for (let i = 0; i < p.entries; i++) {
    const key = keys[i];
    emit('hash', `hash("${key}") = ${javaHash(key)}，计算扰动后的桶位置。`, 9 );
    items.push({ label: key, bucket: bucketIndex(key, metrics.capacity) });
    metrics.size++;
    emit('buckets', `PUT · ${key} → bucket[${bucketIndex(key, metrics.capacity)}]`, 14 );
    if (metrics.size > metrics.threshold) {
      metrics.capacity *= 2;
      metrics.threshold = Math.floor(metrics.capacity * p.factor);
      metrics.resizes++;
      items.forEach(item => { item.bucket = bucketIndex(item.label, metrics.capacity); });
      emit('resize', `size 超过 threshold，容量扩为 ${metrics.capacity}，重新分配桶位置。`, 39 );
    }
  }
  return frames;
}
function kafka(p) {
  const frames = [];
  const assignments = Array.from({ length: p.partitions }, (_, i) => i % p.consumers);
  const offsets = Array(p.partitions).fill(0);
  const items = Array.from({ length: p.partitions }, () => []);
  const metrics = { produced: 0, consumed: 0, lag: 0, idle: Math.max(0, p.consumers - p.partitions) };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, assignments, offsets, items });
  emit('producer', `consumer-group 中 ${Math.min(p.partitions, p.consumers)} 个消费者获得分区。`, 0);
  for (let i = 0; i < p.messages; i++) {
    const partition = i % p.partitions;
    items[partition].push(`m${i + 1}`);
    metrics.produced++;
    metrics.lag++;
    emit('broker', `消息 m${i + 1} 追加至 partition-${partition}，offset=${items[partition].length - 1}。`, 1);
  }
  for (let i = 0; i < p.messages; i++) {
    const partition = i % p.partitions;
    metrics.consumed++;
    metrics.lag--;
    emit('consumer', `consumer-${assignments[partition]} 消费 m${i + 1}，当前记录 offset=${Math.floor(i / p.partitions)}。`, 2);
    offsets[partition] = Math.floor(i / p.partitions) + 1;
    emit('offset', `COMMIT · partition-${partition} 提交下一条待消费位点 ${offsets[partition]}。`, 3);
  }
  return frames;
}
function kafkaReplication(p) {
  const frames = [];
  const metrics = { sent: 0, confirmed: 0, lost: 0, rejected: 0 };
  const replicas = Array.from({ length: Math.max(1, p.replicas) }, (_, i) => ({ id: `B${i + 1}`, role: i === 0 ? 'leader' : 'follower', up: true, held: 0 }));
  const isr = replicas.map(r => r.id);
  const snapshotState = () => snapshot(frames, { metrics, items: replicas.map(r => ({ ...r })), isr: [...isr] });
  const leader = () => replicas.find(r => r.role === 'leader' && r.up);
  const emit = (active, message, code) => { snapshotState(); frames.at(-1).active = active; frames.at(-1).message = message; frames.at(-1).code = code; };
  emit('follower', `分区就绪：${replicas.length} 个副本均在 ISR，Leader 为 ${replicas[0].id}，acks=${p.acks === '0' ? '0（发送即返回）' : p.acks === '1' ? '1（Leader 确认）' : 'all（ISR 全量确认）'}。`, 0);
  const ackWords = p.acks === '0' ? 'acks=0：发送方立即返回，不等待任何确认。' : p.acks === '1' ? 'acks=1：Leader 写入日志即向生产者确认。' : `acks=all：需 ISR 内 ${isr.length} 个副本全部复制后才确认。`;
  for (let i = 1; i <= p.messages; i++) {
    const current = leader();
    if (!current || (p.acks === 'all' && isr.length < p.minIsr)) {
      metrics.rejected++;
      const reason = !current ? `Leader 不可用` : `ISR(${isr.length}) < min.insync.replicas(${p.minIsr})`;
      emit('reject', `m${i} 写入被拒绝：${reason}。acks=all 下 ISR 不足时宁可拒绝也不给低安全确认。`, 3);
      continue;
    }
    metrics.sent++;
    current.held += 1;
    if (p.acks === '1') metrics.confirmed++;
    emit('leader', `m${i} 追加到 ${current.id}（现持 ${current.held} 条）。${ackWords}`, 0);
    if (p.crashAfter === i) {
      const crashed = current;
      crashed.up = false;
      isr.splice(isr.indexOf(crashed.id), 1);
      const next = replicas.find(r => r.up);
      if (next) {
        next.role = 'leader';
        if (p.acks === '1') metrics.lost++;
        emit('leader', `⚠ ${crashed.id} 崩溃：m${i} 尚未复制到 ISR 副本、只存在于崩溃 Leader 上${p.acks === '1' ? '——它已被确认，确认过的消息仍然丢失！' : p.acks === '0' ? '——发送方从未收到确认（acks=0 无从感知丢失）。' : '——它未满足全量确认，随崩溃丢弃；已确认的消息没有丢失。'} 新 Leader=${next.id}，ISR=[${isr.join(', ')}]。`, 1);
      } else {
        metrics.lost += i;
        emit('leader', `⚠ ${crashed.id} 崩溃且没有其他副本：已写入的 ${i} 条消息全部丢失，分区不可用。`, 1);
      }
      continue;
    }
    if (replicas.length > 1) {
      for (const r of replicas) if (r.role === 'follower' && r.up) r.held += 1;
      const synced = replicas.find(r => r.role === 'follower' && r.up);
      emit('follower', `ISR 副本复制 m${i} 完成（副本现持 ${synced?.held ?? 0} 条）。`, 2);
    }
    if (p.acks === 'all') {
      metrics.confirmed++;
      emit('commit', `m${i} 已由 ISR 全部副本复制并确认（提交位点推进到 m${i}）。`, 4);
    }
  }
  if (p.acks === 'all' && !leader() && p.messages > 0 && p.crashAfter === p.messages) {
    emit('reject', `分区已无可用 Leader，acks=all 的后续写入全部被拒绝。`, 3);
  }
  return frames;
}
function mysql(p) {
  const frames = [];
  const rows = Array.from({ length: 64 }, (_, i) => i + 1);
  const metrics = { reads: 0, scanned: 0, found: false, target: p.target };
  const emit = (active, message, code, items = []) => snapshot(frames, { active, message, code, metrics, items });
  emit('query', `SELECT * FROM users WHERE id = ${p.target}`, 0);
  if (p.indexed) {
    metrics.reads++;
    const page = bisectRight([17, 33, 49], p.target);
    emit('index', `读取根页，定位第 ${page + 1} 个叶子页。`, 1);
    metrics.reads++;
    const leaf = rows.slice(page * 16, page * 16 + 16);
    metrics.scanned = 1;
    metrics.found = leaf.includes(p.target);
    emit('leaf', `读取叶子页 [${leaf[0]}, ${leaf.at(-1)}]，在页内定位 id=${p.target}。`, 2, leaf);
  } else {
    for (let page = 0; page < 4; page++) {
      const leaf = rows.slice(page * 16, page * 16 + 16);
      metrics.reads++;
      metrics.scanned += leaf.includes(p.target) ? leaf.indexOf(p.target) + 1 : 16;
      metrics.found = leaf.includes(p.target);
      emit('leaf', `全表扫描第 ${page + 1} 页，已检查 ${metrics.scanned} 行。`, 3, leaf);
      if (metrics.found) break;
    }
  }
  emit('result', metrics.found ? `找到记录：{ id: ${p.target}, name: "user_${p.target}" }` : '未找到记录。', 4);
  return frames;
}
function mysqlIsolation(p) {
  const iso = p.isolation || 'repeatable';
  const isoName = { uncommitted: 'READ UNCOMMITTED', committed: 'READ COMMITTED', repeatable: 'REPEATABLE READ', serializable: 'SERIALIZABLE' }[iso];
  const levelNote = { uncommitted: '不设读保护：任何最新值（含未提交）直接可见。', committed: '每条语句独立创建一致性快照（语句级 ReadView）。', repeatable: '第一个一致性快照在事务首次读取时建立，之后整段事务复用。', serializable: '快照语义同可重复读，且范围读加锁，几乎完全串行。' }[iso];
  const frames = [];
  const metrics = { dirty: 0, nonRepeatable: 0, phantom: 0, blocked: 0 };
  const txA = { state: '未开始' };
  const txC = { state: '—' };
  const items = { txA: txA.state, txC: txC.state, txB: { row: 100, count: 2 }, row: { committed: 100 }, table: { committed: 2 } };
  let rowLatest = 100;
  let rowCommitted = 100;
  let countLatest = 2;
  let countCommitted = 2;
  let rowSnapshot = null;
  let countSnapshot = null;
  const emit = (active, message, code) => { items.txA = txA.state; items.txC = txC.state; items.row.committed = rowCommitted; items.table.committed = countCommitted; snapshot(frames, { active, message, code, metrics, items }); };
  const readRow = () => {
    let seen;
    if (iso === 'uncommitted') seen = rowLatest;
    else if (iso === 'repeatable' || iso === 'serializable') { if (rowSnapshot === null) rowSnapshot = rowCommitted; seen = rowSnapshot; }
    else seen = rowCommitted;
    items.txB.row = seen;
    return seen;
  };
  const readCount = () => {
    let seen;
    if (iso === 'uncommitted') seen = countLatest;
    else if (iso === 'repeatable' || iso === 'serializable') { if (countSnapshot === null) countSnapshot = countCommitted; seen = countSnapshot; }
    else seen = countCommitted;
    items.txB.count = seen;
    return seen;
  };
  emit('txb', `读事务 B 开始（隔离级别 = ${isoName}）。${levelNote}`, 0);
  emit('row', 'B 基线读 id=1：balance=100。', 1);
  readRow();
  txA.state = 'ACTIVE';
  rowLatest = 500;
  emit('txa', '写事务 A 将 id=1 的 balance 更新为 500，尚未提交。', 2);
  const dirtySeen = readRow();
  if (dirtySeen === 500) { metrics.dirty = 1; emit('undo', `⚠ B 读到 balance=500——A 还未提交！脏读发生：READ UNCOMMITTED 无视 ReadView，直接看到未提交版本。`, 3); }
  else emit('undo', `B 读到 balance=100：ReadView 只认已提交版本，A 的未提交修改对 B 不可见。`, 3);
  txA.state = 'COMMITTED';
  rowCommitted = 500;
  emit('txa', 'A 提交，balance=500 成为已提交版本。', 4);
  const rerunSeen = readRow();
  if (rerunSeen !== 100) { metrics.nonRepeatable = 1; emit('undo', `⚠ B 同事务第二次读到 balance=500（基线为 100）：不可重复读——每次语句新建快照，读到了提交后的新值。`, 5); }
  else emit('undo', `B 再次读到 balance=100：事务首个快照被整段复用，两次读值一致（可重复读）。`, 5);
  readCount();
  emit('row', 'B 范围查询：age BETWEEN 20 AND 30 共 2 行。', 6);
  if (iso === 'serializable') {
    metrics.blocked = 1;
    txC.state = 'BLOCKED';
    emit('txa', `⚠ 事务 C 尝试插入 25 岁新行，但该范围正被 B 的读锁保护——C 被阻塞等待，直到 B 提交。`, 7);
  } else {
    txC.state = 'COMMITTED';
    countLatest = 3; countCommitted = 3;
    emit('txa', `事务 C 插入 25 岁新行并提交，表现有 3 行（B 未锁范围，写入直接成功）。`, 7);
  }
  const phantomSeen = readCount();
  if (phantomSeen !== 2) { metrics.phantom = 1; emit('undo', `⚠ B 再次范围查询得到 ${phantomSeen} 行（此前 2 行）：幻读——范围查询的行集被并发插入改变。`, 9); }
  else emit('undo', `B 再次范围查询仍为 2 行：快照内行集稳定，未出现幻读。`, 9);
  if (iso === 'serializable') {
    txC.state = 'COMMITTED';
    countCommitted = 3;
    emit('txb', `B 提交，范围读锁释放；被阻塞的 C 插入随后完成，表最终 3 行。脏读 ${metrics.dirty} · 不可重复读 ${metrics.nonRepeatable} · 幻读 ${metrics.phantom} · 锁等待 ${metrics.blocked}`, 8);
  } else {
    emit('txb', `B 提交。本次运行结果——脏读 ${metrics.dirty} · 不可重复读 ${metrics.nonRepeatable} · 幻读 ${metrics.phantom} · 锁等待 ${metrics.blocked}`, 8);
  }
  return frames;
}
function rabbitmqExchange(p) {
  const type = p.exchange || 'topic';
  const typeName = { direct: 'DIRECT', topic: 'TOPIC', fanout: 'FANOUT' }[type];
  const directQ1 = ['orders.created'];
  const directQ2 = ['user.login'];
  const bindNote = type === 'direct' ? 'QueueA 绑定 orders.created · QueueB 绑定 user.login，仅精确匹配'
    : type === 'topic' ? 'QueueA 绑定 orders.* · QueueB 绑定 *.login，按通配符匹配'
      : '忽略 routingKey，向所有绑定队列广播';
  const matchTopic = (pattern, key) => {
    const ps = pattern.split('.');
    const ks = key.split('.');
    return ps.length === ks.length && ps.every((part, i) => part === '*' || part === ks[i]);
  };
  const targetOf = key => {
    if (type === 'fanout') return 'both';
    if (type === 'direct') {
      if (directQ1.includes(key)) return 'q1';
      if (directQ2.includes(key)) return 'q2';
      return null;
    }
    if (matchTopic('orders.*', key)) return 'q1';
    if (matchTopic('*.login', key)) return 'q2';
    return null;
  };
  const routeNote = key => type === 'direct' ? `key 与绑定完全一致` : type === 'topic' ? `key 匹配绑定模式` : 'FANOUT 无视 key 广播';
  const frames = [];
  const metrics = { q1: 0, q2: 0, dropped: 0, total: 0 };
  const items = { q1: [], q2: [], void: [] };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items });
  const msgs = [
    { key: 'orders.created', label: '下单事件' },
    { key: 'orders.paid', label: '支付完成' },
    { key: 'user.login', label: '登录事件' },
    { key: 'user.registered', label: '注册事件' },
    { key: 'metrics.heartbeat', label: '心跳指标' },
  ];
  emit('exchange', `交换机 ${typeName} 就绪：${bindNote}。`, 0);
  msgs.forEach((msg, i) => {
    const token = `m${i + 1}`;
    emit('producer', `basicPublish：发送「${token} · ${msg.label}」，routingKey=${msg.key}`, 1);
    const target = targetOf(msg.key);
    if (target === 'both') {
      items.q1.push(token); metrics.q1++; metrics.total++;
      items.q2.push(token); metrics.q2++; metrics.total++;
      emit('q1', `FANOUT 广播：${token} 同时投递 QueueA 与 QueueB（${routeNote(msg.key)}）。`, 2);
    } else if (target) {
      items[target].push(token);
      metrics[target]++; metrics.total++;
      const queue = target === 'q1' ? 'QueueA' : 'QueueB';
      const bound = target === 'q1' ? (type === 'direct' ? 'orders.created' : 'orders.*') : (type === 'direct' ? 'user.login' : '*.login');
      emit(target, `✓ ${routeNote(msg.key)}：routingKey=${msg.key} 命中 ${queue} 的绑定「${bound}」，${token} 入队。`, 2);
    } else {
      items.void.push(token);
      metrics.dropped++;
      emit('void', `✗ routingKey=${msg.key} 未命中 QueueA / QueueB 的任何绑定，${token} 在交换机处被丢弃。`, 3);
    }
  });
  emit('exchange', `路由完成：QueueA ${metrics.q1} 条 · QueueB ${metrics.q2} 条 · 未匹配丢弃 ${metrics.dropped} 条（${typeName}）。`, 4);
  return frames;
}
function rabbitmqAck(p) {
  const autoAck = !!p.autoAck;
  const maxRetry = Math.max(1, Math.min(4, p.maxRetry || 3));
  const frames = [];
  const metrics = { delivered: 0, acked: 0, requeued: 0, dead: 0 };
  const items = { queue: ['m1', 'm2', 'm3', 'm4', 'm5'], done: [], dead: [], lost: [] };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items });
  const labels = { m1: '订单创建', m2: '支付回调', m3: '出库单', m4: '积分变动', m5: '通知推送' };
  const fails = ['m2', 'm4'];
  const retries = {};
  emit('queue', `orders.work 就绪，${items.queue.length} 条消息待消费；autoAck=${autoAck ? '开（投递即确认）' : `关（手动确认，失败最多重试 ${maxRetry} 次）`}。m2、m4 处理必然抛异常。`, 0);
  while (items.queue.length) {
    const token = items.queue.shift();
    const attempt = (retries[token] = (retries[token] || 0) + 1);
    if (!fails.includes(token)) {
      metrics.delivered++;
      metrics.acked++;
      items.done.push(token);
      emit('consumer', `投递 ${token}（${labels[token]}）→ 处理成功，${autoAck ? 'autoAck 自动确认' : 'basicAck 确认'}，broker 删除消息。`, 1);
      continue;
    }
    if (autoAck) {
      metrics.delivered++;
      metrics.acked++;
      metrics.dead++;
      items.lost.push(token);
      emit('consumer', `投递 ${token}（${labels[token]}）即被 autoAck 确认，broker 删除消息。`, 1);
      emit('consumer', `✗ 处理 ${token} 抛异常，但消息已确认删除——无法重试，${token} 丢失（计入未成功消息）。`, 3);
      continue;
    }
    if (attempt < maxRetry) {
      metrics.delivered++;
      metrics.requeued++;
      items.queue.unshift(token);
      emit('consumer', `第 ${attempt}/${maxRetry} 次投递 ${token}（${labels[token]}）：处理失败，准备重回队列。`, 1);
      emit('queue', `basicNack(requeue=true)：${token} 回到队首，等待第 ${attempt + 1} 次投递。`, 2);
    } else {
      metrics.delivered++;
      metrics.dead++;
      items.dead.push(token);
      emit('consumer', `第 ${attempt}/${maxRetry} 次投递 ${token}（${labels[token]}）仍失败：basicNack(requeue=false) 拒收。`, 1);
      emit('dlx', `拒收触发死信路由：${token} 经 x-dead-letter-exchange 转发到死信队列。`, 3);
      emit('dead', `死信队列收到 ${token}（${labels[token]}），等待补偿处理。`, 3);
    }
  }
  emit('consumer', `运行结束：投递 ${metrics.delivered} · 确认 ${metrics.acked} · 回队 ${metrics.requeued} · 未成功 ${metrics.dead}（${autoAck ? 'autoAck 下为已确认即丢失' : '已全部进入死信队列'}）。`, 4);
  return frames;
}
function rocketmqTx(p) {
  const scenario = p.scenario || 'commit';
  const scene = scenario === 'commit' ? { tag: '① 事务成功 · Commit', note: '订单 #1001 落库并提交，事务状态表写入 COMMIT。' } : scenario === 'rollback' ? { tag: '② 业务失败 · Rollback', note: '订单扣减库存失败，本地事务回滚，事务状态表写入 ROLLBACK。' } : { tag: '③ 确认丢失 · 回查', note: '订单 #1001 落库并提交，但 Commit 确认在发送前发生进程/网络异常——Broker 从未收到。' };
  const frames = [];
  const metrics = { half: 0, committed: 0, rolledback: 0, checks: 0 };
  const items = { half: [], visible: [], gone: [] };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items });
  emit('producer', `事务消息演示就绪：场景「${scene.tag}」。单条消息 m1 · 订单 #1001 创建。`, 0);
  metrics.half++;
  items.half.push('m1');
  emit('producer', 'sendMessageInTransaction：producer 将 m1 作为半消息发出，并携带事务监听器待命。', 1);
  emit('half', 'Broker 半消息队列暂存 m1（PREPARED）：对消费者不可见，等待事务确认决定命运。', 1);
  emit('tx', `本地事务执行：${scene.note}`, 2);
  if (scenario === 'commit') {
    emit('commit', '事务执行器返回 COMMIT_MESSAGE：producer 向 Broker 发送 Commit 确认。', 3);
    metrics.committed++;
    items.half.shift();
    items.visible.push('m1');
    emit('commit', 'Broker 将 m1 从半消息转正为普通消息：对消费者可见，可被拉取。', 3);
    emit('consumer', '消费者拉取到 m1 · 订单 #1001 已入库，事务消息完成使命。', 5);
  } else if (scenario === 'rollback') {
    emit('commit', '事务执行器返回 ROLLBACK_MESSAGE：producer 向 Broker 发送 Rollback 确认。', 3);
    metrics.rolledback++;
    items.half.shift();
    items.gone.push('m1');
    emit('commit', 'Broker 删除半消息 m1：它从未对消费者可见，仿佛没有发生过。', 3);
    emit('consumer', '消费者侧没有任何新消息——回滚的 m1 不会投递出去。', 5);
  } else {
    emit('half', '⏳ Broker 迟迟未收到事务确认（网络异常或进程抖动），m1 悬停在半消息队列。', 3);
    emit('half', 'Broker 发起事务回查：checkLocalTransaction(m1) 回调 producer，询问本地事务的真实状态。', 4);
    metrics.checks++;
    emit('tx', '回查处理：producer 查询业务库事务状态表——订单 #1001 已提交，返回 COMMIT_MESSAGE。', 2);
    metrics.committed++;
    items.half.shift();
    items.visible.push('m1');
    emit('commit', 'Broker 收到回查结果 COMMIT：m1 转正为普通消息，悬空状态解除。', 3);
    emit('consumer', '消费者拉取到 m1 · 订单 #1001 已入库——最终一致达成。', 5);
  }
  emit('producer', `运行结束：半消息 ${metrics.half} 条 · 提交可见 ${metrics.committed} · 回滚丢弃 ${metrics.rolledback} · 回查 ${metrics.checks} 次。`, 4);
  return frames;
}
function syncLock(p) {
  const scenario = p.scenario || 'biased';
  const sceneTag = { biased: '① 无竞争 · 偏向锁', light: '② 交替竞争 · 轻量级锁', heavy: '③ 激烈竞争 · 重量级锁' }[scenario];
  const sceneNote = { biased: 'T1 单线程反复进入同一临界区——锁的首选项是偏向锁。', light: 'T1 持锁期间 T2 进入，但临界区很短——轻量级锁靠自旋即可完成交接。', heavy: 'T1 长时间持锁，T2 / T3 持续竞争——自旋让位给阻塞，线程 park 更划算。' }[scenario];
  const frames = [];
  const metrics = { locks: 0, spins: 0, blocks: 0, upgrades: 0 };
  const items = { state: 'unlocked', owner: null, spinners: [], waiters: [] };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items });
  emit('thread', `锁升级演示就绪：场景「${sceneTag}」。${sceneNote}`, 59 );
  if (scenario === 'biased') {
    emit('thread', 'T1 首次调用 critical()：Mark Word 处于无锁可偏向状态，尝试获取偏向锁。', 59 );
    items.state = 'biased';
    items.owner = 'T1';
    metrics.locks++;
    emit('mark', 'CAS 成功：T1 的线程 id 写入 Mark Word 偏向位 → 偏向锁（owner=T1）。此后 T1 重入无需再做 CAS。', 10 );
    emit('entry', 'T1 进入临界区执行——无竞争，没有任何同步开销。', 59 );
    metrics.locks++;
    emit('mark', 'T1 再次调用 critical()：偏向锁命中——同线程直接通过，零成本重入。', 10 );
    emit('thread', 'T1 退出临界区：偏向锁不立即释放，仍偏向 T1，等待下次快速获取。', 47 );
  } else if (scenario === 'light') {
    items.state = 'biased';
    items.owner = 'T1';
    metrics.locks++;
    emit('mark', 'T1 获取偏向锁进入临界区（owner=T1）。', 10 );
    emit('thread', 'T2 竞争进入：发现锁已偏向 T1（非本线程）→ 触发偏向撤销。', 59 );
    items.state = 'unlocked';
    items.owner = null;
    emit('mark', '撤销完成：Mark Word 回到无锁状态；T2 改走轻量级锁路径。', 18 );
    items.state = 'light';
    items.owner = 'T2';
    metrics.upgrades++;
    metrics.locks++;
    emit('mark', '轻量级加锁：T2 在栈中建 Lock Record 拷贝原 Mark Word，CAS 替换为指向锁记录的指针 → 轻量级锁（owner=T2）。', 18 );
    emit('thread', 'T1 再次进入：锁已是轻量级 → T1 同样建 Lock Record 尝试 CAS → 失败（T2 正持锁）。', 18 );
    items.spinners = ['T1'];
    metrics.spins += 2;
    emit('mark', 'T1 原地自旋重试 CAS：临界区很短，自旋 2 轮后 T2 即将释放。', 18 );
    items.spinners = [];
    items.owner = 'T1';
    metrics.locks++;
    emit('entry', 'T2 执行完毕释放锁 → T1 自旋 CAS 成功获锁（owner=T1），全程无人阻塞。', 47 );
    items.state = 'unlocked';
    items.owner = null;
    emit('thread', 'T1 执行完毕退出。本次运行：升级 1 次 · 自旋 2 · 阻塞 0——轻量级锁用自旋换无阻塞。', 47 );
  } else {
    items.state = 'light';
    items.owner = 'T1';
    metrics.upgrades++;
    metrics.locks++;
    emit('mark', '竞争开始：偏向锁先被撤销，以轻量级锁运行（升级 1 次），T1 获锁进入长临界区。', 18 );
    items.spinners = ['T2'];
    metrics.spins += 8;
    emit('thread', 'T2 进入：CAS 失败开始自旋……T1 的临界区迟迟不结束，自旋 8 轮仍未成功。', 18 );
    items.state = 'heavy';
    items.spinners = [];
    metrics.upgrades++;
    emit('mark', '自旋超过阈值仍未获锁 → 锁膨胀：Mark Word 由锁记录指针改为指向 ObjectMonitor（升级 2 次）。', 30 );
    items.waiters = ['T2', 'T3'];
    metrics.blocks += 2;
    emit('monitor', 'Monitor 接管：T2、T3 竞争失败 → park 挂起进入 EntryList 排队。', 31 );
    items.owner = null;
    emit('entry', 'T1 长临界区执行完毕，释放锁并通知 Monitor。', 47 );
    items.owner = 'T2';
    items.waiters = ['T3'];
    metrics.locks++;
    emit('monitor', 'Monitor 从 EntryList 唤醒队首 T2 并移交锁所有权（owner=T2）。', 31 );
    items.owner = 'T3';
    items.waiters = [];
    metrics.locks++;
    emit('monitor', 'T2 执行完毕释放 → 唤醒 T3（owner=T3）。', 31 );
    items.state = 'unlocked';
    items.owner = null;
    emit('thread', 'T3 执行完毕退出。本次运行：升级 2 次 · 阻塞 2——高竞争下 park 让出 CPU 优于空转自旋。', 47 );
  }
  emit('thread', `运行结束：加锁成功 ${metrics.locks} · 自旋 ${metrics.spins} · 阻塞 ${metrics.blocks} · 升级 ${metrics.upgrades} 次。`, 47 );
  return frames;
}
function aqsQueue(p) {
  const scenario = p.scenario || 'fair';
  const sceneTag = { fair: '① 公平模式 · FIFO 排队', reentrant: '② 可重入 · state 计数', nonfair: '③ 非公平 · 释放瞬间插队' }[scenario];
  const sceneNote = { fair: 'T1 持锁期间 T2 / T3 先后到达：公平锁要求按到达顺序排队，逐个获锁。', reentrant: 'T1 持锁后再次 lock 同一把锁：同一线程直接重入，state 递增而不排队。', nonfair: 'T2 已在队列中等待，T1 释放瞬间 T3 才到达——非公平锁允许 T3 先直接 CAS 抢一次。' }[scenario];
  const frames = [];
  const metrics = { locks: 0, reentries: 0, parks: 0, handoffs: 0 };
  const items = { state: 0, owner: null, queue: [] };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items });
  emit('thread', `AQS 队列演示就绪：场景「${sceneTag}」。${sceneNote}`, 14 );
  if (scenario === 'fair') {
    items.state = 1;
    items.owner = 'T1';
    metrics.locks++;
    emit('state', 'T1 lock()：state=0 空闲 → CAS 置 1 成功，锁归 T1，CLH 队列为空。', 3 );
    items.queue = ['T2'];
    metrics.parks++;
    emit('queue', 'T2 lock()：state=1 已占用，CAS 必然失败 → 入队为队尾并 park 挂起。CLH = [T2]。', 4 );
    items.queue = ['T2', 'T3'];
    metrics.parks++;
    emit('queue', 'T3 lock()：同样失败 → 排在 T2 之后。CLH = [T2 → T3]，先进先出。', 4 );
    items.state = 0;
    items.owner = null;
    emit('cas', 'T1 unlock()：state 1→0；公平锁 unpark 队首后继 T2，让它醒来重新竞争。', 30 );
    items.state = 1;
    items.owner = 'T2';
    items.queue = ['T3'];
    metrics.locks++;
    metrics.handoffs++;
    emit('state', 'T2 被唤醒后 CAS 0→1 成功——FIFO 顺序获锁（owner=T2）并出队。', 3 );
    items.state = 0;
    items.owner = null;
    emit('cas', 'T2 unlock()：state 归零，继续 unpark 下一节点 T3。', 30 );
    items.state = 1;
    items.owner = 'T3';
    items.queue = [];
    metrics.locks++;
    metrics.handoffs++;
    emit('state', 'T3 按序获锁（owner=T3），队列清空。', 3 );
    items.state = 0;
    items.owner = null;
    emit('thread', 'T3 unlock() 释放。公平模式下每个线程都按到达顺序拿到锁。', 35 );
  } else if (scenario === 'reentrant') {
    items.state = 1;
    items.owner = 'T1';
    metrics.locks++;
    emit('state', 'T1 第一次 lock()：state 0→1（owner=T1）。', 3 );
    items.queue = ['T2'];
    metrics.parks++;
    emit('queue', 'T2 lock()：state=1 已占用 → 入队 park 等待。CLH = [T2]。', 4 );
    items.state = 2;
    metrics.reentries++;
    emit('state', 'T1 再次 lock()：owner 就是 T1 → 可重入，state 1→2，无需排队。重入的线程不受 CLH 队列影响。', 3 );
    items.state = 1;
    emit('cas', 'T1 第一次 unlock()：state 2→1——还没归零，锁仍然被 T1 持有。', 30 );
    items.state = 0;
    items.owner = null;
    emit('cas', 'T1 第二次 unlock()：state 1→0，锁才真正释放，unpark 队首 T2。', 30 );
    items.state = 1;
    items.owner = 'T2';
    items.queue = [];
    metrics.locks++;
    metrics.handoffs++;
    emit('state', 'T2 获锁（owner=T2），队列清空。', 3 );
    items.state = 0;
    items.owner = null;
    emit('thread', 'T2 unlock() 释放。state 从 2 递减到 0 需要两次 unlock。', 35 );
  } else {
    items.state = 1;
    items.owner = 'T1';
    metrics.locks++;
    emit('state', 'T1 lock()：CAS 0→1 成功（owner=T1）。', 3 );
    items.queue = ['T2'];
    metrics.parks++;
    emit('queue', 'T2 lock()：state=1 已占用 → 入队 park 等待。CLH = [T2]。', 4 );
    items.state = 0;
    items.owner = null;
    emit('cas', 'T1 unlock()：state 1→0，unpark 队首 T2——但 T2 刚醒，还没拿到锁。', 30 );
    items.state = 1;
    items.owner = 'T3';
    metrics.locks++;
    emit('cas', '非公平插队：释放瞬间 T3 到达，无视队列直接 CAS 0→1 成功——T3 抢在 T2 前面拿到锁（owner=T3，T2 仍在队列）。', 3 );
    items.state = 0;
    items.owner = null;
    emit('cas', 'T3 unlock()：state 归零，T2 这才真正有机会竞争。', 30 );
    items.state = 1;
    items.owner = 'T2';
    items.queue = [];
    metrics.locks++;
    metrics.handoffs++;
    emit('state', 'T2 终于获锁（owner=T2），队列清空。非公平锁吞吐更高，但排队线程可能被插队。', 3 );
    items.state = 0;
    items.owner = null;
    emit('thread', 'T2 unlock() 释放。本次运行 T3 插队 1 次、T2 全程排队。', 35 );
  }
  emit('thread', `运行结束：获取成功 ${metrics.locks} · 重入 ${metrics.reentries} · 排队 ${metrics.parks} · 唤醒移交 ${metrics.handoffs} 次。`, 35 );
  return frames;
}
function zookeeperLeader(p) {
  const scenario = p.scenario || 'crash-recovery';
  const sceneTag = { 'crash-recovery': '① Leader 宕机 · 崩溃恢复', partition: '② 网络分区 · 防脑裂', 'follower-down': '③ Follower 宕机 · 无需选举' }[scenario];
  const sceneNote = { 'crash-recovery': '本场景将演示 S1 宕机后 S2-S5 如何投票选出新 Leader。', partition: '本场景将演示分区把 S4/S5 隔离在少数派后，为什么选不出第二个 Leader。', 'follower-down': '本场景将演示 Follower 宕机为何不触发选举，恢复后如何追平。' }[scenario];
  const frames = [];
  const metrics = { elections: 0, votes: 0, handovers: 0, synced: 0 };
  const items = { states: ['LEADING', 'FOLLOWING', 'FOLLOWING', 'FOLLOWING', 'FOLLOWING'] };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items });
  emit('cluster', `ZK 集群就绪：5 节点 · quorum = 3，S1 为 Leader（zxid=9），写请求由它提案并过半确认。场景「${sceneTag}」。${sceneNote}`, 0);
  if (scenario === 'crash-recovery') {
    metrics.elections++;
    items.states = ['DOWN', 'LOOKING', 'LOOKING', 'LOOKING', 'LOOKING'];
    emit('fault', 'S1 宕机！S2-S5 与 Leader 的心跳超时 → 全部进入 LOOKING，触发第 1 次选举。投票规则：每节点先投自己 (zxid, myid)。', 1);
    metrics.votes = 4;
    emit('looking', '首轮投票：S2 投 (8, 2) · S3 投 (7, 3) · S4 投 (6, 4) · S5 投 (5, 5)。先比 zxid——数据最新者有资格当主；zxid 相同才比 myid。', 2);
    emit('ballot', 'S3/S4/S5 收到更大票值 (8, 2) 后改投 S2 → S2 得 4 票。S2 的 zxid=8 是多数派里最新的：S1 上独享的 9 号事务未复制给任何节点，不进入新任期。', 3);
    metrics.handovers++;
    items.states = ['DOWN', 'LEADING', 'FOLLOWING', 'FOLLOWING', 'FOLLOWING'];
    emit('leading', '4 票 ≥ quorum(3)：S2 当选新 Leader → 进入 LEADING；S3-S5 转 FOLLOWING，Leader 交接 1 次。', 4);
    metrics.synced = 3;
    emit('sync', 'S2 以 zxid=8 为基准向 S3/S4/S5 同步缺失事务（各自落后 7/6/5）——补齐后即可对外服务。', 5);
    emit('cluster', 'S3/S4/S5 全部追平 zxid=8，集群在新 Leader 带领下恢复读写；S1 重启后将以 Follower 身份追平加入。', 5);
  } else if (scenario === 'partition') {
    metrics.elections++;
    items.states = ['LEADING', 'FOLLOWING', 'FOLLOWING', 'LOOKING', 'LOOKING'];
    emit('fault', '网络分区：S4/S5 与 S1-S3 断开。多数派一侧（S1/S2/S3，3 台）心跳正常；S4/S5 超时进入 LOOKING，触发第 1 次选举——但圈子只有 2 台。', 1);
    metrics.votes = 2;
    emit('looking', '少数派侧投票：S4 投 (9, 4) · S5 投 (9, 5)——zxid 相同比 myid，S5 得 2 票（S4 改投）。', 2);
    emit('ballot', '2 票 < quorum(3)：少数派永远凑不齐过半，选举注定失败。若 2 台也能选主，多数派与少数派就会各有一个 Leader——脑裂。过半机制让「第二个 Leader」在规则上不可能出现。', 3);
    emit('leading', '多数派一侧不受影响：S1 保持 LEADING 继续服务，新写入正常提案并被 S2/S3 确认（zxid 9 → 10）。', 4);
    metrics.synced = 2;
    emit('sync', '分区恢复：S4/S5 重新连上 S1，请求补齐分区期间缺失的 10 号事务。', 5);
    items.states = ['LEADING', 'FOLLOWING', 'FOLLOWING', 'FOLLOWING', 'FOLLOWING'];
    emit('cluster', 'S4/S5 追平 zxid=10 → 转 FOLLOWING 重新加入。整个分区过程 Leader 始终是 S1：零交接，零脑裂。', 5);
  } else {
    items.states = ['LEADING', 'FOLLOWING', 'DOWN', 'FOLLOWING', 'FOLLOWING'];
    emit('fault', 'Follower S3 宕机：剩余 S1/S2/S4/S5 共 4 台 ≥ quorum(3)，法定人数仍满足——Follower 单点故障不触发选举。', 1);
    emit('leading', 'S3 缺席期间集群照常写入：S1 提案 10 号事务，S2/S4/S5 过半确认。只要多数派健康，写服务不中断。', 4);
    emit('looking', 'S3 重启：以 LOOKING 状态接入集群，向 S1 请求同步缺席期间的事务。', 2);
    metrics.synced = 1;
    emit('sync', 'S1 把缺失事务同步给 S3（zxid 9 → 10）。', 5);
    items.states = ['LEADING', 'FOLLOWING', 'FOLLOWING', 'FOLLOWING', 'FOLLOWING'];
    emit('cluster', 'S3 追平数据 → 转 FOLLOWING，重新加入服务。全程零选举、零交接。', 5);
  }
  emit('cluster', `运行结束：触发选举 ${metrics.elections} · 投票 ${metrics.votes} · Leader 交接 ${metrics.handovers} · 数据同步 ${metrics.synced} 台。`, 5);
  return frames;
}
function esInverted(p) {
  const scenario = p.scenario || 'match';
  const sceneTag = { match: '① match 查询 · 倒排构建', phrase: '② match_phrase · 位置与词序', update: '③ 文档更新删除 · 倒排维护' }[scenario];
  const sceneNote = { match: '本场景先写入 3 篇文档观察倒排构建，再发起 match 查询。', phrase: '本场景从已含 3 篇文档的索引出发，对比 match 与 match_phrase 的命中差异。', update: '本场景从已含 3 篇文档的索引出发，演示更新与删除对倒排词项的维护。' }[scenario];
  const frames = [];
  const metrics = { docs: 0, terms: 0, postings: 0, hits: 0 };
  const docs = [];
  const items = { inverted: [], query: null, qmode: 'match', found: [] };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items });
  function index(doc) { docs.push(doc); }
  function rebuild() {
    const map = new Map();
    for (const d of docs) for (const t of d.terms) {
      if (!map.has(t)) map.set(t, []);
      if (!map.get(t).includes(d.id)) map.get(t).push(d.id);
    }
    items.inverted = [...map.entries()].map(([term, list]) => ({ term, docs: list })).sort((a, b) => a.term.localeCompare(b.term));
    metrics.docs = docs.length;
    metrics.terms = items.inverted.length;
    metrics.postings = items.inverted.reduce((sum, t) => sum + t.docs.length, 0);
  }
  function query(text, mode, hits) {
    items.query = text;
    items.qmode = mode || 'match';
    items.found = hits;
    metrics.hits = hits.length;
  }
  emit('doc', `ES 索引就绪：blog 索引为空。场景「${sceneTag}」。${sceneNote}`, 0);
  if (scenario === 'match') {
    index({ id: 'm1', terms: ['java', '后端', '中间件', '缓存'] });
    rebuild();
    emit('analyzer', '写入 m1："java 后端 中间件 缓存" → standard 分词器切出 [java 后端 中间件 缓存]，逐词挂接倒排表（中文整词切分属教学示意，真实场景用 ik 分词器）。', 22 );
    index({ id: 'm2', terms: ['java', '框架', 'spring', '并发'] });
    rebuild();
    emit('analyzer', '写入 m2："java 框架 spring 并发" → java 已有 postings 追加 m2；新增词项 框架 / spring / 并发。', 22 );
    index({ id: 'm3', terms: ['后端', '服务', '分布式'] });
    rebuild();
    emit('token', '写入 m3："后端 服务 分布式" → 后端 postings 追加 m3；新增 服务 / 分布式。三篇文档全部入索引。', 22 );
    emit('inverted', '倒排表成形：9 个词项 · 11 条定位记录。postings 让「哪篇文档含 java」变成一次表查询——写入期的组织换查询期的零全库扫描。', 22 );
    query('java', 'match', ['m1', 'm2']);
    emit('query', 'match 查询 "java"：定位词项 java → postings [m1, m2]，2 篇命中；若全文扫描则要读完 3 篇原文逐一比对。', 22 );
    emit('result', '相关度：m1 与 m2 各含 1 次 java（词频相同）→ 同分返回；词频越高、词项越罕见（IDF 越大），BM25 打分越高。', 22 );
    emit('inverted', `运行结束：文档 ${metrics.docs} · 词项 ${metrics.terms} · 定位 ${metrics.postings} · 命中 ${metrics.hits} 篇。`, 22 );
  } else if (scenario === 'phrase') {
    index({ id: 'm1', terms: ['java', '后端', '中间件', '缓存'] });
    index({ id: 'm2', terms: ['java', '框架', 'spring', '并发'] });
    index({ id: 'm3', terms: ['后端', '服务', '分布式'] });
    rebuild();
    emit('doc', '索引已含 3 篇文档：m1 "java 后端 中间件 缓存" · m2 "java 框架 spring 并发" · m3 "后端 服务 分布式"。', 22 );
    emit('inverted', '倒排还记录 position：java 在 m1@1、m2@1；后端在 m1@2、m3@1。词与词是否相邻、谁先谁后，都由位置信息回答。', 22 );
    query('java 后端', 'match', ['m1', 'm2', 'm3']);
    emit('query', 'match "java 后端"：拆成词项 {java, 后端} 取并集 → m1（双词命中，得分最高）· m2 · m3 共 3 篇命中。', 22 );
    query('java 后端', 'match_phrase', ['m1']);
    emit('query', 'match_phrase "java 后端"：额外要求两词相邻且顺序一致 → 只有 m1（java@1 紧接 后端@2）命中。对照：若查 "后端 java" 词序反转，连 m1 也命中不了。', 22 );
    emit('inverted', `运行结束：文档 ${metrics.docs} · 词项 ${metrics.terms} · 定位 ${metrics.postings} · 命中 ${metrics.hits} 篇。`, 22 );
  } else {
    index({ id: 'm1', terms: ['java', '后端', '中间件', '缓存'] });
    index({ id: 'm2', terms: ['java', '框架', 'spring', '并发'] });
    index({ id: 'm3', terms: ['后端', '服务', '分布式'] });
    rebuild();
    emit('doc', '索引已含 3 篇文档（同 match 场景文档集）。本场景更新 m2、删除 m3，观察词项的增删。', 1 );
    docs[1] = { id: 'm2', terms: ['java', '框架', 'vertx', '异步'] };
    rebuild();
    emit('analyzer', 'UPDATE m2："java 框架 spring 并发" → "java 框架 vertx 异步"。底层是删除旧版本再索引新版本：spring / 并发 失去引用从倒排消失，vertx / 异步 挂接进来。', 1 );
    docs.splice(2, 1);
    rebuild();
    emit('token', 'DELETE m3：移除 后端 / 服务 / 分布式 的定位——服务、分布式 失去全部引用 → 词项消失；后端仍由 m1 引用，postings 收缩为 [m1]。', 2 );
    query('spring', 'match', []);
    emit('query', 'match 查询 "spring"：该词项已在更新时移除，倒排表查无此项 → 命中 0 篇——被删的词再也搜不到。', 4 );
    query('java', 'match', ['m1', 'm2']);
    emit('result', 'match 查询 "java"：词项 java 的 postings [m1, m2] 仍命中 2 篇——m2 更新后 java 保留，m3 删除不影响。', 5 );
    emit('inverted', `运行结束：文档 ${metrics.docs} · 词项 ${metrics.terms} · 定位 ${metrics.postings} · 命中 ${metrics.hits} 篇。`, 5 );
  }
  return frames;
}
function volatileJmm(p) {
  const scenario = p.scenario || 'visible';
  const sceneTag = { visible: '① 可见性 · 普通字段 vs volatile', order: '② 指令重排 · 双重检查锁', atomic: '③ 原子性 · volatile 的局限' }[scenario];
  const sceneNote = { visible: 'T1 发布 flag=true，T2 循环等待——先看普通字段如何丢更新，再加 volatile 修复。', order: '双重检查锁中 T1 首次 new Singleton()——先看重排泄漏半初始化对象，再加 volatile 修复。', atomic: 'T1/T2 对 volatile count 各自增一次——看裸 volatile 丢更新，再原子化修复。' }[scenario];
  const frames = [];
  const metrics = { reads: 0, writes: 0, lost: 0, barriers: 0 };
  const items = { kind: 'flag', volatile: false, atomized: false, value: 'false', t1seen: '—', t2seen: '—', running: false };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items });
  if (scenario === 'visible') {
    items.running = true;
    items.t2seen = 'false';
    emit('shared', `JMM 演示就绪：flag 是普通 boolean 字段（无 volatile）。场景「${sceneTag}」。${sceneNote}`, 9 );
    items.value = 'false';
    metrics.reads++;
    emit('t2', 'T2 第 1 轮读 flag：命中本地缓存副本 → false → while 循环继续。', 30 );
    metrics.reads++;
    emit('t2', 'T2 第 2 轮读：缓存副本仍 false → 继续。读的是自己核心的缓存行，与主存是否变化无关。', 30 );
    items.t1seen = 'true';
    emit('t1', 'T1 执行 flag = true：写入 T1 核心的私有缓存行——主存此刻仍是 false，T2 无从得知。', 40 );
    metrics.reads++;
    metrics.lost++;
    emit('t2', 'T2 第 3 轮读：缓存行未失效 → 仍读到 false。T1 已发布而 T2 看不到——可见性问题的现场。', 30 );
    metrics.reads++;
    metrics.lost++;
    emit('t2', 'T2 继续死循环；JIT 甚至把 while 里的读提升到循环外——flag 之后再生效也永远读旧值。', 30 );
    items.volatile = true;
    items.value = 'true';
    metrics.writes++;
    metrics.barriers++;
    emit('volatile', '修复：flag 加 volatile。volatile 写 = 立即回写主存（主存 → true）并广播失效其他核心的缓存行。', 10 );
    metrics.reads++;
    items.t2seen = 'true';
    items.running = false;
    emit('t2', 'T2 的缓存行已被广播失效 → 下一次读穿透到主存 → 读到 true → 退出循环。', 17 );
    emit('shared', `运行结束：普通字段丢 ${metrics.lost} 次可见更新；volatile 写-读建立 happens-before，一次失效立刻可见。`, 17 );
  } else if (scenario === 'order') {
    items.kind = 'instance';
    items.value = 'null';
    items.t2seen = 'null';
    emit('shared', `DCL 演示就绪：instance 为普通字段。场景「${sceneTag}」。${sceneNote}`, 69 );
    metrics.reads++;
    emit('t2', 'T2 第一次检查 instance == null → 成立（尚未创建）→ 走空分支。', 74 );
    items.t1seen = '分配';
    emit('t1', 'T1 进入同步块：obj = alloc() 分配内存——对象字段仍是默认值，构造尚未执行。', 69 );
    items.t1seen = '写引用';
    items.value = '半初始化';
    metrics.writes++;
    emit('t1', '重排发生：编译器/CPU 把「写引用」提前到构造之前 → instance = obj，半初始化对象泄漏到共享视野。', 69 );
    metrics.reads++;
    metrics.lost++;
    items.t2seen = '半初始化';
    emit('t2', 'T2 检查 instance != null → 直接使用半初始化对象 → 读到默认字段甚至 NPE——经典 DCL 缺陷。', 74 );
    items.volatile = true;
    metrics.barriers++;
    emit('volatile', '修复：instance 加 volatile。volatile 写插入 StoreStore 屏障：此前的普通写（构造）禁止重排到它之后。', 71 );
    items.t1seen = '构造';
    emit('t1', 'T1 重走 new：分配 → 构造函数执行，所有字段就绪——此时实例还未发布。', 69 );
    items.t1seen = '写引用';
    items.value = '就绪';
    metrics.writes++;
    emit('t1', 'StoreStore 屏障放行：写引用 instance 并回写主存——发布顺序固定，看到引用的线程必拿到完整对象。', 71 );
    metrics.reads++;
    items.t2seen = '就绪';
    emit('t2', 'T2 再次检查并使用 instance：引用已就绪且对象完整 → 安全。volatile 读建立 acquire 语义，其后操作不会重排到读之前。', 74 );
    emit('shared', `运行结束：普通字段重排让半初始化对象泄漏 ${metrics.lost} 次；volatile 固定「先构造后发布」，T2 之后读取必是完整对象。`, 71 );
  } else {
    items.kind = 'count';
    items.volatile = true;
    items.value = 0;
    emit('shared', `原子性演示就绪：volatile int count = 0。场景「${sceneTag}」。${sceneNote}`, 62 );
    metrics.reads++;
    items.t1seen = '读到 0';
    emit('t1', 'T1 执行 count++ 第 1 步：read count → 0。', 62 );
    metrics.reads++;
    items.t2seen = '读到 0';
    emit('t2', 'T2 同时执行第 1 步：read count → 0——T1 尚未写回。', 62 );
    metrics.writes++;
    items.value = 1;
    items.t1seen = '写入 1';
    emit('t1', 'T1 add(0+1=1) → store：volatile 写回主存 → count = 1。', 62 );
    metrics.writes++;
    metrics.lost++;
    items.t2seen = '写入 1';
    emit('t2', 'T2 基于旧值 0 计算 0+1=1 → store：count = 1——覆盖了 T1 的更新！两次 ++ 只 +1。', 62 );
    metrics.reads++;
    items.t2seen = '读到 1';
    emit('t2', 'T2 验证读：count = 1 ≠ 期望 2。volatile 保证读到的确实是最新值 1，却救不了交错丢失的那一次 ++。', 62 );
    items.atomized = true;
    items.value = 2;
    metrics.writes++;
    metrics.barriers++;
    emit('atomic', '修复：synchronized / AtomicInteger.incrementAndGet()——read-modify-write 合并为原子操作，T1/T2 依次执行 → count = 2。', 63 );
    emit('shared', `运行结束：裸 volatile 丢 ${metrics.lost} 次更新终值 1；原子化后终值 2——复合操作需要比 volatile 更强的同步。`, 63 );
  }
  return frames;
}
function nacosRegistry(p) {
  const scenario = p.scenario || 'register';
  const sceneTag = { register: '① 注册与心跳 · 实例上线', subscribe: '② 订阅与推送 · 扩缩容感知', deregister: '③ 下线与剔除 · 心跳超时' }[scenario];
  const frames = [];
  const metrics = { regs: 0, beats: 0, pushes: 0, removals: 0 };
  const items = { instances: [], sub: null, notify: null, req: null };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items });
  const up = (id, addr) => ({ id, addr, state: 'UP' });
  if (scenario === 'register') {
    emit('registry', `Nacos 集群就绪，服务 order-service 目录为空。场景「${sceneTag}」：新服务从零上线——关注注册动作与心跳租约如何建立。`, 0);
    items.instances.push(up('A', '10.0.0.11:8080'));
    metrics.regs++;
    emit('registry', '实例 A 启动 → nacos.register(service=order-service, ip:port=10.0.0.11:8080, ephemeral=true) → 服务端登记 A，状态健康。', 2);
    items.instances.push(up('B', '10.0.0.12:8080'));
    metrics.regs++;
    emit('registry', '实例 B 接着上线（10.0.0.12:8080）→ 目录 [A, B] 双副本就绪。注册只需一次 HTTP 写入，服务端立即响应。', 2);
    metrics.beats++;
    emit('beat', '5 秒后 A 的首个心跳到达：服务端刷新 lastBeat，租约续期。「心跳」就是每 5s 喊一次「我还活着」——目录据此区分活实例与僵死记录。', 4);
    metrics.beats++;
    emit('beat', 'B 的首个心跳也到达 → 双实例各自进入 5s 续约节奏：每个实例独立持有租约，谁断跳谁进入超时判定。', 4);
    metrics.beats += 2;
    emit('beat', '下一周期：A、B 双双续约成功。只要进程不退出，心跳会这样一直维持下去——注册不是一次性的，是周期性证明。', 4);
    emit('catalog', '目录快照 [A·UP, B·UP]：服务端只把健康实例交给调用方。此刻若有消费者来拉取，拿到的是这份双副本列表，请求可在 A/B 间负载均衡。', 0);
    emit('registry', `运行结束：注册 ${metrics.regs} · 心跳 ${metrics.beats} · 推送 ${metrics.pushes} · 摘除 ${metrics.removals}——写入侧完成：实例上线靠「注册 + 心跳租约」两条腿站稳目录。`, 0);
  } else if (scenario === 'subscribe') {
    items.instances.push(up('A', '10.0.0.11:8080'), up('B', '10.0.0.12:8080'));
    emit('catalog', `场景「${sceneTag}」就绪：order-service 已有 A、B 两个健康实例，心跳正常。消费者 C 即将上线，看它如何感知一次扩容。`, 0);
    items.sub = { active: true, cache: ['A', 'B'], via: '全量拉取' };
    emit('consumer', 'C 启动 → subscribe(order-service)：服务端先全量返回当前列表 [A, B] 写入 C 的本地缓存——打底完成，C 立即可发起调用。', 6);
    emit('notify', '随后 C 建立双通道：UDP 监听接收推送 + 挂起一条长轮询请求。无变更时该请求最长挂 30s，超时返回空列表并自动重挂——始终保持一条「随时可应答」的连接。', 6);
    items.instances.push(up('D', '10.0.0.13:8080'));
    metrics.regs++;
    emit('registry', '扩容：新实例 D 上线注册（10.0.0.13:8080）→ 目录 [A, B, D]。注册先落目录，此刻还没通知 C——服务端发现目录有变，准备推送。', 2);
    items.notify = 'udp';
    items.sub = { active: true, cache: ['A', 'B', 'D'], via: 'UDP 推送' };
    metrics.pushes++;
    emit('notify', '服务端 UDP 推送「目录已变更」→ C 收到通知立即拉取最新列表，缓存更新为 [A, B, D]。UDP 通道毫秒级送达，扩容对 C 近乎瞬时可见。', 6);
    items.notify = 'longpoll';
    items.sub = { active: true, cache: ['A', 'B', 'D'], via: '长轮询兜底' };
    emit('consumer', '若 UDP 报文恰好丢失：C 挂着的长轮询请求被服务端即时应答（不必等满 30s），同样带回新目录——双通道互为兜底，变更必然收敛。', 5);
    metrics.beats++;
    emit('beat', 'D 的首个心跳续约成功 → D 确认健康可用，正式进入负载均衡候选池。新实例从注册到接流量只差这 5 秒的租约确认。', 4);
    items.req = { n: 37, to: 'D' };
    emit('consumer', 'C 发起第 37 次调用：负载均衡把请求分发到新实例 D——扩容完成，全程消费者零重启、零改配置，只靠订阅机制感知。', 5);
    emit('registry', `运行结束：注册 ${metrics.regs} · 心跳 ${metrics.beats} · 推送 ${metrics.pushes} · 摘除 ${metrics.removals}——一次扩容：全量拉取打底 + 双通道通知，C 的目录从 [A, B] 收敛到含 D。`, 0);
  } else {
    items.instances.push(up('A', '10.0.0.11:8080'), up('B', '10.0.0.12:8080'));
    emit('catalog', `场景「${sceneTag}」就绪：目录含 A、B 两个临时实例（ephemeral），心跳正常。本场景演示两种离开：A 优雅下线、B 进程崩溃——同一个目录，两条完全不同的处置路径。`, 0);
    items.instances.splice(0, 1);
    metrics.removals++;
    emit('registry', 'A 调用 deregister（如 Spring 容器关闭钩子 @PreDestroy）→ 服务端把 A 从目录立即摘除——优雅下线是主动通知，无需等任何超时，调用方马上少一个可用地址。', 7);
    emit('beat', 'B 进程被 kill -9：心跳线程随进程消失，也来不及调用 deregister——此刻注册中心还不知道 B 已死，目录里 B 仍是健康状态。', 7);
    emit('beat', '崩溃第 5s：B 的心跳依旧缺席，但服务端不立即判定——GC 停顿、网络抖动都可能造成短暂断跳，误杀健康实例的代价更高，健康窗口设为 15s。', 4);
    items.instances[0].state = 'DOWN';
    emit('catalog', '第 15s：B 仍无心跳 → 标记不健康（DOWN）：从负载均衡候选池摘除、不再分配新请求，但目录中保留可见——若 B 只是长 GC 后恢复，心跳重连还能转回健康。', 0);
    items.instances.splice(0, 1);
    metrics.removals++;
    emit('registry', '第 30s：B 依旧失联 → 超过剔除时限，临时实例被自动摘除，目录清空。崩溃实例无需人工清理：靠「心跳缺失 → 15s 判不健康 → 30s 自动剔除」三级机制收尾。', 4);
    emit('catalog', '对照：若 B 是持久实例（ephemeral=false，如数据库、缓存等常驻地址）——心跳停止也不会被自动剔除！持久实例的生命周期由运维显式管理，只能通过 deregister / 删除 API 下线。', 0);
    emit('registry', `运行结束：注册 ${metrics.regs} · 心跳 ${metrics.beats} · 推送 ${metrics.pushes} · 摘除 ${metrics.removals}——优雅下线即时生效；崩溃下线交给「15s 判定 + 30s 剔除」兜底；持久实例则永不自愈移除。`, 0);
  }
  return frames;
}
function nettyEventLoop(p) {
  const scenario = p.scenario || 'accept';
  const sceneTag = { accept: '① 主从 Reactor · boss 接入与分发', io: '② 串行无锁 · 一轮 select 多连接', slow: '③ 阻塞传染 · 业务线程池卸载' }[scenario];
  const frames = [];
  const metrics = { conns: 0, events: 0, regs: 0, tasks: 0 };
  const items = { conns: [], cur: null, ready: [], task: null, pool: 0 };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items });
  const conn = (id, w) => ({ id, w, state: 'idle' });
  if (scenario === 'accept') {
    emit('boss', `主从 Reactor 就绪：boss = 1 个线程（只监听 OP_ACCEPT），workerGroup = W1 / W2 两个线程（各持 1 个 Selector）。场景「${sceneTag}」：看一条新连接如何从握手到归属某个 worker。`, 122 );
    emit('client', '客户端 A 发起 connect：三次握手由内核完成，连接进入就绪队列——boss 线程的 select 即将被唤醒。boss 不参与握手，只在队列边「接客」。', 122 );
    items.conns.push(conn('conn-1', '—'));
    metrics.conns++;
    emit('boss', 'boss 的 select 命中 OP_ACCEPT → accept() 取出 conn-1。boss 的工作到此为止：不读、不写、不做业务——连接接入后立即移交下一棒。', 122 );
    items.conns[0].w = 'W1';
    metrics.regs++;
    emit('boss', 'boss 按轮询把 conn-1 注册给 W1：此后 conn-1 的 OP_READ / OP_WRITE 由 W1 的 Selector 监听，读写事件只在 W1 线程上发生——连接与线程从此绑定。', 128 );
    items.conns[0].state = 'busy';
    items.cur = 'conn-1 读';
    metrics.events++;
    emit('worker', 'conn-1 的首个请求到达 → W1 的 select 命中 OP_READ：在 W1 线程上执行整条 pipeline（解码 → 业务 → 编码回写），全程无锁——此刻没有任何其他线程碰得到 conn-1。', 166 );
    items.conns[0].state = 'idle';
    items.cur = null;
    emit('client', 'conn-1 处理完成、响应已回写，W1 重新空闲。与此同时客户端 C 的连接完成握手进入就绪队列——boss 的下一次 select 又要命中。', 122 );
    items.conns.push(conn('conn-2', '—'));
    metrics.conns++;
    emit('boss', 'boss 再次 accept → conn-2 接入。注意：刚才 W1 正忙着处理 conn-1，boss 却毫无感觉——接入与 IO 在不同线程，谁也不会拖累谁。', 122 );
    items.conns[1].w = 'W2';
    metrics.regs++;
    emit('boss', '轮询指针移到 W2：conn-2 被注册给 W2。连接轮流分发避免单 worker 过热；当连接数超过 worker 数，多连接共享同一 worker——线程共享，事件仍串行。', 128 );
    items.conns[1].state = 'busy';
    items.cur = 'conn-2 读';
    metrics.events++;
    emit('worker', 'conn-2 的首个请求 → W2 线程处理。此刻 W1 与 W2 各自处理自己名下的连接，互不干扰——不同 worker 上的连接天然并行。', 166 );
    items.conns[1].state = 'idle';
    items.cur = null;
    emit('boss', `运行结束：接入 ${metrics.conns} · 事件 ${metrics.events} · 绑定 ${metrics.regs} · 任务 ${metrics.tasks}——boss 只做接入与分发：连接归谁，事件就永远在谁的线程上发生。`, 122 );
  } else if (scenario === 'io') {
    items.conns.push(conn('conn-1', 'W1'), conn('conn-2', 'W1'), conn('conn-3', 'W1'));
    emit('worker', `场景「${sceneTag}」就绪：conn-1 / conn-2 / conn-3 三个连接都已接入并绑定 W1——一个线程服务三条连接。事件循环 = select 等待 → 处理就绪 IO → 执行任务队列 → 再 select，如此往复。`, 60 );
    items.ready.push('conn-1', 'conn-2', 'conn-3');
    emit('worker', '三个连接同时有读事件 → W1 的 select 一次返回 3 个就绪 key（多路复用：一个线程盯住 N 条连接）。就绪不代表并行——它们将在 W1 上排队、逐个处理。', 60 );
    items.ready.shift();
    items.conns[0].state = 'busy';
    items.cur = 'conn-1 读';
    metrics.events++;
    emit('worker', '先处理 conn-1：读取、解码、业务、编码一气呵成。处理期间 conn-2 / conn-3 只是排队等待，不会插入执行。', 166 );
    items.conns[0].state = 'idle';
    items.ready.shift();
    items.conns[1].state = 'busy';
    items.cur = 'conn-2 读';
    metrics.events++;
    emit('worker', 'conn-1 处理完，同一线程直接切到 conn-2——切换无需任何锁：单线程顺序执行，共享状态天然只有一个人在碰（thread confinement）。', 166 );
    items.conns[1].state = 'idle';
    items.ready.shift();
    items.conns[2].state = 'busy';
    items.cur = 'conn-3 读';
    metrics.events++;
    emit('worker', 'conn-2 完成 → conn-3 接上。一次 select 循环里三个连接全部处理完毕——高频小请求场景下，单 worker 也能扛住大量连接。', 166 );
    items.conns[2].state = 'idle';
    items.cur = null;
    items.task = '心跳写 → conn-1';
    metrics.tasks++;
    emit('worker', 'conn-3 处理完成，事件循环进入本轮收尾：执行任务队列。队列里躺着一个刚投递的任务——conn-1 的 keepalive 定时器经 eventLoop().execute() 提交的「心跳写」。任务与 IO 事件在同一线程交替执行，顺序确定。', 70 );
    items.task = null;
    emit('worker', 'W1 取出心跳任务执行：向 conn-1 写一个 keepalive 帧。注意来源——定时器线程（图外）想操作 conn-1，唯一安全途径就是 execute 把代码「搬」到 W1 线程来跑：无锁、无并发。', 70 );
    items.conns[1].state = 'busy';
    items.cur = 'conn-2 读';
    metrics.events++;
    emit('worker', '任务队列清空，事件循环回到 select → 恰好 conn-2 的新请求已就绪：命中 OP_READ，继续处理。IO 事件与用户任务按序交替，互不打断。', 166 );
    items.conns[1].state = 'idle';
    items.cur = null;
    emit('worker', `运行结束：接入 ${metrics.conns} · 事件 ${metrics.events} · 绑定 ${metrics.regs} · 任务 ${metrics.tasks}——一个线程串起 N 条连接与任意线程投递的任务：所有执行都落在同一条时间线上，无锁因此成为可能。`, 60 );
  } else {
    items.conns.push(conn('conn-1', 'W1'), conn('conn-2', 'W1'));
    emit('worker', `场景「${sceneTag}」就绪：conn-1 与 conn-2 都绑定 W1。conn-1 的业务 handler 里有一处 100ms 的阻塞调用（慢 SQL / 第三方 HTTP 示意），conn-2 是高频小请求——同一个线程上，一场灾难即将发生。`, 87 );
    items.conns[0].state = 'blocked';
    items.cur = 'conn-1 读';
    metrics.events++;
    emit('worker', 'conn-1 读事件 → W1 开始处理：解码、业务……handler 走到那行阻塞调用——W1 原地卡死 100ms。IO 线程上的阻塞调用，代价由整条事件循环承担。', 183 );
    items.ready.push('conn-2');
    emit('worker', '此刻 conn-2 的读事件也就绪，select 明明命中——可 W1 还困在 conn-1 的阻塞调用里，无人处理。就绪事件只能排队：conn-2 的请求要白白多等一个阻塞周期。', 171 );
    items.conns[0].state = 'idle';
    items.cur = null;
    items.ready.shift();
    items.conns[1].state = 'busy';
    items.cur = 'conn-2 读';
    metrics.events++;
    emit('worker', '100ms 后阻塞调用返回：conn-1 处理完成，W1 终于脱身 → 立刻处理早就就绪的 conn-2。它被拖慢了整整一个阻塞周期；若 conn-1 每秒来一次慢请求，这个延迟会无限循环。', 171 );
    items.conns[1].state = 'idle';
    items.cur = null;
    emit('worker', 'conn-2 处理完成。一次拖累也许能忍，但慢请求只要留在 IO 线程，每次都会让同 worker 的连接一起陪等——于是改造开始：把 conn-1 的阻塞调用挪出事件循环。', 87 );
    items.pool = 1;
    metrics.tasks++;
    emit('worker', '修复：conn-1 的 handler 不再直接阻塞——阻塞任务提交给独立业务线程池（pool.submit），W1 提交完立即返回 select，一秒都不多等。', 169 );
    items.pool = 2;
    metrics.tasks++;
    emit('worker', 'conn-1 新请求到达 → W1 再次把慢任务丢给业务池（此时池中两个任务在途），自己瞬间回到 select——IO 线程从此只做「接活、派活」，永远不被占住。', 169 );
    items.conns[1].state = 'busy';
    items.cur = 'conn-2 读';
    metrics.events++;
    emit('worker', '对比帧：conn-2 的请求到达，W1 立刻处理——零排队、零延迟。同一个 W1，修复前 conn-2 要陪 conn-1 干等 100ms，修复后随到随办：卸载的收益肉眼可见。', 183 );
    items.conns[1].state = 'idle';
    items.cur = null;
    items.pool = 1;
    emit('worker', 'conn-2 处理完成。业务池里第一个慢任务也跑完了：结果经回调回投——写 conn-1 的操作被 execute 交回 W1 线程串行执行（回调里不能跨线程直接写 channel）。', 87 );
    emit('worker', `运行结束：接入 ${metrics.conns} · 事件 ${metrics.events} · 绑定 ${metrics.regs} · 任务 ${metrics.tasks}——阻塞任务全部卸载到业务池（第二个任务仍在池中后台执行），IO 线程只做快进快出的派活：谁在事件循环里睡觉，谁就拖垮一船人。`, 87 );
  }
  return frames;
}
function nacosConfig(p) {
  const scenario = p.scenario || 'publish';
  const sceneTag = { publish: '① 启动拉取 · 全量打底与本地缓存', update: '② 动态刷新 · 发布即生效', namespace: '③ 环境隔离 · 误发布与回滚' }[scenario];
  const frames = [];
  const metrics = { pulls: 0, pushes: 0, refreshes: 0, rollbacks: 0 };
  const items = { configs: [], clients: [], notify: null, rollback: null };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items });
  const start = (id, ns) => { const c = { id, ns, cache: null, md5: null, state: 'starting' }; items.clients.push(c); return c; };
  const pull = (c, cfg) => { metrics.pulls++; c.cache = cfg.ver; c.md5 = cfg.md5; c.state = 'ready'; };
  const refresh = (c, cfg) => { metrics.pulls++; metrics.refreshes++; c.cache = cfg.ver; c.md5 = cfg.md5; c.state = 'ready'; };
  if (scenario === 'publish') {
    items.configs.push({ ns: 'dev', ver: 3, md5: '9f2c', ok: true });
    emit('server', `场景「${sceneTag}」就绪：服务端持有 order-service.yaml（dev 命名空间 · v3 · 指纹 9f2c），客户端 A / B 尚未启动。本场景看应用启动那一刻与配置中心之间发生什么。`, 0);
    const a = start('A', 'dev');
    emit('app', 'A 启动（第一次上线）：本地缓存为空 → 向服务端发起全量拉取 GET dev:order-service.yaml——「启动拉取一次打底」，之后运行期不再全量轮询。', 2);
    pull(a, items.configs[0]);
    emit('client', '服务端返回 v3 内容 + MD5 指纹 9f2c → A 写入本地缓存。此后「配置变没变」不再靠反复全量比对，而是靠指纹：指纹相同 = 没变。', 2);
    emit('app', 'A 把 db.url 等配置注入数据源与线程池 bean，应用就绪。变更感知的职责已完全交给监听通道，业务代码不再关心配置从哪来、何时变。', 0);
    const b = start('B', 'dev');
    pull(b, items.configs[0]);
    emit('client', 'B 启动 → 同样全量拉取 → 拿到同一份 v3（指纹 9f2c）。配置是共享资产：任意副本启动都向中心打底一次，中心只存一份、各自落各自的本地缓存。', 2);
    metrics.pulls++;
    emit('server', '对账时刻：A 带本地指纹 9f2c 发起校验 GET → 服务端比对：内容未变，返回「未变更」→ 缓存与已注入的 bean 原样保留——指纹相同不刷新，避免无谓重建。', 4);
    emit('config', `运行结束：拉取 ${metrics.pulls} · 推送 ${metrics.pushes} · 刷新 ${metrics.refreshes} · 回滚 ${metrics.rollbacks}——启动拉取 + 指纹校验就是全部动静：配置没变时，客户端与中心之间安静得只剩挂着的那条长轮询。`, 0);
  } else if (scenario === 'update') {
    items.configs.push({ ns: 'dev', ver: 3, md5: '9f2c', ok: true });
    const a = start('A', 'dev'); a.state = 'ready'; a.cache = 3; a.md5 = '9f2c';
    const b = start('B', 'dev'); b.state = 'ready'; b.cache = 3; b.md5 = '9f2c';
    emit('server', `场景「${sceneTag}」就绪：A、B 都在线（本地缓存 v3 · 指纹 9f2c），各自挂起一条 30s 长轮询 + 一条 UDP 监听。开发即将发布新配置——看它如何不重启就流进每个客户端。`, 0);
    items.configs[0] = { ns: 'dev', ver: 4, md5: 'a1b2', ok: true };
    metrics.pushes++;
    emit('config', '开发把 db.url 改为 v4 地址并发布 → 服务端落库：版本 v3 → v4，指纹 9f2c → a1b2。发布事件即刻唤醒所有通知通道；v3 进历史版本归档，随时可回滚。', 2);
    items.notify = 'udp';
    refresh(a, items.configs[0]);
    emit('listener', 'A 的 UDP 通道毫秒级带回「v4 已发布」→ A 重拉 → 指纹 a1b2 ≠ 9f2c → 刷新数据源与线程池 bean。发布即生效：配置中心的价值就在这「不用重启」的秒级收敛。', 6);
    items.notify = 'poll';
    refresh(b, items.configs[0]);
    emit('client', 'B 的 UDP 恰好丢包——但 B 挂着的那条长轮询被服务端即时应答「有变更」（不必等满 30s）→ B 重拉 v4 → 同样刷新。双通道互为兜底：UDP 快但会丢，轮询稳但稍慢，变更必然收敛。', 5);
    items.notify = null;
    items.configs[0] = { ns: 'dev', ver: 5, md5: 'c3d4', ok: true };
    metrics.pushes++;
    emit('config', '第二次发布：连接池上限改为 200（v5）。版本再 +1，指纹 a1b2 → c3d4，历史继续累积。', 2);
    items.notify = 'udp';
    refresh(a, items.configs[0]);
    emit('listener', 'A 再次经 UDP 秒级感知 → 重拉 → 刷新到 v5。常态下 UDP 就是主通道：连续发布，连续毫秒级生效，全程无感知。', 6);
    items.notify = null;
    refresh(b, items.configs[0]);
    emit('client', 'B 同样刷新到 v5——上一次的轮询兜底只是异常路径，常态下双客户端都在秒级收敛。此刻 A / B 本地缓存完全一致：v5 · c3d4。', 6);
    emit('server', `运行结束：拉取 ${metrics.pulls} · 推送 ${metrics.pushes} · 刷新 ${metrics.refreshes} · 回滚 ${metrics.rollbacks}——两次发布四次刷新：变更 1 用 UDP + 长轮询证明变更必达，变更 2 展示常态下的秒级收敛：配置改完，全集群无感跟上。`, 0);
  } else {
    items.configs.push({ ns: 'dev', ver: 4, md5: 'd4e5', ok: true }, { ns: 'prod', ver: 3, md5: 'p3q4', ok: true });
    emit('server', `场景「${sceneTag}」就绪：同一份代码部署到两个环境——dev 与 prod 命名空间各存一份同名 dataId（order-service.yaml）：dev v4 → dev 库、prod v3 → 生产库。客户端 D 归 dev、P 归 prod。`, 0);
    const d = start('D', 'dev');
    pull(d, items.configs[0]);
    emit('client', 'D 启动 → GET dev:order-service.yaml → 拿到 v4（db.url=dev-db）+ 指纹 d4e5，落本地缓存。同一份代码进 dev 命名空间，拿的是 dev 的配置。', 2);
    const p = start('P', 'prod');
    pull(p, items.configs[1]);
    emit('client', 'P 启动 → GET prod:order-service.yaml → 拿到 v3（db.url=prod-db）+ 指纹 p3q4。dataId 同名、值不同——namespace 是寻址的第三维，把两个环境彻底隔开。', 2);
    items.configs[0] = { ns: 'dev', ver: 5, md5: 'e6f7', ok: false };
    metrics.pushes++;
    emit('config', 'D 的开发误把 dev 配置改成非法连接串并发布 → dev 命名空间进入 v5（错误配置）。注意 prod 的 v3 纹丝不动——发布只落在 dev 这一份 dataId 上。', 7);
    items.notify = 'udp';
    refresh(d, items.configs[0]);
    d.state = 'error';
    emit('client', 'D 的 UDP 带回 v5 → 指纹变化 → 刷新数据源 → 应用立刻报错：连接串非法。动态刷新是双刃剑：坏配置同样秒级生效——这就是灰度与回滚要兜住的风险。', 6);
    items.notify = null;
    items.configs[0] = { ns: 'dev', ver: 4, md5: 'd4e5', ok: true };
    metrics.pushes++;
    metrics.rollbacks++;
    items.rollback = '← 撤销 v5';
    emit('rollback', '运维在控制台一键回滚到 v4 → 服务端恢复上一版本（v5 从当前版本撤下）。回滚 = 再发布一次 v4：历史版本是底气，推送通道是腿。', 2);
    items.rollback = null;
    items.notify = 'udp';
    refresh(d, items.configs[0]);
    d.state = 'ready';
    emit('client', 'D 再次被通知 → 重拉 → 指纹回到 d4e5 → 数据源刷新恢复，应用自愈。发布、出错、回滚、恢复全程零重启——业务代码一行没改。', 6);
    emit('config', `运行结束：拉取 ${metrics.pulls} · 推送 ${metrics.pushes} · 刷新 ${metrics.refreshes} · 回滚 ${metrics.rollbacks}——误发布与回滚全程发生在 dev 命名空间：prod 的 P 一次推送、一次刷新都没有。隔离让生产环境毫发无伤。`, 0);
  }
  return frames;
}
function mysqlReplication(p) {
  const scenario = p.scenario || 'chain';
  const frames = [];
  const metrics = { writes: 0, copies: 0, acks: 0, failovers: 0 };
  let seq = 0;
  const items = { mode: scenario === 'semisync' ? 'semisync' : 'async', master: 'M', events: [], lag: 0, ack: null, route: null };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items });
  const add = (t, s = 'm') => { const e = { id: `e${++seq}`, t, s }; items.events.push(e); return e; };
  if (scenario === 'chain') {
    emit('master', '复制拓扑就绪：应用写主库 M（binlog_format=ROW）；从库 S 已 START SLAVE——IO 线程从 M 拉 binlog 落本地 relay log，SQL 线程再串行回放。真正的复制从第一条事务开始。', 47 );
    const e1 = add('T1 下单');
    metrics.writes++;
    emit('writer', '应用执行 INSERT 订单（id=1001）→ M 本地落库、提交成功并返回。注意：此刻从库 S 什么都不知道——异步复制下，主库提交从不等待复制。', 50 );
    e1.s = 'r';
    emit('binlog', 'M 的 dump 线程把 e1 推给 S 的 IO 线程 → 落进 S 的 relay log。传输毫秒级完成，但 SQL 线程还没动——relay log 是「已收到、未回放」的中转站。', 54 );
    e1.s = 'a';
    metrics.copies++;
    emit('relay', 'S 的 SQL 线程串行回放 e1：订单行在 S 落地。单线程回放保证提交顺序与主库一致——这也是延迟的根源之一。', 56 );
    const e2 = add('T2 改价');
    metrics.writes++;
    emit('writer', '应用又提交 T2：10 万行 UPDATE 批量改价，M 毫秒级完成。binlog 以 ROW 格式记下整整 10 万条变更事件，等着被搬走。', 59 );
    e2.s = 'r';
    items.lag = 2;
    emit('relay', 'e2 到达 S 的 relay log——但 10 万条 ROW 变更让 SQL 线程回放得冒烟。此刻 SHOW SLAVE STATUS：Seconds_Behind_Master = 2 并持续攀升。主库正常、复制没断，只是追不上。', 61 );
    e2.s = 'a';
    metrics.copies++;
    items.lag = 0;
    emit('slave', '回放完成，从库追平。异步复制的真相：M 与 S 是最终一致——延迟是常态，关键是它永远不拖累主库吞吐。', 63 );
    emit('master', `运行结束：写入 ${metrics.writes} · 回放 ${metrics.copies} · 半同步确认 ${metrics.acks} · 切换 ${metrics.failovers}——异步链路全通：binlog 是源头、dump 线程搬、IO 线程落 relay、SQL 线程回放。提交即返回、滞后必追平；延迟不在主库显形，只在读从库时露出旧数据。`, 64 );
  } else if (scenario === 'semisync') {
    emit('master', '半同步复制开启：M 提交事务时，binlog 落盘后不立刻对外确认——必须等至少一个从库回 ack（确认收到 binlog）才返回成功。代价是每次提交多一次往返，换来「主库说成功 = 至少一份副本真的有」。', 69 );
    const e1 = add('T1 支付');
    metrics.writes++;
    emit('writer', '应用提交 T1 支付事务 → binlog 已记 e1 → 但提交被挂起：M 在等 S 的 ack。半同步的「慢」就慢在这：成功返回前，先问一句「你收到了吗」。', 72 );
    e1.s = 'r';
    metrics.acks++;
    items.ack = 'e1 ✓ S';
    emit('binlog', 'S 的 IO 线程把 e1 写进 relay log → 回 ack → M 收到确认，T1 这才提交成功返回应用。binlog 有了、relay 有了——即使 M 立刻宕机，S 也能把这笔事务补出来。', 76 );
    e1.s = 'a';
    metrics.copies++;
    items.ack = null;
    emit('relay', 'SQL 线程把 e1 回放到 S 的数据文件。半同步管「收到」不管「回放完」——但收到就够：数据已离开 M，丢了也能从 S 找回。', 77 );
    const e2 = add('T2 库存');
    metrics.writes++;
    items.mode = 'degraded';
    emit('master', 'S 宕机（网络分区）。此刻 M 提交 T2：半同步等 ack…… 超时！自我保护触发：降级为异步继续提交——宁可暂时牺牲一致，也不让主库写不进去。e2 只有 M 自己知道。', 78 );
    e2.s = 'x';
    emit('binlog', '屋漏偏逢连夜雨：M 也宕机了。盘点损失：e1 在 S 有 relay 副本（安全）；e2 提交于降级窗口——M 以为成功、没有任何从库收到，随 M 一起消失。若全程异步，S 失联期间 M 提交的每一笔都可能丢：半同步把损失窗口压到只剩降级期。', 81 );
    items.master = 'S↑';
    items.mode = 'async';
    metrics.failovers++;
    emit('slave', 'DBA 执行切换：S 停止复制、提升为新主——数据恢复到 e1 为止，丢 e2 这一笔。从库秒变主库，应用改一下连接串即恢复业务（生产上这一步由 MHA/Orchestrator 类工具自动完成）。', 82 );
    const e3 = add('T3 订单');
    metrics.writes++;
    emit('writer', '业务重连新主 S↑：T3 提交成功，S↑ 开始积累自己的 binlog。教训沉淀：半同步保证「至少一个从库确认收到」，把数据丢失从异步的随时可能，压缩到降级窗口内的极少几笔。', 83 );
    emit('master', `运行结束：写入 ${metrics.writes} · 回放 ${metrics.copies} · 半同步确认 ${metrics.acks} · 切换 ${metrics.failovers}——正常窗口零丢失（e1 双保险）、降级窗口丢一笔（e2）、切换后无缝续写（e3）。半同步的价值不是零丢失，而是把丢失窗口缩到可接受的极小。`, 85 );
  } else {
    emit('master', '读写分离拓扑就绪（异步复制）：Proxy 把写请求全部发往 M、读请求默认发往 S——主库专注写、从库扛读。前提：S 的复制延迟越小，读越新鲜。', 90 );
    const e1 = add('T1 下单');
    metrics.writes++;
    emit('writer', '用户下单 → 写路由到 M：订单行落库、T1 提交成功，返回「下单成功」。此刻 S 还不知道 T1 存在。', 92 );
    items.route = 'slave';
    emit('reader', '1 秒后用户刷新「我的订单」→ Proxy 把读路由到 S → S 还没有 T1 → 列表空空如也。用户视角：下单成功但订单没了。这不是 bug——是复制延迟与路由策略的合谋，写后即读正是读写分离最疼的场景。', 96 );
    e1.s = 'a';
    metrics.copies++;
    emit('relay', 'SQL 线程回放完成，S 追平 → 同一查询再次路由到 S：订单出现了。窗口只有几百毫秒——列表读可以忍，但「用户刚写的数据读不到」体验是硬伤。', 97 );
    const e2 = add('T2 改状态');
    metrics.writes++;
    items.route = 'master';
    emit('reader', '支付回调到达：要先读订单状态再决定是否发货。这类读决定写的关键读绝不走从库——强制路由主库：T2 更新与随后的读取同库同序，永不自相矛盾，刚提交的数据立即可见。', 99 );
    e2.s = 'a';
    metrics.copies++;
    items.route = 'slave';
    emit('slave', 'T2 回放完成。看分流效果：报表、列表、详情这些读放大流量全在 S，M 只扛写 + 零星关键读——读流量再涨，挂只读从库横向扩展即可，写库稳如泰山。', 101 );
    emit('master', `运行结束：写入 ${metrics.writes} · 回放 ${metrics.copies} · 半同步确认 ${metrics.acks} · 切换 ${metrics.failovers}——读写分离黄金准则：默认读从库卸压；写后即读、支付回调等关键读强制走主库；延迟窗口交给半同步压缩。路由定对，一致性才有得谈。`, 102 );
  }
  return frames;
}
function esSharding(p) {
  const scenario = p.scenario || 'route';
  const frames = [];
  const metrics = { docs: 0, syncs: 0, promotes: 0, moves: 0 };
  const items = { nodes: ['up', 'up', 'up'], primaries: ['P0@N1', 'P1@N2', 'P2@N3'], docs: [], promotes: [], moving: null };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items });
  const add = (id, shard) => { const d = { id, shard, s: 'm' }; items.docs.push(d); return d; };
  if (scenario === 'route') {
    emit('app', '集群就绪（green）：orders 索引 = 3 主分片 + 每主 1 副本。拓扑 N1: P0+R1 · N2: P1+R2 · N3: P2+R0。文档路由铁律：hash(routing) % 主分片数——同一 _id 永远落同一主分片。', 25 );
    const d1 = add('#1001', 'P2');
    metrics.docs++;
    emit('app', '写入 #1001 → 协调节点算路由：hash(#1001) % 3 = 2 → 落到 P2（N3）。写入只打一个分片，不是广播。', 25 );
    d1.s = 's';
    metrics.syncs++;
    emit('primary', '副本同步完成：P2 的数据复制到 R2（N1）——主副双写，任一分片宕机都有另一份顶着。', 25 );
    const d2 = add('#1002', 'P0');
    metrics.docs++;
    emit('app', '写入 #1002 → hash % 3 = 0 → 落到 P0（N1）。', 25 );
    d2.s = 's';
    metrics.syncs++;
    emit('primary', '副本同步完成：R0（N2）与主分片一致。', 25 );
    const d3 = add('#1003', 'P1');
    metrics.docs++;
    emit('app', '写入 #1003 → hash % 3 = 1 → 落到 P1（N2）。三篇文档散落三个主分片——写入压力天然均摊。', 5);
    d3.s = 's';
    metrics.syncs++;
    emit('primary', '副本同步完成：R1（N3）一致。此刻每个主分片 1 篇文档、副本同步完成，集群依旧 green。', 6);
    emit('coord', `运行结束：写入 ${metrics.docs} · 同步 ${metrics.syncs} · 提升 ${metrics.promotes} · 迁移 ${metrics.moves}——查询「全部订单」时协调节点向 3 个主分片广播、各自返回局部命中再合并排序；而按 _id 点查只打一个分片。hash % N 既均摊了写入，又让单文档读写永远只落一个分片。`, 25 );
  } else if (scenario === 'failover') {
    emit('app', '集群就绪（green）：N1: P0+R1 · N2: P1+R2 · N3: P2+R0——副本永远放在别的节点上，这才是容灾的意义。', 0);
    items.nodes[1] = 'down';
    emit('cluster', '心跳超时：N2 失联！P1 主分片随之下线、R2 副本也丢——集群变 red：写路由到 P1 的请求开始失败。这一刻起，P1 的数据靠谁？', 1 );
    items.promotes.push('R1 → P1');
    items.primaries[1] = 'P1↑@N1';
    metrics.promotes++;
    emit('primary', '副本接管：N1 上 P1 的副本 R1（数据与 P1 完全同步）自动提升为新主 P1↑。丢失窗口只有几十秒，数据零丢失——副本不是冷备份，是随时能接管的活副本。', 2 );
    metrics.syncs++;
    emit('cluster', '新主落定后集群自动补副本：在 N3 上重建 R1′ 并同步完成——颜色从 red 经 yellow（缺副本）回到 green。整个过程无需人工干预。', 3 );
    const d1 = add('#1001', 'P1↑');
    metrics.docs++;
    emit('app', '业务无感恢复：新订单 #1001 写入新主 P1↑（N1）成功——客户端甚至没察觉到刚才发生过主分片切换。', 4 );
    items.nodes[1] = 'up';
    metrics.syncs++;
    emit('cluster', 'N2 恢复上线：但它落后于集群——先以普通节点身份加入、从副本拉齐期间错过的数据（含 #1001），追平后重新参与分片分布。', 5 );
    emit('cluster', `运行结束：写入 ${metrics.docs} · 同步 ${metrics.syncs} · 提升 ${metrics.promotes} · 迁移 ${metrics.moves}——从 P1 失联到新主接管只隔几十秒：副本自动提升挡住故障，补副本让集群回到 green，回归节点追平数据重新入列。副本 + 自动提升 = 分片级高可用。`, 0);
  } else {
    emit('app', '流量翻倍，单分片压力吃紧，需要扩容。本集群 3 个 master 节点，过半原则：minimum_master_nodes = 2——只有凑齐 ≥2 票的节点组才有资格选主。', 0);
    emit('cluster', '直觉操作 PUT orders/_settings 把主分片数从 3 调成 5 → 400 拒绝！主分片数被路由哈希锁死：改了取模分母，所有旧文档的落位全变，等于把数据丢进错误的分片。', 1 );
    items.moving = 'orders → orders-v2';
    metrics.moves++;
    emit('primary', '扩容正道：新建 orders-v2（5 主分片）→ POST /_reindex 后台搬数据——scroll 旧索引、bulk 写新索引，一批批推进。新索引按峰值流量定好分片数，这是唯一的机会窗口。', 2 );
    items.moving = null;
    metrics.moves++;
    items.primaries = ['orders-v2 · 5 主分片'];
    emit('cluster', 'reindex 完成：别名 orders 原子切换到 orders-v2，业务查询无缝改道；旧索引下线删除。扩容全程只有迁移期短暂只读，不停机。', 3 );
    items.nodes[1] = 'split';
    emit('cluster', '网络分区演习：N2 与 N1、N3 断开。N2 只剩自己 1 票 < 2 → 凑不齐 quorum，即使它手上还有主分片副本，也只能降级为只读、绝不自封为主——若没有过半规则，两边各选一个主就是脑裂双主，数据一分为二。', 4 );
    items.nodes[1] = 'up';
    emit('cluster', '分区恢复：N2 重连集群——补齐落后数据后重新入列，3 节点 quorum 复原。孤岛期间它顶多短暂不可写，但没有造成任何数据分裂。', 5 );
    emit('cluster', `运行结束：写入 ${metrics.docs} · 同步 ${metrics.syncs} · 提升 ${metrics.promotes} · 迁移 ${metrics.moves}——纵向调分片数是死路（400 锁死），横向扩容走「新索引 + reindex + 别名」；过半 quorum 让分裂的少数派自动认怂，把脑裂掐死在投票环节。`, 0);
  }
  return frames;
}
function rocketmqOrdered(p) {
  const scenario = p.scenario || 'ordered';
  const frames = [];
  const metrics = { sent: 0, consumed: 0, retries: 0, queued: 0 };
  const items = { q0: [], q1: [], done: [], retrying: null, delayed: [] };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items });
  const syncQueued = () => { metrics.queued = items.q0.length + items.q1.length + items.delayed.length; };
  const enqueue = (queue, label) => { queue.push(label); metrics.sent++; syncQueued(); };
  const consume = (queue, label) => { queue.shift(); items.done.push(label); metrics.consumed++; syncQueued(); };
  if (scenario === 'ordered') {
    emit('q0', '分区有序拓扑就绪：O1（创建→支付）与参照订单 O2 将按业务 key 选队列。顺序的粒度是队列——同一订单恒同队列、队列内单线程串行，跨订单并行互不干扰。', 0);
    enqueue(items.q0, 'O1 创建');
    emit('selector', '发送 O1-创建 → MessageQueueSelector 计算 hash(O1) % 2 = 0 → 消息落入 q0。队列选择器不是随机分发：同一 orderId 每次算出同一个队列。', 1);
    enqueue(items.q0, 'O1 支付');
    emit('selector', '发送 O1-支付 → hash(O1) 恒为 0 → 仍然 q0，排在创建之后。创建与支付在 q0 内排队，先后次序已由入队顺序锁死。', 2);
    enqueue(items.q1, 'O2 创建');
    emit('selector', '发送 O2-创建 → hash(O2) % 2 = 1 → 落入 q1。两个订单落在不同队列——分区有序允许它们各自并行推进。', 3);
    consume(items.q0, 'O1 创建');
    emit('consumer', 'q0 消费者（MessageListenerOrderly，队列独占单线程）取出第一条：O1-创建，业务执行成功。', 4);
    consume(items.q0, 'O1 支付');
    emit('consumer', '同一线程处理下一条：O1-支付。先创建后支付——顺序消费保证 O1 的状态机严格按业务步骤推进。', 5);
    consume(items.q1, 'O2 创建');
    emit('consumer', 'q1 的消费线程并行处理 O2-创建：两个订单两条队列各走各的，吞吐不被「全局一把锁」压死。', 6);
    emit('q0', '运行结束：发送 3 · 消费 3 · 重试 0 · 积压 0——selector 的 hash 是「同订单同队列」的锚，队列内单线程串行是顺序的执行器；代价是单队列吞吐有限，换来的是粒度适中的分区有序：要全局严格有序，就得让所有消息挤进同一队列——大部分业务不需要那个极端。', 0);
  } else if (scenario === 'resend') {
    emit('q0', '顺序消费模型就绪：O1 生命周期四步（创建→支付→出库→完成）将依次进入 q0。消费端 MessageListenerOrderly：失败必须挂起当前队列原地重试——顺序语义下跳过失败消息，等于让后续步骤建立在缺失的前提上。', 0);
    enqueue(items.q0, 'O1 创建');
    emit('selector', '发送 O1-创建 → hash(O1) % 2 = 0 → q0。', 1);
    consume(items.q0, 'O1 创建');
    emit('consumer', '消费 O1-创建：订单落库成功，第一步完成。', 2);
    enqueue(items.q0, 'O1 支付');
    consume(items.q0, 'O1 支付');
    emit('consumer', '发送并消费 O1-支付：扣款成功。前两步顺利，真正的考验在出库——库存是顺序消费里最容易失败的环节。', 3);
    enqueue(items.q0, 'O1 出库');
    emit('selector', '发送 O1-出库 → q0。出库依赖实时库存：存在失败可能，看消费端如何应对。', 4);
    items.retrying = items.q0.shift();
    metrics.retries++;
    syncQueued();
    emit('consumer', '消费 O1-出库失败：库存不足！返回 SUSPEND_CURRENT_QUEUE_A_MOMENT——当前队列被挂起，失败消息回到队头，1 秒后原地重投递。注意：不是跳过、不是丢弃，是整个队列暂停等它先过。', 5);
    metrics.retries++;
    emit('q0', '1 秒后重投递：仍失败（库存未补）。继续挂起重试。这段时间 q0 不再派发任何消息——尚未发送的「完成」不可能越过出库被提前消费，这就是顺序语义宁可慢、不可乱。', 6);
    items.retrying = null;
    items.done.push('O1 出库');
    metrics.consumed++;
    syncQueued();
    emit('consumer', '第三次重投递成功：库存已补足，出库完成。两次失败全部原地消化，没有一条消息被跳过、没有一步被乱序。', 7);
    enqueue(items.q0, 'O1 完成');
    consume(items.q0, 'O1 完成');
    emit('consumer', '最后发送并消费 O1-完成：出库成功后「完成」才轮到——若采用并发消费的跳过策略，完成先于出库执行，订单会「已完成」却从未出库。', 8);
    emit('q0', '运行结束：发送 4 · 消费 4 · 重试 2 · 积压 0——失败发生在第 4 步出库，连续两次 SUSPEND 挂起让队列停摆约 2 秒，但换来了零乱序：每一条后续消息都建立在真实的前序结果上。挂起重试的代价是吞吐暂时归零，收益是业务前提永不被越过。', 0);
  } else {
    emit('schedule', '延迟消息模型就绪：投递延迟在 Broker 端完成。发送时带 delayTimeLevel（固定 18 档：1s/5s/10s/30s/1m/2m/…/1h/2h，第 5 档 = 1 分钟），消息先进 SCHEDULE_TOPIC_XXXX 的对应级别定时队列，到点才被转投业务队列——消费端无感知。', 0);
    items.delayed.push({ label: '超时关闭 #3001', lvl: 5 });
    metrics.sent++;
    syncQueued();
    emit('producer', '发送「订单超时关闭」#3001，setDelayTimeLevel(5)（1 分钟，教学示意：业务上通常等 30 分钟）。消息没有进业务队列，而是落入 SCHEDULE_TOPIC_XXXX 第 5 档定时队列——此刻任何消费者都拉不到它。', 1);
    emit('schedule', '定时线程扫描中：已静置约 40 秒，剩余约 20 秒。定时队列里的延迟消息不占业务队列、不阻塞普通消息——支付超时关单这类「晚点做」的活都被 Broker 排队托管。', 2);
    items.delayed = [];
    items.q1.push('超时关闭 #3001');
    syncQueued();
    emit('schedule', '到点！定时调度把 #3001 从 SCHEDULE_TOPIC 转投到业务 topic → 落入 q1 队尾。此刻它才变成一条普通消息：延迟只发生在 Broker 内部，投递即现身。', 3);
    consume(items.q1, '超时关闭 #3001');
    emit('consumer', '消费者拉取到 #3001 并执行关单：消费逻辑与普通消息一模一样——延迟对消费端完全透明，关单在发送后 1 分钟才被处理，恰到好处地给了用户支付缓冲期。', 4);
    emit('schedule', '运行结束：发送 1 · 消费 1 · 重试 0 · 积压 0——延迟消息 = 发送端一个档位参数 + Broker 端 SCHEDULE_TOPIC 定时转投：18 个固定档位是吞吐与实现成本的折中（不支持下单时指定任意秒数），消息先进定时队列静置、到点转投业务队列，消费者只是「晚些时候看到它」。', 0);
  }
  return frames;
}
function jucCoordination(p) {
  const scenario = p.scenario || 'countdown';
  const frames = [];
  const metrics = { submitted: 0, arrived: 0, released: 0, waiting: 0 };
  const items = { jobs: [], coord: { st: 'idle', txt: '' } };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items });
  const job = label => items.jobs.push({ label, st: 'idle' });
  const setSt = (i, st) => { items.jobs[i].st = st; };
  const coord = (st, txt) => { items.coord.st = st; items.coord.txt = txt; };
  if (scenario === 'countdown') {
    job('① 拉用户'); job('② 拉商品'); job('③ 拉库存');
    coord('idle', 'CountDownLatch = 3');
    emit('main', '「订单页渲染」协作模型就绪。页面要三份数据：用户资料、商品详情、实时库存——单线程串行拉取约 240ms，明显偏慢。换并行思路：三任务进线程池同时拉，主线程等「三份都齐」再一次性渲染。等齐的机关 = CountDownLatch(3)：初始化计数 3，子任务各完成一次就 countDown 减一，归零瞬间放行主线程。', 29 );
    items.jobs.forEach(j => (j.st = 'run'));
    metrics.submitted = 3;
    metrics.waiting = 1;
    coord('wait', 'await 阻塞 · 门闩剩 3');
    emit('pool', '三个拉取任务提交线程池并行执行；主线程调用 latch.await() 挂起等待——await 是让出 CPU 的阻塞等待，不是空转轮询。此刻主线程被 park，三线程各拉各的。', 34 );
    setSt(0, 'done');
    metrics.arrived = 1;
    coord('wait', '门闩剩 2');
    emit('w1', '线程① 45ms 完成拉用户 → countDown()：门闩 3→2。②③ 未归，主线程继续阻塞——只等「齐 N 件事」，不关心谁先完成。', 43 );
    setSt(1, 'done');
    metrics.arrived = 2;
    coord('wait', '门闩剩 1');
    emit('w2', '线程② 72ms 完成拉商品 → countDown()：2→1。', 43 );
    setSt(2, 'done');
    metrics.arrived = 3;
    coord('open', '归零 · 唤醒主线程');
    emit('w3', '线程③ 90ms 完成拉库存（最慢的一个）→ countDown()：1→0！计数归零瞬间，主线程被唤醒。总耗时 ≈ 最慢任务 90ms，而不是三者相加的 240ms。', 43 );
    metrics.released = 1;
    coord('ok', '三份数据齐 · 已合并渲染');
    emit('main', '主线程 await 返回：三份数据齐备，一次性合并渲染订单页。警惕 await 的代价：若某任务异常退出、countDown 永远不执行，主线程将永久阻塞——生产代码必须用 await(timeout) 超时兜底（本课教学省略该分支）。', 35 );
    emit('main', '运行结束：发起 3 · 到达 3 · 放行 1 · 阻塞 1——CountDownLatch = 主线程等 N 件事齐的一次性闸门：await 挂起、countDown 报数、归零放行。归零后不可复用，要再等一批就 new 一个新的。', 29 );
  } else if (scenario === 'barrier') {
    job('A 分片'); job('B 分片'); job('C 分片');
    coord('idle', 'CyclicBarrier(3) · 已到 0/3');
    emit('main', '「两阶段分片统计」协作模型就绪：同一份大文件拆成 A/B/C 三个分片。阶段一：各线程独立统计自己的分片；阶段二：把统计结果合并写汇总。阶段二建立在阶段一之上——先算完的线程不能自顾自冲进阶段二，必须等三人都到齐。CyclicBarrier(3)：每个线程算完 await() 等同伴，最后到达者触发屏障打开。', 51 );
    items.jobs.forEach(j => (j.st = 'run'));
    metrics.submitted = 3;
    coord('busy', '阶段一 · 各自统计中');
    emit('pool', '三线程同时开工：A/B/C 各自扫描分片做阶段一统计（约 35/52/68ms，天然错开）。', 57 );
    setSt(0, 'wait');
    metrics.arrived = 1;
    metrics.waiting = 1;
    coord('wait', '已到 1/3 · A 等待');
    emit('w1', 'A 线程先算完（35ms）→ barrier.await()：到齐 1/3，未满 → A 阻塞挂起等同伴。先到者的代价：真实等待。', 60 );
    setSt(1, 'wait');
    metrics.arrived = 2;
    metrics.waiting = 2;
    coord('wait', '已到 2/3 · A/B 等待');
    emit('w2', 'B 线程 52ms 算完 → await()：2/3，仍不满 → B 也阻塞。此刻 A、B 都在等最后一个 C。', 60 );
    setSt(0, 'run'); setSt(1, 'run'); setSt(2, 'run');
    metrics.arrived = 3;
    metrics.released = 3;
    coord('open', '到齐 3/3 · 放行阶段二');
    emit('w3', 'C 线程最后算完（68ms）→ await()：到齐 3/3！最后到达者触发屏障打开——A、B 同时被唤醒，三人齐刷刷进入阶段二。与 Latch 的分工差异在此：Latch 等的是外部事件报数，Barrier 是线程之间互相等齐。', 60 );
    items.jobs.forEach(j => (j.st = 'done'));
    coord('ok', '屏障复位 0/3 · 可循环');
    emit('pool', '阶段二：A/B/C 各把统计段写进汇总文件的不同区段——互不重叠、无需加锁。写完这一轮流程结束，屏障自动复位（cyclic 得名于此）：下一批文件可以直接再来一轮同样的两阶段协作。', 61 );
    emit('coord', '运行结束：发起 3 · 到达 3 · 放行 3 · 阻塞 2——A、B 两位先到者各真实阻塞一次，C 扮演「开门人」不等待。Barrier 的等待是线程互相等齐、放行后自动复位可复用；若某线程中断或超时，屏障会被打破（BrokenBarrierException），其余线程集体退出。', 51 );
  } else {
    job('甲'); job('乙'); job('丙');
    coord('idle', 'Semaphore(2) · 许可 2/2');
    emit('main', '「写审计日志」限流模型就绪：三线程并发请求写库，但数据库连接池只放得下 2 个连接。Semaphore(2)：acquire() 拿到一张许可才能进临界区，release() 归还。与锁不同：许可不绑定持有者——任何线程都能 release，它管的是「同时在场人数」，不是「谁独占」。', 74 );
    metrics.submitted = 3;
    metrics.arrived = 2;
    metrics.waiting = 1;
    setSt(0, 'run'); setSt(1, 'run'); setSt(2, 'wait');
    coord('wait', '许可 0/2 · 丙排队');
    emit('pool', '甲、乙先后 acquire 成功（许可 2→0）进入临界区写日志；丙 acquire 失败——许可耗尽，阻塞进入 FIFO 等待队列。连接池同时占用数被死死压在水位 2 以内。', 79 );
    metrics.released = 1;
    setSt(0, 'done');
    coord('open', '许可 1/2 · 唤醒丙');
    emit('w1', '甲写完日志、释放连接 → release()：许可 0→1，唤醒队首的丙。归还许可的动作谁做都行——哪怕不是甲本人，只要还一张，排队者就能前进。', 86 );
    metrics.arrived = 3;
    setSt(2, 'run');
    coord('wait', '许可 0/2 · 乙丙执行中');
    emit('w3', '丙被唤醒 acquire() 成功（许可 1→0）进入临界区开始写——排队约 30ms 后终于拿到资源。此刻乙仍在执行，池内 2 个连接再次占满。', 83 );
    metrics.released = 2;
    setSt(1, 'done');
    coord('open', '许可 1/2 · 空置待取');
    emit('w2', '乙写完 → release()：许可 0→1。此刻无人排队——许可空置，等待下一个请求来取。', 86 );
    metrics.released = 3;
    setSt(2, 'done');
    coord('ok', '许可 2/2 · 全部归还');
    emit('w3', '丙写完 → release()：许可 1→2，全部归还。三笔审计日志全部落库，全程池内占用从未超过 2。', 86 );
    emit('coord', '运行结束：发起 3 · 到达 3 · 放行 3 · 阻塞 1——2 张许可 3 个请求：甲、乙直通，丙排队直到甲 release 才进场。Semaphore 是流量闸门：构造定水位、acquire 进水、release 放水；连接池、限流乃至 1 张许可的互斥锁都是它的用武之地。', 74 );
  }
  return frames;
}
function mysqlCrash(p) {
  const scenario = p.scenario || 'wal';
  const frames = [];
  const metrics = { committed: 0, flushed: 0, replayed: 0, rolledback: 0 };
  const items = { pages: [{ id: 'P5', st: 'clean' }], redos: [], undos: [], binlogs: [], state: 'running' };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items });
  const page = () => items.pages[0];
  const dirty = () => { page().st = 'dirty'; };
  const clean = () => { page().st = 'clean'; };
  const addRedo = t => items.redos.push({ t, st: 'prepared' });
  const redoSt = (t, st) => { const r = items.redos.find(r => r.t === t); if (r) r.st = st; };
  const addUndo = (t, v) => items.undos.push({ t, v });
  const dropUndo = t => { items.undos = items.undos.filter(u => u.t !== t); };
  const addBin = t => items.binlogs.push({ t });
  const state = s => { items.state = s; };
  if (scenario === 'wal') {
    emit('app', 'WAL 教学模型就绪：余额 1000 的一行记录落在数据页 P5。本场景看一次「提交」的完整磁盘轨迹——区分两件事：「已提交」（redo 已 fsync）与「已落盘」（数据页已刷到磁盘），它们通常不在同一时刻发生。', 53 );
    dirty();
    addUndo('T1', 1000);
    emit('bp', 'T1 执行 UPDATE bal = 1000-100：直接在 Buffer Pool 里的页 P5 上改写为 900——磁盘一个字都没动，P5 从此是脏页。同时 undo log 记下旧值 1000 的回滚映像：只要 T1 没提交，随时能把它抹回原样。', 59 );
    addRedo('T1');
    redoSt('T1', 'committed');
    dropUndo('T1');
    metrics.committed = 1;
    emit('redo', 'T1 COMMIT：redo log buffer 里的修改记录（P5 的某偏移 → 900）顺序写入 redo log 文件并 fsync——这一刻才是「提交成功」的法律依据。顺序追加写日志 vs 随机写数据页：快一个数量级。', 64 );
    emit('app', '客户端收到 commit ok。注意此刻 P5 在磁盘上还是旧版本（1000）——内存 900 与磁盘 1000 的「分裂」是常态，不是错误。若实例此刻崩溃，内存里的 900 会蒸发，但 redo 里有记录，重启可找回（下个场景演示）。', 68 );
    dirty();
    addUndo('T2', 900);
    emit('bp', 'T2 执行 UPDATE bal = 900-200：Buffer Pool 改写 P5 为 700，页保持脏；undo 记旧值 900。', 75 );
    addRedo('T2');
    redoSt('T2', 'committed');
    dropUndo('T2');
    metrics.committed = 2;
    emit('redo', 'T2 COMMIT：redo 记录（P5 → 700）顺序写盘并 fsync，提交成功。', 78 );
    dirty();
    addUndo('T3', 700);
    addRedo('T3');
    redoSt('T3', 'committed');
    dropUndo('T3');
    metrics.committed = 3;
    emit('bp', 'T3 执行 UPDATE bal = 700-400 并 COMMIT：页 P5 改写为 400，redo 第三条记录（P5 → 400）fsync 落盘。三条 redo 全部安全，而 P5 依然躺在 Buffer Pool 里当脏页——提交密集发生时磁盘数据页可以「欠账」，日志不许欠。', 81 );
    clean();
    metrics.flushed = 1;
    emit('data', '运行结束：提交 3 · 刷盘 1 · 重放 0 · 回滚 0——后台刷脏线程出手：把 P5（T1+T2+T3 的累计结果 400）一次性随机写盘，页变干净，checkpoint 前移到三条 redo 之后。若无 WAL，三个事务要三次随机写页、三次等落盘；有 WAL，三次顺序写日志 + 一次批量刷页。这就是 InnoDB 敢把 fsync 预算全花在日志上的原因。', 84 );
  } else if (scenario === 'crash') {
    emit('app', '崩溃恢复教学模型就绪：同一账本（余额 1000 落页 P5）。本场景盯住「崩溃的那一秒」：提交过的事务凭什么不丢？重启时引擎做了什么？', 92 );
    dirty();
    addRedo('T1');
    redoSt('T1', 'committed');
    metrics.committed = 1;
    emit('bp', 'T1 执行 UPDATE bal = 1000-100 并 COMMIT：Buffer Pool 改写 P5 为 900（脏页），redo 记录 fsync 落盘，提交成功。', 96 );
    clean();
    metrics.flushed = 1;
    emit('data', '后台刷脏线程把 P5 写盘：磁盘页 = 900，checkpoint 前移越过 T1——从此 T1 不再需要 redo 保命，页自己已经在盘上了。', 98 );
    dirty();
    addRedo('T2');
    redoSt('T2', 'committed');
    metrics.committed = 2;
    emit('redo', 'T2 执行 UPDATE bal = 900-200 并 COMMIT：redo 记录（P5 → 700）fsync 落盘。页 P5 再次变脏——磁盘还是 900，checkpoint 停步不前。', 104 );
    dirty();
    addRedo('T3');
    redoSt('T3', 'committed');
    metrics.committed = 3;
    emit('redo', 'T3 执行 UPDATE bal = 700-400 并 COMMIT：redo 第三条记录（P5 → 400）fsync 落盘，提交成功。此刻磁盘现场：P5 = 900（T1 版本），redo 文件里躺着 T1~T3 三条记录，checkpoint 停在 T1 之后。', 110 );
    state('down');
    emit('app', '实例崩溃（模拟 kill -9）：Buffer Pool 瞬间蒸发——内存里的 700、400 全没了。磁盘上只剩：P5 = 900（T1 已刷盘的部分）+ 完整的三条 redo。问题：T2、T3 是「已提交」的事务，用户的钱不能因为一次宕机就退回原状。', 114 );
    dirty();
    metrics.replayed = 2;
    state('recovering');
    emit('redo', '重启自动恢复：引擎定位 checkpoint（T1 之后），从那里开始顺序重放 redo——T2 的记录把页从 900 改回 700，T3 的记录改到 400。roll-forward 的目标：让磁盘页追平所有已提交事务，一条不落。', 121 );
    state('ok');
    emit('redo', '运行结束：提交 3 · 刷盘 1 · 重放 2 · 回滚 0——恢复完成，P5 = 400，T2、T3 分毫未丢。redo 重放是「已提交零丢失」的兑现机制；若没有 WAL 直接改页，崩溃可能把提交过的修改留在写了一半的磁盘页上，那才是真丢。日志先行 + 重放兜底 = InnoDB 敢向客户端承诺 commit ok 的底气。', 124 );
  } else {
    emit('app', '两阶段提交教学模型就绪。一条事务的持久化横跨两个独立文件：InnoDB 的 redo（引擎内部，崩溃恢复用）与 Server 层的 binlog（归档 + 从库复制）。提交必须让两者原子对齐——主库提交了而 binlog 没有，从库就永远缺这笔账。协议拆三步：redo prepare → binlog fsync → redo commit。事务 T1（-100）开演。', 132 );
    dirty();
    addUndo('T1', 1000);
    addRedo('T1');
    emit('redo', 'T1 修改页 P5（1000→900）后，第一步：redo 写入 prepare 标记并落盘——引擎侧已就绪，redo 里躺着 T1 的完整修改记录，随时可提交可回滚。', 137 );
    addBin('T1');
    emit('binlog', '第二步：Server 把 T1 的变更写入 binlog（事务 id = 1）并 fsync。binlog 落盘意味着：从库可能已经收到并执行了这条事务。', 142 );
    redoSt('T1', 'committed');
    dropUndo('T1');
    metrics.committed = 1;
    emit('redo', '第三步：redo 补写 commit 标记——T1 正式提交。三步齐：引擎与归档对 T1 达成一致，从库与主库不会分裂。', 145 );
    dirty();
    addUndo('T2', 900);
    addRedo('T2');
    emit('redo', 'T2（-200）走到第一步：redo prepare 落盘。此刻正处于窗口 A：引擎已就绪、binlog 还没写。若此刻崩溃，T2 该怎么处置？', 149 );
    state('down');
    emit('app', '崩溃！磁盘现场：redo 里有 T2 的 prepare、binlog 里没有 xid=2。麻烦在于 prepare 已落盘——引擎单看 redo 会以为 T2 可以提交，必须借助第二个文件做判定。', 153 );
    redoSt('T2', 'discard');
    dropUndo('T2');
    items.redos = items.redos.filter(r => r.t !== 'T2');
    metrics.rolledback = 1;
    state('running');
    emit('undo', '恢复判定：binlog 中没有 xid=2 → 从库从未收到 T2，主库也不该有它 → 按 undo 把 P5 上 T2 的修改抹掉（回滚）。redo 里那份 prepare 记录随之作废。规则一：binlog 没有 → 回滚。', 155 );
    dirty();
    addUndo('T3', 900);
    addRedo('T3');
    addBin('T3');
    emit('binlog', 'T3（-200）连走两步：redo prepare 落盘 + binlog（xid=3）fsync。此刻处于窗口 B：binlog 已有 T3、redo commit 还没写。若此刻崩溃呢？', 160 );
    state('down');
    emit('app', '崩溃！磁盘现场：redo 有 T3 prepare、binlog 有 xid=3、commit 标记缺失。与 T2 相反——从库可能已经执行了 T3，主库若回滚就是主从不一致。', 164 );
    redoSt('T3', 'committed');
    dropUndo('T3');
    metrics.replayed = 1;
    metrics.committed = 2;
    state('ok');
    emit('redo', '恢复判定：binlog 中有 xid=3 → 从库已收到，主库必须提交 → 恢复器补写 redo commit，T3 就地转正。规则二：binlog 有 → 补提交。', 166 );
    emit('app', '运行结束：提交 2 · 刷盘 0 · 重放 1 · 回滚 1——两个崩溃窗口、一条判定规则：binlog 有 xid 就补 commit，没有就回滚。prepare 先行的意义：崩溃后引擎能区分「已就绪未归档」（T2，作废）与「已归档未拍板」（T3，转正）。redo 与 binlog 各记一半、靠 xid 对齐，两阶段提交把「主从不分裂」从口号变成协议。', 169 );
  }
  return frames;
}
function concurrentHashmap(p) {
  const scenario = p.scenario || 'put';
  const frames = [];
  const metrics = { inserted: 0, conflicts: 0, migrated: 0, spread: 0 };
  const items = { threads: [], slots: [], newSlots: null, counter: { base: 0, cells: [], last: null } };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items });
  const threads = ops => ops.forEach(([id, op]) => items.threads.push({ id, st: 'idle', op }));
  const setSt = (id, st, op) => { const t = items.threads.find(t => t.id === id); t.st = st; if (op) t.op = op; };
  const slotOf = i => items.slots[i];
  const putKey = (i, k) => { slotOf(i).keys.push(k); metrics.inserted++; };
  const markFwd = i => { slotOf(i).fwd = true; metrics.migrated++; };
  const mkSlots = n => { for (let i = 0; i < n; i++) items.slots.push({ i, keys: [], fwd: false }); };
  const fill = (i, keys) => { slotOf(i).keys.push(...keys); };
  if (scenario === 'put') {
    mkSlots(8);
    threads([['T1', 'put(k1)'], ['T2', 'put(k2)'], ['T3', 'put(k3)'], ['T4', 'put(k4)']]);
    emit('table', '「并发写活动报名表」模型就绪：8 槽空表，T1~T4 四个线程即将并发 put。JDK8 的写入只有两档：hash 定位的槽位为空 → CAS 无锁直插；槽位已被占 → synchronized 锁住该 bin 头节点做链尾追加。锁永远不落在整张表上。', 29 );
    setSt('T1', 'done', 'put(k1) ✓');
    putKey(3, 'k1');
    emit('t1', 'T1 put(k1)：hash 定位槽 3——空槽！CAS(tab[3], null, k1) 原子直插成功，全程没有锁。这是快路径：读多写少场景下大多数 put 都该走这里，代价只有一次 CAS。', 37 );
    setSt('T2', 'done', 'put(k2) ✓');
    setSt('T3', 'wait', 'put(k3) 排队');
    metrics.conflicts = 2;
    putKey(3, 'k2');
    emit('t2', 'T2 put(k2) 与 T3 put(k3) 几乎同时落槽 3：tab[3] 已被 k1 占据，两次 CAS 双双失败（冲突 +2）。T2 抢到先手：synchronized(头节点 k1) 进入慢路径，锁内链尾追加 k2；T3 没抢到锁，在 bin 锁外阻塞排队——注意排队粒度：等的是「槽 3 的 bin 锁」，不是整张表。', 55 );
    setSt('T4', 'done', 'put(k4) ✓');
    putKey(5, 'k4');
    emit('t4', 'T4 put(k4)：hash 定位槽 5——空槽，CAS 直插成功，甚至没察觉到槽 3 正有线程持锁。写不同槽的线程完全并行：这正是「锁单 bin」与「锁全表」的分水岭——Hashtable 的 synchronized(this) 此刻会让 T4 在门外等 T2 写完。', 58 );
    setSt('T3', 'done', 'put(k3) ✓');
    putKey(3, 'k3');
    emit('t3', 'T2 释放 bin 锁 → 唤醒 T3：T3 获得锁后链尾追加 k3（槽 3 链长 3：k1→k2→k3）。同槽的写被串行化，但串行范围只限于这一个 bin——这就是 JDK8 的锁粒度：空槽 CAS（无锁）、冲突锁单 bin（微串行），并发度≈桶数。', 43 );
    emit('sync', '运行结束：写入 4 · 撞槽 2 · 迁移 0 · 分流 0——四次写入里 T1、T4 走 CAS 快路径零锁开销，T2、T3 撞同一槽才各付出一次锁等待。若换成 Hashtable：四次写全部全局互斥、理论并发度 1；JDK7 分段锁把表切成 16 段、并发度 16；JDK8 锁到单个 bin、并发度等于桶数——锁粒度进化的终点是「只锁被触碰的那一小块」。', 29 );
  } else if (scenario === 'resize') {
    mkSlots(8);
    threads([['T1', 'put(G)'], ['T2', 'put(H)'], ['T3', '只读'], ['T4', '只读']]);
    fill(0, ['A']); fill(2, ['B', 'C']); fill(3, ['D']); fill(5, ['E']); fill(6, ['F']);
    emit('table', '「并发扩容」模型就绪：8 槽表已存 6 个 key（A、B·C、D、E、F），恰好等于扩容阈值 0.75×8=6——下一次 put 将先触发扩容到 16 槽再插入。教学 hash：key 的去留由高位决定，低位置 0 的留原槽、置 1 的进原槽+8。', 102 );
    items.newSlots = [];
    for (let i = 0; i < 16; i++) items.newSlots.push({ i, keys: [], fwd: false });
    setSt('T1', 'run', 'put(G) 触发扩容');
    emit('t1', 'T1 put(G) 发现 size 已达阈值 → 先扩容：分配 16 槽新表，随后从尾槽向前逐个迁移旧槽（transferIndex 协作指针递减）。迁移期间旧表读写不冻结——这是 CHM 与「拷贝整表再替换」式扩容的本质区别。', 107 );
    markFwd(7); markFwd(6); markFwd(5);
    items.newSlots[6].keys.push('F'); items.newSlots[5].keys.push('E');
    emit('table', '迁移推进：槽 7、6、5 依次迁完——F 进新表槽 6、E 进新表槽 5，每个迁完的旧槽原地放入 ForwardingNode（fwd 占位）。fwd 是一张「路由牌」：告诉后来的线程这槽已搬走、请沿它去新表。', 108 );
    markFwd(3); markFwd(2);
    items.newSlots[3].keys.push('D');
    items.newSlots[2].keys.push('B'); items.newSlots[10].keys.push('C');
    emit('table', '迁移槽 3、2：D 是低位 key → 新表槽 3。槽 2 的链 B→C 按 hash 高位拆成两段——B 低位段 → 新表槽 2，C 高位段 → 新表槽 10（原槽+8）。一条链就地劈开，这就是扩容的 rehash：每槽 keys 只可能去「原槽」或「原槽+旧容量」两个位置。', 109 );
    markFwd(1); markFwd(4); markFwd(0);
    items.newSlots[0].keys.push('A');
    emit('table', '迁移收尾：槽 1、4（空）、0（A → 新表槽 0）迁完，旧表 8 槽全部挂上 fwd。此后旧表退化为纯「路由牌」：任何访问先看旧槽，是 fwd 就转新表——已迁槽绝不会再被写入旧表，扩容期间的数据才不丢不重。', 110 );
    setSt('T1', 'done', 'put(G) ✓');
    metrics.inserted++;
    items.newSlots[4].keys.push('G');
    emit('t1', 'T1 恢复执行 put(G)：定位旧槽 4——fwd！读线程沿 fwd.next 到新表槽 4 找到数据，写线程同样转入新表执行插入。G 落位新表槽 4，旧表纹丝不动。', 118 );
    setSt('T2', 'done', 'put(H) ✓');
    metrics.inserted++;
    items.newSlots[12].keys.push('H');
    emit('t2', 'T2 put(H)：新请求直接对新表寻址——高位 key H 落新表槽 12，CAS 直插成功。扩容已经完成，此后一切读写都发生在 16 槽新表上。', 127 );
    emit('sync', '运行结束：写入 2 · 撞槽 0 · 迁移 8 · 分流 0——8 个旧槽全部迁移并挂 fwd：F/E/D/B/A 留原槽、C/G 高位移位。迁移是「按槽协作」而非「整表停摆」：中途的任何 put 要么转新表、要么等该槽迁完，绝不写进已搬走的旧槽。fwd 占位 + 路由，让并发扩容既不停顿也不丢数据。', 102 );
  } else {
    mkSlots(8);
    threads([['T1', 'put(k1)'], ['T2', 'put(k2)'], ['T3', 'put(k3)'], ['T4', 'size() 观察者']]);
    emit('sync', '「弱一致计数」模型就绪：CHM 不维护一个被全表锁保护的 size 字段，而是 baseCount（CAS 自增）+ CounterCell[]（撞车分流）。低并发一次 CAS 搞定；高并发撞车者把增量写进自己散列的 cell，size() 最后 base + Σcells 求和。', 136 );
    setSt('T1', 'done', 'put(k1) ✓');
    setSt('T2', 'done', 'put(k2) ✓');
    items.counter.base = 1;
    items.counter.cells.push(1);
    metrics.inserted = 2; metrics.conflicts = 1; metrics.spread = 1;
    slotOf(1).keys.push('k1'); slotOf(3).keys.push('k2');
    emit('t1', 'T1、T2 同时 put：T1 的 CAS(baseCount 0→1) 抢先成功（base=1）；T2 撞车失败——不无限重试，把增量写进自己的 CounterCell[0]（+1）。两笔写入都成功落槽，计数被拆成 base 1 + cells[0]=1 两处。', 147 );
    setSt('T3', 'done', 'put(k3) ✓');
    items.counter.base = 2;
    metrics.inserted = 3;
    slotOf(5).keys.push('k3');
    emit('t3', 'T3 put(k3)：此刻无竞争，CAS(baseCount 1→2) 一次成功，base=2。低并发下 CounterCell 完全闲置——cells 只在撞车时才被启用。', 147 );
    setSt('T1', 'done', 'put(k4) ✓');
    setSt('T2', 'done', 'put(k5) ✓');
    items.counter.base = 3;
    items.counter.cells.push(1);
    metrics.inserted = 5; metrics.conflicts = 2; metrics.spread = 2;
    slotOf(2).keys.push('k4'); slotOf(4).keys.push('k5');
    emit('t1', 'T1、T2 第二轮同时 put（k4、k5）：T1 的 CAS(base 2→3) 再胜；T2 再败 → 这次散列到 CounterCell[1]（+1）。base=3、cells=[1,1]，实时共 5 笔写入——热点被两个 cell 摊开，谁都不必死等 baseCount。', 147 );
    items.counter.last = 5;
    setSt('T3', 'run', 'put(k6) 插入完成');
    slotOf(6).keys.push('k6');
    metrics.inserted = 6;
    emit('sync', '此刻外部线程调用 size()：sum = base 3 + cells[1+1] = 5。就在求和读快照的同一瞬间，T3 的 k6 已落槽（第 6 笔写入完成）但它的 baseCount CAS 还没落位——快照读不到 → size() 返回 5，而真实是 6。弱一致窗口：size() 不是精确值，是「某一时刻的近似快照」。', 154 );
    setSt('T3', 'done', 'put(k6) ✓');
    items.counter.base = 4;
    items.counter.last = 6;
    emit('t3', 'T3 的 CAS(base 3→4) 落位：base=4、cells=[1,1]，实时 6 笔全部入账。此时再调 size() = 4+2 = 6，收敛到精确值——滞后只存在于求和与写入交错的瞬间。', 162 );
    emit('sync', '运行结束：写入 6 · 撞槽 2 · 分流 2——两次撞车各分流进一个 cell，size() 快照 5 → 收敛 6。size() 弱一致的根源：计数是打散的（base+cells），求和是并发的——想拿精确值就要付出全局锁，CHM 用弱一致换吞吐。这与 LongAdder 同一思想：热点计数拆成多份并行累加，读时再合并。', 136 );
  }
  return frames;
}
function mysqlLock(p) {
  const scenario = p.scenario || 'row';
  const frames = [];
  const metrics = { locked: 0, blocked: 0, deadlocks: 0, aborted: 0 };
  const items = { threads: [], locks: [], wait: [], victim: null };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items });
  const threads = ops => ops.forEach(([id, op]) => items.threads.push({ id, st: 'idle', op }));
  const setSt = (id, st, op) => { const t = items.threads.find(t => t.id === id); t.st = st; if (op) t.op = op; };
  const getLock = name => { let l = items.locks.find(x => x.name === name); if (!l) { items.locks.push({ name, kind: 'X', holder: null }); l = items.locks[items.locks.length - 1]; } return l; };
  const grant = (name, holder, kind) => { const l = getLock(name); l.kind = kind; l.holder = holder; };
  const release = name => { getLock(name).holder = null; };
  const lockOnce = () => metrics.locked++;
  const pushWait = id => { if (!items.wait.includes(id)) items.wait.push(id); };
  const dropWait = id => { items.wait = items.wait.filter(w => w !== id); };
  if (scenario === 'row') {
    threads([['T1', 'UPDATE 行1'], ['T2', 'UPDATE 行1'], ['T3', 'SELECT 行1']]);
    emit('db', '「行锁互斥与排队」模型就绪：products 表行 1（stock=100）。T1、T2 都要 UPDATE 行 1；T3 只做普通 SELECT。InnoDB 的锁粒度是「行」：UPDATE / DELETE / SELECT ... FOR UPDATE 才申请行锁，普通 SELECT 走 MVCC 快照读、不申请任何锁。', 51 );
    setSt('T1', 'run', 'UPDATE 行1');
    grant('行 1', 'T1', 'X');
    lockOnce();
    emit('t1', 'T1 执行 UPDATE products SET stock=stock-1 WHERE id=1：向行 1 申请 X（排他）锁——该行空闲，锁管理器授予。T1 在行 1 上持锁修改（stock 100→99），未提交。', 56 );
    setSt('T1', 'done', 'UPDATE 行1 · 未提交');
    setSt('T2', 'wait', 'UPDATE 行1');
    pushWait('T2');
    metrics.blocked = 1;
    emit('t2', 'T2 对同一行执行 UPDATE：X 与 X 不兼容——后到者不是报错而是「等待」：请求进入锁等待队列阻塞，等行 1 的持有者释放。默认 innodb_lock_wait_timeout=50s，超时才抛 1205；期间行 1 对一切写者与加锁读者关闭。', 71 );
    setSt('T3', 'done', 'SELECT 行1');
    emit('t3', 'T3 SELECT * FROM products WHERE id=1：普通读不撞锁——MVCC 快照读直接返回当前已提交版本（stock=100），从 T1 的 X 锁旁边零等待穿过。X 锁只挡「写」与「加锁读」（FOR UPDATE / LOCK IN SHARE MODE）：InnoDB 的读写因此互不阻塞。', 78 );
    setSt('T1', 'done', 'COMMIT');
    release('行 1');
    emit('t1', 'T1 提交 COMMIT：事务一结束，行 1 的 X 锁即释放，InnoDB 唤醒等待队列中的 T2。排队等的是「锁」不是「事务」——T1 释放的瞬间 T2 就有机会。', 83 );
    setSt('T2', 'run', 'UPDATE 行1');
    dropWait('T2');
    grant('行 1', 'T2', 'X');
    lockOnce();
    emit('t2', 'T2 从等待队列被唤醒 → 获得行 1 的 X 锁 → 执行 UPDATE：读到的是 T1 提交后的 stock=99（先提交先生效），修改完成。', 90 );
    setSt('T2', 'done', 'COMMIT');
    release('行 1');
    emit('t2', 'T2 提交，行 1 的锁再次释放——互斥只发生在同一行的写者之间，不同行与普通读全程不受影响。', 94 );
    emit('db', '运行结束：加锁成功 2 · 锁等待 1 · 死锁环 0 · 回滚 0——整场只有 T2 为同一行付出了一次等待，期间 T3 的普通读畅通无阻。行锁把互斥面收窄到「冲突的那一行」：若换成 MyISAM 的整表写锁，T3 与所有写者都会堵在 T1 后面；InnoDB 让不冲突的读写完全并行。', 95 );
  } else if (scenario === 'gap') {
    threads([['T1', 'FOR UPDATE 行2-5'], ['T2', 'INSERT id=3']]);
    emit('db', '「间隙锁 · 防幻读」模型就绪：RR（可重复读）隔离级别下，products 已有 id = 1、2、5、9——行 2 与行 5 之间夹着一段空的间隙 (2,5)（id 3、4 的位置）。T1 将对 id BETWEEN 2 AND 5 做范围加锁读；T2 想向该间隙插入 id=3。幻读要防的，正是「读的范围里被别人插进新行」。', 108 );
    setSt('T1', 'run', 'FOR UPDATE 行2-5');
    grant('行 2', 'T1', 'X'); grant('行 5', 'T1', 'X'); grant('间隙 (2,5)', 'T1', 'GAP');
    lockOnce();
    emit('t1', 'T1 SELECT * FROM products WHERE id BETWEEN 2 AND 5 FOR UPDATE：加锁成功（一次语句拿到三把锁，计数 +1）——行 2、行 5 各挂一把 X 记录锁（管已存在的行），两行之间的空隙 (2,5) 被 GAP 间隙锁罩住（管还不存在的行）。记录锁 ∪ 间隙锁合称 next-key lock：该区间既不许改旧行、也不许插新行。', 113 );
    setSt('T1', 'done', '加锁读 · 未提交');
    setSt('T2', 'wait', 'INSERT id=3');
    pushWait('T2');
    metrics.blocked = 1;
    emit('t2', 'T2 INSERT INTO products VALUES (3, …)：插入动作先申请「插入意向锁」（声明我要往这个间隙放行）→ 与 T1 持有的间隙锁 (2,5) 不兼容 → 阻塞排队。间隙锁互斥的不是某一行，而是「往空隙里插入」这个动作——行锁管已存在的行，间隙锁管还不存在的行。', 127 );
    emit('db', '对照：若隔离级别是 RC（读已提交）——没有间隙锁，T2 的插入直接成功并提交；T1 再次执行同条件加锁读会看到多出的 id=3。同一事务两次相同查询结果不一致，这就是幻读。RR 用 next-key 把「旧行 + 空隙」锁成一个连续区间，从入口堵死幻读。', 134 );
    setSt('T1', 'done', 'COMMIT');
    release('行 2'); release('行 5'); release('间隙 (2,5)');
    emit('t1', 'T1 提交 COMMIT：next-key 锁全部释放（两把行锁 + 一把间隙锁），InnoDB 唤醒等待插入的 T2——T1 的读一致性已由锁保证完毕，此刻放行插入不再产生幻读。', 139 );
    setSt('T2', 'run', 'INSERT id=3');
    dropWait('T2');
    grant('行 3', 'T2', 'X');
    lockOnce();
    emit('t2', 'T2 被唤醒：插入意向得到许可 → id=3 落位为正式数据行（加锁成功 2），原间隙 (2,5) 被它劈成 (2,3) 与 (3,5) 两段。此后 id 集合变为 1、2、3、5、9。', 146 );
    setSt('T2', 'done', 'COMMIT');
    release('行 3');
    emit('t2', 'T2 提交。间隙锁只存在于 RR 及以上的隔离级别；代价是放大阻塞面——紧邻被锁间隙的插入全部排队，高并发插入的热点区间要慎用范围加锁读。', 150 );
    emit('db', '运行结束：加锁成功 2 · 锁等待 1 · 死锁环 0 · 回滚 0——T1 的范围加锁读一次拿到三把锁（行2 X + 行5 X + 间隙 GAP）计为 1 次加锁成功；T2 的插入被间隙锁挡了一次，直到 T1 提交才放行。若没有间隙锁（RC），T2 无需等待，但 T1 的加锁读就会遭遇幻读——间隙锁是 RR 用一段阻塞换来的「读什么就是什么」。', 152 );
  } else {
    threads([['T1', 'UPDATE 行1 → 行2'], ['T2', 'UPDATE 行2 → 行1']]);
    emit('db', '「死锁环」模型就绪：两个事务都做两步更新——T1 先改行 1 再改行 2；T2 先改行 2 再改行 1。加锁顺序正好相反，是死锁最经典的成因；死锁检测器（后台线程，周期性扫描锁等待构成的等待图）已就位。', 166 );
    setSt('T1', 'run', 'UPDATE 行1');
    grant('行 1', 'T1', 'X'); lockOnce();
    emit('t1', 'T1 第一步 UPDATE 行1：行 1 空闲 → X 锁授予（加锁成功 1）。T1 攥住行 1 不松手。', 170 );
    setSt('T1', 'done', '持行1 · 等行2');
    setSt('T2', 'run', 'UPDATE 行2');
    grant('行 2', 'T2', 'X'); lockOnce();
    emit('t2', 'T2 第一步 UPDATE 行2：行 2 空闲 → X 锁授予（加锁成功 2）。两把锁各归其主，此刻一切正常。', 172 );
    setSt('T2', 'done', '持行2 · 等行1');
    setSt('T1', 'wait', 'UPDATE 行2');
    pushWait('T1');
    metrics.blocked = 1;
    emit('t1', 'T1 第二步要行 2：被 T2 持有 → X 冲突 → T1 进入等待队列（锁等待 1）。此刻 T1 是典型的「持有并等待」：攥着行 1 不放，等行 2 释放。', 173 );
    setSt('T2', 'wait', 'UPDATE 行1');
    pushWait('T2');
    metrics.blocked = 2;
    emit('t2', 'T2 第二步要行 1：被 T1 持有 → T2 也进入等待队列（锁等待 2）。等待图闭合：T1 → 等行2 ← 持行2 的 T2 → 等行1 ← 持行1 的 T1——循环等待成环。若无人干预，两个事务会互等到底，直到 50s 锁等待超时各自抛 1205。', 175 );
    metrics.deadlocks = 1;
    emit('detector', '死锁检测器扫描等待图：发现环 行1 → T1 → 行2 → T2 → 行1（死锁环 1）。死锁四条件在此齐备：互斥（行锁不共享）、持有并等待（都攥一把等一把）、不可剥夺（锁只能由持有者自己释放）、循环等待（等待关系成环）——检测器立即介入，而不是干等超时。', 212 );
    setSt('T2', 'idle', 'victim · 已回滚');
    metrics.aborted = 1;
    items.victim = 'T2';
    release('行 2');
    dropWait('T2');
    emit('detector', '挑选 victim：比较两事务的 undo 代价（已修改行数），T2 与 T1 各写 1 行打平 → 按内部规则取 T2。回滚 T2：按 undo 把行 2 恢复原值、释放其全部锁，并向应用返回 Error 1213（Deadlock found when trying to get lock; try restarting transaction）。死锁不罚双方——只牺牲代价小的一个，持锁的另一方继续。', 214 );
    setSt('T1', 'run', 'UPDATE 行2');
    dropWait('T1');
    items.victim = null;
    grant('行 2', 'T1', 'X'); lockOnce();
    emit('t1', '行 2 随 T2 回滚释放 → T1 被唤醒，X 锁授予（加锁成功 3）→ 完成第二步 UPDATE → COMMIT，两行更新原子生效，锁全部释放。T1 全程未回滚：死锁的代价由 victim 单方承担。', 221 );
    setSt('T1', 'done', 'COMMIT · 两行完成');
    release('行 1'); release('行 2');
    setSt('T2', 'run', '重试 UPDATE 行1');
    grant('行 1', 'T2', 'X'); lockOnce();
    emit('t2', '应用捕获 1213 → 重试整个事务：T2 以全新事务重新执行——UPDATE 行1 → 行 1 已被 T1 提交释放 → X 锁授予（加锁成功 4）→ 继续行 2 → 提交。重试的是「完整事务」而非从断点续跑：只有完整重放，业务语义才原子。', 228 );
    setSt('T2', 'done', '重试 COMMIT');
    release('行 1');
    emit('db', '运行结束：加锁成功 4 · 锁等待 2 · 死锁环 1 · 回滚 1——互反的加锁顺序把死锁四条件凑齐后，InnoDB 的选择不是超时硬等而是主动检测：发现环 → 回滚 undo 代价最小的 victim → 释放其锁让另一方走完 → 应用捕获 1213 重试整个事务。根治手段是让环无从形成：所有事务按固定顺序加锁（先小 id 后大 id），并把事务缩短到最小。', 231 );
  }
  return frames;
}
function kafkaEos(p) {
  const scenario = p.scenario || 'idempotent';
  const frames = [];
  const metrics = { sent: 0, stored: 0, committed: 0, blocked: 0 };
  const items = { producer: { st: 'idle', op: '', pid: 0, epoch: 1 }, coordinator: null, parts: { P0: [], P1: [] }, lso: null, lastSeq: 0, flash: null };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items });
  const push = (part, t, st, by) => items.parts[part].push({ t, st, by: by || null });
  const flash = (kind, text) => items.flash = { kind, text };
  if (scenario === 'idempotent') {
    items.producer = { st: 'idle', op: '幂等开 · pid7', pid: 7, epoch: 1 };
    items.lastSeq = 3;
    emit('broker', '「幂等去重」模型就绪：扣款服务写分区 P0，Broker 已存 3 条历史（lastSeq=3）。开启 enable.idempotence=true 后每个批次携带 (PID=7, 序列号)，Broker 为 (7, P0) 维护已确认序列号窗口：seq ≤ 窗口 = 重复；seq > 窗口+1 = 乱序。不开启幂等时 Broker 不记任何序列号——重试就是重复扣款。', 0);
    items.producer = { st: 'run', op: '发 pay-101 · s4', pid: 7, epoch: 1 };
    metrics.sent++; metrics.stored++; metrics.committed++;
    items.lastSeq = 4;
    push('P0', 'pay-101', 'ok');
    emit('producer', '发 pay-101（seq=4）：Broker 校验 4 = lastSeq(3)+1 连续 → 追加落盘，窗口推进到 lastSeq=4。幂等的代价只是批次头多两个字段：PID 标身份、seq 标次序——Broker 端一次查表就完成去重。', 1);
    items.producer = { st: 'retry', op: '重发 pay-101 · s4', pid: 7, epoch: 1 };
    metrics.sent++;
    metrics.blocked++;
    flash('dup', '✗ 重复 s4 · 去重拦截');
    emit('check', '响应超时 → producer 重发同一批（pid7, seq=4）：Broker 校验 4 ≤ lastSeq(4) → 已在窗口内 → 判重复：不重复追加，直接回 ack 让发送端以为成功。扣款只发生一次。若无幂等，这一批会再落盘一次——pay-101 被扣两次。', 2);
    items.flash = null;
    items.producer = { st: 'run', op: '管道 s5/s6 在途', pid: 7, epoch: 1 };
    metrics.sent += 2;
    emit('producer', '继续业务：producer 发出 pay-102（seq=5）与 pay-103（seq=6）——max.in.flight=2 的管道里两批在途（幂等下 broker 强制保序语义，但网络重排/重试仍可能让批次乱序触达）。', 3);
    metrics.blocked++;
    flash('gap', '✗ 跳号 s6 · OutOfOrder');
    emit('check', '乱序触达：pay-103（seq=6）先到——窗口 lastSeq=4，预期下一号是 5，6 是跳号 → Broker 拒绝追加并抛 OutOfOrderSequenceException（拦截 1）。若没有序列校验，它会在 pay-102 之前落盘，同分区的顺序就错乱了。', 4);
    items.producer = { st: 'run', op: '校准 lastSeq=5', pid: 7, epoch: 1 };
    metrics.stored++; metrics.committed++;
    items.lastSeq = 5;
    push('P0', 'pay-102', 'ok');
    flash('calib', '校准 · 窗口同步');
    emit('check', '晚到的 pay-102（seq=5）触达：5 = 窗口+1 → 合法追加落盘，lastSeq=5。producer 收到 OutOfOrder 异常后启动校准：向 Broker 拉取已确认窗口，重建「下一条该发什么」的共识——校准是幂等协议的自我修复，不是人工介入。', 5);
    items.producer = { st: 'run', op: '重发 pay-103 · s6', pid: 7, epoch: 1 };
    metrics.sent++; metrics.stored++; metrics.committed++;
    items.lastSeq = 6;
    push('P0', 'pay-103', 'ok');
    flash('calib', '校准后重发 s6');
    emit('producer', '校准完成 → 重发 pay-103（seq=6）：窗口 5+1=6 连续 → 落盘生效，lastSeq=6。被拒的批次从未写入，重发是全新的合法写入——幂等让「重试」从风险变成常规操作。', 6);
    items.producer = { st: 'done', op: '幂等完成 · pid7 · 3 条生效', pid: 7, epoch: 1 };
    items.flash = null;
    emit('broker', '运行结束：发送 5 · 落盘 3 · 生效 3 · 拦截 2——5 次发送尝试里，重发的 pay-101 被窗口判重拦截、抢跑的 pay-103 被跳号拦截，最终 P0 只多 3 条消息、扣款不重不漏。PID+seq 是幂等的全部秘密：Broker 记住每个 (PID, 分区) 最近确认的序列号，≤ 它是重复、> 窗口+1 是乱序——at-least-once 的「重试可能重复」就这样被压成 exactly-once 的单分区写入。', 0);
  } else if (scenario === 'transaction') {
    items.producer = { st: 'idle', op: '事务开 · pid21 · e1', pid: 21, epoch: 1 };
    items.coordinator = { st: 'idle', txn: null, epoch: 1, holder: 'P' };
    items.lso = { P0: 2, P1: 2 };
    emit('coord', '「事务与隔离」模型就绪：事务 producer（transactional.id=txn-pay）注册，coordinator 分配 PID=21、epoch=1。P0（订单）/P1（账务）各有 2 条历史，LSO=2。消费者开 read_committed：只能读到 LSO 之内的已拍板数据。单分区有序有幂等就够，跨分区原子必须事务：begin → 各分区写带事务标记的数据 → commit 两阶段拍板。', 0);
    items.producer = { st: 'run', op: 'begin T1', pid: 21, epoch: 1 };
    items.coordinator = { st: 'active', txn: 'T1', epoch: 1, holder: 'P' };
    emit('coord', 'T1 begin：coordinator 把事务 T1 记入事务状态（真实为 __transaction_state 分区，由 coordinator 持久化）。此刻分区还不知情——直到第一批带 T1 标记的数据到达。', 1);
    metrics.sent++; metrics.stored++;
    push('P0', 'ord-301', 'pend');
    emit('broker', 'T1 写 P0：ord-301 带事务标记物理追加成功（发送 1 · 落盘 1），但处于未提交段——LSO 仍是 2，read_committed 消费者读不到它；read_uncommitted 却读得到：脏读就是这么来的。', 2);
    metrics.sent++; metrics.stored++;
    push('P1', 'led-301', 'pend');
    emit('producer', 'T1 写 P1：led-301 追加（T1 第二段）。此刻若进程崩溃，T1 悬空：P0 有 ord-301、P1 没有——但 read_committed 两侧都不可见，外部看不到「一半成功」。事务的原子性不是写入时不落盘，而是拍板前不对外可见。', 3);
    items.producer = { st: 'run', op: 'T1 commit · 拍板', pid: 21, epoch: 1 };
    metrics.committed += 2;
    items.parts.P0[0].st = 'ok';
    items.parts.P1[0].st = 'ok';
    items.lso = { P0: 3, P1: 3 };
    items.coordinator = { st: 'idle', txn: null, epoch: 1, holder: 'P' };
    flash('marker', '✓ commit marker · LSO 越过');
    emit('coord', 'T1 commit 两阶段：coordinator 记录 PREPARE（拍板，不可反悔）→ 各分区 leader 写 commit marker → LSO 越过 T1 → ord-301 与 led-301 同时放行给 read_committed（生效 2）。marker 是分区里「这笔事务已拍板」的墓碑，LSO 是读取端的闸门——两者配合，跨分区提交才原子可见。', 4);
    items.flash = null;
    items.producer = { st: 'run', op: 'T2 写 cpn-302', pid: 21, epoch: 1 };
    items.coordinator = { st: 'active', txn: 'T2', epoch: 1, holder: 'P' };
    metrics.sent++; metrics.stored++;
    push('P0', 'cpn-302', 'pend');
    emit('producer', '营销事务 T2 begin 并写 P0：发券-302 落盘待拍板（发送 3 · 落盘 3）——券对 read_committed 不可见。业务跑完发现券模板已失效：放弃这笔。', 5);
    items.producer = { st: 'run', op: 'T2 abort · 拍板作废', pid: 21, epoch: 1 };
    metrics.blocked++;
    items.parts.P0[1].st = 'abort';
    items.lso = { P0: 4, P1: 3 };
    items.coordinator = { st: 'idle', txn: null, epoch: 1, holder: 'P' };
    flash('abort', '✗ abort marker · 圈禁 302');
    emit('coord', 'T2 abort：coordinator 记录 abort → P0 写 abort marker。数据物理还在（cpn-302 占着 offset 4），但被 marker 圈成作废段：read_committed 读到 marker 就跳过整段（拦截 1），LSO 同样越过——abort 是「拍板作废」，不是物理删除。Kafka 的日志只追加，逻辑上的回滚靠 marker 圈禁实现。', 6);
    items.flash = null;
    emit('consumer', '消费端盘点：P0 可见 ord-301（cpn-302 跳过）、P1 可见 led-301——跨两个分区的 T1 作为一个整体出现，T2 仿佛从未发生过。物理上 3 条都追加了，逻辑上生效的只有 2 条。', 7);
    items.producer = { st: 'done', op: '事务完成 · pid21 · 2 条生效', pid: 21, epoch: 1 };
    items.flash = null;
    emit('broker', '运行结束：发送 3 · 落盘 3 · 生效 2 · 拦截 1——一个提交事务（两分区同时生效）、一个作废事务（圈禁过滤）。Kafka 事务 = 分区物理追加 + coordinator 两阶段拍板 + 各分区 marker 圈定 + 读取端 LSO 把关：提交跨分区原子可见，作废对 read_committed 零感知。', 0);
  } else {
    items.producer = { st: 'idle', op: '实例A · pid31 · e1', pid: 31, epoch: 1 };
    items.coordinator = { st: 'idle', txn: null, epoch: 1, holder: 'A' };
    items.lso = { P0: 1, P1: 1 };
    emit('coord', '「僵尸驱逐」模型就绪：扣款服务实例 A 以 transactional.id=txn-pay 注册，coordinator 分配 PID=31、epoch=1（当前持有者 A）。P0（订单）/P1（账务）各 1 条历史。分区数据归属 PID——epoch 是「实例代数」：同一业务身份的每次注册代数 +1，旧代实例的请求从此代数落后。', 0);
    metrics.sent++; metrics.stored++;
    items.producer = { st: 'run', op: 'A · T1 写 ord-501', pid: 31, epoch: 1 };
    items.coordinator = { st: 'active', txn: 'T1', epoch: 1, holder: 'A' };
    push('P0', 'ord-501', 'pend', 'A');
    emit('producer', 'A 处理「订单-501 扣款」：事务 T1 写 P0，ord-501 落盘待拍板（发送 1 · 落盘 1）。正要写 P1 账务时……', 1);
    items.producer = { st: 'down', op: 'A 崩溃 · e1 失联', pid: 31, epoch: 1 };
    emit('broker', 'A 崩溃（进程终止/网络断）：T1 悬为 pending——ord-501 物理还在 P0，但从未拍板，read_committed 读不到。coordinator 侧 A 的会话消失，T1 等待裁决（真实中由 transaction.timeout.ms 兜底，本模型让新实例接管时一并裁决）。', 2);
    items.producer = { st: 'idle', op: '实例B · pid31 · e2', pid: 31, epoch: 2 };
    items.coordinator = { st: 'idle', txn: null, epoch: 2, holder: 'B' };
    emit('coord', '实例 B 重启接管（同 transactional.id）→ InitPid：coordinator 认出这是同一业务身份的继任者——PID=31 保留（消息归属不变），epoch 递增到 2。A 的 epoch=1 从此作废：代数校验已就位，僵尸的枪里没有子弹。', 3);
    metrics.blocked++;
    items.parts.P0[0].st = 'abort';
    items.lso = { P0: 2, P1: 1 };
    flash('abort', '✗ T1 作废 · 圈禁 A 残留');
    emit('coord', 'coordinator 裁决 A 的残局：持有者已换代，T1 永无可能被提交 → 判作废：写 abort marker 圈禁 P0 里 A 的 ord-501（拦截 1）。残留物理仍在 offset 1，但读到 marker 的消费者一律跳过——A 的半个事务从逻辑上消失，P1 上没有它的痕迹。', 4);
    items.flash = null;
    metrics.sent += 2; metrics.stored += 2;
    items.producer = { st: 'run', op: 'B · T2 重放 501', pid: 31, epoch: 2 };
    items.coordinator = { st: 'active', txn: 'T2', epoch: 2, holder: 'B' };
    push('P0', 'ord-501', 'pend', 'B');
    push('P1', 'led-501', 'pend', 'B');
    emit('producer', 'B 按业务状态机重放订单-501（事务未拍板就必须重做或放弃，不能假装发生过）：T2 写 P0 ord-501 + P1 led-501（发送 3 · 落盘 3）。同一条业务数据、新的代数——若 A 复活后 T1 也被提交，就会出现两份「扣款成功」；epoch 要拦的正是这个。', 5);
    items.producer = { st: 'run', op: 'B · T2 commit 拍板', pid: 31, epoch: 2 };
    metrics.committed += 2;
    items.parts.P0[1].st = 'ok';
    items.parts.P1[0].st = 'ok';
    items.lso = { P0: 3, P1: 2 };
    items.coordinator = { st: 'idle', txn: null, epoch: 2, holder: 'B' };
    flash('marker', '✓ T2 commit · 双分区生效');
    emit('coord', 'B commit T2：PREPARE → 两分区写 commit marker → LSO 越过（P0=3, P1=2）→ B 的 ord-501/led-501 同时生效（生效 2）。消费者此刻只认 B 的这版 501。', 6);
    metrics.blocked++;
    flash('fence', '✗ 僵尸 A · epoch 落后');
    emit('coord', '僵尸复活！网络分区恢复，A 的旧连接仍以为自己是主人，把 T1 的 commit（epoch=1 标记）发给 coordinator——代数校验：1 < 当前 2 → fence：拒绝并抛 FencedInstanceEpochException（拦截 2）。A 的客户端收到异常只能自毁退出——双写被挡在代数校验之外。', 7);
    items.producer = { st: 'done', op: 'B 完成 · e2 · 501 生效', pid: 31, epoch: 2 };
    items.flash = null;
    emit('consumer', '运行结束：发送 3 · 落盘 3 · 生效 2 · 拦截 2——A 的残留被作废圈禁、A 的复活请求被 epoch 拒绝，最终生效的只有 B 重放的一版 501。幂等（单分区不重）+ 事务（跨分区原子）+ epoch（僵尸无子弹）+ read_committed（不读未拍板）= Kafka 的精确一次：每层解决一个「可能重复/分裂/复活」的漏洞，四层合起来才敢对账。', 0);
  }
  return frames;
}
function esWrite(p) {
  const scenario = p.scenario || 'nrt';
  const frames = [];
  const metrics = { docs: 0, refreshs: 0, merges: 0, replayed: 0 };
  const items = { state: 'idle', buffer: [], tlog: [], segs: [], search: null, lost: [], flash: null };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items });
  const flash = (kind, text) => items.flash = { kind, text };
  if (scenario === 'nrt') {
    emit('client', '「近实时」模型就绪：向主分片写 w1/w2/w3。每条写入 = 内存 buffer（暂存）+ translog 同步 fsync（durability=request：ack 前日志先落盘）。此刻 0 个段——能不能搜取决于「段」，不取决于 buffer。', 23 );
    metrics.docs++; items.buffer.push('w1'); items.tlog.push({ t: 'w1', fs: true });
    emit('client', '写 w1：进 buffer（1/3），translog 已 fsync（✓）才回 ack——已确认就不会丢。但搜索看不到它：倒排还没建。', 23 );
    metrics.docs++; items.buffer.push('w2'); items.tlog.push({ t: 'w2', fs: true });
    emit('client', '写 w2：buffer 2 条、translog ✓。数据躺在「待搜索区」，要等 refresh 快照成段。', 23 );
    metrics.docs++; items.buffer.push('w3'); items.tlog.push({ t: 'w3', fs: true });
    emit('client', '写 w3：确认写入 3，buffer 满 3 条。默认 refresh_interval=1s——每秒（或 buffer 满时）自动执行一次 refresh。', 23 );
    items.search = { hits: [] };
    flash('miss', '✗ 0 命中 · 未 refresh');
    emit('search', '立即搜索：3 条都在 buffer——倒排还没建，0 命中。「近实时」的含义就在这一秒里：写入确认 ≠ 可搜索，中间隔着一次 refresh。', 23 );
    items.flash = null;
    items.search = null;
    metrics.refreshs++; items.segs.push({ id: 'seg-A', docs: ['w1', 'w2', 'w3'], mem: true, del: [] }); items.buffer = [];
    flash('refresh', 'refresh · seg-A 可搜');
    emit('segs', 'refresh #1：把 buffer 快照生成内存段 seg-A（w1-3），buffer 清空，段即刻进倒排、可被搜索。refresh 只动内存（OS page cache）不落盘——「可搜」与「已落盘」是两回事。', 23 );
    items.flash = null;
    items.search = { hits: ['w1', 'w2', 'w3'] };
    emit('search', '搜索命中 3：seg-A 的倒排已可查。内存段同样服务读请求——新数据先走内存快速可见，代价是小段会持续积累。', 23 );
    metrics.docs++; items.buffer.push('w4'); items.tlog.push({ t: 'w4', fs: true });
    items.search = { hits: ['w1', 'w2', 'w3'] };
    emit('client', '再写 w4（确认写入 4）：进 buffer、translog ✓。此刻搜索仍只命中 w1-3——w4 要等下一次 refresh。', 7);
    items.search = null;
    metrics.refreshs++; items.segs.push({ id: 'seg-B', docs: ['w4'], mem: true, del: [] }); items.buffer = [];
    flash('refresh', 'refresh · seg-B 生成');
    emit('segs', 'refresh #2：seg-B（w4）生成。段列表 seg-A + seg-B，都在内存、都可搜——若写入持续，段会一直累积；查询要归并的段越多越慢，那是 merge 机制的由来。', 8);
    items.flash = null;
    items.search = { hits: ['w1', 'w2', 'w3', 'w4'] };
    emit('segs', '运行结束：确认写入 4 · 刷新 2 · 合并 0 · 回放 0——两次 refresh 把 4 笔写入变成 2 个可搜内存段。近实时 = 写入确认（buffer+translog，日志语义）与可搜索（refresh 成段，索引语义）分离，两者默认隔一个 refresh_interval（1s）。', 23 );
  } else if (scenario === 'crash') {
    items.state = 'ok';
    emit('client', '「崩溃恢复」模型就绪：durability=request（每笔 ack 前 translog fsync）。先写 3 笔并 refresh 一次，再把日志切 async 写 2 笔——最后实例崩溃，看谁活下来。', 23 );
    metrics.docs++; items.buffer.push('w1'); items.tlog.push({ t: 'w1', fs: true });
    emit('client', '写 w1：buffer 1 条、translog 已 fsync（✓）后 ack——request 模式下确认即安全，日志已在磁盘。', 23 );
    metrics.docs++; items.buffer.push('w2'); items.tlog.push({ t: 'w2', fs: true });
    emit('client', '写 w2：buffer 2 条、日志 ✓。', 23 );
    metrics.docs++; items.buffer.push('w3'); items.tlog.push({ t: 'w3', fs: true });
    emit('client', '写 w3：确认写入 3。此刻 data 的完整路径：buffer 3 条 + translog 3 条已 fsync + 0 个段。', 23 );
    metrics.refreshs++; items.segs.push({ id: 'seg-A', docs: ['w1', 'w2', 'w3'], mem: true, del: [] }); items.buffer = [];
    flash('refresh', 'refresh · seg-A 可搜');
    emit('segs', 'refresh #1：seg-A（w1-3）生成，内存段可搜——但注意它没有 fsync：进程一死、内存段即蒸发。数据安全吗？安全：translog 里 w1-3 已落盘，恢复时靠它兜底。', 23 );
    items.flash = null;
    metrics.docs += 2; items.buffer.push('w4', 'w5'); items.tlog.push({ t: 'w4', fs: false }, { t: 'w5', fs: false });
    emit('client', '运维把 translog 切成 durability=async（每 5s 批量 fsync）换吞吐：写 w4、w5 立即回 ack——但此刻它们只在 OS page cache，日志还没 fsync（无 ✓）。', 23 );
    items.state = 'down'; items.buffer = []; items.segs = []; items.tlog = items.tlog.filter(l => l.fs); items.lost = ['w4', 'w5'];
    flash('crash', '实例崩溃');
    emit('buffer', '实例崩溃！内存态全灭：buffer（w4/w5）、内存段 seg-A、未 fsync 的日志尾巴。磁盘上只剩：translog 里已 fsync 的 w1-3——那是本次恢复的全部家底。', 23 );
    items.state = 'recovering'; metrics.replayed += 3; items.segs.push({ id: 'seg-A′', docs: ['w1', 'w2', 'w3'], mem: true, del: [] });
    flash('replay', 'translog 回放 w1-3');
    emit('translog', '启动恢复：加载 commit point → 从 translog 回放 w1-3（已 fsync 的逐条重放）→ 重新 refresh 出内存段 seg-A′。ack 过的数据一笔没少——request 模式的每一笔都 fsync 过。', 7);
    items.flash = null;
    items.state = 'ok'; items.search = { hits: ['w1', 'w2', 'w3'] };
    flash('lost', '✗ w4/w5 · async 窗口丢失');
    emit('client', '恢复完成：搜索命中 w1-3。对照 w4/w5——ack 已回但日志未 fsync，translog 里没有它们，随崩溃消失（丢尾巴 ≤ sync_interval=5s）。request 与 async 的取舍：每请求一次 fsync 的吞吐，换最多一个批窗口的丢失风险。', 8);
    items.flash = null;
    emit('segs', '运行结束：确认写入 5 · 刷新 1 · 合并 0 · 回放 3——已 fsync 的 3 笔经 translog 找回，async 尾巴 2 笔（w4/w5）确认后仍丢失。durability 决定「ack 到底意味着什么」：request = ack 即磁盘；async = ack 只是内存。', 23 );
  } else {
    emit('client', '「段合并」模型就绪：每次 refresh 生成一个小段——先连写 w1-4、每笔立即 refresh 造出 4 个小段，再观察查询、更新、删除与合并如何纠缠。', 0);
    for (let i = 1; i <= 4; i++) {
      metrics.docs++; items.tlog.push({ t: `w${i}`, fs: true });
      metrics.refreshs++; items.segs.push({ id: `seg-${i}`, docs: [`w${i}`], mem: true, del: [] });
      emit('client', `写 w${i} → refresh #${i}：seg-${i}（w${i}）生成。模拟写入流量下每个刷新周期产出一个新段——先接受小段，后面统一收拾（确认写入 ${i}）。`, i);
    }
    items.search = { hits: ['w1', 'w2', 'w3', 'w4'] };
    emit('search', '搜索：一次查询要跨全部 4 个段归并结果——段多 = 每查询多几份段开销。写入量大时段的增长是查询延迟的主要敌人，merge 线程就是为它而生的。', 5 );
    metrics.merges++; items.segs = [{ id: 'seg-A', docs: ['w1', 'w2', 'w3'], mem: false, del: [] }, { id: 'seg-4', docs: ['w4'], mem: true, del: [] }];
    flash('merge', 'merge #1 · 3 小段归并');
    emit('segs', '后台 merge #1：把 seg-1~seg-3 归并为 seg-A 并落盘（段 4 → 2）。合并只读旧段、产出新段，旧段随后废弃待删——段一旦生成就永不就地修改。', 6 );
    items.flash = null;
    items.search = null;
    metrics.docs++; items.buffer.push('w2′'); items.tlog.push({ t: 'w2′', fs: true });
    items.segs.find(s => s.id === 'seg-A').del.push('w2');
    emit('client', '更新 w2 → w2′：ES 没有就地改——新版本 w2′ 照常写 buffer + translog（确认写入 5），同时给 seg-A 里的旧 w2 打删除标记（tombstone）。旧值还占着空间，搜索端已不再返回它。', 7 );
    metrics.refreshs++; items.segs.push({ id: 'seg-B', docs: ['w2′'], mem: true, del: [] }); items.buffer = [];
    flash('refresh', 'refresh · seg-B 生成');
    emit('segs', 'refresh #5：w2′ 成段 seg-B。段列表：seg-A（磁盘 · 含 tombstone w2）、seg-4、seg-B——更新不重写旧段，只是另起一段。', 8 );
    items.flash = null;
    items.segs.find(s => s.id === 'seg-A').del.push('w3');
    flash('del', 'tombstone · 删 w3');
    emit('client', '删除 w3：给 seg-A 里的 w3 打删除标记（同时写 translog 防丢）。逻辑视图：w3 立即从搜索消失；物理视图：w3 的字节还在磁盘上，要等含它的段被合并才真正释放。', 9 );
    items.flash = null;
    metrics.merges++; items.segs = [{ id: 'seg-C', docs: ['w1', 'w2′', 'w4'], mem: false, del: [] }];
    flash('merge', 'merge #2 · 清除 tombstone');
    emit('segs', '后台 merge #2：把 seg-A（含两处 tombstone）、seg-B、seg-4 归并为 seg-C 落盘——合并时真正丢弃被删的旧 w2 与 w3，磁盘空间此刻才释放（段 3 → 1）。删除的执行者从来不是 DELETE，而是 merge。', 10 );
    items.flash = null;
    items.search = { hits: ['w1', 'w2′', 'w4'] };
    emit('segs', '运行结束：确认写入 5 · 刷新 5 · 合并 2 · 回放 0——w1-4 四次 refresh 攒出 4 个小段、w2′ 一次 refresh，两次合并把段收成 1 个（seg-C）；最终可搜的只有 w1 / w2′ / w4，被删的旧值与 w3 在合并那一刻才被清理——段只增不改，merge 是唯一的「删除执行者」。', 0);
  }
  return frames;
}
function functionCalling(p) {
  const scenario = p.scenario || 'single';
  const sceneTag = { single: '① 单工具 · 一次往返', multi: '② 并行调用 · 校验重试', guard: '③ 护栏拦截 · 幻觉工具' }[scenario];
  const frames = [];
  const metrics = { rounds: 0, calls: 0, executed: 0, retries: 0, blocked: 0 };
  const items = { calls: [], answer: null };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items });
  const ask = (name, short, full) => {
    metrics.calls++;
    const call = { name, short, full, st: 'ask', res: null };
    items.calls.push(call);
    return call;
  };
  emit('app', `Function Calling 教学模型就绪：场景「${sceneTag}」。已注册工具 get_weather(location, date)（返回 JSON）——模型不直连工具，只输出结构化调用请求，由应用校验后执行真实服务，结果回填后再生成终答。`, 0);
  if (scenario === 'single') {
    emit('user', '用户：「北京明天要带伞吗？」——请求携带工具定义，模型可以选择直接回答，也可以选择调用工具。', 1);
    const c = ask('get_weather', '北京', 'get_weather { "location": "北京", "date": "2026-09-10" }');
    metrics.rounds = 1;
    emit('llm', `LLM 生成轮 1：模型没有编造答案，而是输出结构化 tool_calls——${c.full}，finish_reason=TOOL_CALLS。「要调工具」是程序可直接解析的信号，不是自然语言。`, 2);
    emit('schema', 'JSON Schema 校验通过：location 非空、date 为合法 yyyy-MM-dd 且必填齐全 → 放行。校验不过的调用绝不会进入执行。', 4);
    c.res = '晴 · 22°C · 无雨';
    c.st = 'ok';
    metrics.executed++;
    emit('tool', '应用执行真实工具 get_weather → 200 { "condition": "晴", "temp": "22°C", "precip": "无雨" }——执行者是应用，模型全程不直连外部服务。', 5);
    emit('app', '工具结果以 role=tool 消息回填对话：模型下一轮生成能读到这份真实返回。', 6);
    metrics.rounds = 2;
    items.answer = '北京明天晴、22°C、无雨——不用带伞。';
    emit('llm', `LLM 生成轮 2：finish_reason=STOP——模型不再请求工具，基于回填的真实数据组织终答：「${items.answer}」工具调用循环结束。终答里的数字来自工具返回，不是模型编造。`, 7);
  } else if (scenario === 'multi') {
    emit('user', '用户：「北京和上海明天分别多少度？都适合晨跑吗？」——一个问题可能需要多个工具结果，模型可以并行输出多个 tool_calls。', 1);
    const c1 = ask('get_weather', '北京', 'get_weather { "location": "北京", "date": "2026-09-10" }');
    const c2 = ask('get_weather', '上海 · date=「明天」', 'get_weather { "location": "上海", "date": "明天" }');
    metrics.rounds = 1;
    emit('llm', 'LLM 生成轮 1：并行输出两个 tool_calls（同一轮生成，分别携带独立参数对象）——北京带合法日期，上海却把 date 写成了「明天」。', 2);
    c1.res = '晴 · 22°C';
    c1.st = 'ok';
    c2.st = 'bad';
    metrics.executed++;
    metrics.retries++;
    emit('schema', 'Schema 校验：北京通过；上海 date=「明天」不是 yyyy-MM-dd 格式 → 拒绝执行（重试 1）。失败不崩溃、不跳过——把错误结果回填给模型自纠。', 4);
    emit('tool', '北京执行成功：get_weather → 200 { "condition": "晴", "temp": "22°C" }。', 5);
    emit('app', '回填两条消息：北京 role=tool 真实结果 + 上海 role=tool 错误信息（date 格式非法，需修正为 yyyy-MM-dd）。', 6);
    const c3 = ask('get_weather', '上海 · 2026-09-10', 'get_weather { "location": "上海", "date": "2026-09-10" }');
    metrics.rounds = 2;
    emit('llm', 'LLM 生成轮 2：读到错误回填后自纠——重新输出上海的 tool_calls，date 已修正为 2026-09-10。', 2);
    emit('schema', 'Schema 校验通过：上海 date 已修正 → 放行。', 4);
    c3.res = '多云 · 26°C';
    c3.st = 'ok';
    metrics.executed++;
    emit('tool', '上海执行成功：get_weather → 200 { "condition": "多云", "temp": "26°C" }。', 5);
    emit('app', '上海结果以 role=tool 回填——两城数据齐了。', 6);
    metrics.rounds = 3;
    items.answer = '北京明天晴 22°C、上海多云 26°C——两地都适合晨跑，跑完记得补水。';
    emit('llm', `LLM 生成轮 3：finish_reason=STOP，基于两城真实返回终答：「${items.answer}」。期间经历了 1 次校验拒绝与模型自纠——护栏不是「挡住用户」，而是给模型一个修正的机会。`, 7);
  } else {
    emit('user', '用户连环追问三城天气，最后追加一句「顺便看下湿度」——任务本身没问题，但湿度不在已注册工具的能力内。', 1);
    const asks = [
      ask('get_weather', '北京', 'get_weather { "location": "北京", "date": "2026-09-10" }'),
      ask('get_weather', '上海', 'get_weather { "location": "上海", "date": "2026-09-10" }'),
      ask('get_weather', '广州', 'get_weather { "location": "广州", "date": "2026-09-10" }'),
    ];
    const res = ['晴 · 22°C', '多云 · 26°C', '阵雨 · 29°C'];
    for (let i = 0; i < 3; i++) {
      metrics.rounds = i + 1;
      emit('llm', `LLM 生成轮 ${i + 1}：输出 tool_calls get_weather{${['北京', '上海', '广州'][i]}, 2026-09-10}。`, 2);
      emit('schema', 'Schema 校验通过 → 放行执行。', 4);
      asks[i].res = res[i];
      asks[i].st = 'ok';
      metrics.executed++;
      emit('tool', `${['北京', '上海', '广州'][i]}执行成功：get_weather → 200 { "condition": "${res[i].split(' · ')[0]}", "temp": "${res[i].split(' · ')[1]}" }。`, 5);
      emit('app', '结果 role=tool 回填，模型带着新数据进入下一轮。', 6);
    }
    const h = ask('forecast_humidity', '', 'forecast_humidity { "location": "北京" }');
    metrics.rounds = 4;
    h.st = 'block';
    metrics.blocked++;
    emit('llm', 'LLM 生成轮 4：模型幻觉出工具 forecast_humidity——白名单里根本没有这个名字（可能来自提示注入或训练幻觉）。', 2);
    emit('schema', '白名单拦截（护栏）：工具名不在可用列表 → 不执行，回填 Unknown tool 错误，让模型收敛到已注册能力。', 8);
    emit('app', 'Unknown tool 错误回填：模型被告知「没有这个工具」，而不是得到一次伪造的数据。', 6);
    metrics.rounds = 5;
    items.answer = '可用工具只支持温度与降水查询，暂无湿度数据接口——可以查其他城市的温度与降水。';
    emit('llm', `LLM 生成轮 5：模型接受能力边界，基于三城真实数据终答：「${items.answer}」——没有湿度数据就如实说没有，不编造数字。5 轮 < max_iterations=8 兜底上限，循环有界且正常收敛。`, 7);
  }
  emit('app', `运行结束：生成轮 ${metrics.rounds} · 工具调用 ${metrics.calls} · 执行成功 ${metrics.executed} · 校验重试 ${metrics.retries} · 护栏拦截 ${metrics.blocked}——大模型从「会说话」到「能做事」的闭环：请求工具、应用执行、结果回填、基于事实作答。`, 8);
  return frames;
}
function threadlocalLeak(p) {
  const scenario = p.scenario || 'request';
  const sceneTag = { request: '① 请求线程 · 每线程一份副本', 'pool-leak': '② 线程复用 · 脏读与 value 泄漏', 'pool-remove': '③ 治理方案 · remove() 斩断引用' }[scenario];
  const frames = [];
  const metrics = { requests: 0, reads: 0, dirty: 0, leaks: 0, removes: 0 };
  const items = { threads: [], heap: [], removed: false };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items });
  const row = id => items.threads.find(t => t.id === id);
  emit('tl', `ThreadLocal 教学模型就绪：场景「${sceneTag}」。set/get 读写的是「当前线程」自己的 ThreadLocalMap：key 是 ThreadLocal 实例（弱引用），value 是存入对象（强引用）——只有线程销毁或 remove() 能斩断引用链。`, 12 );
  if (scenario === 'request') {
    metrics.requests++;
    emit('req', 'req#1 到达：应用为它新建请求线程 T1（教学简化：每请求一线程，请求结束即销毁）。', 12 );
    items.threads.push({ id: 'req#1', w: 'T1', ctx: '—', st: 'run' });
    emit('tl', 'req#1 业务代码 CTX.set(U1)：把用户上下文 U1 存入 T1 自己的 ThreadLocalMap——key 是 CTX 实例，value 是 U1。', 60 );
    row('req#1').ctx = 'U1';
    emit('map', 'T1 的 ThreadLocalMap 出现 entry：[CTX → U1]。这份数据只挂在 T1 上——其他线程各自处理别的请求，get 不到 U1，互不可见。', 71 );
    emit('tl', 'req#1 处理中读上下文：CTX.get() → U1（读取 1）——同线程存取命中本线程 entry，无需层层传参。', 62 );
    metrics.reads++;
    emit('req', 'req#1 处理完成，请求线程 T1 销毁——整张 ThreadLocalMap 作为线程对象的一部分一起被 GC，CTX→U1 随线程消失：无残留、无泄漏。', 73 );
    row('req#1').st = 'done'; row('req#1').ctx = '—';
    metrics.requests++;
    emit('req', 'req#2 到达：新建线程 T2 处理，与已销毁的 T1 毫无关系。', 12 );
    items.threads.push({ id: 'req#2', w: 'T2', ctx: '—', st: 'run' });
    emit('tl', 'req#2 调 CTX.set(U2)：写入 T2 自己的 ThreadLocalMap——两个请求各存各的，互不覆盖。', 60 );
    row('req#2').ctx = 'U2';
    emit('map', 'T2 的 ThreadLocalMap：[CTX → U2]；T1 的 Map 已随线程销毁。每线程一份副本，这正是「线程私有」的字面意思。', 71 );
    emit('tl', 'req#2 CTX.get() → U2（读取 2）——如果它去 get U1 会返回 null，因为 U1 在另一张已销毁的 Map 里。', 62 );
    metrics.reads++;
    emit('req', 'req#2 完成，T2 销毁——两张 Map 都只存在于各自线程的生命周期内。', 73 );
    row('req#2').st = 'done'; row('req#2').ctx = '—';
    emit('tl', `运行结束：请求 ${metrics.requests} · 读取 ${metrics.reads} · 脏读 ${metrics.dirty} · 泄漏 ${metrics.leaks} · 清理 ${metrics.removes}——线程不存活，Map 不残留：独立线程本身就是 ThreadLocal 最彻底的「清理」。`, 12 );
  } else if (scenario === 'pool-leak') {
    metrics.requests++;
    emit('req', 'req#1 到达：线程池分配 worker W1 处理（关键差异：W1 处理完不被销毁，而是空闲待命，等待下一个请求）。', 37 );
    items.threads.push({ id: 'req#1', w: 'W1', ctx: '—', st: 'run' });
    emit('tl', 'req#1 调 CTX.set(U1)：entry 写入 W1 的 ThreadLocalMap。', 60 );
    row('req#1').ctx = 'U1';
    emit('map', 'W1 的 ThreadLocalMap：[CTX → U1]——此刻一切正常，与独立线程场景无异。', 71 );
    emit('tl', 'req#1 处理中 CTX.get() → U1（读取 1）。', 62 );
    metrics.reads++;
    emit('req', 'req#1 处理完成——但 W1 不销毁：请求代码漏写了清理（没有 finally remove），CTX→U1 原样留在 W1 的 ThreadLocalMap 里。', 37 );
    row('req#1').st = 'residue';
    emit('entry', '随后请求上下文对象出栈：ThreadLocal 实例失去全部强引用——Entry 的 key 是弱引用，GC 一来就把 key 置 null：悬空 entry 诞生（key 已死，entry 还在）。', 71 );
    metrics.leaks++;
    items.heap.push('U1');
    emit('heap', '悬空 entry 的 value 仍是强引用：W1 活着、entry 不移除 → U1 永远占着堆（泄漏 1）。GC 帮不上忙——它只能回收弱 key，动不了强 value。', 72 );
    metrics.requests++;
    emit('req', 'req#2 到达：还是 W1 处理。', 37 );
    items.threads.push({ id: 'req#2', w: 'W1', ctx: '—', st: 'run' });
    emit('tl', 'req#2 业务代码直接 CTX.get()——本请求没 set 过，正常应返回 null（未登录），却命中 req#1 残留的 U1 → 脏读（脏读 1 · 读取 2）。', 62 );
    metrics.reads++;
    metrics.dirty++;
    row('req#2').ctx = 'U1'; row('req#2').st = 'dirty';
    emit('heap', '堆侧：U1 仍被 W1 的悬空 entry 强引用，无法回收——脏读与泄漏是同一个根因：entry 没被移除。', 72 );
    emit('tl', `运行结束：请求 ${metrics.requests} · 读取 ${metrics.reads} · 脏读 ${metrics.dirty} · 泄漏 ${metrics.leaks} · 清理 ${metrics.removes}——复用的 W1 不清除：req#2 读到 req#1 的上下文（脏读），req#1 的 value 悬空滞留（泄漏）。`, 12 );
  } else {
    metrics.requests++;
    emit('req', '治理版：req#1 到达，线程池分配 worker W1。', 49 );
    items.threads.push({ id: 'req#1', w: 'W1', ctx: '—', st: 'run' });
    emit('tl', 'req#1 调 CTX.set(U1)：写入 W1 的 ThreadLocalMap。', 60 );
    row('req#1').ctx = 'U1';
    emit('map', 'W1 的 ThreadLocalMap：[CTX → U1]——重点看请求结束时的 finally。', 71 );
    emit('tl', 'req#1 处理中 CTX.get() → U1（读取 1）。', 62 );
    metrics.reads++;
    emit('tl', 'req#1 结束，finally 块执行 CTX.remove()（清理 1）：当前线程的 entry 整条移除——key、value、Entry 对象一起消失，value 的引用链被斩断。', 66 );
    row('req#1').ctx = '—'; row('req#1').st = 'done';
    metrics.removes++;
    items.removed = true;
    emit('entry', '引用链对比：remove() 前 value ← entry ← ThreadLocalMap ← 线程（引用链完整，value 无法回收）；remove() 后 U1 失去全部引用，随时可被 GC——不必等 W1 销毁。', 66 );
    metrics.requests++;
    emit('req', 'req#2 到达：同一 worker W1 处理。', 49 );
    items.threads.push({ id: 'req#2', w: 'W1', ctx: '—', st: 'run' });
    emit('tl', 'req#2 CTX.get() → null（未登录）——上一请求的 U1 已被 remove 清掉，读不到任何残留（读取 2 · 脏读 0）。', 62 );
    metrics.reads++;
    row('req#2').st = 'done';
    emit('heap', '堆侧全程无滞留：治理版的每次请求边界都干干净净。', 73 );
    emit('tl', `运行结束：请求 ${metrics.requests} · 读取 ${metrics.reads} · 脏读 ${metrics.dirty} · 泄漏 ${metrics.leaks} · 清理 ${metrics.removes}——同样的线程复用，remove() 让存取边界清晰：拿到的只可能是本请求 set 的值，或 null。`, 12 );
  }
  return frames;
}
function blockingQueue(p) {
  const scenario = p.scenario || 'abq';
  const sceneTag = { abq: '① 有界阻塞 · put/take 与唤醒', lbq: '② 双锁拆分 · 入队出队不互斥', backpressure: '③ 背压传递 · 慢消费反制' }[scenario];
  const frames = [];
  const metrics = { puts: 0, takes: 0, blocks: 0, wakes: 0, peak: 0 };
  const items = { q: [], waiting: [], prod: null, cons: null };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items });
  const cap = scenario === 'backpressure' ? 2 : 3;
  const sync = () => { if (items.q.length > metrics.peak) metrics.peak = items.q.length; };
  const put = (m, note) => {
    metrics.puts++;
    items.q.push(m);
    sync();
    items.prod = { txt: `P · ${m} 入队`, st: 'ok' };
    emit('ring', `put ${m} 成功（尝试 ${metrics.puts}）：队列 [${items.q.join(' ')}]（${items.q.length}/${cap}）——${note}（put 成功会 signal notEmpty，唤醒在等 take 的消费者）。`, 148 );
  };
  const blockedPut = (m, note) => {
    metrics.puts++;
    metrics.blocks++;
    items.waiting.push(m);
    items.prod = { txt: `P · put ${m} · 阻塞`, st: 'block' };
    emit('lock', `队满（${items.q.length}/${cap}）→ put ${m} 阻塞在 notFull 条件上（尝试 ${metrics.puts} · 阻塞 ${metrics.blocks}）——${note}。${m} 没有入队，线程挂起让出 CPU，等消费者 take 后的 signal 唤醒。`, 148 );
  };
  const take = (m, note) => {
    items.q.shift();
    metrics.takes++;
    items.cons = { txt: `C · take ${m}`, st: 'run' };
    emit('cons', `take 出队 ${m}（消费 ${metrics.takes}）→ 空位 +1——${note}。`, 154 );
  };
  const wakePut = () => {
    const m = items.waiting.shift();
    items.q.push(m);
    metrics.wakes++;
    sync();
    items.prod = { txt: `P · ${m} 被唤醒补入`, st: 'ok' };
    emit('ring', `take 的 signal notFull 唤醒阻塞中的生产者 → ${m} 补入（唤醒 ${metrics.wakes}）：队列 [${items.q.join(' ')}]——唤醒是成对的：put 成功 signal notEmpty，take 成功 signal notFull。`, 144 );
  };
  emit('ring', `阻塞队列教学模型就绪：场景「${sceneTag}」。`, 103 );
  if (scenario === 'abq') {
    emit('ring', `ArrayBlockingQueue 容量 ${cap}（环状数组）：put 队满 → 阻塞在 notFull 条件；take 队空 → 阻塞在 notEmpty 条件。生产者连发 5 条、消费者稍后跟进——看第 4、5 次 put 怎么被「卡」住又怎么被「唤醒」。`, 29 );
    emit('prod', '生产者启动：开始连发 5 条消息。', 29 );
    put('m1', '容量未满，直接入队');
    put('m2', '直接入队');
    put('m3', '队列满 3/3——环状数组容量到顶，再写必须等空位');
    blockedPut('m4', '环状数组已满，没有空位可写');
    emit('cons', '消费者姗姗来迟，开始逐条 take。', 154 );
    take('m1', '队列 [m2 m3]，腾出一个空位');
    wakePut();
    blockedPut('m5', '消费者消化得不够快，队又满了');
    take('m2', '又腾出一个空位');
    wakePut();
    take('m3', '队列 [m4 m5]，继续消化');
    take('m4', '队列 [m5]');
    take('m5', '队列回到空——5 条消息全部被消费');
    emit('stats', `运行结束：put 尝试 ${metrics.puts} · take 成功 ${metrics.takes} · 阻塞 ${metrics.blocks} · 唤醒 ${metrics.wakes} · 峰值积压 ${metrics.peak}——第 4、5 次 put 都因队满阻塞在 notFull，各被一次 take 的 signal 唤醒后补入：没有忙等、没有丢失，队列容量就是生产者的「红绿灯」。`, 29 );
  } else if (scenario === 'lbq') {
    emit('ring', `LinkedBlockingQueue 容量 ${cap}（链表）：putLock 只护尾指针、takeLock 只护头指针——入队与出队持不同锁，可以真正并行。代价：count 需要 AtomicInteger 单独维护。`, 69 );
    emit('prod', '生产者与消费者同时开工：看双锁下入队、出队如何互不排队。', 69 );
    put('m1', '生产者持 putLock 入队');
    put('m2', 'P 持 putLock 连发（只碰尾指针）');
    take('m1', 'C 持 takeLock 出队（只碰头指针）——put 与 take 各护一端：若在单锁的 ArrayBlockingQueue 里，这两类操作要互相排队');
    put('m3', 'P 继续入队，与 C 的出队互不干扰');
    take('m2', 'C 追着消化');
    put('m4', 'P 入队与 C 出队可以发生在同一时刻');
    take('m3', 'C 继续 take，P 无需等待 C');
    put('m5', 'P 完成全部 5 次入队——全程没有一次在锁上等对方');
    take('m4', '队列 [m5]');
    take('m5', '队列清空');
    emit('stats', `运行结束：put 尝试 ${metrics.puts} · take 成功 ${metrics.takes} · 阻塞 ${metrics.blocks} · 唤醒 ${metrics.wakes} · 峰值积压 ${metrics.peak}——双锁之下入队出队互不阻塞：生产与消费的并发度由「锁的粒度」决定，而不是队列容量。`, 69 );
  } else {
    emit('ring', `容量 ${cap} 的有界队列 + 处理很慢的消费者：生产者高速连发 5 条——积压到顶后，看看「慢消费」的压力传到哪里。`, 103 );
    emit('prod', '消费者处理速度很慢（每 take 之间都要做长处理），生产者开始高速连发。', 103 );
    put('m1', '空位充足，秒入队');
    put('m2', '队列满 2/2——积压已达容量上限');
    blockedPut('m3', '消费者还在处理上一条，队列没有空位');
    blockedPut('m4', '生产者没有停手，继续尝试——又一次阻塞');
    blockedPut('m5', '三次 put 全被挡在门外：生产速率被队列硬生生钳制——背压：压力从慢消费一路反传回生产源头');
    emit('cons', '消费者终于处理完一条，开始逐条 take。', 154 );
    take('m1', '空位 +1');
    wakePut();
    take('m2', '继续慢慢消化');
    wakePut();
    take('m3', '又腾出空位');
    wakePut();
    take('m4', '队列 [m5]');
    take('m5', '队列清空');
    emit('stats', `运行结束：put 尝试 ${metrics.puts} · take 成功 ${metrics.takes} · 阻塞 ${metrics.blocks} · 唤醒 ${metrics.wakes} · 峰值积压 ${metrics.peak}——5 条消息一条不少，但其中 3 次 put 是「被唤醒后才补入」：有界队列把消费速率原样传回生产端、形成背压闭环；换成无界队列不会阻塞——代价是洪峰全吞进内存。`, 103 );
  }
  return frames;
}
function casAtomic(p) {
  const scenario = p.scenario || 'spin';
  const sceneTag = { spin: '① CAS 自旋 · 比较并交换', aba: '② ABA 复现 · 版本号拦截', adder: '③ LongAdder · 分段降竞争' }[scenario];
  const frames = [];
  const metrics = { ops: 0, ok: 0, spins: 0, detect: 0, spread: 0 };
  const items = { slot: null, stamp: null, blocked: false, base: null, cells: [], sum: null };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items });
  emit('cas', `CAS 教学模型就绪：场景「${sceneTag}」。计量口径：一次 compareAndSet 调用 +1 次尝试（含失败后重试的调用）。`, 98 );
  if (scenario === 'spin') {
    items.slot = 0;
    emit('thread', 'T1 开始自增：先读槽位当前值作为期望 → expect = 0（读与 CAS 之间，别人可能已经改过——这正是 CAS 要处理的不确定性）。', 38 );
    metrics.ops++;
    metrics.ok++;
    items.slot = 1;
    emit('cas', 'T1 compareAndSet(0, 1)：内存当前值仍 == 期望 0 → 写入成功（尝试 1 · 成功 1）。槽位 = 1——单变量更新无锁完成。', 40 );
    emit('thread', 'T2 也要自增：读槽位——但拿到的是它自己动作开始前的旧值 expect = 0（T1 的写入发生在 T2 读之后、CAS 之前）。', 38 );
    metrics.ops++;
    metrics.spins++;
    emit('cas', 'T2 compareAndSet(0, 1) 失败：内存当前值 1 ≠ 期望 0（尝试 2 · 自旋 1）。失败 ≠ 放弃：乐观并发把失败当作「期望过期」的信号。', 44 );
    emit('thread', 'T2 重读槽位 → expect = 1（自旋循环第二步：读新值再比）。', 38 );
    metrics.ops++;
    metrics.ok++;
    items.slot = 2;
    emit('cas', 'T2 compareAndSet(1, 2)：这次匹配 → 写入成功（尝试 3 · 成功 2）。槽位 = 2——两次自增全程没有互斥锁。', 40 );
    emit('stats', `运行结束：CAS 尝试 ${metrics.ops} · 成功提交 ${metrics.ok} · 自旋 ${metrics.spins} · ABA 拦截 ${metrics.detect}——自旋 = 失败 → 重读 → 再比 的循环：竞争越激烈空转越多，这是无锁的主要代价。`, 38 );
  } else if (scenario === 'aba') {
    items.slot = 10;
    emit('thread', '两幕对照：先用无版本戳的 AtomicInteger 演「值被改走又改回」，再换 AtomicStampedReference 重演同一剧本。槽位初始 10。', 62 );
    emit('thread', 'T2 出场：想把槽位改成 20 再改回 10——制造一段「无人察觉」的值往返。', 62 );
    metrics.ops++;
    metrics.ok++;
    items.slot = 20;
    emit('cas', 'T2：compareAndSet(10, 20) 成功（尝试 1 · 成功 1）——槽位改走：10 → 20。', 64 );
    metrics.ops++;
    metrics.ok++;
    items.slot = 10;
    emit('cas', 'T2：compareAndSet(20, 10) 成功（尝试 2 · 成功 2）——又改回：20 → 10。此刻值确实回到 10，但「10」已经被动过两次。', 64 );
    metrics.ops++;
    metrics.ok++;
    items.slot = 30;
    emit('cas', 'T1 姗姗来迟：compareAndSet(10, 30)——无戳版直接通过（尝试 3 · 成功 3）！期望与当前值都是 10，CAS 只比数值：值相同就放行。T1 无从得知中间被改走又改回——ABA 的表象欺骗。', 67 );
    items.slot = 10;
    items.stamp = 0;
    emit('stamp', '换成 AtomicStampedReference：槽位 10 + 版本戳 stamp 0——此后每次写都要同步更新戳，值与戳都匹配才算数。重演同一剧本：', 71 );
    metrics.ops++;
    metrics.ok++;
    items.slot = 20;
    items.stamp = 1;
    emit('cas', 'T2：stamped CAS(10→20, stamp 0→1) 成功（尝试 4 · 成功 4）：值变 20，戳推到 1——修改留下了痕迹。', 74 );
    metrics.ops++;
    metrics.ok++;
    items.slot = 10;
    items.stamp = 2;
    emit('cas', 'T2：stamped CAS(20→10, stamp 1→2) 成功（尝试 5 · 成功 5）：值虽回到 10，戳已到 2——数值还原了，版本没还原。', 74 );
    metrics.ops++;
    metrics.detect++;
    items.blocked = true;
    emit('stamp', 'T1：stamped CAS(10→30, 期望 stamp 0)——值 10 匹配，但当前戳 2 ≠ 期望 0 → 拒绝写入（尝试 6 · ABA 拦截 1）：版本戳记住了「中间被动过」。', 76 );
    items.blocked = false;
    emit('thread', 'T1 重读当前状态 → 期望 (10, stamp 2)，换新戳重试。', 62 );
    metrics.ops++;
    metrics.ok++;
    items.slot = 30;
    items.stamp = 3;
    emit('cas', 'T1：stamped CAS(10→30, stamp 2→3) 成功（尝试 7 · 成功 6）：值 30、戳 3——被拦截后重试才通过。', 74 );
    emit('stats', `运行结束：CAS 尝试 ${metrics.ops} · 成功提交 ${metrics.ok} · 自旋 ${metrics.spins} · ABA 拦截 ${metrics.detect}——无戳版 3 次调用全部通过（最后一次是表象欺骗）；加戳后同剧本的值往返在第 7 次调用被拦下。值相同 ≠ 没被动过：版本戳把「历史」变成可比较的状态。`, 62 );
  } else {
    items.base = 0;
    emit('cells', '8 个线程并发 +1。低竞争时都直写 base（一个共享计数）；一旦 base 上 CAS 撞车，不原地死磕，把增量写进自己的 Cell[] 槽位；sum() = base + Σcells。', 114 );
    emit('thread', 'T1~T3 率先到达并直写 base；T4~T8 同时读到 base=3，将撞车分流。', 114 );
    metrics.ops++;
    metrics.ok++;
    items.base = 1;
    emit('cas', 'T1：CAS(base 0→1) 直写成功（尝试 1 · 成功 1）——低竞争，base 上一步到位。', 114 );
    metrics.ops++;
    metrics.ok++;
    items.base = 2;
    emit('cas', 'T2：CAS(base 1→2) 成功（尝试 2 · 成功 2）。', 114 );
    metrics.ops++;
    metrics.ok++;
    items.base = 3;
    emit('cas', 'T3：CAS(base 2→3) 成功（尝试 3 · 成功 3）——base = 3。', 114 );
    const divert = (t, cell) => {
      metrics.ops++;
      emit('cas', `${t}：base CAS 撞车（尝试 ${metrics.ops}，未成功——多线程同时读到 base=3）→ 不再自旋死磕，把增量转写自己的 Cell[${cell}]。`, 114 );
      metrics.ops++;
      metrics.ok++;
      metrics.spread++;
      const c = items.cells.find(x => x.i === cell);
      if (c) c.n++;
      else items.cells.push({ i: cell, n: 1 });
      emit('cells', `${t}：Cell[${cell}] CAS +1 成功（尝试 ${metrics.ops} · 成功 ${metrics.ok} · 分流 ${metrics.spread}）——写竞争从全网争一个数，摊成每槽少数线程。`, 114 );
    };
    divert('T4', 0);
    divert('T5', 1);
    divert('T6', 0);
    divert('T7', 2);
    divert('T8', 1);
    items.sum = 8;
    emit('cells', `sum() = base + Σcells = ${items.base} + ${items.cells.map(c => c.n).join(' + ')} = ${items.sum}——与 8 个线程的总提交一致。sum 是弱一致快照（累加瞬间的并发写入可能缺席），教学脚本按最终态求和。`, 114 );
    emit('stats', `运行结束：CAS 尝试 ${metrics.ops} · 成功提交 ${metrics.ok} · 自旋 ${metrics.spins} · 分流 ${metrics.spread}——5 个撞车线程全部转向自己的 Cell，base 只被 3 个线程直写：热点从「一个数」摊成「base + 3 个 Cell」，写吞吐不随竞争塌陷。`, 98 );
  }
  return frames;
}
function completableFuture(p) {
  const scenario = p.scenario || 'chain';
  const sceneTag = { chain: '① 链式组装 · 串行与并行', error: '② 异常传播 · exceptionally 兜底', pool: '③ 线程池隔离 · 阻塞传染' }[scenario];
  const frames = [];
  const metrics = { stages: 0, steps: 0, errors: 0, fallbacks: 0, skipped: 0, queued: 0 };
  const items = { phase: null, jobs: [], value: null };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items });
  const requeue = () => { metrics.queued = items.jobs.filter(j => j.st === 'wait').length; };
  const add = (id, st, activeNode, msg, code) => {
    items.jobs.push({ id, st });
    metrics.stages++;
    requeue();
    emit(activeNode, msg, code);
  };
  const finish = (ids, msg) => {
    ids.forEach(id => { items.jobs.find(j => j.id === id).st = 'done'; });
    requeue();
    emit('cf', msg, 67 );
  };
  emit('deps', `CompletableFuture 教学模型就绪：场景「${sceneTag}」。`, 67 );
  if (scenario === 'chain') {
    emit('deps', '六节点依赖图：A/C/D 互不依赖（层 1 并行）；B = A.thenApply、E = C.thenCombine(D)（层 2）；F = B.thenCombine(E) 收尾（层 3）。若全部串行要 6 层——看编排如何把墙钟压到 3 层。', 36 );
    items.phase = { txt: '① 并行扇出 · 依赖图', st: 'run' };
    add('A', 'run', 'task', '提交任务 A：supplyAsync 立即开工。', 36 );
    add('C', 'run', 'task', '提交任务 C：与 A 互不依赖，同一时钟步内并行执行。', 36 );
    add('D', 'run', 'task', '提交任务 D：A、C、D 三个 future 同时开跑。', 36 );
    emit('pool', 'A/C/D 在 commonPool 的工作线程上并行执行——串行要 3 层的事，并行 1 层完成。', 25 );
    finish(['A', 'C', 'D'], 'A、C、D 同时完成——层 1 结束。');
    metrics.steps = 1;
    emit('deps', '层 1 完成（时钟 1）：A、C、D 的结果同时就绪——3 个节点只花 1 个时钟步。', 25 );
    add('B', 'run', 'deps', 'B = A.thenApply(...)：依赖 A 的结果，串行接续。', 39 );
    add('E', 'run', 'deps', 'E = C.thenCombine(D)：等 C、D 都完成再汇合——扇出之后的汇合点。', 40 );
    emit('pool', 'B 与 E 同属层 2：两个回调并行执行，互不等待。', 25 );
    finish(['B', 'E'], 'B、E 完成——层 2 结束。');
    metrics.steps = 2;
    emit('deps', '层 2 完成（时钟 2）：B 拿到 A 的产物继续加工；E 把 C、D 合并——两个层 2 节点也在同一时钟步内并行完成。', 25 );
    add('F', 'run', 'deps', 'F = B.thenCombine(E)：两条支路的结果在终点汇合。', 40 );
    finish(['F'], 'F 完成——全链出终值。');
    metrics.steps = 3;
    items.value = { t: 'F ✓ 全链终值' };
    emit('result', '层 3 完成（时钟 3）：F 汇合 B、E 出终值——6 个节点、3 个时钟步。', 42 );
    emit('stats', `运行结束：节点 ${metrics.stages} · 时钟 ${metrics.steps} 层（串行对照 6 层）· 异常 0 · 兜底 0 · 跳过 0——依赖图决定墙钟：能并行的绝不成串。收益来自「谁依赖谁」的声明式描述，而不是手写线程协调。`, 36 );
  } else if (scenario === 'error') {
    emit('deps', '链 A→B→C→D（thenApply 串行接续），B 抛异常：看异常如何沿链传播、下游回调如何被跳过、链尾 exceptionally 如何接管。', 51 );
    items.phase = { txt: '② 异常链 · exceptionally 兜底', st: 'warn' };
    add('A', 'run', 'task', 'A 提交执行。', 51 );
    finish(['A'], 'A 成功。');
    metrics.steps = 1;
    emit('deps', 'A 成功（时钟 1）——链条正常起步。', 25 );
    add('B', 'run', 'deps', 'B = A.thenApply(...)：接续 A 的结果开始执行。', 53 );
    metrics.steps = 2;
    metrics.errors++;
    items.jobs.find(j => j.id === 'B').st = 'error';
    emit('cf', 'B 执行中抛出异常（时钟 2 · 异常 1）——B 以异常完结：这个 future 携带的是异常，不是值。', 52 );
    metrics.skipped++;
    add('C', 'skip', 'deps', 'C 依赖 B 的结果：B 异常完结 → C 的回调体不会执行，直接继承异常（跳过 1）——传播规则：下游 stage 不执行，而不是拿到 null 继续跑。', 52 );
    emit('deps', 'C 跳过：它连执行的机会都没有，整个链以异常往下传。', 52 );
    metrics.skipped++;
    add('D', 'skip', 'deps', 'D 依赖 C → 同样跳过（跳过 2）：下游整条链以异常完结。', 52 );
    items.value = { t: 'fallback · 兜底值' };
    metrics.fallbacks++;
    emit('result', '链尾 exceptionally 接住异常 → 返回兜底值（兜底 1）：异常链被截断、转回正常值——终值 = 兜底值，调用方 join 到的是结果而不是异常。', 56 );
    emit('stats', `运行结束：节点 ${metrics.stages} · 时钟 ${metrics.steps} 层 · 异常 ${metrics.errors} · 跳过 ${metrics.skipped} · 兜底 ${metrics.fallbacks}——B 之后没有一个回调体执行；没有 exceptionally 的链会把异常抛给调用方。传播 ≠ 吞掉，只有异常入口（exceptionally/handle）能截住。`, 51 );
  } else {
    emit('pool', '两幕对照，同一批任务（2 个阻塞型 B + 4 个计算型 C）：先全混进一个 2 线程池，再改成隔离池重演。commonPool 线程数 ≈ CPU−1：阻塞任务占一个，就少一个可算的线程。', 65 );
    items.phase = { txt: '幕 1 · 混池（2 线程）', st: 'warn' };
    add('B1', 'run', 'task', 'B1 提交（阻塞型：模拟 RPC 长调用，线程睡着等响应）→ 占住线程 1。', 67 );
    add('B2', 'run', 'task', 'B2 提交 → 占住线程 2。池已满：两个稀缺线程被「睡着的任务」长期占用。', 67 );
    add('C1', 'wait', 'pool', 'C1 提交（计算型）→ 两个线程都被占 → 排队（排队 1）——它等的不是计算量，是线程。', 65 );
    add('C2', 'wait', 'pool', 'C2 提交 → 继续排队（排队 2）。', 65 );
    add('C3', 'wait', 'pool', 'C3 提交 → 排队（排队 3）。', 65 );
    add('C4', 'wait', 'pool', 'C4 提交 → 排队（排队 4）：4 个计算任务全部被 2 个阻塞任务挡在门外——阻塞传染：一个 RPC 调用 ≈ 少一个 CPU 线程。', 65 );
    items.jobs.find(j => j.id === 'B1').st = 'done';
    items.jobs.find(j => j.id === 'C1').st = 'run';
    requeue();
    metrics.steps = 1;
    emit('pool', 'B1 的 RPC 返回（时钟 1 结束）→ 释放线程 1，C1 入池开跑（排队 3）——计算任务被阻塞任务拖着走。', 65 );
    items.jobs.find(j => j.id === 'B2').st = 'done';
    items.jobs.find(j => j.id === 'C2').st = 'run';
    requeue();
    emit('pool', 'B2 返回 → C2 入池（排队 2）：线程一格一格地释放，排队缓慢消化。', 65 );
    items.jobs.find(j => j.id === 'C1').st = 'done';
    items.jobs.find(j => j.id === 'C2').st = 'done';
    items.jobs.find(j => j.id === 'C3').st = 'run';
    items.jobs.find(j => j.id === 'C4').st = 'run';
    requeue();
    metrics.steps = 2;
    emit('pool', 'C1、C2 完成（时钟 2）→ C3、C4 入池（排队 0）——计算任务总算跑起来了。', 65 );
    items.jobs.find(j => j.id === 'C3').st = 'done';
    items.jobs.find(j => j.id === 'C4').st = 'done';
    requeue();
    metrics.steps = 3;
    emit('pool', 'C3、C4 完成（时钟 3）：混池墙钟 3 层——其中 1 层是等阻塞任务，计算全程被拖着。', 65 );
    items.jobs.length = 0;
    items.phase = { txt: '幕 2 · 隔离池（两池并行）', st: 'ok' };
    emit('pool', '改用隔离池重演同批任务：B1′、B2′ 提交到独立的阻塞任务池；C1′~C4′ 提交到 2 线程计算池——两类任务互不占道。', 80 );
    add('B1′', 'run', 'task', 'B1′ 在独立阻塞池开跑——不占计算线程。', 67 );
    add('B2′', 'run', 'task', 'B2′ 同池开跑，与计算池并行。', 67 );
    add('C1′', 'run', 'pool', 'C1′ 在计算池开跑。', 80 );
    add('C2′', 'run', 'pool', 'C2′ 同池开跑（两池并行推进）。', 80 );
    add('C3′', 'wait', 'pool', 'C3′ 按计算池容量排队（排队 1）——这是池内正常排队，不是被别人挡路。', 80 );
    add('C4′', 'wait', 'pool', 'C4′ 加入排队（排队 2）：阻塞任务 B′ 一直在自己的池里跑，谁也没挡谁。', 80 );
    items.jobs.find(j => j.id === 'C1′').st = 'done';
    items.jobs.find(j => j.id === 'C2′').st = 'done';
    items.jobs.find(j => j.id === 'C3′').st = 'run';
    items.jobs.find(j => j.id === 'C4′').st = 'run';
    requeue();
    metrics.steps = 4;
    emit('pool', 'C1′、C2′ 完成（时钟 4）→ C3′、C4′ 入池（排队 0）：期间 B′ 在独立池里并行跑完。', 80 );
    items.jobs.find(j => j.id === 'C3′').st = 'done';
    items.jobs.find(j => j.id === 'C4′').st = 'done';
    requeue();
    metrics.steps = 5;
    items.value = { t: '全部完成 · 隔离池墙钟 2 层' };
    emit('result', 'C3′、C4′ 完成（时钟 5）：隔离池墙钟 2 层 vs 混池 3 层——同样的任务少花 1 层：阻塞与计算并行，谁也不用等谁。', 80 );
    emit('stats', `运行结束：节点累计 ${metrics.stages}（两幕各 6）· 演示时钟 ${metrics.steps} 层（混池 3 · 隔离 2）· 排队峰值 4 → 结束时 ${metrics.queued}——阻塞传染的解药不是更快的线程，而是按任务性质分池：把「会睡着的任务」与「要算的任务」隔离。`, 67 );
  }
  return frames;
}
function mysqlSharding(p) {
  const scenario = p.scenario || 'shard';
  const sceneTag = { shard: '① 取模分片 · 路由到表', rehash: '② 扩容之痛 · rehash 全量搬', ring: '③ 一致性哈希 · 平滑迁移' }[scenario];
  const frames = [];
  const metrics = { writes: 0, locates: 0, broadcast: 0, total: 0, moves: 0, nodes: 0 };
  const items = { mode: scenario, phase: null, tables: [], oldTables: [], ring: [], moved: [], last: null };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items });
  if (scenario === 'shard') {
    for (let i = 0; i < 8; i++) items.tables.push({ i, keys: [] });
    items.phase = { txt: '逐笔路由写入', st: 'run' };
    emit('app', `分库分表教学模型就绪：场景「${sceneTag}」。`, 47 );
    emit('router', '物理布局：4 库 × 每库 2 表 = 8 张物理表 orders_0 ~ orders_7。槽位 = hash(order_id) % 8；槽 0-1 → db0、2-3 → db1、4-5 → db2、6-7 → db3。', 48 );
    for (let i = 1; i <= 8; i++) {
      const slot = i % 8, db = Math.floor(slot / 2);
      emit('key', `o${i}（order_id=${i}）→ ${i} % 8 = 槽 ${slot}：路由在写之前就算好落点，一条记录只会落到一张表。`, 57 );
      items.tables[slot].keys.push(`o${i}`);
      metrics.writes++;
      metrics.total++;
      items.last = { kind: 'write', text: `✓ o${i} → 槽 ${slot}` };
      emit('dbs', `o${i} 写入 db${db}.orders_${slot}（路由写入 ${metrics.writes} / 数据 ${metrics.total}）——8 张表按哈希摊开，没有热点表。`, 62 );
    }
    items.last = null;
    items.phase = { txt: '带分片键查询 ×2', st: 'ok' };
    emit('app', '两笔带分片键的查询（WHERE order_id = ?）：应用用同一公式反推槽位，只查那一张表——路由的精确性让单表定位成为可能。', 64 );
    emit('key', '查询 order_id=3 → 槽 3 → 只扫 db1.orders_3（单表命中 1）——其余 7 张表完全不碰。', 68 );
    metrics.locates++;
    items.last = { kind: 'locate', text: '定位 order_id=3 · 槽 3' };
    emit('dbs', '查询 order_id=7 → 槽 7 → 只扫 db3.orders_7（单表命中 2）。两次定位都只落在一张表上。', 72 );
    metrics.locates++;
    items.last = { kind: 'locate', text: '定位 order_id=7 · 槽 7' };
    items.last = null;
    items.phase = { txt: '无分片键查询 ×1', st: 'warn' };
    emit('app', '一笔无分片键的查询（WHERE user_id = 9）：user_id 不参与路由——应用不知道数据在哪个槽，广播是唯一的答案。', 74 );
    metrics.broadcast++;
    items.last = { kind: 'broadcast', text: '广播 · orders_0~orders_7 全扫' };
    emit('dbs', '广播查询（广播 1）：orders_0 ~ orders_7 全扫再归并——一次查询被放大成 8 份执行：连接、扫描、归并开销全部 ×表数。', 78 );
    emit('app', `运行结束：路由写入 ${metrics.writes} · 单表命中 ${metrics.locates} · 广播 ${metrics.broadcast}——路由精确性来自分片键：带它只查一张表，不带它就要付全部 8 张表的代价。`, 80 );
  } else if (scenario === 'rehash') {
    const slots4 = [1, 2, 3, 0, 1, 2, 3, 0];
    for (let i = 0; i < 4; i++) items.tables.push({ i, keys: [] });
    items.phase = { txt: '现状 · 4 槽分布', st: 'run' };
    emit('app', `分库分表教学模型就绪：场景「${sceneTag}」。`, 86 );
    emit('router', '现状：取模数 = 4，orders_0 ~ orders_3 四张表。8 笔历史订单 o1 ~ o8（order_id=1~8）按 %4 分布。', 87 );
    for (let i = 1; i <= 8; i++) {
      items.tables[slots4[i - 1]].keys.push(`o${i}`);
      metrics.total++;
      emit('dbs', `o${i}（id=${i}）→ %4 = 槽 ${slots4[i - 1]} → orders_${slots4[i - 1]}（数据 ${metrics.total}/8）。`, 98 );
    }
    items.phase = { txt: '旧表作废', st: 'warn' };
    items.tables.forEach(t => { t.retired = true; });
    items.last = { kind: 'retire', text: '4 张旧表整体作废' };
    emit('mig', '扩容：4 槽 → 8 槽，取模数 4 → 8。对取模分片这不是「加一张表」——每行数据的新槽位都要按 %8 重算，4 张旧表整体作废。', 101 );
    emit('mig', '迁移窗口新老路由并存：要么应用双写、要么停写——数据量越大窗口越长，这是取模扩容最痛的代价。', 104 );
    items.oldTables = items.tables;
    items.tables = [];
    for (let i = 0; i < 8; i++) items.tables.push({ i, keys: [] });
    items.last = null;
    items.phase = { txt: '按 %8 全量重放', st: 'run' };
    for (let i = 1; i <= 8; i++) {
      const to = i % 8;
      items.tables[to].keys.push(`o${i}`);
      metrics.writes++;
      metrics.moves++;
      items.last = { kind: 'replay', text: i === 4 ? 'o4：旧槽 0 → 新槽 4' : i === 8 ? 'o8：旧槽 0 → 新槽 0' : `o${i}：旧槽 ${slots4[i - 1]} → 新槽 ${to}` };
      emit('dbs', `o${i} 按新模数重放：%4 旧槽 ${slots4[i - 1]} → %8 新槽 ${to}，写入重建后的 orders_${to}（搬移 ${metrics.moves}）——槽号看似没变的 o8 也进了物理上的新表：旧表整体作废，没有哪行能留在原地。`, 114 );
    }
    emit('mig', `运行结束：数据 ${metrics.total} · 搬移 ${metrics.moves}（100%）——取模扩容没有「只搬一部分」：模数变了，历史行全部重算重放。`, 116 );
  } else {
    items.ring = [0, 4, 8, 12].map((pos, n) => ({ id: `N${n + 1}`, pos, keys: [], born: false }));
    metrics.total = 16;
    metrics.nodes = items.ring.length;
    items.phase = { txt: '哈希环 · 4 节点均衡', st: 'run' };
    emit('app', `分库分表教学模型就绪：场景「${sceneTag}」。`, 123 );
    emit('router', '哈希环就绪：环上 16 个位置（pos 0~15），节点 N1~N4 摆位 pos 0/4/8/12——key 哈希后沿环顺时针找第一个节点。o1~o16 的哈希 = id % 16，正好铺满全环。', 126 );
    for (let i = 1; i <= 16; i++) {
      const kp = i % 16;
      const own = [0, 4, 8, 12].find(np => np >= kp);
      items.ring.find(n => n.pos === (own === undefined ? 0 : own)).keys.push(`o${i}`);
    }
    items.ring.forEach(node => emit('dbs', `${node.id}（pos ${node.pos}）← ${node.keys.join('、')}——4 个节点各管 4 个 key，均衡无热点。`, 136 ));
    items.phase = { txt: '新节点 N5 加入', st: 'warn' };
    emit('mig', '容量评估 → 增加一个分片：新节点哈希落在 pos 2（N1@0 与 N2@4 之间的弧段上）。一致性哈希的搬家规则：只有「顺时针遇到的第一个节点从此变成 N5」的那一小段弧上的 key 需要搬家——即 pos 1~2。', 139 );
    metrics.nodes++;
    items.ring.push({ id: 'N5', pos: 2, keys: [], born: true });
    const taken = [];
    const n2 = items.ring.find(n => n.id === 'N2');
    n2.keys = n2.keys.filter(k => {
      const kp = Number(k.slice(1)) % 16;
      if (kp === 1 || kp === 2) { taken.push(k); return false; }
      return true;
    });
    taken.forEach(k => {
      metrics.moves++;
      items.moved.push({ k, from: 'N2', to: 'N5' });
      items.last = { kind: 'move', text: `${k} 迁 N5` };
      emit('mig', `${k}（pos ${Number(k.slice(1)) % 16}）：原归属 N2 → 逆时针区间内出现新节点 N5 → 归属切换（搬移 ${metrics.moves}）——它和同弧段的一小撮 key 是这次扩容的全部代价。`, 148 );
    });
    items.ring.find(n => n.id === 'N5').keys = taken;
    items.last = null;
    emit('mig', '其余 14 个 key 的归属节点没有任何变化——一致性哈希把「扩容搬家」限制在新节点接管的一小段弧上。', 151 );
    items.phase = { txt: '平滑迁移完成', st: 'ok' };
    emit('mig', `运行结束：环节点 ${metrics.nodes} · 数据 ${metrics.total} · 平滑搬移 ${metrics.moves}（12.5%）——同数据量若用取模从 4 槽扩到 5 槽，约一半 key 要重新落位；一致性哈希只动 2/16，迁移可以小步按需做。`, 153 );
  }
  return frames;
}
function mysqlExplain(p) {
  const scenario = p.scenario || 'plan';
  const sceneTag = { plan: '① 读懂执行计划 · type/key/rows', fail: '② 索引失效 · 常见反模式', force: '③ 选错索引 · FORCE 纠正' }[scenario];
  const frames = [];
  const metrics = { plans: 0, const: 0, ref: 0, all: 0, failed: 0, degraded: 0, filesorts: 0, corrected: 0 };
  const items = { mode: scenario, phase: null, plans: [], last: null };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items });
  const add = (sql, type, key, rows, st) => {
    metrics.plans++;
    if (type === 'const') metrics.const++;
    else if (type === 'ref') metrics.ref++;
    else if (type === 'ALL') metrics.all++;
    items.plans.push({ n: metrics.plans, sql, type, key, rows, st });
    return metrics.plans;
  };
  if (scenario === 'plan') {
    items.phase = { txt: '三计划对比 · 1 万行表', st: 'run' };
    emit('opt', `EXPLAIN 教学模型就绪：场景「${sceneTag}」。`, 52 );
    emit('sql', 'orders 表 1 万行，索引：PRIMARY(id) · idx_user(user_id)。对三条 SQL 各 EXPLAIN 一次，读 type / key / rows 三列。', 53 );
    add('WHERE id = 42', 'const', 'PRIMARY', 1, 'ok');
    emit('plan', '计划 1：WHERE id = 42 → type=const · key=PRIMARY · rows=1——主键等值一次定位，最廉价的访问路径（估算 1 行）。', 55 );
    emit('idx', 'const 的底气：PRIMARY 索引树按主键有序，等值查询沿树直落叶子——不需要扫任何别的东西。', 57 );
    add('WHERE user_id = 7', 'ref', 'idx_user', 12, 'ok');
    emit('plan', '计划 2：WHERE user_id = 7 → type=ref · key=idx_user · rows=12——二级索引等值命中 12 行，比全表扫少三个数量级。', 59 );
    emit('exec', 'ref 路径：先在 idx_user 树里定位 12 个主键，再回表取 12 行——回表是二级索引的固定动作，胜在只回少数行。', 61 );
    add('WHERE status = 1', 'ALL', 'NULL', 10000, 'fail');
    emit('plan', '计划 3：WHERE status = 1 → type=ALL · key=NULL · rows=10000——status 没有索引，只能全表逐行扫。', 64 );
    emit('exec', '估算量对比：1 vs 12 vs 10000——rows 不是精确值而是代价的度量：优化器的每一次取舍都写在这一列里。', 66 );
    emit('plan', `运行结束：计划 ${metrics.plans}（const ${metrics.const} · ref ${metrics.ref} · ALL ${metrics.all}）——type 决定访问方式等级，key 指明走的树，rows 标注估算代价：三列合起来就是一条 SQL 的命运。`, 68 );
  } else if (scenario === 'fail') {
    items.phase = { txt: '三条反模式逐一 EXPLAIN', st: 'run' };
    emit('opt', `EXPLAIN 教学模型就绪：场景「${sceneTag}」。`, 76 );
    emit('sql', '三条「人眼看着没问题」的 SQL，对 idx_user（user_id）与复合索引 idx_area_user(area, user_id) 各制造一次失效。', 77 );
    emit('sql', '反模式 1：WHERE user_id + 1 = 8——为了查询把列包进了算术表达式。', 79 );
    add('WHERE user_id + 1 = 8', 'ALL', 'NULL', 10000, 'fail');
    metrics.failed++;
    emit('opt', '优化器想用 idx_user 却无从下手：索引树按「列值本身」有序，而比较对象是 user_id+1 的运算结果——树的无序性让定位失效 → type=ALL（失效 1）。', 81 );
    emit('exec', '失效的代价是 10000 行逐行算 user_id+1 再比对；正确写法是把运算挪到等号另一边：WHERE user_id = 7。', 83 );
    emit('sql', '反模式 2：WHERE phone = 13812340000——varchar 列与数字直接比大小。', 84 );
    add('WHERE phone = 13812340000', 'ALL', 'NULL', 10000, 'fail');
    metrics.failed++;
    emit('opt', 'phone 是 varchar 而条件给的是数字：比较前要隐式 CAST——索引树里存的是字符串，CAST 后的值与树序对不上 → 整列隐式转换，索引再次失效（失效 2）。', 86 );
    emit('exec', '两条反模式殊途同归：ALL + rows=10000。索引能否生效取决于「比较能否直接在树上定位」，任何绕过列值的写法都在杀死索引。', 88 );
    emit('sql', '反模式 3：复合索引 idx_area_user(area, user_id)，却只按 user_id 查。', 91 );
    add('WHERE user_id = 7 · 仅后列', 'index', 'idx_area_user', 10000, 'warn');
    metrics.degraded++;
    emit('opt', '复合索引先按前导列 area 排序：缺了前导列，无法按前缀定位——只能从树头到尾扫整棵索引树：type=index（退化 1）。', 94 );
    emit('idx', 'type=index ≠ 全表扫（ALL），但也远差于定位：它遍历整棵 B+ 树把 1 万行的 user_id 全读出来过滤——比全表扫少一次回表，仅此而已。', 96 );
    emit('plan', `运行结束：计划 ${metrics.plans}（索引失效 ${metrics.failed} · 退化全索引扫 ${metrics.degraded}）——失效的三个机理各不相同（包裹 / 转换 / 前导列缺失），下场却同样昂贵：type 每退一档，都要有可解释的原因。`, 98 );
  } else {
    items.phase = { txt: '默认计划 · 带 filesort', st: 'warn' };
    emit('opt', `EXPLAIN 教学模型就绪：场景「${sceneTag}」。`, 105 );
    emit('sql', '慢 SQL：SELECT * FROM orders WHERE user_id IN (…) ORDER BY created_at DESC LIMIT 20——user_id 过滤强，但还要按 created_at 排序。', 106 );
    emit('opt', '优化器按统计信息估代价：走 idx_user 过滤到 400 行，但 ORDER BY created_at 与 idx_user 的树序无关 → 400 行要文件排序（filesort），代价不低。', 110 );
    add('user_id IN (…) ORDER BY created_at', 'ref', 'idx_user', 400, 'warn');
    metrics.filesorts++;
    emit('plan', '计划 1：type=ref · key=idx_user · rows=400 + filesort——纸面过滤最优，实际要额外排序（文件排序 1）。', 112 );
    emit('exec', 'filesort 的真相：MySQL 先把 400 行捞进 sort_buffer 排序再取前 20——数据量大时 spill 到磁盘，代价可能超过多扫几行索引。', 114 );
    items.phase = { txt: 'FORCE INDEX 纠正', st: 'ok' };
    emit('sql', 'DBA 复查：created_at 上有 idx_created——按它顺序扫，ORDER BY 直接免排序，LIMIT 20 还能提前终止。', 116 );
    add('FORCE INDEX(idx_created) · 同 SQL', 'range', 'idx_created', 20, 'ok');
    metrics.corrected++;
    emit('plan', '计划 2：type=range · key=idx_created · rows=20 · 无 filesort——按 created_at 树序扫到 20 行即停（纠正后改善 1）。', 119 );
    emit('opt', '对比计划 1 与 2：idx_created 单看 rows 过滤更弱，但「免排序 + 提前终止」的总代价更低——FORCE INDEX 把统计信息可能算错的那笔账，用人的验证覆盖掉。', 121 );
    emit('plan', `运行结束：计划 ${metrics.plans}（文件排序 ${metrics.filesorts} · 纠正后改善 ${metrics.corrected}）——优化器选错不丢人，丢人的是不用 EXPLAIN 对比就迷信 FORCE。`, 123 );
  }
  return frames;
}
function esQuery(p) {
  const scenario = p.scenario || 'bm25';
  const sceneTag = { bm25: '① BM25 打分 · 词频/稀有度/长度', query: '② 查询语义 · match/term/短语', rank: '③ 排序干预 · boost 与 function_score' }[scenario];
  const frames = [];
  const metrics = { docs: 0, scored: 0, hits: 0, saturated: 0, rare: 0, queries: 0, matchHits: 0, termHits: 0, phraseHits: 0, boosted: 0, rescored: 0 };
  const items = { mode: scenario, phase: null, q: null, cur: [], chips: [], results: [], docs: [] };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items });
  emit('score', `ES 查询与打分教学模型就绪：场景「${sceneTag}」。`, 0);
  if (scenario === 'bm25') {
    items.phase = { txt: '语料就绪 · 查询 snow', st: 'run' };
    metrics.docs = 4;
    emit('query', '语料 4 篇：d1 长文（约 40 词）、d2 短文（8 词）、d3 短文（6 词）、d4 与 snow 无关。第一轮查询 match "snow"——BM25 对每篇文档逐词项打分。', 25 );
    metrics.queries++;
    items.q = { t: 'match "snow"', st: 'ok' };
    emit('analy', 'snow 在 d1/d2/d3 各命中不同次数，d4 无命中——看分数如何被词频、稀有度与文档长度三股力量掰开。', 25 );
    items.cur.push({ id: 'd1', t: 'd1 · 1.1', st: 'ok', title: 'd1 长文命中 snow×1：长度归一摊薄词频——40 词里出现 1 次，密度低，得分被压到 1.1' });
    metrics.scored++;
    metrics.hits++;
    emit('score', 'd1（长文）命中 1 次 → 1.1 分：长度归一生效——同样命中一次，出现在长文里不如出现在短文里值钱（打分 1）。', 25 );
    items.cur.push({ id: 'd2', t: 'd2 · 1.8', st: 'ok', title: 'd2 短文命中 snow×1：短文词频密度高 → 1.8 > 长文的 1.1' });
    metrics.scored++;
    metrics.hits++;
    emit('score', 'd2（短文）同命中 1 次 → 1.8 分：同样的 1 次命中，短文 > 长文——长度归一让短文档的同频命中更值钱（打分 2）。', 25 );
    items.cur.push({ id: 'd3', t: 'd3 · 2.4', st: 'ok', title: 'd3 命中 snow×3：词频饱和——3 次 = 2.4，远不到 1.8×3 = 5.4' });
    metrics.scored++;
    metrics.hits++;
    metrics.saturated++;
    items.chips.push({ t: '词频饱和：3 次 = 2.4 ≪ 3 倍单次分 5.4', st: 'warn' });
    emit('score', 'd3 命中 3 次 → 2.4 分：词频饱和生效——堆 3 次词频只换来 1.3 倍的分数，远低于「1 次分 ×3 = 5.4」（打分 3 · 饱和 1）：防止堆词刷分的长文靠这个机制被压制。', 25 );
    items.cur.push({ id: 'd4', t: 'd4 · 0', st: 'bad', title: 'd4 不含 snow：无命中 0 分' });
    metrics.scored++;
    emit('score', 'd4 无 snow → 0 分（打分 4）。snow 查询汇总：打分 4 篇 · 命中 3 篇——排序：d3 2.4 > d2 1.8 > d1 1.1。', 25 );
    emit('fields', 'query 1 完成：命中 {d1, d2, d3}。但 BM25 还有第三股力量没登场——稀有度。', 25 );
    items.phase = { txt: '稀有词对照 · zebra', st: 'run' };
    metrics.queries++;
    items.q = { t: 'match "zebra" · 稀有词对照', st: 'rare' };
    emit('query', 'query 2：换稀有词 zebra——4 篇语料里只有 d1 包含它（IDF = log(4/1)），而 snow 出现在 3 篇（IDF = log(4/3)）：词越稀有，命中它的辨识度越高。', 25 );
    metrics.rare++;
    items.cur = [{ id: 'd1', t: 'd1 · zebra ×1 → 3.9', st: 'ok', title: '同一篇 d1、同样命中 1 次：稀有词 zebra 的 IDF 加权让它拿到 3.9，远高于常见词 snow 的 1.1' }];
    items.chips.push({ t: '稀有词加权：zebra 同命中 1 次 3.9 > snow 的 1.1（同一篇 d1）', st: 'ok' });
    emit('score', 'zebra 同命中 1 次 → 3.9 分：稀有词加权生效（稀有 1）——对照同一篇 d1：snow 只得 1.1，zebra 却拿 3.9：IDF 衡量的是辨识度，不是拼写难度。', 25 );
    emit('rank', `运行结束：语料 ${metrics.docs} · 打分 ${metrics.scored} · 命中 ${metrics.hits} · 词频饱和 ${metrics.saturated} · 稀有词加权 ${metrics.rare}——词频饱和压住堆词，长度归一奖励短文，IDF 让稀有词脱颖而出：BM25 的排序是这三股力量的和。`, 25 );
  } else if (scenario === 'query') {
    items.phase = { txt: '三查询语义对照', st: 'run' };
    metrics.docs = 3;
    emit('query', '语料 3 篇：d1「The quick brown fox jumps」· d2「quick fox in a forest」· d3「the fox sits on the brown box」。同一批文档，三种查询，三种语义。', 25 );
    items.q = { t: 'match "quick fox"', st: 'run' };
    metrics.queries++;
    emit('analy', 'match：查询文本先过分析器 → 分词 [quick, fox]，词项之间默认 OR——命中任一单词的文档都算命中。', 25 );
    emit('fields', 'd1 含 quick+fox、d2 含 quick+fox、d3 只含 fox——命中 2 篇：d1、d2。', 25 );
    metrics.matchHits = 2;
    items.results.push({ t: 'match → 2 篇（d1 · d2）', st: 'ok', title: '分词 + OR：命中 quick 或 fox 任一即可，语义最宽松' });
    emit('rank', 'match "quick fox" → 命中 d1、d2（match 2）：分词后的词项并集，最接近「模糊包含」的语义。', 25 );
    items.q = { t: 'term "Quick"', st: 'run' };
    metrics.queries++;
    emit('analy', 'term：不走分析器——拿原文 "Quick" 与索引里的词项做精确匹配。索引里存的是分析器分词后的词项：小写 quick，没有 "Quick"。', 25 );
    emit('fields', '原文 "Quick" vs 词项 quick：大小写不同即不匹配 → 0 命中。人眼觉得该命中的，term 未必认——它从不做分词、不做大小写折叠。', 25 );
    metrics.termHits = 0;
    items.results.push({ t: 'term → 0 篇（原文精确 · 大小写敏感）', st: 'bad', title: 'term 不过分析器：索引里没有 "Quick" 这个词项' });
    emit('rank', 'term "Quick" → 0 命中（term 0）：term 查询是「索引词项 vs 你给的原文」——想命中就得先用 match 走同一套分析器。', 25 );
    items.q = { t: 'match_phrase "brown fox"', st: 'run' };
    metrics.queries++;
    emit('analy', 'match_phrase：在 match 基础上要求词项相邻且顺序一致——"brown fox" 必须是连续的 brown→fox。', 6);
    emit('fields', 'd1「quick brown fox」：brown 紧邻 fox 且顺序一致 → 命中；d3「fox … brown box」：fox 在前 brown 在后、还隔着单词 → 不命中。', 6);
    metrics.phraseHits = 1;
    items.results.push({ t: 'match_phrase → 1 篇（d1 · 词序相邻）', st: 'ok', title: '词项相邻 + 顺序一致：最接近「短语」语义' });
    emit('rank', 'match_phrase "brown fox" → 命中 d1（phrase 1）：词序与相邻性是它和 match 的分水岭——d3 两个词都在，但顺序与距离都不对。', 6);
    emit('rank', `运行结束：查询 ${metrics.queries}（match 命中 ${metrics.matchHits} · term 命中 ${metrics.termHits} · phrase 命中 ${metrics.phraseHits}）——同一批文档三种查询三种结果：先分词再查是 match 家族，term 是原文精确匹配，phrase 再压一层词序约束。`, 25 );
  } else {
    items.phase = { txt: '基础排序 · 相近得分', st: 'run' };
    metrics.docs = 3;
    metrics.queries++;
    items.q = { t: 'match "java" · 基础分', st: 'run' };
    items.docs = [{ id: 'd1', t: 'd1 · 2.00', st: 'ok', note: '标题含 java（title 命中 + body 命中）' }, { id: 'd2', t: 'd2 · 1.90', st: 'ok', note: '正文含 java' }, { id: 'd3', t: 'd3 · 1.85', st: 'ok', note: '正文含 java · 高点击' }];
    emit('query', '语料 3 篇都含 java：纯文本相关性的基础分非常接近（1.85 ~ 2.00）——业务上我们希望「标题命中」与「高点击」能改写这个排序。', 0);
    emit('rank', '基础排序：d1 2.00 > d2 1.90 > d3 1.85——纯 BM25 分不出业务轻重。', 2 );
    items.q = { t: 'title^3 · 字段加权', st: 'run' };
    metrics.queries++;
    emit('fields', '给 title 字段加 boost 3（title^3）：命中 title 的字段得分 ×3——把「标题更重要」的业务判断写进基础打分。', 7 );
    metrics.boosted++;
    items.docs[0].t = 'd1 · 5.00 · 升首';
    items.docs[0].st = 'ok';
    items.chips.push({ t: 'title^3：d1 命中 title → 得分 ×3 → 5.00 升至榜首', st: 'ok' });
    emit('rank', 'd1 因 title 命中被 ×3：5.00 稳居第一（字段加权 1）——boost 是查询期静态干预：权重写死在查询里，人人同权。', 7 );
    items.q = { t: 'function_score · 点击率折算', st: 'run' };
    metrics.queries++;
    emit('fields', '再叠加 function_score：把 d3 的点击率折算成业务分 +0.5——高点击的文档虽然文本相关稍弱，但用户用脚投票证明了它更值得看。', 8 );
    metrics.rescored++;
    items.docs[2].t = 'd3 · 2.35 · 越位';
    items.docs[2].st = 'ok';
    items.chips.push({ t: 'function_score：点击率 +0.5 → d3 2.35 越过 d2 1.90', st: 'ok' });
    emit('rank', 'd3：1.85 + 0.5 = 2.35，越过 d2（业务分干预 1）——function_score 是查询期动态干预：把实时业务指标折算成分数参与排序。', 8 );
    emit('rank', `运行结束：语料 ${metrics.docs} · 字段加权 ${metrics.boosted} · 业务分干预 ${metrics.rescored}——最终排序 d1 5.00 > d3 2.35 > d2 1.90：BM25 决定「自然」，boost 与 function_score 决定「业务」，两层干预各管一件事。`, 0);
  }
  return frames;
}
function kafkaConsumer(p) {
  const scenario = p.scenario || 'assign';
  const sceneTag = { assign: '① 组内分配 · 分区归属', rebalance: '② Rebalance · eager 全组暂停', coop: '③ 增量协作 · cooperative 交接' }[scenario];
  const frames = [];
  const metrics = { joins: 0, assigned: 0, consumed: 0, rebalances: 0, revoked: 0, handed: 0 };
  const items = { mode: scenario, phase: null, members: [], flash: null };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items });
  const flash = (t, st) => { items.flash = { t, st }; };
  const setParts = (id, parts, st) => { const m = items.members.find(x => x.id === id); m.parts = parts; m.st = st; };
  const join = id => {
    metrics.joins++;
    items.members.push({ id, parts: '', st: 'run' });
    emit('client', `${id} 用同一 group.id 向协调器发起 JoinGroup（加入 ${metrics.joins}/4）。`, 1);
  };
  emit('client', `Kafka 消费组教学模型就绪：场景「${sceneTag}」。`, 0);
  if (scenario === 'assign') {
    items.phase = { txt: 'join 消费组', st: 'run' };
    emit('coord', 'topic orders：P0~P5 共 6 个分区。4 个消费者共用同一 group.id 加入后，协调器按 range 策略把分区摊给成员——分配结果就是每个成员的「领地」。', 0);
    join('C1'); join('C2'); join('C3'); join('C4');
    items.members.forEach(m => { m.st = 'ok'; });
    items.phase = { txt: 'range 分配', st: 'run' };
    for (const [id, parts, n] of [['C1', 'P0·P1', 2], ['C2', 'P2·P3', 2], ['C3', 'P4', 1], ['C4', 'P5', 1]]) {
      metrics.assigned += n;
      setParts(id, parts, 'ok');
      flash(`${id} ← ${parts}`, 'ok');
      emit('assign', `${id} ← ${parts}：range 按成员顺序把连续分区切成段（归属 ${metrics.assigned}/6）——每个分区同一时刻只归一名成员。`, 2);
    }
    items.phase = { txt: '组内唯一消费', st: 'run' };
    for (const [msg, part, owner] of [['m0', 'P0', 'C1'], ['m1', 'P1', 'C1'], ['m2', 'P2', 'C2'], ['m3', 'P3', 'C2'], ['m4', 'P4', 'C3'], ['m5', 'P5', 'C4']]) {
      metrics.consumed++;
      flash(`${msg} · ${part} → ${owner}`, 'ok');
      emit('parts', `${msg} 投到 ${part}，${owner} poll 拉取——分区唯一归属让这条消息组内只被 ${owner} 消费（消费 ${metrics.consumed}/6）。`, 3);
    }
    emit('client', `运行结束：加入 ${metrics.joins} · 归属 ${metrics.assigned} · 消费 ${metrics.consumed}——6 条消息 × 6 分区一一对应：组内唯一消费 = 每把分区钥匙只配一名成员；成员再多，钥匙只有 6 把。`, 0);
  } else if (scenario === 'rebalance') {
    items.phase = { txt: 'join 消费组', st: 'run' };
    emit('coord', '同样的 4 成员 6 分区。这版的关键：成员不会永远健康——心跳超时就会触发全组再平衡。', 0);
    join('C1'); join('C2'); join('C3'); join('C4');
    items.members.forEach(m => { m.st = 'ok'; });
    items.phase = { txt: 'range 分配', st: 'run' };
    for (const [id, parts, n] of [['C1', 'P0·P1', 2], ['C2', 'P2·P3', 2], ['C3', 'P4', 1], ['C4', 'P5', 1]]) {
      metrics.assigned += n;
      setParts(id, parts, 'ok');
      flash(`${id} ← ${parts}`, 'ok');
      emit('assign', `${id} ← ${parts}（归属 ${metrics.assigned}/6）。`, 2);
    }
    items.phase = { txt: '消费进行中', st: 'run' };
    for (const [msg, part, owner] of [['m0', 'P0', 'C1'], ['m1', 'P1', 'C1'], ['m2', 'P2', 'C2'], ['m3', 'P3', 'C2']]) {
      metrics.consumed++;
      flash(`${msg} · ${part} → ${owner}`, 'ok');
      emit('parts', `${msg} 在 ${part} 由 ${owner} 消费（消费 ${metrics.consumed}/8）。`, 3);
    }
    items.phase = { txt: '心跳超时 · 判定死亡', st: 'warn' };
    flash('C3 心跳超时', 'bad');
    emit('watch', 'C3 的心跳窗口耗尽——死亡没有「通知」，只有协调器侧的超时判定：成员管理靠心跳、不靠握手。', 4);
    metrics.rebalances++;
    setParts('C3', '', 'dead');
    flash('rebalance #1 · generation 0 → 1', 'warn');
    emit('coord', '协调器触发 rebalance：generation 0→1（代际 +1，旧代的消费进度作废）。eager 协议：先全体撤销、再重新分配——这就是 Stop-The-World。', 5);
    items.phase = { txt: 'eager · 全体撤销', st: 'bad' };
    for (const [id, parts, n] of [['C1', 'P0·P1', 2], ['C2', 'P2·P3', 2], ['C4', 'P5', 1]]) {
      metrics.revoked += n;
      setParts(id, '', 'revoked');
      flash(`撤销 ${id} · ${parts}`, 'bad');
      emit('client', `${id} 撤销持有的 ${parts}、暂停消费（撤销 ${metrics.revoked}/6）——eager 的规则：谁也别想留着分区继续跑。`, 5);
    }
    metrics.revoked++;
    flash('C3 的 P4 由协调器回收', 'bad');
    emit('coord', 'C3 已死亡：它持有的 P4 由协调器收回（撤销 6/6）——6 个分区全部回到无主状态，组内消费整体停摆。', 5);
    items.phase = { txt: '新代际重分配', st: 'run' };
    for (const [id, parts] of [['C1', 'P0·P4'], ['C2', 'P2·P3'], ['C4', 'P1·P5']]) {
      metrics.assigned += 2;
      setParts(id, parts, 'ok');
      flash(`${id} ← ${parts}`, 'ok');
      emit('assign', `新代际分配：${id} ← ${parts}（归属 ${metrics.assigned}/12）——幸存的三名成员每人 2 个分区。`, 2);
    }
    items.phase = { txt: '恢复消费', st: 'run' };
    for (const [msg, part, owner] of [['m4', 'P4', 'C1'], ['m5', 'P5', 'C4'], ['m6', 'P2', 'C2'], ['m7', 'P3', 'C2']]) {
      metrics.consumed++;
      flash(`${msg} · ${part} → ${owner}`, 'ok');
      emit('parts', `${msg} 在 ${part} 由 ${owner} 消费（消费 ${metrics.consumed}/8）——重平衡结束后，消费从新归属继续。`, 3);
    }
    emit('client', `运行结束：加入 ${metrics.joins} · 归属 ${metrics.assigned} · 消费 ${metrics.consumed} · 重平衡 ${metrics.rebalances} · 撤销 ${metrics.revoked}——eager 的账：一次成员死亡 = 全组 6 个分区集体暂停一轮。`, 0);
  } else {
    items.phase = { txt: 'join 消费组', st: 'run' };
    emit('coord', '4 成员 6 分区、初始分配与场景 ② 相同——但消费组启用了 cooperative-sticky：重平衡不再要求全体撤销。', 0);
    join('C1'); join('C2'); join('C3'); join('C4');
    items.members.forEach(m => { m.st = 'ok'; });
    items.phase = { txt: 'range 分配', st: 'run' };
    for (const [id, parts, n] of [['C1', 'P0·P1', 2], ['C2', 'P2·P3', 2], ['C3', 'P4', 1], ['C4', 'P5', 1]]) {
      metrics.assigned += n;
      setParts(id, parts, 'ok');
      flash(`${id} ← ${parts}`, 'ok');
      emit('assign', `${id} ← ${parts}（归属 ${metrics.assigned}/6）。`, 2);
    }
    items.phase = { txt: '消费进行中', st: 'run' };
    for (const [msg, part, owner] of [['m0', 'P0', 'C1'], ['m1', 'P1', 'C1'], ['m2', 'P2', 'C2']]) {
      metrics.consumed++;
      flash(`${msg} · ${part} → ${owner}`, 'ok');
      emit('parts', `${msg} 在 ${part} 由 ${owner} 消费（消费 ${metrics.consumed}/6）。`, 3);
    }
    items.phase = { txt: '成员主动离组', st: 'warn' };
    flash('C2 LeaveGroup', 'bad');
    emit('watch', 'C2 主动离组：先 commitSync 提交消费进度、再 LeaveGroup——优雅离组把「是否需要再平衡」的决定权交给协调器。', 7);
    metrics.rebalances++;
    setParts('C2', '', 'left');
    flash('rebalance #1 · generation 0 → 1', 'warn');
    emit('coord', '协调器代际 +1。cooperative 的规则：只撤销「必须移交」的分区——C2 走了，它持有的 P2、P3 需要过户；其他成员的领地原封不动。', 6);
    items.phase = { txt: '增量撤销与交接', st: 'run' };
    metrics.revoked += 2;
    flash('C2 撤销 P2·P3', 'bad');
    emit('client', 'C2 撤销 P2·P3（撤销 2）——这是本次重平衡唯一的停摆面：只有这两个分区短暂暂停。', 6);
    metrics.handed++;
    setParts('C1', 'P0·P1·P2', 'ok');
    flash('P2 → C1', 'ok');
    emit('assign', 'P2 当场过户给 C1（交接 1）——C1 原有的 P0·P1 原地续跑，没有一秒停顿。', 6);
    metrics.handed++;
    setParts('C4', 'P5·P3', 'ok');
    flash('P3 → C4', 'ok');
    emit('assign', 'P3 过户给 C4（交接 2）——C3 的 P4 全程未动：消费不中断的重平衡，代价只有两个分区的过户窗口。', 6);
    items.phase = { txt: '消费不中断', st: 'run' };
    for (const [msg, part, owner] of [['m3', 'P3', 'C4'], ['m4', 'P4', 'C3'], ['m5', 'P5', 'C4']]) {
      metrics.consumed++;
      flash(`${msg} · ${part} → ${owner}`, 'ok');
      emit('parts', `${msg} 在 ${part} 由 ${owner} 消费（消费 ${metrics.consumed}/6）——重平衡期间 C1、C3 的消费从未暂停。`, 3);
    }
    emit('client', `运行结束：加入 ${metrics.joins} · 归属 ${metrics.assigned} · 消费 ${metrics.consumed} · 重平衡 ${metrics.rebalances} · 撤销 ${metrics.revoked} · 交接 ${metrics.handed}——cooperative 把「全组暂停」压缩成「两分区过户」：eager 为一致性付全体代价，cooperative 只为移交付局部代价。`, 0);
  }
  return frames;
}
function kafkaStorage(p) {
  const scenario = p.scenario || 'log';
  const sceneTag = { log: '① 分区与段 · 稀疏索引二分', zerocopy: '② 零拷贝 · 拷贝次数对照', roll: '③ 段滚动 · 删除与回收' }[scenario];
  const frames = [];
  const metrics = { writes: 0, found: 0, copied: 0, direct: 0, rolled: 0, deleted: 0, freed: 0 };
  const items = { mode: scenario, phase: null, segs: [], flash: null, chips: [] };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items });
  const flash = (t, st) => { items.flash = { t, st }; };
  const chip = (t, st) => items.chips.push({ t, st });
  emit('log', `Kafka 日志存储教学模型就绪：场景「${sceneTag}」。`, 0);
  if (scenario === 'log') {
    items.phase = { txt: '顺序追加 · 4 条', st: 'run' };
    emit('log', '分区目录 logs/orders-0/ 下：00000000000000000000.log 与同名 .index 成对出现。消息只能顺序追加到 .log 尾部——顺序写是 Kafka 吞吐的第一来源。', 0);
    for (let i = 0; i < 4; i++) {
      metrics.writes++;
      flash(`✎ m${i} · offset ${i}`, 'ok');
      emit('produce', `m${i} append：写入 .log 尾部，offset ${i}（写入 ${metrics.writes}/4）——同一分区的写永远是追加，从不在中间改。`, 1);
    }
    items.segs.push({ id: 'S0', range: 'offset 0~3 · 4 条', st: 'ok' });
    items.phase = { txt: '按 offset 读 · 稀疏索引二分', st: 'run' };
    emit('index', '要读 offset 3：.index 是稀疏索引——它不记录每条消息，只存若干 (相对 offset, 物理位置) 对。检索问题 = 二分查找问题。', 2);
    metrics.found++;
    flash('二分定位 → offset 3 · pos 114', 'ok');
    emit('index', '读 offset 3：索引二分命中 (3 → pos 114)，在 .log 内直接 seek 到物理位置读出（二分定位 1/2）——不需要扫段。', 3);
    metrics.found++;
    flash('二分定位 → offset 0 · pos 0', 'ok');
    emit('index', '读 offset 0：二分落到段首 pos 0（二分定位 2/2）——两次读取各付 log₂(4) 次比较，消息再多也只是 log₂ 增长。', 3);
    chip('稀疏索引：读任意 offset 都走二分——索引稀疏换来的代价是 log₂ 级查找', 'ok');
    emit('page', '读出路径：定位后按 pos 读 .log 页——数据进 page cache 后对上层透明：写入与读取共享同一份缓存。', 3);
    emit('log', `运行结束：写入 ${metrics.writes} · 二分定位 ${metrics.found}——.log 顺序写、.index 稀疏二分：段是 Kafka 一切读写的最小组织单位。`, 0);
  } else if (scenario === 'zerocopy') {
    items.phase = { txt: '写入 · 落 page cache', st: 'run' };
    metrics.writes++;
    flash('✎ m0 · 10KB', 'ok');
    emit('produce', '一条 10KB 消息 m0 写入成功——数据落在 page cache（内核页缓存）：磁盘文件的一切读写都经它中转（写入 1/1）。', 1);
    items.phase = { txt: '对照 · 传统 read+write：4 次拷贝', st: 'warn' };
    emit('sock', '消费者要把这条 10KB 从磁盘发到网络。传统做法 read + write：四次搬运，逐次点亮。', 4);
    metrics.copied++;
    flash('拷贝 1/4 · DMA 磁盘 → page cache', 'warn');
    emit('page', '① read：磁盘 .log → page cache——DMA 引擎搬运，不占 CPU（拷贝 1/4）。', 4);
    metrics.copied++;
    flash('拷贝 2/4 · CPU page cache → 用户态', 'warn');
    emit('page', '② read：page cache → 用户态缓冲区——CPU 逐字节复制：同一份数据内核、用户各持一份（拷贝 2/4）。', 4);
    metrics.copied++;
    flash('拷贝 3/4 · CPU 用户态 → socket', 'warn');
    emit('sock', '③ write：用户态 → socket 发送缓冲——第二次 CPU 搬运（拷贝 3/4）：数据在用户态走了一遭又回内核。', 4);
    metrics.copied++;
    flash('拷贝 4/4 · DMA socket → 网卡', 'warn');
    emit('sock', '④ socket 缓冲 → 网卡：DMA 上线（拷贝 4/4）。合计：4 次拷贝 = 2 DMA + 2 CPU——10KB 尚可，换成大消息就是灾难。', 4);
    chip('传统 read+write：4 次拷贝（2 DMA + 2 CPU）——CPU 是这条路最贵的搬运工', 'warn');
    items.phase = { txt: '对照 · sendfile 零拷贝', st: 'run' };
    metrics.direct++;
    flash('零拷贝① · skb 引用页框（0 搬运）', 'ok');
    emit('sock', 'Kafka 服务端改走 sendfile：内核把 page cache 的页框「引用」挂进 socket 的 skb——引用不是复制，这一步 0 字节搬运（直发 1/2）。', 5);
    metrics.direct++;
    flash('零拷贝② · DMA 直发网卡', 'ok');
    emit('page', '网卡 DMA 引擎按页框引用直读 page cache 上线（直发 2/2）——全程 0 次 CPU 拷贝：数据自始至终没有离开内核。', 5);
    chip('sendfile 路径：2 次 DMA · 0 次 CPU——大消息高吞吐的关键是让 CPU 少干活', 'ok');
    emit('log', `运行结束：写入 ${metrics.writes} · 传统拷贝 ${metrics.copied} · 零拷贝直发 ${metrics.direct}——同一条消息两条路：4 次搬运 vs 2 次搬运，省掉的还恰是最贵的 CPU 拷贝。`, 0);
  } else {
    items.phase = { txt: '连续写入 8 条', st: 'run' };
    emit('log', '段容量阈值 = 4 条（教学示意，真实 Kafka 按 1GB 或时间滚动）。先让 S0 写满 4 条，看滚动如何发生。', 6);
    for (let i = 0; i < 4; i++) {
      metrics.writes++;
      flash(`✎ m${i} → S0 · offset ${i}`, 'ok');
      emit('log', `m${i} append：S0 · offset ${i}（写入 ${metrics.writes}/8）。`, 1);
    }
    items.segs.push({ id: 'S0', range: 'offset 0~3', st: 'ro' });
    metrics.writes++;
    metrics.rolled++;
    items.segs.push({ id: 'S1', range: 'offset 4~', st: 'ok' });
    flash('滚动 → 新段 S1 · m4 入 S1', 'warn');
    emit('log', 'm4 append：S0 已满 → 滚动出新段 S1（滚动 1），S0 转只读——m4 落在 S1 · offset 4（写入 5/8）。滚动让「删除」可以退化成「丢文件」。', 6);
    for (let i = 5; i < 8; i++) {
      metrics.writes++;
      flash(`✎ m${i} → S1 · offset ${i}`, 'ok');
      emit('log', `m${i} append：S1 · offset ${i}（写入 ${metrics.writes}/8）。`, 1);
    }
    items.phase = { txt: 'retention · 删除与回收', st: 'warn' };
    metrics.deleted++;
    items.segs[0].st = 'gone';
    flash('删除段 S0', 'bad');
    emit('log', '保留时长到期：S0（最早的段）过期——整体删除文件（删除 1）：Kafka 从不逐条删消息，段过期即删。', 7);
    metrics.freed += 4;
    flash('回收 4 条消息的磁盘空间', 'ok');
    emit('log', '段文件消失 → 4 条消息的空间一次性归还磁盘（回收 4）——「删除 = 丢整段」：没有碎片化，只有整段消失。', 7);
    emit('log', `运行结束：写入 ${metrics.writes} · 滚动 ${metrics.rolled} · 删除 ${metrics.deleted} · 回收 ${metrics.freed}——滚动把删除的粒度从「条」放大到「段」：回收与碎片化一并解决。`, 0);
  }
  return frames;
}
function rabbitmqCluster(p) {
  const scenario = p.scenario || 'topology';
  const sceneTag = { topology: '① 集群拓扑 · 跨节点路由', failover: '② 宕机对照 · 镜像接管', quorum: '③ quorum 队列 · 多数派语义' }[scenario];
  const frames = [];
  const metrics = { routed: 0, written: 0, consumed: 0, unavailable: 0, promoted: 0, acks: 0 };
  const items = { mode: scenario, phase: null, brokers: [], queues: [], flash: null };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items });
  const flash = (t, st) => { items.flash = { t, st }; };
  const broker = id => items.brokers.find(b => b.id === id);
  const setRole = (id, role, st) => { const b = broker(id); b.role = role; b.st = st; };
  const queue = id => items.queues.find(q => q.id === id);
  emit('cluster', `RabbitMQ 集群教学模型就绪：场景「${sceneTag}」。`, 0);
  if (scenario === 'topology') {
    items.brokers.push({ id: 'B1', role: '种子节点', st: 'ok' });
    items.phase = { txt: '组建集群', st: 'run' };
    emit('cluster', 'B1 先启动：共享 cookie 与集群名就位，成为种子节点。', 1);
    items.brokers.push({ id: 'B2', role: '节点', st: 'ok' });
    emit('cluster', 'B2 执行 join_cluster rabbit@B1 加入——元数据（交换机/队列/绑定）全网一致，任意节点都能回答「队列在哪」。', 1);
    items.brokers.push({ id: 'B3', role: '节点', st: 'ok' });
    emit('cluster', 'B3 同样加入。3 节点就绪——注意：数据的分布并不均匀：普通队列只活在声明它的节点。', 1);
    items.phase = { txt: '声明队列 · 宿主节点', st: 'run' };
    items.queues.push({ id: 'Q1', kind: '普通', loc: '宿主 B2', msgs: [], st: 'ok' });
    flash('Q1 声明于 B2', 'ok');
    emit('queues', 'Q1 声明在 B2：普通队列的数据只存在于宿主节点 B2——这是普通队列的核心性质。', 2);
    items.queues.push({ id: 'Q2', kind: '普通', loc: '宿主 B3', msgs: [], st: 'ok' });
    flash('Q2 声明于 B3', 'ok');
    emit('queues', 'Q2 声明在 B3：同理，数据只活在自己的宿主。客户端连哪台都能发——因为元数据全网共享。', 2);
    items.phase = { txt: '跨节点路由', st: 'run' };
    emit('client', '客户端这次连的是 B1——但 m1 的目标 Q1 在 B2：集群内路由会完成这次「跨界投递」。', 2);
    metrics.routed++;
    flash('m1 → 路由转发到 Q1 @ B2', 'ok');
    emit('route', 'B1 收到 m1：按元数据定位 Q1 宿主 = B2 → 集群内转发（路由 1/2）——「连错节点」在集群里不是错误。', 2);
    metrics.written++;
    queue('Q1').msgs.push('m1');
    flash('✓ Q1（宿主 B2）收到 m1', 'ok');
    emit('queues', 'Q1 在宿主 B2 追加 m1（写入 1/2）——数据落在 B2，与客户端连在哪儿无关。', 0);
    metrics.consumed++;
    flash('消费端从 Q1 取走 m1', 'ok');
    emit('client', '消费端（连接 B2）从 Q1 取走 m1（消费 1/2）——跨节点路由到宿主后，就是一次本地出队。', 0);
    emit('client', '同一段旅程再来一次：m2 的目标 Q2，宿主在 B3。', 2);
    metrics.routed++;
    flash('m2 → 路由转发到 Q2 @ B3', 'ok');
    emit('route', 'B1 收到 m2 → 定位 Q2 宿主 B3 → 转发（路由 2/2）。', 2);
    metrics.written++;
    queue('Q2').msgs.push('m2');
    flash('✓ Q2（宿主 B3）收到 m2', 'ok');
    emit('queues', 'Q2 在宿主 B3 追加 m2（写入 2/2）。', 0);
    metrics.consumed++;
    flash('消费端从 Q2 取走 m2', 'ok');
    emit('client', '消费端（连接 B3）从 Q2 取走 m2（消费 2/2）。', 0);
    emit('cluster', `运行结束：路由 ${metrics.routed} · 写入 ${metrics.written} · 消费 ${metrics.consumed}——普通队列 = 数据单点 + 元数据共享：路由层把「队列在哪」翻译成「发去哪」。`, 0);
  } else if (scenario === 'failover') {
    items.brokers.push({ id: 'B1', role: '节点', st: 'ok' }, { id: 'B2', role: '节点', st: 'ok' }, { id: 'B3', role: '节点', st: 'ok' });
    items.phase = { txt: '声明两种队列', st: 'run' };
    items.queues.push({ id: 'Q镜像', kind: '镜像', loc: 'B2 主 · B3 镜像', msgs: [], st: 'ok' });
    flash('Q镜像 声明：master B2 · 镜像 B3', 'ok');
    emit('queues', 'Q镜像 声明在 B2：镜像队列把 master 的每次变更同步给镜像副本 B3——多活一份数据，就多一份可用性预算。', 3);
    items.queues.push({ id: 'Q普通', kind: '普通', loc: '宿主 B2', msgs: [], st: 'ok' });
    flash('Q普通 声明：只存 B2', 'warn');
    emit('queues', 'Q普通 声明在 B2：没有任何副本——对照组，它的命运完全系于 B2 一台机器。', 2);
    items.phase = { txt: '宕机前 · 写入与消费', st: 'run' };
    metrics.written++;
    queue('Q镜像').msgs.push('m1');
    flash('✓ Q镜像（master B2）写入 m1', 'ok');
    emit('queues', 'm1 写入 Q镜像：master B2 落盘并同步镜像 B3（写入 1/3）。', 3);
    metrics.consumed++;
    flash('消费端取走 m1', 'ok');
    emit('client', '消费端从 Q镜像 取走 m1（消费 1/3）。', 0);
    metrics.written++;
    queue('Q镜像').msgs.push('m2');
    flash('✓ Q镜像 写入 m2', 'ok');
    emit('queues', 'm2 写入 Q镜像（写入 2/3）——m2 还没来得及被消费，宕机先到了。', 3);
    items.phase = { txt: 'B2 宕机 · 两种命运', st: 'warn' };
    setRole('B2', '宕机', 'down');
    flash('B2 宕机', 'bad');
    emit('cluster', 'B2 宕机：心跳中断、其余节点感知。对两种队列，这是同一次宕机、两种结局。', 0);
    metrics.unavailable++;
    queue('Q普通').st = 'down';
    flash('✗ Q普通 随宿主失联', 'bad');
    emit('queues', 'Q普通：数据随宿主一起消失——客户端连 B3 也取不到（不可用 1）。普通队列的可用性 = 宿主的可用性。', 2);
    metrics.promoted++;
    setRole('B3', 'Q镜像 新主', 'ok');
    queue('Q镜像').loc = 'B3 主 · B2 宕机';
    flash('✓ 镜像 B3 提升为新 master', 'ok');
    emit('elect', 'Q镜像：master 消失 → 镜像副本 B3 提升为新 master（接管 1）——m1、m2 一条不少地留在 B3。', 3);
    items.phase = { txt: '故障转移 · 恢复服务', st: 'run' };
    emit('client', '客户端故障转移：重连 B3——对业务侧只是换了个连接地址；Q镜像 换了宿主，消息没丢。', 5);
    metrics.written++;
    queue('Q镜像').msgs.push('m3');
    flash('✓ m3 写入新主 B3', 'ok');
    emit('queues', 'm3 → 新 master B3 写入（写入 3/3）：镜像提升后队列继续可用。', 3);
    metrics.consumed++;
    flash('消费端取走 m2', 'ok');
    emit('client', '消费端从提升后的 B3 取走宕机前写入的 m2（消费 2/3）。', 0);
    metrics.consumed++;
    flash('消费端取走 m3', 'ok');
    emit('client', 'm3 也被取走（消费 3/3）——Q镜像 的消费中断只发生在接管的那一瞬间。', 0);
    emit('cluster', `运行结束：写入 ${metrics.written} · 消费 ${metrics.consumed} · 不可用 ${metrics.unavailable} · 接管 ${metrics.promoted}——同一次宕机两种结局：没有副本的 Q普通 失联，有镜像的 Q镜像 只断一瞬。`, 0);
  } else {
    items.brokers.push({ id: 'B1', role: 'Q 副本', st: 'ok' }, { id: 'B2', role: 'Q 副本', st: 'ok' }, { id: 'B3', role: 'Q 副本', st: 'ok' });
    items.phase = { txt: '声明 quorum 队列', st: 'run' };
    items.queues.push({ id: 'Q', kind: 'quorum', loc: 'B1·B2·B3 各持副本', msgs: [], st: 'ok' });
    flash('Q · quorum 队列 · 3 副本', 'ok');
    emit('queues', 'Q 声明为 quorum 队列：队列本身就是 Raft 复制组——B1/B2/B3 各持一份完整副本，没有主从之分，只有多数派。', 7);
    items.phase = { txt: '多数派 ack · 写入', st: 'run' };
    metrics.written++;
    metrics.acks += 2;
    queue('Q').msgs.push('m1');
    flash('✓ m1 提交 · 多数派 ack 2', 'ok');
    emit('elect', '写 m1：leader（B1 上的副本）提案 → B2 ack → 2/3 多数达成 → 提交（写 1/4 · ack 2）——ack 的含义是数据已在多数派，而不是只在主节点。', 6);
    metrics.consumed++;
    flash('消费端取走 m1', 'ok');
    emit('client', '消费端从 Q 取走 m1（消费 1/4）。', 0);
    metrics.written++;
    metrics.acks += 2;
    queue('Q').msgs.push('m2');
    flash('✓ m2 提交 · 多数派 ack 4', 'ok');
    emit('elect', '写 m2：B3 ack → 多数派 → 提交（写 2/4 · ack 4）。', 6);
    metrics.consumed++;
    flash('消费端取走 m2', 'ok');
    emit('client', 'm2 被取走（消费 2/4）。', 0);
    items.phase = { txt: 'B1 宕机 · 多数仍在', st: 'warn' };
    setRole('B1', '宕机', 'down');
    flash('B1 宕机', 'bad');
    emit('cluster', 'B1 宕机：副本组剩 B2 + B3 = 2/3——多数派依然存在：队列不降级、不拒写。', 6);
    flash('副本组自动选主 → B2 接任', 'warn');
    emit('elect', 'quorum 组内部自动选举：B2 接任 leader——这个动作对客户端完全透明，没有不可用窗口。', 4);
    metrics.written++;
    metrics.acks += 2;
    queue('Q').msgs.push('m3');
    flash('✓ m3 提交 · 多数派 ack 6', 'ok');
    emit('elect', '写 m3：新 leader B2 提案 → B3 ack → 提交（写 3/4 · ack 6）——宕机期间写入照常，只是 ack 不再经过 B1。', 6);
    metrics.consumed++;
    flash('消费端取走 m3', 'ok');
    emit('client', 'm3 被取走（消费 3/4）。', 0);
    metrics.written++;
    metrics.acks += 2;
    queue('Q').msgs.push('m4');
    flash('✓ m4 提交 · 多数派 ack 8', 'ok');
    emit('elect', '写 m4（写 4/4 · ack 8）——从 B1 宕机到此刻，Q 的写入与消费从未中断。', 6);
    metrics.consumed++;
    flash('消费端取走 m4', 'ok');
    emit('client', 'm4 被取走（消费 4/4）。', 0);
    emit('cluster', `运行结束：写入 ${metrics.written} · 消费 ${metrics.consumed} · 多数派 ack ${metrics.acks}——每条消息 = 2 个副本 ack（4 条 × 2）；宕机不过半，复制组自动续命。`, 0);
  }
  return frames;
}
function rocketmqDledger(p) {
  const scenario = p.scenario || 'master-slave';
  const sceneTag = { 'master-slave': '① 主从异步 · 丢失窗口', raft: '② DLedger 多数派 · 零丢失', return: '③ 旧主回归 · 日志追平' }[scenario];
  const frames = [];
  const metrics = { writes: 0, synced: 0, lost: 0, failovers: 0, elected: 0, replayed: 0 };
  const items = { mode: scenario, phase: null, roles: [], flash: null, chips: [] };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items });
  const flash = (t, st) => { items.flash = { t, st }; };
  const chip = (t, st) => items.chips.push({ t, st });
  const setRole = (id, role, st) => {
    const existing = items.roles.find(r => r.id === id);
    if (existing) { existing.role = role; existing.st = st; } else { items.roles.push({ id, role, st }); }
  };
  emit('master', `RocketMQ 高可用教学模型就绪：场景「${sceneTag}」。`, 0);
  if (scenario === 'master-slave') {
    setRole('A', '主', 'ok');
    setRole('B', '从', 'ok');
    items.phase = { txt: '异步复制 · 写入与同步', st: 'run' };
    emit('master', '主从模式：BrokerA（主）+ BrokerB（从）。生产者只写 A，A 把 commitlog 异步同步给 B——brokerRole = ASYNC_MASTER：主库写入即返回成功，不等从库。', 1);
    for (let i = 1; i <= 4; i++) {
      metrics.writes++;
      flash(`✎ m${i} → A`, 'ok');
      emit('master', `m${i}：A 写入 commitlog（写 ${metrics.writes}/6）。`, 1);
      metrics.synced++;
      flash(`B ✓ 复制 m${i}`, 'ok');
      emit('slave', `B 复制完成 m${i}（同步 ${metrics.synced}/4）——此刻主从一致。`, 0);
    }
    chip('前 4 条：A/B 完全一致——异步窗口内没有消息', 'ok');
    metrics.writes++;
    flash('✎ m5 → A（ack 即回）', 'ok');
    emit('master', 'm5：A 写入并立刻 ack（写 5/6）——异步复制没有等待 B：m5 进入「同步路上」的窗口。', 1);
    metrics.writes++;
    chip('m5/m6 处在复制延迟窗口：A 已 ack、B 尚未收到——「成功」≠「安全」', 'warn');
    flash('✎ m6 → A（ack 即回）', 'ok');
    emit('master', 'm6 同样 ack（写 6/6）——窗口里的这两条消息，是接下来事故的伏笔。', 1);
    items.phase = { txt: 'A 宕机 · 切换', st: 'warn' };
    setRole('A', '宕机', 'down');
    flash('A 宕机 · 心跳超时', 'bad');
    emit('namesrv', 'A 的心跳超时——NameServer 感知失联：路由表里 A 下线。', 2);
    metrics.failovers++;
    setRole('B', '新主', 'ok');
    flash('B 提升为新主', 'ok');
    emit('namesrv', 'NameServer 把 B 提升为新主（切换 1）——B 的 commitlog 停在「A 消失的那一刻之前」。', 2);
    items.phase = { txt: '读新主 · 丢失窗口', st: 'bad' };
    metrics.lost++;
    flash('✗ m5 缺失', 'bad');
    emit('producer', '客户端从新主 B 拉取历史消息：m5 不在 B 的 commitlog——丢失 1（m5）。', 2);
    metrics.lost++;
    flash('✗ m6 缺失', 'bad');
    emit('producer', 'm6 同样缺失（丢失 2）：它们只存在于 A 已 ack 而未同步的状态，随 A 一起消失。', 2);
    emit('master', `运行结束：写入 ${metrics.writes} · 同步 ${metrics.synced} · 丢失 ${metrics.lost} · 切换 ${metrics.failovers}——丢的正好是「已 ack 未同步」的 2 条：异步复制的 ack 只代表主库收下了。`, 0);
  } else if (scenario === 'raft') {
    setRole('A', 'leader', 'ok');
    setRole('B', 'follower', 'ok');
    setRole('C', 'follower', 'ok');
    items.phase = { txt: '多数派复制 · 三副本', st: 'run' };
    emit('dledger', 'DLedger：A/B/C 三个副本组成 Raft 日志组（dLedgerEnable = true）——每副本一份 commitlog，写请求复制到多数派后才返回成功：没有主从，只有 leader。', 4);
    for (const [i, who] of [[1, 'B'], [2, 'C'], [3, 'B']]) {
      emit('master', `m${i}：leader A 发起复制提案（任期 1）——B、C 并行收到，谁先 ack 都算多数派一票。`, 3);
      metrics.writes++;
      metrics.synced++;
      flash(`✓ m${i} 提交 · A+${who} 多数派`, 'ok');
      emit('dledger', `${who} 先 ack → A+${who} = 2/3 多数 → 提交（写 ${metrics.writes}/5 · 同步 ${metrics.synced}/5）——ack 的条件是数据在多数派，而不是 leader 说了算。`, 4);
    }
    items.phase = { txt: 'A 宕机 · 选举', st: 'warn' };
    setRole('A', '宕机', 'down');
    flash('A 宕机', 'bad');
    emit('dledger', 'A 宕机——B、C 仍在：多数派不散。已提交的 3 条日志完整躺在 B 与 C 上。', 5);
    metrics.elected++;
    setRole('B', 'leader', 'ok');
    flash('B 当选 leader', 'ok');
    emit('dledger', '选举：B、C 的日志长度相同（各 3 条）→ 按任期与日志比较后 B 赢得 C 的选票 → B 当选（选主 1）——已提交消息一条没丢。', 5);
    items.phase = { txt: '新 leader 续写', st: 'run' };
    for (const [i, who] of [[4, 'C'], [5, 'C']]) {
      emit('slave', `m${i}：新 leader B 发起复制提案（任期 2）——复制到 C。`, 3);
      metrics.writes++;
      metrics.synced++;
      flash(`✓ m${i} 提交 · B+${who} 多数派`, 'ok');
      emit('dledger', `${who} ack → B+${who} 多数 → 提交（写 ${metrics.writes}/5 · 同步 ${metrics.synced}/5）。`, 4);
    }
    emit('dledger', `运行结束：写入 ${metrics.writes} · 同步 ${metrics.synced} · 选主 ${metrics.elected} · 丢失 ${metrics.lost}——DLedger 的 ack = 数据已在多数派：宕机换来的是选举，不是丢失。`, 0);
  } else {
    setRole('A', '宕机', 'down');
    setRole('B', 'leader', 'ok');
    setRole('C', 'follower', 'ok');
    items.phase = { txt: 'B 任上写入', st: 'run' };
    emit('dledger', '时间线推进：A 宕机后 B 当选 leader（任期 2），C 为 follower。B 任上先写入 m1、m2——看新 leader 如何提交。', 4);
    for (const [i] of [[1], [2]]) {
      metrics.writes++;
      metrics.synced++;
      flash(`✓ m${i} 提交 · B+C 多数派`, 'ok');
      emit('dledger', `m${i}：B 提案 → C ack → 2/3 提交（写 ${metrics.writes}/4 · 同步 ${metrics.synced}/4）。`, 4);
    }
    items.phase = { txt: '旧主 A 回归', st: 'warn' };
    setRole('A', 'follower · 回归中', 'warn');
    flash('A 复活 · 任期落后', 'warn');
    emit('master', 'A 复活：它的日志停在宕机前（比 B 少 m1、m2 两条）——B 的任期更高：A 没有竞选资格，只能以 follower 身份回归。', 6);
    metrics.replayed++;
    flash('A ← B 追平 m1', 'ok');
    emit('slave', 'A 以 follower 身份向新 leader B 拉取落后日志：m1 追平（日志追平 1/2）。', 6);
    metrics.replayed++;
    setRole('A', 'follower', 'ok');
    flash('A ← B 追平 m2', 'ok');
    emit('slave', 'm2 追平（追平 2/2）——落后多少拉多少：A 重新成为与多数派一致的普通副本，恢复参与复制。', 6);
    items.phase = { txt: '追平后 · 全员参与', st: 'run' };
    for (const [i, who] of [[3, 'A'], [4, 'A']]) {
      metrics.writes++;
      metrics.synced++;
      flash(`✓ m${i} 提交 · B+${who} 多数派`, 'ok');
      emit('dledger', `m${i}：B 提案 → ${who} ack → 提交（写 ${metrics.writes}/4 · 同步 ${metrics.synced}/4）——回归的 A 也能参与多数派了。`, 4);
    }
    emit('dledger', `运行结束：写入 ${metrics.writes} · 同步 ${metrics.synced} · 追平 ${metrics.replayed} · 选主 ${metrics.elected} · 丢失 ${metrics.lost}——旧主回归 = 任期落后 → follower 身份 → 追平日志：DLedger 把「切主」退化成了普通日志复制。`, 0);
  }
  return frames;
}
function zkLock(p) {
  const scenario = p.scenario || 'ephemeral';
  const sceneTag = { ephemeral: '① 临时节点 · 抢占与释放', queue: '② 顺序节点 · 公平排队', watch: '③ 惊群对比 · 精准唤醒' }[scenario];
  const frames = [];
  const metrics = { acquires: 0, releases: 0, queued: 0, woken: 0 };
  const items = { mode: scenario, phase: null, holder: null, waiters: [], queue: [], flash: null };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items });
  const flash = (t, st) => { items.flash = { t, st }; };
  const phase = (txt, st) => { items.phase = { txt, st }; };
  const hold = id => { metrics.acquires++; items.holder = { id, st: 'hold' }; };
  const release = () => { metrics.releases++; items.holder = null; };
  const removeWaiter = id => { items.waiters = items.waiters.filter(w => w.id !== id); };
  const enqueue = (id, owner) => { metrics.queued++; items.queue.push({ id, owner, st: 'ok' }); };
  const dequeue = id => { items.queue = items.queue.filter(q => q.id !== id); };
  emit('clients', `ZooKeeper 分布式锁教学模型就绪：场景「${sceneTag}」。`, 0);
  if (scenario === 'ephemeral') {
    phase('临时节点 · 抢占与释放', 'run');
    emit('clients', 'A、B 争抢同一个锁根 /lock——临时节点：谁 create 成功谁持锁。', 0);
    hold('A');
    flash('✓ A create /lock 成功', 'ok');
    emit('lock', 'A create EPHEMERAL /lock 成功 → 持锁（获取 1/2）。临时节点的生命与会话绑定。', 0);
    emit('clients', 'B 到达，尝试 create /lock——锁已存在。', 1);
    items.waiters.push({ id: 'B', txt: 'watch /lock · NodeDeleted', st: 'wait' });
    emit('watch', 'B create 抛 NodeExists → 抢锁失败，注册一次性 Watch：exists(/lock, true)——等删除事件。', 2);
    release();
    flash('A delete /lock', 'ok');
    emit('ephemeral', 'A 显式 delete /lock（释放 1/2）——服务端向注册者投递 NodeDeleted。', 3);
    metrics.woken++;
    emit('watch', 'B 收到 NodeDeleted（唤醒 1/1）：Watch 是一次性的——事件消费后自动失效，每次等锁都要重注册。', 2);
    removeWaiter('B');
    hold('B');
    flash('✓ B 重抢成功 → 持锁', 'ok');
    emit('lock', 'B 立刻重抢：create 成功 → 持锁（获取 2/2）。', 0);
    phase('会话兜底 · 自动清理', 'warn');
    release();
    flash('B 会话超时 · 服务端清理节点', 'bad');
    emit('session', 'B 会话中断：连接断开不立即清理——服务端判定会话超时后，自动删除其临时节点（释放 2/2）。持锁进程崩溃，锁也能自动让出，不死锁。', 3);
    phase('演示完成', 'ok');
    emit('lock', `运行结束：获取 ${metrics.acquires} · 释放 ${metrics.releases} · 排队 ${metrics.queued} · 唤醒 ${metrics.woken}——显式 delete 与会话超时都会让锁让出：临时节点把锁的生命周期交给会话。`, 0);
  } else if (scenario === 'queue') {
    phase('顺序取号 · 最小号持锁', 'run');
    emit('clients', 'A/B/C 依次 create EPHEMERAL_SEQUENTIAL 取号：最小序号持锁，其余人只 watch 自己的前驱。', 5);
    items.queue.push({ id: 'c-0000', owner: 'A', st: 'ok' });
    hold('A');
    items.queue[items.queue.length - 1].st = 'hold';
    flash('✓ A 取号 c-0000 · 最小号 → 持锁', 'ok');
    emit('queue', 'A create → c-0000（最小号）→ A 持锁（获取 1/3）。', 5);
    enqueue('c-0001', 'B');
    items.waiters.push({ id: 'B', txt: 'watch 前驱 c-0000', st: 'wait' });
    emit('queue', 'B create → c-0001（排队 1/2）。B 不 watch 锁根，只 watch 前驱 c-0000（A 的节点）。', 7);
    enqueue('c-0002', 'C');
    items.waiters.push({ id: 'C', txt: 'watch 前驱 c-0001', st: 'wait' });
    emit('queue', 'C create → c-0002（排队 2/2）。C watch 前驱 c-0001（B 的节点）。', 7);
    dequeue('c-0000');
    release();
    flash('A delete c-0000', 'ok');
    emit('watch', 'A 释放（释放 1/3）：delete c-0000 → 事件只命中 watch 它的人——也就是 B。', 7);
    metrics.woken++;
    removeWaiter('B');
    hold('B');
    const bQueue = items.queue.find(q => q.id === 'c-0001');
    if (bQueue) bQueue.st = 'hold';
    flash('✓ B 被唤醒 · c-0001 最小号 → 持锁', 'ok');
    emit('queue', 'B 收到前驱删除事件（唤醒 1/2）→ c-0001 成为最小序号 → B 持锁（获取 2/3）。', 6);
    dequeue('c-0001');
    release();
    flash('B delete c-0001', 'ok');
    emit('watch', 'B 释放（释放 2/3）：delete c-0001 → 只唤醒 watch 它的 C。', 7);
    metrics.woken++;
    removeWaiter('C');
    hold('C');
    const cQueue = items.queue.find(q => q.id === 'c-0002');
    if (cQueue) cQueue.st = 'hold';
    flash('✓ C 被唤醒 · c-0002 最小号 → 持锁', 'ok');
    emit('queue', 'C 收到事件（唤醒 2/2）→ c-0002 最小号 → C 持锁（获取 3/3）。', 6);
    dequeue('c-0002');
    release();
    flash('C delete c-0002', 'ok');
    emit('watch', 'C 释放（释放 3/3）：后面没有人 watch c-0002——队列清空，无人被唤醒。', 3);
    phase('演示完成', 'ok');
    emit('queue', `运行结束：获取 ${metrics.acquires} · 释放 ${metrics.releases} · 排队 ${metrics.queued} · 唤醒 ${metrics.woken}——唤醒沿队列链单播：delete c-000N 只吵醒 watch 它的后一位，惊群消失。`, 7);
  } else {
    phase('惊群 · 一次唤醒全部', 'run');
    emit('clients', 'A 持普通临时锁；X/Y/Z 加入抢锁，失败者全部 watch 同一个锁节点——先看惊群。', 0);
    hold('A');
    flash('✓ A create /lock 成功 → 持锁', 'ok');
    emit('lock', 'A create 成功 → 持锁（获取 1/4）。', 0);
    items.waiters.push({ id: 'X', txt: 'watch /lock · NodeDeleted', st: 'wait' });
    emit('clients', 'X create → NodeExists：抢锁失败。X 注册 Watch 等删除事件。', 2);
    items.waiters.push({ id: 'Y', txt: 'watch /lock · NodeDeleted', st: 'wait' });
    emit('clients', 'Y create → NodeExists：失败。Y 同样 watch 锁节点。', 2);
    items.waiters.push({ id: 'Z', txt: 'watch /lock · NodeDeleted', st: 'wait' });
    emit('clients', 'Z create → NodeExists：失败。Z watch 锁节点——现在 3 个等待者都挂在同一个节点上。', 2);
    release();
    metrics.woken += 3;
    phase('惊群 · 三人同时重抢', 'warn');
    flash('A delete /lock · 惊群唤醒 3 个等待者', 'warn');
    emit('watch', 'A 释放（释放 1/3）：delete /lock → NodeDeleted 同时投递给 X、Y、Z（唤醒 3/5）——一次释放吵醒所有人，只有 1 人能赢：羊群效应。', 4);
    removeWaiter('X');
    hold('X');
    flash('✓ X 抢到 → 持锁', 'ok');
    emit('lock', '三人同时重抢 create——只有 X 成功（获取 2/4）。Y、Z 再次 NodeExists。', 0);
    phase('转公平排队 · watch 前驱', 'run');
    emit('watch', 'Y、Z 改用临时顺序节点排队：每人取号、只 watch 前驱——把广播唤醒变回单播。', 5);
    enqueue('c-0001', 'Y');
    const yWaiter = items.waiters.find(w => w.id === 'Y');
    if (yWaiter) yWaiter.txt = 'watch 前驱 = 持锁者 X'; else items.waiters.push({ id: 'Y', txt: 'watch 前驱 = 持锁者 X', st: 'wait' });
    emit('queue', 'Y create → c-0001（排队 1/2）。Y 的前驱是当前持锁者 X——watch 它的节点。', 7);
    enqueue('c-0002', 'Z');
    const zWaiter = items.waiters.find(w => w.id === 'Z');
    if (zWaiter) zWaiter.txt = 'watch 前驱 c-0001'; else items.waiters.push({ id: 'Z', txt: 'watch 前驱 c-0001', st: 'wait' });
    emit('queue', 'Z create → c-0002（排队 2/2）。Z 的前驱是 c-0001（Y 的节点）——watch 它。', 7);
    release();
    metrics.woken++;
    removeWaiter('Y');
    hold('Y');
    const yQueue = items.queue.find(q => q.id === 'c-0001');
    if (yQueue) yQueue.st = 'hold';
    flash('X 释放 → 只唤醒 Y（精准）', 'ok');
    emit('watch', 'X 释放（释放 2/3）：事件只命中 watch 前驱的 Y——精准 1 次（唤醒 4/5），Z 不受打扰。', 7);
    dequeue('c-0001');
    release();
    metrics.woken++;
    removeWaiter('Z');
    hold('Z');
    const zQueue = items.queue.find(q => q.id === 'c-0002');
    if (zQueue) zQueue.st = 'hold';
    flash('Y 释放 → 只唤醒 Z（精准）', 'ok');
    emit('watch', 'Y 释放（释放 3/3）：delete c-0001 → 只唤醒 Z（唤醒 5/5）。', 7);
    phase('演示完成 · Z 持锁', 'ok');
    emit('queue', `运行结束：获取 ${metrics.acquires} · 释放 ${metrics.releases} · 排队 ${metrics.queued} · 唤醒 ${metrics.woken}——Z 当前持锁（获取 4/4，未释放）。watch 锁节点 = 释放 1 次唤醒 3 人（惊群）；watch 前驱 = 释放 1 次只吵醒该轮到的人。`, 6);
  }
  return frames;
}
function seataTx(p) {
  const scenario = p.scenario || 'success';
  const sceneTag = { success: '① 全局事务 · 分支注册与二阶段', rollback: '② 失败补偿 · undo_log 逆向回滚', tcc: '③ 模式对比 · AT 与 TCC' }[scenario];
  const frames = [];
  const metrics = { globals: 0, branches: 0, commits: 0, rollbacks: 0, undos: 0 };
  const items = { mode: scenario, phase: null, xid: null, branches: [], chips: [], flash: null };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items });
  const flash = (t, st) => { items.flash = { t, st }; };
  const phase = (txt, st) => { items.phase = { txt, st }; };
  const setBranch = (id, txt, st, note) => {
    const found = items.branches.find(b => b.id === id);
    if (found) { found.txt = txt; found.st = st; found.note = note; } else { items.branches.push({ id, txt, st, note }); }
  };
  const chip = (t, st) => items.chips.push({ t, st });
  const rmRegister = ['rm1 · 订单', 'rm2 · 库存', 'rm3 · 账户'];
  emit('tm', `Seata 分布式事务教学模型就绪：场景「${sceneTag}」。`, 0);
  if (scenario === 'success') {
    phase('TX-001 · 一阶段：分支执行与注册', 'run');
    emit('tm', '下单链路 = 订单 / 库存 / 账户三张表。AT 模式一阶段：本地 SQL 照常提交，业务零感知。', 0);
    metrics.globals++;
    items.xid = 'TX-001';
    emit('tc', 'TM 开启全局事务：TC 生成全局 XID = TX-001（全局 1/1）并下发——XID 沿调用链透传给每个 RM。', 1);
    for (const id of rmRegister) {
      metrics.branches++;
      setBranch(id, '本地已提交', 'ok', 'undo_log 已记录');
      flash(`✓ ${id} 本地提交 · 分支已注册`, 'ok');
      emit('rm', `${id}：本地 SQL 执行并提交 → 向 TC 注册分支（分支 ${metrics.branches}/3）→ 改前数据前镜像写入 undo_log。`, 3);
    }
    phase('二阶段 · 全局提交', 'run');
    emit('tc', '三个分支全部成功 → TM 发起全局提交。', 4);
    for (const id of rmRegister) {
      metrics.commits++;
      setBranch(id, '提交完成', 'ok', 'undo_log 已删除');
      flash(`TC 通知 ${id}：删除 undo_log`, 'ok');
      emit('undo', `${id}：二阶段提交——删除 undo_log（提交 ${metrics.commits}/3）。本地已提交，无需任何回滚动作。`, 4);
    }
    phase('全局提交完成', 'ok');
    emit('tm', `运行结束：全局 ${metrics.globals} · 分支 ${metrics.branches} · 提交 ${metrics.commits} · 回滚 ${metrics.rollbacks} · undo ${metrics.undos}——三分支全部成功：二阶段 = 删镜像；undo_log 从不参与业务回滚。`, 0);
  } else if (scenario === 'rollback') {
    phase('TX-002 · 一阶段：前两分支成功', 'run');
    emit('tm', '这一笔：订单 + 库存成功后，账户在扣款前校验失败——注意校验发生在分支注册之前。', 0);
    metrics.globals++;
    items.xid = 'TX-002';
    emit('tc', 'TC 生成全局 XID = TX-002（全局 1/1）。', 1);
    metrics.branches++;
    setBranch('rm1 · 订单', '本地已提交', 'ok', 'undo_log 已记录');
    flash('✓ 订单分支注册', 'ok');
    emit('rm', 'rm1 · 订单：插入订单本地提交 → 注册分支（分支 1/2）→ 前镜像写入 undo_log。', 3);
    metrics.branches++;
    setBranch('rm2 · 库存', '本地已提交', 'ok', 'undo_log 已记录');
    flash('✓ 库存分支注册', 'ok');
    emit('rm', 'rm2 · 库存：扣 2 件（100 → 98）本地提交 → 注册分支（分支 2/2）→ 前镜像 100 写入 undo_log。', 3);
    flash('rm3 余额不足 · 校验失败', 'bad');
    emit('rm', 'rm3 · 账户：扣款前校验余额 98 < 200 → 业务异常——校验在分支注册之前：本地没有提交，也没有 undo_log。', 2);
    phase('二阶段 · 全局回滚', 'warn');
    emit('tm', '异常沿调用链传回 TM → 发起全局回滚：TC 逐个通知已注册分支。', 5);
    metrics.rollbacks++;
    metrics.undos++;
    setBranch('rm1 · 订单', '← 反向 SQL 补偿', 'warn', '补偿中');
    chip('订单 #9002 恢复', 'ok');
    flash('✓ 订单分支前镜像补偿', 'ok');
    emit('undo', 'TC 通知订单 RM：读 undo_log 前镜像 → 生成反向 SQL（删除刚插入的订单）→ 逆向补偿（回滚 1/2 · undo 1/2）。', 5);
    metrics.rollbacks++;
    metrics.undos++;
    setBranch('rm2 · 库存', '← 反向 SQL 补偿', 'warn', '补偿中');
    chip('库存 98→100', 'ok');
    flash('✓ 库存分支前镜像补偿', 'ok');
    emit('undo', 'TC 通知库存 RM：前镜像 100 → 反向 UPDATE 恢复（回滚 2/2 · undo 2/2）。', 5);
    setBranch('rm1 · 订单', '已回滚', 'ok', 'undo_log 已删除');
    setBranch('rm2 · 库存', '已回滚', 'ok', 'undo_log 已删除');
    phase('回滚完成', 'ok');
    emit('tm', `运行结束：全局 ${metrics.globals} · 分支 ${metrics.branches} · 提交 ${metrics.commits} · 回滚 ${metrics.rollbacks} · undo ${metrics.undos}——未注册的账户分支不在回滚范围：补偿只覆盖「已注册且已改」的分支。`, 0);
  } else {
    phase('TX-003 · TCC Try', 'run');
    emit('tm', '同一链路改用 TCC：每分支实现 Try / Confirm / Cancel 业务接口——没有 undo_log，补偿是业务动作。', 6);
    metrics.globals++;
    items.xid = 'TX-003';
    emit('tc', 'TC 生成全局 XID = TX-003（全局 1/2）。', 1);
    for (const id of rmRegister) {
      metrics.branches++;
      setBranch(id, 'Try 预留 ✓', 'warn', '资源已冻结');
      flash(`✓ ${id} Try 冻结成功`, 'ok');
      emit('tcc', `${id}：Try 预留资源（分支 ${metrics.branches}/5）。`, 6);
    }
    phase('TX-003 · Confirm 真扣减', 'run');
    emit('tc', '三分支 Try 全部成功 → TC 二阶段 Confirm：真扣减。', 7);
    for (const id of rmRegister) {
      metrics.commits++;
      setBranch(id, 'Confirm ✓', 'ok', '真扣减完成');
      chip(`${id} Confirm ✓`, 'ok');
      flash(`✓ ${id} Confirm`, 'ok');
      emit('tcc', `${id}：Confirm 扣减完成（提交 ${metrics.commits}/3）。`, 7);
    }
    phase('TX-004 · Try 再失败', 'warn');
    emit('tm', '下一笔 TX-004：这次账户在 Try 阶段就冻结失败。', 6);
    metrics.globals++;
    items.xid = 'TX-004';
    emit('tc', 'TC 生成全局 XID = TX-004（全局 2/2）。', 1);
    for (const id of ['rm1 · 订单', 'rm2 · 库存']) {
      metrics.branches++;
      setBranch(id, 'Try 预留 ✓', 'warn', '资源已冻结');
      flash(`✓ ${id} Try 冻结成功`, 'ok');
      emit('tcc', `${id}：Try 冻结成功（分支 ${metrics.branches}/5）。`, 6);
    }
    setBranch('rm3 · 账户', 'Try ✗', 'bad', '余额不足 · 冻结失败');
    flash('rm3 Try 失败 → 触发 Cancel', 'bad');
    emit('tcc', 'rm3 · 账户：Try 冻结失败（余额不足）→ 不进入二阶段；全局回滚 = Cancel 其余已 Try 的分支。', 7);
    phase('TX-004 · Cancel 解冻', 'run');
    emit('tc', 'TC 二阶段 Cancel：已 Try 的订单、库存做业务补偿（解冻）；从未 Try 成功的账户无需动作。', 7);
    for (const id of ['rm1 · 订单', 'rm2 · 库存']) {
      metrics.rollbacks++;
      setBranch(id, 'Cancel 解冻', 'bad', '业务补偿完成');
      chip(`${id} Cancel ✓`, 'bad');
      flash(`↺ ${id} Cancel 解冻`, 'bad');
      emit('tcc', `${id}：Cancel 业务补偿（回滚 ${metrics.rollbacks}/2）。`, 7);
    }
    phase('TCC 演示完成', 'ok');
    emit('tm', `运行结束：全局 ${metrics.globals} · 分支 ${metrics.branches} · 提交 ${metrics.commits} · 回滚 ${metrics.rollbacks} · undo ${metrics.undos}——AT 的补偿 = undo_log 镜像回放；TCC 的补偿 = 业务 Cancel 接口，全程没有 undo_log。`, 0);
  }
  return frames;
}
function zab(p) {
  const scenario = p.scenario || 'broadcast';
  const sceneTag = { broadcast: '① 广播 · 两阶段提交写', crash: '② Leader 崩溃 · 选主', recovery: '③ 恢复 · 新纪元与日志同步' }[scenario];
  const frames = [];
  const metrics = { writes: 0, acks: 0, commits: 0, elections: 0, discarded: 0, synced: 0 };
  const items = { mode: scenario, phase: null, roles: [], chips: [], flash: null };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items });
  const flash = (t, st) => { items.flash = { t, st }; };
  const phase = (txt, st) => { items.phase = { txt, st }; };
  const chip = (t, st) => items.chips.push({ t, st });
  const setRole = (id, txt, st) => {
    const found = items.roles.find(r => r.id === id);
    if (found) { found.txt = txt; found.st = st; } else { items.roles.push({ id, txt, st }); }
  };
  const broadcast = (t, seq) => {
    metrics.writes++;
    emit('propose', `${t}：Leader A 把写请求编码为 Proposal (e1,${seq}) 广播给 B、C。`, 0);
    metrics.acks++;
    emit('follower', `${t}：B ACK——已落日志，回执给 A。`, 1);
    metrics.acks++;
    emit('follower', `${t}：C ACK——已落日志，回执给 A。`, 1);
    metrics.commits++;
    chip(`${t} (e1,${seq})`, 'ok');
    flash(`✓ ${t} 提交 · A+B+C = 3/3`, 'ok');
    emit('quorum', `过半确认（ACK ${metrics.acks}/6）→ A 提交并广播 COMMIT（提交 ${metrics.commits}/3）——FIFO 序号：先提出先提交。`, 1);
  };
  emit('leader', `ZAB 协议教学模型就绪：场景「${sceneTag}」。`, 0);
  if (scenario === 'broadcast') {
    phase('e1 · 广播与多数派', 'run');
    setRole('A', 'A · e1 Leader', 'ok');
    setRole('B', 'B · e1 Follower', 'ok');
    setRole('C', 'C · e1 Follower', 'ok');
    emit('leader', 'A 为 e1 纪元 Leader，B/C 为 Follower。客户端把写请求发给 A。', 0);
    broadcast('T1', 1);
    broadcast('T2', 2);
    broadcast('T3', 3);
    emit('leader', `运行结束：写 ${metrics.writes} · ACK ${metrics.acks} · 提交 ${metrics.commits} · 选主 ${metrics.elections} · 丢弃 ${metrics.discarded} · 追平 ${metrics.synced}——每条 Proposal = 2 个 Follower ACK + Leader 自己 = 3/3 过半；提交顺序 = FIFO 序号顺序，客户端看到全局一致。`, 2);
  } else if (scenario === 'crash') {
    phase('e1 · T1 正常提交', 'run');
    setRole('A', 'A · e1 Leader', 'ok');
    setRole('B', 'B · e1 Follower', 'ok');
    setRole('C', 'C · e1 Follower', 'ok');
    emit('leader', 'A 为 e1 Leader。先看一笔正常的写，再让 Leader 在「未过半」时崩溃。', 0);
    metrics.writes++;
    emit('propose', 'T1：Proposal (e1,1) 广播。', 0);
    metrics.acks++;
    emit('follower', 'B ACK T1。', 1);
    metrics.acks++;
    emit('follower', 'C ACK T1。', 1);
    metrics.commits++;
    chip('T1 (e1,1)', 'ok');
    flash('✓ T1 提交 · A+B+C = 3/3', 'ok');
    emit('quorum', '过半（ACK 2/4）→ T1 提交（提交 1/2）——已进入「过半集合」的事务，之后无论怎么切换都安全。', 1);
    phase('T2 · ACK 未过半 · A 崩溃', 'warn');
    metrics.writes++;
    emit('propose', 'T2：Proposal (e1,2) 广播。', 0);
    metrics.acks++;
    emit('follower', 'B ACK T2（ACK 3/4）——B 的日志里有 T1 + T2。', 1);
    setRole('A', 'A · 宕机', 'down');
    flash('A 崩溃 · C 的 ACK 永远到不了', 'bad');
    emit('follower', 'C 的 ACK 还在路上……A 先崩了：T2 停在「只有 B 一票」的状态。', 3);
    phase('选举 · e2', 'warn');
    emit('epoch', 'B、C 失联检测 → 进入选举：比较日志新旧——B 含 T1+T2（zxid 领先 C 的 T1）→ B 得多数票。', 3);
    metrics.elections++;
    setRole('B', 'B · e2 Leader', 'ok');
    flash('B 当选 e2 Leader', 'ok');
    emit('epoch', 'B 成为 e2 纪元 Leader（选主 1/1）。新纪元的先决条件：已提交事务必然在 B 的日志里。', 4);
    metrics.discarded++;
    phase('e2 · 丢弃未提交', 'bad');
    flash('T2 丢弃 · 从未提交', 'bad');
    emit('recovery', '新纪元只承认已提交（过半）历史：T2 只有 B 一票、未过半 → 从未真正提交 → 丢弃（丢弃 1/1）——丢的不是已提交数据。', 5);
    phase('e2 · 新 Leader 续写', 'run');
    metrics.writes++;
    emit('propose', 'T3：新 Leader B 广播 Proposal (e2,1)。', 0);
    metrics.acks++;
    emit('follower', 'C ACK T3（ACK 4/4）→ B+C = 2/3 过半。', 1);
    metrics.commits++;
    chip('T3 (e2,1)', 'ok');
    flash('✓ T3 提交 · B+C = 2/3', 'ok');
    emit('quorum', '过半 → B 提交并广播 COMMIT（提交 2/2）——新纪元只需要一票凑成多数派。', 1);
    emit('leader', `运行结束：写 ${metrics.writes} · ACK ${metrics.acks} · 提交 ${metrics.commits} · 选主 ${metrics.elections} · 丢弃 ${metrics.discarded} · 追平 ${metrics.synced}——T1、T3 进入过半集合；T2 从未达到提交判据：切换丢的是「假提交」，不是已提交事务。`, 0);
  } else {
    phase('e1 · T1 提交后崩溃', 'run');
    setRole('A', 'A · e1 Leader', 'ok');
    setRole('B', 'B · e1 Follower', 'ok');
    setRole('C', 'C · e1 Follower', 'ok');
    emit('leader', 'A 为 e1 Leader。这一场：A 在 T1 正常提交后崩溃，B/C 日志等长——考验 myid 决胜与旧主回归。', 0);
    metrics.writes++;
    emit('propose', 'T1：Proposal (e1,1) 广播。', 0);
    metrics.acks++;
    emit('follower', 'B ACK T1。', 1);
    metrics.acks++;
    emit('follower', 'C ACK T1。', 1);
    metrics.commits++;
    chip('T1 (e1,1)', 'ok');
    flash('✓ T1 提交 · A+B+C = 3/3', 'ok');
    emit('quorum', '过半（ACK 2/4）→ T1 提交（提交 1/3）——此刻 A 崩溃。', 1);
    phase('选举 · B/C 日志等长', 'warn');
    setRole('A', 'A · 宕机', 'down');
    flash('A 崩溃 · T1 已安全提交', 'bad');
    emit('epoch', 'A 崩溃：B、C 都只有 T1，日志等长 → zxid 相同，按 myid 决胜。', 3);
    metrics.elections++;
    setRole('B', 'B · e2 Leader', 'ok');
    flash('B 当选 · myid 更大', 'ok');
    emit('epoch', 'B 的 myid 更大 → 赢得 C 的选票，e2 纪元开始（选主 1/1）。', 4);
    phase('e2 · T2/T3 提交', 'run');
    metrics.writes++;
    emit('propose', 'T2：Proposal (e2,1) 广播。', 0);
    metrics.acks++;
    emit('follower', 'C ACK T2（ACK 3/4）→ B+C 过半。', 1);
    metrics.commits++;
    chip('T2 (e2,1)', 'ok');
    flash('✓ T2 提交 · B+C = 2/3', 'ok');
    emit('quorum', '过半 → 提交 T2（提交 2/3）。', 1);
    metrics.writes++;
    emit('propose', 'T3：Proposal (e2,2) 广播。', 0);
    metrics.acks++;
    emit('follower', 'C ACK T3（ACK 4/4）→ B+C 过半。', 1);
    metrics.commits++;
    chip('T3 (e2,2)', 'ok');
    flash('✓ T3 提交 · B+C = 2/3', 'ok');
    emit('quorum', '过半 → 提交 T3（提交 3/3）。', 1);
    phase('A 回归 · follower 追平', 'run');
    setRole('A', 'A · e2 Follower 同步中', 'warn');
    flash('A 复活 · 纪元落后', 'warn');
    emit('recovery', 'A 回归：日志停在 e1（缺 T2/T3）→ 纪元落后、无竞选资格 → 以 follower 身份向新 Leader B 拉取日志。', 6);
    metrics.synced++;
    setRole('A', 'A · e2 Follower', 'ok');
    flash('A 追平全部日志', 'ok');
    emit('recovery', 'A 向 B 拉齐 T1+T2+T3（追平 1/1）——已提交事务一条不丢：切主退化成一次普通日志复制。', 6);
    emit('leader', `运行结束：写 ${metrics.writes} · ACK ${metrics.acks} · 提交 ${metrics.commits} · 选主 ${metrics.elections} · 丢弃 ${metrics.discarded} · 追平 ${metrics.synced}——新 Leader 日志必然包含全部已提交事务；回归的旧主只能当 follower 追平。`, 0);
  }
  return frames;
}
function jvm(p) {
  const frames = [];
  let eden = [];
  const old = [];
  const metrics = { allocated: 0, collections: 0, reclaimed: 0, promoted: 0 };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items: { eden, old } });
  emit('allocation', '分代回收教学模型已就绪。', 44 );
  for (let i = 1; i <= p.allocations; i++) {
    if (eden.length === p.eden) {
      metrics.collections++;
      const surviving = Math.floor(eden.length * p.survival / 100);
      metrics.reclaimed += eden.length - surviving;
      old.push(...eden.slice(0, surviving));
      metrics.promoted += surviving;
      eden = [];
      emit('gc', `Young GC #${metrics.collections}：回收 ${p.eden - surviving} 个对象，${surviving} 个存活对象晋升。`, 56 );
    }
    eden.push(`obj-${i}`);
    metrics.allocated++;
    emit('eden', `对象 obj-${i} 在 Eden 分配，占用 ${eden.length}/${p.eden}。`, 50 );
  }
  return frames;
}
const runners = { redis, threadpool, hashmap, kafka, 'kafka-replication': kafkaReplication, 'kafka-eos': kafkaEos, 'kafka-consumer': kafkaConsumer, 'kafka-storage': kafkaStorage, mysql, 'mysql-isolation': mysqlIsolation, 'mysql-crash': mysqlCrash, 'concurrent-hashmap': concurrentHashmap, 'mysql-lock': mysqlLock, 'rabbitmq-exchange': rabbitmqExchange, 'rabbitmq-ack': rabbitmqAck, 'rabbitmq-cluster': rabbitmqCluster, 'rocketmq-tx': rocketmqTx, 'rocketmq-ordered': rocketmqOrdered, 'rocketmq-dledger': rocketmqDledger, 'juc-coordination': jucCoordination, 'sync-lock': syncLock, 'aqs-queue': aqsQueue, 'zookeeper-leader': zookeeperLeader, 'es-inverted': esInverted, 'volatile-jmm': volatileJmm, 'nacos-registry': nacosRegistry, 'netty-eventloop': nettyEventLoop, 'nacos-config': nacosConfig, 'zk-lock': zkLock, 'seata-tx': seataTx, zab, 'mysql-replication': mysqlReplication, 'es-sharding': esSharding, 'es-write': esWrite, jvm, 'function-calling': functionCalling, 'threadlocal-leak': threadlocalLeak, 'blocking-queue': blockingQueue, 'cas-atomic': casAtomic, 'completable-future': completableFuture, 'mysql-sharding': mysqlSharding, 'mysql-explain': mysqlExplain, 'es-query': esQuery, ...redisRunners, ...jvmRunners, ...agentRunners };
export function simulate(id, params) {
  if (!runners[id]) throw new Error(`Unknown lab: ${id}`);
  return runners[id](params);
}
