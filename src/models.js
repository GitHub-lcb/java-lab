import { LRUCache } from 'lru-cache';
import { bisectRight } from 'd3-array';
import { redisRunners } from './redisModels.js';
import { jvmRunners } from './jvmModels.js';

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
  emit('client', '实验就绪，等待请求进入。', 0, p.key);
  for (let i = 0; i < p.requests; i++) {
    const key = keys[i % keys.length];
    metrics.requests++;
    emit('app', `请求 #${i + 1}：读取 ${key}`, 1, key);
    if (p.cache && cache.get(key)) {
      metrics.hits++;
      emit('cache', `HIT · ${key} 已存在，直接返回缓存数据。`, 2, key);
    } else {
      metrics.misses++;
      emit('db', `${p.cache ? 'MISS' : 'BYPASS'} · 从 MySQL 读取 ${key}。`, 3, key);
      if (p.cache) {
        cache.set(key, { id: key });
        emit('cache', `SET · 写入 ${key}，缓存占用 ${cache.size}/${p.capacity}。`, 4, key);
      }
    }
    emit('client', `请求 #${i + 1} 完成，返回用户数据。`, 5, key);
  }
  return frames;
}
function threadpool(p) {
  const frames = [];
  const metrics = { submitted: 0, workers: 0, queued: 0, rejected: 0, caller: 0 };
  const items = [];
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items });
  emit('submit', '线程池就绪，当前工作线程数为 0。', 0);
  for (let i = 1; i <= p.tasks; i++) {
    metrics.submitted++;
    if (metrics.workers < p.core) {
      metrics.workers++;
      items.push({ label: `task-${i}`, state: 'worker' });
      emit('workers', `task-${i}：创建核心线程执行。`, 1);
    } else if (metrics.queued < p.queue) {
      metrics.queued++;
      items.push({ label: `task-${i}`, state: 'queue' });
      emit('queue', `task-${i}：核心线程忙碌，进入有界队列。`, 2);
    } else if (metrics.workers < p.max) {
      metrics.workers++;
      items.push({ label: `task-${i}`, state: 'worker' });
      emit('workers', `task-${i}：队列已满，创建非核心线程。`, 3);
    } else {
      const caller = p.policy === 'caller';
      metrics[caller ? 'caller' : 'rejected']++;
      items.push({ label: `task-${i}`, state: caller ? 'caller' : 'rejected' });
      emit('reject', `task-${i}：${caller ? '由提交任务的调用线程执行。' : '触发 AbortPolicy，任务被拒绝。'}`, 4);
    }
  }
  return frames;
}
function hashmap(p) {
  const frames = [];
  const items = [];
  const metrics = { capacity: p.capacity, size: 0, threshold: Math.floor(p.capacity * p.factor), resizes: 0 };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items });
  emit('key', '空 HashMap 已初始化。', 0);
  const keys = ['Aa', 'BB', 'java', 'redis', 'spring', 'kafka', 'mysql', 'jvm', 'lock', 'thread', 'cache', 'queue'];
  for (let i = 0; i < p.entries; i++) {
    const key = keys[i];
    emit('hash', `hash("${key}") = ${javaHash(key)}，计算扰动后的桶位置。`, 1);
    items.push({ label: key, bucket: bucketIndex(key, metrics.capacity) });
    metrics.size++;
    emit('buckets', `PUT · ${key} → bucket[${bucketIndex(key, metrics.capacity)}]`, 2);
    if (metrics.size > metrics.threshold) {
      metrics.capacity *= 2;
      metrics.threshold = Math.floor(metrics.capacity * p.factor);
      metrics.resizes++;
      items.forEach(item => { item.bucket = bucketIndex(item.label, metrics.capacity); });
      emit('resize', `size 超过 threshold，容量扩为 ${metrics.capacity}，重新分配桶位置。`, 3);
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
  if (phantomSeen !== 2) { metrics.phantom = 1; emit('undo', `⚠ B 再次范围查询得到 ${phantomSeen} 行（此前 2 行）：幻读——范围查询的行集被并发插入改变。`, 6); }
  else emit('undo', `B 再次范围查询仍为 2 行：快照内行集稳定，未出现幻读。`, 6);
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
  emit('thread', `锁升级演示就绪：场景「${sceneTag}」。${sceneNote}`, 0);
  if (scenario === 'biased') {
    emit('thread', 'T1 首次调用 critical()：Mark Word 处于无锁可偏向状态，尝试获取偏向锁。', 0);
    items.state = 'biased';
    items.owner = 'T1';
    metrics.locks++;
    emit('mark', 'CAS 成功：T1 的线程 id 写入 Mark Word 偏向位 → 偏向锁（owner=T1）。此后 T1 重入无需再做 CAS。', 1);
    emit('entry', 'T1 进入临界区执行——无竞争，没有任何同步开销。', 0);
    metrics.locks++;
    emit('mark', 'T1 再次调用 critical()：偏向锁命中——同线程直接通过，零成本重入。', 1);
    emit('thread', 'T1 退出临界区：偏向锁不立即释放，仍偏向 T1，等待下次快速获取。', 5);
  } else if (scenario === 'light') {
    items.state = 'biased';
    items.owner = 'T1';
    metrics.locks++;
    emit('mark', 'T1 获取偏向锁进入临界区（owner=T1）。', 1);
    emit('thread', 'T2 竞争进入：发现锁已偏向 T1（非本线程）→ 触发偏向撤销。', 0);
    items.state = 'unlocked';
    items.owner = null;
    emit('mark', '撤销完成：Mark Word 回到无锁状态；T2 改走轻量级锁路径。', 2);
    items.state = 'light';
    items.owner = 'T2';
    metrics.upgrades++;
    metrics.locks++;
    emit('mark', '轻量级加锁：T2 在栈中建 Lock Record 拷贝原 Mark Word，CAS 替换为指向锁记录的指针 → 轻量级锁（owner=T2）。', 2);
    emit('thread', 'T1 再次进入：锁已是轻量级 → T1 同样建 Lock Record 尝试 CAS → 失败（T2 正持锁）。', 2);
    items.spinners = ['T1'];
    metrics.spins += 2;
    emit('mark', 'T1 原地自旋重试 CAS：临界区很短，自旋 2 轮后 T2 即将释放。', 2);
    items.spinners = [];
    items.owner = 'T1';
    metrics.locks++;
    emit('entry', 'T2 执行完毕释放锁 → T1 自旋 CAS 成功获锁（owner=T1），全程无人阻塞。', 5);
    items.state = 'unlocked';
    items.owner = null;
    emit('thread', 'T1 执行完毕退出。本次运行：升级 1 次 · 自旋 2 · 阻塞 0——轻量级锁用自旋换无阻塞。', 5);
  } else {
    items.state = 'light';
    items.owner = 'T1';
    metrics.upgrades++;
    metrics.locks++;
    emit('mark', '竞争开始：偏向锁先被撤销，以轻量级锁运行（升级 1 次），T1 获锁进入长临界区。', 2);
    items.spinners = ['T2'];
    metrics.spins += 8;
    emit('thread', 'T2 进入：CAS 失败开始自旋……T1 的临界区迟迟不结束，自旋 8 轮仍未成功。', 2);
    items.state = 'heavy';
    items.spinners = [];
    metrics.upgrades++;
    emit('mark', '自旋超过阈值仍未获锁 → 锁膨胀：Mark Word 由锁记录指针改为指向 ObjectMonitor（升级 2 次）。', 3);
    items.waiters = ['T2', 'T3'];
    metrics.blocks += 2;
    emit('monitor', 'Monitor 接管：T2、T3 竞争失败 → park 挂起进入 EntryList 排队。', 4);
    items.owner = null;
    emit('entry', 'T1 长临界区执行完毕，释放锁并通知 Monitor。', 5);
    items.owner = 'T2';
    items.waiters = ['T3'];
    metrics.locks++;
    emit('monitor', 'Monitor 从 EntryList 唤醒队首 T2 并移交锁所有权（owner=T2）。', 4);
    items.owner = 'T3';
    items.waiters = [];
    metrics.locks++;
    emit('monitor', 'T2 执行完毕释放 → 唤醒 T3（owner=T3）。', 4);
    items.state = 'unlocked';
    items.owner = null;
    emit('thread', 'T3 执行完毕退出。本次运行：升级 2 次 · 阻塞 2——高竞争下 park 让出 CPU 优于空转自旋。', 5);
  }
  emit('thread', `运行结束：加锁成功 ${metrics.locks} · 自旋 ${metrics.spins} · 阻塞 ${metrics.blocks} · 升级 ${metrics.upgrades} 次。`, 5);
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
  emit('thread', `AQS 队列演示就绪：场景「${sceneTag}」。${sceneNote}`, 0);
  if (scenario === 'fair') {
    items.state = 1;
    items.owner = 'T1';
    metrics.locks++;
    emit('state', 'T1 lock()：state=0 空闲 → CAS 置 1 成功，锁归 T1，CLH 队列为空。', 1);
    items.queue = ['T2'];
    metrics.parks++;
    emit('queue', 'T2 lock()：state=1 已占用，CAS 必然失败 → 入队为队尾并 park 挂起。CLH = [T2]。', 2);
    items.queue = ['T2', 'T3'];
    metrics.parks++;
    emit('queue', 'T3 lock()：同样失败 → 排在 T2 之后。CLH = [T2 → T3]，先进先出。', 2);
    items.state = 0;
    items.owner = null;
    emit('cas', 'T1 unlock()：state 1→0；公平锁 unpark 队首后继 T2，让它醒来重新竞争。', 4);
    items.state = 1;
    items.owner = 'T2';
    items.queue = ['T3'];
    metrics.locks++;
    metrics.handoffs++;
    emit('state', 'T2 被唤醒后 CAS 0→1 成功——FIFO 顺序获锁（owner=T2）并出队。', 1);
    items.state = 0;
    items.owner = null;
    emit('cas', 'T2 unlock()：state 归零，继续 unpark 下一节点 T3。', 4);
    items.state = 1;
    items.owner = 'T3';
    items.queue = [];
    metrics.locks++;
    metrics.handoffs++;
    emit('state', 'T3 按序获锁（owner=T3），队列清空。', 1);
    items.state = 0;
    items.owner = null;
    emit('thread', 'T3 unlock() 释放。公平模式下每个线程都按到达顺序拿到锁。', 5);
  } else if (scenario === 'reentrant') {
    items.state = 1;
    items.owner = 'T1';
    metrics.locks++;
    emit('state', 'T1 第一次 lock()：state 0→1（owner=T1）。', 1);
    items.queue = ['T2'];
    metrics.parks++;
    emit('queue', 'T2 lock()：state=1 已占用 → 入队 park 等待。CLH = [T2]。', 2);
    items.state = 2;
    metrics.reentries++;
    emit('state', 'T1 再次 lock()：owner 就是 T1 → 可重入，state 1→2，无需排队。重入的线程不受 CLH 队列影响。', 1);
    items.state = 1;
    emit('cas', 'T1 第一次 unlock()：state 2→1——还没归零，锁仍然被 T1 持有。', 4);
    items.state = 0;
    items.owner = null;
    emit('cas', 'T1 第二次 unlock()：state 1→0，锁才真正释放，unpark 队首 T2。', 4);
    items.state = 1;
    items.owner = 'T2';
    items.queue = [];
    metrics.locks++;
    metrics.handoffs++;
    emit('state', 'T2 获锁（owner=T2），队列清空。', 1);
    items.state = 0;
    items.owner = null;
    emit('thread', 'T2 unlock() 释放。state 从 2 递减到 0 需要两次 unlock。', 5);
  } else {
    items.state = 1;
    items.owner = 'T1';
    metrics.locks++;
    emit('state', 'T1 lock()：CAS 0→1 成功（owner=T1）。', 1);
    items.queue = ['T2'];
    metrics.parks++;
    emit('queue', 'T2 lock()：state=1 已占用 → 入队 park 等待。CLH = [T2]。', 2);
    items.state = 0;
    items.owner = null;
    emit('cas', 'T1 unlock()：state 1→0，unpark 队首 T2——但 T2 刚醒，还没拿到锁。', 4);
    items.state = 1;
    items.owner = 'T3';
    metrics.locks++;
    emit('cas', '非公平插队：释放瞬间 T3 到达，无视队列直接 CAS 0→1 成功——T3 抢在 T2 前面拿到锁（owner=T3，T2 仍在队列）。', 1);
    items.state = 0;
    items.owner = null;
    emit('cas', 'T3 unlock()：state 归零，T2 这才真正有机会竞争。', 4);
    items.state = 1;
    items.owner = 'T2';
    items.queue = [];
    metrics.locks++;
    metrics.handoffs++;
    emit('state', 'T2 终于获锁（owner=T2），队列清空。非公平锁吞吐更高，但排队线程可能被插队。', 1);
    items.state = 0;
    items.owner = null;
    emit('thread', 'T2 unlock() 释放。本次运行 T3 插队 1 次、T2 全程排队。', 5);
  }
  emit('thread', `运行结束：获取成功 ${metrics.locks} · 重入 ${metrics.reentries} · 排队 ${metrics.parks} · 唤醒移交 ${metrics.handoffs} 次。`, 5);
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
    emit('analyzer', '写入 m1："java 后端 中间件 缓存" → standard 分词器切出 [java 后端 中间件 缓存]，逐词挂接倒排表（中文整词切分属教学示意，真实场景用 ik 分词器）。', 1);
    index({ id: 'm2', terms: ['java', '框架', 'spring', '并发'] });
    rebuild();
    emit('analyzer', '写入 m2："java 框架 spring 并发" → java 已有 postings 追加 m2；新增词项 框架 / spring / 并发。', 1);
    index({ id: 'm3', terms: ['后端', '服务', '分布式'] });
    rebuild();
    emit('token', '写入 m3："后端 服务 分布式" → 后端 postings 追加 m3；新增 服务 / 分布式。三篇文档全部入索引。', 2);
    emit('inverted', '倒排表成形：9 个词项 · 11 条定位记录。postings 让「哪篇文档含 java」变成一次表查询——写入期的组织换查询期的零全库扫描。', 3);
    query('java', 'match', ['m1', 'm2']);
    emit('query', 'match 查询 "java"：定位词项 java → postings [m1, m2]，2 篇命中；若全文扫描则要读完 3 篇原文逐一比对。', 4);
    emit('result', '相关度：m1 与 m2 各含 1 次 java（词频相同）→ 同分返回；词频越高、词项越罕见（IDF 越大），BM25 打分越高。', 5);
    emit('inverted', `运行结束：文档 ${metrics.docs} · 词项 ${metrics.terms} · 定位 ${metrics.postings} · 命中 ${metrics.hits} 篇。`, 5);
  } else if (scenario === 'phrase') {
    index({ id: 'm1', terms: ['java', '后端', '中间件', '缓存'] });
    index({ id: 'm2', terms: ['java', '框架', 'spring', '并发'] });
    index({ id: 'm3', terms: ['后端', '服务', '分布式'] });
    rebuild();
    emit('doc', '索引已含 3 篇文档：m1 "java 后端 中间件 缓存" · m2 "java 框架 spring 并发" · m3 "后端 服务 分布式"。', 1);
    emit('inverted', '倒排还记录 position：java 在 m1@1、m2@1；后端在 m1@2、m3@1。词与词是否相邻、谁先谁后，都由位置信息回答。', 3);
    query('java 后端', 'match', ['m1', 'm2', 'm3']);
    emit('query', 'match "java 后端"：拆成词项 {java, 后端} 取并集 → m1（双词命中，得分最高）· m2 · m3 共 3 篇命中。', 4);
    query('java 后端', 'match_phrase', ['m1']);
    emit('query', 'match_phrase "java 后端"：额外要求两词相邻且顺序一致 → 只有 m1（java@1 紧接 后端@2）命中。对照：若查 "后端 java" 词序反转，连 m1 也命中不了。', 4);
    emit('inverted', `运行结束：文档 ${metrics.docs} · 词项 ${metrics.terms} · 定位 ${metrics.postings} · 命中 ${metrics.hits} 篇。`, 5);
  } else {
    index({ id: 'm1', terms: ['java', '后端', '中间件', '缓存'] });
    index({ id: 'm2', terms: ['java', '框架', 'spring', '并发'] });
    index({ id: 'm3', terms: ['后端', '服务', '分布式'] });
    rebuild();
    emit('doc', '索引已含 3 篇文档（同 match 场景文档集）。本场景更新 m2、删除 m3，观察词项的增删。', 1);
    docs[1] = { id: 'm2', terms: ['java', '框架', 'vertx', '异步'] };
    rebuild();
    emit('analyzer', 'UPDATE m2："java 框架 spring 并发" → "java 框架 vertx 异步"。底层是删除旧版本再索引新版本：spring / 并发 失去引用从倒排消失，vertx / 异步 挂接进来。', 1);
    docs.splice(2, 1);
    rebuild();
    emit('token', 'DELETE m3：移除 后端 / 服务 / 分布式 的定位——服务、分布式 失去全部引用 → 词项消失；后端仍由 m1 引用，postings 收缩为 [m1]。', 2);
    query('spring', 'match', []);
    emit('query', 'match 查询 "spring"：该词项已在更新时移除，倒排表查无此项 → 命中 0 篇——被删的词再也搜不到。', 4);
    query('java', 'match', ['m1', 'm2']);
    emit('result', 'match 查询 "java"：词项 java 的 postings [m1, m2] 仍命中 2 篇——m2 更新后 java 保留，m3 删除不影响。', 5);
    emit('inverted', `运行结束：文档 ${metrics.docs} · 词项 ${metrics.terms} · 定位 ${metrics.postings} · 命中 ${metrics.hits} 篇。`, 5);
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
    emit('shared', `JMM 演示就绪：flag 是普通 boolean 字段（无 volatile）。场景「${sceneTag}」。${sceneNote}`, 0);
    items.value = 'false';
    metrics.reads++;
    emit('t2', 'T2 第 1 轮读 flag：命中本地缓存副本 → false → while 循环继续。', 1);
    metrics.reads++;
    emit('t2', 'T2 第 2 轮读：缓存副本仍 false → 继续。读的是自己核心的缓存行，与主存是否变化无关。', 1);
    items.t1seen = 'true';
    emit('t1', 'T1 执行 flag = true：写入 T1 核心的私有缓存行——主存此刻仍是 false，T2 无从得知。', 2);
    metrics.reads++;
    metrics.lost++;
    emit('t2', 'T2 第 3 轮读：缓存行未失效 → 仍读到 false。T1 已发布而 T2 看不到——可见性问题的现场。', 1);
    metrics.reads++;
    metrics.lost++;
    emit('t2', 'T2 继续死循环；JIT 甚至把 while 里的读提升到循环外——flag 之后再生效也永远读旧值。', 1);
    items.volatile = true;
    items.value = 'true';
    metrics.writes++;
    metrics.barriers++;
    emit('volatile', '修复：flag 加 volatile。volatile 写 = 立即回写主存（主存 → true）并广播失效其他核心的缓存行。', 3);
    metrics.reads++;
    items.t2seen = 'true';
    items.running = false;
    emit('t2', 'T2 的缓存行已被广播失效 → 下一次读穿透到主存 → 读到 true → 退出循环。', 4);
    emit('shared', `运行结束：普通字段丢 ${metrics.lost} 次可见更新；volatile 写-读建立 happens-before，一次失效立刻可见。`, 4);
  } else if (scenario === 'order') {
    items.kind = 'instance';
    items.value = 'null';
    items.t2seen = 'null';
    emit('shared', `DCL 演示就绪：instance 为普通字段。场景「${sceneTag}」。${sceneNote}`, 5);
    metrics.reads++;
    emit('t2', 'T2 第一次检查 instance == null → 成立（尚未创建）→ 走空分支。', 6);
    items.t1seen = '分配';
    emit('t1', 'T1 进入同步块：obj = alloc() 分配内存——对象字段仍是默认值，构造尚未执行。', 5);
    items.t1seen = '写引用';
    items.value = '半初始化';
    metrics.writes++;
    emit('t1', '重排发生：编译器/CPU 把「写引用」提前到构造之前 → instance = obj，半初始化对象泄漏到共享视野。', 5);
    metrics.reads++;
    metrics.lost++;
    items.t2seen = '半初始化';
    emit('t2', 'T2 检查 instance != null → 直接使用半初始化对象 → 读到默认字段甚至 NPE——经典 DCL 缺陷。', 6);
    items.volatile = true;
    metrics.barriers++;
    emit('volatile', '修复：instance 加 volatile。volatile 写插入 StoreStore 屏障：此前的普通写（构造）禁止重排到它之后。', 7);
    items.t1seen = '构造';
    emit('t1', 'T1 重走 new：分配 → 构造函数执行，所有字段就绪——此时实例还未发布。', 5);
    items.t1seen = '写引用';
    items.value = '就绪';
    metrics.writes++;
    emit('t1', 'StoreStore 屏障放行：写引用 instance 并回写主存——发布顺序固定，看到引用的线程必拿到完整对象。', 7);
    metrics.reads++;
    items.t2seen = '就绪';
    emit('t2', 'T2 再次检查并使用 instance：引用已就绪且对象完整 → 安全。volatile 读建立 acquire 语义，其后操作不会重排到读之前。', 6);
    emit('shared', `运行结束：普通字段重排让半初始化对象泄漏 ${metrics.lost} 次；volatile 固定「先构造后发布」，T2 之后读取必是完整对象。`, 7);
  } else {
    items.kind = 'count';
    items.volatile = true;
    items.value = 0;
    emit('shared', `原子性演示就绪：volatile int count = 0。场景「${sceneTag}」。${sceneNote}`, 8);
    metrics.reads++;
    items.t1seen = '读到 0';
    emit('t1', 'T1 执行 count++ 第 1 步：read count → 0。', 8);
    metrics.reads++;
    items.t2seen = '读到 0';
    emit('t2', 'T2 同时执行第 1 步：read count → 0——T1 尚未写回。', 8);
    metrics.writes++;
    items.value = 1;
    items.t1seen = '写入 1';
    emit('t1', 'T1 add(0+1=1) → store：volatile 写回主存 → count = 1。', 8);
    metrics.writes++;
    metrics.lost++;
    items.t2seen = '写入 1';
    emit('t2', 'T2 基于旧值 0 计算 0+1=1 → store：count = 1——覆盖了 T1 的更新！两次 ++ 只 +1。', 8);
    metrics.reads++;
    items.t2seen = '读到 1';
    emit('t2', 'T2 验证读：count = 1 ≠ 期望 2。volatile 保证读到的确实是最新值 1，却救不了交错丢失的那一次 ++。', 8);
    items.atomized = true;
    items.value = 2;
    metrics.writes++;
    metrics.barriers++;
    emit('atomic', '修复：synchronized / AtomicInteger.incrementAndGet()——read-modify-write 合并为原子操作，T1/T2 依次执行 → count = 2。', 9);
    emit('shared', `运行结束：裸 volatile 丢 ${metrics.lost} 次更新终值 1；原子化后终值 2——复合操作需要比 volatile 更强的同步。`, 9);
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
    emit('boss', `主从 Reactor 就绪：boss = 1 个线程（只监听 OP_ACCEPT），workerGroup = W1 / W2 两个线程（各持 1 个 Selector）。场景「${sceneTag}」：看一条新连接如何从握手到归属某个 worker。`, 0);
    emit('client', '客户端 A 发起 connect：三次握手由内核完成，连接进入就绪队列——boss 线程的 select 即将被唤醒。boss 不参与握手，只在队列边「接客」。', 0);
    items.conns.push(conn('conn-1', '—'));
    metrics.conns++;
    emit('boss', 'boss 的 select 命中 OP_ACCEPT → accept() 取出 conn-1。boss 的工作到此为止：不读、不写、不做业务——连接接入后立即移交下一棒。', 0);
    items.conns[0].w = 'W1';
    metrics.regs++;
    emit('boss', 'boss 按轮询把 conn-1 注册给 W1：此后 conn-1 的 OP_READ / OP_WRITE 由 W1 的 Selector 监听，读写事件只在 W1 线程上发生——连接与线程从此绑定。', 3);
    items.conns[0].state = 'busy';
    items.cur = 'conn-1 读';
    metrics.events++;
    emit('worker', 'conn-1 的首个请求到达 → W1 的 select 命中 OP_READ：在 W1 线程上执行整条 pipeline（解码 → 业务 → 编码回写），全程无锁——此刻没有任何其他线程碰得到 conn-1。', 4);
    items.conns[0].state = 'idle';
    items.cur = null;
    emit('client', 'conn-1 处理完成、响应已回写，W1 重新空闲。与此同时客户端 C 的连接完成握手进入就绪队列——boss 的下一次 select 又要命中。', 0);
    items.conns.push(conn('conn-2', '—'));
    metrics.conns++;
    emit('boss', 'boss 再次 accept → conn-2 接入。注意：刚才 W1 正忙着处理 conn-1，boss 却毫无感觉——接入与 IO 在不同线程，谁也不会拖累谁。', 0);
    items.conns[1].w = 'W2';
    metrics.regs++;
    emit('boss', '轮询指针移到 W2：conn-2 被注册给 W2。连接轮流分发避免单 worker 过热；当连接数超过 worker 数，多连接共享同一 worker——线程共享，事件仍串行。', 3);
    items.conns[1].state = 'busy';
    items.cur = 'conn-2 读';
    metrics.events++;
    emit('worker', 'conn-2 的首个请求 → W2 线程处理。此刻 W1 与 W2 各自处理自己名下的连接，互不干扰——不同 worker 上的连接天然并行。', 4);
    items.conns[1].state = 'idle';
    items.cur = null;
    emit('boss', `运行结束：接入 ${metrics.conns} · 事件 ${metrics.events} · 绑定 ${metrics.regs} · 任务 ${metrics.tasks}——boss 只做接入与分发：连接归谁，事件就永远在谁的线程上发生。`, 0);
  } else if (scenario === 'io') {
    items.conns.push(conn('conn-1', 'W1'), conn('conn-2', 'W1'), conn('conn-3', 'W1'));
    emit('worker', `场景「${sceneTag}」就绪：conn-1 / conn-2 / conn-3 三个连接都已接入并绑定 W1——一个线程服务三条连接。事件循环 = select 等待 → 处理就绪 IO → 执行任务队列 → 再 select，如此往复。`, 0);
    items.ready.push('conn-1', 'conn-2', 'conn-3');
    emit('worker', '三个连接同时有读事件 → W1 的 select 一次返回 3 个就绪 key（多路复用：一个线程盯住 N 条连接）。就绪不代表并行——它们将在 W1 上排队、逐个处理。', 0);
    items.ready.shift();
    items.conns[0].state = 'busy';
    items.cur = 'conn-1 读';
    metrics.events++;
    emit('worker', '先处理 conn-1：读取、解码、业务、编码一气呵成。处理期间 conn-2 / conn-3 只是排队等待，不会插入执行。', 4);
    items.conns[0].state = 'idle';
    items.ready.shift();
    items.conns[1].state = 'busy';
    items.cur = 'conn-2 读';
    metrics.events++;
    emit('worker', 'conn-1 处理完，同一线程直接切到 conn-2——切换无需任何锁：单线程顺序执行，共享状态天然只有一个人在碰（thread confinement）。', 4);
    items.conns[1].state = 'idle';
    items.ready.shift();
    items.conns[2].state = 'busy';
    items.cur = 'conn-3 读';
    metrics.events++;
    emit('worker', 'conn-2 完成 → conn-3 接上。一次 select 循环里三个连接全部处理完毕——高频小请求场景下，单 worker 也能扛住大量连接。', 4);
    items.conns[2].state = 'idle';
    items.cur = null;
    items.task = '心跳写 → conn-1';
    metrics.tasks++;
    emit('worker', 'conn-3 处理完成，事件循环进入本轮收尾：执行任务队列。队列里躺着一个刚投递的任务——conn-1 的 keepalive 定时器经 eventLoop().execute() 提交的「心跳写」。任务与 IO 事件在同一线程交替执行，顺序确定。', 5);
    items.task = null;
    emit('worker', 'W1 取出心跳任务执行：向 conn-1 写一个 keepalive 帧。注意来源——定时器线程（图外）想操作 conn-1，唯一安全途径就是 execute 把代码「搬」到 W1 线程来跑：无锁、无并发。', 5);
    items.conns[1].state = 'busy';
    items.cur = 'conn-2 读';
    metrics.events++;
    emit('worker', '任务队列清空，事件循环回到 select → 恰好 conn-2 的新请求已就绪：命中 OP_READ，继续处理。IO 事件与用户任务按序交替，互不打断。', 4);
    items.conns[1].state = 'idle';
    items.cur = null;
    emit('worker', `运行结束：接入 ${metrics.conns} · 事件 ${metrics.events} · 绑定 ${metrics.regs} · 任务 ${metrics.tasks}——一个线程串起 N 条连接与任意线程投递的任务：所有执行都落在同一条时间线上，无锁因此成为可能。`, 0);
  } else {
    items.conns.push(conn('conn-1', 'W1'), conn('conn-2', 'W1'));
    emit('worker', `场景「${sceneTag}」就绪：conn-1 与 conn-2 都绑定 W1。conn-1 的业务 handler 里有一处 100ms 的阻塞调用（慢 SQL / 第三方 HTTP 示意），conn-2 是高频小请求——同一个线程上，一场灾难即将发生。`, 0);
    items.conns[0].state = 'blocked';
    items.cur = 'conn-1 读';
    metrics.events++;
    emit('worker', 'conn-1 读事件 → W1 开始处理：解码、业务……handler 走到那行阻塞调用——W1 原地卡死 100ms。IO 线程上的阻塞调用，代价由整条事件循环承担。', 4);
    items.ready.push('conn-2');
    emit('worker', '此刻 conn-2 的读事件也就绪，select 明明命中——可 W1 还困在 conn-1 的阻塞调用里，无人处理。就绪事件只能排队：conn-2 的请求要白白多等一个阻塞周期。', 6);
    items.conns[0].state = 'idle';
    items.cur = null;
    items.ready.shift();
    items.conns[1].state = 'busy';
    items.cur = 'conn-2 读';
    metrics.events++;
    emit('worker', '100ms 后阻塞调用返回：conn-1 处理完成，W1 终于脱身 → 立刻处理早就就绪的 conn-2。它被拖慢了整整一个阻塞周期；若 conn-1 每秒来一次慢请求，这个延迟会无限循环。', 6);
    items.conns[1].state = 'idle';
    items.cur = null;
    emit('worker', 'conn-2 处理完成。一次拖累也许能忍，但慢请求只要留在 IO 线程，每次都会让同 worker 的连接一起陪等——于是改造开始：把 conn-1 的阻塞调用挪出事件循环。', 0);
    items.pool = 1;
    metrics.tasks++;
    emit('worker', '修复：conn-1 的 handler 不再直接阻塞——阻塞任务提交给独立业务线程池（pool.submit），W1 提交完立即返回 select，一秒都不多等。', 7);
    items.pool = 2;
    metrics.tasks++;
    emit('worker', 'conn-1 新请求到达 → W1 再次把慢任务丢给业务池（此时池中两个任务在途），自己瞬间回到 select——IO 线程从此只做「接活、派活」，永远不被占住。', 7);
    items.conns[1].state = 'busy';
    items.cur = 'conn-2 读';
    metrics.events++;
    emit('worker', '对比帧：conn-2 的请求到达，W1 立刻处理——零排队、零延迟。同一个 W1，修复前 conn-2 要陪 conn-1 干等 100ms，修复后随到随办：卸载的收益肉眼可见。', 4);
    items.conns[1].state = 'idle';
    items.cur = null;
    items.pool = 1;
    emit('worker', 'conn-2 处理完成。业务池里第一个慢任务也跑完了：结果经回调回投——写 conn-1 的操作被 execute 交回 W1 线程串行执行（回调里不能跨线程直接写 channel）。', 0);
    emit('worker', `运行结束：接入 ${metrics.conns} · 事件 ${metrics.events} · 绑定 ${metrics.regs} · 任务 ${metrics.tasks}——阻塞任务全部卸载到业务池（第二个任务仍在池中后台执行），IO 线程只做快进快出的派活：谁在事件循环里睡觉，谁就拖垮一船人。`, 0);
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
    emit('master', '复制拓扑就绪：应用写主库 M（binlog_format=ROW）；从库 S 已 START SLAVE——IO 线程从 M 拉 binlog 落本地 relay log，SQL 线程再串行回放。真正的复制从第一条事务开始。', 0);
    const e1 = add('T1 下单');
    metrics.writes++;
    emit('writer', '应用执行 INSERT 订单（id=1001）→ M 本地落库、提交成功并返回。注意：此刻从库 S 什么都不知道——异步复制下，主库提交从不等待复制。', 1);
    e1.s = 'r';
    emit('binlog', 'M 的 dump 线程把 e1 推给 S 的 IO 线程 → 落进 S 的 relay log。传输毫秒级完成，但 SQL 线程还没动——relay log 是「已收到、未回放」的中转站。', 2);
    e1.s = 'a';
    metrics.copies++;
    emit('relay', 'S 的 SQL 线程串行回放 e1：订单行在 S 落地。单线程回放保证提交顺序与主库一致——这也是延迟的根源之一。', 3);
    const e2 = add('T2 改价');
    metrics.writes++;
    emit('writer', '应用又提交 T2：10 万行 UPDATE 批量改价，M 毫秒级完成。binlog 以 ROW 格式记下整整 10 万条变更事件，等着被搬走。', 4);
    e2.s = 'r';
    items.lag = 2;
    emit('relay', 'e2 到达 S 的 relay log——但 10 万条 ROW 变更让 SQL 线程回放得冒烟。此刻 SHOW SLAVE STATUS：Seconds_Behind_Master = 2 并持续攀升。主库正常、复制没断，只是追不上。', 5);
    e2.s = 'a';
    metrics.copies++;
    items.lag = 0;
    emit('slave', '回放完成，从库追平。异步复制的真相：M 与 S 是最终一致——延迟是常态，关键是它永远不拖累主库吞吐。', 6);
    emit('master', `运行结束：写入 ${metrics.writes} · 回放 ${metrics.copies} · 半同步确认 ${metrics.acks} · 切换 ${metrics.failovers}——异步链路全通：binlog 是源头、dump 线程搬、IO 线程落 relay、SQL 线程回放。提交即返回、滞后必追平；延迟不在主库显形，只在读从库时露出旧数据。`, 0);
  } else if (scenario === 'semisync') {
    emit('master', '半同步复制开启：M 提交事务时，binlog 落盘后不立刻对外确认——必须等至少一个从库回 ack（确认收到 binlog）才返回成功。代价是每次提交多一次往返，换来「主库说成功 = 至少一份副本真的有」。', 0);
    const e1 = add('T1 支付');
    metrics.writes++;
    emit('writer', '应用提交 T1 支付事务 → binlog 已记 e1 → 但提交被挂起：M 在等 S 的 ack。半同步的「慢」就慢在这：成功返回前，先问一句「你收到了吗」。', 1);
    e1.s = 'r';
    metrics.acks++;
    items.ack = 'e1 ✓ S';
    emit('binlog', 'S 的 IO 线程把 e1 写进 relay log → 回 ack → M 收到确认，T1 这才提交成功返回应用。binlog 有了、relay 有了——即使 M 立刻宕机，S 也能把这笔事务补出来。', 2);
    e1.s = 'a';
    metrics.copies++;
    items.ack = null;
    emit('relay', 'SQL 线程把 e1 回放到 S 的数据文件。半同步管「收到」不管「回放完」——但收到就够：数据已离开 M，丢了也能从 S 找回。', 3);
    const e2 = add('T2 库存');
    metrics.writes++;
    items.mode = 'degraded';
    emit('master', 'S 宕机（网络分区）。此刻 M 提交 T2：半同步等 ack…… 超时！自我保护触发：降级为异步继续提交——宁可暂时牺牲一致，也不让主库写不进去。e2 只有 M 自己知道。', 4);
    e2.s = 'x';
    emit('binlog', '屋漏偏逢连夜雨：M 也宕机了。盘点损失：e1 在 S 有 relay 副本（安全）；e2 提交于降级窗口——M 以为成功、没有任何从库收到，随 M 一起消失。若全程异步，S 失联期间 M 提交的每一笔都可能丢：半同步把损失窗口压到只剩降级期。', 5);
    items.master = 'S↑';
    items.mode = 'async';
    metrics.failovers++;
    emit('slave', 'DBA 执行切换：S 停止复制、提升为新主——数据恢复到 e1 为止，丢 e2 这一笔。从库秒变主库，应用改一下连接串即恢复业务（生产上这一步由 MHA/Orchestrator 类工具自动完成）。', 6);
    const e3 = add('T3 订单');
    metrics.writes++;
    emit('writer', '业务重连新主 S↑：T3 提交成功，S↑ 开始积累自己的 binlog。教训沉淀：半同步保证「至少一个从库确认收到」，把数据丢失从异步的随时可能，压缩到降级窗口内的极少几笔。', 7);
    emit('master', `运行结束：写入 ${metrics.writes} · 回放 ${metrics.copies} · 半同步确认 ${metrics.acks} · 切换 ${metrics.failovers}——正常窗口零丢失（e1 双保险）、降级窗口丢一笔（e2）、切换后无缝续写（e3）。半同步的价值不是零丢失，而是把丢失窗口缩到可接受的极小。`, 0);
  } else {
    emit('master', '读写分离拓扑就绪（异步复制）：Proxy 把写请求全部发往 M、读请求默认发往 S——主库专注写、从库扛读。前提：S 的复制延迟越小，读越新鲜。', 0);
    const e1 = add('T1 下单');
    metrics.writes++;
    emit('writer', '用户下单 → 写路由到 M：订单行落库、T1 提交成功，返回「下单成功」。此刻 S 还不知道 T1 存在。', 1);
    items.route = 'slave';
    emit('reader', '1 秒后用户刷新「我的订单」→ Proxy 把读路由到 S → S 还没有 T1 → 列表空空如也。用户视角：下单成功但订单没了。这不是 bug——是复制延迟与路由策略的合谋，写后即读正是读写分离最疼的场景。', 2);
    e1.s = 'a';
    metrics.copies++;
    emit('relay', 'SQL 线程回放完成，S 追平 → 同一查询再次路由到 S：订单出现了。窗口只有几百毫秒——列表读可以忍，但「用户刚写的数据读不到」体验是硬伤。', 3);
    const e2 = add('T2 改状态');
    metrics.writes++;
    items.route = 'master';
    emit('reader', '支付回调到达：要先读订单状态再决定是否发货。这类读决定写的关键读绝不走从库——强制路由主库：T2 更新与随后的读取同库同序，永不自相矛盾，刚提交的数据立即可见。', 4);
    e2.s = 'a';
    metrics.copies++;
    items.route = 'slave';
    emit('slave', 'T2 回放完成。看分流效果：报表、列表、详情这些读放大流量全在 S，M 只扛写 + 零星关键读——读流量再涨，挂只读从库横向扩展即可，写库稳如泰山。', 5);
    emit('master', `运行结束：写入 ${metrics.writes} · 回放 ${metrics.copies} · 半同步确认 ${metrics.acks} · 切换 ${metrics.failovers}——读写分离黄金准则：默认读从库卸压；写后即读、支付回调等关键读强制走主库；延迟窗口交给半同步压缩。路由定对，一致性才有得谈。`, 0);
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
    emit('app', '集群就绪（green）：orders 索引 = 3 主分片 + 每主 1 副本。拓扑 N1: P0+R1 · N2: P1+R2 · N3: P2+R0。文档路由铁律：hash(routing) % 主分片数——同一 _id 永远落同一主分片。', 0);
    const d1 = add('#1001', 'P2');
    metrics.docs++;
    emit('app', '写入 #1001 → 协调节点算路由：hash(#1001) % 3 = 2 → 落到 P2（N3）。写入只打一个分片，不是广播。', 1);
    d1.s = 's';
    metrics.syncs++;
    emit('primary', '副本同步完成：P2 的数据复制到 R2（N1）——主副双写，任一分片宕机都有另一份顶着。', 2);
    const d2 = add('#1002', 'P0');
    metrics.docs++;
    emit('app', '写入 #1002 → hash % 3 = 0 → 落到 P0（N1）。', 3);
    d2.s = 's';
    metrics.syncs++;
    emit('primary', '副本同步完成：R0（N2）与主分片一致。', 4);
    const d3 = add('#1003', 'P1');
    metrics.docs++;
    emit('app', '写入 #1003 → hash % 3 = 1 → 落到 P1（N2）。三篇文档散落三个主分片——写入压力天然均摊。', 5);
    d3.s = 's';
    metrics.syncs++;
    emit('primary', '副本同步完成：R1（N3）一致。此刻每个主分片 1 篇文档、副本同步完成，集群依旧 green。', 6);
    emit('coord', `运行结束：写入 ${metrics.docs} · 同步 ${metrics.syncs} · 提升 ${metrics.promotes} · 迁移 ${metrics.moves}——查询「全部订单」时协调节点向 3 个主分片广播、各自返回局部命中再合并排序；而按 _id 点查只打一个分片。hash % N 既均摊了写入，又让单文档读写永远只落一个分片。`, 0);
  } else if (scenario === 'failover') {
    emit('app', '集群就绪（green）：N1: P0+R1 · N2: P1+R2 · N3: P2+R0——副本永远放在别的节点上，这才是容灾的意义。', 0);
    items.nodes[1] = 'down';
    emit('cluster', '心跳超时：N2 失联！P1 主分片随之下线、R2 副本也丢——集群变 red：写路由到 P1 的请求开始失败。这一刻起，P1 的数据靠谁？', 1);
    items.promotes.push('R1 → P1');
    items.primaries[1] = 'P1↑@N1';
    metrics.promotes++;
    emit('primary', '副本接管：N1 上 P1 的副本 R1（数据与 P1 完全同步）自动提升为新主 P1↑。丢失窗口只有几十秒，数据零丢失——副本不是冷备份，是随时能接管的活副本。', 2);
    metrics.syncs++;
    emit('cluster', '新主落定后集群自动补副本：在 N3 上重建 R1′ 并同步完成——颜色从 red 经 yellow（缺副本）回到 green。整个过程无需人工干预。', 3);
    const d1 = add('#1001', 'P1↑');
    metrics.docs++;
    emit('app', '业务无感恢复：新订单 #1001 写入新主 P1↑（N1）成功——客户端甚至没察觉到刚才发生过主分片切换。', 4);
    items.nodes[1] = 'up';
    metrics.syncs++;
    emit('cluster', 'N2 恢复上线：但它落后于集群——先以普通节点身份加入、从副本拉齐期间错过的数据（含 #1001），追平后重新参与分片分布。', 5);
    emit('cluster', `运行结束：写入 ${metrics.docs} · 同步 ${metrics.syncs} · 提升 ${metrics.promotes} · 迁移 ${metrics.moves}——从 P1 失联到新主接管只隔几十秒：副本自动提升挡住故障，补副本让集群回到 green，回归节点追平数据重新入列。副本 + 自动提升 = 分片级高可用。`, 0);
  } else {
    emit('app', '流量翻倍，单分片压力吃紧，需要扩容。本集群 3 个 master 节点，过半原则：minimum_master_nodes = 2——只有凑齐 ≥2 票的节点组才有资格选主。', 0);
    emit('cluster', '直觉操作 PUT orders/_settings 把主分片数从 3 调成 5 → 400 拒绝！主分片数被路由哈希锁死：改了取模分母，所有旧文档的落位全变，等于把数据丢进错误的分片。', 1);
    items.moving = 'orders → orders-v2';
    metrics.moves++;
    emit('primary', '扩容正道：新建 orders-v2（5 主分片）→ POST /_reindex 后台搬数据——scroll 旧索引、bulk 写新索引，一批批推进。新索引按峰值流量定好分片数，这是唯一的机会窗口。', 2);
    items.moving = null;
    metrics.moves++;
    items.primaries = ['orders-v2 · 5 主分片'];
    emit('cluster', 'reindex 完成：别名 orders 原子切换到 orders-v2，业务查询无缝改道；旧索引下线删除。扩容全程只有迁移期短暂只读，不停机。', 3);
    items.nodes[1] = 'split';
    emit('cluster', '网络分区演习：N2 与 N1、N3 断开。N2 只剩自己 1 票 < 2 → 凑不齐 quorum，即使它手上还有主分片副本，也只能降级为只读、绝不自封为主——若没有过半规则，两边各选一个主就是脑裂双主，数据一分为二。', 4);
    items.nodes[1] = 'up';
    emit('cluster', '分区恢复：N2 重连集群——补齐落后数据后重新入列，3 节点 quorum 复原。孤岛期间它顶多短暂不可写，但没有造成任何数据分裂。', 5);
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
    emit('main', '「订单页渲染」协作模型就绪。页面要三份数据：用户资料、商品详情、实时库存——单线程串行拉取约 240ms，明显偏慢。换并行思路：三任务进线程池同时拉，主线程等「三份都齐」再一次性渲染。等齐的机关 = CountDownLatch(3)：初始化计数 3，子任务各完成一次就 countDown 减一，归零瞬间放行主线程。', 0);
    items.jobs.forEach(j => (j.st = 'run'));
    metrics.submitted = 3;
    metrics.waiting = 1;
    coord('wait', 'await 阻塞 · 门闩剩 3');
    emit('pool', '三个拉取任务提交线程池并行执行；主线程调用 latch.await() 挂起等待——await 是让出 CPU 的阻塞等待，不是空转轮询。此刻主线程被 park，三线程各拉各的。', 1);
    setSt(0, 'done');
    metrics.arrived = 1;
    coord('wait', '门闩剩 2');
    emit('w1', '线程① 45ms 完成拉用户 → countDown()：门闩 3→2。②③ 未归，主线程继续阻塞——只等「齐 N 件事」，不关心谁先完成。', 2);
    setSt(1, 'done');
    metrics.arrived = 2;
    coord('wait', '门闩剩 1');
    emit('w2', '线程② 72ms 完成拉商品 → countDown()：2→1。', 3);
    setSt(2, 'done');
    metrics.arrived = 3;
    coord('open', '归零 · 唤醒主线程');
    emit('w3', '线程③ 90ms 完成拉库存（最慢的一个）→ countDown()：1→0！计数归零瞬间，主线程被唤醒。总耗时 ≈ 最慢任务 90ms，而不是三者相加的 240ms。', 4);
    metrics.released = 1;
    coord('ok', '三份数据齐 · 已合并渲染');
    emit('main', '主线程 await 返回：三份数据齐备，一次性合并渲染订单页。警惕 await 的代价：若某任务异常退出、countDown 永远不执行，主线程将永久阻塞——生产代码必须用 await(timeout) 超时兜底（本课教学省略该分支）。', 5);
    emit('main', '运行结束：发起 3 · 到达 3 · 放行 1 · 阻塞 1——CountDownLatch = 主线程等 N 件事齐的一次性闸门：await 挂起、countDown 报数、归零放行。归零后不可复用，要再等一批就 new 一个新的。', 0);
  } else if (scenario === 'barrier') {
    job('A 分片'); job('B 分片'); job('C 分片');
    coord('idle', 'CyclicBarrier(3) · 已到 0/3');
    emit('main', '「两阶段分片统计」协作模型就绪：同一份大文件拆成 A/B/C 三个分片。阶段一：各线程独立统计自己的分片；阶段二：把统计结果合并写汇总。阶段二建立在阶段一之上——先算完的线程不能自顾自冲进阶段二，必须等三人都到齐。CyclicBarrier(3)：每个线程算完 await() 等同伴，最后到达者触发屏障打开。', 0);
    items.jobs.forEach(j => (j.st = 'run'));
    metrics.submitted = 3;
    coord('busy', '阶段一 · 各自统计中');
    emit('pool', '三线程同时开工：A/B/C 各自扫描分片做阶段一统计（约 35/52/68ms，天然错开）。', 1);
    setSt(0, 'wait');
    metrics.arrived = 1;
    metrics.waiting = 1;
    coord('wait', '已到 1/3 · A 等待');
    emit('w1', 'A 线程先算完（35ms）→ barrier.await()：到齐 1/3，未满 → A 阻塞挂起等同伴。先到者的代价：真实等待。', 2);
    setSt(1, 'wait');
    metrics.arrived = 2;
    metrics.waiting = 2;
    coord('wait', '已到 2/3 · A/B 等待');
    emit('w2', 'B 线程 52ms 算完 → await()：2/3，仍不满 → B 也阻塞。此刻 A、B 都在等最后一个 C。', 3);
    setSt(0, 'run'); setSt(1, 'run'); setSt(2, 'run');
    metrics.arrived = 3;
    metrics.released = 3;
    coord('open', '到齐 3/3 · 放行阶段二');
    emit('w3', 'C 线程最后算完（68ms）→ await()：到齐 3/3！最后到达者触发屏障打开——A、B 同时被唤醒，三人齐刷刷进入阶段二。与 Latch 的分工差异在此：Latch 等的是外部事件报数，Barrier 是线程之间互相等齐。', 4);
    items.jobs.forEach(j => (j.st = 'done'));
    coord('ok', '屏障复位 0/3 · 可循环');
    emit('pool', '阶段二：A/B/C 各把统计段写进汇总文件的不同区段——互不重叠、无需加锁。写完这一轮流程结束，屏障自动复位（cyclic 得名于此）：下一批文件可以直接再来一轮同样的两阶段协作。', 5);
    emit('coord', '运行结束：发起 3 · 到达 3 · 放行 3 · 阻塞 2——A、B 两位先到者各真实阻塞一次，C 扮演「开门人」不等待。Barrier 的等待是线程互相等齐、放行后自动复位可复用；若某线程中断或超时，屏障会被打破（BrokenBarrierException），其余线程集体退出。', 0);
  } else {
    job('甲'); job('乙'); job('丙');
    coord('idle', 'Semaphore(2) · 许可 2/2');
    emit('main', '「写审计日志」限流模型就绪：三线程并发请求写库，但数据库连接池只放得下 2 个连接。Semaphore(2)：acquire() 拿到一张许可才能进临界区，release() 归还。与锁不同：许可不绑定持有者——任何线程都能 release，它管的是「同时在场人数」，不是「谁独占」。', 0);
    metrics.submitted = 3;
    metrics.arrived = 2;
    metrics.waiting = 1;
    setSt(0, 'run'); setSt(1, 'run'); setSt(2, 'wait');
    coord('wait', '许可 0/2 · 丙排队');
    emit('pool', '甲、乙先后 acquire 成功（许可 2→0）进入临界区写日志；丙 acquire 失败——许可耗尽，阻塞进入 FIFO 等待队列。连接池同时占用数被死死压在水位 2 以内。', 1);
    metrics.released = 1;
    setSt(0, 'done');
    coord('open', '许可 1/2 · 唤醒丙');
    emit('w1', '甲写完日志、释放连接 → release()：许可 0→1，唤醒队首的丙。归还许可的动作谁做都行——哪怕不是甲本人，只要还一张，排队者就能前进。', 2);
    metrics.arrived = 3;
    setSt(2, 'run');
    coord('wait', '许可 0/2 · 乙丙执行中');
    emit('w3', '丙被唤醒 acquire() 成功（许可 1→0）进入临界区开始写——排队约 30ms 后终于拿到资源。此刻乙仍在执行，池内 2 个连接再次占满。', 3);
    metrics.released = 2;
    setSt(1, 'done');
    coord('open', '许可 1/2 · 空置待取');
    emit('w2', '乙写完 → release()：许可 0→1。此刻无人排队——许可空置，等待下一个请求来取。', 4);
    metrics.released = 3;
    setSt(2, 'done');
    coord('ok', '许可 2/2 · 全部归还');
    emit('w3', '丙写完 → release()：许可 1→2，全部归还。三笔审计日志全部落库，全程池内占用从未超过 2。', 5);
    emit('coord', '运行结束：发起 3 · 到达 3 · 放行 3 · 阻塞 1——2 张许可 3 个请求：甲、乙直通，丙排队直到甲 release 才进场。Semaphore 是流量闸门：构造定水位、acquire 进水、release 放水；连接池、限流乃至 1 张许可的互斥锁都是它的用武之地。', 0);
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
    emit('app', 'WAL 教学模型就绪：余额 1000 的一行记录落在数据页 P5。本场景看一次「提交」的完整磁盘轨迹——区分两件事：「已提交」（redo 已 fsync）与「已落盘」（数据页已刷到磁盘），它们通常不在同一时刻发生。', 0);
    dirty();
    addUndo('T1', 1000);
    emit('bp', 'T1 执行 UPDATE bal = 1000-100：直接在 Buffer Pool 里的页 P5 上改写为 900——磁盘一个字都没动，P5 从此是脏页。同时 undo log 记下旧值 1000 的回滚映像：只要 T1 没提交，随时能把它抹回原样。', 1);
    addRedo('T1');
    redoSt('T1', 'committed');
    dropUndo('T1');
    metrics.committed = 1;
    emit('redo', 'T1 COMMIT：redo log buffer 里的修改记录（P5 的某偏移 → 900）顺序写入 redo log 文件并 fsync——这一刻才是「提交成功」的法律依据。顺序追加写日志 vs 随机写数据页：快一个数量级。', 2);
    emit('app', '客户端收到 commit ok。注意此刻 P5 在磁盘上还是旧版本（1000）——内存 900 与磁盘 1000 的「分裂」是常态，不是错误。若实例此刻崩溃，内存里的 900 会蒸发，但 redo 里有记录，重启可找回（下个场景演示）。', 3);
    dirty();
    addUndo('T2', 900);
    emit('bp', 'T2 执行 UPDATE bal = 900-200：Buffer Pool 改写 P5 为 700，页保持脏；undo 记旧值 900。', 4);
    addRedo('T2');
    redoSt('T2', 'committed');
    dropUndo('T2');
    metrics.committed = 2;
    emit('redo', 'T2 COMMIT：redo 记录（P5 → 700）顺序写盘并 fsync，提交成功。', 5);
    dirty();
    addUndo('T3', 700);
    addRedo('T3');
    redoSt('T3', 'committed');
    dropUndo('T3');
    metrics.committed = 3;
    emit('bp', 'T3 执行 UPDATE bal = 700-400 并 COMMIT：页 P5 改写为 400，redo 第三条记录（P5 → 400）fsync 落盘。三条 redo 全部安全，而 P5 依然躺在 Buffer Pool 里当脏页——提交密集发生时磁盘数据页可以「欠账」，日志不许欠。', 6);
    clean();
    metrics.flushed = 1;
    emit('data', '运行结束：提交 3 · 刷盘 1 · 重放 0 · 回滚 0——后台刷脏线程出手：把 P5（T1+T2+T3 的累计结果 400）一次性随机写盘，页变干净，checkpoint 前移到三条 redo 之后。若无 WAL，三个事务要三次随机写页、三次等落盘；有 WAL，三次顺序写日志 + 一次批量刷页。这就是 InnoDB 敢把 fsync 预算全花在日志上的原因。', 0);
  } else if (scenario === 'crash') {
    emit('app', '崩溃恢复教学模型就绪：同一账本（余额 1000 落页 P5）。本场景盯住「崩溃的那一秒」：提交过的事务凭什么不丢？重启时引擎做了什么？', 0);
    dirty();
    addRedo('T1');
    redoSt('T1', 'committed');
    metrics.committed = 1;
    emit('bp', 'T1 执行 UPDATE bal = 1000-100 并 COMMIT：Buffer Pool 改写 P5 为 900（脏页），redo 记录 fsync 落盘，提交成功。', 1);
    clean();
    metrics.flushed = 1;
    emit('data', '后台刷脏线程把 P5 写盘：磁盘页 = 900，checkpoint 前移越过 T1——从此 T1 不再需要 redo 保命，页自己已经在盘上了。', 2);
    dirty();
    addRedo('T2');
    redoSt('T2', 'committed');
    metrics.committed = 2;
    emit('redo', 'T2 执行 UPDATE bal = 900-200 并 COMMIT：redo 记录（P5 → 700）fsync 落盘。页 P5 再次变脏——磁盘还是 900，checkpoint 停步不前。', 3);
    dirty();
    addRedo('T3');
    redoSt('T3', 'committed');
    metrics.committed = 3;
    emit('redo', 'T3 执行 UPDATE bal = 700-400 并 COMMIT：redo 第三条记录（P5 → 400）fsync 落盘，提交成功。此刻磁盘现场：P5 = 900（T1 版本），redo 文件里躺着 T1~T3 三条记录，checkpoint 停在 T1 之后。', 4);
    state('down');
    emit('app', '实例崩溃（模拟 kill -9）：Buffer Pool 瞬间蒸发——内存里的 700、400 全没了。磁盘上只剩：P5 = 900（T1 已刷盘的部分）+ 完整的三条 redo。问题：T2、T3 是「已提交」的事务，用户的钱不能因为一次宕机就退回原状。', 5);
    dirty();
    metrics.replayed = 2;
    state('recovering');
    emit('redo', '重启自动恢复：引擎定位 checkpoint（T1 之后），从那里开始顺序重放 redo——T2 的记录把页从 900 改回 700，T3 的记录改到 400。roll-forward 的目标：让磁盘页追平所有已提交事务，一条不落。', 6);
    state('ok');
    emit('redo', '运行结束：提交 3 · 刷盘 1 · 重放 2 · 回滚 0——恢复完成，P5 = 400，T2、T3 分毫未丢。redo 重放是「已提交零丢失」的兑现机制；若没有 WAL 直接改页，崩溃可能把提交过的修改留在写了一半的磁盘页上，那才是真丢。日志先行 + 重放兜底 = InnoDB 敢向客户端承诺 commit ok 的底气。', 0);
  } else {
    emit('app', '两阶段提交教学模型就绪。一条事务的持久化横跨两个独立文件：InnoDB 的 redo（引擎内部，崩溃恢复用）与 Server 层的 binlog（归档 + 从库复制）。提交必须让两者原子对齐——主库提交了而 binlog 没有，从库就永远缺这笔账。协议拆三步：redo prepare → binlog fsync → redo commit。事务 T1（-100）开演。', 0);
    dirty();
    addUndo('T1', 1000);
    addRedo('T1');
    emit('redo', 'T1 修改页 P5（1000→900）后，第一步：redo 写入 prepare 标记并落盘——引擎侧已就绪，redo 里躺着 T1 的完整修改记录，随时可提交可回滚。', 1);
    addBin('T1');
    emit('binlog', '第二步：Server 把 T1 的变更写入 binlog（事务 id = 1）并 fsync。binlog 落盘意味着：从库可能已经收到并执行了这条事务。', 2);
    redoSt('T1', 'committed');
    dropUndo('T1');
    metrics.committed = 1;
    emit('redo', '第三步：redo 补写 commit 标记——T1 正式提交。三步齐：引擎与归档对 T1 达成一致，从库与主库不会分裂。', 3);
    dirty();
    addUndo('T2', 900);
    addRedo('T2');
    emit('redo', 'T2（-200）走到第一步：redo prepare 落盘。此刻正处于窗口 A：引擎已就绪、binlog 还没写。若此刻崩溃，T2 该怎么处置？', 4);
    state('down');
    emit('app', '崩溃！磁盘现场：redo 里有 T2 的 prepare、binlog 里没有 xid=2。麻烦在于 prepare 已落盘——引擎单看 redo 会以为 T2 可以提交，必须借助第二个文件做判定。', 5);
    redoSt('T2', 'discard');
    dropUndo('T2');
    items.redos = items.redos.filter(r => r.t !== 'T2');
    metrics.rolledback = 1;
    state('running');
    emit('undo', '恢复判定：binlog 中没有 xid=2 → 从库从未收到 T2，主库也不该有它 → 按 undo 把 P5 上 T2 的修改抹掉（回滚）。redo 里那份 prepare 记录随之作废。规则一：binlog 没有 → 回滚。', 6);
    dirty();
    addUndo('T3', 900);
    addRedo('T3');
    addBin('T3');
    emit('binlog', 'T3（-200）连走两步：redo prepare 落盘 + binlog（xid=3）fsync。此刻处于窗口 B：binlog 已有 T3、redo commit 还没写。若此刻崩溃呢？', 7);
    state('down');
    emit('app', '崩溃！磁盘现场：redo 有 T3 prepare、binlog 有 xid=3、commit 标记缺失。与 T2 相反——从库可能已经执行了 T3，主库若回滚就是主从不一致。', 8);
    redoSt('T3', 'committed');
    dropUndo('T3');
    metrics.replayed = 1;
    metrics.committed = 2;
    state('ok');
    emit('redo', '恢复判定：binlog 中有 xid=3 → 从库已收到，主库必须提交 → 恢复器补写 redo commit，T3 就地转正。规则二：binlog 有 → 补提交。', 9);
    emit('app', '运行结束：提交 2 · 刷盘 0 · 重放 1 · 回滚 1——两个崩溃窗口、一条判定规则：binlog 有 xid 就补 commit，没有就回滚。prepare 先行的意义：崩溃后引擎能区分「已就绪未归档」（T2，作废）与「已归档未拍板」（T3，转正）。redo 与 binlog 各记一半、靠 xid 对齐，两阶段提交把「主从不分裂」从口号变成协议。', 0);
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
    emit('table', '「并发写活动报名表」模型就绪：8 槽空表，T1~T4 四个线程即将并发 put。JDK8 的写入只有两档：hash 定位的槽位为空 → CAS 无锁直插；槽位已被占 → synchronized 锁住该 bin 头节点做链尾追加。锁永远不落在整张表上。', 0);
    setSt('T1', 'done', 'put(k1) ✓');
    putKey(3, 'k1');
    emit('t1', 'T1 put(k1)：hash 定位槽 3——空槽！CAS(tab[3], null, k1) 原子直插成功，全程没有锁。这是快路径：读多写少场景下大多数 put 都该走这里，代价只有一次 CAS。', 4);
    setSt('T2', 'done', 'put(k2) ✓');
    setSt('T3', 'wait', 'put(k3) 排队');
    metrics.conflicts = 2;
    putKey(3, 'k2');
    emit('t2', 'T2 put(k2) 与 T3 put(k3) 几乎同时落槽 3：tab[3] 已被 k1 占据，两次 CAS 双双失败（冲突 +2）。T2 抢到先手：synchronized(头节点 k1) 进入慢路径，锁内链尾追加 k2；T3 没抢到锁，在 bin 锁外阻塞排队——注意排队粒度：等的是「槽 3 的 bin 锁」，不是整张表。', 5);
    setSt('T4', 'done', 'put(k4) ✓');
    putKey(5, 'k4');
    emit('t4', 'T4 put(k4)：hash 定位槽 5——空槽，CAS 直插成功，甚至没察觉到槽 3 正有线程持锁。写不同槽的线程完全并行：这正是「锁单 bin」与「锁全表」的分水岭——Hashtable 的 synchronized(this) 此刻会让 T4 在门外等 T2 写完。', 6);
    setSt('T3', 'done', 'put(k3) ✓');
    putKey(3, 'k3');
    emit('t3', 'T2 释放 bin 锁 → 唤醒 T3：T3 获得锁后链尾追加 k3（槽 3 链长 3：k1→k2→k3）。同槽的写被串行化，但串行范围只限于这一个 bin——这就是 JDK8 的锁粒度：空槽 CAS（无锁）、冲突锁单 bin（微串行），并发度≈桶数。', 7);
    emit('sync', '运行结束：写入 4 · 撞槽 2 · 迁移 0 · 分流 0——四次写入里 T1、T4 走 CAS 快路径零锁开销，T2、T3 撞同一槽才各付出一次锁等待。若换成 Hashtable：四次写全部全局互斥、理论并发度 1；JDK7 分段锁把表切成 16 段、并发度 16；JDK8 锁到单个 bin、并发度等于桶数——锁粒度进化的终点是「只锁被触碰的那一小块」。', 0);
  } else if (scenario === 'resize') {
    mkSlots(8);
    threads([['T1', 'put(G)'], ['T2', 'put(H)'], ['T3', '只读'], ['T4', '只读']]);
    fill(0, ['A']); fill(2, ['B', 'C']); fill(3, ['D']); fill(5, ['E']); fill(6, ['F']);
    emit('table', '「并发扩容」模型就绪：8 槽表已存 6 个 key（A、B·C、D、E、F），恰好等于扩容阈值 0.75×8=6——下一次 put 将先触发扩容到 16 槽再插入。教学 hash：key 的去留由高位决定，低位置 0 的留原槽、置 1 的进原槽+8。', 0);
    items.newSlots = [];
    for (let i = 0; i < 16; i++) items.newSlots.push({ i, keys: [], fwd: false });
    setSt('T1', 'run', 'put(G) 触发扩容');
    emit('t1', 'T1 put(G) 发现 size 已达阈值 → 先扩容：分配 16 槽新表，随后从尾槽向前逐个迁移旧槽（transferIndex 协作指针递减）。迁移期间旧表读写不冻结——这是 CHM 与「拷贝整表再替换」式扩容的本质区别。', 1);
    markFwd(7); markFwd(6); markFwd(5);
    items.newSlots[6].keys.push('F'); items.newSlots[5].keys.push('E');
    emit('table', '迁移推进：槽 7、6、5 依次迁完——F 进新表槽 6、E 进新表槽 5，每个迁完的旧槽原地放入 ForwardingNode（fwd 占位）。fwd 是一张「路由牌」：告诉后来的线程这槽已搬走、请沿它去新表。', 2);
    markFwd(3); markFwd(2);
    items.newSlots[3].keys.push('D');
    items.newSlots[2].keys.push('B'); items.newSlots[10].keys.push('C');
    emit('table', '迁移槽 3、2：D 是低位 key → 新表槽 3。槽 2 的链 B→C 按 hash 高位拆成两段——B 低位段 → 新表槽 2，C 高位段 → 新表槽 10（原槽+8）。一条链就地劈开，这就是扩容的 rehash：每槽 keys 只可能去「原槽」或「原槽+旧容量」两个位置。', 3);
    markFwd(1); markFwd(4); markFwd(0);
    items.newSlots[0].keys.push('A');
    emit('table', '迁移收尾：槽 1、4（空）、0（A → 新表槽 0）迁完，旧表 8 槽全部挂上 fwd。此后旧表退化为纯「路由牌」：任何访问先看旧槽，是 fwd 就转新表——已迁槽绝不会再被写入旧表，扩容期间的数据才不丢不重。', 4);
    setSt('T1', 'done', 'put(G) ✓');
    metrics.inserted++;
    items.newSlots[4].keys.push('G');
    emit('t1', 'T1 恢复执行 put(G)：定位旧槽 4——fwd！读线程沿 fwd.next 到新表槽 4 找到数据，写线程同样转入新表执行插入。G 落位新表槽 4，旧表纹丝不动。', 5);
    setSt('T2', 'done', 'put(H) ✓');
    metrics.inserted++;
    items.newSlots[12].keys.push('H');
    emit('t2', 'T2 put(H)：新请求直接对新表寻址——高位 key H 落新表槽 12，CAS 直插成功。扩容已经完成，此后一切读写都发生在 16 槽新表上。', 6);
    emit('sync', '运行结束：写入 2 · 撞槽 0 · 迁移 8 · 分流 0——8 个旧槽全部迁移并挂 fwd：F/E/D/B/A 留原槽、C/G 高位移位。迁移是「按槽协作」而非「整表停摆」：中途的任何 put 要么转新表、要么等该槽迁完，绝不写进已搬走的旧槽。fwd 占位 + 路由，让并发扩容既不停顿也不丢数据。', 0);
  } else {
    mkSlots(8);
    threads([['T1', 'put(k1)'], ['T2', 'put(k2)'], ['T3', 'put(k3)'], ['T4', 'size() 观察者']]);
    emit('sync', '「弱一致计数」模型就绪：CHM 不维护一个被全表锁保护的 size 字段，而是 baseCount（CAS 自增）+ CounterCell[]（撞车分流）。低并发一次 CAS 搞定；高并发撞车者把增量写进自己散列的 cell，size() 最后 base + Σcells 求和。', 0);
    setSt('T1', 'done', 'put(k1) ✓');
    setSt('T2', 'done', 'put(k2) ✓');
    items.counter.base = 1;
    items.counter.cells.push(1);
    metrics.inserted = 2; metrics.conflicts = 1; metrics.spread = 1;
    slotOf(1).keys.push('k1'); slotOf(3).keys.push('k2');
    emit('t1', 'T1、T2 同时 put：T1 的 CAS(baseCount 0→1) 抢先成功（base=1）；T2 撞车失败——不无限重试，把增量写进自己的 CounterCell[0]（+1）。两笔写入都成功落槽，计数被拆成 base 1 + cells[0]=1 两处。', 2);
    setSt('T3', 'done', 'put(k3) ✓');
    items.counter.base = 2;
    metrics.inserted = 3;
    slotOf(5).keys.push('k3');
    emit('t3', 'T3 put(k3)：此刻无竞争，CAS(baseCount 1→2) 一次成功，base=2。低并发下 CounterCell 完全闲置——cells 只在撞车时才被启用。', 2);
    setSt('T1', 'done', 'put(k4) ✓');
    setSt('T2', 'done', 'put(k5) ✓');
    items.counter.base = 3;
    items.counter.cells.push(1);
    metrics.inserted = 5; metrics.conflicts = 2; metrics.spread = 2;
    slotOf(2).keys.push('k4'); slotOf(4).keys.push('k5');
    emit('t1', 'T1、T2 第二轮同时 put（k4、k5）：T1 的 CAS(base 2→3) 再胜；T2 再败 → 这次散列到 CounterCell[1]（+1）。base=3、cells=[1,1]，实时共 5 笔写入——热点被两个 cell 摊开，谁都不必死等 baseCount。', 3);
    items.counter.last = 5;
    setSt('T3', 'run', 'put(k6) 插入完成');
    slotOf(6).keys.push('k6');
    metrics.inserted = 6;
    emit('sync', '此刻外部线程调用 size()：sum = base 3 + cells[1+1] = 5。就在求和读快照的同一瞬间，T3 的 k6 已落槽（第 6 笔写入完成）但它的 baseCount CAS 还没落位——快照读不到 → size() 返回 5，而真实是 6。弱一致窗口：size() 不是精确值，是「某一时刻的近似快照」。', 4);
    setSt('T3', 'done', 'put(k6) ✓');
    items.counter.base = 4;
    items.counter.last = 6;
    emit('t3', 'T3 的 CAS(base 3→4) 落位：base=4、cells=[1,1]，实时 6 笔全部入账。此时再调 size() = 4+2 = 6，收敛到精确值——滞后只存在于求和与写入交错的瞬间。', 5);
    emit('sync', '运行结束：写入 6 · 撞槽 2 · 分流 2——两次撞车各分流进一个 cell，size() 快照 5 → 收敛 6。size() 弱一致的根源：计数是打散的（base+cells），求和是并发的——想拿精确值就要付出全局锁，CHM 用弱一致换吞吐。这与 LongAdder 同一思想：热点计数拆成多份并行累加，读时再合并。', 0);
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
    emit('db', '「行锁互斥与排队」模型就绪：products 表行 1（stock=100）。T1、T2 都要 UPDATE 行 1；T3 只做普通 SELECT。InnoDB 的锁粒度是「行」：UPDATE / DELETE / SELECT ... FOR UPDATE 才申请行锁，普通 SELECT 走 MVCC 快照读、不申请任何锁。', 0);
    setSt('T1', 'run', 'UPDATE 行1');
    grant('行 1', 'T1', 'X');
    lockOnce();
    emit('t1', 'T1 执行 UPDATE products SET stock=stock-1 WHERE id=1：向行 1 申请 X（排他）锁——该行空闲，锁管理器授予。T1 在行 1 上持锁修改（stock 100→99），未提交。', 1);
    setSt('T1', 'done', 'UPDATE 行1 · 未提交');
    setSt('T2', 'wait', 'UPDATE 行1');
    pushWait('T2');
    metrics.blocked = 1;
    emit('t2', 'T2 对同一行执行 UPDATE：X 与 X 不兼容——后到者不是报错而是「等待」：请求进入锁等待队列阻塞，等行 1 的持有者释放。默认 innodb_lock_wait_timeout=50s，超时才抛 1205；期间行 1 对一切写者与加锁读者关闭。', 2);
    setSt('T3', 'done', 'SELECT 行1');
    emit('t3', 'T3 SELECT * FROM products WHERE id=1：普通读不撞锁——MVCC 快照读直接返回当前已提交版本（stock=100），从 T1 的 X 锁旁边零等待穿过。X 锁只挡「写」与「加锁读」（FOR UPDATE / LOCK IN SHARE MODE）：InnoDB 的读写因此互不阻塞。', 2);
    setSt('T1', 'done', 'COMMIT');
    release('行 1');
    emit('t1', 'T1 提交 COMMIT：事务一结束，行 1 的 X 锁即释放，InnoDB 唤醒等待队列中的 T2。排队等的是「锁」不是「事务」——T1 释放的瞬间 T2 就有机会。', 3);
    setSt('T2', 'run', 'UPDATE 行1');
    dropWait('T2');
    grant('行 1', 'T2', 'X');
    lockOnce();
    emit('t2', 'T2 从等待队列被唤醒 → 获得行 1 的 X 锁 → 执行 UPDATE：读到的是 T1 提交后的 stock=99（先提交先生效），修改完成。', 4);
    setSt('T2', 'done', 'COMMIT');
    release('行 1');
    emit('t2', 'T2 提交，行 1 的锁再次释放——互斥只发生在同一行的写者之间，不同行与普通读全程不受影响。', 5);
    emit('db', '运行结束：加锁成功 2 · 锁等待 1 · 死锁环 0 · 回滚 0——整场只有 T2 为同一行付出了一次等待，期间 T3 的普通读畅通无阻。行锁把互斥面收窄到「冲突的那一行」：若换成 MyISAM 的整表写锁，T3 与所有写者都会堵在 T1 后面；InnoDB 让不冲突的读写完全并行。', 0);
  } else if (scenario === 'gap') {
    threads([['T1', 'FOR UPDATE 行2-5'], ['T2', 'INSERT id=3']]);
    emit('db', '「间隙锁 · 防幻读」模型就绪：RR（可重复读）隔离级别下，products 已有 id = 1、2、5、9——行 2 与行 5 之间夹着一段空的间隙 (2,5)（id 3、4 的位置）。T1 将对 id BETWEEN 2 AND 5 做范围加锁读；T2 想向该间隙插入 id=3。幻读要防的，正是「读的范围里被别人插进新行」。', 0);
    setSt('T1', 'run', 'FOR UPDATE 行2-5');
    grant('行 2', 'T1', 'X'); grant('行 5', 'T1', 'X'); grant('间隙 (2,5)', 'T1', 'GAP');
    lockOnce();
    emit('t1', 'T1 SELECT * FROM products WHERE id BETWEEN 2 AND 5 FOR UPDATE：加锁成功（一次语句拿到三把锁，计数 +1）——行 2、行 5 各挂一把 X 记录锁（管已存在的行），两行之间的空隙 (2,5) 被 GAP 间隙锁罩住（管还不存在的行）。记录锁 ∪ 间隙锁合称 next-key lock：该区间既不许改旧行、也不许插新行。', 1);
    setSt('T1', 'done', '加锁读 · 未提交');
    setSt('T2', 'wait', 'INSERT id=3');
    pushWait('T2');
    metrics.blocked = 1;
    emit('t2', 'T2 INSERT INTO products VALUES (3, …)：插入动作先申请「插入意向锁」（声明我要往这个间隙放行）→ 与 T1 持有的间隙锁 (2,5) 不兼容 → 阻塞排队。间隙锁互斥的不是某一行，而是「往空隙里插入」这个动作——行锁管已存在的行，间隙锁管还不存在的行。', 2);
    emit('db', '对照：若隔离级别是 RC（读已提交）——没有间隙锁，T2 的插入直接成功并提交；T1 再次执行同条件加锁读会看到多出的 id=3。同一事务两次相同查询结果不一致，这就是幻读。RR 用 next-key 把「旧行 + 空隙」锁成一个连续区间，从入口堵死幻读。', 1);
    setSt('T1', 'done', 'COMMIT');
    release('行 2'); release('行 5'); release('间隙 (2,5)');
    emit('t1', 'T1 提交 COMMIT：next-key 锁全部释放（两把行锁 + 一把间隙锁），InnoDB 唤醒等待插入的 T2——T1 的读一致性已由锁保证完毕，此刻放行插入不再产生幻读。', 3);
    setSt('T2', 'run', 'INSERT id=3');
    dropWait('T2');
    grant('行 3', 'T2', 'X');
    lockOnce();
    emit('t2', 'T2 被唤醒：插入意向得到许可 → id=3 落位为正式数据行（加锁成功 2），原间隙 (2,5) 被它劈成 (2,3) 与 (3,5) 两段。此后 id 集合变为 1、2、3、5、9。', 4);
    setSt('T2', 'done', 'COMMIT');
    release('行 3');
    emit('t2', 'T2 提交。间隙锁只存在于 RR 及以上的隔离级别；代价是放大阻塞面——紧邻被锁间隙的插入全部排队，高并发插入的热点区间要慎用范围加锁读。', 5);
    emit('db', '运行结束：加锁成功 2 · 锁等待 1 · 死锁环 0 · 回滚 0——T1 的范围加锁读一次拿到三把锁（行2 X + 行5 X + 间隙 GAP）计为 1 次加锁成功；T2 的插入被间隙锁挡了一次，直到 T1 提交才放行。若没有间隙锁（RC），T2 无需等待，但 T1 的加锁读就会遭遇幻读——间隙锁是 RR 用一段阻塞换来的「读什么就是什么」。', 0);
  } else {
    threads([['T1', 'UPDATE 行1 → 行2'], ['T2', 'UPDATE 行2 → 行1']]);
    emit('db', '「死锁环」模型就绪：两个事务都做两步更新——T1 先改行 1 再改行 2；T2 先改行 2 再改行 1。加锁顺序正好相反，是死锁最经典的成因；死锁检测器（后台线程，周期性扫描锁等待构成的等待图）已就位。', 0);
    setSt('T1', 'run', 'UPDATE 行1');
    grant('行 1', 'T1', 'X'); lockOnce();
    emit('t1', 'T1 第一步 UPDATE 行1：行 1 空闲 → X 锁授予（加锁成功 1）。T1 攥住行 1 不松手。', 1);
    setSt('T1', 'done', '持行1 · 等行2');
    setSt('T2', 'run', 'UPDATE 行2');
    grant('行 2', 'T2', 'X'); lockOnce();
    emit('t2', 'T2 第一步 UPDATE 行2：行 2 空闲 → X 锁授予（加锁成功 2）。两把锁各归其主，此刻一切正常。', 1);
    setSt('T2', 'done', '持行2 · 等行1');
    setSt('T1', 'wait', 'UPDATE 行2');
    pushWait('T1');
    metrics.blocked = 1;
    emit('t1', 'T1 第二步要行 2：被 T2 持有 → X 冲突 → T1 进入等待队列（锁等待 1）。此刻 T1 是典型的「持有并等待」：攥着行 1 不放，等行 2 释放。', 2);
    setSt('T2', 'wait', 'UPDATE 行1');
    pushWait('T2');
    metrics.blocked = 2;
    emit('t2', 'T2 第二步要行 1：被 T1 持有 → T2 也进入等待队列（锁等待 2）。等待图闭合：T1 → 等行2 ← 持行2 的 T2 → 等行1 ← 持行1 的 T1——循环等待成环。若无人干预，两个事务会互等到底，直到 50s 锁等待超时各自抛 1205。', 2);
    metrics.deadlocks = 1;
    emit('detector', '死锁检测器扫描等待图：发现环 行1 → T1 → 行2 → T2 → 行1（死锁环 1）。死锁四条件在此齐备：互斥（行锁不共享）、持有并等待（都攥一把等一把）、不可剥夺（锁只能由持有者自己释放）、循环等待（等待关系成环）——检测器立即介入，而不是干等超时。', 3);
    setSt('T2', 'idle', 'victim · 已回滚');
    metrics.aborted = 1;
    items.victim = 'T2';
    release('行 2');
    dropWait('T2');
    emit('detector', '挑选 victim：比较两事务的 undo 代价（已修改行数），T2 与 T1 各写 1 行打平 → 按内部规则取 T2。回滚 T2：按 undo 把行 2 恢复原值、释放其全部锁，并向应用返回 Error 1213（Deadlock found when trying to get lock; try restarting transaction）。死锁不罚双方——只牺牲代价小的一个，持锁的另一方继续。', 4);
    setSt('T1', 'run', 'UPDATE 行2');
    dropWait('T1');
    items.victim = null;
    grant('行 2', 'T1', 'X'); lockOnce();
    emit('t1', '行 2 随 T2 回滚释放 → T1 被唤醒，X 锁授予（加锁成功 3）→ 完成第二步 UPDATE → COMMIT，两行更新原子生效，锁全部释放。T1 全程未回滚：死锁的代价由 victim 单方承担。', 5);
    setSt('T1', 'done', 'COMMIT · 两行完成');
    release('行 1'); release('行 2');
    setSt('T2', 'run', '重试 UPDATE 行1');
    grant('行 1', 'T2', 'X'); lockOnce();
    emit('t2', '应用捕获 1213 → 重试整个事务：T2 以全新事务重新执行——UPDATE 行1 → 行 1 已被 T1 提交释放 → X 锁授予（加锁成功 4）→ 继续行 2 → 提交。重试的是「完整事务」而非从断点续跑：只有完整重放，业务语义才原子。', 6);
    setSt('T2', 'done', '重试 COMMIT');
    release('行 1');
    emit('db', '运行结束：加锁成功 4 · 锁等待 2 · 死锁环 1 · 回滚 1——互反的加锁顺序把死锁四条件凑齐后，InnoDB 的选择不是超时硬等而是主动检测：发现环 → 回滚 undo 代价最小的 victim → 释放其锁让另一方走完 → 应用捕获 1213 重试整个事务。根治手段是让环无从形成：所有事务按固定顺序加锁（先小 id 后大 id），并把事务缩短到最小。', 0);
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
    emit('client', '「近实时」模型就绪：向主分片写 w1/w2/w3。每条写入 = 内存 buffer（暂存）+ translog 同步 fsync（durability=request：ack 前日志先落盘）。此刻 0 个段——能不能搜取决于「段」，不取决于 buffer。', 0);
    metrics.docs++; items.buffer.push('w1'); items.tlog.push({ t: 'w1', fs: true });
    emit('client', '写 w1：进 buffer（1/3），translog 已 fsync（✓）才回 ack——已确认就不会丢。但搜索看不到它：倒排还没建。', 1);
    metrics.docs++; items.buffer.push('w2'); items.tlog.push({ t: 'w2', fs: true });
    emit('client', '写 w2：buffer 2 条、translog ✓。数据躺在「待搜索区」，要等 refresh 快照成段。', 2);
    metrics.docs++; items.buffer.push('w3'); items.tlog.push({ t: 'w3', fs: true });
    emit('client', '写 w3：确认写入 3，buffer 满 3 条。默认 refresh_interval=1s——每秒（或 buffer 满时）自动执行一次 refresh。', 3);
    items.search = { hits: [] };
    flash('miss', '✗ 0 命中 · 未 refresh');
    emit('search', '立即搜索：3 条都在 buffer——倒排还没建，0 命中。「近实时」的含义就在这一秒里：写入确认 ≠ 可搜索，中间隔着一次 refresh。', 4);
    items.flash = null;
    items.search = null;
    metrics.refreshs++; items.segs.push({ id: 'seg-A', docs: ['w1', 'w2', 'w3'], mem: true, del: [] }); items.buffer = [];
    flash('refresh', 'refresh · seg-A 可搜');
    emit('segs', 'refresh #1：把 buffer 快照生成内存段 seg-A（w1-3），buffer 清空，段即刻进倒排、可被搜索。refresh 只动内存（OS page cache）不落盘——「可搜」与「已落盘」是两回事。', 5);
    items.flash = null;
    items.search = { hits: ['w1', 'w2', 'w3'] };
    emit('search', '搜索命中 3：seg-A 的倒排已可查。内存段同样服务读请求——新数据先走内存快速可见，代价是小段会持续积累。', 6);
    metrics.docs++; items.buffer.push('w4'); items.tlog.push({ t: 'w4', fs: true });
    items.search = { hits: ['w1', 'w2', 'w3'] };
    emit('client', '再写 w4（确认写入 4）：进 buffer、translog ✓。此刻搜索仍只命中 w1-3——w4 要等下一次 refresh。', 7);
    items.search = null;
    metrics.refreshs++; items.segs.push({ id: 'seg-B', docs: ['w4'], mem: true, del: [] }); items.buffer = [];
    flash('refresh', 'refresh · seg-B 生成');
    emit('segs', 'refresh #2：seg-B（w4）生成。段列表 seg-A + seg-B，都在内存、都可搜——若写入持续，段会一直累积；查询要归并的段越多越慢，那是 merge 机制的由来。', 8);
    items.flash = null;
    items.search = { hits: ['w1', 'w2', 'w3', 'w4'] };
    emit('segs', '运行结束：确认写入 4 · 刷新 2 · 合并 0 · 回放 0——两次 refresh 把 4 笔写入变成 2 个可搜内存段。近实时 = 写入确认（buffer+translog，日志语义）与可搜索（refresh 成段，索引语义）分离，两者默认隔一个 refresh_interval（1s）。', 0);
  } else if (scenario === 'crash') {
    items.state = 'ok';
    emit('client', '「崩溃恢复」模型就绪：durability=request（每笔 ack 前 translog fsync）。先写 3 笔并 refresh 一次，再把日志切 async 写 2 笔——最后实例崩溃，看谁活下来。', 0);
    metrics.docs++; items.buffer.push('w1'); items.tlog.push({ t: 'w1', fs: true });
    emit('client', '写 w1：buffer 1 条、translog 已 fsync（✓）后 ack——request 模式下确认即安全，日志已在磁盘。', 1);
    metrics.docs++; items.buffer.push('w2'); items.tlog.push({ t: 'w2', fs: true });
    emit('client', '写 w2：buffer 2 条、日志 ✓。', 2);
    metrics.docs++; items.buffer.push('w3'); items.tlog.push({ t: 'w3', fs: true });
    emit('client', '写 w3：确认写入 3。此刻 data 的完整路径：buffer 3 条 + translog 3 条已 fsync + 0 个段。', 3);
    metrics.refreshs++; items.segs.push({ id: 'seg-A', docs: ['w1', 'w2', 'w3'], mem: true, del: [] }); items.buffer = [];
    flash('refresh', 'refresh · seg-A 可搜');
    emit('segs', 'refresh #1：seg-A（w1-3）生成，内存段可搜——但注意它没有 fsync：进程一死、内存段即蒸发。数据安全吗？安全：translog 里 w1-3 已落盘，恢复时靠它兜底。', 4);
    items.flash = null;
    metrics.docs += 2; items.buffer.push('w4', 'w5'); items.tlog.push({ t: 'w4', fs: false }, { t: 'w5', fs: false });
    emit('client', '运维把 translog 切成 durability=async（每 5s 批量 fsync）换吞吐：写 w4、w5 立即回 ack——但此刻它们只在 OS page cache，日志还没 fsync（无 ✓）。', 5);
    items.state = 'down'; items.buffer = []; items.segs = []; items.tlog = items.tlog.filter(l => l.fs); items.lost = ['w4', 'w5'];
    flash('crash', '实例崩溃');
    emit('buffer', '实例崩溃！内存态全灭：buffer（w4/w5）、内存段 seg-A、未 fsync 的日志尾巴。磁盘上只剩：translog 里已 fsync 的 w1-3——那是本次恢复的全部家底。', 6);
    items.state = 'recovering'; metrics.replayed += 3; items.segs.push({ id: 'seg-A′', docs: ['w1', 'w2', 'w3'], mem: true, del: [] });
    flash('replay', 'translog 回放 w1-3');
    emit('translog', '启动恢复：加载 commit point → 从 translog 回放 w1-3（已 fsync 的逐条重放）→ 重新 refresh 出内存段 seg-A′。ack 过的数据一笔没少——request 模式的每一笔都 fsync 过。', 7);
    items.flash = null;
    items.state = 'ok'; items.search = { hits: ['w1', 'w2', 'w3'] };
    flash('lost', '✗ w4/w5 · async 窗口丢失');
    emit('client', '恢复完成：搜索命中 w1-3。对照 w4/w5——ack 已回但日志未 fsync，translog 里没有它们，随崩溃消失（丢尾巴 ≤ sync_interval=5s）。request 与 async 的取舍：每请求一次 fsync 的吞吐，换最多一个批窗口的丢失风险。', 8);
    items.flash = null;
    emit('segs', '运行结束：确认写入 5 · 刷新 1 · 合并 0 · 回放 3——已 fsync 的 3 笔经 translog 找回，async 尾巴 2 笔（w4/w5）确认后仍丢失。durability 决定「ack 到底意味着什么」：request = ack 即磁盘；async = ack 只是内存。', 0);
  } else {
    emit('client', '「段合并」模型就绪：每次 refresh 生成一个小段——先连写 w1-4、每笔立即 refresh 造出 4 个小段，再观察查询、更新、删除与合并如何纠缠。', 0);
    for (let i = 1; i <= 4; i++) {
      metrics.docs++; items.tlog.push({ t: `w${i}`, fs: true });
      metrics.refreshs++; items.segs.push({ id: `seg-${i}`, docs: [`w${i}`], mem: true, del: [] });
      emit('client', `写 w${i} → refresh #${i}：seg-${i}（w${i}）生成。模拟写入流量下每个刷新周期产出一个新段——先接受小段，后面统一收拾（确认写入 ${i}）。`, i);
    }
    items.search = { hits: ['w1', 'w2', 'w3', 'w4'] };
    emit('search', '搜索：一次查询要跨全部 4 个段归并结果——段多 = 每查询多几份段开销。写入量大时段的增长是查询延迟的主要敌人，merge 线程就是为它而生的。', 5);
    metrics.merges++; items.segs = [{ id: 'seg-A', docs: ['w1', 'w2', 'w3'], mem: false, del: [] }, { id: 'seg-4', docs: ['w4'], mem: true, del: [] }];
    flash('merge', 'merge #1 · 3 小段归并');
    emit('segs', '后台 merge #1：把 seg-1~seg-3 归并为 seg-A 并落盘（段 4 → 2）。合并只读旧段、产出新段，旧段随后废弃待删——段一旦生成就永不就地修改。', 6);
    items.flash = null;
    items.search = null;
    metrics.docs++; items.buffer.push('w2′'); items.tlog.push({ t: 'w2′', fs: true });
    items.segs.find(s => s.id === 'seg-A').del.push('w2');
    emit('client', '更新 w2 → w2′：ES 没有就地改——新版本 w2′ 照常写 buffer + translog（确认写入 5），同时给 seg-A 里的旧 w2 打删除标记（tombstone）。旧值还占着空间，搜索端已不再返回它。', 7);
    metrics.refreshs++; items.segs.push({ id: 'seg-B', docs: ['w2′'], mem: true, del: [] }); items.buffer = [];
    flash('refresh', 'refresh · seg-B 生成');
    emit('segs', 'refresh #5：w2′ 成段 seg-B。段列表：seg-A（磁盘 · 含 tombstone w2）、seg-4、seg-B——更新不重写旧段，只是另起一段。', 8);
    items.flash = null;
    items.segs.find(s => s.id === 'seg-A').del.push('w3');
    flash('del', 'tombstone · 删 w3');
    emit('client', '删除 w3：给 seg-A 里的 w3 打删除标记（同时写 translog 防丢）。逻辑视图：w3 立即从搜索消失；物理视图：w3 的字节还在磁盘上，要等含它的段被合并才真正释放。', 9);
    items.flash = null;
    metrics.merges++; items.segs = [{ id: 'seg-C', docs: ['w1', 'w2′', 'w4'], mem: false, del: [] }];
    flash('merge', 'merge #2 · 清除 tombstone');
    emit('segs', '后台 merge #2：把 seg-A（含两处 tombstone）、seg-B、seg-4 归并为 seg-C 落盘——合并时真正丢弃被删的旧 w2 与 w3，磁盘空间此刻才释放（段 3 → 1）。删除的执行者从来不是 DELETE，而是 merge。', 10);
    items.flash = null;
    items.search = { hits: ['w1', 'w2′', 'w4'] };
    emit('segs', '运行结束：确认写入 5 · 刷新 5 · 合并 2 · 回放 0——w1-4 四次 refresh 攒出 4 个小段、w2′ 一次 refresh，两次合并把段收成 1 个（seg-C）；最终可搜的只有 w1 / w2′ / w4，被删的旧值与 w3 在合并那一刻才被清理——段只增不改，merge 是唯一的「删除执行者」。', 0);
  }
  return frames;
}
function jvm(p) {
  const frames = [];
  let eden = [];
  const old = [];
  const metrics = { allocated: 0, collections: 0, reclaimed: 0, promoted: 0 };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items: { eden, old } });
  emit('allocation', '分代回收教学模型已就绪。', 0);
  for (let i = 1; i <= p.allocations; i++) {
    if (eden.length === p.eden) {
      metrics.collections++;
      const surviving = Math.floor(eden.length * p.survival / 100);
      metrics.reclaimed += eden.length - surviving;
      old.push(...eden.slice(0, surviving));
      metrics.promoted += surviving;
      eden = [];
      emit('gc', `Young GC #${metrics.collections}：回收 ${p.eden - surviving} 个对象，${surviving} 个存活对象晋升。`, 2);
    }
    eden.push(`obj-${i}`);
    metrics.allocated++;
    emit('eden', `对象 obj-${i} 在 Eden 分配，占用 ${eden.length}/${p.eden}。`, 1);
  }
  return frames;
}
const runners = { redis, threadpool, hashmap, kafka, 'kafka-replication': kafkaReplication, 'kafka-eos': kafkaEos, mysql, 'mysql-isolation': mysqlIsolation, 'mysql-crash': mysqlCrash, 'concurrent-hashmap': concurrentHashmap, 'mysql-lock': mysqlLock, 'rabbitmq-exchange': rabbitmqExchange, 'rabbitmq-ack': rabbitmqAck, 'rocketmq-tx': rocketmqTx, 'rocketmq-ordered': rocketmqOrdered, 'juc-coordination': jucCoordination, 'sync-lock': syncLock, 'aqs-queue': aqsQueue, 'zookeeper-leader': zookeeperLeader, 'es-inverted': esInverted, 'volatile-jmm': volatileJmm, 'nacos-registry': nacosRegistry, 'netty-eventloop': nettyEventLoop, 'nacos-config': nacosConfig, 'mysql-replication': mysqlReplication, 'es-sharding': esSharding, 'es-write': esWrite, jvm, ...redisRunners, ...jvmRunners };
export function simulate(id, params) {
  if (!runners[id]) throw new Error(`Unknown lab: ${id}`);
  return runners[id](params);
}
