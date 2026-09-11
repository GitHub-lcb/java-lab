const snapshot = (frames, state) => frames.push(structuredClone(state));
const emitFrame = (frames, active, message, code, metrics, items = [], dataLabel = 'STATE') => {
  snapshot(frames, { active, message, code, metrics, items, dataLabel });
};

function redisTypes(p) {
  const frames = [];
  const values = [];
  const unique = new Set();
  const metrics = { commands: 0, stored: 0, duplicates: 0, result: 0 };
  emitFrame(frames, 'client', `准备执行 ${p.type.toUpperCase()} 数据结构实验。`, 20 , metrics, values, 'DATASET');
  for (let i = 0; i < p.items; i++) {
    const value = i > 0 && p.duplicateEvery > 0 && i % p.duplicateEvery === 0 ? `member-${i - 1}` : `member-${i}`;
    metrics.commands++;
    emitFrame(frames, 'command', `发送第 ${i + 1} 条命令，值为 ${value}。`, 26 , metrics, values, 'DATASET');
    const deduplicates = p.type === 'set' || p.type === 'zset';
    if (deduplicates && unique.has(value)) {
      metrics.duplicates++;
      emitFrame(frames, 'structure', `${p.type.toUpperCase()} 已存在 ${value}，不会新增成员。`, 29 , metrics, values, 'DATASET');
    } else {
      unique.add(value);
      if (p.type === 'string') values[0] = value;
      else if (p.type === 'hash') values.push(`field-${i}=${value}`);
      else if (p.type === 'zset') values.push(`${i}:${value}`);
      else values.push(value);
      emitFrame(frames, 'structure', `${value} 已写入 ${p.type.toUpperCase()}。`, 29 , metrics, values, 'DATASET');
    }
    metrics.stored = values.length;
  }
  metrics.result = values.length;
  emitFrame(frames, 'result', `实验完成：${metrics.commands} 次写入请求，实际保存 ${metrics.stored} 个条目。`, 35 , metrics, values, 'DATASET');
  return frames;
}

function redisExpiry(p) {
  const frames = [];
  const keys = [];
  const metrics = { writes: 0, alive: 0, expired: 0, evicted: 0, denied: 0 };
  const view = time => keys.map(item => `${item.key} · TTL ${Math.max(0, item.expiresAt - time)}s`);
  emitFrame(frames, 'writer', `maxmemory 可容纳 ${p.memory} 个键，策略为 ${p.policy}。`, 29 , metrics, [], 'KEYSPACE');
  for (let i = 0; i < p.keys; i++) {
    metrics.writes++;
    if (keys.length >= p.memory && p.policy === 'noeviction') {
      metrics.denied++;
      emitFrame(frames, 'policy', `写入 key:${i} 被拒绝：已达到 maxmemory。`, 44 , metrics, view(0), 'KEYSPACE');
      continue;
    }
    if (keys.length >= p.memory) {
      const removed = keys.shift();
      metrics.evicted++;
      emitFrame(frames, 'policy', `内存不足，${p.policy} 淘汰 ${removed.key}。`, 44 , metrics, view(0), 'KEYSPACE');
    }
    keys.push({ key: `key:${i}`, expiresAt: p.ttl + (p.jitter ? i % 3 : 0) });
    metrics.alive = keys.length;
    emitFrame(frames, 'memory', `SET key:${i} EX ${p.ttl + (p.jitter ? i % 3 : 0)}`, 49 , metrics, view(0), 'KEYSPACE');
  }
  for (let time = 1; time <= p.ttl + 2; time++) {
    const before = keys.length;
    for (let i = keys.length - 1; i >= 0; i--) if (keys[i].expiresAt <= time) keys.splice(i, 1);
    const expiredNow = before - keys.length;
    metrics.expired += expiredNow;
    metrics.alive = keys.length;
    emitFrame(frames, expiredNow ? 'expiry' : 'clock', `时间 +${time}s：${expiredNow ? `删除 ${expiredNow} 个到期键` : '尚无键到期'}。`, 58 , metrics, view(time), 'KEYSPACE');
  }
  return frames;
}

function redisPenetration(p) {
  const frames = [];
  const nullKeys = new Set();
  const metrics = { requests: 0, dbQueries: 0, blocked: 0, nullHits: 0 };
  emitFrame(frames, 'client', `准备请求 ${p.requests} 次不存在的商品 ID。`, 85 , metrics, [], 'NULL CACHE');
  for (let i = 0; i < p.requests; i++) {
    const key = `product:missing:${i % p.distinct}`;
    metrics.requests++;
    emitFrame(frames, 'client', `请求 ${key}。`, 85 , metrics, [...nullKeys], 'NULL CACHE');
    if (p.bloom) {
      metrics.blocked++;
      emitFrame(frames, 'guard', `BloomFilter 判定 ${key} 一定不存在，拦截回源。`, 88 , metrics, [...nullKeys], 'NULL CACHE');
    } else if (p.nullCache && nullKeys.has(key)) {
      metrics.nullHits++;
      emitFrame(frames, 'cache', `命中空值缓存 ${key}，返回不存在。`, 101 , metrics, [...nullKeys], 'NULL CACHE');
    } else {
      metrics.dbQueries++;
      emitFrame(frames, 'db', `数据库未找到 ${key}。`, 98 , metrics, [...nullKeys], 'NULL CACHE');
      if (p.nullCache) {
        nullKeys.add(key);
        emitFrame(frames, 'cache', `SET ${key} <NULL> EX 60，短暂缓存空结果。`, 101 , metrics, [...nullKeys], 'NULL CACHE');
      }
    }
  }
  emitFrame(frames, 'result', `完成：数据库承受 ${metrics.dbQueries} 次查询。`, 104 , metrics, [...nullKeys], 'NULL CACHE');
  return frames;
}

function redisBreakdown(p) {
  const frames = [];
  const metrics = { requests: p.concurrent, dbQueries: 0, waiting: 0, stale: 0 };
  const queue = Array.from({ length: p.concurrent }, (_, i) => `req-${i + 1}`);
  emitFrame(frames, 'hotkey', `热点 Key 到期，${p.concurrent} 个请求同时到达。`, 43 , metrics, queue, 'CONCURRENT REQUESTS');
  if (p.strategy === 'none') {
    for (const request of queue) {
      metrics.dbQueries++;
      emitFrame(frames, 'db', `${request} 缓存未命中，独立查询数据库。`, 147 , metrics, queue.slice(metrics.dbQueries), 'CONCURRENT REQUESTS');
    }
  } else if (p.strategy === 'mutex') {
    metrics.dbQueries = 1;
    metrics.waiting = Math.max(0, p.concurrent - 1);
    emitFrame(frames, 'lock', '第一个请求获得重建锁，其余请求等待或短暂重试。', 85 , metrics, queue.slice(1), 'WAIT QUEUE');
    emitFrame(frames, 'db', '持锁请求查询数据库并重建缓存。', 147 , metrics, queue.slice(1), 'WAIT QUEUE');
    metrics.waiting = 0;
    emitFrame(frames, 'cache', '缓存重建完成，等待请求读取新值。', 109 , metrics, [], 'WAIT QUEUE');
  } else {
    metrics.dbQueries = 1;
    metrics.stale = p.concurrent;
    emitFrame(frames, 'cache', '逻辑过期：所有请求先返回旧值。', 109 , metrics, queue, 'STALE RESPONSES');
    emitFrame(frames, 'lock', '一个请求获得重建锁，在后台刷新缓存。', 85 , metrics, queue, 'STALE RESPONSES');
    emitFrame(frames, 'db', '后台任务查询数据库并写入新值。', 147 , metrics, queue, 'STALE RESPONSES');
  }
  emitFrame(frames, 'result', `数据库查询 ${metrics.dbQueries} 次；等待 ${metrics.waiting}，旧值响应 ${metrics.stale}。`, 72 , metrics, [], 'RESULT');
  return frames;
}

function redisAvalanche(p) {
  const frames = [];
  const buckets = Array(p.window).fill(0);
  for (let i = 0; i < p.keys; i++) buckets[p.jitter ? i % p.window : 0]++;
  const metrics = { expired: 0, peakDb: 0, overloaded: 0, rejected: 0 };
  emitFrame(frames, 'clock', `${p.keys} 个 Key ${p.jitter ? `分散在 ${p.window} 秒窗口` : '设置相同过期时间'}。`, 23 , metrics, buckets.map((n, i) => `t+${i + 1}s · ${n}`), 'EXPIRY TIMELINE');
  for (let i = 0; i < buckets.length; i++) {
    const expired = buckets[i];
    metrics.expired += expired;
    emitFrame(frames, 'cache', `t+${i + 1}s：${expired} 个 Key 同时失效。`, 45 , metrics, buckets.map((n, j) => `t+${j + 1}s · ${n}`), 'EXPIRY TIMELINE');
    const admitted = p.rateLimit ? Math.min(expired, p.dbCapacity) : expired;
    metrics.rejected += expired - admitted;
    metrics.peakDb = Math.max(metrics.peakDb, admitted);
    if (admitted > p.dbCapacity) metrics.overloaded++;
    emitFrame(frames, p.rateLimit && expired > admitted ? 'guard' : 'db', p.rateLimit && expired > admitted ? `限流放行 ${admitted}，快速失败 ${expired - admitted}。` : `数据库在该秒接收 ${admitted} 次回源。`, 52 , metrics, buckets.map((n, j) => `t+${j + 1}s · ${n}`), 'EXPIRY TIMELINE');
  }
  emitFrame(frames, 'result', `峰值回源 ${metrics.peakDb}/s，过载时间片 ${metrics.overloaded}。`, 56 , metrics, buckets.map((n, i) => `t+${i + 1}s · ${n}`), 'EXPIRY TIMELINE');
  return frames;
}

function redisConsistency(p) {
  const frames = [];
  const metrics = { dbVersion: 1, cacheVersion: 1, staleReads: 0, consistent: '是' };
  const view = () => [`DB · v${metrics.dbVersion}`, metrics.cacheVersion ? `Cache · v${metrics.cacheVersion}` : 'Cache · EMPTY'];
  emitFrame(frames, 'writer', '初始状态：数据库和缓存均为 v1。', 37 , metrics, view(), 'VERSIONS');
  if (p.strategy === 'deleteFirst') {
    metrics.cacheVersion = 0;
    emitFrame(frames, 'cache', '写线程先删除缓存。', 93 , metrics, view(), 'VERSIONS');
    if (p.concurrentRead) {
      emitFrame(frames, 'reader', '并发读缓存未命中，读取数据库旧值 v1。', 41 , metrics, view(), 'VERSIONS');
    }
    metrics.dbVersion = 2;
    emitFrame(frames, 'db', '写线程把数据库更新为 v2。', 45 , metrics, view(), 'VERSIONS');
    if (p.concurrentRead) {
      metrics.cacheVersion = 1;
      metrics.staleReads++;
      emitFrame(frames, 'cache', '并发读把稍早读取的 v1 回填到缓存，产生最终脏数据。', 48 , metrics, view(), 'VERSIONS');
    }
  } else if (p.strategy === 'dbFirst') {
    metrics.dbVersion = 2;
    emitFrame(frames, 'db', '先提交数据库更新 v2。', 45 , metrics, view(), 'VERSIONS');
    if (p.concurrentRead) {
      metrics.staleReads++;
      emitFrame(frames, 'reader', '删除前的并发读可能短暂命中缓存 v1。', 41 , metrics, view(), 'VERSIONS');
    }
    metrics.cacheVersion = 0;
    emitFrame(frames, 'cache', '数据库提交成功后删除缓存；下一次读取将回填 v2。', 93 , metrics, view(), 'VERSIONS');
  } else {
    metrics.cacheVersion = 0;
    emitFrame(frames, 'cache', '第一次删除缓存。', 93 , metrics, view(), 'VERSIONS');
    if (p.concurrentRead) emitFrame(frames, 'reader', '并发读取得数据库 v1，准备回填。', 41 , metrics, view(), 'VERSIONS');
    metrics.dbVersion = 2;
    emitFrame(frames, 'db', '数据库更新为 v2。', 45 , metrics, view(), 'VERSIONS');
    if (p.concurrentRead) metrics.cacheVersion = 1;
    emitFrame(frames, 'cache', '延迟后第二次删除缓存，清除竞态窗口中的旧值。', 48 , metrics, view(), 'VERSIONS');
    metrics.cacheVersion = 0;
  }
  metrics.consistent = !metrics.cacheVersion || metrics.cacheVersion === metrics.dbVersion ? '是' : '否';
  emitFrame(frames, 'result', `最终：DB=v${metrics.dbVersion}，Cache=${metrics.cacheVersion ? `v${metrics.cacheVersion}` : 'EMPTY'}，一致=${metrics.consistent}。`, 78 , metrics, view(), 'VERSIONS');
  return frames;
}

function redisAtomic(p) {
  const frames = [];
  const metrics = { success: 0, finalStock: p.stock, oversold: 0, retries: 0 };
  const view = () => [`stock · ${metrics.finalStock}`, `success · ${metrics.success}`, `retries · ${metrics.retries}`];
  emitFrame(frames, 'request', `${p.clients} 个客户端并发购买，每单扣减 ${p.demand}，初始库存 ${p.stock}。`, 41 , metrics, view(), 'INVENTORY');
  if (p.strategy === 'getSet') {
    emitFrame(frames, 'read', `所有客户端几乎同时 GET 到库存 ${p.stock}。`, 78 , metrics, view(), 'INVENTORY');
    metrics.success = p.stock >= p.demand ? p.clients : 0;
    metrics.finalStock = p.stock >= p.demand ? p.stock - p.demand : p.stock;
    metrics.oversold = Math.max(0, metrics.success - Math.floor(p.stock / p.demand));
    emitFrame(frames, 'write', `多个 SET 相互覆盖，最终库存只扣减一次，却确认了 ${metrics.success} 单。`, 84 , metrics, view(), 'INVENTORY');
  } else {
    const possible = Math.floor(p.stock / p.demand);
    metrics.success = Math.min(p.clients, possible);
    metrics.finalStock = p.stock - metrics.success * p.demand;
    if (p.strategy === 'watch') {
      emitFrame(frames, 'read', 'WATCH 记录库存版本，客户端分别排队 MULTI/EXEC。', 78 , metrics, view(), 'INVENTORY');
      metrics.retries = Math.min(Math.max(0, p.clients - 1), metrics.success);
      emitFrame(frames, 'guard', `版本变化使 ${metrics.retries} 次 EXEC 取消，客户端重新读取后再提交。`, 102 , metrics, view(), 'INVENTORY');
      emitFrame(frames, 'write', `WATCH + MULTI/EXEC 完成 ${metrics.success} 单，库存不会被覆盖写回。`, 100 , metrics, view(), 'INVENTORY');
    } else {
      emitFrame(frames, 'guard', 'Lua 在 Redis 事件循环中原子检查库存并执行扣减。', 102 , metrics, view(), 'INVENTORY');
      emitFrame(frames, 'write', `脚本完成 ${metrics.success} 单，库存不足的请求直接返回失败。`, 100 , metrics, view(), 'INVENTORY');
    }
  }
  emitFrame(frames, 'result', `最终库存 ${metrics.finalStock}，成功 ${metrics.success} 单，超卖 ${metrics.oversold} 单。`, 68 , metrics, view(), 'INVENTORY');
  return frames;
}

function redisLock(p) {
  const frames = [];
  const metrics = { acquired: 1, overlaps: 0, renewals: 0, unsafeUnlocks: 0 };
  let owner = 'client-A';
  const view = () => [`owner · ${owner || 'none'}`, `lease · ${p.lease}s`, `work · ${p.work}s`];
  emitFrame(frames, 'client', `${owner} 使用 SET lock token NX EX ${p.lease} 获得锁。`, 42 , metrics, view(), 'LOCK STATE');
  emitFrame(frames, 'lock', `${owner} 开始执行 ${p.work}s 的临界区任务。`, 44 , metrics, view(), 'LOCK STATE');
  if (p.work > p.lease && p.watchdog) {
    metrics.renewals = Math.ceil(p.work / p.lease) - 1;
    for (let i = 0; i < metrics.renewals; i++) emitFrame(frames, 'watchdog', `看门狗第 ${i + 1} 次续期，锁仍归 client-A。`, 53 , metrics, view(), 'LOCK STATE');
  } else if (p.work > p.lease && p.clients > 1) {
    owner = 'client-B';
    metrics.acquired++;
    metrics.overlaps++;
    emitFrame(frames, 'lock', '租约先于业务结束到期，client-B 获得同一把锁，两个客户端并发执行。', 67 , metrics, view(), 'LOCK STATE');
    if (p.safeUnlock) {
      emitFrame(frames, 'release', 'client-A 用 Lua 比较 token，发现已非锁持有者，不执行 DEL。', 72 , metrics, view(), 'LOCK STATE');
    } else {
      metrics.unsafeUnlocks++;
      owner = null;
      emitFrame(frames, 'release', 'client-A 直接 DEL，误删了 client-B 的锁。', 72 , metrics, view(), 'LOCK STATE');
    }
  }
  if (owner === 'client-A') owner = null;
  emitFrame(frames, 'result', `完成：重叠执行 ${metrics.overlaps} 次，危险解锁 ${metrics.unsafeUnlocks} 次。`, 86 , metrics, view(), 'LOCK STATE');
  return frames;
}

function redisPersistence(p) {
  const frames = [];
  const metrics = { acknowledged: 0, durable: 0, lost: 0, fsyncs: 0 };
  const log = [];
  emitFrame(frames, 'client', `启动 ${p.mode} 持久化实验。`, 44 , metrics, log, 'DURABLE LOG');
  for (let i = 1; i <= p.writes; i++) {
    metrics.acknowledged++;
    log.push(`SET order:${i}`);
    emitFrame(frames, 'memory', `写入 #${i} 已在内存执行并返回成功。`, 48 , metrics, log, 'DURABLE LOG');
    const flush = p.mode === 'aofAlways' || (p.mode === 'aofEverysec' && i % 5 === 0) || (p.mode === 'rdb' && i % 6 === 0);
    if (flush) {
      metrics.durable = i;
      metrics.fsyncs++;
      emitFrame(frames, 'disk', `${p.mode === 'rdb' ? '生成 RDB 快照' : 'AOF 刷盘'}，当前持久化到 #${i}。`, 62 , metrics, log.slice(0, metrics.durable), 'DURABLE LOG');
    }
  }
  metrics.lost = metrics.acknowledged - metrics.durable;
  emitFrame(frames, 'crash', `进程崩溃：内存中尚未持久化的 ${metrics.lost} 条写入丢失。`, 68 , metrics, log.slice(0, metrics.durable), 'DURABLE LOG');
  emitFrame(frames, 'recover', `重启恢复 ${metrics.durable}/${metrics.acknowledged} 条已确认写入。`, 80 , metrics, log.slice(0, metrics.durable), 'DURABLE LOG');
  return frames;
}

function redisHa(p) {
  const frames = [];
  const metrics = { writes: 0, replicated: 0, lost: 0, promoted: 0, available: '正常', shards: p.mode === 'cluster' ? 3 : 1 };
  const state = [];
  emitFrame(frames, 'client', `${p.mode === 'cluster' ? 'Cluster 三分片' : 'Sentinel 主从组'}启动，副本数 ${p.replicas}。`, 48 , metrics, state, 'TOPOLOGY');
  for (let i = 1; i <= p.writes; i++) {
    metrics.writes++;
    state.push(`write-${i}`);
    emitFrame(frames, 'primary', `主节点确认写入 #${i}。`, 88 , metrics, state, 'TOPOLOGY');
  }
  metrics.replicated = p.replicas ? Math.max(0, p.writes - p.lag) : 0;
  emitFrame(frames, 'replica', `异步复制已推进到 #${metrics.replicated}，落后 ${p.writes - metrics.replicated} 条。`, 98 , metrics, state.slice(0, metrics.replicated), 'TOPOLOGY');
  metrics.available = '切换中';
  emitFrame(frames, 'failure', '主节点故障，连接短暂中断并开始故障转移。', 105 , metrics, state.slice(0, metrics.replicated), 'TOPOLOGY');
  metrics.lost = p.writes - metrics.replicated;
  if (p.replicas > 0) {
    metrics.promoted = 1;
    metrics.available = '恢复';
    emitFrame(frames, 'failover', `选举一个副本为新主节点；未复制的 ${metrics.lost} 条写入无法恢复。`, 111 , metrics, state.slice(0, metrics.replicated), 'TOPOLOGY');
  } else {
    metrics.available = '中断';
    emitFrame(frames, 'failover', '没有可提升副本，服务保持中断。', 111 , metrics, [], 'TOPOLOGY');
  }
  return frames;
}

function redisOps(p) {
  const frames = [];
  const hotRequests = Math.round(p.requests * p.hotRatio / 100);
  const metrics = { hotKeys: p.hotRatio >= 50 ? 1 : 0, bigKeys: p.valueKb >= 512 ? 1 : 0, peakQps: hotRequests, valueKb: p.valueKb };
  const samples = [`product:hot · ${hotRequests} req`, `product:other · ${p.requests - hotRequests} req`, `payload:1 · ${p.valueKb} KB`];
  emitFrame(frames, 'traffic', `采样 ${p.requests} 次请求，统计 Key 访问分布。`, 33 , metrics, samples, 'DIAGNOSTICS');
  if (metrics.hotKeys) {
    emitFrame(frames, 'hotkey', `product:hot 占 ${p.hotRatio}%，判定为流量热点。`, 37 , metrics, samples, 'DIAGNOSTICS');
  } else {
    emitFrame(frames, 'hotkey', '访问分布较均匀，未发现明显热点。', 39 , metrics, samples, 'DIAGNOSTICS');
  }
  emitFrame(frames, 'memory', `MEMORY USAGE payload:1 返回约 ${p.valueKb} KB。`, 42 , metrics, samples, 'DIAGNOSTICS');
  if (metrics.bigKeys) {
    emitFrame(frames, 'scan', '发现大 Key：应拆分数据并避免在线 DEL 阻塞。', 45 , metrics, samples, 'DIAGNOSTICS');
  } else {
    emitFrame(frames, 'scan', '当前样本未达到教学阈值 512 KB。', 47 , metrics, samples, 'DIAGNOSTICS');
  }
  emitFrame(frames, 'result', `诊断完成：热点 Key ${metrics.hotKeys} 个，大 Key ${metrics.bigKeys} 个。`, 51 , metrics, samples, 'DIAGNOSTICS');
  return frames;
}

function redisCapstone(p) {
  const frames = [];
  const metrics = { dbQueries: 0, peakDb: 0, stale: 0, risks: 0 };
  const risks = [];
  emitFrame(frames, 'gateway', `商品详情服务进入故障演练，数据库容量 ${p.dbCapacity}/s。`, 61 , metrics, risks, 'RISK REGISTER');

  emitFrame(frames, 'gateway', `${p.missing} 个不存在商品 ID 到达入口。`, 63 , metrics, risks, 'RISK REGISTER');
  if (p.bloom) {
    emitFrame(frames, 'protection', '布隆过滤器拦截确定不存在的 ID，数据库无额外查询。', 66 , metrics, risks, 'RISK REGISTER');
  } else {
    metrics.dbQueries += p.missing;
    metrics.peakDb = Math.max(metrics.peakDb, p.missing);
    metrics.risks++;
    risks.push('缓存穿透');
    emitFrame(frames, 'db', `不存在的 Key 造成 ${p.missing} 次数据库查询。`, 68 , metrics, risks, 'RISK REGISTER');
  }

  emitFrame(frames, 'cache', `热点商品缓存到期，${p.concurrent} 个请求同时未命中。`, 74 , metrics, risks, 'RISK REGISTER');
  const rebuildQueries = p.rebuild === 'none' ? p.concurrent : 1;
  metrics.dbQueries += rebuildQueries;
  metrics.peakDb = Math.max(metrics.peakDb, rebuildQueries);
  if (p.rebuild === 'none') {
    metrics.risks++;
    risks.push('缓存击穿');
  } else if (p.rebuild === 'logical') {
    metrics.stale += p.concurrent;
  }
  emitFrame(frames, p.rebuild === 'none' ? 'db' : 'protection', `${p.rebuild === 'none' ? '无重建协调' : p.rebuild === 'mutex' ? '互斥重建' : '逻辑过期'}：数据库查询 ${rebuildQueries} 次。`, 86 , metrics, risks, 'RISK REGISTER');

  const avalancheTotal = 24;
  const avalanchePeak = p.jitter ? Math.ceil(avalancheTotal / 6) : avalancheTotal;
  metrics.dbQueries += avalancheTotal;
  metrics.peakDb = Math.max(metrics.peakDb, avalanchePeak);
  if (!p.jitter) {
    metrics.risks++;
    risks.push('缓存雪崩');
  }
  emitFrame(frames, p.jitter ? 'protection' : 'db', `${avalancheTotal} 个 Key ${p.jitter ? '分散到 6 秒到期' : '同时到期'}，峰值 ${avalanchePeak}/s。`, 95 , metrics, risks, 'RISK REGISTER');

  if (p.safeWrite) {
    emitFrame(frames, 'cache', '商品更新先提交数据库，再删除缓存；最终缓存为空并等待回填。', 105 , metrics, risks, 'RISK REGISTER');
  } else {
    metrics.stale++;
    metrics.risks++;
    risks.push('最终脏缓存');
    emitFrame(frames, 'cache', '先删除缓存的竞态让旧版本被并发读重新回填。', 105 , metrics, risks, 'RISK REGISTER');
  }

  emitFrame(frames, 'result', metrics.risks ? `演练完成：发现 ${metrics.risks} 类风险，峰值回源 ${metrics.peakDb}/s。` : `演练完成：四类风险均已治理，峰值回源 ${metrics.peakDb}/s。`, 115 , metrics, risks, 'RISK REGISTER');
  return frames;
}

export const redisRunners = {
  'redis-types': redisTypes,
  'redis-expiry': redisExpiry,
  'redis-penetration': redisPenetration,
  'redis-breakdown': redisBreakdown,
  'redis-avalanche': redisAvalanche,
  'redis-consistency': redisConsistency,
  'redis-atomic': redisAtomic,
  'redis-lock': redisLock,
  'redis-persistence': redisPersistence,
  'redis-ha': redisHa,
  'redis-ops': redisOps,
  'redis-capstone': redisCapstone,
};
