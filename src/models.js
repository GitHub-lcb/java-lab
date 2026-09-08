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
const runners = { redis, threadpool, hashmap, kafka, mysql, jvm, ...redisRunners, ...jvmRunners };
export function simulate(id, params) {
  if (!runners[id]) throw new Error(`Unknown lab: ${id}`);
  return runners[id](params);
}
