const eq = value => ({ kind: 'equals', value });
const range = (min, max) => ({ kind: 'integerRange', min, max });
const step = (id, title, command, expected, expectText, why, retry) => ({ id, title, command, expected, expectText, why, retry: retry || '检查执行顺序和练习数据。必要时从本课第一步重新开始。' });
const clean = keys => step('clean', '建立可重复的初始状态', `DEL ${keys}`, range(0, keys.split(' ').length), '非负整数：删除的 Key 数', '仅删除当前课程会话中的这些练习 Key，使重复练习拥有相同起点。');
const set = (id, key, value) => step(id, '写入练习值', `SET ${key} ${value}`, eq('OK'), 'OK', 'SET 创建或覆盖字符串值。这是显式写入，不是 Redis 自动查询数据库。');
const get = (id, key, value, why) => step(id, '读取并核对结果', `GET ${key}`, eq(value), value === null ? '(nil)' : JSON.stringify(value), why);
function guide(objective, scope, steps) {
  return { objective, scope, steps, commands: steps.map(item => item.command), check: '按顺序完成返回值验证。通过表示本命令实验完成，不等于掌握本课所有生产场景。' };
}
export const realLabCatalog = {
  redis: guide('区分未命中、回填、命中和空字符串，验证缓存失效后的状态。', '真实执行缓存侧命令；由你模拟应用回填，不连接业务数据库或测量缓存性能。', [
    clean('product:1 empty:1'),
    get('miss', 'product:1', null, 'nil 表示 Key 不存在，不说明数据库也没有商品。'),
    set('fill', 'product:1', '"Java Book"'),
    get('hit', 'product:1', 'Java Book', '相同 GET 现在读到了值。前后状态由 SET 改变，Redis 不会自动回源。'),
    set('empty', 'empty:1', '""'),
    get('empty-read', 'empty:1', '', '空字符串是存在的值。不能用真假值代替 null 判断未命中。'),
    step('invalidate', '让缓存失效', 'DEL product:1', eq(1), '1', 'DEL 返回删除的 Key 数，不是字符串 OK。'),
    get('miss-again', 'product:1', null, '删除之后再次独立验证 nil，应用下一次读取可以重新回源。'),
  ]),
  'redis-types': guide('比较五种类型，验证重复、顺序与更新语义。', '验证逻辑类型和命令语义；不展示内部编码转换、内存开销或大规模性能。', [
    clean('value:1 user:1 queue:1 tags rank'), set('string', 'value:1', 'hello'),
    step('string-type', '检查逻辑类型', 'TYPE value:1', eq('string'), 'string', 'TYPE 返回逻辑类型，不是 SDS、listpack 等内部编码。'),
    step('hash-new', '新增 Hash 字段', 'HSET user:1 name Alice', eq(1), '1', 'HSET 返回新增字段数。'),
    step('hash-update', '覆盖 Hash 字段', 'HSET user:1 name Bob', eq(0), '0', '0 不是失败；name 已存在，只更新值，没有新增字段。'),
    step('hash-read', '核对字段值', 'HGET user:1 name', eq('Bob'), 'Bob', '更新已经生效，字段数量不变。'),
    step('list-add', '保留重复元素', 'RPUSH queue:1 java redis java', eq(3), '3', 'RPUSH 返回写入后的列表长度，允许重复值。'),
    step('list-read', '核对插入顺序', 'LRANGE queue:1 0 -1', eq(['java', 'redis', 'java']), '[java, redis, java]', '列表顺序和重复项都应保留。'),
    step('set-add', '集合成员去重', 'SADD tags java redis java', eq(2), '2', '相同成员只保存一次，新增成员只有两个。'),
    step('set-read', '核对集合成员', 'SMEMBERS tags', { kind: 'set', value: ['java', 'redis'] }, 'java 和 redis，顺序不限', 'Set 不保证输出顺序，校验成员而非返回顺序。'),
    step('rank-add', '创建排行成员', 'ZADD rank 98 alice', eq(1), '1', 'Sorted Set 的 member 唯一，score 用于排序。'),
    step('rank-update', '更新已有成员分数', 'ZADD rank 99 alice', eq(0), '0', '更新 score 不新增成员，默认 ZADD 返回 0。'),
    step('rank-read', '检查成员与分值', 'ZRANGE rank 0 -1 WITHSCORES', eq(['alice', '99']), '[alice, 99]', 'score 以 RESP 字符串返回；排行榜仍只有一个 alice。'),
  ]),
  'redis-expiry': guide('识别 TTL 的 -1、-2，验证 SET 清除 TTL 和 Key 自然到期。', '真实验证过期语义；不修改实例 maxmemory，不执行淘汰压力测试。', [
    clean('session:1'),
    step('absent', '不存在的 Key', 'TTL session:1', eq(-2), '-2', 'Key 不存在时 TTL 返回 -2。'),
    set('persistent', 'session:1', 'active'),
    step('no-expiry', '没有到期时间', 'TTL session:1', eq(-1), '-1', 'Key 存在但没有过期时间。'),
    step('expire', '设置过期时间', 'EXPIRE session:1 120', eq(1), '1', '给存在的 Key 设置 120 秒到期时间。'),
    step('countdown', '核对倒计时', 'TTL session:1', range(1, 120), '1 到 120 秒', 'TTL 按秒报告剩余时间，不是最初设置的值。', '如果返回 -2，练习停留过久。重新开始，在 EXPIRE 后及时读取。'),
    set('overwrite', 'session:1', 'renewed'),
    step('ttl-cleared', '覆盖后的 TTL', 'TTL session:1', eq(-1), '-1', '普通 SET 会清除原 TTL。需要保留时应显式用 KEEPTTL，或重新指定 EX/PX。'),
    step('short-lease', '设置短期过期', 'EXPIRE session:1 1', eq(1), '1', '设置 1 秒后到期，下一步观察自然失效。'),
    step('gone', '等待至少 1 秒后读取', 'GET session:1', eq(null), '(nil)', '到期后读取视为不存在，无需应用为每个 Key 创建定时器。', '仍返回 renewed 表示尚未到期，稍等后重试这一步。'),
    step('gone-ttl', '确认 Key 已不存在', 'TTL session:1', eq(-2), '-2', '过期后返回 -2，与最初不存在时相同。'),
  ]),
  'redis-atomic': guide('验证重复扣减，再复现“单命令原子仍可扣成负数”的边界。', '只验证 INCR/DECR 语义；此终端不执行 WATCH 事务或任意 Lua，也不模拟并发订单。', [
    clean('stock:sku1'), set('stock', 'stock:sku1', '2'),
    step('first-decrement', '扣减第一件', 'DECR stock:sku1', eq(1), '1', 'DECR 返回扣减后的整数。'),
    step('second-decrement', '扣减第二件', 'DECR stock:sku1', eq(0), '0', '同一条 DECR 必须再次执行，上一条返回 1 不能证明本步骤完成。'),
    step('oversell', '库存为零时继续扣减', 'DECR stock:sku1', eq(-1), '-1', '原子性防止命令内部被打断，却不自动增加“库存大于零”的业务条件。'),
    get('negative', 'stock:sku1', '-1', 'GET 返回字符串 "-1"，与 DECR 的整数 -1 类型不同。'),
    step('repair', '补回一件', 'INCR stock:sku1', eq(0), '0', '演示恢复数值；生产库存补偿仍需订单幂等和一致性设计。'),
  ]),
  'redis-penetration': guide('验证不存在值与应用定义的空值标记。', '负缓存命令练习，不调用 Bloom 模块，不统计数据库回源。', [
    clean('product:missing:1'), get('missing', 'product:missing:1', null, '缓存 nil 不能直接证明数据库也没有商品。'),
    step('sentinel', '写入短期空值标记', 'SET product:missing:1 __NULL__ EX 120', eq('OK'), 'OK', '__NULL__ 是应用约定，Redis 不赋予它特殊空值含义。'),
    get('negative-hit', 'product:missing:1', '__NULL__', '应用识别标记后避免重复回源；标记必须避免与合法业务值混淆。'),
    step('ttl', '确认标记会到期', 'TTL product:missing:1', range(1, 120), '1 到 120 秒', '新商品不能被永久隐藏，负缓存需要短 TTL 或主动失效。'),
  ]),
  'redis-breakdown': guide('观察热点缓存失效前后的状态。', '单请求预备实验，不模拟并发回源、重建锁或等待队列。', [
    clean('product:hot'), set('hot', 'product:hot', 'old-value'), get('hit', 'product:hot', 'old-value', '热点由访问流量决定，不是特殊的 Redis 类型。'),
    step('invalidate', '主动失效热点', 'DEL product:hot', eq(1), '1', '删除模拟未命中起点，未真实触发并发数据库查询。'),
    get('miss', 'product:hot', null, '若许多请求同时进入这个窗口，才可能放大数据库压力。'),
  ]),
  'redis-avalanche': guide('对比一组 Key 的不同剩余 TTL。', '观察到期时间差，不测吞吐量或过载；每步操作时间也会影响 TTL。', [
    clean('batch:1 batch:2'),
    step('a', '设置第一组 TTL', 'SET batch:1 value EX 120', eq('OK'), 'OK', '第一组 Key 设置 120 秒到期。'),
    step('b', '设置第二组 TTL', 'SET batch:2 value EX 180', eq('OK'), 'OK', '不同 TTL 分散到期时间，但不能应对整个 Redis 故障。'),
    step('ttl-a', '读取第一组倒计时', 'TTL batch:1', range(1, 120), '1 到 120 秒', '结果取决于写入到读取经过的时间。'),
    step('ttl-b', '读取第二组倒计时', 'TTL batch:2', range(1, 180), '1 到 180 秒', '这个小样本不是随机分布的性能实验。'),
  ]),
  'redis-consistency': guide('核对旧值、删除、新值回填的缓存侧时序。', '不连接数据库，不证明所有并发交错均一致。', [
    clean('product:1'), set('old', 'product:1', 'v1'), get('old-read', 'product:1', 'v1', '缓存里当前保存旧版本。'),
    step('delete', '执行失效', 'DEL product:1', eq(1), '1', '通常在数据库事务提交后执行；本练习没有数据库提交动作。'),
    get('miss', 'product:1', null, '删除后下一次读需要回源。'), set('new', 'product:1', 'v2'),
    get('new-read', 'product:1', 'v2', '这个时序收敛到 v2，不代表没有删除失败和旧读窗口。'),
  ]),
  'redis-lock': guide('验证 NX 加锁成功、竞争失败和租约到期。', '只验证单实例租约，不执行安全解锁 Lua。GET 后 DEL 并不原子，不能作为生产解锁方案。', [
    clean('lock:order:1'),
    step('acquire', 'A 获取租约', 'SET lock:order:1 token-a NX EX 120', eq('OK'), 'OK', 'NX 要求 Key 不存在，EX 与写入在同一命令中设置租约。'),
    step('contend', 'B 竞争同一租约', 'SET lock:order:1 token-b NX EX 120', eq(null), '(nil)', '语法合法但条件不满足。HTTP 成功不等于加锁成功。'),
    get('owner', 'lock:order:1', 'token-a', 'B 未覆盖 A。读取仅供观察，不能与 DEL 分开拼成安全解锁。'),
    step('shorten', '将教学租约缩为 1 秒', 'EXPIRE lock:order:1 1', eq(1), '1', '仅用于构造到期场景，生产续期需要原子校验持有者。'),
    step('expired', '等待至少 1 秒检查到期', 'TTL lock:order:1', eq(-2), '-2', '租约已不存在，但业务线程未必结束。', '返回 0 或 1 请稍等后重试，TTL=0 不代表 Key 已不存在。'),
  ]),
  'redis-persistence': guide('准备可供后续恢复演练使用的检查值。', '未检测持久化配置，不重启用户 Redis，不判定恢复演练完成。', [
    clean('durable:order:1 durable:sequence'), set('order', 'durable:order:1', 'paid'),
    step('sequence', '建立序列', 'INCR durable:sequence', eq(1), '1', '不存在的计数器从 0 开始递增。'),
    get('order-read', 'durable:order:1', 'paid', '内存可读不能证明已经落盘。'),
    get('sequence-read', 'durable:sequence', '1', '需另行配置并执行恢复演练，才能验证数据丢失窗口。'),
  ]),
  'redis-ha': guide('建立当前连接上的读写探针。', '不检测 Sentinel/Cluster，不触发故障转移；PONG 只证明当前节点可响应。', [
    clean('ha:probe'), step('ping', '检查连接', 'PING', eq('PONG'), 'PONG', '一次 PONG 不是集群健康或复制一致性的证明。'),
    set('probe', 'ha:probe', 'ready'), get('read', 'ha:probe', 'ready', '当前节点可读写这个探针。'),
    step('exists', '检查存在性', 'EXISTS ha:probe', eq(1), '1', '切换验证还需结合拓扑和复制位点，本练习未执行切换。'),
  ]),
  'redis-ops': guide('区分存在性、类型和过期属性。', '不测内存字节数、热点频率和慢查询。', [
    clean('payload:1'), set('sample', 'payload:1', 'sample'),
    step('type', '查询类型', 'TYPE payload:1', eq('string'), 'string', 'TYPE 不能说明占用多少内存或有多热。'),
    step('exists', '检查存在', 'EXISTS payload:1', eq(1), '1', '存在性独立于内容和大小。'),
    step('pttl', '检查过期属性', 'PTTL payload:1', eq(-1), '-1', 'PTTL 的正数单位为毫秒，-1 仍表示未设置过期。'),
  ]),
  'redis-capstone': guide('核对商品缓存从未命中到新版本回填的生命周期。', '不执行完整应用、数据库并发或故障治理组合。', [
    clean('product:9001'), get('missing', 'product:9001', null, '这个课程会话最初没有商品缓存。'),
    set('v1', 'product:9001', 'v1'), get('read-v1', 'product:9001', 'v1', '首次回填后命中 v1。'),
    step('invalidate', '使旧缓存失效', 'DEL product:9001', eq(1), '1', '旧缓存被删除。'),
    get('missing-again', 'product:9001', null, '这次 nil 必须在删除后独立验证。'),
    set('v2', 'product:9001', 'v2'), get('read-v2', 'product:9001', 'v2', '读到 v2 才能证明回填结果，而不只看 SET 是否成功。'),
  ]),
};
