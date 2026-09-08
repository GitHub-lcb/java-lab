const q = (id, lessonId, topic, prompt, choices, answer, explanation) => ({ id, lessonId, topic, prompt, choices, answer, explanation });
const stage = (id, title, phase, questions) => ({ id, title, phase, passScore: 4, questions });

export const redisStages = [
  stage('foundations', '基础原理阶段测验', '基础原理', [
    q('f-cache-miss', 'redis', 'Cache-Aside', 'Cache-Aside 读取缓存未命中后，通常怎样继续？', ['直接返回空值', '读取数据库并回填缓存', '删除数据库'], 1, 'Cache-Aside 在未命中时读取数据源，成功后回填缓存。'),
    q('f-list-set', 'redis-types', '数据类型', '哪项准确描述 List 与 Set？', ['两者都自动去重', 'List 保留重复，Set 成员唯一', 'Set 保留插入顺序且允许重复'], 1, 'List 可保存重复元素；Set 的成员具有唯一性。'),
    q('f-zset', 'redis-types', 'Sorted Set', '实时排行榜按分数查询，优先使用什么？', ['Hash', 'List', 'Sorted Set'], 2, 'Sorted Set 使用唯一 member 与 score 维护排序。'),
    q('f-expiry-eviction', 'redis-expiry', 'TTL 与淘汰', 'TTL 到期与 maxmemory 淘汰是什么关系？', ['同一个触发机制', '两个独立机制', '只有 Cluster 才区分'], 1, 'TTL 控制过期；达到 maxmemory 后才按策略考虑淘汰。'),
    q('f-noeviction', 'redis-expiry', '内存策略', 'noeviction 达到内存上限后会怎样？', ['拒绝增加内存的写命令', '自动淘汰最旧 Key', '清空所有过期时间'], 0, 'noeviction 不主动淘汰现有数据，而是让增加内存的写入失败。'),
  ]),
  stage('governance', '缓存治理阶段测验', '缓存治理', [
    q('g-penetration', 'redis-penetration', '缓存穿透', '缓存穿透描述的是哪种情况？', ['单个热点 Key 过期', '大量 Key 同时过期', '请求的数据在缓存和数据库都不存在'], 2, '穿透请求在缓存和数据源都找不到结果，因此会反复落到数据源。'),
    q('g-bloom', 'redis-penetration', '布隆过滤器', '布隆过滤器返回“可能存在”后应该怎样？', ['直接认定存在', '继续查询缓存或数据源', '删除该 Key'], 1, '布隆过滤器允许假阳性，“可能存在”仍需后续查询确认。'),
    q('g-mutex', 'redis-breakdown', '缓存击穿', '互斥重建如何降低热点 Key 击穿压力？', ['让所有请求同时查数据库', '只允许一个请求重建，其余等待或重试', '永久返回旧值'], 1, '同一 Key 的并发回源被合并成一次数据库查询。'),
    q('g-logical', 'redis-breakdown', '逻辑过期', '逻辑过期的主要代价是什么？', ['可能短暂返回旧数据', '无法使用 Redis', '必须关闭数据库'], 0, '逻辑过期优先保障延迟和可用性，因此允许短暂旧值。'),
    q('g-avalanche', 'redis-avalanche', '缓存雪崩', 'TTL 抖动为什么能缓解雪崩？', ['减少数据总量', '把失效事件分散到时间窗口', '让数据库容量无限'], 1, '抖动不减少总回源，而是降低同一时刻的峰值。'),
  ]),
  stage('correctness', '并发正确性阶段测验', '并发正确性', [
    q('c-write-order', 'redis-consistency', '缓存一致性', '常见 Cache-Aside 更新顺序是什么？', ['先更新数据库，再删除缓存', '只更新缓存', '先删数据库再写缓存'], 0, '先提交数据库，再删除缓存通常更容易最终收敛，但删除失败仍需补偿。'),
    q('c-stale-window', 'redis-consistency', '一致性窗口', '最终缓存为空是否代表过程中从未发生旧读？', ['代表', '不代表，删除前仍可能命中旧值', '仅 Hash 类型代表'], 1, '最终收敛和操作过程中的短暂旧读是不同问题。'),
    q('c-transaction', 'redis-atomic', 'Redis 事务', 'MULTI/EXEC 中运行时错误是否会自动回滚之前的命令？', ['会', '不会', '仅 Sentinel 会'], 1, 'Redis 事务不提供关系数据库式的运行时自动回滚。'),
    q('c-pipeline', 'redis-atomic', 'Pipeline', 'Pipeline 的核心作用是什么？', ['减少网络往返', '自动提供事务原子性', '自动回滚命令'], 0, 'Pipeline 批量发送以减少 RTT，本身不等于原子事务。'),
    q('c-unlock', 'redis-lock', '安全解锁', '为什么释放分布式锁要原子比较 token 再删除？', ['防止旧客户端删除新持有者的锁', '为了增加 TTL', '为了压缩 Value'], 0, '租约到期后锁可能已被别人获得，旧客户端不能直接 DEL。'),
  ]),
  stage('reliability', '可靠性与运维阶段测验', '可靠性与运维', [
    q('r-aof', 'redis-persistence', 'AOF', 'AOF everysec 的典型折中是什么？', ['每条命令都同步且绝不丢失', '可能丢失约一秒写入，降低同步开销', '只记录读取命令'], 1, 'everysec 通常在性能和约一秒的数据丢失窗口之间折中。'),
    q('r-backup', 'redis-persistence', '备份恢复', '启用 RDB/AOF 后是否还需要独立备份？', ['不需要', '需要，并应进行恢复演练', '只有 Cluster 需要'], 1, '持久化无法替代应对误删、逻辑错误和灾难的独立备份。'),
    q('r-replication', 'redis-ha', '异步复制', '主节点已经返回成功，故障时还可能丢失写入吗？', ['可能，副本也许尚未收到', '不可能', '只有 Set 类型可能'], 0, '主节点确认进度可能领先于异步复制进度。'),
    q('r-sentinel-cluster', 'redis-ha', '高可用与分片', 'Sentinel 和 Cluster 的核心区别之一是什么？', ['Sentinel 支持 SQL', 'Cluster 不支持副本', 'Sentinel 管理单主高可用，Cluster 还提供分片'], 2, 'Sentinel 不做数据分片；Cluster 将 hash slots 分布到多个主节点。'),
    q('r-hot-big', 'redis-ops', '线上诊断', '关于热 Key 与大 Key，哪项正确？', ['是同一个概念', '访问频率与对象大小是两个独立维度', '只在单机模式存在'], 1, '小 Key 可能很热，大 Key 也可能很少被访问。'),
  ]),
];

export const jvmStages = [
  stage('jvm-bytecode-loading', '字节码与类加载阶段测验', '字节码与类加载', [
    q('jvm-bc-iadd', 'jvm-bytecode', '操作数栈', 'iadd 执行时，两个 int 操作数来自哪里？', ['Class 文件常量池', '当前栈帧的操作数栈', 'Java 堆'], 1, 'iadd 从当前栈帧的操作数栈弹出两个 int，并把计算结果压回该栈。'),
    q('jvm-bc-slot', 'jvm-bytecode', '局部变量槽位', '静态方法 add(int a, int b) 中，slot 0 通常保存什么？', ['this 引用', '参数 a', '返回地址'], 1, '静态方法没有 this，参数从 slot 0 开始；实例方法的 slot 0 通常保存 this。'),
    q('jvm-cl-identity', 'jvm-classloading', '类身份', 'JVM 中一个类的身份由什么共同确定？', ['类名和定义它的 ClassLoader', '源文件路径和包大小', '线程名和堆地址'], 0, '相同类名由不同 ClassLoader 定义，仍可能形成 JVM 中不同的类型。'),
    q('jvm-cl-init', 'jvm-classloading', '初始化时机', '已经加载 Class 是否表示一定执行了 <clinit>？', ['一定', '不一定，主动使用通常才触发初始化', '只要经过 Bootstrap 就一定'], 1, '加载、链接和初始化是不同阶段，加载完成不等于类初始化已执行。'),
    q('jvm-cl-bootstrap', 'jvm-classloading', 'Bootstrap', 'Class.getClassLoader() 返回 null 通常表示什么？', ['类没有被加载', '由 Bootstrap ClassLoader 加载', '发生了类加载错误'], 1, 'Java API 使用 null 表示 Bootstrap ClassLoader，而不是表示不存在加载器。'),
  ]),
  stage('jvm-memory-allocation', '内存区域与对象分配阶段测验', '内存与对象', [
    q('jvm-mem-private', 'jvm-memory', '线程私有区域', '增加线程数量时，哪类内存开销通常会随线程分别增加？', ['Java 堆完整副本', 'Java 虚拟机栈', 'Class 文件常量池副本'], 1, '每个线程拥有自己的 PC 和栈；同一 JVM 中的 Java 堆由线程共享。'),
    q('jvm-mem-reference', 'jvm-memory', '引用与对象', '局部变量 order 与它指向的 Order 对象通常位于哪里？', ['都必定位于栈', '引用槽位可在栈帧，对象通常在堆', '都位于方法区'], 1, '引用变量所在槽位与被引用对象是两个不同的存储概念。'),
    q('jvm-mem-error', 'jvm-memory', '故障映射', '无限递归最直接对应哪类故障？', ['StackOverflowError', 'ClassNotFoundException', 'ArithmeticException'], 0, '每次递归创建新栈帧，线程栈无法继续扩展时可能抛出 StackOverflowError。'),
    q('jvm-alloc-tlab', 'jvm-allocation', 'TLAB', 'TLAB 为什么能降低常见小对象分配的竞争？', ['每个线程在预留区域内移动分配指针', '对象不再进入堆', '关闭了垃圾回收'], 0, '线程可在自己的 TLAB 内使用指针碰撞分配，减少共享堆顶上的协调。'),
    q('jvm-alloc-slow', 'jvm-allocation', '分配慢路径', '启用 TLAB 后，是否完全不再经过共享分配路径？', ['是', '否，TLAB 补充等仍需协调', '只有数组会经过'], 1, 'TLAB 本身需要从共享堆获得，补充、策略限制和特殊对象仍可能进入更重的路径。'),
  ]),
  stage('jvm-gc-foundations', '可达性与分代回收阶段测验', '垃圾回收', [
    q('jvm-gc-cycle', 'jvm-roots', '可达性', 'A 与 B 互相引用，但不存在从 GC Roots 到它们的路径，会怎样？', ['仍可被回收', '因为循环而永久存活', '自动成为静态对象'], 0, '可达性分析从 Roots 遍历，不可达的引用环仍属于回收候选。'),
    q('jvm-gc-transitive', 'jvm-roots', '传递可达', 'Root 引用 A，A 引用 B，B 是否可达？', ['不可达，必须由 Root 直接引用', '可达，引用链具有传递性', '只有 B 是数组时可达'], 1, '只要存在从任一 Root 出发的引用路径，对象就是可达的。'),
    q('jvm-gc-weak', 'jvm-roots', '弱引用观测', '调用 System.gc() 后，WeakReference 是否保证立即被清除？', ['保证', '不保证，GC 请求和引用处理时机由 JVM 决定', '仅 Java 8 保证'], 1, 'System.gc() 是请求，不是立即回收契约；一次仍可达的观测不能证明对象永久存活。'),
    q('jvm-gc-young', 'jvm', 'Young GC', 'Young GC 与 Full GC 的关系是什么？', ['完全相同', 'Young GC 主要处理年轻代，不等于 Full GC', 'Young GC 只处理类元数据'], 1, 'Young GC 和 Full GC 的触发范围、停顿与收集器行为不同，不能混为一谈。'),
    q('jvm-gc-survival', 'jvm', '存活率', '年轻代对象存活率升高，通常会带来什么压力？', ['复制或晋升工作可能增加', '每次一定释放更多空间', '保证以后不再 GC'], 0, '更多对象存活意味着本轮释放空间减少，并增加存活对象复制或晋升成本。'),
  ]),
  stage('jvm-collector-selection', '垃圾收集器选择阶段测验', '性能与诊断', [
    q('jvm-col-serial', 'jvm-collectors', 'Serial', 'Serial 收集器停顿阶段最典型的执行方式是什么？', ['单个 GC 工作线程', '无限个并发线程', '只由应用线程执行'], 0, 'Serial 使用单个 GC 工作线程完成相应回收阶段，适合资源受限或小型堆等场景。'),
    q('jvm-col-parallel', 'jvm-collectors', 'Parallel', 'Parallel GC 的主要优化目标是什么？', ['吞吐量', '硬实时零停顿', '减少 Class 文件大小'], 0, 'Parallel 使用多个 GC 工作线程并行回收，主要面向吞吐量。'),
    q('jvm-col-g1', 'jvm-collectors', 'G1', 'G1 为什么把堆划分为多个 Region？', ['可以按收益选择回收集合并逐步回收', '保证对象永不晋升', '让所有对象都进入线程栈'], 0, 'Region 化让 G1 可以选择一组区域进行疏散，并结合停顿目标安排回收集合。'),
    q('jvm-col-target', 'jvm-collectors', '停顿目标', 'G1 的停顿目标应怎样理解？', ['硬性 SLA', '启发式调度的软目标', '网络请求超时'], 1, '收集器尽力根据预测选择回收集合，但目标不是每次停顿的硬上限。'),
    q('jvm-col-evidence', 'jvm-collectors', '观测边界', 'MXBean 返回当前收集器名称后，可以直接得出什么？', ['当前 JVM 使用的收集器身份', '三种收集器的性能排名', '生产业务的最佳参数'], 0, 'MXBean 能识别当前 JVM 的收集器与累计指标，不能代替受控跨进程基准。'),
  ]),
];

export const learningModules = {
  Redis: {
    name: 'Redis',
    eyebrow: 'REDIS CURRICULUM',
    pathTitle: 'Redis 核心学习路径',
    reportDescription: 'Redis 专题学习证据完成度',
    completionNote: '核心课程证据已完整，可以进入真实 Java + Redis 环境练习。',
    nextNote: '优先完成缺失的对照场景和阶段测验；错题连续答对两次后再进入真实环境。',
    stages: redisStages,
  },
  JVM: {
    name: 'JVM',
    eyebrow: 'JVM CURRICULUM',
    pathTitle: 'JVM 核心学习路径',
    reportDescription: 'JVM 专题学习证据完成度',
    completionNote: '基础学习证据已完整，可以进入收集器对比、GC 日志和内存故障诊断。',
    nextNote: '优先补齐实验与对照场景，再通过阶段测验；错题连续答对两次后移出复习队列。',
    stages: jvmStages,
  },
};

export function getLearningModule(moduleName) {
  return learningModules[moduleName] || learningModules.Redis;
}
