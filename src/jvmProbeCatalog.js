export const jvmProbeCatalog = {
  'jvm-bytecode': { probe: 'bytecode', title: '真实 javap 字节码', objective: '由本机 JDK 对固定 ArithmeticSample.class 执行 javap -c -p。', evidence: ['输出包含 iload、iadd、ireturn', 'Class 来自当前编译产物'], boundary: '只反汇编项目内固定样例，不接受用户源码、类名或命令参数。' },
  'jvm-classloading': { probe: 'classloaders', title: '实际 ClassLoader 链', objective: '读取 String 与网关样例类的真实定义加载器和父加载器。', evidence: ['String 的 API 加载器值为 null，代表 Bootstrap', '样例类由应用加载器定义'], boundary: '观察当前网关 JVM；不同 JDK 版本的加载器类名会不同。' },
  'jvm-memory': { probe: 'memory', title: '实际内存池快照', objective: '读取 MemoryMXBean 与全部 MemoryPoolMXBean 的当前值。', evidence: ['堆 used、committed、max', '非堆和具体内存池名称'], boundary: '这是采样瞬间的真实值，不是堆转储，也不能单独证明泄漏。' },
  'jvm-allocation': { probe: 'allocation', title: '线程分配字节观测', objective: '在网关线程中创建 256 个 1 KiB 数组，并读取线程分配字节差值。', evidence: ['业务 payload 为 262144 字节', '实际分配通常还包含数组头和引用数组'], boundary: '分配字节由 HotSpot ThreadMXBean 提供；不等于对象存活量、堆增量或 TLAB 慢路径次数。' },
  'jvm-roots': { probe: 'references', title: '弱引用回收观测', objective: '移除强引用后请求 GC，观察 WeakReference 与 ReferenceQueue。', evidence: ['可能观察到弱引用被清除', 'System.gc 只是请求，结果允许未立即回收'], boundary: '不生成 heap dump，不能枚举真实 GC Roots；单次未回收也不表示对象永远存活。' },
  jvm: { probe: 'gc', title: '实际垃圾收集器指标', objective: '读取 GarbageCollectorMXBean 的收集器名称、累计次数和耗时。', evidence: ['名称取决于当前 JDK 与启动参数', 'count/time 是网关 JVM 启动后的累计值'], boundary: 'MXBean 累计指标不是某一次课程演示的独立耗时，也不区分所有停顿阶段。' },
  'jvm-collectors': { probe: 'gc', title: '识别当前 JVM 收集器', objective: '读取当前网关 JVM 的 GarbageCollectorMXBean 名称、累计次数和耗时。', evidence: ['收集器名称来自当前 JVM，而不是前端推测', '一次网关进程只证明当前启动配置'], boundary: '该探针不会切换 JVM 启动参数，也不产生 Serial、Parallel、G1 的横向性能基准。' },
};
