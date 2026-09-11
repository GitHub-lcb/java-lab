const snapshot = (frames, active, message, code, metrics, items, dataLabel) => frames.push(structuredClone({ active, message, code, metrics, items, dataLabel }));

function bytecode(p) {
  const frames = [];
  const stack = [];
  const locals = { 0: p.left, 1: p.right };
  const metrics = { pc: 0, stackDepth: 0, localVariables: 2, result: 0 };
  const emit = (active, instruction, message, code) => {
    metrics.stackDepth = stack.length;
    snapshot(frames, active, message, code, metrics, stack.map((value, index) => `slot ${index} · ${value}`), 'OPERAND STACK');
    frames[frames.length - 1].instruction = instruction;
  };
  emit('classfile', 'ready', '方法 add(int a, int b) 已进入解释执行。', 13 );
  metrics.pc++; stack.push(locals[0]); emit('locals', 'iload_0', `iload_0：把局部变量 a=${p.left} 压入操作数栈。`, 14 );
  metrics.pc++; stack.push(locals[1]); emit('locals', 'iload_1', `iload_1：把局部变量 b=${p.right} 压入操作数栈。`, 15 );
  metrics.pc++; const right = stack.pop(); const left = stack.pop(); stack.push(left + right); emit('stack', 'iadd', `iadd：弹出 ${right} 与 ${left}，压入和 ${left + right}。`, 16 );
  metrics.pc++; metrics.result = stack.pop(); emit('return', 'ireturn', `ireturn：返回栈顶整数 ${metrics.result}，当前栈帧结束。`, 17 );
  return frames;
}

function classloading(p) {
  const frames = [];
  const metrics = { requested: 1, delegated: 0, defined: 0, verified: 0, initialized: 0, owner: '等待' };
  const visited = [];
  const emit = (active, message, code) => snapshot(frames, active, message, code, metrics, [...visited], 'DELEGATION TRACE');
  emit('request', `请求加载 ${p.target}。`, 67 );
  const chain = p.custom ? ['Custom', 'Application', 'Extension', 'Bootstrap'] : ['Application', 'Extension', 'Bootstrap'];
  for (const loader of chain) {
    metrics.delegated++;
    visited.push(loader);
    emit(loader === 'Bootstrap' ? 'bootstrap' : loader === 'Application' ? 'application' : 'parent', `${loader} ClassLoader 接到委派请求。`, 18 );
  }
  const jdkClass = p.target.startsWith('java.');
  metrics.owner = jdkClass ? 'Bootstrap' : 'Application';
  metrics.defined = 1;
  emit(jdkClass ? 'bootstrap' : 'application', `${metrics.owner} 找到并定义 ${p.target}。`, 29 );
  metrics.verified = 1;
  emit('link', '验证字节码并为静态字段分配默认值。', 73 );
  metrics.initialized = 1;
  emit('initialize', '首次主动使用触发 <clinit>，类初始化完成。', 74 );
  return frames;
}

function memory(p) {
  const frames = [];
  const metrics = { threads: p.threads, stackFrames: 0, heapObjects: 0, loadedClasses: 0 };
  const items = [];
  const emit = (active, message, code) => snapshot(frames, active, message, code, metrics, items, 'RUNTIME AREAS');
  emit('threads', `创建 ${p.threads} 个 Java 线程。每个线程拥有独立 PC 与 Java 栈。`, 56 );
  metrics.stackFrames = p.threads * p.depth;
  items.push(`${p.threads} stacks · ${metrics.stackFrames} frames`);
  emit('stacks', `每个线程递归深度 ${p.depth}，共形成 ${metrics.stackFrames} 个栈帧。`, 60 );
  metrics.heapObjects = p.objects;
  items.push(`heap · ${p.objects} objects`);
  emit('heap', `${p.objects} 个对象分配在所有线程共享的堆中。`, 78 );
  metrics.loadedClasses = p.classes;
  items.push(`class metadata · ${p.classes}`);
  emit('metadata', `加载 ${p.classes} 个类，其类元数据进入方法区的实现区域。`, 85 );
  emit('result', '局部变量随栈帧退出；仍被引用的堆对象不会因此立即消失。', 90 );
  return frames;
}

function allocation(p) {
  const frames = [];
  const metrics = { allocated: 0, bytes: 0, fastPath: 0, slowPath: 0, tlabRefills: 0 };
  const items = [];
  let remaining = 0;
  const emit = (active, message, code) => snapshot(frames, active, message, code, metrics, [...items], 'ALLOCATION TRACE');
  emit('new', `准备分配 ${p.objects} 个对象，每个教学单位大小 ${p.size}。`, 50 );
  for (let i = 1; i <= p.objects; i++) {
    if (p.tlab) {
      if (remaining < p.size) {
        remaining = Math.max(p.tlabCapacity, p.size);
        metrics.slowPath++;
        metrics.tlabRefills++;
        emit('tlab', `对象 #${i} 前申请新的 TLAB，容量 ${remaining}。`, 31 );
      }
      remaining -= p.size;
      metrics.fastPath++;
      emit('tlab', `对象 #${i} 通过线程本地 bump-the-pointer 快速分配。`, 36 );
    } else {
      metrics.slowPath++;
      emit('heap', `对象 #${i} 进入共享分配路径，需要协调堆顶指针。`, 36 );
    }
    metrics.allocated++;
    metrics.bytes += p.size;
    items.push(`obj-${i} · ${p.size}`);
  }
  emit('result', `完成 ${metrics.allocated} 次分配：快速路径 ${metrics.fastPath}，共享/补充路径 ${metrics.slowPath}。`, 80 );
  return frames;
}

function roots(p) {
  const frames = [];
  const metrics = { objects: p.objects, roots: p.roots, reachable: 0, reclaimed: 0, cyclesReclaimed: 0 };
  const items = Array.from({ length: p.objects }, (_, index) => `obj-${index + 1} · 未标记`);
  const emit = (active, message, code) => snapshot(frames, active, message, code, metrics, [...items], 'MARK STATE');
  emit('objects', `堆中有 ${p.objects} 个对象，其中设置 ${p.roots} 个 GC Root 起点。`, 33 );
  const reachable = Math.min(p.objects, p.roots * 2);
  for (let i = 0; i < reachable; i++) {
    metrics.reachable++;
    items[i] = `obj-${i + 1} · reachable`;
    emit(i < p.roots ? 'roots' : 'mark', `从 ${i < p.roots ? 'GC Root' : '已标记对象'} 到达 obj-${i + 1}。`, 39 );
  }
  if (p.cycle && p.objects - reachable >= 2) {
    items[p.objects - 2] = `obj-${p.objects - 1} ↔ cycle`;
    items[p.objects - 1] = `obj-${p.objects} ↔ cycle`;
    emit('cycle', '两个不可达对象相互引用，但没有从 GC Roots 可达的路径。', 41 );
    metrics.cyclesReclaimed = 1;
  }
  metrics.reclaimed = p.objects - metrics.reachable;
  for (let i = reachable; i < p.objects; i++) items[i] = `obj-${i + 1} · reclaimable`;
  emit('sweep', `标记结束：${metrics.reclaimed} 个不可达对象可回收，包括不可达循环。`, 55 );
  return frames;
}

function collectors(p) {
  const frames = [];
  const names = { serial: 'Serial', parallel: 'Parallel', g1: 'G1' };
  const liveMb = Math.round(p.youngMb * p.livePercent / 100);
  const workers = p.collector === 'serial' ? 1 : p.gcThreads;
  const concurrentWork = p.collector === 'g1' ? Math.ceil(liveMb * 0.6) : 0;
  const pauseInput = p.collector === 'g1' ? Math.ceil(p.youngMb * 0.35) + liveMb : p.youngMb + liveMb;
  const metrics = { pauseWork: Math.ceil(pauseInput / workers), workers, concurrentWork, reclaimedMb: p.youngMb - liveMb, liveMb };
  const items = [`collector · ${names[p.collector]}`, `young workload · ${p.youngMb} MB`, `live set · ${liveMb} MB`];
  const emit = (active, message, code) => snapshot(frames, active, message, code, metrics, items, 'COLLECTOR PLAN');
  emit('workload', `${names[p.collector]} 接到 ${p.youngMb} MB 年轻代负载，其中 ${liveMb} MB 对象存活。`, 39 );
  if (p.collector === 'g1') {
    emit('mark', `G1 把 ${concurrentWork} 个工作单位放入并发标记阶段，应用可与部分 GC 工作重叠。`, 59 );
  } else {
    emit('mark', `${names[p.collector]} 在本模型中把存活识别计入停顿工作，不设置并发工作量。`, 59 );
  }
  emit('pause', `应用进入停顿阶段，需要承担 ${metrics.pauseWork} 个抽象工作单位。`, 64 );
  emit('workers', `${workers} 个 GC 工作线程承担停顿内工作；配置值为 ${p.gcThreads}。`, 64 );
  emit('reclaim', `本轮识别 ${liveMb} MB 存活对象，回收 ${metrics.reclaimedMb} MB。`, 66 );
  return frames;
}

function jit(p) {
  const mode = p.mode || 'tiered';
  const calls = p.calls ?? 600;
  const threshold = p.threshold ?? 200;
  const inlineOn = p.inline ?? true;
  const escapeOn = p.escape ?? true;
  const frames = [];
  const metrics = { interpreted: 0, c1: 0, c2: 0, inlined: 0, escapes: 0 };
  const items = [];
  const sceneTag = mode === 'inline' ? '② 内联与逃逸分析' : mode === 'flags' ? '③ 编译参数 · 门槛对比' : '① 分级编译 · 阈值与热度';
  const label = mode === 'inline' ? 'INLINE + ESCAPE' : mode === 'flags' ? 'THRESHOLD COMPARE' : 'COMPILE TRACE';
  const emit = (active, message, code) => snapshot(frames, active, message, code, metrics, items, label);
  emit('call', `JIT 热点编译教学模型就绪：场景「${sceneTag}」。`, 54 );
  if (mode === 'tiered') {
    const interp = Math.min(calls, 300);
    metrics.interpreted = interp;
    items.push(`调用 1–${interp} · 解释执行`);
    emit('interp', `前 ${interp} 次调用由解释器执行并收集 profile（解释 ${interp}/${calls}）。`, 65 );
    if (calls >= 300) {
      emit('c1', '第 300 次调用越过 C1 阈值：方法被编译为 C1 快速编译代码——编译在调用边界之后生效。', 70 );
      if (calls > 300) {
        const c1 = Math.min(calls, 600) - 300;
        metrics.c1 = c1;
        items.push(`调用 301–${300 + c1} · C1 编译执行`);
        emit('c1', `第 301~${300 + c1} 次在 C1 编译代码上执行（C1 ${c1}）。`, 74 );
      }
      if (calls > 600) {
        emit('c2', '第 600 次调用越过 C2 阈值：触发 C2 深度优化——更激进的内联与标量替换。', 70 );
        metrics.c2 = calls - 600;
        items.push(`调用 601–${calls} · C2 深度优化`);
        emit('c2', `第 601~${calls} 次在 C2 优化代码上执行（C2 ${calls - 600}）。`, 74 );
      }
    }
    emit('call', `运行结束：解释 ${metrics.interpreted} · C1 ${metrics.c1} · C2 ${metrics.c2}——编译发生在越过阈值的下一次调用：热度决定执行方式，调低阈值只提前切换、不改变峰值收益。`, 54 );
  } else if (mode === 'inline') {
    metrics.c2 = calls;
    items.push(`调用 1–${calls} · C2 编译入口`);
    emit('c2', `方法早已越过 C2 阈值：入口是 C2 编译代码，${calls} 次调用全程不经解释器（C2 ${calls}）。`, 54 );
    if (inlineOn) {
      metrics.inlined = 2;
      items.push('内联 2 个调用点 · 无调用开销');
      emit('inline', 'C2 依据 profile 内联 2 个调用点：order() 与 price() 被展开到方法体内——调用与栈帧开销消失（内联调用点 2）。', 74 );
    } else {
      items.push('内联关闭 · 保留真实调用');
      emit('inline', '内联开关关闭：两个调用点保持真实方法调用——每次调用都付出调用与栈帧开销（内联调用点 0）。', 74 );
    }
    if (escapeOn) {
      metrics.escapes = calls;
      items.push(`${calls} 个 Order 未逃逸 · 标量替换`);
      emit('alloc', `每次调用创建 Order：分析确认对象不逃逸出方法 → 字段直接拆分到寄存器或栈（标量替换 ${calls}）——零堆分配、零 GC 压力。`, 41 );
    } else {
      items.push(`${calls} 个 Order 逃逸 · 堆分配`);
      emit('alloc', `逃逸分析关闭：Order 走普通堆分配——${calls} 个对象进入堆，随后由 GC 回收（标量替换 0）。`, 41 );
    }
    emit('call', `运行结束：解释 0 · C1 0 · C2 ${metrics.c2} · 内联 ${metrics.inlined} · 标量替换 ${metrics.escapes}——内联消除调用开销、标量替换消除堆分配：两者都依赖 C2 画像成立，画像失效会触发去优化。`, 54 );
  } else {
    metrics.interpreted = calls;
    items.push(`Pass A · JVM 默认阈值 1000 · 解释 ${calls}`);
    emit('call', `Pass A：JVM 默认 -XX:CompileThreshold=1000——调用 ${calls} 次未达门槛，一次编译都不发生：全程解释执行（解释 ${calls}）。`, 65 );
    if (calls > threshold) {
      metrics.interpreted = calls + threshold;
      metrics.c1 = calls - threshold;
      items.push(`Pass B · -XX:CompileThreshold=${threshold} · C1 ${calls - threshold}`);
      emit('c1', `Pass B：调低到 -XX:CompileThreshold=${threshold}——前 ${threshold} 次仍解释，第 ${threshold + 1} 次起由 C1 编译代码执行（累计解释 ${calls + threshold} · C1 ${calls - threshold}）。`, 70 );
    } else {
      metrics.interpreted = calls + calls;
      items.push(`Pass B · -XX:CompileThreshold=${threshold} · 仍全部解释`);
      emit('interp', `Pass B：-XX:CompileThreshold=${threshold} 未低于调用次数 → 依旧全程解释（累计解释 ${calls + calls}）——门槛没有被越过，调参没有收益。`, 70 );
    }
    emit('call', `运行结束：解释 ${metrics.interpreted} · C1 ${metrics.c1} · C2 0——调低门槛让方法更早进入 C1：收益是提前切到编译代码，代价是 C1 编译本身占用启动期 CPU。`, 54 );
  }
  return frames;
}

export const jvmRunners = {
  'jvm-bytecode': bytecode,
  'jvm-classloading': classloading,
  'jvm-memory': memory,
  'jvm-allocation': allocation,
  'jvm-roots': roots,
  'jvm-collectors': collectors,
  'jvm-jit': jit,
};
