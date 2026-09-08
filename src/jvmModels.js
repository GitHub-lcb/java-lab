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
  emit('classfile', 'ready', '方法 add(int a, int b) 已进入解释执行。', 0);
  metrics.pc++; stack.push(locals[0]); emit('locals', 'iload_0', `iload_0：把局部变量 a=${p.left} 压入操作数栈。`, 1);
  metrics.pc++; stack.push(locals[1]); emit('locals', 'iload_1', `iload_1：把局部变量 b=${p.right} 压入操作数栈。`, 2);
  metrics.pc++; const right = stack.pop(); const left = stack.pop(); stack.push(left + right); emit('stack', 'iadd', `iadd：弹出 ${right} 与 ${left}，压入和 ${left + right}。`, 3);
  metrics.pc++; metrics.result = stack.pop(); emit('return', 'ireturn', `ireturn：返回栈顶整数 ${metrics.result}，当前栈帧结束。`, 4);
  return frames;
}

function classloading(p) {
  const frames = [];
  const metrics = { requested: 1, delegated: 0, defined: 0, verified: 0, initialized: 0, owner: '等待' };
  const visited = [];
  const emit = (active, message, code) => snapshot(frames, active, message, code, metrics, [...visited], 'DELEGATION TRACE');
  emit('request', `请求加载 ${p.target}。`, 0);
  const chain = p.custom ? ['Custom', 'Application', 'Extension', 'Bootstrap'] : ['Application', 'Extension', 'Bootstrap'];
  for (const loader of chain) {
    metrics.delegated++;
    visited.push(loader);
    emit(loader === 'Bootstrap' ? 'bootstrap' : loader === 'Application' ? 'application' : 'parent', `${loader} ClassLoader 接到委派请求。`, 1);
  }
  const jdkClass = p.target.startsWith('java.');
  metrics.owner = jdkClass ? 'Bootstrap' : 'Application';
  metrics.defined = 1;
  emit(jdkClass ? 'bootstrap' : 'application', `${metrics.owner} 找到并定义 ${p.target}。`, 2);
  metrics.verified = 1;
  emit('link', '验证字节码并为静态字段分配默认值。', 3);
  metrics.initialized = 1;
  emit('initialize', '首次主动使用触发 <clinit>，类初始化完成。', 4);
  return frames;
}

function memory(p) {
  const frames = [];
  const metrics = { threads: p.threads, stackFrames: 0, heapObjects: 0, loadedClasses: 0 };
  const items = [];
  const emit = (active, message, code) => snapshot(frames, active, message, code, metrics, items, 'RUNTIME AREAS');
  emit('threads', `创建 ${p.threads} 个 Java 线程。每个线程拥有独立 PC 与 Java 栈。`, 0);
  metrics.stackFrames = p.threads * p.depth;
  items.push(`${p.threads} stacks · ${metrics.stackFrames} frames`);
  emit('stacks', `每个线程递归深度 ${p.depth}，共形成 ${metrics.stackFrames} 个栈帧。`, 1);
  metrics.heapObjects = p.objects;
  items.push(`heap · ${p.objects} objects`);
  emit('heap', `${p.objects} 个对象分配在所有线程共享的堆中。`, 2);
  metrics.loadedClasses = p.classes;
  items.push(`class metadata · ${p.classes}`);
  emit('metadata', `加载 ${p.classes} 个类，其类元数据进入方法区的实现区域。`, 3);
  emit('result', '局部变量随栈帧退出；仍被引用的堆对象不会因此立即消失。', 4);
  return frames;
}

function allocation(p) {
  const frames = [];
  const metrics = { allocated: 0, bytes: 0, fastPath: 0, slowPath: 0, tlabRefills: 0 };
  const items = [];
  let remaining = 0;
  const emit = (active, message, code) => snapshot(frames, active, message, code, metrics, [...items], 'ALLOCATION TRACE');
  emit('new', `准备分配 ${p.objects} 个对象，每个教学单位大小 ${p.size}。`, 0);
  for (let i = 1; i <= p.objects; i++) {
    if (p.tlab) {
      if (remaining < p.size) {
        remaining = Math.max(p.tlabCapacity, p.size);
        metrics.slowPath++;
        metrics.tlabRefills++;
        emit('tlab', `对象 #${i} 前申请新的 TLAB，容量 ${remaining}。`, 1);
      }
      remaining -= p.size;
      metrics.fastPath++;
      emit('tlab', `对象 #${i} 通过线程本地 bump-the-pointer 快速分配。`, 2);
    } else {
      metrics.slowPath++;
      emit('heap', `对象 #${i} 进入共享分配路径，需要协调堆顶指针。`, 2);
    }
    metrics.allocated++;
    metrics.bytes += p.size;
    items.push(`obj-${i} · ${p.size}`);
  }
  emit('result', `完成 ${metrics.allocated} 次分配：快速路径 ${metrics.fastPath}，共享/补充路径 ${metrics.slowPath}。`, 3);
  return frames;
}

function roots(p) {
  const frames = [];
  const metrics = { objects: p.objects, roots: p.roots, reachable: 0, reclaimed: 0, cyclesReclaimed: 0 };
  const items = Array.from({ length: p.objects }, (_, index) => `obj-${index + 1} · 未标记`);
  const emit = (active, message, code) => snapshot(frames, active, message, code, metrics, [...items], 'MARK STATE');
  emit('objects', `堆中有 ${p.objects} 个对象，其中设置 ${p.roots} 个 GC Root 起点。`, 0);
  const reachable = Math.min(p.objects, p.roots * 2);
  for (let i = 0; i < reachable; i++) {
    metrics.reachable++;
    items[i] = `obj-${i + 1} · reachable`;
    emit(i < p.roots ? 'roots' : 'mark', `从 ${i < p.roots ? 'GC Root' : '已标记对象'} 到达 obj-${i + 1}。`, 1);
  }
  if (p.cycle && p.objects - reachable >= 2) {
    items[p.objects - 2] = `obj-${p.objects - 1} ↔ cycle`;
    items[p.objects - 1] = `obj-${p.objects} ↔ cycle`;
    emit('cycle', '两个不可达对象相互引用，但没有从 GC Roots 可达的路径。', 2);
    metrics.cyclesReclaimed = 1;
  }
  metrics.reclaimed = p.objects - metrics.reachable;
  for (let i = reachable; i < p.objects; i++) items[i] = `obj-${i + 1} · reclaimable`;
  emit('sweep', `标记结束：${metrics.reclaimed} 个不可达对象可回收，包括不可达循环。`, 3);
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
  emit('workload', `${names[p.collector]} 接到 ${p.youngMb} MB 年轻代负载，其中 ${liveMb} MB 对象存活。`, 0);
  if (p.collector === 'g1') {
    emit('mark', `G1 把 ${concurrentWork} 个工作单位放入并发标记阶段，应用可与部分 GC 工作重叠。`, 2);
  } else {
    emit('mark', `${names[p.collector]} 在本模型中把存活识别计入停顿工作，不设置并发工作量。`, 2);
  }
  emit('pause', `应用进入停顿阶段，需要承担 ${metrics.pauseWork} 个抽象工作单位。`, 3);
  emit('workers', `${workers} 个 GC 工作线程承担停顿内工作；配置值为 ${p.gcThreads}。`, 3);
  emit('reclaim', `本轮识别 ${liveMb} MB 存活对象，回收 ${metrics.reclaimedMb} MB。`, 4);
  return frames;
}

export const jvmRunners = {
  'jvm-bytecode': bytecode,
  'jvm-classloading': classloading,
  'jvm-memory': memory,
  'jvm-allocation': allocation,
  'jvm-roots': roots,
  'jvm-collectors': collectors,
};
