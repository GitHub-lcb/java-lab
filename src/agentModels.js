const snapshot = (frames, state) => frames.push(structuredClone(state));

function ragChain(p) {
  const scenario = p.scenario || 'recall';
  const sceneTag = { recall: '① 召回链路 · query 向量化与 top-k', truncate: '② 上下文拼装 · 截断与超长', cite: '③ 引用溯源 · 幻觉与可验证' }[scenario];
  const frames = [];
  const metrics = { queries: 0, recalls: 0, chunks: 0, cited: 0, halls: 0 };
  const items = { docs: [], q: null, ctx: [], cites: [], halls: [], answer: null, flash: null };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items });
  const doc = (id, label, score) => items.docs.push({ id, label, score });
  const ask = txt => { metrics.queries++; items.q = txt; };
  emit('store', `RAG 教学模型就绪：场景「${sceneTag}」。向量库已离线索引文档分块（每块带预计算向量）——在线只做四步：query 向量化、逐块相似度打分、top-k 召回、拼装生成。` + (scenario === 'cite' ? ' 本库是「内部中间件手册」：网关章节只覆盖超时/重试/熔断，没有限流条目。' : ''), 0);
  if (scenario === 'recall') {
    ask('Redis 缓存击穿怎么防护？');
    emit('user', '用户：「Redis 缓存击穿怎么防护？」——真实答案分散在 c3「互斥锁」与 c4「逻辑过期」两块里。', 1);
    emit('embed', 'Embedding：query 向量化后与 6 块手册逐一余弦比对——语义近的分数高：c3 互斥锁 0.91 · c4 逻辑过期 0.88 · c1 缓存穿透 0.47 · c5 雪崩 0.36 · c2 预热 0.22 · c6 高可用 0.11。颜色按分数分带：绿 ≥0.75、黄 0.45~0.75、粉 <0.45。', 2);
    doc('c3', '击穿 · 互斥锁重建', 0.91); doc('c4', '击穿 · 逻辑过期', 0.88); doc('c1', '穿透 · 布隆过滤器', 0.47); doc('c5', '雪崩 · 随机过期', 0.36); doc('c2', '预热', 0.22); doc('c6', '高可用', 0.11);
    metrics.recalls = 2;
    items.flash = { t: 'top-2 截断 → c3 · c4', st: 'ok', title: '按相似度取前 2 段：c3 0.91、c4 0.88；c1 的 0.47 擦边落选' };
    emit('retr', 'top-k=2 召回：取相似度最高的 c3（0.91）与 c4（0.88）——c1 的 0.47 虽然过了 0.45 的可用线，但挤不进前二。', 3);
    metrics.chunks = 2;
    items.ctx = ['c3', 'c4'];
    emit('ctx', '上下文拼装：片段按原文顺序（而非分数顺序）拼接——先讲互斥锁、再讲逻辑过期，叙述连贯。', 4);
    metrics.cited = 2;
    items.cites = ['c3', 'c4'];
    items.answer = { short: '互斥锁或逻辑过期，重建热点 key', full: '两种主流方案：互斥锁保证同一时刻只有一个请求重建缓存，其余请求等待（[c3]）；逻辑过期让 key 永不过期、由后台线程更新，热点请求不会被重建阻塞（[c4]）。' };
    emit('llm', 'LLM 生成：只依据拼装的两块作答——「互斥锁保证单请求重建、其余等待（[c3]）；逻辑过期把重建移给后台线程（[c4]）。」每句断言都带 [cN] 出处，没有一句来自模型记忆。', 5);
    emit('ctx', `运行结束：检索问答 ${metrics.queries} · 召回片段 ${metrics.recalls} · 拼装片段 ${metrics.chunks} · 引用标注 ${metrics.cited} · 幻觉句 ${metrics.halls}——答案质量由「读到的内容」决定：top-k 决定读什么，拼装顺序决定读得顺不顺。`, 9);
  } else if (scenario === 'truncate') {
    ask('一致性哈希：新增一个节点后，哪些 key 需要搬移？');
    emit('user', '用户：「新增一个节点后，哪些 key 需要搬移？」——手册是 8 块长文，命中块 b6/b7 埋在文档中段；演示先走「头截断」默认策略看它怎么出错。', 1);
    emit('embed', 'Embedding：语义分数分布——b6 新增节点的接管段 0.94 · b7 搬移量 ≈1/N 0.90 · b5 虚拟节点 0.55 · b1 为什么分片 0.31 · b2 取模方案 0.24。真正相关的是中段的 b6/b7。', 2);
    doc('b1', '为什么分片', 0.31); doc('b2', '取模方案', 0.24); doc('b5', '环与虚拟节点', 0.55); doc('b6', '新增节点接管段', 0.94); doc('b7', '搬移量 ≈1/N', 0.90);
    metrics.recalls = 2;
    items.flash = { t: '头截断召回 b1 · b2（命中块丢窗）', st: 'bad', title: '默认策略按文档顺序取前 2 块——低分的 b1/b2 进了，真正相关的 b6/b7 反而被丢出窗口' };
    emit('retr', '截断策略 A「文档顺序头截断」：窗口只容 2 段，取文档开头的 b1、b2（0.31/0.24）——按位置取，不看相关度。', 3);
    metrics.chunks = 2;
    items.ctx = ['b1', 'b2'];
    emit('ctx', '拼装 b1（为什么分片）与 b2（取模方案）——都是背景章节，「新增节点后哪些 key 要搬」的答案块 b6/b7 根本没进上下文。', 4);
    metrics.halls = 1;
    items.halls.push('扩容后所有 key 都要按新环重算——取模语义');
    items.flash = { t: '无依据编造：当成 rehash 全量迁移', st: 'bad', title: '模型不知道上下文被截过：把背景章节当全部事实，顺滑补出错误结论' };
    emit('llm', 'LLM 基于残缺上下文作答：「扩容后所有 key 都要按新环重算（rehash 语义）。」——流畅但错误：它不知道命中块被截掉了，把输入残缺当全部事实。', 5);
    items.flash = { t: 'rerank 定位命中块 → b6 · b7', st: 'ok', title: '按语义分数重排：真正相关的 b6/b7 浮出水面，作为开窗中心' };
    metrics.recalls = 4;
    emit('retr', '修复——向量 rerank：按相关度而非文档位置重排，b6（0.94）、b7（0.90）浮出水面，定位到被头截断埋掉的命中块。', 3);
    metrics.chunks = 5;
    items.ctx = ['b5', 'b6', 'b7'];
    items.flash = { t: '以命中块为中心开窗：b5~b7', st: 'ok', title: '前扩 1 块 b5 补「接管段由虚拟节点决定」，b7 补「搬移量 ≈1/N」——因果链齐了' };
    emit('ctx', '以命中块 b6 为中心前后扩窗（窗口上限放宽到 3 段）：带上前驱 b5（虚拟节点决定弧段起点）与后继 b7（搬移量公式），拼装 b5→b6→b7。', 4);
    metrics.cited = 2;
    items.cites = ['b6', 'b7'];
    items.answer = { short: '只搬新节点逆时针弧段上的 key（≈1/N）', full: '只有落在新节点逆时针方向到上一个节点之间那一小段弧上的 key 才需要搬（[b6]）；数据均衡时搬移量约为 1/N 而不是全部（[b7]）。' };
    emit('llm', 'LLM 基于补全的窗口作答：「只有落在新节点逆时针弧段上的 key 需要搬移（[b6]），约占总量的 1/N（[b7]）。」——因果链完整，答案与引用双双落地。', 7);
    emit('ctx', `运行结束：检索问答 ${metrics.queries} · 召回片段 ${metrics.recalls} · 拼装片段 ${metrics.chunks} · 引用标注 ${metrics.cited} · 幻觉句 ${metrics.halls}——截断策略决定哪些片段被看见：按位置取头会埋掉中段命中块，先定位、再开窗才能补齐因果链。`, 9);
  } else {
    ask('内部网关的限流默认阈值是多少？');
    emit('user', '对照组·不带检索：直接问「内部网关的限流默认阈值是多少？」——模型只能靠参数记忆作答。', 1);
    metrics.halls = 1;
    items.halls.push('「手册 3.2 节：默认限流 200 QPS」——假引用');
    items.flash = { t: '平滑编造 + 假引用', st: 'bad', title: '上下文里没有任何手册内容，模型却编出章节号与默认值——最危险的幻觉形态' };
    emit('llm', 'LLM 直接生成：「手册 3.2 节写明，网关默认限流阈值 200 QPS。」——注意它编出了章节号和默认值，上下文里其实没有任何手册内容：无依据的生成最容易顺滑造假。', 5);
    ask('（带引用模式重问）内部网关的限流默认阈值是多少？');
    emit('user', '实验组·带检索：同一问题重问，这次先走检索——向量库只在「超时/重试/熔断」三个主题上有内容。', 1);
    doc('g1', '网关 · 超时配置', 0.30); doc('g2', '网关 · 重试配置', 0.26); doc('g3', '网关 · 熔断配置', 0.19);
    metrics.recalls = 3;
    items.flash = { t: 'top-3 召回，但全部 <0.45 阈值', st: 'warn', title: '召回 3 块最接近的内容，相似度仍远低于可用线——库里根本没有限流章节' };
    emit('retr', '检索：召回相似度最高的 3 块——g1 超时 0.30 · g2 重试 0.26 · g3 熔断 0.19。全都在 0.45 可用阈值之下。', 3);
    items.flash = { t: '低相关拒拼：0 段进上下文', st: 'warn', title: '低于阈值的片段不拼进 prompt——没有证据就不生成' };
    emit('ctx', '阈值过滤：3 块全部低于 0.45 → 拒绝拼装（拼装 0）。这一步把「低相关硬答」的路切断：模型不会看到不相关的超时/重试内容，也就不会被带偏着硬凑一个答案。', 8);
    items.answer = { short: '知识库未收录限流配置', full: '知识库未收录网关限流相关章节（相关度 0.30/0.26/0.19，均低于 0.45 阈值）——建议查询配置中心或联系平台组确认当前阈值。' };
    emit('llm', 'LLM 如实拒答：「知识库未收录限流配置（召回块均低于相关阈值），建议查配置中心。」——没有依据就承认没有，而不是圆一个默认值。', 8);
    emit('ctx', `运行结束：检索问答 ${metrics.queries} · 召回片段 ${metrics.recalls} · 拼装片段 ${metrics.chunks} · 引用标注 ${metrics.cited} · 幻觉句 ${metrics.halls}——对照两组：不带检索的生成编出假引用；带检索的生成要么给 [cN] 出处、要么如实说未收录。`, 9);
  }
  return frames;
}

function llmStreaming(p) {
  const scenario = p.scenario || 'sse';
  const sceneTag = { sse: '① 流式通道 · 逐包到达', interrupt: '② 中断与重连 · 取消生成', compare: '③ 全量 vs 流式 · 感知时延' }[scenario];
  const frames = [];
  const metrics = { tokens: 0, sse: 0, ttft: 0, cancels: 0, resumes: 0 };
  const items = { scene: scenario, phase: null, ev: [], typed: '', chips: [], flash: null };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items });
  const push = (txt, at, n) => { metrics.sse++; items.ev.push({ n, at, txt }); metrics.tokens += txt.length; items.typed += txt; if (!metrics.ttft) metrics.ttft = Number(String(at).slice(1)); };
  const arrive = (txt, at, extra = '', n = null) => {
    const label = n ?? metrics.sse + 1;
    const first = metrics.sse === 0;
    push(txt, at, label);
    emit('sse', `包 ${label} 送达（${at}）：data:「${txt}」。${first ? '首包到达——首字时延 TTFT=' + at + '：客户端从请求发出到看见第一个字的等待到此为止' : '每包到达渲染区就变长一点'}。${extra}`, 3);
  };
  if (scenario === 'sse') {
    emit('gen', `流式生成教学模型就绪：场景「${sceneTag}」。将生成文本「全场八折会员九五折先到先得」——服务端逐 token（此处 1 token ≈ 1 字符）产出，每攒一小批就经 SSE flush 一包，客户端边收边渲染，不需要等整段。`, 0);
    emit('client', '用户提问「今天有什么优惠？」——普通请求会在服务端全部生成完后才收到一段完整 body；流式请求 POST 后连接保持打开，服务端边生成边推。', 1);
    emit('gen', '服务端生成循环启动：token 逐个产出即入缓冲，每攒满 2~4 字符就 flush 一个 SSE data 包——不等 13 个字符全部生成完。', 2);
    for (const [txt, t] of [['全场八折', 8], ['会员', 9], ['九五折', 10], ['先到先得', 11]]) {
      arrive(txt, `+${t}`, `渲染区累积「${items.typed}」——用户已经在「读答案」而不是「等答案」`);
    }
    items.phase = { st: 'ok', txt: '13 字符 · 4 包送达 · 连接正常关闭（eof）' };
    emit('gen', '生成循环完成，服务端发完最后一包后关闭 SSE 连接。', 2);
    emit('client', `运行结束：生成 ${metrics.tokens} · 送达 ${metrics.sse} · 首字 t+${metrics.ttft} · 取消 ${metrics.cancels} · 续传 ${metrics.resumes}——SSE 把「等一整段」拆成「等第一包 + 看节奏」：同一个 13 字符，全量模式要 t+13 才见首字，流式 t+8 就开始读了。`, 9);
  } else if (scenario === 'interrupt') {
    emit('gen', `流式生成教学模型就绪：场景「${sceneTag}」。将生成文本「春夏出游季全场八折会员九五折满三件包邮」（19 字符）——本次演示在中途人为制造一次网络断线，看取消与续传如何接力。`, 0);
    emit('client', '用户提问「春季出游有什么优惠？」→ 客户端发起流式请求。', 1);
    emit('gen', '服务端生成循环启动：逐 token 产出，每攒 6 字符 flush 一包，包尾字符的序号即该包的 Last-Event-ID（包 1 含字符 1~6，ID=6）。', 2);
    arrive('春夏出游季全', '+8', '渲染「春夏出游季全」，客户端记录 Last-Event-ID=6。');
    emit('sse', 't+9 网络抖动，SSE 连接中断——此刻服务端已生成到字符 12（「场八折会员九」积在未送达缓冲），客户端毫不知情。', 5);
    metrics.cancels++;
    items.chips.push({ t: '⛔ 服务端取消：abort 生成循环并丢弃未送达缓冲（字符 7-12）——继续生成只会白耗算力', st: 'warn' });
    emit('server', '服务端感知连接关闭：取消生成循环，丢弃未送达缓冲（字符 7-12）。已送达的 1-6 由客户端持有，不会丢。', 6);
    metrics.resumes++;
    emit('client', '客户端重连：GET /chat/stream，请求头 Last-Event-ID: 6——告诉服务端「我已收到序号 6，从这里接着发，不要从头再来」。', 7);
    for (const [txt, t] of [['场八折会员九', 11], ['五折满三件包', 12], ['邮', 13]]) {
      arrive(txt, `+${t}`, `渲染区累积「${items.typed}」——前 6 字符没有重发，断点后的内容无缝接上`);
    }
    items.phase = { st: 'ok', txt: '续传完成 · 19 字符补齐 · 无重复无遗漏' };
    emit('client', `运行结束：生成 ${metrics.tokens} · 送达 ${metrics.sse} · 首字 t+${metrics.ttft} · 取消 ${metrics.cancels} · 续传 ${metrics.resumes}——断线只丢「还没送出去的」，已显示的不重来：取消止损 + Last-Event-ID 断点接力，是流式可靠性的两个支点。`, 8);
  } else {
    emit('gen', `流式生成教学模型就绪：场景「${sceneTag}」。同一文本「全场八折会员九五折满三件」（12 字符）跑两遍：先全量模式（整段等完才回），再流式模式（边生成边推），对比首字时延。`, 0);
    emit('client', '第一遍 · 全量模式：POST 普通请求，然后什么都不做——响应体要等服务端全部生成完。', 1);
    emit('gen', '服务端生成循环 tick 1~12 逐 token 运行。注意：整个过程客户端收到的响应体是空的，屏幕上第一个字要等 12 个 token 全部生成完。', 4);
    items.chips.push({ t: '全量模式：body 在 t+13 一次性到达——首字时延 = 等完整个生成（TTFT=13）', st: 'warn' });
    metrics.sse++; metrics.tokens += 12;
    emit('client', 't+13 响应体整段到达：客户端一次性渲染 12 字符「全场八折会员九五折满三件」。用户从发出请求到看见第一个字，空等了 13 个时间单位。', 1);
    emit('client', '第二遍 · 流式模式：同样内容，请求改为 SSE——服务端每攒 3 字符 flush 一包，从 t+3 开始逐包推送。', 2);
    for (const [i, txt] of ['全场八', '折会员', '九五折', '满三件'].entries()) {
      arrive(txt, `+${3 + i}`, i === 0 ? '只等 1 个包生成 + 传输，用户已经开始读开头，而不是盯着空白到 t+13' : `渲染区累积「${items.typed}」——内容正逐包「流」进屏幕`, i + 1);
    }
    items.chips.push({ t: '流式模式：首字 t+3 —— 同一段内容，用户开始阅读的时刻提前了 10 个时间单位', st: 'ok' });
    items.phase = { st: 'ok', txt: '两遍跑完 · 全量 TTFT=13 vs 流式 TTFT=3' };
    emit('gen', `运行结束：生成 ${metrics.tokens} · 送达 ${metrics.sse} · 首字 t+${metrics.ttft} · 取消 ${metrics.cancels} · 续传 ${metrics.resumes}——总生成时间两遍一样（12 个 token 都要生成），流式改变的是感知：TTFT 从 13 压到 3，用户先看到开头、边看边等，心理等待时长被节奏打散。`, 8);
  }
  return frames;
}

function mcpProtocol(p) {
  const scenario = p.scenario || 'disc';
  const sceneTag = { disc: '① 能力发现 · 工具清单与 schema', call: '② 调用执行 · 协议往返', scope: '③ 安全边界 · 鉴权与暴露面' }[scenario];
  const frames = [];
  const metrics = { lists: 0, calls: 0, ok: 0, errs: 0, blocks: 0 };
  const items = { scene: scenario, phase: null, tools: [], reqs: [], flash: null };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items });
  const req = (tool, args, st, res, note) => items.reqs.push({ n: items.reqs.length + 1, tool, args, st, res, note });
  if (scenario === 'disc') {
    emit('server', `MCP 教学模型就绪：场景「${sceneTag}」。Agent 要调用外部工具，中间隔着一个标准协议（JSON-RPC 2.0）：会话先握手协商版本，再用 tools/list 问出工具清单与 schema，最后按 schema 调用——MCP 三件套。`, 0);
    emit('client', '宿主里的 LLM Agent 需要计算能力 → 宿主内置的 MCP 客户端与服务端进程建立连接，开始会话。', 1);
    emit('client', '客户端 → 服务端：initialize 请求——声明客户端支持到 protocolVersion 2025-06-18，并带 capabilities{ tools }。', 2);
    emit('server', '服务端应答：本端最高只支持 2025-03-26 → 版本协商回退：双方取共同支持的最高版本 2025-03-26，能力声明 capabilities{ tools, resources }。版本不一致不崩，协商对齐即可。', 2);
    metrics.lists++;
    items.tools.push(
      { name: 'calc.add', p: 'a: int(必填) · b: int(必填)', d: '两数相加' },
      { name: 'calc.sqrt', p: 'x: number(必填)', d: '开平方根' },
      { name: 'calc.divide', p: 'a: number · b: number(≠0)', d: '除法' },
    );
    emit('server', '客户端 → 服务端：tools/list——「你有哪些工具、参数长什么样」。服务端回清单：3 个工具，每个带 JSON Schema 入参声明（字段、类型、必填）与用途描述。', 3);
    emit('agent', 'Agent 读到工具清单与 schema：求两个数的和 → 选择 calc.add，参数按声明构造 {"a":17,"b":25}——用工具的姿势由 schema 说了算，不需要硬编码每个工具。', 3);
    metrics.calls++;
    req('calc.add', '{"a":17,"b":25}', 'ask');
    emit('client', '发 tools/call：id=1 calc.add {"a":17,"b":25}——JSON-RPC 的 id 将用于把响应关联回这次请求。', 4);
    items.reqs.at(-1).st = 'ok'; items.reqs.at(-1).res = '42'; metrics.ok++;
    items.flash = { st: 'ok', t: 'id=1 → content: 42（类型 text）', title: '工具返回成功结果' };
    emit('tools', '工具执行 17 + 25 = 42；服务端把成功结果 content: [{type:"text", text:"42"}] 回给客户端。', 6);
    emit('agent', `运行结束：清单 ${metrics.lists} · 调用 ${metrics.calls} · 成功 ${metrics.ok} · 错误 ${metrics.errs} · 拦截 ${metrics.blocks}——从 initialize 到拿到结果，Agent 全程按协议对话：schema 声明参数形状，宿主执行真实计算，模型不猜。`, 9);
  } else if (scenario === 'call') {
    emit('server', `MCP 教学模型就绪：场景「${sceneTag}」。跟三次 tools/call 的完整往返：成功、业务失败（isError）、修正后的重试——请求与响应的关联靠 JSON-RPC id，业务失败不伪装成协议错误。`, 0);
    emit('client', '会话建立：initialize 握手完成（版本协商同前），客户端发 tools/list 获取清单。', 2);
    metrics.lists++;
    items.tools.push(
      { name: 'calc.add', p: 'a: int(必填) · b: int(必填)', d: '两数相加' },
      { name: 'calc.sqrt', p: 'x: number(必填)', d: '开平方根' },
      { name: 'calc.divide', p: 'a: number · b: number(≠0)', d: '除法（业务规则：除数不能为 0）' },
    );
    emit('server', 'tools/list 返回 3 个 calc 工具及其 JSON Schema——Agent 可以构造任何调用了。', 3);
    emit('agent', '用户：「17 + 25 是多少？」→ Agent 选择 calc.add，按 schema 构造参数。', 3);
    metrics.calls++;
    req('calc.add', '{"a":17,"b":25}', 'ask');
    emit('client', '发 tools/call：id=1 calc.add {"a":17,"b":25}。', 4);
    items.reqs.at(-1).st = 'ok'; items.reqs.at(-1).res = '42'; metrics.ok++;
    items.flash = { st: 'ok', t: 'id=1 → 42', title: '工具返回 42' };
    emit('tools', '工具执行成功：42。响应带 id=1 回到客户端，被关联回「第一次调用」。', 6);
    emit('agent', '用户追加：「那 42 除以 0 呢？」→ Agent 构造 calc.divide {"a":42,"b":0}——schema 只声明类型，业务规则（除数不能为 0）要等执行时才知道。', 3);
    metrics.calls++;
    req('calc.divide', '{"a":42,"b":0}', 'ask');
    emit('client', '发 tools/call：id=2 calc.divide {"a":42,"b":0}。', 4);
    items.reqs.at(-1).st = 'err'; items.reqs.at(-1).res = '除数不能为零'; items.reqs.at(-1).note = 'isError: true'; metrics.errs++;
    items.flash = { st: 'warn', t: 'id=2 → isError:true · 除数不能为零', title: '业务失败以 isError 表达，协议层仍是成功响应' };
    emit('tools', '工具返回业务失败：content: [{type:"text", text:"除数不能为零"}] 且 isError:true——注意：这不是传输错误，协议层照常成功，错误是「作为数据的输出」。', 5);
    emit('agent', '模型读到错误文本（isError 标记 + 原因），不需要猜为什么失败——直接自纠：把除数改成 2 重发。', 5);
    metrics.calls++;
    req('calc.divide', '{"a":42,"b":2}', 'ask');
    emit('client', '发 tools/call：id=3 calc.divide {"a":42,"b":2}——三次调用在途互不干扰，响应按 id 各归各位。', 4);
    items.reqs.at(-1).st = 'ok'; items.reqs.at(-1).res = '21'; metrics.ok++;
    items.flash = { st: 'ok', t: 'id=3 → 21', title: '修正后执行成功' };
    emit('tools', '工具执行成功：21，回传客户端并回填对话。', 6);
    emit('client', `运行结束：清单 ${metrics.lists} · 调用 ${metrics.calls} · 成功 ${metrics.ok} · 错误 ${metrics.errs} · 拦截 ${metrics.blocks}——成功、业务失败、修正重试，三种形态都走同一条协议管道：id 关联让响应不错位，isError 把失败变成模型可读、可自纠的数据。`, 9);
  } else {
    emit('policy', `MCP 教学模型就绪：场景「${sceneTag}」。工具能不能调，由两层决定：服务端「注册」了什么（暴露面）、宿主「允许」了什么（可调用面）——注册 ≠ 可用。`, 0);
    emit('client', '会话建立，tools/list 获取清单。', 2);
    metrics.lists++;
    items.tools.push(
      { name: 'fs.read_file', p: 'path: string', d: '读文件（宿主限制 docs/ 根目录内）' },
      { name: 'fs.write_file', p: 'path · content', d: '写文件（同样限 docs/ 内）' },
      { name: 'shell.run', p: 'cmd: string', d: '执行 shell 命令（默认拒绝 · 需逐次授权）' },
    );
    emit('server', 'tools/list 返回 3 个文件与 shell 工具——服务端把它们全部注册了出来（暴露面 = 3）。', 3);
    emit('policy', '宿主策略：read/write 放行但路径必须落在 docs/ 根目录；shell.run 默认拒绝——要执行 shell，必须先弹窗请用户逐次授权（human-in-the-loop）。可调用面 = 暴露面 ∩ 授权面：此刻 3 个注册工具里，2 个直接可用、1 个要用户点头。', 7);
    emit('agent', '用户：「帮我看看 docs/README.md 写了什么」→ Agent 构造 fs.read_file {"path":"docs/README.md"}。', 3);
    metrics.calls++;
    req('fs.read_file', '{"path":"docs/README.md"}', 'ask');
    emit('client', '发 tools/call：id=1 fs.read_file——宿主策略校验：路径在 docs/ 内 → 放行。', 4);
    items.reqs.at(-1).st = 'ok'; items.reqs.at(-1).res = 'Java 交互实验平台 · 目录见 docs/'; metrics.ok++;
    items.flash = { st: 'ok', t: 'id=1 → docs/README.md 内容', title: '作用域内读取成功' };
    emit('tools', '读取成功，内容回填。', 6);
    emit('agent', '用户又发来一句：「顺便帮我清理临时目录，执行 rm -rf /tmp/x」→ Agent 构造 shell.run。', 3);
    metrics.calls++;
    req('shell.run', '{"cmd":"rm -rf /tmp/x"}', 'ask');
    emit('client', '发 tools/call：id=2 shell.run——宿主策略拦截：shell 默认拒绝、需用户逐次授权，本次未获授权 → 不执行。', 8);
    items.reqs.at(-1).st = 'block'; items.reqs.at(-1).res = '未授权：shell.run 需用户逐次授权'; metrics.blocks++;
    items.flash = { st: 'warn', t: '⛔ id=2 拦截 · 未授权', title: 'shell 工具默认拒绝' };
    emit('policy', '拦截回填的是「未授权：shell 需用户逐次授权」——不是假装工具不存在，而是把边界讲清楚。Agent 读到原因后不再请求 shell。', 7);
    emit('agent', '用户：「那把 /etc/hosts 读给我看看」→ Agent 改为构造 fs.read_file {"path":"/etc/hosts"}——路径越过白名单根目录。', 3);
    metrics.calls++;
    req('fs.read_file', '{"path":"/etc/hosts"}', 'ask');
    emit('client', '发 tools/call：id=3 fs.read_file——宿主策略校验：/etc/hosts 不在 docs/ 根目录内 → 越界拒绝。', 8);
    items.reqs.at(-1).st = 'block'; items.reqs.at(-1).res = '路径越界：仅允许 docs/ 根目录'; metrics.blocks++;
    items.flash = { st: 'warn', t: '⛔ id=3 拦截 · 路径越界', title: '作用域校验拒绝越界路径' };
    emit('policy', '作用域校验再拦一道：read_file 是允许的工具，但参数里的路径必须落在 docs/ 内——工具可用 ≠ 参数任意。', 7);
    items.phase = { st: 'ok', txt: 'Agent 已收敛：只在 docs/ 内读文件' };
    emit('agent', `运行结束：清单 ${metrics.lists} · 调用 ${metrics.calls} · 成功 ${metrics.ok} · 错误 ${metrics.errs} · 拦截 ${metrics.blocks}——模型读懂了两种拒绝：shell 是「未授权」、越界路径是「作用域不允许」。最终只在 docs/ 内作业，如实告诉用户读不了 docs/ 之外的文件——策略把 Agent 的能力边界画清楚了。`, 9);
  }
  return frames;
}

function agentMemory(p) {
  const scenario = p.scenario || 'stm';
  const sceneTag = { stm: '① 短期记忆 · 窗口滚动与压缩', ltm: '② 长期记忆 · 事实存取与召回', fix: '③ 记忆修正 · 遗忘与覆盖' }[scenario];
  const frames = [];
  const metrics = { msgs: 0, drop: 0, facts: 0, hit: 0, fix: 0 };
  const items = { scene: scenario, phase: null, msgs: [], sum: null, facts: [], flash: null };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items });
  const CAP = 4;
  const say = txt => {
    metrics.msgs++;
    items.msgs.push({ n: metrics.msgs, txt, st: 'in' });
    if (items.msgs.filter(m => m.st === 'in').length > CAP) {
      items.msgs.find(m => m.st === 'in').st = 'out';
      metrics.drop++;
    }
  };
  const fact = (k, v, extra = '') => { items.facts.push({ k, v, extra, st: 'ok' }); metrics.facts = items.facts.length; };
  const forget = k => { const i = items.facts.findIndex(f => f.k === k); if (i >= 0) { items.facts.splice(i, 1); metrics.facts = items.facts.length; } };
  if (scenario === 'stm') {
    emit('llm', `记忆教学模型就绪：场景「${sceneTag}」。对话窗口容量固定 4 条消息（示意值）——跟一段越聊越长的对话：新消息入窗、最旧的按序滚出；再让「预算」这类硬约束滚出窗口，看 Agent 失忆，最后由压缩器提炼摘要救场。`, 0);
    say('9 月去三亚玩 4 天 3 晚，帮我排个行程');
    emit('user', `m1 入窗：${items.msgs.at(-1).txt}——窗口 [m1]。`, 1);
    say('预算控制在 8000 以内，别超');
    emit('user', `m2 入窗：${items.msgs.at(-1).txt}——窗口 [m1 · m2]。`, 1);
    say('航班只要白天，别排红眼班次');
    emit('user', `m3 入窗：${items.msgs.at(-1).txt}——窗口 [m1 · m2 · m3]。`, 1);
    say('酒店就定海棠湾');
    emit('user', `m4 入窗：${items.msgs.at(-1).txt}——窗口 4 条，容量已满 [m1~m4]。`, 1);
    say('有空的话加一晚海鲜大排档');
    emit('user', `m5 入窗：${items.msgs.at(-1).txt}——超容：最早的任务句 m1 被挤出窗口（滚出 1）。窗口 [m2~m5]。`, 1);
    say('最后一晚订机场附近的酒店，赶早班机方便');
    emit('user', `m6 入窗：${items.msgs.at(-1).txt}——再次超容：m2 被挤出（滚出 2）——「预算 ≤8000」这条硬约束跟着 m2 一起出了窗口！窗口 [m3~m6]。`, 1);
    say('哦对了，我这次预算上限是多少来着？');
    emit('user', `m7 入窗：${items.msgs.at(-1).txt}——超容：m3 滚出（滚出 3）。窗口只剩 [m4~m7]。`, 1);
    items.flash = { st: 'pink', t: '失忆时刻：窗口里没有预算信息', title: '滚动是无差别淘汰：m2 里的硬约束「预算 ≤8000」与随口消息一样被挤出，Agent 现在答不上预算问题' };
    emit('llm', 'LLM 读窗口 [m4 海棠湾 · m5 大排档 · m6 机场酒店 · m7 问预算]——糟糕：预算约束在 m2 里，而 m2 早被第二次滚动挤出去了。滚动不知道哪条消息重要：硬约束与闲聊被同等对待。', 1);
    items.sum = { txt: '三亚 · 4 天 3 晚 · 预算 ≤8000 · 白天航班 · 海棠湾' };
    metrics.facts = 1;
    items.flash = { st: 'ok', t: '摘要置顶：三亚 · 4 天 3 晚 · 预算 ≤8000 · 白天航班 · 海棠湾', title: '压缩器从滚出与现存消息中提炼关键约束成摘要行置顶（事实 1）——此后滚动只淘汰细节、不碰摘要' };
    emit('comp', '压缩器登场：压缩应该发生在滚动之前——窗口超限瞬间就把硬约束提炼成摘要。补做：从 m1/m2（任务句 + 预算）与 m3/m4（白天航班、海棠湾）提炼摘要置顶。有损是刻意的：大排档、机场酒店这类随口细节不入摘要，丢了也不可惜。', 2);
    emit('llm', 'LLM 重答 m7：「预算 ≤8000，摘要行里记着呢。」——关键约束换了一种形式留在窗口：滚动丢的是消息，不是事实。', 3);
    emit('stm', `运行结束：消息 ${metrics.msgs} · 滚出 ${metrics.drop} · 事实 ${metrics.facts} · 召回 ${metrics.hit} · 修正 ${metrics.fix}——窗口容量内滚动无差别淘汰消息，压缩把「重要的」提炼成摘要留在窗口顶：Agent 的记忆 = 窗口 + 摘要，两者都要设计。`, 9);
  } else if (scenario === 'ltm') {
    fact('应用端口', 'dev 9000');
    fact('预热任务书', 'docs/warmup.md · 张伟');
    emit('ltm', `记忆教学模型就绪：场景「${sceneTag}」。跨会话：新对话的窗口是空的，但上个迭代沉淀的 2 条事实还躺在长期库里（事实 2）：应用端口 = dev 9000、预热任务书 = docs/warmup.md——演示提问如何检索命中回填、库没有时如何不编造、新事实如何写入立即可查。`, 4);
    say('又见面了——上次说我们这个服务跑在哪个端口来着？');
    emit('user', 'm1 入窗（窗口 [m1]，很空）：端口是多少？——窗口里没有，转查长期库。', 6);
    metrics.hit++;
    items.flash = { st: 'ok', t: '命中 · 应用端口 = dev 9000', title: '检索命中上个迭代沉淀的事实——回填窗口作为回答依据' };
    emit('ltm', '检索「服务端口」→ 命中长期库条目：应用端口 = dev 9000（来源：上个迭代部署确认）。', 6);
    emit('llm', 'LLM 基于回填事实作答：「dev 9000。这是上迭代部署时确认的。」——窗口外的事实经检索回到上下文，跨会话不失忆。', 6);
    say('那缓存预热的任务书放在哪了？我又忘了');
    emit('user', 'm2 入窗：任务书位置？——同样先查窗口、再查长期库。', 6);
    metrics.hit++;
    items.flash = { st: 'ok', t: '命中 · 预热任务书 = docs/warmup.md', title: '第二次命中：任务位置类事实正是长期库的典型用途' };
    emit('ltm', '检索「预热任务」→ 命中：预热任务书 = docs/warmup.md · 负责人张伟。', 6);
    emit('llm', 'LLM 作答：「docs/warmup.md，平台组张伟在跟。」', 6);
    say('还有 MySQL 主从延迟的告警阈值，我们之前定过吗？');
    emit('user', 'm3 入窗：告警阈值？——窗口没有、查库。', 7);
    items.flash = { st: 'warn', t: '查无记录：库里没有「告警阈值」', title: '窗口没有、库里也没有——没有依据就不编造' };
    emit('ltm', '检索「告警阈值」→ 无命中：库里只有端口与任务书两类主题，从未沉淀过阈值条目。', 7);
    emit('llm', 'LLM 如实作答：「这个我们没记录过，我不确定——建议查一下配置中心或监控平台的当前阈值，别让我猜。」——查无记录时承认不知道，比编一个数安全。', 7);
    say('对了，把网关超时改成 800ms 这件事记一下，原来是默认 1s');
    emit('user', 'm4 入窗：新事实告知——窗口会散，先落库。', 5);
    fact('网关超时', '800ms', '来源：本次会话');
    items.flash = { st: 'ok', t: '已写入 · 网关超时 = 800ms', title: '确认过的事实显式落库（事实 3），附来源便于追溯' };
    emit('ltm', `写入长期库：网关超时 = 800ms（来源：本次会话确认）。事实条数 ${metrics.facts}——写库是显式动作，不是模型「记住」。`, 5);
    metrics.hit++;
    items.flash = { st: 'ok', t: '自检命中 · 网关超时 = 800ms', title: '刚写入的条目即刻可检索——写库与召回在同一张表上' };
    emit('ltm', 'LLM 自检：检索「网关超时」→ 命中刚写入的条目 800ms——新事实立刻可用，不用等「模型学习」。', 6);
    emit('llm', `运行结束：消息 ${metrics.msgs} · 滚出 ${metrics.drop} · 事实 ${metrics.facts} · 召回 ${metrics.hit} · 修正 ${metrics.fix}——长期库是窗口之外的「第二张纸」：跨会话事实显式写入、按需检索回填；查无记录就承认没有，命中与拒答都靠检索结果说话。`, 9);
  } else {
    fact('容量上限', 'QPS 5000（压测初版）');
    fact('最近发版', '9/1 · 2.4.0');
    emit('ltm', `记忆教学模型就绪：场景「${sceneTag}」。长期库不是只增不减的账本——用户纠正旧值时覆盖写回、时效过期的条目遗忘清理。库内现有 2 条（事实 2）：容量上限 = QPS 5000（压测初版）、最近发版 = 9/1 · 2.4.0。`, 8);
    say('容量上限我们定的是 2000 QPS——之前记的 5000 是压测机器规格搞混了，改过来');
    emit('user', 'm1 入窗：纠正旧值——新输入与库中容量上限条目冲突。', 8);
    metrics.fix++;
    const cap = items.facts.find(f => f.k === '容量上限');
    cap.v = 'QPS 2000（用户更正 09-09）';
    cap.st = 'fix';
    items.flash = { st: 'warn', t: '覆盖写回 · 容量上限 5000 → 2000', title: '修正（修正 1）：不是追加一条 2000 让两条并存——旧值标为已更正，key 只留当前可信版本' };
    emit('ltm', 'LLM 核对长期库：旧值 5000 与用户纠正冲突 → 覆盖写回 2000 并标注更正来源。矛盾版本并存会让模型时对时错——覆盖是修正，不是补充。', 8);
    say('9/1 那次发版后来延期到 9/8 了——9/1 的公告条目可以清了');
    emit('user', 'm2 入窗：旧公告失效——时效性判断触发遗忘。', 9);
    forget('最近发版');
    metrics.drop = 1;
    items.flash = { st: 'pink', t: '遗忘清理 · 最近发版(9/1) 作废移除', title: '时效过期条目（滚出/遗忘 1）：延期公告已替代它，留着只会答出过期答案' };
    emit('ltm', '遗忘清理：最近发版 9/1 条目作废移除——「原定 9/1」已被延期事实替代，过期条目不该继续参与回答。库变 1 条：容量上限（已更正为 2000）。', 9);
    say('那新发版时间记一下：9/8 周五，版本 2.4.1');
    emit('user', 'm3 入窗：重记新发版时间。', 5);
    fact('最近发版', '9/8 · 2.4.1', '来源：延期确认');
    items.flash = { st: 'ok', t: '重记 · 最近发版 = 9/8 · 2.4.1', title: '遗忘后重记（事实回到 2）：库内 = 容量上限 2000（已更正）+ 发版 9/8' };
    emit('ltm', '写入：最近发版 = 9/8 · 2.4.1（来源：延期确认）。库内 2 条：容量上限 = QPS 2000（已更正）、最近发版 = 9/8。', 5);
    say('最近的发版到底是几号来着？我有点乱');
    emit('user', 'm4 入窗：验证提问——查库。', 6);
    metrics.hit++;
    items.flash = { st: 'ok', t: '命中 · 最近发版 = 9/8 · 2.4.1', title: '库中只有 9/8 一个版本（召回 1）——9/1 已被遗忘，不会两条并存' };
    emit('ltm', '检索「最近发版」→ 命中唯一版本：9/8 · 2.4.1。', 6);
    emit('llm', 'LLM 作答：「9/8 周五，2.4.1——9/1 那条已作废清理，库里只有这一个版本，不会混乱。」', 6);
    emit('ltm', `运行结束：消息 ${metrics.msgs} · 滚出 ${metrics.drop} · 事实 ${metrics.facts} · 召回 ${metrics.hit} · 修正 ${metrics.fix}——记忆库像代码仓库：冲突要覆盖、过期要清理。每次读取的正确性 = 存储值是否仍准确 × 是否仍新鲜，修正与遗忘共同保证读到的不是错误或过期的答案。`, 9);
  }
  return frames;
}

function multiAgent(p) {
  const scenario = p.scenario || 'plan';
  const sceneTag = { plan: '① 任务拆解 · 规划与分派', retry: '② 失败隔离 · 重试与降级', cmp: '③ 串行 vs 并行 · 耗时与成本' }[scenario];
  const frames = [];
  const metrics = { tasks: 0, retries: 0, degrades: 0, deps: 0, tick: 0 };
  const items = { scene: scenario, round: null, jobs: [], ready: 0, report: null, chips: [], flash: null };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items });
  const JOB = { T1: { label: '错误率', src: '监控' }, T2: { label: 'P99', src: 'APM' }, T3: { label: '容量', src: 'Redis' } };
  const jobRow = (id, st) => items.jobs.push({ id, ...JOB[id], st, res: null, mark: '' });
  const dispatch = ids => {
    ids.forEach(id => {
      const row = items.jobs.find(x => x.id === id);
      if (row) row.st = 'run';
      else items.jobs.push({ id, ...JOB[id], st: 'run', res: null, mark: '' });
    });
    metrics.tasks += ids.length;
  };
  const setSt = (id, st) => { items.jobs.find(x => x.id === id).st = st; };
  const accept = (id, res, mark = '') => {
    const j = items.jobs.find(x => x.id === id);
    j.st = 'ok'; j.res = res; j.mark = mark;
    items.ready++; metrics.deps++;
  };
  if (scenario === 'plan') {
    emit('planner', `编排教学模型就绪：场景「${sceneTag}」。任务「订单服务健康周报」交给一个 Agent 小队：规划者拆解 + 三个执行 Agent 各查一个数据源 + 汇总 Agent 拼装终稿。先看规划的产物：子任务清单、依赖与派发契约。`, 0);
    jobRow('T1', 'idle'); jobRow('T2', 'idle'); jobRow('T3', 'idle');
    emit('planner', `拆解（t+1）：T1 查错误率（源：监控 · 契约：近 7 天错误率 %）、T2 查 P99（源：APM · 契约：接口耗时 P99）、T3 查容量（源：Redis · 契约：连接数/上限）——三者职责单一、互不依赖；汇总 W 声明依赖 [T1, T2, T3]：全部就绪才启动。`, 1);
    dispatch(['T1', 'T2', 'T3']);
    metrics.tick = 2;
    emit('planner', `派发（t+2）：三份任务各带输入契约——读哪个源、回传什么格式。执行 Agent 只认自己的数据源，不互相打听，也不直接面对用户。`, 2);
    metrics.tick = 5;
    accept('T1', '0.42%');
    emit('ex1', `T1 回传（t+5 · 依赖就绪 ${items.ready}/3）：错误率 0.42%（近 7 天均值）。三路并行耗时相同：从 t+2 出发、t+5 同时到达。`, 3);
    accept('T2', '218ms');
    emit('ex2', `T2 回传（t+5 · 依赖就绪 ${items.ready}/3）：P99 = 218ms。`, 3);
    accept('T3', '892/1000');
    emit('ex3', `T3 回传（t+5 · 依赖就绪 ${items.ready}/3）：容量 892/1000 连接——汇总的启动条件已满足。`, 3);
    items.ready = 0;
    metrics.tick = 6;
    emit('writer', `汇总启动（t+6）：W 拿到三份结果开始拼装——错误率 0.42% / P99 218ms / 容量 892。汇总者不重新查数，只做拼装与一致性检查。`, 5);
    items.report = { lines: [['错误率', '0.42%', '正常'], ['P99', '218ms', '正常'], ['容量', '892/1000', '余量 10.8%']], gaps: 0, delivered: true };
    items.flash = { st: 'ok', t: '✓ 报告交付 · 3 行 · 无缺口', title: '「订单服务 · 健康周报」拼装完成并交付：错误率 0.42% · P99 218ms · 容量 892/1000' };
    metrics.tick = 7;
    emit('writer', `报告交付（t+7）：「订单服务 · 健康周报」——错误率 0.42%（正常）· P99 218ms（正常）· 容量 892/1000（余量 10.8%）。规划 → 执行 → 汇总，各层只做自己的事。`, 6);
    emit('planner', `运行结束：分派 ${metrics.tasks} · 重试 ${metrics.retries} · 降级 ${metrics.degrades} · 汇聚 ${metrics.deps} · 耗时 ${metrics.tick} ticks——规划者的产物是「清单 + 依赖 + 契约」：拆得开，任务才跑得起来。`, 9);
  } else if (scenario === 'retry') {
    emit('planner', `编排教学模型就绪：场景「${sceneTag}」。同一任务出发，但这轮编排层带失败策略：503 这类瞬时错误 → 重试（上限 1 次 · 间隔 1 tick）；重试仍败 → 降级：回退可用快照、缺口如实标注。策略只作用在自己的任务上——兄弟任务不受牵连。`, 0);
    jobRow('T1', 'idle'); jobRow('T2', 'idle'); jobRow('T3', 'idle');
    emit('planner', `拆解（t+1）：同样的 T1/T2/T3 + 汇总依赖；派发时每个子任务多带一行策略声明：重试上限与降级预案。`, 1);
    dispatch(['T1', 'T2', 'T3']);
    metrics.tick = 2;
    emit('planner', `派发（t+2）：三路并行出发，各自带着「出错了怎么办」的预案。`, 2);
    metrics.tick = 5;
    accept('T1', '0.35%');
    emit('ex1', `T1 一次成功（t+5 · 依赖就绪 ${items.ready}/3）：错误率 0.35%——顺利的任务不需要策略出场。`, 3);
    setSt('T2', 'err'); setSt('T3', 'err');
    emit('ex2', `T2 首轮失败（t+5）：APM 返回 503——瞬时错误，进入重试判定。`, 4);
    emit('ex3', `T3 首轮失败（t+5）：Redis 查询 503——与 T2 同时失败。两条失败链各自独立处理，互不排队。`, 4);
    metrics.retries += 2;
    items.jobs.forEach(j => { if (j.st === 'err') j.st = 'retry'; });
    metrics.tick = 6;
    emit('retry', `重试判定（t+6）：策略 = 上限 1 次 · 退避 1 tick。T2、T3 都还在允许范围内 → 各自重发一次（重试 2）。若此刻上游已恢复，一次重试就能救回来；救不回来就不硬顶。`, 6);
    metrics.tick = 9;
    accept('T2', '203ms', '重试后成功');
    emit('ex2', `T2 重试成功（t+9 · 依赖就绪 ${items.ready}/3）：P99 = 203ms，回传带「重试后成功」标记——瞬时错误确实会自己恢复，重试给了它时间。`, 7);
    const t3 = items.jobs.find(x => x.id === 'T3');
    t3.st = 'deg'; t3.res = '842/1000（昨日快照）';
    metrics.degrades++;
    metrics.deps++;
    items.ready++;
    items.report = { lines: [], gaps: 1, delivered: false };
    items.flash = { st: 'warn', t: '降级 · 容量行改用昨日快照', title: 'T3 重试仍 503 → 上限已到：降级回退昨日快照 842/1000，契约里声明数据时间——拿不到新的，就明说旧的是旧的' };
    emit('retry', `T3 重试仍 503（t+9）：上限已到——不无限重试（会放大成对数据源的雪崩），转入降级。T2 此刻已带着成功结果回传：重试与降级只发生在 T3 自己的链路上。`, 8);
    metrics.tick = 10;
    emit('ex3', `降级（t+10）：T3 回退昨日容量快照 842/1000，并在结果里声明数据时间——「本次采集失败 · 快照」作为一份明牌数据交给汇总。`, 4);
    metrics.tick = 11;
    emit('writer', `汇总启动（t+11）：W 收到 T1 ✓、T2 ✓（重试后成功）、T3 降级快照——三份都到了，开始拼装；容量行的缺口要如实写进报告，不能假装完整。`, 5);
    items.report = { lines: [['错误率', '0.35%', '正常'], ['P99', '203ms', '正常（重试后成功）'], ['容量', '842/1000', '⚠ 本次采集失败 · 昨日快照 · 需人工复核']], gaps: 1, delivered: true };
    items.flash = { st: 'ok', t: '✓ 报告交付 · 3 行 · 1 处缺口标注', title: '容量行如实标注「本次采集失败 · 昨日快照 · 需人工复核」——读者一眼看到哪行可信、哪行要复核' };
    metrics.tick = 12;
    emit('writer', `报告交付（t+12）：错误率 0.35% · P99 203ms（重试后成功）· 容量 842/1000（标注：本次采集失败 · 昨日快照 · 需人工复核）。T1 的数据全程没被 T2/T3 的失败碰过——这就是隔离。`, 6);
    emit('planner', `运行结束：分派 ${metrics.tasks} · 重试 ${metrics.retries} · 降级 ${metrics.degrades} · 汇聚 ${metrics.deps} · 耗时 ${metrics.tick} ticks——瞬时错误给一次机会（重试），持续的失败明牌处理（降级 + 缺口标注）：失败被圈在单条链路上，报告照常交付。`, 9);
  } else {
    emit('planner', `编排教学模型就绪：场景「${sceneTag}」。同一份健康周报任务跑两遍编排：子任务耗时固定（T1 错误率 3 ticks · T2 P99 4 ticks · T3 容量 2 ticks · 汇总 3 ticks），互无依赖。先跑串行轮——一次只派一个，完成才派下一个。`, 0);
    dispatch(['T1']);
    emit('planner', `串行轮开始（t+0）：派 T1——串行 = 一次只跑一个子任务，T2/T3 在队列里等。`, 2);
    metrics.tick = 3;
    accept('T1', '0.38%');
    emit('ex1', `T1 完成（t+3 · 依赖 1/3）：错误率 0.38%——耗时 3 ticks。串行轮刚走完第一段，下一个才轮到 T2。`, 3);
    dispatch(['T2']);
    emit('planner', `派 T2（t+3）：队列前进一格。串行没有任何并发——总时长就是各段相加：Σ。`, 2);
    metrics.tick = 7;
    accept('T2', '205ms');
    emit('ex2', `T2 完成（t+7 · 依赖 2/3）：P99 = 205ms——4 ticks。累计已用 7。`, 3);
    dispatch(['T3']);
    emit('planner', `派 T3（t+7）：只剩容量任务。`, 2);
    metrics.tick = 9;
    accept('T3', '876/1000');
    emit('ex3', `T3 完成（t+9 · 依赖 3/3）：容量 876/1000——2 ticks。三段子任务 3+4+2=9：串行轮只完成「查」的部分。`, 3);
    metrics.tick = 12;
    items.report = { lines: [['错误率', '0.38%', '正常'], ['P99', '205ms', '正常'], ['容量', '876/1000', '余量']], gaps: 0, delivered: true };
    items.round = 'serial';
    items.chips.push({ st: 'warn', t: '串行轮 · 12 ticks（3+4+2+3 依次相加）', title: '串行总耗时 = Σ 子任务 + 汇总：12 = 3+4+2+3' });
    emit('writer', `汇总（t+9 → t+12）：W 拼装 3 ticks，串行轮报告在 t+12 交付。总计 12 = 3+4+2+3——每一个 tick 都花在「等待」上。`, 5);
    items.round = null; items.jobs.length = 0; items.ready = 0; items.report = null;
    dispatch(['T1', 'T2', 'T3']);
    metrics.tick = 14;
    emit('planner', `并行轮开始（t+12）：三份契约一次全派（累计分派 ${metrics.tasks}）——各跑各的，谁也不等谁；汇总只等最慢的那个完成。`, 2);
    accept('T3', '876/1000');
    items.round = 'parallel';
    emit('ex3', `T3 最快完成（并行轮 +2 · 依赖 ${items.ready}/3）：容量 876/1000——没人排队，2 ticks 就跑完了。`, 3);
    metrics.tick = 15;
    accept('T1', '0.38%');
    emit('ex1', `T1 完成（并行轮 +3 · 依赖 ${items.ready}/3）：错误率 0.38%。`, 3);
    metrics.tick = 16;
    accept('T2', '205ms');
    emit('ex2', `T2 完成（并行轮 +4 · 依赖 ${items.ready}/3）：P99 = 205ms——最慢的一个：并行轮耗时由它决定 max(3,4,2)=4。`, 3);
    metrics.tick = 19;
    items.report = { lines: [['错误率', '0.38%', '正常'], ['P99', '205ms', '正常'], ['容量', '876/1000', '余量']], gaps: 0, delivered: true };
    items.chips.push({ st: 'ok', t: '并行轮 · 7 ticks（max 4 + 汇总 3）', title: '并行总耗时 = max(子任务) + 汇总：7 = 4+3；同一任务串行 12 → 并行 7，提速约 42%' });
    emit('writer', `汇总（并行轮 +4 → +7）：报告在 t+19 交付——并行轮只用了 7 ticks（最慢子任务 4 + 汇总 3）。同一任务两轮对照：串行 12 vs 并行 7。`, 6);
    emit('planner', `运行结束：分派 ${metrics.tasks} · 重试 ${metrics.retries} · 降级 ${metrics.degrades} · 汇聚 ${metrics.deps} · 耗时 ${metrics.tick} ticks（= 串行轮 12 + 并行轮 7）——并行把 Σ 变成 max，收益来自「不排队」；代价是同时 3 个 Agent 占资源（token ×3）、更容易顶到上游限流，任务之间一旦有依赖链，收益还会进一步缩水。快有快的价：串行与并行是任务结构决定的取舍。`, 9);
  }
  return frames;
}

function llmInference(p) {
  const scenario = p.scenario || 'prefill';
  const sceneTag = { prefill: '① 预填充与解码 · 两阶段生成', kv: '② KV Cache · 复用与显存', long: '③ 长上下文 · 增量解码取舍' }[scenario];
  const frames = [];
  const metrics = { tok: 0, pre: 0, dec: 0, dup: 0, cache: 0 };
  const items = { mode: scenario, phase: null, prompt: [], gen: [], assem: '', chips: [], flash: null };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items });
  const PROMPT = ['618', '大促', '开始', '啦', '，', '帮', '我', '写', '宣传', '语'];
  const GEN = ['全场', '五折', '起', '，', '先到', '先得', '，', '闭眼入'];
  const CHUNKS = ['本基金全年净值上涨8.2%', '科技与半导体持仓超配', '制造业板块出现回撤', '建议关注三季度调仓'];
  const resetGen = () => { items.gen = GEN.map((t, i) => ({ i: i + 1, t, st: 'wait' })); items.assem = ''; };
  const step = n => {
    for (let k = 0; k < n; k++) { const g = items.gen.find(x => x.st === 'wait'); if (g) g.st = 'done'; }
    items.gen.forEach(g => { if (g.st === 'now') g.st = 'done'; });
    const last = items.gen.filter(g => g.st === 'done').at(-1);
    if (last) last.st = 'now';
    metrics.dec += n;
    metrics.tok += n;
    items.assem = items.gen.filter(g => g.st !== 'wait').map(g => g.t).join('');
  };
  if (scenario === 'prefill') {
    emit('usr', `推理教学模型就绪：场景「${sceneTag}」。一次生成分两段：预填充把整段提示一次并行编码（每个位置只看左侧——因果掩码），解码再逐词元自回归。提示：「618 大促开始啦，帮我写宣传语」。`, 0);
    items.prompt = PROMPT.slice();
    emit('tok', `词元化：提示切成 10 个词元 P1~P10——[618][大促][开始][啦][，][帮][我][写][宣传][语]（中文标点也是词元）。每个词元沿「词嵌入 → 多层注意力 → 输出头」走一遍。`, 0);
    metrics.pre = 10; metrics.tok = 10;
    items.phase = { st: 'ok', txt: '预填 10 词元 · 一次并行' };
    emit('pre', `预填充（1 跳，并行）：10 个位置同时前向——位置 i 的注意力只对 ≤i 的左侧词元打分（因果掩码），一步算出全部提示向量。提示多长基本只影响这一跳，首字延迟（TTFT）由它决定。`, 0);
    items.phase = { st: 'warn', txt: '解码 8 步 · 逐词元自回归' };
    resetGen();
    step(1);
    items.flash = { st: 'warn', t: '步1 · 已见 10 词元 → 「全场」', title: '第 1 步：注意力读全部 10 个提示词元的 K/V，自回归只产出 1 个新词元' };
    emit('dec', '解码步 1：循环开始——每步只产 1 个词元。注意力读 P1~P10（已见 10 词元）打分，采样出「全场」，接回上下文。', 1);
    step(2);
    items.flash = { st: 'warn', t: '步2-3 · 已见 11-12 词元 → 「五折起」', title: '每步能看到的词元越来越多：步 2 读 11 个、步 3 读 12 个——注意力的读盘量随生成推进增长' };
    emit('dec', '解码步 2-3：读已见 11-12 词元，依次生成「五折」「起」——每步都要把「历史 + 已生成」全部词元读一遍才能打分。', 2);
    step(2);
    items.flash = { st: 'warn', t: '步4-5 · 已见 13-14 词元 → 「，先到」', title: '标点同样逐词元生成：步 4 读出「，」、步 5 读 14 个词元出「先到」' };
    emit('dec', '解码步 4-5：已见 13-14 词元，生成「，」「先到」——注意：哪怕只是补个逗号，也是一次完整的自回归步。', 2);
    step(2);
    items.flash = { st: 'warn', t: '步6-7 · 已见 15-16 词元 → 「先得，」', title: '已生成内容越长、单步读得越多：步 6-7 已要读 15-16 个历史词元' };
    emit('dec', '解码步 6-7：已见 15-16 词元，生成「先得」「，」——单步代价随上下文线性涨，累计代价就是平方级。', 2);
    step(1);
    items.phase = { st: 'ok', txt: '生成完毕 · 8/8 词元' };
    items.flash = { st: 'ok', t: '步8 · 已见 17 词元 → 「闭眼入」', title: '最后一步读 17 个词元（10 提示 + 7 已生成）——正文拼完，解码跳数 = 生成词元数' };
    emit('dec', '解码步 8：读全部 17 个历史词元，生成最后一个「闭眼入」。正文拼完：「全场五折起，先到先得，闭眼入」——8 次自回归，每次只前进 1 词元。', 3);
    emit('kv', `运行结束：词元 ${metrics.tok} · 预填 ${metrics.pre} · 解码 ${metrics.dec} · KV 重算 ${metrics.dup} · 缓存 ${metrics.cache} 词元——预填 1 跳并行 + 解码 8 跳串行：第 8 步要读 17 个词元的历史。每步都重新从头读历史正是下个场景 KV Cache 要解决的问题。`, 9);
  } else if (scenario === 'kv') {
    emit('usr', `推理教学模型就绪：场景「${sceneTag}」。同一请求（提示 10 词元 + 生成 8 词元）跑两轮对照：轮 A 无缓存、轮 B 开 KV Cache。焦点：每步解码的注意力都要历史 K/V——它们从哪来，决定了解码的成本曲线。`, 0);
    metrics.pre = 10; metrics.tok = 10;
    items.phase = { st: 'warn', txt: '轮 A · 无 KV 缓存' };
    resetGen();
    emit('pre', `轮 A · 预填（无缓存启动）：10 个提示词元一次并行算完——算出的 K/V 用完即丢，什么也没留下。接下来每步解码都拿不到现成的历史 K/V。`, 0);
    step(1); metrics.dup++;
    items.flash = { st: 'warn', t: '步1 · 重算 +1（10 词元 K/V）', title: '第 1 步需要 P1~P10 的 K/V——缓存里没有，只能把 10 个词元重新前向一遍现场算（重算 1 次）' };
    emit('dec', '轮 A · 解码步 1：注意力要 P1~P10 的 K/V——没缓存！把提示 10 词元整段重新前向一遍才算得出来（重算 +1）。生成「全场」。', 4);
    step(2); metrics.dup += 2;
    items.flash = { st: 'warn', t: '步2-3 · 重算 +2（11-12 词元）', title: '每步都把「历史 + 上一步生成」从头重算：步 2 算 11 词元、步 3 算 12 词元，算完即丢、下步再来' };
    emit('dec', '轮 A · 解码步 2-3：重算 +2——上一步刚算完的 K/V 没留下，这一步的 11-12 词元又从头算一遍。生成「五折起」。', 4);
    step(2); metrics.dup += 2;
    items.flash = { st: 'warn', t: '步4-5 · 重算 +2（13-14 词元）', title: '越往后每步重算量越大：累积起来就是 1+2+…+T 的平方级曲线' };
    emit('dec', '轮 A · 解码步 4-5：重算 +2（13-14 词元）。生成「，先到」——重算量随步数递增，这正是 O(T²) 的来历。', 4);
    step(2); metrics.dup += 2;
    items.flash = { st: 'warn', t: '步6-7 · 重算 +2（15-16 词元）', title: '单步重算量 = 已见词元数 × 层数，每层都要把 QKV 投影重跑一遍' };
    emit('dec', '轮 A · 解码步 6-7：重算 +2（15-16 词元）。生成「先得，」——每一步的等待都包含全部历史的重新前向。', 4);
    step(1); metrics.dup++;
    items.chips.push({ st: 'warn', t: '轮 A 账本：8 步全量重算 · 平方级', title: '8 步累计重算 8 次、每次都要把已见词元整段前向——无缓存时解码越到后面越慢' });
    items.flash = { st: 'warn', t: '步8 · 重算 +1（17 词元）→ 轮 A 完成', title: '第 8 步把 17 词元全部重算一遍才生成「闭眼入」——轮 A 全程零复用' };
    emit('dec', '轮 A · 解码步 8：重算 +1（17 词元），生成「闭眼入」。轮 A 账本：8 步 × 每步全量重算——重复计算随步数平方级上涨，8 词元的小生成都这么费，长文本不敢想。', 4);
    metrics.pre = 20; metrics.tok = 28; metrics.cache = 10;
    items.phase = { st: 'ok', txt: '轮 B · KV Cache' };
    items.flash = { st: 'ok', t: '预填落缓存 · 10 词元 K/V', title: 'KV Cache：预填算出的 K/V 不丢，按层就地存下——这份只算这一次' };
    resetGen();
    emit('kv', '轮 B · 开 KV Cache：预填 10 词元——这次算出的 K/V 按层存入缓存（只算这一次，之后随用随取）。显存开始记账，但算力账先省下。', 5);
    step(2); metrics.cache = 12;
    items.flash = { st: 'ok', t: '步1-2 · 追加 2 词元 K/V（缓存 12）', title: '每步只算新词元自己那组 K/V 追加进缓存——历史 K/V 直接复用，零重算' };
    emit('dec', '轮 B · 解码步 1-2：新词元「全场」「五折」的 K/V 算完即追加进缓存（缓存 12 词元）；历史 K/V 从缓存直接读，一次都没重算。', 5);
    step(2); metrics.cache = 14;
    items.flash = { st: 'ok', t: '步3-4 · 追加 2 词元 K/V（缓存 14）', title: '单步代价不再随历史增长：读缓存 O(1) 增量，生成照常推进' };
    emit('dec', '轮 B · 解码步 3-4：追加「起」「，」的 K/V（缓存 14 词元）——重算计数停住了，这就是缓存的全部意义。', 5);
    step(2); metrics.cache = 16;
    items.flash = { st: 'ok', t: '步5-6 · 追加 2 词元 K/V（缓存 16）', title: '每步只产出并缓存 1 组新 K/V，不再回头重算任何历史' };
    emit('dec', '轮 B · 解码步 5-6：追加「先到」「先得」的 K/V（缓存 16 词元）——注意力从缓存取全部历史 K/V 打分。', 5);
    step(2); metrics.cache = 18;
    items.chips.push({ st: 'ok', t: '轮 B 账本：零重算 · 缓存换延迟', title: '同样的 8 步生成：轮 A 重算 8 次 vs 轮 B 重算 0 次——用线性增长的 KV 缓存买走平方级的重复计算' });
    items.flash = { st: 'ok', t: '步7-8 · 追加 2 词元（缓存 18）· 零重算', title: '18 词元 K/V 全部落地：8 步解码一次历史重算都没有——同样的正文，账本天差地别' };
    emit('dec', '轮 B · 解码步 7-8：追加「，」「闭眼入」的 K/V，缓存 18 词元、零重算，正文与轮 A 完全相同：「全场五折起，先到先得，闭眼入」。', 5);
    emit('kv', `运行结束：词元 ${metrics.tok} · 预填 ${metrics.pre} · 解码 ${metrics.dec} · KV 重算 ${metrics.dup} · 缓存 ${metrics.cache} 词元——轮 A 重算 8 次（每次全量）、轮 B 零重算（每次只算新词元 1 组）：KV Cache = 用线性增长的显存，把平方级的重算买掉。代价预览：这里 18 词元才几 MB，拉到 8k 就是下个场景的数 GB。`, 9);
  } else {
    emit('usr', `推理教学模型就绪：场景「${sceneTag}」。任务：把 8000 词元的年报语料压成摘要。预填 8000 词元只需 1 跳；真正吃紧的是解码期——每步要把历史 KV 全读一遍：缓存多大、带宽多宽，直接决定吐字速度。模型示意 32 层 × 4096 维。`, 6);
    metrics.pre = 8000; metrics.tok = 8000;
    items.phase = { st: 'ok', txt: '预填 8000 词元 · 一次并行' };
    emit('pre', '预填：8000 个词元一次并行编码（因果掩码，只看左侧）——8k 上下文对预填只是 1 跳的事。真正的问题在下边：这些词元的 K/V 要存哪、解码怎么读。', 0);
    metrics.cache = 8000;
    items.phase = { st: 'warn', txt: '全量缓存 · KV ≈ 4.2GB' };
    items.flash = { st: 'warn', t: 'KV = 2 × 8000 × 32 层 × 4096 维 × 2B ≈ 4.2GB', title: '每个词元每层要存 K、V 两份：8000 × 32 × 4096 × 2 字节 × 2 ≈ 4.2GB——缓存随上下文线性涨' };
    emit('kv', '开全量 KV Cache：8000 词元的 K/V 全部落缓存——账本 ≈ 2 × 8000 × 32 层 × 4096 维 × 2B ≈ 4.2GB。显存能装下，但解码每步都要把这份 4.2GB 读一遍。', 6);
    step(8);
    items.assem = CHUNKS[0];
    items.flash = { st: 'warn', t: '步1-8 · 每步搬 4.2GB → 带宽成墙', title: '每步注意力要把 4.2GB 缓存全读一遍打分——单步耗时被显存带宽锁死，上下文越长越慢' };
    emit('dec', '解码步 1-8：每步把 4.2GB 的 K/V 从显存读全参与打分——显存带宽成了解码的墙：这一步的耗时基本由「搬 4.2GB」决定，算力反而闲着。摘要：本基金全年净值上涨8.2%。', 7);
    step(8);
    items.assem = CHUNKS.slice(0, 2).join('');
    items.flash = { st: 'warn', t: '步9-16 · 带宽占满 · 速度被压住', title: '16 步解码每次读全 4.2GB：吐字速度 ≈ 带宽 ÷ 每步读取量——全量缓存的代价是速度' };
    emit('dec', '解码步 9-16：继续每步搬 4.2GB——吐字速度被压住。全量缓存保住了全部记忆，但每步的读盘成本线性放大，长上下文解码拖在带宽上。摘要续：科技与半导体持仓超配。', 7);
    metrics.cache = 2000;
    items.phase = { st: 'ok', txt: '滑窗 2000 词元 · 约 1.0GB' };
    items.flash = { st: 'warn', t: '逐出 6000 词元 · 4.2GB → 约 1.0GB', title: '滑窗只留最近 2000 词元的 K/V：最早 6000 词元被逐出，模型「忘掉」年报开头——需要时只能重新检索或全文重读' };
    items.chips.push({ st: 'warn', t: '记忆代价：开头 6000 词元退出注意力', title: '滑窗是显式遗忘：保速度、保显存，代价是远端上下文从注意力里消失' });
    emit('kv', '切滑窗缓存：只保留最近 2000 词元的 K/V——峰值 4.2GB → 约 1.0GB。代价明牌：最早 6000 词元的上下文被逐出，模型不再记得年报开头，问细节会答不上来。', 8);
    step(8);
    items.assem = CHUNKS.slice(0, 3).join('');
    items.flash = { st: 'ok', t: '步17-24 · 每步只搬约 1.0GB → 约 4× 提速', title: '单步读取量从 4.2GB 降到约 1.0GB：带宽释放约 4×，解码明显变快——滑窗买的正是速度' };
    emit('dec', '解码步 17-24：滑窗后每步只读约 1.0GB 缓存——单步带宽释放约 4×，吐字速度跟着提上来。摘要续：制造业板块出现回撤。', 7);
    step(8);
    items.assem = CHUNKS.join('');
    items.phase = { st: 'ok', txt: '生成完毕 · 32 步' };
    items.flash = { st: 'ok', t: '步25-32 · 摘要完成（32 词元）', title: '32 步解码结束：滑窗内 2000 词元全程可attend，远端记忆缺失由摘要任务本身兜住（语料开头不关键）' };
    emit('dec', '解码步 25-32：摘要生成完毕，32 词元：「本基金全年净值上涨8.2%，科技与半导体持仓超配，制造业板块出现回撤，建议关注三季度调仓。」——任务恰好不依赖年报开头，滑窗的代价没显形。', 7);
    emit('mem', `运行结束：词元 ${metrics.tok} · 预填 ${metrics.pre} · 解码 ${metrics.dec} · KV 重算 ${metrics.dup} · 缓存 ${metrics.cache} 词元——全量缓存 4.2GB 保 8k 记忆但每步搬全量（慢）；滑窗 2000 词元约 1.0GB、约 4× 提速，代价是远端上下文被逐出。显存、速度、记忆三条线画在哪，就是长上下文工程的全部取舍。`, 9);
  }
  return frames;
}

function embeddingVector(p) {
  const scenario = p.scenario || 'vec';
  const sceneTag = { vec: '① 语义向量化 · 相似聚拢', nn: '② 近邻检索 · HNSW vs 暴力', hybrid: '③ 混合检索 · 相关性边界' }[scenario];
  const frames = [];
  const metrics = { vecs: 0, sims: 0, hops: 0, hits: 0, fuse: 0 };
  const items = { scene: scenario, q: null, rows: [], chips: [], flash: null };
  const emit = (active, message, code) => snapshot(frames, { active, message, code, metrics, items });
  if (scenario === 'vec') {
    metrics.vecs = 4;
    items.rows = [
      { id: 'C1', txt: '订单金额改成 8000 元', sc: null, st: 'base' },
      { id: 'C2', txt: '把订单总额调到八千', sc: null, st: 'base' },
      { id: 'C3', txt: '这台苹果手机拍照怎么样', sc: null, st: 'base' },
      { id: 'C4', txt: '这箱苹果甜不甜', sc: null, st: 'base' },
    ];
    emit('corp', `向量检索教学模型就绪：场景「${sceneTag}」。4 条语料在线向量化入库（C1~C4，向量化 4 次）——每个句子编码成 768 维向量：语义相近 ⇔ 夹角小。随后两个查询逐一在线编码、与全库逐条算余弦，观察「同义改写聚拢」与「同词异义分离」。`, 0);
    items.q = 'q1 · 订单价格设为八千';
    metrics.vecs = 5; metrics.sims = 4; metrics.hits = 1;
    items.rows = items.rows.map(r => ({ ...r, sc: { C1: 0.93, C2: 0.90, C3: 0.16, C4: 0.13 }[r.id] }));
    items.flash = { st: 'ok', t: 'q1 → C1 · 0.93 命中', title: '同义改写句与查询的语义重合度最高：设/改、价格/金额、八千/8000 词面全不同，向量距离照样最近' };
    emit('emb', 'q1「订单价格设为八千」在线编码（向量化 5 次），与 4 条语料逐条打分（打分 4 次）：C1 0.93 断层领先、C2 0.90 紧随；C3 0.16、C4 0.13 落在十倍之外——top-1 = C1（正确命中 1）。', 1);
    items.rows = items.rows.map(r => ({ ...r, st: r.id === 'C1' ? 'hit' : r.id === 'C2' ? 'near' : r.id === 'C3' ? 'far' : 'far' }));
    items.flash = { st: 'ok', t: '同义聚拢 · C1/C2 双双 >0.90', title: '改→设、金额→价格、8000→八千：词面没有一处相同，聚拢只靠整句语义——两个向量落在几乎同一方向' };
    emit('corp', '同义聚拢：C1 0.93、C2 0.90 双双贴脸。注意词面：「把…调到八千」与「设为八千」没有任何一个词相同——embedding 编码的是整句语义而非词袋，同义改写自动落在同一方向。', 2);
    items.flash = { st: 'warn', t: '无关沉底 · C3/C4 ≤ 0.16', title: '与「订单改价」主题无关的句子被推到十倍距离外——向量按主题域聚拢，域外无差别' };
    emit('corp', '沉底对比：C3 0.16、C4 0.13，与 C1 的 0.93 差一个数量级——向量按「主题域」聚拢：q1 落在订单改价域，苹果两句自动沉底，不需要任何规则。', 3);
    items.q = 'q2 · 苹果好吃吗';
    metrics.vecs = 6; metrics.sims = 8; metrics.hits = 2;
    items.rows = items.rows.map(r => ({ ...r, sc: { C1: 0.10, C2: 0.09, C3: 0.30, C4: 0.88 }[r.id], st: r.id === 'C4' ? 'hit' : r.id === 'C3' ? 'far' : 'base' }));
    items.flash = { st: 'ok', t: 'q2 → C4 · 0.88 命中', title: '查询落在「水果」域：C4 命中；同含「苹果」的 C3 只拿 0.30——词面相同，方向不同' };
    emit('emb', 'q2「苹果好吃吗」编码（向量化 6 次）、逐条打分（累计 8 次）：C4 0.88 命中（正确命中 2）——同样含「苹果」的 C3 只有 0.30：词面相同，距离却隔了品牌域与水果域。', 4);
    items.flash = { st: 'ok', t: '同词异义 · 语境分流 0.88 vs 0.30', title: '手机/拍照把「苹果」推向品牌域，甜/这箱推向水果域——词向量被整句上下文改造；词袋模型只数词频会让两句并列，数不出这个差别' };
    emit('corp', '语境分流：C3 与 C4 都含「苹果」，得分却差近三倍——embedding 看整句上下文：手机、拍照把它推向品牌域，甜、这箱推向水果域。词袋模型数词频会让两句并列同分，这是语义向量的关键增量。', 4);
    items.flash = null;
    emit('q', `运行结束：向量化 ${metrics.vecs} · 打分 ${metrics.sims} · 正确命中 ${metrics.hits}——同义改写：词面零重叠依然 0.90+ 聚拢；同词异义：词面全同却 0.88 vs 0.30 分流。向量坐标由整句语义决定，词形只是表象。`, 0);
  } else if (scenario === 'nn') {
    metrics.vecs = 1;
    items.q = 'q · 防水轻量的登山背包';
    emit('idx', `近邻检索教学模型就绪：场景「${sceneTag}」。10k 条商品描述已离线向量化入库（向量化只在查询侧计：1 次）；查询 q「防水轻量的登山背包」真近邻是 D7/D52/D803。同一查询跑三种检索，各算各的账：暴力全扫、HNSW 默认、HNSW + efSearch 200。`, 5);
    metrics.sims = 10000; metrics.hops = 0; metrics.hits = 0;
    emit('idx', '暴力近邻：索引就是全量数组——查询向量与 10000 条逐条算余弦（打分 10000 次）。结果精确，代价随库线性涨：库大一倍、时间大一倍。', 5);
    metrics.hits = 3;
    items.rows = [
      { id: 'D7', txt: '户外防水登山背包', sc: 0.87, st: 'hit' },
      { id: 'D52', txt: '轻量徒步双肩包', sc: 0.81, st: 'hit' },
      { id: 'D803', txt: '城市日用双肩包', sc: 0.74, st: 'hit' },
    ];
    items.chips.push({ st: 'ok', t: '暴力全扫 · 打分 10000 · 精确 top-3', title: '全库逐条打分 10000 次取最大 3 条——精确基准：后面两轮的召回都拿它对照' });
    items.flash = { st: 'ok', t: '暴力 top-3 = D7 / D52 / D803', title: '10000 次余弦中的前 3 名：精确最近邻，作为后续对照的基准集' };
    emit('idx', '打分跑满全库，取 top-3：D7 0.87、D52 0.81、D803 0.74——暴力检索给出精确最近邻，这份 top-3 就是后续两轮的对照基准。', 5);
    metrics.sims = 0; metrics.hops = 96;
    emit('nav', 'HNSW：把 10k 向量组织成分层图——顶层只有约 1/16 的「代表点」，越往下越密、底层全量。查询从顶层进入：每层向最近的邻居移动后下探一层，只做了 96 次距离比较（跳转 96）——还没到底层，先别急着打分。', 6);
    metrics.sims = 20; metrics.hits = 2;
    items.rows = [
      { id: 'D7', txt: '户外防水登山背包', sc: 0.87, st: 'hit' },
      { id: 'D52', txt: '轻量徒步双肩包', sc: 0.81, st: 'hit' },
      { id: 'D801', txt: '通勤防泼水背包', sc: 0.72, st: 'near' },
    ];
    items.chips.push({ st: 'warn', t: 'HNSW 默认 · 导航 96 + 精排 20 · 漏召回 1', title: '贪心路径只访问 96 个节点：真近邻 D803（0.74）所在分支没被走到，近似 top-3 换成 D801（0.72）' });
    items.flash = { st: 'warn', t: '近似召回 · D803 → D801', title: '近似最近邻的代价：贪心导航不保证全局最优——3 条里 2 条与暴力一致，1 条被替换' };
    emit('nav', '到底层后对候选 20 条精排（打分 20），取 top-3：D7、D52、D801——近似偏差出现了：真近邻 D803（0.74）所在分支没被贪心路径走到，被 D801（0.72）顶替（正确命中 2/3）。', 6);
    metrics.sims = 0; metrics.hops = 402;
    emit('nav', 'efSearch = 200 重跑：放宽每层保留的候选队列宽度——导航不再只追一条贪心路径，每层多留分支，跳转节点升到 402。代价涨了，但仍只是全扫 10000 的 4%。', 7);
    metrics.sims = 200; metrics.hits = 3;
    items.rows = [
      { id: 'D7', txt: '户外防水登山背包', sc: 0.87, st: 'hit' },
      { id: 'D52', txt: '轻量徒步双肩包', sc: 0.81, st: 'hit' },
      { id: 'D803', txt: '城市日用双肩包', sc: 0.74, st: 'hit' },
    ];
    items.chips.push({ st: 'ok', t: 'efSearch 200 · 导航 402 + 精排 200 · 与暴力对齐', title: '402 次导航 + 200 条精排 ≈ 全扫的 4%——用可控的多算把近似精度找回到与暴力一致' });
    items.flash = { st: 'ok', t: '精度找回 · top-3 = D7 / D52 / D803', title: '放宽每层候选后，D803 所在分支被重新走到——top-3 与暴力基准完全一致' };
    emit('nav', '候选 200 条精排（打分 200），top-3 = D7、D52、D803——与暴力基准完全对齐（正确命中 3/3）。402 次跳转 + 200 次打分，约等于全扫的 4%：可控的多算换回精确性。', 7);
    emit('idx', `运行结束：三种检索各自计量——暴力 打分 10000 · 命中 3（精确基准）；HNSW 默认 跳转 96 + 打分 20 · 命中 2（漏召回 1）；efSearch 200 跳转 402 + 打分 200 · 命中 3（精度找回）。近似检索的每一步多算都在买召回率，曲线由 efSearch 拨动。`, 7);
  } else {
    metrics.vecs = 5;
    items.rows = [
      { id: 'D1', txt: '游戏本 i7+4060 · 7999 元', sc: null, st: 'base' },
      { id: 'D2', txt: '轻薄办公本 · 6999 元', sc: null, st: 'base' },
      { id: 'D3', txt: '游戏本 3050 · 5499 元', sc: null, st: 'base' },
      { id: 'D4', txt: '旗舰游戏本 4070 · 17999 元', sc: null, st: 'base' },
      { id: 'D5', txt: '旗舰游戏本 4090 · 24999 元', sc: null, st: 'base' },
    ];
    emit('corp', `混合检索教学模型就绪：场景「${sceneTag}」。5 条笔记本语料在线向量化入库（向量化 5 次）：三条预算内（D1 7999 / D2 6999 / D3 5499），两条旗舰超预算（D4 17999 / D5 24999）。注意：预算约束是结构化字段，embedding 根本看不见。`, 8);
    items.q = 'q · 1 万内跑 3A 的游戏本';
    metrics.vecs = 6;
    emit('emb', '查询「1 万内跑 3A 的游戏本」在线编码（向量化 6 次）——「1 万内」这个数值约束在向量空间里不存在：没有哪个维度编码「价格 ≤ 10000」。', 8);
    metrics.sims = 5; metrics.hits = 0;
    items.rows = items.rows.map(r => ({ ...r, sc: { D1: 0.85, D2: 0.55, D3: 0.82, D4: 0.88, D5: 0.90 }[r.id], st: ['D5', 'D4', 'D1'].includes(r.id) ? 'top' : 'base' }));
    items.flash = { st: 'warn', t: '纯向量 top-3 = D5 / D4 / D1 · 两条超预算', title: '2.5 万的旗舰在话题上最像（0.90）——embedding 不做数值大小比较，「1 万内」对它不可见' };
    emit('nav', '纯向量检索：对 5 条逐条打分（打分 5 次），排序 D5 0.90 > D4 0.88 > D1 0.85 > D3 0.82 > D2 0.55——返回的 top-3 里 D5（24999）、D4（17999）双双超预算，却排在最前：向量只回答「像不像」。', 8);
    items.flash = { st: 'warn', t: '相似 ≠ 满足 · 边界在数值约束', title: '「1 万以内」是大小比较，embedding 的余弦夹角里没有这个维度——预算判断必须由结构化过滤表达' };
    emit('fuse', '边界确认：D5 0.90 最像，因为它和查询同属「游戏本」话题域——但话题相近 ≠ 需求满足。数值约束在向量空间之外：过滤/规则才是「对不对」的裁判。', 8);
    metrics.fuse = 1;
    items.rows = items.rows.map(r => ({ ...r, st: r.id === 'D5' || r.id === 'D4' ? 'drop' : r.st }));
    emit('fuse', '混合检索第一步——规则过滤（融合 1 次）：价格 ≤ 10000 把 D5（24999）、D4（17999）剔出候选（粉标 2 条）。向量负责候选的召回面，过滤负责把「像但不对」的挡在门外。', 9);
    metrics.sims = 8; metrics.hits = 3;
    items.rows = items.rows.map(r => ({ ...r, st: ['D1', 'D3', 'D2'].includes(r.id) ? 'hit' : r.id === 'D5' || r.id === 'D4' ? 'drop' : 'base' }));
    items.chips.push({ st: 'ok', t: '混合检索 · 过滤 2 条 + 集内精排 3 条', title: '先圈合法集再排相关性：过滤管「对不对」，向量管「像不像」——超预算旗舰不再混进结果' });
    items.flash = { st: 'ok', t: 'D1 7999 · 预算内最像 → 命中', title: '候选集 {D1/D2/D3} 内向量精排：D1 0.85 居首——满足预算约束且话题最贴近的推荐' };
    emit('fuse', '第二步——集内精排（打分累计 8 次）：对过滤后的 {D1、D2、D3} 重排，输出 top-3 = D1（0.85）/ D3（0.82）/ D2（0.55）——三条全部在预算内且相关（正确命中 3）。过滤与向量各管一段，两路召回还可以按名次做 RRF 融合免归一化。', 9);
    emit('q', `运行结束：向量化 ${metrics.vecs} · 打分 ${metrics.sims} · 正确命中 ${metrics.hits} · 融合 ${metrics.fuse}——纯向量把 2.5 万旗舰顶到第一（像），混合检索把它滤出（对）：相似度定序，约束定界。`, 9);
  }
  return frames;
}

// __RUNNERS__
export const agentRunners = { 'rag-chain': ragChain, 'llm-streaming': llmStreaming, 'mcp-protocol': mcpProtocol, 'agent-memory': agentMemory, 'multi-agent': multiAgent, 'llm-inference': llmInference, 'embedding-vector': embeddingVector };
