import React, { useState, useEffect, useMemo, useRef } from 'react';
import { createRoot } from 'react-dom/client';
import { createMachine, assign } from 'xstate';
import { useMachine } from '@xstate/react';
import { ReactFlow, Background, Controls, Handle, Position, MarkerType } from '@xyflow/react';
import { Activity, Archive, ArrowRight, ArrowUpRight, Binary, Bookmark, BookmarkCheck, BookOpen, Box, Boxes, Braces, Check, ChevronDown, ChevronRight, CircleAlert, CircleCheck, Clock, Code2, CodeXml, Coffee, Copy, CornerDownLeft, Cpu, Database, DatabaseZap, Expand, FileCheck2, FlaskConical, GraduationCap, Info, Layers, LibraryBig, ListOrdered, Lock, LocateFixed, Maximize, MemoryStick, Minimize, Monitor, Moon, Network, NotebookPen, PanelLeft, PanelLeftClose, PanelLeftOpen, Pause, Play, Recycle, RefreshCw, RotateCcw, Route, Save, Search, SearchX, Send, Server, ShieldAlert, ShieldCheck, SkipBack, SlidersHorizontal, Sprout, StepForward, Sun, Table2, Terminal, Users, Waypoints, Workflow, X } from 'lucide-react';
import { labs, groups, getLab, planned } from './catalog';
import { simulate } from './models';
import { normalizeParams, playbackView, readLocal, writeLocal } from './settings';
import { getDiagramPositions } from './diagramLayout.js';
import { gradeQuiz, isChallengeComplete, isMastered, recordChallengeRun } from './curriculum';
import { submitReviewAnswer, submitStage } from './learning';
import LearningCenter from './LearningCenter.jsx';
import { learningModules } from './assessmentCatalog.js';
import { isElementFullscreen, toggleElementFullscreen } from './fullscreen.js';
import RealRedisLab from './RealRedisLab.jsx';
import { realLabCatalog } from './realLabCatalog.js';
import LessonDeepDive from './LessonDeepDive.jsx';
import RealJvmLab, { JvmProbeAside } from './RealJvmLab.jsx';
import { jvmProbeCatalog } from './jvmProbeCatalog.js';
import '@xyflow/react/dist/style.css';
import '@fontsource/ibm-plex-mono/latin-400.css';
import '@fontsource/ibm-plex-mono/latin-500.css';
import './styles.css';

const Icons = { Activity, Archive, ArrowRight, ArrowUpRight, Binary, Bookmark, BookmarkCheck, BookOpen, Box, Boxes, Braces, Check, ChevronDown, ChevronRight, CircleAlert, CircleCheck, Clock, Code2, CodeXml, Coffee, Copy, CornerDownLeft, Cpu, Database, DatabaseZap, Expand, FileCheck2, FlaskConical, GraduationCap, Info, Layers, LibraryBig, ListOrdered, Lock, LocateFixed, Maximize, MemoryStick, Minimize, Monitor, Moon, Network, NotebookPen, PanelLeft, PanelLeftClose, PanelLeftOpen, Pause, Play, Recycle, RefreshCw, RotateCcw, Route, Save, Search, SearchX, Send, Server, ShieldAlert, ShieldCheck, SkipBack, SlidersHorizontal, Sprout, StepForward, Sun, Table2, Terminal, Users, Waypoints, Workflow, X };
const priorityRank = { P0: 0, P1: 1, P2: 2 };
const codeFiles = { hashmap: 'HashMapDemo.java', threadpool: 'ThreadPoolDemo.java', kafka: 'KafkaConsumerDemo.java', redis: 'CacheAsideDemo.java', jvm: 'GenerationalGC.java' };
function codeFileName(id) {
  if (id === 'mysql') return 'query.sql';
  if (codeFiles[id]) return codeFiles[id];
  const pascal = id.replace(/-([a-z])/g, (_, c) => c.toUpperCase());
  return pascal.charAt(0).toUpperCase() + pascal.slice(1) + '.java';
}
function Icon({ name, size = 18, ...props }) {
  const Component = Icons[name] || Icons.Box;
  return <Component size={size} strokeWidth={1.65} aria-hidden="true" {...props} />;
}
function IconButton({ icon, label, ...props }) {
  return <button type="button" className="icon-button" title={label} aria-label={label} {...props}><Icon name={icon} /></button>;
}
const player = createMachine({
  id: 'player', initial: 'paused', context: { index: 0, length: 1, frames: null, completed: false },
  on: {
    LOAD: { target: '.paused', actions: assign({ index: 0, length: ({ event }) => event.frames.length, frames: ({ event }) => event.frames, completed: false }) },
    RESET: { target: '.paused', actions: assign({ index: 0, completed: false }) },
    SEEK: { target: '.paused', actions: assign({ index: ({ event, context }) => Math.max(0, Math.min(context.length - 1, event.index)), completed: false }) },
    PREV: { target: '.paused', actions: assign({ index: ({ context }) => Math.max(0, context.index - 1), completed: false }) },
    NEXT: [
      { guard: ({ context }) => context.index >= context.length - 2, target: '.paused', actions: assign({ index: ({ context }) => context.length - 1, completed: true }) },
      { actions: assign({ index: ({ context }) => context.index + 1 }) },
    ],
  },
  states: {
    paused: { on: { PLAY: { target: 'playing', actions: assign({ index: ({ context }) => context.index === context.length - 1 ? 0 : context.index, completed: ({ context }) => context.index === context.length - 1 ? false : context.completed }) } } },
    playing: { on: { PAUSE: 'paused' } },
  },
});
function LabNode({ data }) {
  return <div className={`lab-node ${data.color} ${data.active ? 'active' : ''}`}>
    <Handle type="target" position={Position.Left} />
    <div className="node-top"><span className="node-icon"><Icon name={data.icon} size={23} /></span><span className="node-state">{data.active ? 'ACTIVE' : data.badge}</span></div>
    <strong>{data.label}</strong><span className="node-caption">{data.caption}</span>
    <Handle type="source" position={Position.Right} />
  </div>;
}
const nodeTypes = { lab: LabNode };
function Diagram({ lab, frame }) {
  const container = useRef(null);
  const [flow, setFlow] = useState(null);
  const [compact, setCompact] = useState(() => window.matchMedia('(max-width: 760px)').matches);
  useEffect(() => { const query = window.matchMedia('(max-width: 760px)'); const update = () => setCompact(query.matches); query.addEventListener('change', update); return () => query.removeEventListener('change', update); }, []);
  useEffect(() => {
    if (!flow || !container.current) return;
    let animation;
    const observer = new ResizeObserver(() => {
      cancelAnimationFrame(animation);
      animation = requestAnimationFrame(() => flow.fitView({ padding: lab.layout === 'wide' && !compact ? 0.1 : 0.25 }));
    });
    observer.observe(container.current);
    return () => { observer.disconnect(); cancelAnimationFrame(animation); };
  }, [flow, lab.id, compact]);
  const positions = getDiagramPositions(lab.nodes.length, { wide: lab.layout === 'wide', compact });
  const nodes = lab.nodes.map(([id, label, caption, icon, color], i) => ({ id, position: { x: positions[i][0], y: positions[i][1] }, type: 'lab', data: { label, caption, icon, color, active: frame.active === id, badge: `0${i + 1}` }, draggable: false }));
  const edges = lab.edges.map(([source, target, label], i) => ({ id: `${source}-${target}`, source, target, label,
    type: 'smoothstep', animated: frame.active === target,
    style: { stroke: frame.active === target ? '#55c694' : '#657c75', strokeWidth: frame.active === target ? 2 : 1.3 },
    markerEnd: { type: MarkerType.ArrowClosed, color: '#6b9383' },
    labelStyle: { fill: 'var(--muted)', fontSize: 11 }, labelBgStyle: { fill: 'var(--stage)' }, labelBgPadding: [8, 6], labelBgBorderRadius: 3,
  }));
  return <div className="diagram-canvas" ref={container}><ReactFlow key={`${lab.id}-${compact}`} onInit={setFlow} nodes={nodes} edges={edges} nodeTypes={nodeTypes} fitView fitViewOptions={{ padding: 0.25 }} minZoom={0.3} maxZoom={1.6} nodesConnectable={false} elementsSelectable={false} zoomOnScroll={false} preventScrolling={false} ariaLabelConfig={{ 'controls.zoomIn.ariaLabel': '放大', 'controls.zoomOut.ariaLabel': '缩小', 'controls.fitView.ariaLabel': '适应画布' }}>
    <Background color="var(--grid-dot)" gap={22} size={1} />
    <Controls showInteractive={false} position={lab.layout === 'wide' && !compact ? 'bottom-left' : 'bottom-right'} />
  </ReactFlow></div>;
}
function DataView({ lab, frame }) {
  const items = frame.items;
  if (lab.id === 'redis') return <div className="data-strip"><span className="strip-label"><Icon name="DatabaseZap" size={15} /> CACHE KEYS</span><div className="data-items">{items.length ? items.map(key => <span className="data-token green" key={key}>{key}</span>) : <span className="empty-inline">缓存为空</span>}</div><span className="mono faint">{items.length} entries</span></div>;
  if (lab.id === 'hashmap') return <div className="data-strip"><span className="strip-label">BUCKETS</span><div className="data-items">{items.length ? items.map(item => <span className="data-token green" key={item.label}>[{item.bucket}] {item.label}</span>) : <span className="empty-inline">等待插入</span>}</div></div>;
  if (lab.id === 'threadpool') return <div className="data-strip"><span className="strip-label">TASKS</span><div className="data-items">{items.length ? items.map(item => <span className={`data-token ${item.state === 'rejected' ? 'pink' : item.state === 'queue' ? 'yellow' : 'green'}`} title={item.state} key={item.label}>{item.label}</span>) : <span className="empty-inline">等待任务</span>}</div>{frame.metrics.caller > 0 && <span className="mono faint">caller: {frame.metrics.caller}</span>}</div>;
  if (lab.id === 'kafka') return <div className="data-strip"><span className="strip-label">OFFSETS</span><div className="data-items">{frame.assignments.map((consumer, i) => <span className="data-token green" key={i}>P{i} → C{consumer} · offset {frame.offsets[i]} / {items[i].length}</span>)}</div></div>;
  if (lab.id === 'kafka-replication') return <div className="data-strip"><span className="strip-label">REPLICAS · ISR</span><div className="data-items">{items.map(item => <span key={item.id} className={`data-token ${!item.up ? 'pink' : item.role === 'leader' ? 'green' : 'yellow'}`} title={!item.up ? '已宕机' : item.role === 'leader' ? 'Leader 副本' : 'ISR 跟随副本'}>{item.id} · {item.up ? (item.role === 'leader' ? 'Leader' : 'Follower') : 'Down'}{item.up ? ` · ${item.held} 条` : ' · 曾持消息未复制'}</span>)}</div><span className="mono faint">ISR [{frame.isr.length}] · {frame.isr.join(' → ') || '空'} · sent {frame.metrics.sent} / confirmed {frame.metrics.confirmed}{frame.metrics.lost > 0 ? ` / lost ${frame.metrics.lost}` : ''}{frame.metrics.rejected > 0 ? ` / rejected ${frame.metrics.rejected}` : ''}</span></div>;
  if (lab.id === 'mysql-isolation') return <div className="data-strip"><span className="strip-label">READ CONSISTENCY</span><div className="data-items"><span className="data-token green">B 读行 · balance = {items.txB.row}</span><span className="data-token yellow">B 范围 · {items.txB.count} 行</span><span className={`data-token ${items.txA === 'ACTIVE' ? 'yellow' : items.txA === 'COMMITTED' ? 'green' : ''}`}>A · {items.txA}</span><span className={`data-token ${items.txC === 'BLOCKED' ? 'pink' : items.txC === 'COMMITTED' ? 'green' : items.txC === 'ACTIVE' ? 'yellow' : ''}`}>{items.txC === '—' ? 'C 未开始' : `C · ${items.txC}`}</span></div><span className="mono faint">已提交：balance {items.row.committed} · 表 {items.table.committed} 行</span></div>;
  if (lab.id === 'mysql-replication') return <div className="data-strip"><span className="strip-label"><Icon name="Copy" size={15} /> REPLICATION</span><div className="data-items"><span className={`data-token ${items.mode === 'degraded' ? 'pink' : 'green'}`} title={items.mode === 'degraded' ? '半同步 ack 超时已降级异步：暂时牺牲一致保主库可用' : items.mode === 'semisync' ? '半同步复制：主库提交前等从库 ack' : '异步复制：主库提交零等待，从库尽力追赶'}>{items.mode === 'degraded' ? '降级异步' : items.mode === 'semisync' ? '半同步' : '异步'}</span><span className="data-token" title={items.master === 'S↑' ? '从库已提升为新主' : '主库'}>{items.master}</span>{items.events.length ? items.events.map(e => <span key={e.id} className={`data-token ${e.s === 'a' ? 'green' : e.s === 'r' ? 'yellow' : 'pink'}`} title={`${e.t} · ${e.s === 'a' ? '从库已回放 · 主从一致' : e.s === 'r' ? '已到从库 relay log · 待回放' : e.s === 'x' ? '降级窗口写入 · 随主库宕机丢失' : '主库已提交 · binlog 记录中'}`}>{e.t}</span>) : <span className="empty-inline">等待事务</span>}{items.ack ? <span className="data-token green" title="半同步确认：从库已收到 binlog">ack {items.ack}</span> : null}{items.route ? <span className={`data-token ${items.route === 'master' ? 'green' : 'yellow'}`} title={items.route === 'master' ? '本次读强制走主库：强一致' : '本次读走从库：卸压但有延迟窗口'}>{items.route === 'master' ? '读主库' : '读从库'}</span> : null}{items.lag > 0 ? <span className="data-token yellow" title="复制延迟：主库已提交但从库未回放（Seconds_Behind_Master 教学示意）">lag {items.lag}s</span> : null}</div><span className="mono faint">写入 {frame.metrics.writes} · 回放 {frame.metrics.copies} · 确认 {frame.metrics.acks} · 切换 {frame.metrics.failovers}</span></div>;
  if (lab.id === 'es-sharding') return <div className="data-strip"><span className="strip-label"><Icon name="Activity" size={15} /> SHARDING</span><div className="data-items">{items.nodes.map((st, i) => <span key={i} className={`data-token ${st === 'up' ? 'green' : st === 'down' ? 'pink' : 'yellow'}`} title={st === 'up' ? `节点 N${i + 1} 在线` : st === 'down' ? `节点 N${i + 1} 宕机：其上主分片失联` : `节点 N${i + 1} 网络分区：单票不足 quorum 已降级`}>{`N${i + 1} · ${st === 'up' ? 'UP' : st === 'down' ? 'DOWN' : 'SPLIT'}`}</span>)}{items.promotes.length ? items.promotes.map((pr, i) => <span className="data-token green" key={`pr-${i}`} title="副本自动提升为新主分片">提升 {pr}</span>) : null}{items.docs.length ? items.docs.map((d, i) => <span key={`d-${i}`} className={`data-token ${d.s === 's' ? 'green' : 'yellow'}`} title={d.s === 's' ? `${d.id} 副本已同步 · 主副一致` : `${d.id} 已写入主分片 ${d.shard} · 副本同步中`}>{d.shard} · {d.id}</span>) : null}{items.moving ? <span className="data-token yellow" title="reindex 数据迁移进行中">reindex {items.moving}</span> : null}{!items.docs.length && !items.promotes.length && !items.moving ? <span className="empty-inline">集群空闲</span> : null}</div><span className="mono faint">{items.primaries.join(' · ')} · 写入 {frame.metrics.docs} / 同步 {frame.metrics.syncs} / 提升 {frame.metrics.promotes} / 迁移 {frame.metrics.moves}</span></div>;
  if (lab.id === 'es-write') return <div className="data-strip"><span className="strip-label"><Icon name="FileCheck2" size={15} /> WRITE PATH · SEGMENTS</span><div className="data-items">{items.state !== 'idle' ? <span className={`data-token ${items.state === 'down' ? 'pink' : items.state === 'recovering' ? 'yellow' : 'green'}`} title={items.state === 'down' ? '实例崩溃：内存态（buffer / 内存段 / 未 fsync 日志）全部丢失' : items.state === 'recovering' ? '启动恢复：commit point + 磁盘段 + translog 回放中' : '实例运行中'}>{items.state === 'down' ? '实例崩溃' : items.state === 'recovering' ? '恢复中' : '运行中'}</span> : null}{items.buffer.length ? items.buffer.map(b => <span className="data-token yellow" key={b} title={`${b} 在 indexing buffer：已确认但未 refresh——搜索不可见，等 refresh 快照成段`}>buf {b}</span>) : null}{items.tlog.length ? items.tlog.map(l => <span className={`data-token ${l.fs ? 'green' : 'yellow'}`} key={l.t} title={l.fs ? `${l.t} 已 fsync：日志在磁盘，崩溃可从 translog 回放` : `${l.t} 未 fsync：只在 OS cache，崩溃即丢（async 窗口）`}>log {l.t}{l.fs ? ' ✓' : ''}</span>) : null}{items.segs.length ? items.segs.map(s => <span className={`data-token ${s.mem ? 'yellow' : 'green'}`} key={s.id} title={`${s.id} · ${s.mem ? '内存段：已 refresh 可搜索 · 未落盘，崩溃丢失风险靠 translog 兜底' : '磁盘段：已 fsync 落盘 · 崩溃安全'} · 文档 ${s.docs.join('·')}${s.del.length ? ` · ${s.del.join('·')} 已删（tombstone）· 空间待合并释放` : ''}`}>{s.id} · {s.mem ? '内存' : '磁盘'} · {s.docs.map(d => s.del.includes(d) ? `✗${d}` : d).join('·')}</span>) : items.state !== 'down' ? <span className="empty-inline">0 段 · 等待写入</span> : null}{items.search ? <span className={`data-token ${items.search.hits.length ? 'green' : 'pink'}`} title={items.search.hits.length ? `搜索命中 ${items.search.hits.length} 条：跨全部段归并` : '搜索 0 命中：数据还在 buffer，等下一次 refresh 才可见'}>搜索 · {items.search.hits.length ? `命中 ${items.search.hits.join(' ')}` : '0 命中'}</span> : null}{items.lost.length ? items.lost.map(d => <span className="data-token pink" key={`x${d}`} title={`${d}：ack 已回但日志未 fsync（durability=async 批窗口内），translog 里没有它——随崩溃消失`}>✗ {d} 丢失</span>) : null}{items.flash ? <span className={`data-token ${items.flash.kind === 'crash' || items.flash.kind === 'lost' || items.flash.kind === 'del' || items.flash.kind === 'miss' ? 'pink' : items.flash.kind === 'merge' || items.flash.kind === 'replay' ? 'green' : 'yellow'}`} title={items.flash.kind === 'miss' ? '搜索 miss：buffer 中的数据对搜索不可见——近实时 = 等一次 refresh 成段' : items.flash.kind === 'refresh' ? 'refresh：buffer 快照生成内存段（OS cache）→ 段即刻可搜，buffer 清空' : items.flash.kind === 'crash' ? '实例崩溃：内存态（buffer/内存段/未 fsync 日志）全灭，只剩磁盘段与已 fsync 的 translog' : items.flash.kind === 'replay' ? 'translog 回放：把已 fsync 的日志逐条重放、重建可搜状态' : items.flash.kind === 'lost' ? '丢失：ack 已回但未 fsync，不在 translog 里——durability=async 的批窗口代价' : items.flash.kind === 'merge' ? '后台 merge：小段归并为大段，合并时才真正丢弃 tombstone 文档' : '删除/更新 tombstone：段内打删除标记，搜索即过滤，空间待合并释放'}>{items.flash.text}</span> : null}</div><span className="mono faint">写入 {frame.metrics.docs} · 刷新 {frame.metrics.refreshs} · 合并 {frame.metrics.merges} · 回放 {frame.metrics.replayed}</span></div>;
  if (lab.id === 'rocketmq-ordered') return <div className="data-strip"><span className="strip-label"><Icon name="ListOrdered" size={15} /> ORDERED QUEUES</span><div className="data-items">{items.q0.length ? items.q0.map((m, i) => <span className="data-token yellow" key={`q0-${i}`} title="队列 q0 待消费（顺序语义：队列内 FIFO）">q0 · {m}</span>) : null}{items.q1.length ? items.q1.map((m, i) => <span className="data-token yellow" key={`q1-${i}`} title="队列 q1 待消费（跨队列可并行）">q1 · {m}</span>) : null}{items.retrying ? <span className="data-token pink" title="顺序消费失败：SUSPEND_CURRENT_QUEUE_A_MOMENT 挂起当前队列，原地重试">重试 {items.retrying}</span> : null}{items.delayed.length ? items.delayed.map((d, i) => <span className="data-token yellow" key={`d-${i}`} title={`延迟消息 ${d.label}：SCHEDULE_TOPIC_XXXX L${d.lvl} 档定时队列中，消费者不可见`}>延 {d.label} · L{d.lvl}</span>) : null}{items.done.length ? items.done.map((m, i) => <span className="data-token green" key={`dn-${i}`} title="已按序消费完成">✓ {m}</span>) : null}{!items.q0.length && !items.q1.length && !items.retrying && !items.delayed.length && !items.done.length ? <span className="empty-inline">等待投递</span> : null}</div><span className="mono faint">发送 {frame.metrics.sent} · 消费 {frame.metrics.consumed} · 重试 {frame.metrics.retries} · 积压 {frame.metrics.queued}</span></div>;
  if (lab.id === 'juc-coordination') return <div className="data-strip"><span className="strip-label"><Icon name="Users" size={15} /> JUC SYNC</span><div className="data-items">{items.jobs.map((j, i) => <span key={`j-${i}`} className={`data-token ${j.st === 'done' ? 'green' : j.st === 'wait' ? 'pink' : j.st === 'run' ? 'yellow' : ''}`} title={j.st === 'idle' ? `${j.label} 待启动` : j.st === 'run' ? `${j.label} 执行中` : j.st === 'wait' ? `${j.label} 阻塞等待协调` : `${j.label} 已完成`}>{j.st === 'done' ? '✓ ' : j.st === 'wait' ? '等 ' : ''}{j.label}</span>)}{items.jobs.length ? <span className={`data-token ${items.coord.st === 'wait' ? 'pink' : items.coord.st === 'open' || items.coord.st === 'ok' ? 'green' : items.coord.st === 'busy' ? 'yellow' : ''}`} title="协调器当前状态">{items.coord.txt}</span> : <span className="empty-inline">等待运行</span>}</div><span className="mono faint">发起 {frame.metrics.submitted} · 到达 {frame.metrics.arrived} · 放行 {frame.metrics.released} · 阻塞 {frame.metrics.waiting}</span></div>;
  if (lab.id === 'mysql-crash') return <div className="data-strip"><span className="strip-label"><Icon name="DatabaseZap" size={15} /> INNODB WAL</span><div className="data-items">{items.redos.map((r, i) => <span key={`r-${i}`} className={`data-token ${r.st === 'committed' ? 'green' : r.st === 'discard' ? 'pink' : 'yellow'}`} title={r.st === 'committed' ? `${r.t} 已 commit（或已 fsync 落盘）` : r.st === 'discard' ? `${r.t} prepare 作废 · 随事务回滚` : `${r.t} prepare 已落盘 · 待 commit`}>redo {r.t}</span>)}{items.binlogs.length ? items.binlogs.map((b, i) => <span className="data-token green" key={`b-${i}`} title="binlog 已 fsync：从库可能已收到">bin {b.t}</span>) : null}{items.undos.length ? items.undos.map((u, i) => <span className="data-token yellow" key={`u-${i}`} title={`${u.t} 回滚映像：旧值 ${u.v}，事务未提交期间生效`}>undo {u.t}</span>) : null}{items.state === 'down' ? <span className="data-token pink" title="实例崩溃：Buffer Pool 内容丢失">实例崩溃</span> : null}{items.state === 'recovering' ? <span className="data-token yellow" title="启动恢复：从 checkpoint 重放 redo">恢复中</span> : null}{items.state === 'ok' ? <span className="data-token green" title="恢复完成">已恢复</span> : null}<span className={`data-token ${items.pages[0].st === 'clean' ? 'green' : 'yellow'}`} title={items.pages[0].st === 'clean' ? 'P5 已刷盘 · 与磁盘一致' : 'P5 为脏页 · 修改只在 Buffer Pool'}>{items.pages[0].st === 'clean' ? 'P5 ✓' : 'P5 脏'}</span>{!items.redos.length && !items.binlogs.length && !items.undos.length ? <span className="empty-inline">等待事务</span> : null}</div><span className="mono faint">提交 {frame.metrics.committed} · 刷盘 {frame.metrics.flushed} · 重放 {frame.metrics.replayed} · 回滚 {frame.metrics.rolledback}</span></div>;
  if (lab.id === 'concurrent-hashmap') return <div className="data-strip"><span className="strip-label"><Icon name="Box" size={15} /> {items.newSlots ? 'RESIZE 8 → 16' : 'CHM TABLE'}</span><div className="data-items">{items.threads.map(t => <span key={t.id} className={`data-token ${t.st === 'done' ? 'green' : t.st === 'wait' ? 'pink' : t.st === 'run' ? 'yellow' : ''}`} title={`${t.id} · ${t.op} · ${t.st === 'done' ? '已完成' : t.st === 'wait' ? '阻塞等待 bin 锁' : t.st === 'run' ? '执行中' : '待启动'}`}>{t.st === 'done' ? '✓ ' : t.st === 'wait' ? '等 ' : ''}{t.id} · {t.op.replace(/ ✓$/, '').replace(/ 排队$/, '')}</span>)}{items.slots.some(s => s.fwd) ? <span className="data-token pink" title="已迁移槽位：fwd 占位，读写遇它自动转新表">fwd ×{items.slots.filter(s => s.fwd).length}/8</span> : null}{items.slots.filter(s => !s.fwd && s.keys.length).map(s => <span key={s.i} className={`data-token ${s.keys.length > 1 ? 'yellow' : 'green'}`} title={`槽 ${s.i} · ${s.keys.length > 1 ? '链式追加' : '单 key 直插'}`}>槽{s.i} · {s.keys.join('→')}</span>)}{items.newSlots ? items.newSlots.filter(n => n.keys.length).map(n => <span className="data-token green" key={`n${n.i}`} title={`新表槽 ${n.i}（16 槽）`}>新{n.i} · {n.keys.join('→')}</span>) : null}{items.counter.base > 0 || items.counter.cells.length ? <span className="data-token" title="baseCount：CAS 自增的全局计数">base {items.counter.base}</span> : null}{items.counter.cells.length ? <span className="data-token yellow" title="CounterCell[]：CAS 撞车线程把增量分流进自己的 cell">cells [{items.counter.cells.join('+')}]</span> : null}{items.counter.last != null ? <span className={`data-token ${items.counter.last < frame.metrics.inserted ? 'pink' : 'green'}`} title="size() = baseCount + Σcells 的弱一致快照：求和瞬间的写入可能未入账">size≈{items.counter.last}{items.counter.last < frame.metrics.inserted ? ' · 滞后' : ''}</span> : null}{!items.slots.some(s => s.keys.length || s.fwd) && !(items.newSlots && items.newSlots.some(n => n.keys.length)) ? <span className="empty-inline">table 空 · 等待写入</span> : null}</div><span className="mono faint">写入 {frame.metrics.inserted} · 撞槽 {frame.metrics.conflicts} · 迁移 {frame.metrics.migrated} · 分流 {frame.metrics.spread}</span></div>;
  if (lab.id === 'mysql-lock') return <div className="data-strip"><span className="strip-label"><Icon name="Lock" size={15} /> INNODB LOCKS</span><div className="data-items">{items.threads.map(t => <span key={t.id} className={`data-token ${t.st === 'done' ? 'green' : t.st === 'wait' ? 'pink' : t.st === 'run' ? 'yellow' : ''}`} title={`${t.id} · ${t.op} · ${t.st === 'done' ? '已完成/提交' : t.st === 'wait' ? '锁等待中：阻塞排队' : t.st === 'run' ? '执行中' : t.st === 'idle' ? '被回滚 · 待应用重试' : '待启动'}`}>{t.st === 'done' ? '✓ ' : t.st === 'wait' ? '等 ' : ''}{t.id} · {t.op}</span>)}{items.locks.filter(l => l.holder).map(l => <span key={l.name} className={`data-token ${l.kind === 'GAP' ? 'yellow' : 'green'}`} title={`${l.name} · ${l.kind === 'GAP' ? '间隙锁：罩住还不存在的行，插入意向锁与之互斥 → 防幻读' : 'X 排他记录锁：写者互斥、普通快照读不受影响'} · 由 ${l.holder} 持有`}>{l.name} · {l.kind} · {l.holder}</span>)}{items.wait.length ? items.wait.map(w => <span className="data-token pink" key={w} title={`${w} 在锁等待队列阻塞：请求的锁被他人持有（innodb_lock_wait_timeout 默认 50s，超时抛 1205）`}>等 {w}</span>) : null}{items.victim ? <span className="data-token pink" title={`victim=${items.victim}：死锁环中 undo 代价最小者，被回滚并释放全部锁，应用收到 Error 1213 需重试整个事务`}>✗ {items.victim} · victim 回滚</span> : null}{!items.locks.some(l => l.holder) && !items.wait.length && items.threads.every(t => t.st === 'idle') ? <span className="empty-inline">等待事务启动</span> : null}</div><span className="mono faint">加锁 {frame.metrics.locked} · 锁等待 {frame.metrics.blocked} · 死锁环 {frame.metrics.deadlocks} · 回滚 {frame.metrics.aborted}</span></div>;
  if (lab.id === 'kafka-eos') return <div className="data-strip"><span className="strip-label"><Icon name="ShieldCheck" size={15} /> EOS EXACTLY-ONCE</span><div className="data-items"><span className={`data-token ${items.producer.st === 'down' ? 'pink' : items.producer.st === 'done' ? 'green' : items.producer.st === 'run' || items.producer.st === 'retry' ? 'yellow' : ''}`} title={`producer pid${items.producer.pid} · epoch ${items.producer.epoch} · ${items.producer.st === 'down' ? '崩溃/失联：未拍板事务悬空，等待新实例接管裁决' : items.producer.st === 'done' ? '已结束：全部批次落盘生效/拍板完成' : items.producer.st === 'retry' ? '重试发送：批次携带 (PID, seq)，由 Broker 判重' : items.producer.st === 'run' ? '活动写者：批次携带 (PID, seq)，Broker 按序校验' : '就绪：等待下一条指令'}`}>{items.producer.st === 'done' ? '✓ ' : ''}{items.producer.op}</span>{items.coordinator ? <span className={`data-token ${items.coordinator.st === 'active' ? 'yellow' : 'green'}`} title={`事务协调器 · ${items.coordinator.st === 'active' ? `事务 ${items.coordinator.txn} 未拍板：两阶段裁决中，数据对 read_committed 不可见` : '空闲：可 begin 新事务'} · epoch ${items.coordinator.epoch} · 持有者 ${items.coordinator.holder}`}>coord · {items.coordinator.st === 'active' ? `${items.coordinator.txn} 中` : '空闲'} · e{items.coordinator.epoch} · {items.coordinator.holder}</span> : null}{items.parts.P0.map((e, i) => <span key={`p0-${i}`} className={`data-token ${e.st === 'ok' ? 'green' : e.st === 'abort' ? 'pink' : 'yellow'}`} title={`P0 · ${e.t} · ${e.st === 'ok' ? '已生效：read_committed 可见' : e.st === 'abort' ? '作废段：abort marker 圈禁 · 读端跳过（物理仍占 offset）' : '已落盘 · 待拍板：read_committed 不可见'}${e.by ? ` · 来源 ${e.by}` : ''}`}>{e.st === 'ok' ? '✓ ' : e.st === 'abort' ? '✗ ' : ''}P0 · {e.t}{e.st === 'pend' ? ' · 待拍板' : ''}</span>)}{items.parts.P1.map((e, i) => <span key={`p1-${i}`} className={`data-token ${e.st === 'ok' ? 'green' : e.st === 'abort' ? 'pink' : 'yellow'}`} title={`P1 · ${e.t} · ${e.st === 'ok' ? '已生效：read_committed 可见' : e.st === 'abort' ? '作废段：abort marker 圈禁 · 读端跳过（物理仍占 offset）' : '已落盘 · 待拍板：read_committed 不可见'}${e.by ? ` · 来源 ${e.by}` : ''}`}>{e.st === 'ok' ? '✓ ' : e.st === 'abort' ? '✗ ' : ''}P1 · {e.t}{e.st === 'pend' ? ' · 待拍板' : ''}</span>)}{items.lso ? <span className="data-token" title="LSO last stable offset：读取闸门——read_committed 只读闸门内已拍板数据，abort 段同样被 marker 圈禁跳过">LSO · P0 {items.lso.P0} · P1 {items.lso.P1}</span> : null}{items.flash ? <span className={`data-token ${items.flash.kind === 'marker' ? 'green' : items.flash.kind === 'calib' ? 'yellow' : 'pink'}`} title={items.flash.kind === 'dup' ? '重复批次拦截：seq ≤ 已确认窗口 → 不落盘直接回 ack，重试不再造成重复' : items.flash.kind === 'gap' ? '乱序拦截：seq > 窗口+1 → 抛 OutOfOrderSequenceException，producer 拉窗口校准后重发' : items.flash.kind === 'calib' ? '幂等自愈：校准已确认窗口，把「下一条该发什么」重建为共识' : items.flash.kind === 'marker' ? 'commit marker 已写：两阶段拍板完成，LSO 越过 → 跨分区原子放行' : items.flash.kind === 'abort' ? 'abort marker 圈禁：数据物理保留、逻辑作废，read_committed 跳过整段' : 'epoch 代数校验拒绝：旧代实例的请求抛 FencedInstanceEpochException，僵尸无子弹'}>{items.flash.text}</span> : null}</div><span className="mono faint">发送 {frame.metrics.sent} · 落盘 {frame.metrics.stored} · 生效 {frame.metrics.committed} · 拦截 {frame.metrics.blocked}</span></div>;
  if (lab.id === 'rabbitmq-exchange') return <div className="data-strip"><span className="strip-label"><Icon name="Route" size={15} /> ROUTING</span><div className="data-items">{items.q1.length ? items.q1.map((m, i) => <span className="data-token green" key={`q1-${i}`} title="QueueA">A · {m}</span>) : <span className="empty-inline">QueueA 空</span>}{items.q2.length ? items.q2.map((m, i) => <span className="data-token green" key={`q2-${i}`} title="QueueB">B · {m}</span>) : <span className="empty-inline">QueueB 空</span>}{items.void.length ? items.void.map((m, i) => <span className="data-token pink" key={`v-${i}`} title="未匹配丢弃">✗ {m}</span>) : null}</div><span className="mono faint">A {items.q1.length} · B {items.q2.length} · 丢弃 {items.void.length}</span></div>;
  if (lab.id === 'rabbitmq-ack') return <div className="data-strip"><span className="strip-label"><Icon name="Recycle" size={15} /> DELIVERY</span><div className="data-items">{items.queue.length ? items.queue.map((m, i) => <span className="data-token yellow" key={`q-${i}`} title="待消费">待 {m}</span>) : null}{items.done.length ? items.done.map((m, i) => <span className="data-token green" key={`d-${i}`} title="已确认">✓ {m}</span>) : null}{items.dead.length ? items.dead.map((m, i) => <span className="data-token pink" key={`x-${i}`} title="死信队列">死 {m}</span>) : null}{items.lost.length ? items.lost.map((m, i) => <span className="data-token pink" key={`l-${i}`} title="autoAck 丢失">丢 {m}</span>) : null}{!items.queue.length && !items.done.length && !items.dead.length && !items.lost.length ? <span className="empty-inline">等待投递</span> : null}</div><span className="mono faint">确认 {frame.metrics.acked} · 回队 {frame.metrics.requeued} · 未成功 {frame.metrics.dead}</span></div>;
  if (lab.id === 'rocketmq-tx') return <div className="data-strip"><span className="strip-label"><Icon name="RefreshCw" size={15} /> TX MESSAGE</span><div className="data-items">{items.half.length ? items.half.map((m, i) => <span className="data-token yellow" key={`h-${i}`} title="半消息 PREPARED：消费者不可见">半 {m}</span>) : null}{items.visible.length ? items.visible.map((m, i) => <span className="data-token green" key={`v-${i}`} title="Commit 转正：可被消费">✓ {m}</span>) : null}{items.gone.length ? items.gone.map((m, i) => <span className="data-token pink" key={`g-${i}`} title="Rollback 删除：从未可见">✗ {m}</span>) : null}{!items.half.length && !items.visible.length && !items.gone.length ? <span className="empty-inline">等待发送</span> : null}</div><span className="mono faint">提交 {frame.metrics.committed} · 回滚 {frame.metrics.rolledback} · 回查 {frame.metrics.checks}</span></div>;
  if (lab.id === 'sync-lock') return <div className="data-strip"><span className="strip-label"><Icon name="Monitor" size={15} /> MARK WORD</span><div className="data-items"><span className={`data-token ${items.state === 'biased' ? 'yellow' : items.state === 'light' ? 'green' : items.state === 'heavy' ? 'pink' : ''}`} title="对象头锁状态">{items.state === 'unlocked' ? '无锁' : items.state === 'biased' ? '偏向锁' : items.state === 'light' ? '轻量级锁' : '重量级锁'}</span>{items.owner ? <span className="data-token green" title="持锁线程">持锁 {items.owner}</span> : null}{items.spinners.length ? items.spinners.map((t, i) => <span className="data-token yellow" key={`s-${i}`} title="自旋重试 CAS">旋 {t}</span>) : null}{items.waiters.length ? items.waiters.map((t, i) => <span className="data-token pink" key={`w-${i}`} title="park 挂起于 EntryList">等 {t}</span>) : null}</div><span className="mono faint">升级 {frame.metrics.upgrades} · 自旋 {frame.metrics.spins} · 阻塞 {frame.metrics.blocks}</span></div>;
  if (lab.id === 'aqs-queue') return <div className="data-strip"><span className="strip-label"><Icon name="ListOrdered" size={15} /> CLH QUEUE</span><div className="data-items"><span className={`data-token ${items.state > 1 ? 'pink' : items.state === 1 ? 'green' : ''}`} title="AQS 同步状态：0 空闲 · 1 占用 · N 重入">state {items.state}</span>{items.owner ? <span className="data-token green" title="持锁线程">持锁 {items.owner}</span> : null}{items.queue.length ? items.queue.map((t, i) => <span className="data-token pink" key={`q-${i}`} title="CLH 队列中 park 挂起">等 {t}</span>) : null}</div><span className="mono faint">获取 {frame.metrics.locks} · 重入 {frame.metrics.reentries} · 移交 {frame.metrics.handoffs}</span></div>;
  if (lab.id === 'zookeeper-leader') return <div className="data-strip"><span className="strip-label"><Icon name="Server" size={15} /> ZK CLUSTER</span><div className="data-items">{items.states.map((s, i) => <span key={`s${i}`} className={`data-token ${s === 'LEADING' ? 'green' : s === 'LOOKING' ? 'yellow' : s === 'DOWN' ? 'pink' : ''}`} title={`S${i + 1} · ${s === 'LEADING' ? '主节点：处理写请求与事务提案' : s === 'LOOKING' ? '选举中：广播 (zxid, myid) 投票' : s === 'DOWN' ? '宕机或失联' : '从节点：复制 Leader 事务'}`}>S{i + 1} · {s === 'LEADING' ? '主' : s === 'LOOKING' ? '选' : s === 'DOWN' ? '宕' : '从'}</span>)}</div><span className="mono faint">选举 {frame.metrics.elections} · 投票 {frame.metrics.votes} · 交接 {frame.metrics.handovers} · 同步 {frame.metrics.synced}</span></div>;
  if (lab.id === 'nacos-registry') return <div className="data-strip"><span className="strip-label"><Icon name="Activity" size={15} /> REGISTRY · ORDER-SERVICE</span><div className="data-items">{items.instances.length ? items.instances.map(inst => <span key={inst.id} className={`data-token ${inst.state === 'DOWN' ? 'yellow' : 'green'}`} title={`实例 ${inst.id} · ${inst.addr} · ${inst.state === 'DOWN' ? '心跳超时不健康：已摘出负载池，等待恢复或剔除' : '健康：心跳续约中'}`}>{inst.id} · {inst.state === 'DOWN' ? '心跳超时' : inst.addr}</span>) : <span className="empty-inline">服务目录为空</span>}{items.sub ? <span className="data-token" title={`消费者 C 本地缓存 · 经「${items.sub.via}」更新`}>C 缓存 [{items.sub.cache.join(', ')}]</span> : null}{items.notify ? <span className={`data-token ${items.notify === 'udp' ? 'green' : 'yellow'}`} title={items.notify === 'udp' ? 'UDP 推送：变更毫秒级送达' : '长轮询兜底：挂起请求被即时应答'}>{items.notify === 'udp' ? 'UDP 推送' : '长轮询兜底'}</span> : null}{items.req ? <span className="data-token yellow" title={`消费者 C 的第 ${items.req.n} 次调用`}>#{items.req.n} → {items.req.to}</span> : null}</div><span className="mono faint">注册 {frame.metrics.regs} · 心跳 {frame.metrics.beats} · 推送 {frame.metrics.pushes} · 摘除 {frame.metrics.removals}</span></div>;
  if (lab.id === 'netty-eventloop') return <div className="data-strip"><span className="strip-label"><Icon name="Network" size={15} /> EVENT LOOP</span><div className="data-items">{items.conns.length ? items.conns.map(c => <span key={c.id} className={`data-token ${c.state === 'blocked' ? 'pink' : c.state === 'busy' ? 'yellow' : 'green'}`} title={`${c.id} · ${c.w === '—' ? '已接入，等待分发' : '绑定 ' + c.w} · ${c.state === 'blocked' ? 'handler 阻塞调用中：占住所属 worker，同线程其他连接排队' : c.state === 'busy' ? '事件处理中' : '空闲等待事件'}`}>{c.id} · {c.w === '—' ? '待分发' : c.w}</span>) : <span className="empty-inline">等待连接接入</span>}{items.cur ? <span className="data-token yellow" title="当前正在处理的 IO 事件">处理中 {items.cur}</span> : null}{items.ready.length ? <span className="data-token" title="已就绪但线程正忙：串行排队等待处理">就绪 [{items.ready.join(', ')}]</span> : null}{items.task ? <span className="data-token green" title="EventLoop 任务队列：经 eventLoop.execute 投递，在所属线程串行执行">任务 {items.task}</span> : null}{items.pool > 0 ? <span className="data-token pink" title="业务线程池中执行中的阻塞任务">业务池 ×{items.pool}</span> : null}</div><span className="mono faint">接入 {frame.metrics.conns} · 事件 {frame.metrics.events} · 绑定 {frame.metrics.regs} · 任务 {frame.metrics.tasks}</span></div>;
  if (lab.id === 'nacos-config') return <div className="data-strip"><span className="strip-label"><Icon name="SlidersHorizontal" size={15} /> CONFIG · ORDER-SERVICE.YAML</span><div className="data-items">{items.configs.length ? items.configs.map(cfg => <span key={`${cfg.ns}-${cfg.ver}`} className={`data-token ${cfg.ok ? '' : 'pink'}`} title={`${cfg.ns} 命名空间 · order-service.yaml · ${cfg.ok ? `v${cfg.ver} · 内容指纹 ${cfg.md5}` : `v${cfg.ver} · 错误配置：已发布生效中`}`}>{cfg.ns} · v{cfg.ver} · {cfg.md5}</span>) : <span className="empty-inline">无配置</span>}{items.clients.length ? items.clients.map(c => <span key={c.id} className={`data-token ${c.state === 'error' ? 'pink' : c.state === 'starting' ? 'yellow' : 'green'}`} title={`客户端 ${c.id}（${c.ns}）· ${c.state === 'error' ? '刷新后连接失败：错误配置已生效' : c.state === 'starting' ? '启动中：等待全量拉取' : c.cache ? `本地缓存 v${c.cache} · 指纹 ${c.md5}` : '启动中'}`}>{c.id} · {c.state === 'error' ? '连库失败' : c.cache ? `缓存 v${c.cache}` : '拉取中'}</span>) : null}{items.notify ? <span className={`data-token ${items.notify === 'udp' ? 'green' : 'yellow'}`} title={items.notify === 'udp' ? 'UDP 推送：变更毫秒级送达' : '长轮询兜底：挂起请求被即时应答'}>{items.notify === 'udp' ? 'UDP 推送' : '长轮询兜底'}</span> : null}{items.rollback ? <span className="data-token green" title="回滚 = 把上一版本再发布一次，客户端经推送通道自动恢复">{items.rollback}</span> : null}</div><span className="mono faint">拉取 {frame.metrics.pulls} · 推送 {frame.metrics.pushes} · 刷新 {frame.metrics.refreshes} · 回滚 {frame.metrics.rollbacks}</span></div>;
  if (lab.id === 'es-inverted') return <div className="data-strip"><span className="strip-label"><Icon name="Search" size={15} /> ES INDEX</span><div className="data-items">{items.query ? <span className="data-token yellow" title={`${items.qmode === 'match_phrase' ? 'match_phrase 短语查询：要求词项相邻且顺序一致' : 'match 查询：词项取并集'}：${items.query}`}>{items.qmode === 'match_phrase' ? '词序' : '查询'} {items.query}</span> : null}{items.found.length ? items.found.map(d => <span className="data-token green" key={d} title="命中文档">✓ {d}</span>) : items.query ? <span className="empty-inline">0 篇命中</span> : null}{items.inverted.length ? items.inverted.map(t => <span className="data-token" key={t.term} title={`词项 ${t.term} → 出现于 ${t.docs.join(' · ')}`}>{t.term}·{t.docs.length}</span>) : <span className="empty-inline">空索引 · 等待写入</span>}</div><span className="mono faint">docs {frame.metrics.docs} · 词项 {frame.metrics.terms} · 定位 {frame.metrics.postings} · 命中 {frame.metrics.hits}</span></div>;
  if (lab.id === 'volatile-jmm') return <div className="data-strip"><span className="strip-label"><Icon name="ShieldAlert" size={15} /> SHARED STATE</span><div className="data-items"><span className={`data-token ${items.atomized ? 'green' : items.volatile ? (items.kind === 'count' ? 'yellow' : 'green') : 'yellow'}`} title={items.atomized ? '原子化修复：read-modify-write 一气呵成' : items.volatile ? (items.kind === 'count' ? 'volatile 字段：保证可见但不保证复合原子' : 'volatile：写回主存 + 禁止重排') : '普通字段：写只落线程缓存'}>{(items.kind === 'instance' && items.value === 'null') || (items.kind === 'flag' && items.value === 'false') ? '普通字段' : items.atomized ? 'volatile + 原子操作' : items.volatile ? (items.kind === 'count' ? 'volatile 字段' : 'volatile') : '普通字段'}</span><span className="data-token" title={`主存共享变量（${items.kind === 'flag' ? 'flag' : items.kind === 'instance' ? 'instance' : 'count'}）的最新值`}>主存 {items.value}</span>{items.t1seen !== '—' && <span className="data-token yellow" title="线程 T1（写入/发布方）视角">T1 · {items.t1seen}</span>}{items.t2seen !== '—' && <span className="data-token yellow" title="线程 T2（读取/使用方）视角">T2 · {items.t2seen}</span>}{items.running ? <span className="data-token pink" title="T2 仍在 while 轮询">循环中</span> : null}</div><span className="mono faint">读 {frame.metrics.reads} · 写回 {frame.metrics.writes} · 丢失 {frame.metrics.lost} · 屏障 {frame.metrics.barriers}</span></div>;
  if (lab.id === 'jvm') return <div className="data-strip"><span className="strip-label">HEAP</span><div className="data-items"><span className="data-token green">Eden · {items.eden.length}</span><span className="data-token pink">Old · {items.old.length}</span>{items.eden.map(item => <span key={item} className="heap-object" title={item} />)}</div></div>;
  if (lab.group === 'Redis 专题') return <div className="data-strip"><span className="strip-label"><Icon name="DatabaseZap" size={15} />{frame.dataLabel || 'REDIS STATE'}</span><div className="data-items">{items.length ? items.map((item, i) => <span className="data-token green" key={`${item}-${i}`}>{item}</span>) : <span className="empty-inline">暂无数据</span>}</div><span className="mono faint">{items.length} items</span></div>;
  if (lab.group === 'JVM 专题') return <div className="data-strip"><span className="strip-label"><Icon name="MemoryStick" size={15} />{frame.dataLabel || 'JVM STATE'}</span><div className="data-items">{items.length ? items.map((item, i) => <span className="data-token green" key={`${item}-${i}`}>{item}</span>) : <span className="empty-inline">暂无运行时状态</span>}</div><span className="mono faint">{items.length} items</span></div>;
  return <div className="data-strip"><span className="strip-label">LEAF PAGE</span><div className="data-items">{items.length ? items.map(item => <span key={item} className={`data-token ${item === frame.metrics.target ? 'green' : ''}`}>{item}</span>) : <span className="empty-inline">{frame.metrics.found ? `id=${frame.metrics.target} 已返回` : '等待查询'}</span>}</div></div>;
}
function TextParameter({ id, value, fallback, onCommit }) {
  const [draft, setDraft] = useState(value);
  const [hint, setHint] = useState('');
  const hintTimer = useRef(null);
  useEffect(() => setDraft(value), [value]);
  useEffect(() => () => clearTimeout(hintTimer.current), []);
  function showHint(text) { setHint(text); clearTimeout(hintTimer.current); hintTimer.current = setTimeout(() => setHint(''), 2400); }
  function commit() {
    const trimmed = draft.trim();
    if (!trimmed) { const restored = fallback; setDraft(restored); onCommit(restored); showHint(`空值已回退为默认「${restored}」`); return; }
    const normalized = trimmed.slice(0, 32);
    if (normalized !== draft) { setDraft(normalized); showHint('超过 32 字符的部分已被截断'); }
    onCommit(normalized);
  }
  return <><input id={id} type="text" maxLength={32} value={draft} onChange={e => { setDraft(e.target.value); if (hint) setHint(''); }} onBlur={commit} onKeyDown={e => { if (e.key === 'Enter') e.currentTarget.blur(); }} />{hint && <small className="param-hint" role="status">{hint}</small>}</>;
}
function RealAside({ lab, progress }) {
  const guide = realLabCatalog[lab.id];
  return <aside className="real-aside" aria-label="真实实验任务"><div className="panel-title"><h2>引导步骤</h2><span className="real-badge">{progress.index}/{guide.steps.length}</span></div><div className="real-aside-title">{guide.objective}</div><div className="real-step-list">{guide.steps.map((step, index) => <div className={index < progress.index ? 'done' : index === progress.index ? 'current' : ''} key={step.id}><span>{String(index + 1).padStart(2, '0')}</span><div><strong>{step.title}</strong><code>{step.command}</code></div>{index < progress.index && <Icon name="Check" size={13} />}</div>)}</div><div className="real-aside-note"><Icon name="Info" size={15} /><p>{guide.scope}</p></div><div className="real-aside-note"><Icon name="ShieldAlert" size={15} /><p>仅操作本课独立前缀内的数据。重新开始后，第一步会清理本课列出的练习 Key。</p></div></aside>;
}
function Params({ lab, params, onChange, onSave, onReset, frame }) {
  return <aside className="parameters" aria-label="实验参数">
    <div className="panel-title"><h2><Icon name="SlidersHorizontal" size={16} /> 实验参数</h2><IconButton icon="RotateCcw" label="恢复默认参数" onClick={onReset} /></div>
    <div className="param-fields">{lab.fields.map(field => <div className={`field ${field.type === 'toggle' ? 'toggle-field' : ''}`} key={field.key}>
      <label htmlFor={field.key}>{field.label}</label>
      {field.type === 'select' ? <select id={field.key} value={params[field.key]} onChange={e => onChange(field.key, typeof lab.defaults[field.key] === 'number' ? Number(e.target.value) : e.target.value)}>{field.options.map(([value, label]) => <option value={value} key={value}>{label}</option>)}</select>
        : field.type === 'text' ? <TextParameter id={field.key} value={params[field.key]} fallback={lab.defaults[field.key]} onCommit={value => onChange(field.key, value)} />
        : field.type === 'toggle' ? <label className="switch"><input id={field.key} type="checkbox" checked={params[field.key]} onChange={e => onChange(field.key, e.target.checked)} /><span /></label>
        : <><div className="range-value"><input id={field.key} type="range" min={field.min} max={field.max} step={field.step || 1} value={params[field.key]} onChange={e => onChange(field.key, Number(e.target.value))} /><output htmlFor={field.key}>{params[field.key]}<small>{field.unit}</small></output></div><div className="range-bounds"><span>{field.min}</span><span>{field.max}</span></div></>}
    </div>)}</div>
    <button className="save-params" onClick={onSave}><Icon name="Save" size={15} />保存参数</button>
    <div className="metrics-section"><h2><Icon name="Activity" size={16} /> 实时读数</h2><div className="metrics">{lab.metrics.map(([key, label, unit], i) => <div className="metric" key={key}><span>{label}</span><strong className={i === 1 || i === 3 ? 'accent' : ''}>{typeof frame.metrics[key] === 'boolean' ? frame.metrics[key] ? '找到' : '等待' : frame.metrics[key]}<small>{unit}</small></strong></div>)}</div></div>
    <div className="boundary"><Icon name="Info" size={16} /><div><strong>模型边界</strong><p>{lab.boundary}</p></div></div>
    <a className="source-link" href={lab.source} target="_blank" rel="noreferrer">官方参考文档<Icon name="ArrowUpRight" size={15} /></a>
  </aside>;
}
function Modal({ title, children, onClose, className = '' }) {
  const ref = useRef(null);
  useEffect(() => { const element = ref.current; element.showModal(); return () => element.close(); }, []);
  return <dialog className={className} ref={ref} onCancel={e => { e.preventDefault(); onClose(); }} onClick={e => { if (e.target === ref.current) onClose(); }}><div className="dialog-heading"><h2>{title}</h2><IconButton icon="X" label="关闭" onClick={onClose} /></div>{children}</dialog>;
}
function KnowledgeCheck({ lab, passed, experimentDone, challengeDone, onPass }) {
  const [answers, setAnswers] = useState({});
  const [result, setResult] = useState(null);
  const [changed, setChanged] = useState([]);
  function choose(question, choice) {
    setAnswers(prev => ({ ...prev, [question]: choice }));
    if (result && !changed.includes(question)) setChanged(prev => [...prev, question]);
  }
  function submit() {
    const next = gradeQuiz(lab.quiz, answers);
    setResult(next);
    setChanged([]);
    if (next.passed) onPass(lab.id);
  }
  const status = result ? result.passed ? experimentDone && challengeDone ? '实验、对照任务与验证均已完成，本课已掌握。' : !experimentDone ? '验证通过，还需完整运行实验。' : '验证通过，还需完成全部对照场景。' : `${result.score}/${lab.quiz.length} 正确，请根据解析重新判断。` : passed ? experimentDone && challengeDone ? '本课已掌握，可重新作答。' : '验证题已通过，继续完成实验和对照任务。' : `全部 ${lab.quiz.length} 题答对后记录验证结果。`;
  return <section className="knowledge-check" aria-labelledby="knowledge-check-title">
    <div className="section-title"><h2 id="knowledge-check-title"><Icon name="CircleCheck" size={17} />原理验证</h2><span>{passed ? 'CHECK PASSED' : `${lab.quiz.length} QUESTIONS`}</span></div>
    <div className="quiz-grid">{lab.quiz.map((question, questionIndex) => { const dirty = result && changed.includes(questionIndex); return <fieldset key={question.prompt} className={!dirty && result ? result.details[questionIndex].correct ? 'correct' : 'incorrect' : ''}>
      <legend><span>{String(questionIndex + 1).padStart(2, '0')}</span>{question.prompt}</legend>
      <div className="quiz-options">{question.choices.map((choice, choiceIndex) => <label key={choice}><input type="radio" name={`${lab.id}-question-${questionIndex}`} checked={answers[questionIndex] === choiceIndex} onChange={() => choose(questionIndex, choiceIndex)} /><span>{choice}</span></label>)}</div>
      {!dirty && result && <p className="quiz-explanation"><Icon name={result.details[questionIndex].correct ? 'Check' : 'Info'} size={14} />{question.explanation}</p>}
    </fieldset>; })}</div>
    <div className="quiz-actions"><button className="verify-button" disabled={Object.keys(answers).length !== lab.quiz.length} onClick={submit}><Icon name="Check" size={15} />提交验证</button><span aria-live="polite">{changed.length ? `已修改 ${changed.length} 道题的答案，重新提交后将按新答案判定。` : status}</span></div>
  </section>;
}
function ChallengePanel({ lab, history }) {
  const requirements = lab.challenge.requirements || [];
  const done = requirements.filter(requirement => history[requirement.id]).length;
  return <div className="challenge-column"><div className="section-title"><h2><Icon name="FlaskConical" size={17} />调参任务</h2><span>{done} / {requirements.length} SCENARIOS</span></div><p>{lab.challenge.task}</p><div className="challenge-requirements">{requirements.map(requirement => { const evidence = history[requirement.id]; return <div className={evidence ? 'complete' : ''} key={requirement.id}><span className="requirement-mark">{evidence ? <Icon name="Check" size={12} /> : null}</span><span><strong>{requirement.label}</strong>{evidence && <small>{lab.metrics.slice(0, 2).map(([key, label]) => `${label} ${evidence.metrics[key]}`).join(' · ')}</small>}</span></div>; })}</div><small><strong>观察重点：</strong>{lab.challenge.hint}</small><small><strong>完成标准：</strong>{lab.challenge.success}</small></div>;
}
function LessonPath({ lab, moduleProgress }) {
  if (!lab.module) return null;
  return <div className="module-path"><div><Icon name="Route" size={15} /><span>{lab.module} 核心知识路径</span><strong>{moduleProgress.mastered} / {moduleProgress.total}</strong></div><div className="module-progress" aria-label={`${lab.module} 专题已掌握 ${moduleProgress.mastered} 课`}>{Array.from({ length: moduleProgress.total }, (_, i) => <i className={moduleProgress.lessons[i] ? 'done' : i === moduleProgress.index ? 'current' : ''} key={i} />)}</div></div>;
}
function Experiment({ lab, notify, onComplete, onChallengeRun, experimentDone, challengeHistory, quizPassed, onQuizPass, moduleProgress, bookmarked, onBookmark, onMenu }) {
  const [params, setParams] = useState(() => normalizeParams(lab, readLocal(`java-lab:${lab.id}`, {})));
  const frames = useMemo(() => simulate(lab.id, params), [lab, params]);
  const [state, send] = useMachine(player);
  const { index, complete } = playbackView(state.context, frames);
  const frame = frames[index];
  const playing = state.matches('playing');
  const [speed, setSpeed] = useState(1);
  const [tab, setTab] = useState('diagram');
  const [guide, setGuide] = useState(false);
  const [copied, setCopied] = useState(false);
  const [workbenchFullscreen, setWorkbenchFullscreen] = useState(false);
  const [realProgress, setRealProgress] = useState({ index: 0, complete: false, evidence: [] });
  const logEnd = useRef(null);
  const workbenchRef = useRef(null);
  const hasRealLab = lab.group === 'Redis 专题' && !!realLabCatalog[lab.id];
  const hasJvmProbe = lab.group === 'JVM 专题' && !!jvmProbeCatalog[lab.id];
  const isRealView = tab === 'real' || tab === 'jvm-real';
  useEffect(() => { send({ type: 'LOAD', frames }); }, [frames, send]);
  useEffect(() => { if (!playing) return; const timer = setInterval(() => send({ type: 'NEXT' }), 1100 / speed); return () => clearInterval(timer); }, [playing, speed, send]);
  useEffect(() => {
    function handleKeyDown(event) {
      if (event.repeat || event.metaKey || event.ctrlKey || event.altKey) return;
      const target = event.target;
      if (target instanceof HTMLElement && (target.tagName === 'INPUT' || target.tagName === 'TEXTAREA' || target.tagName === 'SELECT' || target.isContentEditable)) return;
      if (document.querySelector('dialog[open]')) return;
      if (tab === 'real' || tab === 'jvm-real') return;
      if (event.code === 'Space') {
        if (target instanceof HTMLElement && target.tagName === 'BUTTON') return;
        event.preventDefault();
        send({ type: playing ? 'PAUSE' : 'PLAY' });
      } else if (event.code === 'ArrowLeft') { event.preventDefault(); send({ type: 'PREV' }); }
      else if (event.code === 'ArrowRight') { event.preventDefault(); send({ type: 'NEXT' }); }
      else if (event.code === 'KeyR') send({ type: 'RESET' });
    }
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [send, playing, tab]);
  useEffect(() => { if (complete) { onComplete(lab.id); onChallengeRun(lab, params, frame.metrics); } }, [complete, frame.metrics, lab, onChallengeRun, onComplete, params]);
  useEffect(() => { if (tab === 'logs') logEnd.current?.scrollIntoView({ block: 'nearest' }); }, [index, tab]);
  useEffect(() => {
    const update = () => setWorkbenchFullscreen(isElementFullscreen(workbenchRef.current, document));
    document.addEventListener('fullscreenchange', update);
    document.addEventListener('webkitfullscreenchange', update);
    return () => { document.removeEventListener('fullscreenchange', update); document.removeEventListener('webkitfullscreenchange', update); };
  }, []);
  function change(key, value) { setParams(prev => normalizeParams(lab, { ...prev, [key]: value })); }
  async function copy() {
    try { await navigator.clipboard.writeText(lab.code.join('\n')); setCopied(true); setTimeout(() => setCopied(false), 1800); } catch { notify('复制失败，请在代码视图中选择文本复制', 'error'); }
  }
  async function toggleWorkbench() {
    try { await toggleElementFullscreen(workbenchRef.current, document); }
    catch { notify('当前浏览器不支持实验工作区全屏', 'error'); }
  }
  return <main className="workspace">
    <div className="breadcrumb"><button className="mobile-menu icon-button" title="课程目录" aria-label="课程目录" onClick={onMenu}><Icon name="PanelLeft" /></button><span>实验室</span><Icon name="ChevronRight" size={13} /><span>{lab.group}</span><Icon name="ChevronRight" size={13} /><span className="breadcrumb-current">{lab.short}</span><span className="experiment-number">EXPERIMENT / {lab.number}</span></div>
    <LessonPath lab={lab} moduleProgress={moduleProgress} />
    <div className="experiment-heading"><div><div className="title-line"><h1>{lab.title}</h1><span className="topic-tag">{lab.tag}</span></div><p>{lab.summary}</p></div><div className="heading-actions"><button className="quiet-button" aria-label="实验笔记" title="实验笔记" onClick={() => setGuide(true)}><Icon name="BookOpen" size={16} /><span>实验笔记</span></button><IconButton icon={bookmarked ? 'BookmarkCheck' : 'Bookmark'} label={bookmarked ? '取消收藏' : '收藏实验'} onClick={onBookmark} /></div></div>
    <div className="experiment-layout">
      <div className="experiment-main">
        <section ref={workbenchRef} className="workbench" aria-label="实验工作区">
          <div className="workbench-tabs" role="tablist" aria-label="实验视图">{[['diagram', 'Workflow', '架构视图'], ...(hasRealLab ? [['real', 'DatabaseZap', '真实实验']] : []), ...(hasJvmProbe ? [['jvm-real', 'Cpu', 'JDK 实机']] : []), ['code', 'Code2', 'Java 代码'], ['logs', 'Terminal', '运行日志']].map(([id, icon, label]) => <button key={id} id={`tab-${id}`} role="tab" aria-selected={tab === id} aria-controls={`view-${id}`} onClick={() => { if (id === 'real' || id === 'jvm-real') send({ type: 'PAUSE' }); setTab(id); }} className={tab === id ? 'selected' : ''}><Icon name={icon} size={15} />{label}{id === 'logs' && <span className="tab-count">{index}</span>}</button>)}<span className={`stage-status ${playing ? 'running' : ''}`}><i />{tab === 'real' ? realProgress.complete ? '本轮已验证' : '引导实验' : tab === 'jvm-real' ? '实机数据' : playing ? '运行中' : index === frames.length - 1 ? '已完成' : index ? '已暂停' : '就绪'}</span><IconButton icon={workbenchFullscreen ? 'Minimize' : 'Maximize'} label={workbenchFullscreen ? '退出实验工作区全屏' : '实验工作区全屏'} onClick={toggleWorkbench} /></div>
          <div className={`stage ${tab === 'real' ? 'guided-stage' : ''} ${tab === 'jvm-real' ? 'probe-stage' : ''}`} role="tabpanel" id={`view-${tab}`} aria-labelledby={`tab-${tab}`}>
            {hasRealLab && <div className="guided-host" hidden={tab !== 'real'}><RealRedisLab lesson={lab} onComplete={() => notify('本课真实命令实验已通过')} onProgress={setRealProgress} active={tab === 'real'} /></div>}
            {tab === 'diagram' ? <><div className="stage-topline"><span>{lab.id === 'redis' ? 'CACHE-ASIDE ARCHITECTURE' : lab.title}</span><span className="stage-legend"><i className="legend-dot" />当前步骤<span className="legend-line" />数据流向</span></div><Diagram lab={lab} frame={frame} /></>
              : tab === 'real' ? null
              : tab === 'jvm-real' ? <RealJvmLab lesson={lab} />
              : tab === 'code' ? <div className="code-view"><div className="code-top"><span>{codeFileName(lab.id)}<small>教学示意代码</small></span><IconButton icon={copied ? 'Check' : 'Copy'} label={copied ? '已复制' : '复制代码'} onClick={copy} /></div><pre>{lab.code.map((line, i) => <div key={i} className={i === frame.code ? 'highlight-line' : ''}><span className="line-number">{i + 1}</span><code>{line}</code></div>)}</pre></div>
              : <div className="log-view">{frames.slice(0, index + 1).map((entry, i) => <div className="log-line" key={i}><span className="log-index">{String(i).padStart(3, '0')}</span><span className="log-level">{entry.message.includes('MISS') ? 'MISS' : 'INFO'}</span><span>{entry.message}</span></div>)}<div ref={logEnd} /></div>}
          </div>
          {!isRealView && <><DataView lab={lab} frame={frame} /><div className="player"><div className="player-buttons" title="快捷键：空格 播放/暂停 · ←/→ 上一步/下一步 · R 重置（输入框内不生效）"><button className="play-button" onClick={() => send({ type: playing ? 'PAUSE' : 'PLAY' })} aria-label={playing ? '暂停演示' : index === frames.length - 1 ? '重新演示' : '开始演示'}><Icon name={playing ? 'Pause' : 'Play'} size={16} /><span>{playing ? '暂停' : index === frames.length - 1 ? '重新演示' : '开始演示'}</span></button><IconButton icon="SkipBack" label="上一步" disabled={index === 0} onClick={() => send({ type: 'PREV' })} /><IconButton icon="StepForward" label="下一步" disabled={index === frames.length - 1} onClick={() => send({ type: 'NEXT' })} /><IconButton icon="RotateCcw" label="重置实验" onClick={() => send({ type: 'RESET' })} /></div><div className="timeline"><input type="range" aria-label="演示进度" min="0" max={frames.length - 1} value={index} onChange={e => send({ type: 'SEEK', index: Number(e.target.value) })} /><span className="mono">{String(index).padStart(2, '0')} / {frames.length - 1}</span></div><select className="speed" aria-label="播放速度" value={speed} onChange={e => setSpeed(Number(e.target.value))}><option value="0.5">0.5×</option><option value="1">1×</option><option value="2">2×</option></select></div></>}
        </section>
        {!isRealView && <div className="step-status" aria-live="polite"><span className="step-number">{String(index).padStart(2, '0')}</span><span>{frame.message}</span><Icon name="CornerDownLeft" size={15} /></div>}
        <section className="principles"><div className="section-title"><h2><Icon name="NotebookPen" size={17} />原理与关键概念</h2><span>UNDER THE HOOD</span></div><div className="principle-grid">{lab.theory.map(([title, formula, text], i) => <article key={title}><span className="concept-index">0{i + 1}</span><h3>{title}</h3><strong>{formula}</strong><p>{text}</p></article>)}</div></section>
        <LessonDeepDive lessonId={lab.id} />
        {lab.goals && <section className="learning-objectives"><div className="objective-column"><div className="section-title"><h2><Icon name="GraduationCap" size={17} />本课目标</h2><span>LEARNING GOALS</span></div><ul>{lab.goals.map(goal => <li key={goal}><Icon name="Check" size={14} />{goal}</li>)}</ul></div><ChallengePanel lab={lab} history={challengeHistory} /></section>}
        {lab.quiz && <KnowledgeCheck lab={lab} passed={quizPassed} experimentDone={experimentDone} challengeDone={isChallengeComplete(lab.challenge, challengeHistory)} onPass={onQuizPass} />}
      </div>
      {tab === 'real' ? <RealAside lab={lab} progress={realProgress} /> : tab === 'jvm-real' ? <JvmProbeAside lesson={lab} /> : <Params lab={lab} params={params} frame={frame} onChange={change} onSave={() => { const saved = writeLocal(`java-lab:${lab.id}`, params); notify(saved ? '当前参数已保存到本机' : '当前浏览器无法保存参数', saved ? 'success' : 'error'); }} onReset={() => { setParams({ ...lab.defaults }); notify('已恢复默认参数'); }} />}
    </div>
    <footer className="workspace-footer"><span><Icon name="FlaskConical" size={13} />实验驱动理解 · Java 技术实验室</span><span>{tab === 'real' ? '本机 Java 网关 · 真实 Redis 命令' : tab === 'jvm-real' ? '本机 Java 网关 · 真实 JDK 观测' : '浏览器教学模拟 · 非真实服务'}</span></footer>
    {guide && <Modal title={`${lab.title} · 实验笔记`} onClose={() => setGuide(false)}><div className="notes">{lab.theory.map(([title, formula, text]) => <section key={title}><h3>{title}</h3><code>{formula}</code><p>{text}</p></section>)}<p className="note-boundary">{lab.boundary}</p><a href={lab.source} target="_blank" rel="noreferrer">阅读官方参考文档 <Icon name="ArrowUpRight" size={14} /></a></div></Modal>}
  </main>;
}
function App() {
  const [labId, setLabId] = useState(() => getLab(location.hash.slice(1)).id);
  const lab = getLab(labId);
  const [search, setSearch] = useState('');
  const [filter, setFilter] = useState('all');
  const [closed, setClosed] = useState([]);
  const [bookmarks, setBookmarks] = useState(() => { const stored = readLocal('java-lab:bookmarks', []); return Array.isArray(stored) ? stored.filter(id => labs.some(l => l.id === id)) : []; });
  const [completed, setCompleted] = useState(() => { const stored = readLocal('java-lab:completed', []); return Array.isArray(stored) ? [...new Set(stored.filter(id => labs.some(l => l.id === id)))] : []; });
  const [assessments, setAssessments] = useState(() => { const stored = readLocal('java-lab:assessments', []); return Array.isArray(stored) ? [...new Set(stored.filter(id => labs.some(l => l.id === id)))] : []; });
  const [challengeRuns, setChallengeRuns] = useState(() => { const stored = readLocal('java-lab:challenge-runs', {}); return stored && typeof stored === 'object' && !Array.isArray(stored) ? stored : {}; });
  const [learningState, setLearningState] = useState(() => { const stored = readLocal('java-lab:learning-center', {}); return stored && typeof stored === 'object' && !Array.isArray(stored) ? stored : {}; });
  const [centerName, setCenterName] = useState(() => { const stored = readLocal('java-lab:learning-module'); return stored === 'Redis' || stored === 'JVM' ? stored : 'Redis'; });
  const [theme, setTheme] = useState(() => readLocal('java-lab:theme', 'dark') === 'light' ? 'light' : 'dark');
  const [toast, setToast] = useState(null);
  const [mobileOpen, setMobileOpen] = useState(false);
  const [route, setRoute] = useState(false);
  const [focus, setFocus] = useState(false);
  const [fullscreen, setFullscreen] = useState(false);
  const [centerDirty, setCenterDirty] = useState(false);
  const [centerConfirm, setCenterConfirm] = useState(false);
  const [centerPending, setCenterPending] = useState(null);
  const [onboard, setOnboard] = useState(() => !readLocal('java-lab:onboarded', false));
  const toastTimer = useRef(null);
  const notify = React.useCallback((message, kind = 'success') => { setToast({ message, kind }); clearTimeout(toastTimer.current); toastTimer.current = setTimeout(() => setToast(null), 2800); }, []);
  const markComplete = React.useCallback(id => setCompleted(prev => { if (prev.includes(id)) return prev; const next = [...prev, id]; writeLocal('java-lab:completed', next); notify('实验演示已完成，已记入学习记录'); return next; }), [notify]);
  const markAssessment = React.useCallback(id => setAssessments(prev => { if (prev.includes(id)) return prev; const next = [...prev, id]; writeLocal('java-lab:assessments', next); return next; }), []);
  const markChallengeRun = React.useCallback((lesson, params, metrics) => setChallengeRuns(prev => { const history = prev[lesson.id] || {}; const nextHistory = recordChallengeRun(lesson.challenge, history, params, metrics); if (nextHistory === history) return prev; const next = { ...prev, [lesson.id]: nextHistory }; writeLocal('java-lab:challenge-runs', next); return next; }), []);
  const selectCenterModule = React.useCallback(name => { setCenterName(name); writeLocal('java-lab:learning-module', name); }, []);
  const requestCloseCenter = React.useCallback(() => {
    if (centerConfirm) { setCenterConfirm(false); setCenterPending(null); return; }
    if (centerDirty) { setCenterConfirm(true); return; }
    setCenterDirty(false);
    setRoute(false);
  }, [centerDirty, centerConfirm]);
  const confirmLeaveCenter = React.useCallback(() => {
    const pending = centerPending;
    setCenterDirty(false);
    setCenterConfirm(false);
    setCenterPending(null);
    if (pending?.action === 'lesson') { select(pending.id); return; }
    if (pending?.action === 'switch') { selectCenterModule(pending.name); return; }
    setRoute(false);
  }, [centerPending, selectCenterModule]);
  const requestCenterAction = React.useCallback(action => {
    if (centerDirty && !centerConfirm) { setCenterPending(action); setCenterConfirm(true); return; }
    if (action.action === 'lesson') select(action.id);
    else if (action.action === 'switch') selectCenterModule(action.name);
  }, [centerDirty, centerConfirm, selectCenterModule]);
  const openLearningCenter = React.useCallback(() => {
    const context = lab.module && learningModules[lab.module] ? lab.module : null;
    const stored = readLocal('java-lab:learning-module');
    const remembered = stored === 'Redis' || stored === 'JVM' ? stored : 'Redis';
    if (context) {
      if (context !== remembered) notify(`已按当前课程预选「${context} 专题」，记忆保留，可在中心内手动切换`);
      setCenterName(context);
    } else {
      setCenterName(remembered);
    }
    setCenterDirty(false); setCenterConfirm(false); setCenterPending(null); setRoute(true);
  }, [lab.module, notify]);
  useEffect(() => { document.documentElement.dataset.theme = theme; writeLocal('java-lab:theme', theme); }, [theme]);
  useEffect(() => { const change = () => setLabId(getLab(location.hash.slice(1)).id); window.addEventListener('hashchange', change); return () => window.removeEventListener('hashchange', change); }, []);
  useEffect(() => { document.title = `${lab.title} · Java 技术实验室`; }, [lab.title]);
  useEffect(() => { const update = () => setFullscreen(!!document.fullscreenElement); document.addEventListener('fullscreenchange', update); return () => document.removeEventListener('fullscreenchange', update); }, []);
  useEffect(() => () => clearTimeout(toastTimer.current), []);
  function select(id) { setLabId(id); location.hash = id; setMobileOpen(false); setRoute(false); }
  function dismissOnboarding() { setOnboard(false); writeLocal('java-lab:onboarded', true); }
  function toggleBookmark() { const next = bookmarks.includes(labId) ? bookmarks.filter(id => id !== labId) : [...bookmarks, labId]; setBookmarks(next); if (!writeLocal('java-lab:bookmarks', next)) notify('当前浏览器无法保存收藏', 'error'); }
  function handleStageSubmit(stage, answers) { const submission = submitStage(learningState, stage, answers); setLearningState(submission.state); writeLocal('java-lab:learning-center', submission.state); return submission.result; }
  function handleReviewAnswer(question, answer) { const submission = submitReviewAnswer(learningState, question, answer); setLearningState(submission.state); writeLocal('java-lab:learning-center', submission.state); return submission; }
  async function toggleFullscreen() { try { if (document.fullscreenElement) await document.exitFullscreen(); else await document.documentElement.requestFullscreen(); } catch { notify('当前浏览器不支持全屏', 'error'); } }
  const mastered = labs.filter(item => isMastered(item, completed, assessments, challengeRuns));
  const centerModule = learningModules[centerName] || learningModules.Redis;
  const centerPendingLesson = centerPending?.action === 'lesson' ? getLab(centerPending.id) : null;
  const centerPendingModule = centerPending?.action === 'switch' ? learningModules[centerPending.name] || null : null;
  const centerLessons = labs.filter(item => item.module === centerModule.name);
  const currentModuleLessons = lab.module ? labs.filter(item => item.module === lab.module) : [];
  const currentModuleMastered = currentModuleLessons.filter(item => isMastered(item, completed, assessments, challengeRuns));
  const moduleProgress = { total: currentModuleLessons.length, mastered: currentModuleMastered.length, index: Math.max(0, currentModuleLessons.findIndex(item => item.id === labId)), lessons: currentModuleLessons.map(item => isMastered(item, completed, assessments, challengeRuns)) };
  const visible = labs.filter(item => (filter !== 'saved' || bookmarks.includes(item.id)) && `${item.title} ${item.group} ${item.tag} ${(item.goals || []).join(' ')}`.toLowerCase().includes(search.toLowerCase().trim()));
  const plannedVisible = filter === 'all' ? planned.filter(item => `${item.title} ${item.short} ${item.tag} ${item.summary} ${item.scenarios.join(' ')}`.toLowerCase().includes(search.trim().toLowerCase())) : [];
  return <div className={`app ${focus ? 'focus-mode' : ''}`}>
    <header className="topbar"><a className="brand" href="#redis" onClick={() => select('redis')}><span className="brand-mark"><Icon name="Coffee" size={27} /></span><span>Java <strong>技术实验室</strong><small>JAVA INTERACTIVE LAB</small></span></a><nav className="top-nav" aria-label="主导航"><button className={!route ? 'active' : ''} onClick={() => setRoute(false)}><Icon name="FlaskConical" size={16} />实验室</button><button className={route ? 'active' : ''} onClick={openLearningCenter}><Icon name="Route" size={16} />学习中心</button></nav><div className="topbar-actions"><span className="header-divider" /><IconButton icon={theme === 'dark' ? 'Sun' : 'Moon'} label={theme === 'dark' ? '切换浅色主题' : '切换深色主题'} onClick={() => setTheme(theme === 'dark' ? 'light' : 'dark')} /><IconButton icon={focus ? 'PanelLeftOpen' : 'PanelLeftClose'} label={focus ? '退出专注模式' : '专注模式'} onClick={() => setFocus(!focus)} /><IconButton icon={fullscreen ? 'Minimize' : 'Maximize'} label={fullscreen ? '退出全屏' : '全屏'} onClick={toggleFullscreen} /></div></header>
    <div className="app-body">
      {mobileOpen && <button className="sidebar-scrim" aria-label="关闭课程目录" onClick={() => setMobileOpen(false)} />}
      <aside className={`sidebar ${mobileOpen ? 'mobile-open' : ''}`} aria-label="课程目录"><div className="sidebar-heading"><div className="sidebar-title"><Icon name="LibraryBig" size={18} /><h2>课程目录</h2><span>{labs.length}</span></div><p>Java 与中间件 · 交互式原理实验</p></div><div className="sidebar-filter" role="tablist" aria-label="课程筛选"><button role="tab" aria-selected={filter === 'all'} className={filter === 'all' ? 'active' : ''} onClick={() => setFilter('all')}>全部实验</button><button role="tab" aria-selected={filter === 'saved'} className={filter === 'saved' ? 'active' : ''} onClick={() => setFilter('saved')}><Icon name="Bookmark" size={13} />我的收藏{bookmarks.length > 0 && <span>{bookmarks.length}</span>}</button></div><div className="search-box"><Icon name="Search" size={16} /><input type="search" aria-label="搜索实验" placeholder="搜索实验、知识点..." value={search} onChange={e => setSearch(e.target.value)} /></div>
        <div className="course-list">{groups.map((group, i) => { const children = visible.filter(item => item.group === group); const upcoming = plannedVisible.filter(item => item.group === group).sort((a, b) => priorityRank[a.priority] - priorityRank[b.priority]); if (!children.length && !upcoming.length) return null; const allGroupLessons = labs.filter(item => item.group === group); const groupMastered = allGroupLessons.filter(item => isMastered(item, completed, assessments, challengeRuns)); const expanded = !closed.includes(group) || !!search; return <section className="course-group" key={group}><button className="group-heading" aria-expanded={expanded} onClick={() => setClosed(prev => prev.includes(group) ? prev.filter(g => g !== group) : [...prev, group])}><span className="group-number">0{i + 1}</span><span>{group}</span><small>{groupMastered.length}/{allGroupLessons.length}</small><Icon name={expanded ? 'ChevronDown' : 'ChevronRight'} size={14} /></button>{expanded && <div className="group-children">{children.map((item, itemIndex) => { const masteredItem = isMastered(item, completed, assessments, challengeRuns); const runDone = completed.includes(item.id); const missing = !masteredItem && runDone ? ['对照任务', '验证题'].filter(part => part === '对照任务' ? item.challenge && !isChallengeComplete(item.challenge, challengeRuns[item.id]) : item.quiz && !assessments.includes(item.id)) : []; const rowState = masteredItem ? '已掌握' : runDone ? `实验已完成${missing.length ? `，还差 ${missing.join('、')}` : ''}` : ''; const phaseStart = item.phase && (itemIndex === 0 || children[itemIndex - 1].phase !== item.phase); return <React.Fragment key={item.id}>{phaseStart && <div className="phase-label">{item.phase}</div>}<button className={`course-item ${labId === item.id ? 'active' : ''}`} onClick={() => select(item.id)} aria-current={labId === item.id ? 'page' : undefined} title={rowState ? `${item.title} · ${rowState}` : item.title}><Icon name={item.icon} size={17} /><span>{item.short}</span>{masteredItem ? <Icon name="CircleCheck" size={13} className="row-mastered" /> : runDone ? <Icon name="FlaskConical" size={13} className="row-done" /> : labId === item.id ? <span className="active-dot" /> : null}</button></React.Fragment>; })}{upcoming.map(item => <button key={item.id} className="course-item planned" disabled title={`${item.title}：${item.summary}`}><Icon name={item.icon} size={17} /><span>{item.short}</span><em className="planned-badge">{item.priority}</em></button>)}</div>}</section>; })}{plannedVisible.length > 0 && <div className="planned-legend"><em>P0</em> 已排期 · <em>P1</em> 规划中 · <em>P2</em> 远期想法</div>}{!visible.length && !plannedVisible.length && <div className="empty-search"><Icon name={filter === 'saved' ? 'Bookmark' : 'SearchX'} size={26} /><p>{filter === 'saved' && !search ? '还没有收藏的实验' : '没有找到相关实验'}</p><button onClick={() => { setSearch(''); setFilter('all'); }}>查看全部实验</button></div>}</div>
        <div className="sidebar-bottom"><div className="learning-progress"><Icon name="GraduationCap" size={19} /><span>掌握进度<strong>{mastered.length}<small> / {labs.length}</small></strong></span></div><div className="progress-segments" aria-label={`已掌握 ${mastered.length} 个实验`}>{labs.map(item => <i className={isMastered(item, completed, assessments, challengeRuns) ? 'complete' : ''} key={item.id} />)}</div><button className="route-button" onClick={openLearningCenter}>学习中心<Icon name="ArrowUpRight" size={15} /></button><div className="sidebar-footnote"><span title="每课按课程页列出的学习证据记掌握：多数课程需 实验 + 对照任务 + 验证题；部分基础课完成实验即掌握">掌握 = 完成该课列出的学习项</span><Icon name="Sprout" size={17} /></div></div>
      </aside>
      <Experiment key={lab.id} lab={lab} notify={notify} onComplete={markComplete} onChallengeRun={markChallengeRun} experimentDone={completed.includes(lab.id)} challengeHistory={challengeRuns[lab.id] || {}} quizPassed={assessments.includes(labId) ? true : false} onQuizPass={markAssessment} moduleProgress={moduleProgress} bookmarked={bookmarks.includes(labId)} onBookmark={toggleBookmark} onMenu={() => setMobileOpen(true)} />
    </div>
    {toast && <div className={`toast ${toast.kind}`} role={toast.kind === 'error' ? 'alert' : 'status'}><Icon name={toast.kind === 'error' ? 'CircleAlert' : 'Check'} size={16} />{toast.message}</div>}
    {route && <Modal className="learning-dialog" title="学习中心" onClose={requestCloseCenter}><div className="center-module-tabs">{Object.values(learningModules).map(module => <button key={module.name} aria-pressed={centerModule.name === module.name} className={centerModule.name === module.name ? 'active' : ''} onClick={() => requestCenterAction({ action: 'switch', name: module.name })}><span>{module.eyebrow}</span><strong>{module.name} 学习中心</strong>{centerModule.name === module.name && <Icon name="Check" size={13} />}</button>)}</div><LearningCenter key={centerModule.name} moduleConfig={centerModule} lessons={centerLessons} planned={planned.filter(item => item.module === centerModule.name)} completed={completed} assessments={assessments} challengeRuns={challengeRuns} learningState={learningState} onStageSubmit={handleStageSubmit} onReviewAnswer={handleReviewAnswer} onDirtyChange={setCenterDirty} onOpenLesson={id => requestCenterAction({ action: 'lesson', id })} />{centerConfirm && <div className="dialog-confirm" role="alertdialog" aria-label="丢弃当前作答？"><div className="dialog-confirm-card"><div className="dialog-confirm-title"><Icon name="Info" size={17} /><strong>{centerPendingLesson ? '前往该实验将丢弃当前作答' : centerPendingModule ? '切换专题将丢弃当前作答' : '关闭学习中心？'}</strong></div><p>{centerPendingLesson ? `当前有未提交的作答，「${centerPendingLesson.title}」打开后这些内容将丢失。` : centerPendingModule ? `「${centerPendingModule.name} 学习中心」打开后，当前专题未提交的作答将丢失。` : '当前有未提交的测验或复习作答，关闭后将丢失。'}</p><div className="dialog-confirm-actions"><button autoFocus className="quiet-button" onClick={() => { setCenterConfirm(false); setCenterPending(null); }}>继续作答</button><button className="dialog-danger" onClick={confirmLeaveCenter}>放弃并{centerPendingLesson ? '前往' : centerPendingModule ? '切换' : '关闭'}</button></div></div></div>}</Modal>}
    {onboard && <Modal className="onboard-dialog" title="欢迎 · 从一条链路理解 Java 后端" onClose={dismissOnboarding}><div className="onboard">
      <p className="onboard-intro">每个实验都是一条可运行的链路：观看架构动画、调整参数观察行为变化、对照伪代码，最后用调参任务与验证题检验理解。</p>
      <ol className="onboard-steps">
        <li><strong>选一门课</strong>左侧目录按专题分组；首次建议从「Redis 专题 · 01 缓存读写」开始，跟随一次缓存请求的完整旅程。</li>
        <li><strong>驱动演示</strong>播放 / 步进 / 拖动进度条推进步骤；右侧「实时读数」与「运行日志」交叉验证。</li>
        <li><strong>攒下证据</strong>完整播放到最后一步记为实验完成；对照任务与验证题完成后课程记为「掌握」，进度进入学习中心总评。</li>
        <li><strong>快捷键</strong>空格 播放 / 暂停，← → 步进，R 重置；输入框内与真实实验视图中自动失效。</li>
      </ol>
      <div className="onboard-actions"><button className="quiet-button" onClick={dismissOnboarding}>跳过</button><button className="verify-button" autoFocus onClick={dismissOnboarding}>开始实验</button></div>
    </div></Modal>}
  </div>;
}
createRoot(document.getElementById('root')).render(<React.StrictMode><App /></React.StrictMode>);
