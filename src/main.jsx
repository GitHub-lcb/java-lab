import React, { useState, useEffect, useMemo, useRef } from 'react';
import { createRoot } from 'react-dom/client';
import { createMachine, assign } from 'xstate';
import { useMachine } from '@xstate/react';
import { ReactFlow, Background, Controls, Handle, Position, MarkerType } from '@xyflow/react';
import { Activity, Archive, ArrowRight, ArrowUpRight, Binary, Bookmark, BookmarkCheck, BookOpen, Box, Boxes, Braces, Check, ChevronDown, ChevronRight, CircleCheck, Code2, CodeXml, Coffee, Copy, CornerDownLeft, Cpu, Database, DatabaseZap, Expand, FileCheck2, FlaskConical, GraduationCap, Info, Layers, LibraryBig, ListOrdered, LocateFixed, Maximize, MemoryStick, Minimize, Monitor, Moon, Network, NotebookPen, PanelLeft, PanelLeftClose, PanelLeftOpen, Pause, Play, Recycle, RefreshCw, RotateCcw, Route, Save, Search, SearchX, Send, Server, ShieldAlert, SkipBack, SlidersHorizontal, Sprout, StepForward, Sun, Table2, Terminal, Waypoints, Workflow, X } from 'lucide-react';
import { labs, groups, getLab } from './catalog';
import { simulate } from './models';
import { normalizeParams, playbackView, readLocal, writeLocal } from './settings';
import { getDiagramPositions } from './diagramLayout.js';
import { gradeQuiz, isChallengeComplete, isMastered, recordChallengeRun } from './curriculum';
import { submitReviewAnswer, submitStage } from './learning';
import LearningCenter from './LearningCenter.jsx';
import { getLearningModule } from './assessmentCatalog.js';
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

const Icons = { Activity, Archive, ArrowRight, ArrowUpRight, Binary, Bookmark, BookmarkCheck, BookOpen, Box, Boxes, Braces, Check, ChevronDown, ChevronRight, CircleCheck, Code2, CodeXml, Coffee, Copy, CornerDownLeft, Cpu, Database, DatabaseZap, Expand, FileCheck2, FlaskConical, GraduationCap, Info, Layers, LibraryBig, ListOrdered, LocateFixed, Maximize, MemoryStick, Minimize, Monitor, Moon, Network, NotebookPen, PanelLeft, PanelLeftClose, PanelLeftOpen, Pause, Play, Recycle, RefreshCw, RotateCcw, Route, Save, Search, SearchX, Send, Server, ShieldAlert, SkipBack, SlidersHorizontal, Sprout, StepForward, Sun, Table2, Terminal, Waypoints, Workflow, X };
function Icon({ name, size = 18, ...props }) {
  const Component = Icons[name] || Icons.Box;
  return <Component size={size} strokeWidth={1.65} aria-hidden="true" {...props} />;
}
function IconButton({ icon, label, ...props }) {
  return <button type="button" className="icon-button" title={label} aria-label={label} {...props}><Icon name={icon} /></button>;
}
const player = createMachine({
  id: 'player', initial: 'paused', context: { index: 0, length: 1, frames: null },
  on: {
    LOAD: { target: '.paused', actions: assign({ index: 0, length: ({ event }) => event.frames.length, frames: ({ event }) => event.frames }) },
    RESET: { target: '.paused', actions: assign({ index: 0 }) },
    SEEK: { target: '.paused', actions: assign({ index: ({ event, context }) => Math.max(0, Math.min(context.length - 1, event.index)) }) },
    PREV: { target: '.paused', actions: assign({ index: ({ context }) => Math.max(0, context.index - 1) }) },
    NEXT: [
      { guard: ({ context }) => context.index >= context.length - 2, target: '.paused', actions: assign({ index: ({ context }) => context.length - 1 }) },
      { actions: assign({ index: ({ context }) => context.index + 1 }) },
    ],
  },
  states: {
    paused: { on: { PLAY: { target: 'playing', actions: assign({ index: ({ context }) => context.index === context.length - 1 ? 0 : context.index }) } } },
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
  if (lab.id === 'jvm') return <div className="data-strip"><span className="strip-label">HEAP</span><div className="data-items"><span className="data-token green">Eden · {items.eden.length}</span><span className="data-token pink">Old · {items.old.length}</span>{items.eden.map(item => <span key={item} className="heap-object" title={item} />)}</div></div>;
  if (lab.group === 'Redis 专题') return <div className="data-strip"><span className="strip-label"><Icon name="DatabaseZap" size={15} />{frame.dataLabel || 'REDIS STATE'}</span><div className="data-items">{items.length ? items.map((item, i) => <span className="data-token green" key={`${item}-${i}`}>{item}</span>) : <span className="empty-inline">暂无数据</span>}</div><span className="mono faint">{items.length} items</span></div>;
  if (lab.group === 'JVM 专题') return <div className="data-strip"><span className="strip-label"><Icon name="MemoryStick" size={15} />{frame.dataLabel || 'JVM STATE'}</span><div className="data-items">{items.length ? items.map((item, i) => <span className="data-token green" key={`${item}-${i}`}>{item}</span>) : <span className="empty-inline">暂无运行时状态</span>}</div><span className="mono faint">{items.length} items</span></div>;
  return <div className="data-strip"><span className="strip-label">LEAF PAGE</span><div className="data-items">{items.length ? items.map(item => <span key={item} className={`data-token ${item === frame.metrics.target ? 'green' : ''}`}>{item}</span>) : <span className="empty-inline">{frame.metrics.found ? `id=${frame.metrics.target} 已返回` : '等待查询'}</span>}</div></div>;
}
function TextParameter({ id, value, fallback, onCommit }) {
  const [draft, setDraft] = useState(value);
  useEffect(() => setDraft(value), [value]);
  function commit() { const normalized = draft.trim().slice(0, 32) || fallback; setDraft(normalized); onCommit(normalized); }
  return <input id={id} type="text" maxLength={32} value={draft} onChange={e => setDraft(e.target.value)} onBlur={commit} onKeyDown={e => { if (e.key === 'Enter') e.currentTarget.blur(); }} />;
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
  return <dialog className={className} ref={ref} onCancel={onClose} onClick={e => { if (e.target === ref.current) onClose(); }}><div className="dialog-heading"><h2>{title}</h2><IconButton icon="X" label="关闭" onClick={onClose} /></div>{children}</dialog>;
}
function KnowledgeCheck({ lab, passed, experimentDone, challengeDone, onPass }) {
  const [answers, setAnswers] = useState({});
  const [result, setResult] = useState(null);
  function choose(question, choice) { setAnswers(prev => ({ ...prev, [question]: choice })); setResult(null); }
  function submit() {
    const next = gradeQuiz(lab.quiz, answers);
    setResult(next);
    if (next.passed) onPass(lab.id);
  }
  return <section className="knowledge-check" aria-labelledby="knowledge-check-title">
    <div className="section-title"><h2 id="knowledge-check-title"><Icon name="CircleCheck" size={17} />原理验证</h2><span>{passed ? 'CHECK PASSED' : '2 QUESTIONS'}</span></div>
    <div className="quiz-grid">{lab.quiz.map((question, questionIndex) => <fieldset key={question.prompt} className={result ? result.details[questionIndex].correct ? 'correct' : 'incorrect' : ''}>
      <legend><span>{String(questionIndex + 1).padStart(2, '0')}</span>{question.prompt}</legend>
      <div className="quiz-options">{question.choices.map((choice, choiceIndex) => <label key={choice}><input type="radio" name={`${lab.id}-question-${questionIndex}`} checked={answers[questionIndex] === choiceIndex} onChange={() => choose(questionIndex, choiceIndex)} /><span>{choice}</span></label>)}</div>
      {result && <p className="quiz-explanation"><Icon name={result.details[questionIndex].correct ? 'Check' : 'Info'} size={14} />{question.explanation}</p>}
    </fieldset>)}</div>
    <div className="quiz-actions"><button className="verify-button" disabled={Object.keys(answers).length !== lab.quiz.length} onClick={submit}><Icon name="Check" size={15} />提交验证</button><span aria-live="polite">{result?.passed ? experimentDone && challengeDone ? '实验、对照任务与验证均已完成，本课已掌握。' : !experimentDone ? '验证通过，还需完整运行实验。' : '验证通过，还需完成全部对照场景。' : result ? `${result.score}/${lab.quiz.length} 正确，请根据解析重新判断。` : passed ? experimentDone && challengeDone ? '本课已掌握，可重新作答。' : '验证题已通过，继续完成实验和对照任务。' : '全部答对后记录验证结果。'}</span></div>
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
    try { await navigator.clipboard.writeText(lab.code.join('\n')); setCopied(true); setTimeout(() => setCopied(false), 1800); } catch { notify('复制失败，请在代码视图中选择文本复制'); }
  }
  async function toggleWorkbench() {
    try { await toggleElementFullscreen(workbenchRef.current, document); }
    catch { notify('当前浏览器不支持实验工作区全屏'); }
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
              : tab === 'code' ? <div className="code-view"><div className="code-top"><span>{lab.id === 'mysql' ? 'query.sql' : 'TeachingModel.java'}<small>教学伪代码</small></span><IconButton icon={copied ? 'Check' : 'Copy'} label={copied ? '已复制' : '复制代码'} onClick={copy} /></div><pre>{lab.code.map((line, i) => <div key={i} className={i === frame.code ? 'highlight-line' : ''}><span className="line-number">{i + 1}</span><code>{line}</code></div>)}</pre></div>
              : <div className="log-view">{frames.slice(0, index + 1).map((entry, i) => <div className="log-line" key={i}><span className="log-index">{String(i).padStart(3, '0')}</span><span className="log-level">{entry.message.includes('MISS') ? 'MISS' : 'INFO'}</span><span>{entry.message}</span></div>)}<div ref={logEnd} /></div>}
          </div>
          {!isRealView && <><DataView lab={lab} frame={frame} /><div className="player"><div className="player-buttons"><button className="play-button" onClick={() => send({ type: playing ? 'PAUSE' : 'PLAY' })} aria-label={playing ? '暂停演示' : index === frames.length - 1 ? '重新演示' : '开始演示'}><Icon name={playing ? 'Pause' : 'Play'} size={16} /><span>{playing ? '暂停' : index === frames.length - 1 ? '重新演示' : '开始演示'}</span></button><IconButton icon="SkipBack" label="上一步" disabled={index === 0} onClick={() => send({ type: 'PREV' })} /><IconButton icon="StepForward" label="下一步" disabled={index === frames.length - 1} onClick={() => send({ type: 'NEXT' })} /><IconButton icon="RotateCcw" label="重置实验" onClick={() => send({ type: 'RESET' })} /></div><div className="timeline"><input type="range" aria-label="演示进度" min="0" max={frames.length - 1} value={index} onChange={e => send({ type: 'SEEK', index: Number(e.target.value) })} /><span className="mono">{String(index).padStart(2, '0')} / {frames.length - 1}</span></div><select className="speed" aria-label="播放速度" value={speed} onChange={e => setSpeed(Number(e.target.value))}><option value="0.5">0.5×</option><option value="1">1×</option><option value="2">2×</option></select></div></>}
        </section>
        {!isRealView && <div className="step-status" aria-live="polite"><span className="step-number">{String(index).padStart(2, '0')}</span><span>{frame.message}</span><Icon name="CornerDownLeft" size={15} /></div>}
        <section className="principles"><div className="section-title"><h2><Icon name="NotebookPen" size={17} />原理与关键概念</h2><span>UNDER THE HOOD</span></div><div className="principle-grid">{lab.theory.map(([title, formula, text], i) => <article key={title}><span className="concept-index">0{i + 1}</span><h3>{title}</h3><strong>{formula}</strong><p>{text}</p></article>)}</div></section>
        <LessonDeepDive lessonId={lab.id} />
        {lab.goals && <section className="learning-objectives"><div className="objective-column"><div className="section-title"><h2><Icon name="GraduationCap" size={17} />本课目标</h2><span>LEARNING GOALS</span></div><ul>{lab.goals.map(goal => <li key={goal}><Icon name="Check" size={14} />{goal}</li>)}</ul></div><ChallengePanel lab={lab} history={challengeHistory} /></section>}
        {lab.quiz && <KnowledgeCheck lab={lab} passed={quizPassed} experimentDone={experimentDone} challengeDone={isChallengeComplete(lab.challenge, challengeHistory)} onPass={onQuizPass} />}
      </div>
      {tab === 'real' ? <RealAside lab={lab} progress={realProgress} /> : tab === 'jvm-real' ? <JvmProbeAside lesson={lab} /> : <Params lab={lab} params={params} frame={frame} onChange={change} onSave={() => notify(writeLocal(`java-lab:${lab.id}`, params) ? '当前参数已保存到本机' : '当前浏览器无法保存参数')} onReset={() => { setParams({ ...lab.defaults }); notify('已恢复默认参数'); }} />}
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
  const [theme, setTheme] = useState(() => readLocal('java-lab:theme', 'dark') === 'light' ? 'light' : 'dark');
  const [toast, setToast] = useState('');
  const [mobileOpen, setMobileOpen] = useState(false);
  const [route, setRoute] = useState(false);
  const [focus, setFocus] = useState(false);
  const [fullscreen, setFullscreen] = useState(false);
  const toastTimer = useRef(null);
  const notify = React.useCallback(message => { setToast(message); clearTimeout(toastTimer.current); toastTimer.current = setTimeout(() => setToast(''), 2800); }, []);
  const markComplete = React.useCallback(id => setCompleted(prev => { if (prev.includes(id)) return prev; const next = [...prev, id]; writeLocal('java-lab:completed', next); return next; }), []);
  const markAssessment = React.useCallback(id => setAssessments(prev => { if (prev.includes(id)) return prev; const next = [...prev, id]; writeLocal('java-lab:assessments', next); return next; }), []);
  const markChallengeRun = React.useCallback((lesson, params, metrics) => setChallengeRuns(prev => { const history = prev[lesson.id] || {}; const nextHistory = recordChallengeRun(lesson.challenge, history, params, metrics); if (nextHistory === history) return prev; const next = { ...prev, [lesson.id]: nextHistory }; writeLocal('java-lab:challenge-runs', next); return next; }), []);
  useEffect(() => { document.documentElement.dataset.theme = theme; writeLocal('java-lab:theme', theme); }, [theme]);
  useEffect(() => { const change = () => setLabId(getLab(location.hash.slice(1)).id); window.addEventListener('hashchange', change); return () => window.removeEventListener('hashchange', change); }, []);
  useEffect(() => { document.title = `${lab.title} · Java 技术实验室`; }, [lab.title]);
  useEffect(() => { const update = () => setFullscreen(!!document.fullscreenElement); document.addEventListener('fullscreenchange', update); return () => document.removeEventListener('fullscreenchange', update); }, []);
  useEffect(() => () => clearTimeout(toastTimer.current), []);
  function select(id) { setLabId(id); location.hash = id; setMobileOpen(false); setRoute(false); }
  function toggleBookmark() { const next = bookmarks.includes(labId) ? bookmarks.filter(id => id !== labId) : [...bookmarks, labId]; setBookmarks(next); if (!writeLocal('java-lab:bookmarks', next)) notify('当前浏览器无法保存收藏'); }
  function handleStageSubmit(stage, answers) { const submission = submitStage(learningState, stage, answers); setLearningState(submission.state); writeLocal('java-lab:learning-center', submission.state); return submission.result; }
  function handleReviewAnswer(question, answer) { const submission = submitReviewAnswer(learningState, question, answer); setLearningState(submission.state); writeLocal('java-lab:learning-center', submission.state); return submission; }
  async function toggleFullscreen() { try { if (document.fullscreenElement) await document.exitFullscreen(); else await document.documentElement.requestFullscreen(); } catch { notify('当前浏览器不支持全屏'); } }
  const mastered = labs.filter(item => isMastered(item, completed, assessments, challengeRuns));
  const learningModule = getLearningModule(lab.module);
  const learningLessons = labs.filter(item => item.module === learningModule.name);
  const currentModuleLessons = lab.module ? labs.filter(item => item.module === lab.module) : [];
  const currentModuleMastered = currentModuleLessons.filter(item => isMastered(item, completed, assessments, challengeRuns));
  const moduleProgress = { total: currentModuleLessons.length, mastered: currentModuleMastered.length, index: Math.max(0, currentModuleLessons.findIndex(item => item.id === labId)), lessons: currentModuleLessons.map(item => isMastered(item, completed, assessments, challengeRuns)) };
  const visible = labs.filter(item => (filter !== 'saved' || bookmarks.includes(item.id)) && `${item.title} ${item.group} ${item.tag} ${(item.goals || []).join(' ')}`.toLowerCase().includes(search.toLowerCase().trim()));
  return <div className={`app ${focus ? 'focus-mode' : ''}`}>
    <header className="topbar"><a className="brand" href="#redis" onClick={() => select('redis')}><span className="brand-mark"><Icon name="Coffee" size={27} /></span><span>Java <strong>技术实验室</strong><small>JAVA INTERACTIVE LAB</small></span></a><nav className="top-nav" aria-label="主导航"><button className={!route ? 'active' : ''} onClick={() => setRoute(false)}><Icon name="FlaskConical" size={16} />实验室</button><button className={route ? 'active' : ''} onClick={() => setRoute(true)}><Icon name="Route" size={16} />学习中心</button></nav><div className="topbar-actions"><span className="header-divider" /><IconButton icon={theme === 'dark' ? 'Sun' : 'Moon'} label={theme === 'dark' ? '切换浅色主题' : '切换深色主题'} onClick={() => setTheme(theme === 'dark' ? 'light' : 'dark')} /><IconButton icon={focus ? 'PanelLeftOpen' : 'PanelLeftClose'} label={focus ? '退出专注模式' : '专注模式'} onClick={() => setFocus(!focus)} /><IconButton icon={fullscreen ? 'Minimize' : 'Maximize'} label={fullscreen ? '退出全屏' : '全屏'} onClick={toggleFullscreen} /></div></header>
    <div className="app-body">
      {mobileOpen && <button className="sidebar-scrim" aria-label="关闭课程目录" onClick={() => setMobileOpen(false)} />}
      <aside className={`sidebar ${mobileOpen ? 'mobile-open' : ''}`} aria-label="课程目录"><div className="sidebar-heading"><div className="sidebar-title"><Icon name="LibraryBig" size={18} /><h2>课程目录</h2><span>{labs.length}</span></div><p>Java 与中间件 · 交互式原理实验</p></div><div className="sidebar-filter" role="tablist" aria-label="课程筛选"><button role="tab" aria-selected={filter === 'all'} className={filter === 'all' ? 'active' : ''} onClick={() => setFilter('all')}>全部实验</button><button role="tab" aria-selected={filter === 'saved'} className={filter === 'saved' ? 'active' : ''} onClick={() => setFilter('saved')}><Icon name="Bookmark" size={13} />我的收藏{bookmarks.length > 0 && <span>{bookmarks.length}</span>}</button></div><div className="search-box"><Icon name="Search" size={16} /><input type="search" aria-label="搜索实验" placeholder="搜索实验、知识点..." value={search} onChange={e => setSearch(e.target.value)} /></div>
        <div className="course-list">{groups.map((group, i) => { const children = visible.filter(item => item.group === group); if (!children.length) return null; const allGroupLessons = labs.filter(item => item.group === group); const groupMastered = allGroupLessons.filter(item => isMastered(item, completed, assessments, challengeRuns)); const expanded = !closed.includes(group) || !!search; return <section className="course-group" key={group}><button className="group-heading" aria-expanded={expanded} onClick={() => setClosed(prev => prev.includes(group) ? prev.filter(g => g !== group) : [...prev, group])}><span className="group-number">0{i + 1}</span><span>{group}</span>{allGroupLessons.some(item => item.module) && <small>{groupMastered.length}/{allGroupLessons.length}</small>}<Icon name={expanded ? 'ChevronDown' : 'ChevronRight'} size={14} /></button>{expanded && <div className="group-children">{children.map((item, itemIndex) => { const masteredItem = isMastered(item, completed, assessments, challengeRuns); const phaseStart = item.phase && (itemIndex === 0 || children[itemIndex - 1].phase !== item.phase); return <React.Fragment key={item.id}>{phaseStart && <div className="phase-label">{item.phase}</div>}<button className={`course-item ${labId === item.id ? 'active' : ''}`} onClick={() => select(item.id)} aria-current={labId === item.id ? 'page' : undefined} title={item.phase || item.title}><Icon name={item.icon} size={17} /><span>{item.short}</span>{masteredItem ? <Icon name="CircleCheck" size={13} /> : completed.includes(item.id) ? <Icon name="FlaskConical" size={13} /> : labId === item.id ? <span className="active-dot" /> : null}</button></React.Fragment>; })}</div>}</section>; })}{!visible.length && <div className="empty-search"><Icon name={filter === 'saved' ? 'Bookmark' : 'SearchX'} size={26} /><p>{filter === 'saved' && !search ? '还没有收藏的实验' : '没有找到相关实验'}</p><button onClick={() => { setSearch(''); setFilter('all'); }}>查看全部实验</button></div>}</div>
        <div className="sidebar-bottom"><div className="learning-progress"><Icon name="GraduationCap" size={19} /><span>掌握进度<strong>{mastered.length}<small> / {labs.length}</small></strong></span></div><div className="progress-segments" aria-label={`已掌握 ${mastered.length} 个实验`}>{labs.map(item => <i className={isMastered(item, completed, assessments, challengeRuns) ? 'complete' : ''} key={item.id} />)}</div><button className="route-button" onClick={() => setRoute(true)}>{learningModule.name} 学习中心<Icon name="ArrowUpRight" size={15} /></button><div className="sidebar-footnote"><span>实验 + 对照任务 + 验证题 = 掌握</span><Icon name="Sprout" size={17} /></div></div>
      </aside>
      <Experiment key={lab.id} lab={lab} notify={notify} onComplete={markComplete} onChallengeRun={markChallengeRun} experimentDone={completed.includes(lab.id)} challengeHistory={challengeRuns[lab.id] || {}} quizPassed={assessments.includes(labId) ? true : false} onQuizPass={markAssessment} moduleProgress={moduleProgress} bookmarked={bookmarks.includes(labId)} onBookmark={toggleBookmark} onMenu={() => setMobileOpen(true)} />
    </div>
    {toast && <div className="toast" role="status"><Icon name="Info" size={16} />{toast}</div>}
    {route && <Modal className="learning-dialog" title={`${learningModule.name} 学习中心`} onClose={() => setRoute(false)}><LearningCenter moduleConfig={learningModule} lessons={learningLessons} completed={completed} assessments={assessments} challengeRuns={challengeRuns} learningState={learningState} onStageSubmit={handleStageSubmit} onReviewAnswer={handleReviewAnswer} onOpenLesson={select} /></Modal>}
  </div>;
}
createRoot(document.getElementById('root')).render(<React.StrictMode><App /></React.StrictMode>);
