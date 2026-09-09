import React, { useEffect, useMemo, useRef, useState } from 'react';
import { Check, DatabaseZap, Info, Play, RotateCcw, Terminal } from 'lucide-react';
import { createSessionId, executeCommand, runtimeStatus } from './redisRuntime.js';
import { describeGatewayError } from './gatewayErrors.js';
import { advanceLesson, createRun, lessonSession } from './realLesson.js';
import { realLabCatalog } from './realLabCatalog.js';

const API_BASE = import.meta.env.VITE_REDIS_LAB_API || 'http://127.0.0.1:8787';
function outputValue(value) {
  if (value === null) return '(nil)';
  if (value === '') return '"" (空字符串)';
  if (Array.isArray(value)) return value.length ? value.map((item, index) => `${index + 1}) ${outputValue(item)}`).join('\n') : '(empty array)';
  return typeof value === 'number' ? `(integer) ${value}` : String(value);
}

export default function RealRedisLab({ lesson, onComplete, onProgress, active }) {
  const guide = realLabCatalog[lesson.id];
  const session = useMemo(() => lessonSession(createSessionId(), lesson.id), [lesson.id]);
  const [status, setStatus] = useState({ java: false, redis: false, loading: true, message: '正在检测连接' });
  const [run, setRun] = useState(createRun);
  const [command, setCommand] = useState(guide.steps[0].command);
  const [history, setHistory] = useState([]);
  const [feedback, setFeedback] = useState(null);
  const [busy, setBusy] = useState(false);
  const busyRef = useRef(false);
  const logRef = useRef(null);
  const stage = guide.steps[run.index];
  const refresh = async () => {
    setStatus(previous => ({ ...previous, loading: true }));
    const state = await runtimeStatus((url, options) => fetch(url, { ...options, signal: AbortSignal.timeout(5000) }), API_BASE);
    setStatus({ ...state, loading: false });
  };
  useEffect(() => { if (active) refresh(); }, [active]);
  useEffect(() => { onProgress({ ...run, status }); }, [run, status, onProgress]);
  useEffect(() => { if (active && logRef.current) logRef.current.scrollTop = logRef.current.scrollHeight; }, [history, active]);

  async function execute(event) {
    event.preventDefault();
    if (busyRef.current || !status.redis || !command.trim()) return;
    busyRef.current = true;
    setBusy(true);
    const current = command.trim();
    try {
      const response = await executeCommand((url, options) => fetch(url, { ...options, signal: AbortSignal.timeout(5000) }), API_BASE, session, current);
      const entry = { command: current, result: response.result, ok: true };
      const next = advanceLesson(guide, run, entry);
      const verified = next.index > run.index;
      setHistory(previous => [...previous.slice(-99), { ...entry, output: outputValue(entry.result), verified }]);
      setRun(next);
      if (verified) {
        setFeedback({ passed: true, text: stage.why });
        if (next.complete) onComplete(lesson.id);
        else setCommand(guide.steps[next.index].command);
      } else {
        const text = !stage ? '自由命令已执行，全部引导步骤已完成。' : current !== stage.command ? '自由命令已执行；当前引导步骤尚未验证。' : `返回值不符合预期（${stage.expectText}）。${stage.retry}`;
        setFeedback({ passed: false, text });
      }
    } catch (error) {
      const human = describeGatewayError(error);
      setHistory(previous => [...previous.slice(-99), { command: current, output: human.title, detail: human.detail, ok: false }]);
      setFeedback({ passed: false, text: human.hint ? `${human.title}。${human.hint}` : human.title });
    } finally { setBusy(false); busyRef.current = false; }
  }
  function restart() {
    setRun(createRun()); setCommand(guide.steps[0].command); setHistory([]); setFeedback(null);
    setConfirmingRestart(false);
  }
  const [confirmingRestart, setConfirmingRestart] = useState(false);
  const confirmTimer = useRef(null);
  useEffect(() => () => clearTimeout(confirmTimer.current), []);
  function requestRestart() {
    if (busyRef.current) return;
    if (!confirmingRestart) {
      setConfirmingRestart(true);
      confirmTimer.current = setTimeout(() => setConfirmingRestart(false), 4000);
      return;
    }
    clearTimeout(confirmTimer.current);
    restart();
  }
  return <section className="guided-runtime" aria-label="真实 Redis 引导实验">
    <div className="guided-toolbar"><span><Terminal size={15} />{run.index}/{guide.steps.length} 步已验证</span><div><span className={status.redis ? 'online' : 'offline'}>{status.loading ? '检测中' : status.redis ? 'Redis 已连接' : status.message}</span><button title="重新检测" aria-label="重新检测" onClick={refresh} disabled={status.loading}><RotateCcw size={14} /></button><button className={confirmingRestart ? 'confirm-restart' : undefined} title={confirmingRestart ? '再次点击将清空本次执行记录并回到第一步' : '清空本次执行记录，回到第一步'} aria-label={confirmingRestart ? '再次点击以确认清空执行记录并重新开始' : '重新开始'} onClick={requestRestart} disabled={busy}><RotateCcw size={14} />{confirmingRestart ? '确认重新开始？' : '重新开始'}</button></div></div>
    {!status.loading && !status.redis && <div className="guided-offline" role="status"><Info size={14} /><p>{status.java ? <>Redis 不可达：先运行 <code>docker compose -f server/docker-compose.yml up -d</code> 启动容器版 Redis，或本机启动 <code>redis-server</code>（默认 127.0.0.1:6379），再点重新检测。</> : <>需要先启动本机 Java 网关：在项目根目录执行 <code>npm run backend</code>（首次先 <code>npm ci</code>；需 JDK 8+ 的 <code>javac</code>）。</>}</p></div>}
    <div className="guided-current">
      {stage ? <><div><strong>步骤 {run.index + 1} · {stage.title}</strong><code>{stage.command}</code></div><p><span>预期返回</span>{stage.expectText}</p></> : <div><strong><Check size={17} />本轮 {guide.steps.length} 步均已通过返回值验证</strong><p>{guide.scope}</p></div>}
    </div>
    <div className="guided-log terminal-history" ref={logRef} aria-label="真实执行记录">{history.length ? history.map((entry, index) => <div className={entry.ok ? '' : 'error'} key={index}><span className="terminal-prompt">redis&gt;</span><code>{entry.command}</code><pre>{entry.output}</pre>{entry.detail ? <details className="terminal-error-detail"><summary>原始错误信息</summary><code>{entry.detail}</code></details> : null}<small>{entry.verified ? '步骤验证通过' : entry.ok ? '命令已执行' : '执行失败'}</small></div>) : <div className="terminal-empty"><DatabaseZap size={26} /><p>{guide.objective}</p></div>}</div>
    {feedback && <p className={`guided-feedback ${feedback.passed ? 'passed' : ''}`} role="status"><Info size={15} />{feedback.text}</p>}
    <form className="terminal-command" onSubmit={execute}><span>&gt;</span><input aria-label="Redis 命令" value={command} onChange={event => setCommand(event.target.value)} maxLength={512} spellCheck="false" autoComplete="off" /><button type="submit" disabled={busy || !status.redis || !command.trim()}><Play size={15} />{busy ? '执行中' : '执行'}</button></form>
  </section>;
}
