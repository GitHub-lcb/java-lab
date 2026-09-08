import React, { useEffect, useState } from 'react';
import { Activity, CircleAlert, Cpu, Info, RotateCcw } from 'lucide-react';
import { fetchJvmProbe } from './jvmRuntime.js';
import { jvmProbeCatalog } from './jvmProbeCatalog.js';

const API_BASE = import.meta.env.VITE_REDIS_LAB_API || 'http://127.0.0.1:8787';
const bytes = value => value < 0 ? '不可用' : value >= 1048576 ? `${(value / 1048576).toFixed(2)} MiB` : value >= 1024 ? `${(value / 1024).toFixed(2)} KiB` : `${value} B`;
const labels = { javaVersion: 'Java 版本', vmName: '虚拟机', vmVendor: '供应商', processors: '处理器', maxMemory: '最大堆', heapUsed: '堆已使用', heapCommitted: '堆已提交', heapMax: '堆上限', nonHeapUsed: '非堆已使用', sampleClass: '样例类', sampleLoader: '定义加载器', parentLoader: '父加载器', bootstrapClass: '核心类', bootstrapLoader: '核心类加载器', objectCount: '对象数量', payloadBytes: '有效载荷', allocatedBytes: '线程分配字节', threadAllocationSupported: '分配统计支持', weakReferenceCleared: '弱引用已清除', collected: '已观察到回收', note: '观测说明', uptimeMs: 'JVM 运行时间' };
const byteKeys = new Set(['maxMemory', 'heapUsed', 'heapCommitted', 'heapMax', 'nonHeapUsed', 'payloadBytes', 'allocatedBytes', 'used']);
function Scalar({ name, value }) { return <div className="probe-value"><span>{labels[name] || name}</span><strong>{typeof value === 'boolean' ? value ? '是' : '否' : byteKeys.has(name) && typeof value === 'number' ? bytes(value) : String(value)}</strong></div>; }
function ProbeData({ data }) {
  if (data.output) return <pre className="javap-output">{data.output}</pre>;
  const simple = Object.entries(data).filter(([, value]) => !Array.isArray(value));
  const arrays = Object.entries(data).filter(([, value]) => Array.isArray(value));
  return <><div className="probe-grid">{simple.map(([name, value]) => <Scalar key={name} name={name} value={value} />)}</div>{arrays.map(([name, values]) => <div className="probe-list" key={name}><h4>{name === 'pools' ? '内存池' : name === 'collectors' ? '垃圾收集器' : name}</h4>{values.map((item, index) => <div key={index}>{Object.entries(item).map(([key, value]) => <Scalar key={key} name={key} value={value} />)}</div>)}</div>)}</>;
}
export function JvmProbeAside({ lesson }) {
  const config = jvmProbeCatalog[lesson.id];
  return <aside className="real-aside" aria-label="JDK 实机观测说明"><div className="panel-title"><h2><Cpu size={16} />JDK 实机</h2><span className="real-badge"><i />白名单</span></div><div className="real-aside-title">{config.objective}</div><div className="probe-evidence">{config.evidence.map(item => <p key={item}><Activity size={13} />{item}</p>)}</div><div className="real-aside-note"><Info size={15} /><p>{config.boundary}</p></div><div className="real-aside-note"><CircleAlert size={15} /><p>探针固定在项目代码中，不接受任意 Java 源码、类名、启动参数或系统命令。</p></div></aside>;
}
export default function RealJvmLab({ lesson }) {
  const config = jvmProbeCatalog[lesson.id];
  const [state, setState] = useState({ loading: true, data: null, error: '' });
  async function run() {
    setState({ loading: true, data: null, error: '' });
    try {
      const result = await fetchJvmProbe((url, options) => fetch(url, { ...options, signal: AbortSignal.timeout(8000) }), API_BASE, config.probe);
      setState({ loading: false, data: result.data, error: '' });
    } catch (error) { setState({ loading: false, data: null, error: error.message }); }
  }
  useEffect(() => { run(); }, [lesson.id]);
  return <section className="jvm-probe" aria-label="真实 JDK 观测"><div className="probe-toolbar"><span><Cpu size={16} />{config.title}</span><button onClick={run} disabled={state.loading}><RotateCcw size={14} />重新采样</button></div><div className="probe-content" aria-live="polite">{state.loading ? <div className="probe-empty"><Activity size={25} /><p>正在读取本机 Java 网关的真实 JVM 数据</p></div> : state.error ? <div className="probe-empty error"><CircleAlert size={25} /><p>{state.error}</p></div> : <ProbeData data={state.data} />}</div></section>;
}
