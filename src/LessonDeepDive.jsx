import React from 'react';
import { ArrowUpRight, BookOpen } from 'lucide-react';
import { jvmDeepDives } from './jvmDeepDives.js';
import { redisDeepDives } from './redisDeepDives.js';

const deepDives = { ...redisDeepDives, ...jvmDeepDives };

export default function LessonDeepDive({ lessonId }) {
  const content = deepDives[lessonId];
  if (!content) return null;
  return <section className="deep-dive" aria-label="原理精讲">
    <div className="section-title"><h2><BookOpen size={17} />原理精讲</h2><span>WHY IT WORKS</span></div>
    <h3>{content.question}</h3><p className="deep-scenario">{content.scenario}</p>
    <div className="deep-chain">{content.chain.map(([title, body], index) => <article key={title}><span>{String(index + 1).padStart(2, '0')}</span><div><h4>{title}</h4><p>{body}</p></div></article>)}</div>
    <div className="deep-table"><table><thead><tr>{content.headers.map(header => <th key={header}>{header}</th>)}</tr></thead><tbody>{content.rows.map(row => <tr key={row[0]}>{row.map((cell, index) => <td key={index}>{cell}</td>)}</tr>)}</tbody></table></div>
    <p className="deep-misconception"><strong>常见误区</strong>{content.misconception}</p>
    <div className="deep-transfer"><h4>迁移到业务场景</h4><p>{content.transfer}</p><details><summary>展开参考分析</summary><p>{content.answer}</p></details></div>
    <div className="deep-references">{content.links.map(([label, url]) => <a key={url} href={url} target="_blank" rel="noreferrer">{label}<ArrowUpRight size={13} /></a>)}</div>
  </section>;
}
