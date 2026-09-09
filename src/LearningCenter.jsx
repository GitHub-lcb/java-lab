import React, { useEffect, useRef, useState } from 'react';
import { Activity, ArrowRight, Check, CircleCheck, Info, NotebookPen, Route } from 'lucide-react';
import { isChallengeComplete, isMastered } from './curriculum.js';
import { getLearningReport, getWeakQuestions, gradeStage } from './learning.js';

const iconProps = { size: 16, strokeWidth: 1.65, 'aria-hidden': true };

function useDirtyReport(onDirtyChange, dirty) {
  const ref = useRef(onDirtyChange);
  useEffect(() => { ref.current = onDirtyChange; });
  useEffect(() => () => ref.current?.(false), []);
  useEffect(() => { ref.current?.(dirty); }, [dirty]);
}

function StageQuiz({ stage, status, onSubmit, onBack, onDirtyChange }) {
  const [answers, setAnswers] = useState({});
  const [result, setResult] = useState(null);
  const [changed, setChanged] = useState([]);
  const answered = Object.keys(answers).length;
  const dirty = changed.length > 0 || (answered > 0 && !result);
  useDirtyReport(onDirtyChange, dirty);
  function choose(id, value) {
    setAnswers(previous => ({ ...previous, [id]: value }));
    if (result && !changed.includes(id)) setChanged(previous => [...previous, id]);
  }
  function submit() {
    const next = gradeStage(stage, answers);
    setResult(next);
    setChanged([]);
    onSubmit(stage, answers);
  }
  return <div className="stage-quiz">
    <div className="center-content-heading"><button className="back-button" onClick={onBack}><ArrowRight {...iconProps} />返回阶段列表</button><div><span>{stage.phase}</span><h3>{stage.title}</h3><p>答对 {stage.passScore}/{stage.questions.length} 题通过；错误知识点会进入错题本。</p></div><strong>{status?.passed ? '已通过' : status ? `最好 ${status.best}/${stage.questions.length}` : '未作答'}</strong></div>
    <div className="stage-question-list">{stage.questions.map((question, index) => { const detail = result?.details[index]; const dirtyQuestion = result && changed.includes(question.id); return <fieldset key={question.id} className={detail && !dirtyQuestion ? detail.correct ? 'correct' : 'incorrect' : ''}>
      <legend><span>{String(index + 1).padStart(2, '0')}</span><strong>{question.topic}</strong>{question.prompt}</legend>
      <div className="stage-options">{question.choices.map((choice, choiceIndex) => <label key={choice}><input type="radio" name={`${stage.id}-${question.id}`} checked={answers[question.id] === choiceIndex} onChange={() => choose(question.id, choiceIndex)} /><span>{choice}</span></label>)}</div>
      {detail && !dirtyQuestion && <p><Info {...iconProps} />{question.explanation}</p>}
    </fieldset>; })}</div>
    <div className="stage-submit"><button disabled={answered !== stage.questions.length} onClick={submit}><Check {...iconProps} />提交阶段测验</button><span aria-live="polite">{changed.length ? `已修改 ${changed.length} 道题答案，重新提交后将按新答案判定。` : result ? result.passed ? `通过 · ${result.score}/${result.total}` : `未通过 · ${result.score}/${result.total}，错题已加入复习` : '完成全部题目后提交'}</span></div>
  </div>;
}

function ReviewQuestion({ question, history, onAnswer, onOpenLesson, onDirtyChange }) {
  const [selected, setSelected] = useState(undefined);
  const [feedback, setFeedback] = useState(null);
  const [submitted, setSubmitted] = useState(undefined);
  const dirty = selected !== undefined && selected !== submitted;
  useDirtyReport(onDirtyChange, dirty);
  function submit() { const result = onAnswer(question, selected); setFeedback(result); setSubmitted(selected); }
  const lastWrong = Number.isInteger(history.lastWrongChoice) ? question.choices[history.lastWrongChoice] : null;
  return <fieldset className={`review-question ${feedback && !dirty ? feedback.correct ? 'correct' : 'incorrect' : ''}`}>
    <legend><span>{question.topic}</span>{question.prompt}</legend>
    <div className="stage-options">{question.choices.map((choice, index) => <label key={choice}><input type="radio" name={`review-${question.id}`} checked={selected === index} onChange={() => setSelected(index)} /><span>{choice}</span></label>)}</div>
    <div className="review-meta"><span>错误 {history.wrong} 次 · 连续答对 {history.correctStreak}/2</span>{lastWrong && <span>上次误选：{lastWrong}</span>}</div>
    {dirty && feedback && <p>已修改答案，重新验证后将按新答案判定。</p>}
    {feedback && !dirty && <p><Info {...iconProps} />{feedback.correct ? feedback.history.correctStreak >= 2 ? '连续两次答对，已移出错题本。' : '回答正确，再连续答对一次即可完成复习。' : question.explanation}</p>}
    <div className="review-actions"><button disabled={selected === undefined} onClick={submit}><Check {...iconProps} />验证答案</button><button onClick={() => onOpenLesson(question.lessonId)}>返回对应实验<ArrowRight {...iconProps} /></button></div>
  </fieldset>;
}

function PathView({ moduleConfig, lessons, planned = [], completed, assessments, challengeRuns, onOpenLesson }) {
  const phases = [...new Set(lessons.map(lesson => lesson.phase))];
  const truncate = (text, max) => text.length > max ? `${text.slice(0, max)}…` : text;
  return <div className="path-view"><div className="center-content-heading"><div><span>{moduleConfig.eyebrow}</span><h3>{moduleConfig.pathTitle}</h3><p>课程可自由访问；状态仅反映是否完成学习证据。</p></div></div>{phases.map(phase => <section className="path-phase" key={phase}><div className="phase-heading"><h4>{phase}</h4><span>{lessons.filter(item => item.phase === phase && isMastered(item, completed, assessments, challengeRuns)).length}/{lessons.filter(item => item.phase === phase).length}</span></div><div className="path-lessons">{lessons.filter(item => item.phase === phase).map(lesson => { const experiment = completed.includes(lesson.id); const challenge = isChallengeComplete(lesson.challenge, challengeRuns[lesson.id]); const quiz = assessments.includes(lesson.id); const mastered = isMastered(lesson, completed, assessments, challengeRuns); return <button key={lesson.id} onClick={() => onOpenLesson(lesson.id)}><span className={`path-index ${mastered ? 'done' : ''}`}>{mastered ? <Check {...iconProps} /> : lesson.number}</span><span><strong>{lesson.title}</strong><small title={`实验${experiment ? '已完成' : '未完成'} · 对照任务${challenge ? '已完成' : '未完成'} · 验证题${quiz ? '已完成' : '未完成'}`}><i className={experiment ? 'done' : ''} />实验 <i className={challenge ? 'done' : ''} />对照任务 <i className={quiz ? 'done' : ''} />验证题</small></span><ArrowRight {...iconProps} /></button>; })}</div></section>)}{planned.length > 0 && <section className="path-phase" key="__planned"><div className="phase-heading"><h4>规划中课程</h4><span>{planned.length}</span></div><div className="path-lessons">{planned.map(item => <button key={item.id} className="planned" disabled title={`${item.title}：${item.summary}`}><span className="path-index">{item.priority}</span><span><strong>{item.title}</strong><small>{truncate(item.summary, 72)}</small></span></button>)}</div></section>}</div>;
}

function StageView({ stages, learningState, onSubmit, onDirtyChange }) {
  const [active, setActive] = useState(null);
  if (active) return <StageQuiz key={active.id} stage={active} status={learningState.stages?.[active.id]} onSubmit={onSubmit} onDirtyChange={onDirtyChange} onBack={() => setActive(null)} />;
  return <div><div className="center-content-heading"><div><span>STAGE ASSESSMENTS</span><h3>阶段测验</h3><p>每阶段五题，四题正确即通过；不会锁住后续课程。</p></div></div><div className="stage-list">{stages.map((stage, index) => { const state = learningState.stages?.[stage.id]; return <article key={stage.id}><span className={`stage-number ${state?.passed ? 'done' : ''}`}>{state?.passed ? <CircleCheck {...iconProps} /> : String(index + 1).padStart(2, '0')}</span><div><small>{stage.phase}</small><h4>{stage.title}</h4><p>{state ? `作答 ${state.attempts} 次 · 最好 ${state.best}/${stage.questions.length}` : '尚未作答'}</p></div><button onClick={() => setActive(stage)}>{state ? '再次测验' : '开始测验'}<ArrowRight {...iconProps} /></button></article>; })}</div></div>;
}

function ReviewView({ stages, learningState, onAnswer, onOpenLesson, onDirtyChange }) {
  const questions = getWeakQuestions(stages, learningState);
  const dirtyIds = useRef(new Map());
  const reportDirty = React.useCallback((id, dirty) => {
    if (dirty) dirtyIds.current.set(id, true); else dirtyIds.current.delete(id);
    onDirtyChange(dirtyIds.current.size > 0);
  }, [onDirtyChange]);
  useEffect(() => () => onDirtyChange(false), [onDirtyChange]);
  return <div><div className="center-content-heading"><div><span>REVIEW QUEUE</span><h3>错题与薄弱点</h3><p>同一知识点连续答对两次后，从复习队列移除。</p></div><strong>{questions.length} 个待复习</strong></div>{questions.length ? <div className="review-list">{questions.map(question => <ReviewQuestion key={question.id} question={question} history={learningState.questions[question.id]} onAnswer={onAnswer} onDirtyChange={dirty => reportDirty(question.id, dirty)} onOpenLesson={onOpenLesson} />)}</div> : <div className="center-empty"><CircleCheck size={28} strokeWidth={1.4} /><h4>当前没有待复习错题</h4><p>阶段测验中的错误会自动汇总到这里。</p></div>}</div>;
}

function ReportView({ moduleConfig, lessons, completed, assessments, challengeRuns, learningState }) {
  const report = getLearningReport({ lessons, completed, assessments, challengeRuns, stages: moduleConfig.stages, learningState });
  const rows = [['实验完成', report.experiments], ['对照场景', report.challenges], ['课后验证', report.quizzes], ['阶段测验', report.stageAssessments]];
  return <div className="report-view"><div className="report-heading"><div><span>READINESS</span><strong>{report.readiness}<small>%</small></strong><p>{moduleConfig.reportDescription}</p></div><div><span>已掌握课程</span><strong>{report.mastered}<small> / {lessons.length}</small></strong><span>待复习知识点</span><strong>{report.weak}</strong></div></div><div className="report-rows">{rows.map(([label, value]) => <div key={label}><span>{label}</span><strong>{value.done}<small> / {value.total}</small></strong><span>{value.done === value.total ? '已完成' : '继续学习'}</span></div>)}</div><div className="report-note"><Info {...iconProps} /><p>{report.readiness === 100 && report.weak === 0 ? moduleConfig.completionNote : moduleConfig.nextNote}</p></div></div>;
}

export default function LearningCenter({ moduleConfig, lessons, planned = [], completed, assessments, challengeRuns, learningState, onStageSubmit, onReviewAnswer, onOpenLesson, onDirtyChange }) {
  const [tab, setTab] = useState('path');
  const [quizDirty, setQuizDirty] = useState(false);
  const [reviewDirty, setReviewDirty] = useState(false);
  const dirtyRef = useRef(onDirtyChange);
  useEffect(() => { dirtyRef.current = onDirtyChange; });
  useEffect(() => () => dirtyRef.current?.(false), []);
  useEffect(() => { dirtyRef.current?.(quizDirty || reviewDirty); }, [quizDirty, reviewDirty]);
  const tabs = [['path', Route, '学习路径'], ['stages', CircleCheck, '阶段测验'], ['review', NotebookPen, '错题本'], ['report', Activity, '专题总评']];
  const weak = getWeakQuestions(moduleConfig.stages, learningState).length;
  return <div className="learning-center"><nav aria-label={`${moduleConfig.name} 学习中心`}>{tabs.map(([id, Icon, label]) => <button key={id} aria-pressed={tab === id} className={tab === id ? 'active' : ''} onClick={() => setTab(id)}><Icon {...iconProps} /><span>{label}</span>{id === 'review' && weak > 0 && <strong>{weak}</strong>}</button>)}</nav><div className="learning-center-content"><div hidden={tab !== 'path'}><PathView moduleConfig={moduleConfig} lessons={lessons} planned={planned} completed={completed} assessments={assessments} challengeRuns={challengeRuns} onOpenLesson={onOpenLesson} /></div><div hidden={tab !== 'stages'}><StageView stages={moduleConfig.stages} learningState={learningState} onSubmit={onStageSubmit} onDirtyChange={setQuizDirty} /></div><div hidden={tab !== 'review'}><ReviewView stages={moduleConfig.stages} learningState={learningState} onAnswer={onReviewAnswer} onDirtyChange={setReviewDirty} onOpenLesson={onOpenLesson} /></div><div hidden={tab !== 'report'}><ReportView moduleConfig={moduleConfig} lessons={lessons} completed={completed} assessments={assessments} challengeRuns={challengeRuns} learningState={learningState} /></div></div></div>;
}
