import { isChallengeComplete, isMastered } from './curriculum.js';

const normalizeState = state => ({ questions: { ...(state?.questions || {}) }, stages: { ...(state?.stages || {}) } });

export function gradeStage(stage, answers = {}) {
  const details = stage.questions.map(question => ({
    id: question.id,
    selected: answers[question.id],
    correct: answers[question.id] === question.answer,
    answer: question.answer,
    explanation: question.explanation,
  }));
  const score = details.filter(item => item.correct).length;
  return { passed: score >= stage.passScore, score, total: stage.questions.length, details };
}

function recordQuestionState(state, question, selected) {
  const previous = state.questions[question.id] || { attempts: 0, wrong: 0, correctStreak: 0 };
  const correct = selected === question.answer;
  state.questions[question.id] = {
    ...previous,
    attempts: previous.attempts + 1,
    wrong: previous.wrong + (correct ? 0 : 1),
    correctStreak: correct ? previous.correctStreak + 1 : 0,
    lastWrongChoice: correct ? previous.lastWrongChoice : selected,
    topic: question.topic,
    lessonId: question.lessonId,
  };
  return correct;
}

export function submitStage(currentState, stage, answers) {
  const state = normalizeState(currentState);
  const result = gradeStage(stage, answers);
  for (const question of stage.questions) recordQuestionState(state, question, answers[question.id]);
  const previous = state.stages[stage.id] || { attempts: 0, best: 0, passed: false };
  state.stages[stage.id] = {
    attempts: previous.attempts + 1,
    best: Math.max(previous.best, result.score),
    lastScore: result.score,
    passed: previous.passed || result.passed,
  };
  return { state, result };
}

export function submitReviewAnswer(currentState, question, selected) {
  const state = normalizeState(currentState);
  const correct = recordQuestionState(state, question, selected);
  return { state, correct, history: state.questions[question.id] };
}

export function getWeakQuestions(stages, learningState = {}) {
  const history = learningState.questions || {};
  return stages.flatMap(stage => stage.questions || []).filter(question => {
    const item = history[question.id];
    return item?.wrong > 0 && item.correctStreak < 2;
  });
}

export function getLearningReport({ lessons, completed, assessments, challengeRuns, stages, learningState = {} }) {
  const experimentDone = lessons.filter(lesson => completed.includes(lesson.id)).length;
  const quizDone = lessons.filter(lesson => assessments.includes(lesson.id)).length;
  const challengeTotal = lessons.reduce((sum, lesson) => sum + (lesson.challenge?.requirements?.length || 0), 0);
  const challengeDone = lessons.reduce((sum, lesson) => sum + (lesson.challenge?.requirements || []).filter(requirement => challengeRuns[lesson.id]?.[requirement.id]).length, 0);
  const stageDone = stages.filter(stage => learningState.stages?.[stage.id]?.passed).length;
  const mastered = lessons.filter(lesson => isMastered(lesson, completed, assessments, challengeRuns)).length;
  const achieved = experimentDone + quizDone + challengeDone + stageDone;
  const total = lessons.length * 2 + challengeTotal + stages.length;
  const weak = getWeakQuestions(stages, learningState).length;
  return {
    experiments: { done: experimentDone, total: lessons.length },
    challenges: { done: challengeDone, total: challengeTotal },
    quizzes: { done: quizDone, total: lessons.length },
    stageAssessments: { done: stageDone, total: stages.length },
    mastered,
    weak,
    readiness: total ? Math.round(achieved / total * 100) : 0,
    allChallengesComplete: lessons.every(lesson => isChallengeComplete(lesson.challenge, challengeRuns[lesson.id])),
  };
}
