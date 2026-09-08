import test from 'node:test';
import assert from 'node:assert/strict';
import { redisStages } from '../src/assessmentCatalog.js';
import { getLearningReport, getWeakQuestions, gradeStage, submitReviewAnswer, submitStage } from '../src/learning.js';
import { labs } from '../src/catalog.js';

const stage = {
  id: 'stage', passScore: 4,
  questions: Array.from({ length: 5 }, (_, i) => ({ id: `q${i}`, topic: `topic-${i}`, lessonId: 'redis', prompt: `question-${i}`, choices: ['A', 'B'], answer: 0, explanation: `explain-${i}` })),
};

test('Stage assessment passes at four of five correct answers', () => {
  assert.equal(gradeStage(stage, { q0: 0, q1: 0, q2: 0, q3: 0, q4: 1 }).passed, true);
  assert.equal(gradeStage(stage, { q0: 0, q1: 0, q2: 0, q3: 1, q4: 1 }).passed, false);
});

test('Submitting a stage records the wrong choice, topic and best score', () => {
  const answers = { q0: 0, q1: 0, q2: 0, q3: 0, q4: 1 };
  const { state, result } = submitStage({}, stage, answers);
  assert.equal(result.score, 4);
  assert.equal(state.stages.stage.passed, true);
  assert.equal(state.stages.stage.best, 4);
  assert.equal(state.questions.q4.lastWrongChoice, 1);
  assert.equal(state.questions.q4.topic, 'topic-4');
});

test('A wrong question leaves review only after two consecutive correct answers', () => {
  let state = submitStage({}, stage, { q0: 1, q1: 0, q2: 0, q3: 0, q4: 0 }).state;
  assert.deepEqual(getWeakQuestions([stage], state).map(q => q.id), ['q0']);
  state = submitReviewAnswer(state, stage.questions[0], 0).state;
  assert.equal(state.questions.q0.correctStreak, 1);
  assert.deepEqual(getWeakQuestions([stage], state).map(q => q.id), ['q0']);
  state = submitReviewAnswer(state, stage.questions[0], 0).state;
  assert.equal(state.questions.q0.correctStreak, 2);
  assert.deepEqual(getWeakQuestions([stage], state), []);
});

test('Wrong review answer resets the consecutive-correct streak', () => {
  let state = submitStage({}, stage, { q0: 1, q1: 0, q2: 0, q3: 0, q4: 0 }).state;
  state = submitReviewAnswer(state, stage.questions[0], 0).state;
  state = submitReviewAnswer(state, stage.questions[0], 1).state;
  assert.equal(state.questions.q0.correctStreak, 0);
  assert.equal(state.questions.q0.wrong, 2);
});

test('Redis assessment catalog has four complete stages linked to real lessons', () => {
  const redisIds = new Set(labs.filter(lab => lab.group === 'Redis 专题').map(lab => lab.id));
  assert.equal(redisStages.length, 4);
  for (const item of redisStages) {
    assert.equal(item.questions.length, 5, item.id);
    assert.equal(item.passScore, 4, item.id);
    for (const question of item.questions) {
      assert.ok(redisIds.has(question.lessonId), question.id);
      assert.equal(question.choices.length, 3, question.id);
      assert.ok(question.explanation, question.id);
    }
  }
});

test('Learning report combines experiments, challenges, quizzes, stages and weak points', () => {
  const lessons = labs.filter(lab => lab.group === 'Redis 专题').slice(0, 1);
  const lesson = lessons[0];
  const challengeRuns = { [lesson.id]: Object.fromEntries(lesson.challenge.requirements.map(requirement => [requirement.id, { metrics: {} }])) };
  const learningState = { stages: { foundations: { passed: true } }, questions: { weak: { wrong: 1, correctStreak: 0 } } };
  const report = getLearningReport({ lessons, completed: [lesson.id], assessments: [lesson.id], challengeRuns, stages: [{ id: 'foundations', questions: [{ id: 'weak' }] }], learningState });
  assert.equal(report.experiments.done, 1);
  assert.equal(report.challenges.done, lesson.challenge.requirements.length);
  assert.equal(report.quizzes.done, 1);
  assert.equal(report.mastered, 1);
  assert.equal(report.stageAssessments.done, 1);
  assert.equal(report.weak, 1);
  assert.equal(report.readiness, 100);
});

test('Learning report counts weak questions only from the current module stages', () => {
  const redisQuestion = redisStages[0].questions[0];
  const learningState = {
    questions: {
      [redisQuestion.id]: { wrong: 1, correctStreak: 0 },
      'jvm-unrelated': { wrong: 3, correctStreak: 0 },
    },
  };
  const report = getLearningReport({ lessons: [], completed: [], assessments: [], challengeRuns: {}, stages: redisStages, learningState });
  assert.equal(report.weak, 1);
});
