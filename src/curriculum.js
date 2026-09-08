export function gradeQuiz(quiz, answers) {
  const details = quiz.map((question, index) => ({
    correct: answers[index] === question.answer,
    selected: answers[index],
    answer: question.answer,
    explanation: question.explanation,
  }));
  const score = details.filter(item => item.correct).length;
  return { passed: details.length > 0 && score === details.length, score, details };
}

export function matchesRequirement(requirement, params) {
  return Object.entries(requirement.when || {}).every(([key, value]) => params[key] === value);
}

export function recordChallengeRun(challenge, history = {}, params, metrics) {
  const matches = (challenge?.requirements || []).filter(requirement => matchesRequirement(requirement, params));
  if (!matches.length) return history;
  const next = { ...history };
  for (const requirement of matches) next[requirement.id] = { params: { ...params }, metrics: structuredClone(metrics) };
  return next;
}

export function isChallengeComplete(challenge, history = {}) {
  const requirements = challenge?.requirements || [];
  return requirements.length === 0 || requirements.every(requirement => history[requirement.id]);
}

export function isMastered(lab, completed, assessments, challengeRuns = {}) {
  if (!completed.includes(lab.id)) return false;
  if (lab.quiz?.length && !assessments.includes(lab.id)) return false;
  return isChallengeComplete(lab.challenge, challengeRuns[lab.id]);
}
