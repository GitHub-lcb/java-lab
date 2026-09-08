export function matchesExpected(expected, result) {
  if (!expected) return false;
  if (expected.kind === 'equals') return JSON.stringify(result) === JSON.stringify(expected.value);
  if (expected.kind === 'integerRange') return Number.isInteger(result) && result >= expected.min && result <= expected.max;
  if (expected.kind === 'set') return Array.isArray(result) && result.length === expected.value.length && new Set(result).size === result.length && expected.value.every(item => result.includes(item));
  return false;
}
export function advanceLesson(guide, run, entry) {
  const step = guide.steps[run.index];
  if (!step || !entry.ok || entry.command.trim() !== step.command || !matchesExpected(step.expected, entry.result)) return run;
  const index = run.index + 1;
  return { index, complete: index === guide.steps.length, evidence: [...run.evidence, { stepId: step.id, command: entry.command, result: entry.result }] };
}
export const createRun = () => ({ index: 0, complete: false, evidence: [] });
export function lessonSession(base, id) {
  const value = `${base}_${id}`;
  if (!/^[A-Za-z0-9_-]{8,64}$/.test(value)) throw new Error('Invalid course session');
  return value;
}
