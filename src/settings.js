export function normalizeParams(lab, raw = {}) {
  const params = { ...lab.defaults };
  for (const field of lab.fields) {
    const value = raw?.[field.key];
    if (field.type === 'text') {
      if (typeof value === 'string' && value.trim()) params[field.key] = value.trim().slice(0, 32);
    } else if (field.type === 'toggle') {
      if (typeof value === 'boolean') params[field.key] = value;
    } else if (field.type === 'select') {
      if (field.options.some(([v]) => v === value)) params[field.key] = value;
    } else if (typeof value === 'number' && Number.isFinite(value)) {
      const step = field.step || 1;
      params[field.key] = Math.min(field.max, Math.max(field.min, Math.round(value / step) * step));
    }
  }
  if (lab.id === 'threadpool') params.max = Math.max(params.core, params.max);
  return params;
}
export function readLocal(key, fallback) {
  try { return JSON.parse(localStorage.getItem(key)) ?? fallback; } catch { return fallback; }
}
export function writeLocal(key, value) {
  try { localStorage.setItem(key, JSON.stringify(value)); return true; } catch { return false; }
}
export function playbackView(context, frames) {
  const currentRun = context.frames === frames;
  const index = currentRun ? Math.min(context.index, frames.length - 1) : 0;
  return { index, complete: currentRun && !!context.completed };
}
