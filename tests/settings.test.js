import test from 'node:test';
import assert from 'node:assert/strict';
import { normalizeParams, playbackView } from '../src/settings.js';
import { getLab } from '../src/catalog.js';
test('Stored numeric parameters are clamped', () => {
  const p = normalizeParams(getLab('redis'), { capacity: 0, requests: 900 });
  assert.equal(p.capacity, 1);
  assert.equal(p.requests, 12);
});
test('Malformed saved types fall back safely', () => {
  const p = normalizeParams(getLab('redis'), { capacity: 'oops', cache: 'false', key: {} });
  assert.equal(p.capacity, 3);
  assert.equal(p.cache, true);
  assert.equal(p.key, 'user:1001');
});
test('Threadpool maximum cannot be lower than core size', () => {
  const p = normalizeParams(getLab('threadpool'), { core: 4, max: 1 });
  assert.equal(p.max, 4);
});
test('Illegal select values use catalog defaults', () => {
  assert.equal(normalizeParams(getLab('hashmap'), { capacity: 7 }).capacity, 4);
});
test('Changing parameters cannot mark a new run complete using an old index', () => {
  const oldFrames = [{}, {}, {}, {}];
  const newFrames = [{}, {}];
  const view = playbackView({ frames: oldFrames, index: 3 }, newFrames);
  assert.equal(view.index, 0);
  assert.equal(view.complete, false);
});
test('Only the last frame of the loaded run is complete', () => {
  const frames = [{}, {}];
  assert.equal(playbackView({ frames, index: 1 }, frames).complete, true);
});
