import test from 'node:test';
import assert from 'node:assert/strict';
import { isElementFullscreen, toggleElementFullscreen } from '../src/fullscreen.js';

test('Requests fullscreen on the architecture workbench', async () => {
  let requested = 0;
  const element = { requestFullscreen: async () => { requested++; } };
  const result = await toggleElementFullscreen(element, { fullscreenElement: null });
  assert.equal(result, true);
  assert.equal(requested, 1);
});

test('Exits fullscreen when the same workbench is already fullscreen', async () => {
  let exited = 0;
  const element = {};
  const documentLike = { fullscreenElement: element, exitFullscreen: async () => { exited++; } };
  const result = await toggleElementFullscreen(element, documentLike);
  assert.equal(result, false);
  assert.equal(exited, 1);
});

test('Supports WebKit fullscreen APIs', async () => {
  let requested = 0;
  const element = { webkitRequestFullscreen: async () => { requested++; } };
  assert.equal(await toggleElementFullscreen(element, { webkitFullscreenElement: null }), true);
  assert.equal(requested, 1);
  assert.equal(isElementFullscreen(element, { webkitFullscreenElement: element }), true);
});

test('Reports unsupported fullscreen instead of silently doing nothing', async () => {
  await assert.rejects(() => toggleElementFullscreen({}, {}), /not supported/i);
});
