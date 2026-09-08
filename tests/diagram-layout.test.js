import test from 'node:test';
import assert from 'node:assert/strict';
import { getDiagramPositions } from '../src/diagramLayout.js';

test('Diagram layout returns a finite unique position for every node', () => {
  for (const count of [1, 4, 5, 8]) {
    const positions = getDiagramPositions(count, { wide: false, compact: false });
    assert.equal(positions.length, count);
    assert.equal(new Set(positions.map(([x, y]) => `${x}:${y}`)).size, count);
    for (const [x, y] of positions) {
      assert.equal(Number.isFinite(x), true);
      assert.equal(Number.isFinite(y), true);
    }
  }
});

test('Compact diagram layout keeps five nodes in two columns', () => {
  const positions = getDiagramPositions(5, { wide: false, compact: true });
  assert.equal(new Set(positions.map(([x]) => x)).size, 2);
  assert.equal(new Set(positions.map(([, y]) => y)).size, 3);
});

test('Existing four-node standard and wide layouts remain stable', () => {
  assert.deepEqual(getDiagramPositions(4, { wide: false, compact: false }), [[15, 45], [275, 45], [15, 245], [275, 245]]);
  assert.deepEqual(getDiagramPositions(4, { wide: true, compact: false }), [[15, 102], [285, 102], [565, 15], [565, 190]]);
});
