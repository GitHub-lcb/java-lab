const standardFour = [[15, 45], [275, 45], [15, 245], [275, 245]];
const wideFour = [[15, 102], [285, 102], [565, 15], [565, 190]];

export function getDiagramPositions(nodeCount, { wide = false, compact = false } = {}) {
  if (nodeCount <= 4) return (wide && !compact ? wideFour : standardFour).slice(0, nodeCount);
  const columns = compact ? 2 : 3;
  return Array.from({ length: nodeCount }, (_, index) => [15 + (index % columns) * 260, 45 + Math.floor(index / columns) * 200]);
}
