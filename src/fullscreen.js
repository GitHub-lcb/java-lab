export function isElementFullscreen(element, documentLike = document) {
  return (documentLike.fullscreenElement || documentLike.webkitFullscreenElement) === element;
}

export async function toggleElementFullscreen(element, documentLike = document) {
  if (!element) throw new Error('Fullscreen is not supported: target element is missing');
  if (isElementFullscreen(element, documentLike)) {
    const exit = documentLike.exitFullscreen || documentLike.webkitExitFullscreen;
    if (!exit) throw new Error('Fullscreen is not supported by this browser');
    await exit.call(documentLike);
    return false;
  }
  const request = element.requestFullscreen || element.webkitRequestFullscreen;
  if (!request) throw new Error('Fullscreen is not supported by this browser');
  await request.call(element);
  return true;
}
