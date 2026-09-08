const SESSION_KEY = 'java-lab:runtime-session';
const SESSION_PATTERN = /^[A-Za-z0-9_-]{8,64}$/;

export function createSessionId(storage = localStorage, cryptoLike = crypto) {
  const saved = storage.getItem(SESSION_KEY);
  if (saved && SESSION_PATTERN.test(saved)) return saved;
  const generated = cryptoLike.randomUUID ? cryptoLike.randomUUID() : Array.from(cryptoLike.getRandomValues(new Uint8Array(16)), value => value.toString(16).padStart(2, '0')).join('');
  if (!SESSION_PATTERN.test(generated)) throw new Error('Unable to create a valid runtime session');
  storage.setItem(SESSION_KEY, generated);
  return generated;
}

async function request(fetcher, url, options) {
  const response = await fetcher(url, options);
  const payload = await response.json();
  if (!response.ok || payload.ok === false) throw new Error(payload.error || `Gateway request failed (${response.status || 'unknown'})`);
  return payload;
}

export async function executeCommand(fetcher = fetch, baseUrl, session, command) {
  const value = command.trim();
  if (!value) throw new Error('Command is required');
  return request(fetcher, `${baseUrl}/api/command`, { method: 'POST', headers: { 'Content-Type': 'text/plain; charset=utf-8', 'X-Lab-Session': session }, body: value });
}

export async function resetSession(fetcher = fetch, baseUrl, session) {
  return request(fetcher, `${baseUrl}/api/reset`, { method: 'POST', headers: { 'X-Lab-Session': session } });
}

export async function runtimeStatus(fetcher = fetch, baseUrl) {
  try {
    const response = await fetcher(`${baseUrl}/api/runtime`);
    if (!response.ok) throw new Error('Gateway unavailable');
    return response.json();
  } catch {
    return { java: false, redis: false, message: 'Java 网关未启动' };
  }
}
