export async function fetchJvmProbe(fetcher = fetch, baseUrl, probe) {
  if (!/^[a-z]{2,24}$/.test(probe)) throw new Error('Invalid JVM probe');
  const response = await fetcher(`${baseUrl}/api/jvm?probe=${encodeURIComponent(probe)}`);
  const payload = await response.json();
  if (!response.ok || payload.ok === false) throw new Error(payload.error || `JVM probe failed (${response.status || 'unknown'})`);
  return payload;
}
