# Backlog

## Redis after `v1-redis-foundation`

The Redis core path is usable and frozen as a foundation milestone. These
items remain intentionally open and do not block JVM development.

### Content depth

- Add full deep-dive chapters for cache penetration, breakdown and avalanche.
- Add full deep-dive chapters for cache consistency, transactions/Lua and locks.
- Add full deep-dive chapters for persistence, Sentinel/Cluster and operations.
- Expand the capstone with a complete request trace, failure decisions and review rubric.

### Real experiments

- Add a controlled Bloom filter environment. The current real lesson only verifies negative-cache commands.
- Add concurrent request generation for hot-key rebuild and request coalescing.
- Add a database-backed consistency lab with commit, delete failure and compensation evidence.
- Add predefined, reviewed Lua scripts without enabling arbitrary `EVAL` input.
- Add safe token-checked lock release and lease-renewal probes.
- Add disposable Redis instances for RDB/AOF restart and recovery verification.
- Add disposable master/replica, Sentinel and Cluster topologies.
- Add metrics-backed hot-key, big-key and slow-command diagnostics.

### Product and safety

- Include real-lab evidence in the learning report without confusing it with simulated completion.
- Add expiry for abandoned local gateway session tracking.
- Add authentication, quotas, audit logs and per-user runtime isolation before any public gateway.
- Keep the existing Redis instance read/write exercises limited to namespaced learning keys.

## Priority labels

- `redis-content`: explanation, exercise and assessment depth
- `redis-runtime`: real Redis scenarios and topology
- `security`: required before public runtime exposure
- `learning`: progress, evidence and review experience
