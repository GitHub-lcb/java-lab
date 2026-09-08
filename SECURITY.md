# Security Policy

## Scope

The simulated lessons are client-side teaching models. The Java gateway in
`server/` is a local learning service, not a production Redis proxy.

## Do not expose the local gateway directly

The current gateway intentionally has no user authentication. It only binds to
`127.0.0.1` and is designed for one developer machine. Do not publish port
`8787` to the public Internet or place it behind a public reverse proxy as-is.

For a public deployment, add authentication, per-user authorization, rate
limits, command and resource quotas, audit logging, HTTPS, isolated Redis
databases or instances, and process/container sandboxing. Disable the real
experiment mode until those controls exist.

## Reporting

Do not include passwords, tokens, Redis dumps, or production endpoints in an
issue. Report security problems privately to the repository maintainers after
the GitHub repository is created.
