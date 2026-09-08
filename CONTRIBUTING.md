# Contributing

## Development

```powershell
npm ci
npm test
npm run test:java
npm run build
```

The simulated models must remain deterministic. Add a failing test before
changing a model, command policy, learning rule, or runtime client.

## New lessons

Each lesson needs a catalog entry, a deterministic model or a real command
guide, a stated model boundary, official references, a challenge, and tests.
Do not present a browser model as a measurement of production throughput or
latency.

## Runtime changes

Keep the local gateway bound to loopback. Do not add arbitrary command
execution, unrestricted Lua, credentials in source, or a public Redis port.
