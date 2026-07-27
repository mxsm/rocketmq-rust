# rocketmq-sre-core

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](../../../LICENSE-APACHE)

`rocketmq-sre-core` contains implementation-independent coordination rules for
the AI SRE workspace. It builds on the contracts crate to manage incident
transitions and the lifecycle of registered extension descriptors.

## Responsibilities

- Enforce valid incident-state transitions and terminal-state protection.
- Register versioned evidence sources, diagnostic packs, actions, model
  providers, and integrations.
- Support descriptor upgrade, disable, deprecate, and rollback operations.
- Fail closed for unknown schema majors and unsupported required capabilities.
- Register and run versioned, deterministic Wave A `DiagnosticPack`
  implementations without network access.
- Derive confidence from evidence coverage, supporting signals,
  counter-evidence, and partial state; packs cannot supply their own score.
- Replay saved evidence fixtures for topology, consumers, producers, brokers,
  message paths, telemetry, and deployment drift.

## Runtime boundary

The crate depends only on `rocketmq-sre-contracts` at runtime. It has no
network, database, RocketMQ client, model provider, or service-runtime behavior,
and it does not execute actions. Diagnostic conclusions fail closed on invalid
hashes, mixed tenant/cluster scope, unknown citations, local-only required
evidence, and message content.

## Validation

Run from `rocketmq-sre/`:

```powershell
cargo test --locked -p rocketmq-sre-core
```

The tests also replay normal, fault, and missing-evidence fixtures for all eight
Wave A packs, verify deterministic results and citations, enforce low
confidence for incomplete evidence, and reject message bodies.
