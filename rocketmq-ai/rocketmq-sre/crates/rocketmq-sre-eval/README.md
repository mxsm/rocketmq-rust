# rocketmq-sre-eval

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](../../../../LICENSE-APACHE)

`rocketmq-sre-eval` provides deterministic Phase 00 validation utilities,
the Phase 01 read-only Shadow evaluator, and the Phase 2 multi-domain replay
dataset. It loads required-signal manifests,
checks their structure, exports committed JSON Schemas, supplies the
development-only OAuth2/JWKS issuer, and replays all Wave A diagnostics without
network or Executor access.

## Responsibilities

- Parse and validate component required-signal manifests.
- Export Evidence, descriptor, action, MCP capability-manifest, model-provider,
  and related JSON Schemas.
- Detect duplicate or incomplete signal requirements.
- Issue short-lived RS256 development tokens and rotate the local JWKS key ID
  for authentication recovery tests.
- Replay eight Wave A packs across normal, fault, and missing Evidence.
- Exercise deterministic mock, rules-only, and Provider-outage modes.
- Reject fake citations, cross-cluster scope, and prompt-driven tool expansion.
- Prove `mutation_calls=0`, `executor_calls=0`, and
  `executor_connected=false`.
- Replay the fixed Phase 2 denominator across 18 RocketMQ, Kubernetes,
  storage, security, upgrade, runtime, and telemetry failure domains.
- Report Top-3 root-cause accuracy, high-confidence citation coverage,
  read-only query budgets, model calls, and mutation calls.

## Read-only boundary

This crate is test and development infrastructure. The development issuer is
not a production identity provider, its fixture key must not enter production
images, and the crate performs no RocketMQ mutation or real model-provider
request. The Phase 01 evaluator has no MCP, RocketMQ, Connector, Control Plane,
Kubernetes, or Executor client.

## Validation

Run from `rocketmq-ai/rocketmq-sre/`:

```powershell
cargo test --locked -p rocketmq-sre-eval
cargo run --locked -p rocketmq-sre-eval --bin schema-export -- schemas
git diff --exit-code -- schemas
cargo run --locked -p rocketmq-sre-eval --bin phase01-shadow-eval -- --provider mock --compact
cargo run --locked -p rocketmq-sre-eval --bin phase01-shadow-eval -- --provider rules-only --compact
cargo run --locked -p rocketmq-sre-eval --bin phase01-shadow-eval -- --provider outage --compact
cargo test --locked -p rocketmq-sre-eval --test phase2_replay -- --nocapture
```

The tests verify manifest parsing, duplicate detection, schema export, token
claims, JWKS rotation, all 24 Wave A cases, Provider fallback, citation scope,
cluster scope, prompt-injection boundaries, metadata-only Message Journey, and
the fixed Phase 2 replay quality contract. Phase 2 replay consumes only saved
Evidence and timeline fixtures; it makes no network, model, Executor, or
mutation call.
