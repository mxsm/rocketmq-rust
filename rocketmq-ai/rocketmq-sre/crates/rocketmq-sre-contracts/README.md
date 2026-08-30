# rocketmq-sre-contracts

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](../../../../LICENSE-APACHE)

`rocketmq-sre-contracts` owns the versioned data contracts shared by the AI SRE
workspace. It defines typed identifiers, schema compatibility, Evidence,
diagnostic hypotheses, incident state, stable errors, and extension
descriptors.

## Responsibilities

- Define `rocketmq-sre.evidence.v1` queries, snapshots, content, sensitivity,
  freshness, coverage, and diagnostic relationships.
- Produce RFC 8785 canonical JSON hashes using SHA-256.
- Protect incident terminal states and reject unsupported schema features.
- Describe evidence sources, diagnostic packs, actions, providers, and
  integrations without binding them to an implementation.

## Phase 00 boundary

This crate is deliberately data-only. It has no networking, async runtime,
database, model SDK, or RocketMQ implementation dependency. It performs no I/O
and owns no service lifecycle.

## Validation

Run from `rocketmq-ai/rocketmq-sre/`:

```powershell
cargo test --locked -p rocketmq-sre-contracts
cargo run --locked -p rocketmq-sre-eval --bin schema-export -- schemas
```

The tests verify Serde round trips, stable hashing, schema compatibility,
incident transitions, and descriptor serialization.
