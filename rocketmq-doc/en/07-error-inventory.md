---
title: "Error Architecture Inventory"
permalink: /docs/error-inventory/
excerpt: "Current RocketMQ Rust error ownership and migration inventory."
last_modified_at: 2026-07-04T00:00:00+08:00
toc: true
classes: wide
---

# Error Architecture Inventory

Snapshot date: 2026-07-04.

This inventory records the current error architecture baseline for the
no-compatibility redesign. It is not a completion claim; it identifies the
remaining migration surface and assigns ownership by crate boundary.

## Current Signals

Recent repository scans show these open items:

| Signal | Current evidence |
| --- | --- |
| Legacy error API | `RocketmqError`, `LegacyRocketMQResult`, and `LegacyResult` still appear in `rocketmq-error`, `rocketmq-macros`, `rocketmq-remoting`, `rocketmq-client`, and dashboard common code |
| Public `anyhow` alias | `rocketmq-error/src/unified.rs` exposes `pub type Result<T> = anyhow::Result<T>` |
| Public alias callers | `rocketmq-broker`, `rocketmq-controller`, `rocketmq-namesrv`, and a controller example import `rocketmq_error::Result` |
| Dynamic callback errors | client callback and request future paths still use `dyn Error` |
| Distributed protocol mapping | proxy maps `RocketMQError` to gRPC locally; remoting and proxy remoting paths also map directly to `ResponseCode` |
| Generic string variants | store, controller, dashboard backend, and unified errors still contain string-only internal or general variants |
| Credential formatting | `SessionCredentials::Display` now redacts secret key, signature, and security token; broader `Sensitive<T>` infrastructure is still pending |

## Ownership Matrix

| Area | Error owner | Current role | Target role |
| --- | --- | --- | --- |
| `rocketmq-error` | Error kernel | Central enum plus legacy API and public `anyhow` alias | Own typed error kernel, spec registry, context, redaction, recovery, and observability metadata |
| `rocketmq-macros` | Macro output | Generates legacy `RocketmqError` in request header code | Generate typed `RocketMQError` variants only |
| `rocketmq-remoting` | Protocol boundary | Codec and serialization paths still carry legacy errors and direct response mapping | Convert `ErrorSpec` remoting primitives into `ResponseCode` and remarks |
| `rocketmq-client` | Client boundary | Uses `RocketMQError`, dynamic callback errors, and local retry decisions | Use typed callback errors, `RetryClass`, and redacted context |
| `rocketmq-broker` | Broker boundary | Uses typed errors in many areas and direct processor response mapping | Route externally visible processor failures through remoting adapter |
| `rocketmq-namesrv` | NameServer boundary | Uses typed errors and direct response mapping | Route externally visible failures through remoting adapter |
| `rocketmq-store` | Storage domain | Uses local `StoreError` and `StoreError::General` | Keep private domain errors but expose typed storage variants and sources |
| `rocketmq-controller` | Controller domain | Re-exports central controller errors but still has internal string paths and public alias callers | Preserve raft, storage, serde, and network sources in typed variants |
| `rocketmq-auth` | Auth domain | Uses central errors but mixes authentication, authorization, config, and storage semantics | Split authn, authz, config, and storage categories |
| `rocketmq-common` | Shared utilities | Contains shared helpers that can spread string conversion patterns | Keep shared helpers typed and avoid public `anyhow` contracts |
| `rocketmq-tools` | CLI/tools | Uses app-style error handling and string conversion in admin/storage tools | Keep `anyhow` at binary boundary and map typed errors to CLI categories |
| `rocketmq-proxy` | gRPC and remoting proxy boundary | Owns `ProxyError` and local status mapper | Convert central gRPC primitives to `v2::Code` and `tonic::Code` |
| dashboard web backend | HTTP boundary | Uses `DashboardError` and string-based RocketMQ wrappers | Wrap typed `RocketMQError` and map through HTTP adapter |
| standalone projects | App boundaries | Examples and dashboard apps may lag behind root workspace changes | Follow typed public API after root workspace migration |

## Migration Order

The migration must keep deletion after caller movement:

1. Add kernel metadata and redaction primitives.
2. Add protocol primitive specs and adapter contracts.
3. Replace public alias callers.
4. Remove public `anyhow` aliases.
5. Move macros, remoting codecs, client callbacks, and retry decisions.
6. Split store, controller, auth, common, and tools string-only errors.
7. Move proxy, dashboard, and standalone boundaries.
8. Delete legacy `RocketmqError` and add static guards.

## Done Criteria

The redesign is complete only when all of these are true:

- `rocketmq-error` no longer exposes `RocketmqError`,
  `LegacyRocketMQResult`, or `LegacyResult`.
- Library APIs do not expose `anyhow::Result` as the business error contract.
- Every cross-boundary error has `ErrorKind`, stable code, `ErrorSpec`, retry,
  severity, redaction, and observability metadata.
- Remoting, gRPC, HTTP, CLI, and observability exits read from the central
  metadata instead of interpreting display text.
- Credentials, tokens, signatures, and authorization values are redacted by
  default.
- Source chains are preserved for I/O, serde, storage, raft, transport, and
  runtime failures.
- CI rejects unapproved legacy, public `anyhow`, unmapped error metadata, and
  sensitive formatting regressions.
