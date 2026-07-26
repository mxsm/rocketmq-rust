# rocketmq-sre-connector

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](../../../LICENSE-APACHE)

`rocketmq-sre-connector` is the read-only integration boundary between the AI
SRE control plane and RocketMQ MCP. It authenticates with OAuth2 client
credentials, establishes a TLS-protected MCP Streamable HTTP session, verifies
the advertised capability surface, and converts validated wire responses into
canonical Evidence.

## Responsibilities

- Perform the fail-closed MCP protocol, business-schema, tool-digest, tenant,
  scope, and cluster-allowlist handshake.
- Query cluster overview, topics, broker runtime, and consumer lag through MCP.
- Expose protected evidence and capability endpoints on the connector service.
- Refresh an expired token once for an idempotent read request.

## Phase 00 boundary

- MCP is consumed only through Streamable HTTP; this crate does not import the
  MCP server crate or its Rust DTOs.
- A server advertising mutation support, an unknown schema major, or capability
  drift is rejected.
- There is no anonymous fallback, cross-cluster scope expansion, write Tool, or
  RocketMQ management implementation.

## Validation

Run from `rocketmq-sre/`:

```powershell
cargo check --locked -p rocketmq-sre-connector
cargo test --locked -p rocketmq-sre-connector
```

The tests cover capability verification, wire-schema validation, bounded
responses, OAuth2 recovery, tenant isolation, and offboarded-cluster rejection.
