# rocketmq-sre-eval

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](../../../LICENSE-APACHE)

`rocketmq-sre-eval` provides deterministic Phase 00 validation utilities. It
loads required-signal manifests, checks their structure, exports committed JSON
Schemas, and supplies the development-only OAuth2/JWKS issuer used by local
integration tests.

## Responsibilities

- Parse and validate component required-signal manifests.
- Export Evidence, descriptor, action, MCP capability-manifest, model-provider,
  and related JSON Schemas.
- Detect duplicate or incomplete signal requirements.
- Issue short-lived RS256 development tokens and rotate the local JWKS key ID
  for authentication recovery tests.

## Phase 00 boundary

This crate is test and development infrastructure. The development issuer is
not a production identity provider, its fixture key must not enter production
images, and the crate performs no RocketMQ mutation or real model-provider
request.

## Validation

Run from `rocketmq-sre/`:

```powershell
cargo test --locked -p rocketmq-sre-eval
cargo run --locked -p rocketmq-sre-eval --bin schema-export -- schemas
git diff --exit-code -- schemas
```

The tests verify manifest parsing, duplicate detection, schema export, token
claims, and JWKS rotation.
