---
title: "Error Architecture Runbooks"
permalink: /docs/error-runbooks/
excerpt: "Operational and CI runbooks for typed RocketMQ Rust errors."
last_modified_at: 2026-07-04T00:00:00+08:00
toc: true
classes: wide
---

# Error Architecture Runbooks

Use these runbooks when a typed error guard, boundary mapping, or redaction
check fails.

## Runbook: Error Guard Fails

Command:

```powershell
python scripts/error_architecture_guard.py
```

Expected success markers:

```text
ERROR_ARCHITECTURE_GUARD_OK core public surface
ERROR_ARCHITECTURE_GUARD_OK processor boundary mappings
ERROR_ARCHITECTURE_GUARD_OK required mapping adapters
ERROR_ARCHITECTURE_GUARD_OK redaction guards
ERROR_ARCHITECTURE_GUARD_OK all
```

If the guard fails:

1. Read the reported `path:line`.
2. Identify the failed section name.
3. Fix the source regression rather than weakening the guard.
4. Add or update a focused test when the failure exposed missing coverage.
5. Re-run the guard and the relevant Cargo command.

## Runbook: Legacy Or Public Anyhow Regression

Symptoms:

- `RocketmqError`, `RocketMqError`, or `rocketmq_error::Result` appears in core
  error/common code.
- `anyhow::Result` or `anyhow::Error` appears in `rocketmq-error` or
  `rocketmq-common` public contracts.

Action:

1. Replace the old type with `RocketMQError` or `RocketMQResult<T>`.
2. If a domain crate needs private error detail, keep it local and convert to
   `RocketMQError` at the boundary.
3. If the code is a binary or tool entry point, keep `anyhow` at that boundary
   and avoid exporting it back into library APIs.

## Runbook: Processor Mapping Regression

Symptoms:

- The guard reports direct `RequestCodeNotSupported` use in broker or namesrv
  processor production code.
- A processor constructs a generic remoting error by hand.

Action:

1. Select the canonical descriptor at the processor's final owner boundary.
2. Construct a `PublicErrorView`; if retained context is invalid, use the same
   descriptor's `PublicErrorView::descriptor_only` fallback.
3. Call `rocketmq_transport::api::error_response` with an explicit
   `RemotingErrorTarget` so the owner factory, request `opaque`, or existing
   response state is preserved.
4. Keep business-specific success and domain response codes in the owning
   processor.
5. Run focused broker or namesrv tests plus the guard.

## Runbook: gRPC Mapping Regression

Symptoms:

- Proxy returns the wrong `v2::Code` or `tonic::Code` for a `RocketMQError`.
- A new proxy mapping table appears outside `rocketmq-proxy::status`.

Action:

1. Check the error's canonical descriptor and its gRPC projection.
2. Update central metadata first when the mapping is generic.
3. Update `rocketmq-proxy::status` only when local gRPC translation is needed.
4. Add a proxy status test for the affected error.

## Runbook: Redaction Regression

Symptoms:

- Sensitive values appear in `Display`, `Debug`, logs, or test output.
- The guard reports an unredacted `secret`, `password`, `token`, or
  `signature` debug field.

Action:

1. Wrap sensitive values with `Sensitive<T>` or a local helper that returns
   `<redacted>`.
2. Update `Display` and `Debug` implementations together.
3. Add a test that checks the secret value is absent and `<redacted>` is
   present.
4. Run:

```powershell
cargo test -p rocketmq-error error_context_redaction
cargo test -p rocketmq-common debug_and_display_redact_secret_key
python scripts/error_architecture_guard.py
```

## Runbook: New Error Leaf Needs a Descriptor

Action:

1. Add or reuse the structural `ErrorKind` only when local exhaustive matching
   requires it.
2. Add one canonical descriptor and associate every retained direct and wrapped
   leaf with it.
3. Fill code, class, condition, fault, component, fixed message, recovery hint,
   severity, exposure, backtrace policy, remoting/gRPC/HTTP/CLI projections, and
   ordered context schema.
4. Extend the exact catalog snapshot and association completeness tests.
5. Add at least one boundary test if the error crosses remoting, gRPC, HTTP, or
   CLI; Generic exposure must have no dynamic public fields.

## Pull Request Evidence

Every error architecture PR should include:

- Issue link.
- A short description of the affected boundary or domain.
- Focused tests for the changed crate.
- `python scripts/error_architecture_guard.py`.
- package-scoped `cargo fmt -p <package> -- --check` for every affected root
  workspace package.
- Required clippy command for affected Rust projects.

Use [Error Architecture Contribution Guide](19-error-contribution-guide.md) as
the authoring checklist.
