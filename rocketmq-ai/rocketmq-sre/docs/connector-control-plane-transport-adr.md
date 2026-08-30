<!--
Copyright 2023 The RocketMQ Rust Authors
Licensed under the Apache License, Version 2.0.
-->

# ADR: Connector to Control Plane transport

## Status

Accepted for Phase 01 and later compatibility.

## Decision

The Connector initiates an authenticated HTTPS channel to the Control Plane
and uses bounded long-poll requests for registration, heartbeat, command
delivery, and Evidence responses. Deployments may negotiate HTTP/2 through the
mTLS proxy, but the application contract is HTTP request/response rather than
a gRPC streaming contract.

The channel has these fixed boundaries:

- the Connector is always the network initiator;
- non-loopback traffic requires server-authenticated TLS and a Connector
  client certificate;
- the dedicated Connector API is reachable only through the mTLS proxy and is
  not mounted on the public Control Plane listener;
- the proxy overwrites certificate-derived identity headers;
- the application also validates the Connector bearer identity, tenant,
  cluster, session, capability digest, and read-only declaration;
- commands, responses, poll duration, request bytes, Evidence bytes, and
  retries are bounded;
- a lost poll is safe to repeat, and response delivery uses stable command and
  idempotency identifiers;
- no Executor command or RocketMQ/Kubernetes mutation is part of this channel
  in Phase 01 or Phase 02.

MCP remains a separate Streamable HTTP integration between Connector and
`rocketmq-mcp`; MCP wire DTOs are not shared with the Control Plane channel.

## Rationale

Long polling keeps the independently deployable Connector behind an outbound
network boundary, works with common mTLS reverse proxies, and exposes explicit
request-size, deadline, retry, and audit points. It also avoids coupling the
Control Plane to generated gRPC types while the command and Evidence contracts
are still evolving.

The chosen protocol provides the behavior needed by the read-only workflow:
prompt command pickup, heartbeat-based liveness, bounded Evidence delivery,
reconnect, and process restart recovery. A permanently open bidirectional
stream is not required for these semantics.

## Consequences

- Documentation and tests must call this transport an mTLS HTTPS long-poll
  channel, not a deployed gRPC stream.
- HTTP/2 is a transport optimization and is not relied on for correctness.
- Polling load is bounded by configured wait and heartbeat intervals.
- A future streaming transport may be introduced only as a new negotiated
  protocol version. It must preserve the same identity, tenant, cluster,
  capability, idempotency, audit, and read-only checks.
- The existing Connector public health/readiness listener is not an alternate
  command path.

## Verification

The deployment contract and certificate checks are implemented by:

```powershell
.\rocketmq-ai\rocketmq-sre\scripts\verify-mtls-deployment.ps1 -CheckCertificates
.\rocketmq-ai\rocketmq-sre\scripts\phase01-kind-smoke.ps1 -ValidateOnly
```

The live Phase 01 smoke verifies that Evidence returned over this channel is
persisted, canonically hashed, cited by a diagnosis, and never routed to an
Executor.
