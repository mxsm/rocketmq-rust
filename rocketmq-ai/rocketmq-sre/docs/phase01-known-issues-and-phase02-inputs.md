<!--
Copyright 2023 The RocketMQ Rust Authors
Licensed under the Apache License, Version 2.0.
-->

# Phase 01 known issues and Phase 02 inputs

## Known Phase 01 limits

1. The committed Shadow evaluator is fixture-only and deliberately has no
   network. Live Connector, MCP, PostgreSQL, persistent object storage, and
   observability paths are validated by service integration tests and
   environment smoke, not by this offline Job. Compose uses a named local
   volume; production uses the same Evidence contract with an S3-compatible
   store.
2. Automated CI uses mock, rules-only, and outage modes. A real Provider smoke
   remains manual because credentials, region, model access, and data
   residency are organization-specific.
3. The suite stops at the first invalid manifest, fixture, citation, or model
   contract. It does not continue after a safety failure.
4. Timing is wall-clock diagnostic latency from the local evaluator. It is
   useful for regression comparison but is not a production latency SLO.
5. Mock and rules-only cost is correctly recorded as zero. Real usage and cost
   are recorded by `ModelInvocationRecord`, not estimated by the evaluator.
6. Message Journey validates metadata-only fixtures; it does not inspect
   encrypted message bodies or payloads.
7. The Kind Shadow Job is an acceptance Job, not a long-running production
   deployment. It has no service, public endpoint, service account, or egress.
8. Phase 01 produces read-only recommendations only. It has no approval,
   ActionPlan, Executor, Execution Agent, or RocketMQ/Kubernetes mutation path.
9. The UI is a client-only Vite SPA. It does not enable React Server
   Components, server actions, loaders/actions, server rendering, or React
   Router static handlers. The package is pinned to the newest published
   `react-router-dom` release used by this workspace; the npm advisory for the
   optional RSC action path is therefore outside the deployed attack surface.
   Dependency updates remain part of the normal security review.
10. Compose and Kind use an explicit development authentication fixture so the
    desktop UI and smoke suite can run without an external identity provider.
    The production build remains OIDC Authorization Code with PKCE and fails
    closed when its issuer or client configuration is absent. The development
    fixture is not evidence that an organization-specific IdP integration has
    completed acceptance.
11. Compose uses the bounded local Evidence object store. The production
    S3-compatible HTTPS adapter is implemented and covered by contract tests,
    but endpoint credentials, retention policy, encryption policy, and a live
    object-store acceptance run remain deployment-owned.
12. The Connector-to-Control Plane command path is the versioned mTLS HTTPS
    long-poll protocol documented in
    [the transport ADR](connector-control-plane-transport-adr.md). HTTP/2 may be
    negotiated by the proxy; the deployed application protocol is not a gRPC
    bidirectional stream.
13. The ordinary RocketMQ Dashboard and AI SRE UI remain separate products in
    Phase 01. A Dashboard-to-SRE deep link is a later integration and does not
    grant either UI access to the other product's session or mutation APIs.

## Phase 02 inputs

| Priority | Input | Desired next result |
| --- | --- | --- |
| P0 | Persist evaluation runs | Store versioned suite, fixture, rule, prompt, Provider, latency, and cost metadata for trend comparison |
| P0 | Live read-only replay corpus | Export redacted production Evidence into reviewed, tenant-scoped replay sets without message bodies or credentials |
| P0 | Diagnosis quality rubric | Add false-positive, false-negative, citation precision, missing-Evidence, and operator usefulness scoring |
| P0 | Provider evaluation matrix | Compare approved OpenAI, Anthropic, Gemini, Bedrock, DeepSeek, GLM, Kimi/Moonshot, and local profiles under the same Evidence pack |
| P1 | Golden incident sets | Curate reviewed incidents for lag, broker, client, telemetry, topology, and drift regressions |
| P1 | Cost and latency budgets | Define per-purpose model token, latency, retry, and cost envelopes |
| P1 | Long-running shadow | Run a read-only Control Plane consumer that stores candidate diagnoses but exposes no Executor connection |
| P1 | Drift detection | Alert on rule, fixture, prompt, model revision, citation, or confidence drift |
| P2 | Human review workflow | Route incorrect/outdated results into Knowledge and DiagnosticPack review tasks |
| P2 | Phase 02 planning boundary | Add ActionPlan generation only after policy, approval, risk, and Executor contracts exist; keep execution disabled by default |

The last row is a design input, not authorization to add mutation in Phase 01.
