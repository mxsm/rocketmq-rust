<!--
Copyright 2023 The RocketMQ Rust Authors
Licensed under the Apache License, Version 2.0.
-->

# Phase 01 live Kind validation record

This record captures the reproducible Phase 01 live acceptance completed on
2026-07-29 against the `rocketmq-sre-phase00` Kind cluster. It complements the
offline 24-fixture Shadow suite with a real RocketMQ, MCP, Connector, Control
Plane, PostgreSQL, and isolated model-provider path.

## Command

```powershell
.\rocketmq-sre\scripts\phase01-kind-smoke.ps1 `
  -ClusterName rocketmq-sre-phase00 `
  -SkipPhase00Parity
```

The deployment used the checked-in bounded probe limits:

- ten messages;
- 64-byte payloads;
- five messages per second;
- a dedicated synthetic Topic and Consumer Group;
- no Kubernetes service-account token or Executor credential.

## Accepted result

The final run returned:

```text
PHASE01_LIVE_SMOKE_OK target=Kind read_only=true model_assisted=true diagnostic_packs=8 mutation_calls=0 executor_calls=0 total_lag=60
PHASE01_KIND_E2E_OK cluster=rocketmq-sre-phase00 read_only=true
```

The smoke proved:

- the persisted MCP capability digest remained stable and
  `mutation_supported=false`;
- the Connector returned positive canonical Consumer Lag Evidence through the
  protected reverse channel;
- the model-assisted diagnosis cited only persisted Evidence and recorded
  provider/model lineage plus bounded usage;
- all eight Wave A packs persisted a complete result or an explicit
  `partial + missing_required_evidence` result;
- Markdown and HTML inspection reports were generated;
- validated knowledge was imported and retrieved;
- cross-cluster access was denied and public reads produced append-only audit
  records;
- the live workflow made zero cluster mutation and zero Executor calls.

The accepted Wave A set was:

1. `consumer-lag.v2`
2. `consumer-runtime.v1`
3. `broker-health.v1`
4. `producer-connectivity.v1`
5. `message-path.v1`
6. `cluster-topology.v1`
7. `deployment-drift.v1`
8. `telemetry-pipeline.v1`

Later phases add more read-only packs to the same inspection templates. The
Phase 01 smoke validates every expanded record structurally while counting only
the original eight packs in this acceptance result.

## Regression findings closed during acceptance

Two compatibility regressions were found and fixed before the accepted run:

1. The isolated Phase 01 model mock originally supported diagnostic structured
   output only. It now also recognizes the three exact read-only Provider smoke
   shapes for connectivity, schema output, and synthetic tool arguments while
   continuing to reject arbitrary tools.
2. The Phase 01 smoke originally assumed the Phase 01 pack count and global
   OpenAPI boundary would never evolve. It now accepts additive read-only pack
   expansion and understands both Phase 03 and Phase 05 bounded-mutation
   extension names without weakening the MCP read-only or arbitrary-mutation
   prohibitions.

The `local-openai-compatible` profile passed all Provider smoke checks and was
operator-transitioned from `quarantined` back to `certified` before the final
run.

## Repeatability check

On 2026-07-30, a follow-up operator run used the README command from the
repository root without changing the cluster configuration:

```powershell
.\rocketmq-sre\scripts\phase01-kind-smoke.ps1 `
  -ClusterName rocketmq-sre-phase00 `
  -SkipPhase00Parity
```

It returned:

```text
PHASE01_LIVE_SMOKE_OK target=Kind read_only=true model_assisted=true diagnostic_packs=8 mutation_calls=0 executor_calls=0 total_lag=10
PHASE01_KIND_E2E_OK cluster=rocketmq-sre-phase00 read_only=true
```

The same validation pass completed a clean UI dependency install, lint, all 71
UI tests, and the production build. The Rust workspace format check, Clippy,
workspace tests, and the 12-test Provider contract suite also passed after
synchronizing the committed generated schemas with the public contracts.

## Scope statement

This record proves the Phase 01 read-only AI SRE baseline on a local Kind test
cluster. It does not claim that every RocketMQ component Required Signal is
remotely queryable, and it does not certify Phase 03 R2 action execution.
