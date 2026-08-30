# Phase 03 Broker configuration generation CAS

`broker.config.patch_allowlisted.v1` uses a dedicated, fail-closed RocketMQ
Admin protocol instead of the legacy unconditional broker configuration
update.

## Protocol

| Item | Value |
| --- | --- |
| Request code | `UpdateBrokerConfigCas` (`330`) |
| Request header | `expectedGeneration: u64` |
| Request body | Java-properties compatible broker configuration patch |
| Success header | `configGeneration: u64` |
| Conflict response | `InvalidParameter` plus the current `configGeneration` |

The existing `UpdateBrokerConfig` (`25`) request and public Admin API retain
their legacy behavior. A CAS caller never falls back to that request. An older
Broker therefore rejects request code `330` without modifying configuration,
instead of silently ignoring a new header and applying an unconditional
update.

## Supervised flow

1. The Execution Agent reads `brokerConfigGeneration` from authenticated
   Broker runtime diagnostics.
2. The handler validates the exact action allowlist and records the before
   values and observed generation.
3. The Agent submits one `UpdateBrokerConfigCas` request.
4. Broker holds its configuration update lock, compares the expected
   generation, validates the patch, atomically publishes the next
   `ConfigUpdateTransaction`, and projects the same generation into runtime
   capabilities.
5. The Agent verifies the response generation is exactly the observed
   generation plus one and then verifies the live configuration.

A generation conflict is a typed outcome containing both expected and actual
generations. It stops execution and requires a new precheck and plan; the
client does not retry or overwrite the newer state.

## Compatibility and safety

- Missing, zero, or malformed `expectedGeneration` on request code `330` is
  rejected.
- Log-filter TTL updates are excluded because they use a separate controlled
  state machine.
- Restart-required, unsupported, and invalid fields remain rejected by the
  Broker transaction validator.
- The mutation-only Client and Admin Core surfaces expose the generation query
  and CAS result without enabling read-only MCP processes or legacy mixed
  Admin APIs.
- Rollback is another forward CAS transaction: read the latest generation,
  apply the recorded before values with that expected generation, and receive
  a new generation. No generation is decremented or overwritten.

## Focused verification

The protocol tests cover request and response header codecs. Broker tests prove
one successful commit, rejection of a stale generation without state change,
and compatibility of the legacy headerless update. Client and Admin Core
feature checks prove the new capability is available in a
`mutation-client-adapter` build.

## Local acceptance record

The production adapter was accepted on 2026-07-29 with a real loopback
NameServer and Broker, Docker PostgreSQL, and build artifacts isolated on
the selected D/F data drive (D by default):

```powershell
.\rocketmq-ai\rocketmq-sre\scripts\phase03-broker-cas-smoke.ps1
```

The bounded smoke completed with
`PHASE03_BROKER_CAS_SMOKE_OK generation_advanced=true stale_rejected=true
rollback_advanced=true`. It proved one forward CAS, one stale-generation
rejection with no overwrite, and one inverse CAS using the latest generation.

The PostgreSQL Critic tests additionally proved that the R2 plan cannot be
approved without a durable heterogeneous Critic record, that a DeepSeek
failure can fall back to Kimi while preserving the model lineage, and that a
same-family alias cannot unlock approval. The shipped descriptor therefore
advertises supervised execution support. It remains R2: autonomous
authorization is rejected by the Executor registry, and a distinct approver
with cluster scope is still required.
