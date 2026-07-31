# Supervised Runbook Templates

These versioned templates are the initial composite runbooks for Phase 3.
They contain only registered, typed `ExecutionAction` values and never shell,
raw RocketMQ request codes, arbitrary Kubernetes patches, or credential
material.

Template values are safe examples. Before scheduling a runbook, the Control
Plane creates a new immutable definition version with the approved cluster,
resource identifiers, observed generations or UIDs, and secret references.
That derived definition still passes the same catalog, risk, parameter, and
calendar validation before it can enter the Plan/Approval/Executor chain.

| Template | Risk | Default execution |
| --- | --- | --- |
| `proxy-canary-rollout.v1.yaml` | R2 | One canary replica, then manual verification |
| `broker-one-by-one-restart.v1.yaml` | R2 | One Broker at a time with a gate between instances |
| `credential-rotation-overlap.v1.yaml` | R2 | Add candidate, validate overlap, retain manual takeover |
| `telemetry-recovery.v1.yaml` | R1 | Restart one Collector, then verify exporter recovery |

All templates default to `max_parallelism: 1`. A future revision may declare a
bounded parallel group only for independent resource keys.
