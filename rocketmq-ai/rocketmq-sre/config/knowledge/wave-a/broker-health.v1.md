# Broker health diagnosis

Use this runbook when Broker readiness, storage, dispatch, replication, or request latency degrades.

## Required evidence

- `broker_up`, lifecycle state, and bounded Broker runtime snapshot.
- Store health, disk utilization, dispatch backlog, and recovery status.
- Request error and latency signals.
- Controller and route evidence to distinguish local from cluster-wide impact.

## Interpretation

Lifecycle state below ready, unhealthy store status, sustained disk pressure, or increasing dispatch backlog supports Broker degradation. A ready Broker with healthy store and normal request outcomes is counter-evidence. Local-only diagnostics must be marked not-production-verified until the Connector can query them.

## Read-only recommendation

Present the unhealthy subsystem, observed window, and counter-evidence. Do not restart the Broker, truncate storage, clean files, or change replication settings.
