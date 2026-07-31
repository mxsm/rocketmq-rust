# Consumer runtime diagnosis

Use this runbook when a group is registered but clients are disconnected, flapping, paused, or unevenly assigned.

## Required evidence

- Read-only Admin consumer connection and assignment metadata.
- MCP group and subscription summary.
- Bounded runtime, log, and Kubernetes restart evidence.
- Lag evidence for impact and recovery confirmation.

## Interpretation

No connected clients, repeated session churn, or an allocation count inconsistent with queue count supports a runtime problem. Stable client sessions and complete allocation are counter-evidence. Client addresses and principals must be pseudonymized before persistence or model use.

## Read-only recommendation

Correlate session changes with deployment and application-log events. Provide the affected pseudonymous clients and time window; do not terminate clients or rebalance the group.
