# Consumer lag diagnosis

Use this runbook when total lag, lag slope, queue skew, or message age is increasing.

## Required evidence

- MCP consumer lag by topic, group, queue, and Broker.
- Producer and consumer rates over the same bounded time range.
- Consumer runtime connectivity, assignment, and pause metadata.
- Broker readiness and store-health counter-evidence.

## Interpretation

Growing lag with production rate above consumption rate supports a throughput deficit. A high queue-skew ratio supports assignment or hot-partition imbalance. Zero connected clients supports a consumer-runtime outage. Falling lag and a stable oldest-message age are counter-evidence. Never infer zero lag from an unavailable source.

## Read-only recommendation

Show the affected queues and clients, then ask the operator to verify consumer capacity, assignment, and application errors. Do not reset offsets or start consumers from the SRE system.
