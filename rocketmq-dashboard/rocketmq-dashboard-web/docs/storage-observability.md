# Storage observability

## Operator status

Authenticated users can request `GET /api/ops/storage/status`. The response is
designed for the OPS UI and automation and contains only bounded operational
facts:

- selected backend: File, SQLite, MySQL, or PostgreSQL
- topology mode: Single-node or Multi-node capable
- health: Available, Degraded, or Unavailable
- schema or format version
- process-local observation start time and latest successful write time
- safe available bytes for filesystem-backed stores when available
- pool size and idle connection count for SQL stores when available
- bounded reason for degraded or unavailable state

The endpoint does not expose absolute file paths, database URLs, usernames,
passwords, tokens, TLS certificates, request payloads, or raw database errors.
Those details stay in access-controlled platform diagnostics. `GET
/api/health/live` remains a minimal liveness probe and `GET /api/health/ready`
is a minimal readiness probe; neither is an OPS diagnostics API.

The OPS Refresh action always re-requests the status endpoint. If the request
fails, it keeps the last successfully rendered status, records the refresh
error in the UI, and does not present stale information as a fresh healthy
probe.

## Metrics and logs

Storage instrumentation uses only low-cardinality dimensions. The approved
dimensions are `backend`, `operation`, and `result`; use a bounded error class
only when needed for alert routing. Do not use environment IDs, endpoint
addresses, consumer groups, database names, database hosts, user IDs, paths,
session data, token hashes, audit payloads, or exception text as metric labels.

The current fixed catalog covers the authenticated status request,
persistence-gated dashboard mutations, history collection, history retention,
and session/audit cleanup. It intentionally does **not** instrument arbitrary
repository reads, migration internals, or offline CLI backup/verify/restore
stages yet; those operations remain covered by their command result and
structured diagnostics rather than a claim of complete repository coverage.

The emitted operational signals are:

| Signal | Type | Purpose |
| --- | --- | --- |
| Fixed catalog operation duration | histogram | Detect slow status, persisted mutation, collector, retention, and cleanup work. |
| Fixed catalog operation result | counter | Alert on repeated persistence-gated write, collector, retention, or cleanup failures. |
| SQL pool size and idle connections | gauges | Identify sustained pool exhaustion without exposing a connection URL. |
| Persistence failure class | counter | Detect fixed capacity, connection, conflict, timeout, or other failures without error text. |
| History collection result | counter and duration | Detect a collector that no longer persists samples. |
| History retention result | counter and duration | Detect cleanup failures or unexpectedly slow retention work. |
| Session and audit cleanup result | counter and duration | Detect safety-maintenance failures. |
| Safe available filesystem bytes | gauge | Alert before File/SQLite writes become unsafe. |

Logs use the same bounded backend, operation, and result fields. They must not
write database URLs, credentials, TLS material, session tokens, message bodies,
or raw administrative configuration. Prefer a stable error category and a
request correlation identifier supplied by the existing logging layer.

## Suggested alerts

Thresholds must be calibrated to the workload and storage capacity. The table
is a starting point, not a substitute for an on-call runbook.

| Alert | Example condition | Initial response |
| --- | --- | --- |
| Storage unavailable | OPS or readiness reports Unavailable for 2 minutes | Stop writes, inspect the bounded backend result and platform database/filesystem health. |
| Consecutive write failure | Any storage write failure for 5 consecutive attempts | Check credentials/TLS, capacity, locks, and database availability; do not switch backends in place. |
| Collector stalled | Latest history collection success is older than 3 collection intervals | Inspect task logs and lease/pool health; preserve existing history before remediation. |
| Low safe capacity | Safe available bytes below the configured reserve for 5 minutes | Expand storage or retention capacity before writes fail. |
| Pool exhaustion | Idle SQL connections remain zero while acquisition failures occur for 5 minutes | Inspect database capacity and per-replica pool limits; do not raise limits beyond database capacity blindly. |
| Cleanup failure | Session, audit, or retention cleanup reports errors for 15 minutes | Inspect error category and storage health; ensure policy data remains intact before retrying. |

For an incident, capture the sanitized status payload, metric time range,
dashboard version, backend type, replica count, and database/platform event
timeline. Do not attach a URL secret, certificate, session token, or message
payload to an incident ticket.
