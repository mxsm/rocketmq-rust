# Capability verification records

The coverage API separates registration, handlers, deployment configuration,
target Evidence quality, test validation, and production validation. A test report
can establish the test layer while production validation remains unknown.

`POST /internal/v1/capability-verifications` exists only on the private workload
listener. Its OIDC identity needs the `capability_verifier_service` role and the
normal read scope for the authorized tenant and cluster. UI operators, Connector,
model, and execution identities cannot submit reports. Development authentication
cannot submit any production-level outcome. The server supplies `verified_by`,
`record_id`, and `recorded_at`; callers cannot supply these fields.

The typed `CapabilityVerificationRequest` supplies:

| Field | Meaning |
| --- | --- |
| `tenant_id`, `cluster_id` | Must match workload claims and the stored cluster owner. |
| `pack_id`, `pack_version` | Exact qualified ID and compiled semantic version. |
| `level` | `test` or `production`; these are independent facts. |
| `method` | `deterministic_fixture` or `live_read_only`. |
| `outcome` | `passed`, `failed`, `inconclusive`, or `revoked`. |
| `resource` | Optional exact canonical Evidence resource, such as `store-pressure/broker-a`. Navigation resources such as `broker/broker-a` are different. |
| `investigation_id`, `revision_id` | Both present or both absent. Supplied Evidence must belong to the saved, authorized revision. |
| `time_range`, `observed_at`, `valid_until` | Explicit report coverage and validity. |
| `source_ref`, `report_digest` | Bounded `urn:` or `report:` reference and SHA-256 report digest. The server never fetches a report URL or accepts raw logs and credentials. |
| `evidence_ids` | At most 64 concrete saved references. |

A report observation cannot be in the future or more than one day old at ingestion.
Its covered interval must end by that observation and span at most the preceding
day. Validity ends after the observation and within 30 days. Supplied Evidence must
be visible, unexpired, in the same tenant and cluster, and within the stated
resource and time interval.

A production `passed` report also needs `live_read_only`, a concrete resource,
and current Evidence satisfying every compiled required field and freshness
bound. Synthetic, partial, stale, missing, unsupported, and unchecked external
content cannot establish this fact. Declaring a method does not bypass the checks.
Fixture-based tests demonstrate implementation behavior and never become
production validation.

Records are append-only. Scope, exact pack version, level, and report digest
identify a retry; it returns the original record without reordering history.
Digests are normalized to lowercase. Reusing an identity for different content
returns a conflict. The latest source `observed_at` wins within a scope. At equal
times, revoked, failed, and inconclusive results take precedence over passed.
Late delivery of an older pass cannot restore success. An expired or revoked
latest report remains unknown; the reader does not fall back to an older pass.

`GET /v1/capabilities/coverage` retains its legacy fields and returns verification
provenance under the layer's optional `verification` property. The default cluster
overview excludes resource-only reports. An explicit `resource` query selects a
canonical target. Saved investigation/revision queries use their selected Evidence
and the pack's matching canonical resource; a report bound to another revision
cannot be substituted. When both resource and revision are supplied, the resource
must be a visible saved reference. Current required proof IDs and quality are
rechecked before a production fact remains present.

Coverage does not grant execution authority. `execution_eligible` remains false;
approval and execution interfaces are unchanged.

## Native Prometheus scope

The default `ROCKETMQ_SRE_PROMETHEUS_CLUSTER_LABEL=rocketmq_cluster` preserves
existing deployments. This project's native Prometheus exporter uses `cluster`:
configure `ROCKETMQ_SRE_PROMETHEUS_CLUSTER_LABEL=cluster` for that source and keep
the label in `ROCKETMQ_SRE_SOURCE_LABEL_ALLOWLIST`. The selected external cluster
value must match the physical value emitted by the source. Queries and response
validation use this same configured label. The collector never removes the
cluster selector or combines different label spellings to find a result.

The fixed send-latency query also selects the requested topic. It accepts the
current unit-free histogram name and its legacy millisecond-suffixed alias,
using the same selectors for both. The projection preserves cumulative buckets
and their bounds. Bucket totals are not added into a latency value; quantiles and
threshold conclusions remain unavailable until explicitly supported. Route
consistency, registration age, and Store thresholds likewise remain missing when
the actual source does not provide them.
