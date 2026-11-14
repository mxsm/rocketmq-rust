# Store pressure observations

`store-pressure.v2` consumes `admin-query:store-pressure/<logical-broker>`.
The Connector uses the fixed read-only Admin diagnostic operation for that exact
Broker. The same safe observation is published by the authenticated public MCP
Tool `rocketmq_get_broker_diagnostics`. Collection grants no mutation permission.

## Compatibility and evidence quality

The additive public `store_pressure` member uses
`rocketmq.store-pressure-observation.v1`. Older responses omit it and remain
readable. Each optional value preserves absence separately from a measured zero.
The existing all-required `store_health` projection is unchanged.

Compiled `store-pressure.v1` remains addressable at version `1.0.0` with its
original three explicit Boolean predicates. Its catalog does not implement the
formerly documented 85%, 500 ms, or unspecified 1000-backlog thresholds. Those
numbers are not effective runtime policy. The registry's active version is v2;
coverage retains both major versions so old saved references remain intelligible.

Ordinary Broker, FullCluster and StoreHa inspections select v2. The explicit
`store_pressure_legacy_v1` compatibility template selects only exact v1; it
preserves the old Boolean-input qualification baseline without adding legacy
noise to ordinary inspection results. Existing saved results are unchanged.
This is an additive inspection enum value on the existing authenticated route,
not a generic pack selector. Closed generated clients must include the new value.
The full compiled catalog has 33 major-qualified IDs, current selection has 32,
and the separately named 32-pack fixture baseline still pins its original IDs.
The default `backlog_store` evidence chain selects v2. Only an authorized saved
revision whose primary pack is exactly `store-pressure.v1` pins that chain to
v1, using its actual compiled requirements. The saved revision is not rewritten.

The v2 observation preserves each master/replica independently. Names must match
the canonical Broker resource and instance IDs must be unique. One instance's
policy or measurements cannot fill another instance's missing fields. Source
failures, unsupported fields and unavailable-replica counts remain visible.

The Connector records completion time and the oldest actual source age. Each
Broker's own observation point is also retained. Future, missing, out-of-window
or older-than-300-second points cannot establish current pressure findings. An
instant Admin read is a point observation, not historical range coverage.
The normal source-age-plus-time-in-storage check still applies.

An actual disk or admission predicate can be reported without claiming overall
Store health. The source does not supply sustained flush/dispatch progress or a
warranted stall interval. V2 therefore returns an Inconclusive whole-pack result
with precise observed findings and missing facts. `missing_evidence_facts` is
shared by the engine, investigation planner and coverage quality projection;
injecting `partial=false` and `coverage=Available` cannot supply that proof or
establish production verification. Execution eligibility remains false.

The dispatcher and fixed-evidence replay share a version-owned rule-input
selector. Only v2 may interpret validated partial Store observations as context;
other pack versions retain Available-only admission. Current exact bindings,
hashes, per-instance times and numeric domains are checked before inclusion.
Saved Run revisions retain the concrete findings and their actual Evidence IDs,
but remain Partial/Inconclusive. Stale, malformed and out-of-scope context cannot
be promoted by a caller-provided completeness flag.

For this compiled version, the investigation progress fingerprint excludes
capture timestamps and the projection's derived signal labels. Refreshing the
same observations under new Evidence IDs therefore does not create progress.
Measured queue age, counters, disk probes, policy, instance identity and source
completeness remain part of the comparison. Time-window and freshness checks run before
rule-input admission; excluding a capture clock does not extend evidence life.

## Collection budgets and reproducible source checks

When the read-only Admin source is enabled, each Admin RPC has a separate
millisecond budget. Its default is the smaller of half the Connector's overall
request timeout and 5000 ms. `ROCKETMQ_SRE_ADMIN_RPC_TIMEOUT_MS` can override this
budget, but must be positive and strictly less than the overall timeout. This
leaves time to assemble known-instance observations when a replica times out;
the original absolute query deadline is unchanged. A timed-out RPC remains an
unavailable observation, never a zero counter or a successful cancellation.

The opt-in ignored regression
`sources::live_store_pressure::live_store_pressure_uses_fixed_canonical_read_and_owned_shutdown`
uses the normal Connector configuration and its owned runtime. It requires
`ROCKETMQ_SRE_LIVE_STORE_PRESSURE=1`, a private HTTPS CA configuration, and the
fixed isolated `Local-c02-pressure` / `broker-a` fixture. Its output must be a new
file outside the repository; an existing output is refused.
`ROCKETMQ_SRE_LIVE_STORE_PRESSURE_EXPECT_UNAVAILABLE` accepts only `0` (default)
or `1`. The observation data path is public read-only Admin RPC; the MCP branch
checks authenticated TLS compatibility separately. These engineering samples
do not establish organizational production verification.

## Fields, units and policy provenance

| Observation | Unit and interpretation |
| --- | --- |
| `disk.commit_log_max_used_ratio` | Finite fraction in 0..1, maximum among known configured-root probes. It identifies an observed configured root, not the current active allocation root. |
| `disk.commit_log_legacy_min_used_ratio` | Legacy best-spare minimum, retained as context. It never certifies all roots safe. |
| `disk.commit_log_expected_paths`, `commit_log_observed_paths`, `commit_log_unknown_paths` | Expected root count, received probe count, and invalid/unknown probe count. A complete below-threshold claim requires every configured root plus ConsumeQueue to have a valid probe. |
| `disk.consume_queue_used_ratio` | Actual ConsumeQueue root used-capacity fraction. |
| `runtime.sync_flush_queue_depth` | Currently pending requests. Positive depth alone is not a stall. |
| `runtime.sync_flush_oldest_wait_millis` | Oldest pending request age in milliseconds, including queue wait. It is not flush I/O latency or a latency quantile. |
| `runtime.sync_flush_timeout_total` | Process-lifetime timed-out request count. A positive total is historical context, not a current rate or fault. |
| `runtime.dispatch_behind_bytes` | Byte backlog. Positive backlog alone does not establish stalled dispatch. |
| `runtime.os_page_cache_busy` | The producer's actual CommitLog lock-age predicate. |
| `runtime.last_flush_error` | A retained last-flush failure observation; it is not proof that flushing remains stalled now. |
| `runtime.transient_store_pool_deficient` | Producer-reported pool deficiency predicate. It does not measure pool latency. |
| `runtime.writeable` | Producer-reported primary-write admission. A closed admission does not identify a disk root cause. |

Unknown raw disk probes (`-1`), non-finite numbers and ratios outside 0..1 are
absent measurements. Absolute root names, arbitrary configuration and the raw
Broker runtime table are never included in the safe projection.

Policy ratios come from the Store-owned normalized cleanup configuration:

| Policy | Normalization | Actual predicate |
| --- | --- | --- |
| `disk_warning_used_ratio` | Raw percentage /100, clamped to 0.35..0.90 | Observed ratio strictly greater than warning ratio |
| `disk_clean_forcibly_used_ratio` | Raw percentage /100, clamped to 0.30..0.85 | Strictly greater than the forcible-cleanup ratio; immediate file cleanup also has its own explicit enable flag |
| `disk_max_used_ratio` | Raw percentage clamped to 10..95, then /100 | Strictly greater than the cleanup-start ratio |
| `sync_flush_backlog_reject_depth` | Actual Broker value in requests | Enabled value greater than zero and queue depth greater than or equal to it |
| `sync_flush_backlog_reject_wait_millis` | Actual Broker value in milliseconds | Enabled value greater than zero and oldest pending age greater than or equal to it |
| `dispatch_backlog_reject_bytes` | Actual Broker value in bytes | Enabled value greater than zero and dispatch backlog greater than or equal to it |

Default normalized disk ratios are 0.90 / 0.85 / 0.75 respectively. The v2 rules
also validate ratio domains, threshold ordering and probe counts when reading
saved or replayed evidence. Contradictory policy is Unknown. Zero admission
limits mean explicitly disabled; absent limits mean missing policy. Neither is
an overall healthy verdict.

`configuration_generation` is present only when the Store-owned configuration
matches the relevant runtime configuration snapshot and both generation fields
in the same public response agree. Otherwise it is absent with
`store_policy_generation_unverified`. A timestamp or pack version is not a
configuration generation. Observation values are instantaneous and do not claim
that all independent runtime counters were captured atomically.

## Remaining source limits

Explicit `tiered_configured=false` means not configured; a missing field remains
Unknown. The legacy Tiered fallback false is never used for this decision.
Readiness and a minimum pinned WAL segment do not establish WAL pressure. Actual
retained bytes, pin age, progress and relevant policy are still needed.

Sustained flush/dispatch stalls need monotonic progress, a bounded interval and
an effective stall policy. Current queue age, historical timeout totals, positive
backlog and WAL segment existence cannot replace them. Safe per-root max and
completeness do not establish active-root identity or probe freshness independent
of the enclosing runtime observation. These limits remain explicit upstream
signal gaps rather than fabricated healthy values.

The remaining producers are tracked separately: [role-aware disk roots and
probe age](https://github.com/mxsm/rocketmq-rust/issues/12636), [monotonic
flush/dispatch progress and stall policy](https://github.com/mxsm/rocketmq-rust/issues/12637),
and [Tiered WAL retention, pin age and progress](https://github.com/mxsm/rocketmq-rust/issues/12638).
These dependencies are not implemented by this observation version; their
missing facts remain Unknown.

Synthetic regression fixtures exercise parsing and interpretation only. Actual
isolated source samples and any organizational production verification are
separate records; neither a compiled descriptor nor a fixture grants that status.
