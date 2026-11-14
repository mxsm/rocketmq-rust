# Windowed send latency observations

`send-latency.v2` consumes the fixed read-only resource
`prometheus:send-latency/<literal-topic>`. It measures a recorded Broker
Store-submit/result population. It does not measure a client send, a complete
Client/Proxy/Remoting/Broker/Store path, or a causal segment decomposition.
The four other exact-topic segment populations and their correlation remain
explicitly unavailable. The whole pack, saved Run revision and evidence chain
remain Partial/Inconclusive; no production or execution qualification follows.

## Source and population contract

The native producer is the Broker `record_send_message_latency` histogram. Its
unit is milliseconds, and its recorded population consists of the Store-submit
and result path after Broker validation. Some attempts rejected before that
point are outside the population. Metrics sampling and label cardinality policy
can further limit the observations. The `other` topic label is ambiguous even
when the requested topic is literally named `other`; it never qualifies an
exact-topic distribution.

The collector selects one entire family, in this order:

1. `rocketmq_send_message_latency_bucket`
2. `rocketmq_rocketmq_send_message_latency_bucket`
3. `rocketmq_send_message_latency_milliseconds_bucket`
4. `rocketmq_rocketmq_send_message_latency_milliseconds_bucket`

Aliases are compatibility spellings, never additional traffic. A malformed
preferred family cannot be filled from a lower-priority alias. An extra node
present only in another alias makes the population ambiguous. The fixed
cluster-label configuration and exact topic matcher apply to every request and
every returned series, including empty series. Complete private series identity
is retained through validation; scrape job/instance addresses are not published.
Duplicate scrapes of the same node and bucket are rejected rather than summed.

The supported grid is the default explicit millisecond grid of the native
OpenTelemetry SDK 0.33.1: `0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000,
2500, 5000, 7500, 10000, +Inf`. Every original histogram must have every bucket,
with matching actual scrape times. Unknown grids remain unsupported; the union
of observed buckets is not proof of completeness.

## Three reads and bounded work

One admitted canonical tool query issues at most three physical Prometheus
HTTP reads, with one absolute deadline, cancellation signal, total response-byte
budget and total row-work budget. There are no internal retries.

1. A fixed all-alias range-vector selector at the completed window end returns
   actual scrape points from `/api/v1/query`.
2. A fixed `increase(selected_family{exact_matchers}[window])` expression at
   the same end returns an increase for each original series.
3. The identical all-alias raw selector is repeated. Canonically ordered labels
   and samples must equal the first response, including unselected aliases.

The increase and raw identity/bucket sets must match exactly. Each original
histogram is checked before increases are summed locally. This avoids hiding a
missing or invalid original bucket behind server-side aggregation. Prometheus
warnings/infos, changed source data, incomplete sets, malformed numbers or
exceeded bounds cannot retain a qualified percentile from an earlier read.
Only closed diagnostic reasons are published.

`ROCKETMQ_SRE_SOURCE_MAX_ROWS` and `ROCKETMQ_SRE_MAX_RESPONSE_BYTES` remain total
bounds for this operation. The default 500 rows may be insufficient for a dense
or long window. Use an explicit trusted operator allowance for an appropriate
workload; the collector never downsamples, silently shortens the requested
window or triples a configured bound. Sanitization that truncates the saved
proof clears the displayed Broker estimate and predicate qualification.

## Time and quantile interpretation

The original nanosecond requested interval is retained. Prometheus points use a
millisecond grid, with explicit query endpoints `floor_ms(start)` and
`floor_ms(end)`. Actual millisecond samples in the requested left-open,
right-closed interval have exactly the same membership on that grid. Increase
uses the declared grid width; no nanosecond extrapolation precision is claimed.
The HTTP `time` retains milliseconds. An interval that collapses to one
millisecond or ends after collection starts cannot establish a distribution.

Requested interval, effective query grid, actual scrape span, oldest last-used
scrape and collection completion are distinct fields. `Evidence.observed_at`
is real collection completion. Source freshness is the checked, rounded-up age
of the oldest last-used scrape at completion. Current eligibility adds time in
storage. Re-querying historical data cannot renew it. PostgreSQL's documented
outer timestamp truncation to microseconds is accepted without rewriting the
original content interval or changing sample membership. Inspection execution,
Run evaluation and finalization recheck current eligibility.

Prometheus `increase` adjusts counter resets and extrapolates endpoints, so its
estimated count can be fractional. The local strict classic P99 calculation uses
cumulative bucket increases, never bucket counts as milliseconds or summed P99s.
Invalid/nonmonotonic grids are rejected instead of silently repaired. Raw
counters must be nonnegative exact integers in the safe numeric domain.

Two actual scrape points are the mathematical minimum. This profile adds
engineering eligibility checks: each original observed span covers at least
80% of the query interval, edge/internal gaps are at most 60 seconds, and at
least 100 reset-adjusted `+Inf` increments were actually observed before a local
predicate is evaluated. These are explicit eligibility policies, not
Prometheus requirements or statistical confidence guarantees. Sparse estimates
do not establish a predicate. Empty series are missing; complete constant
counters mean no recorded traffic, not zero-millisecond healthy latency.

For a rank in the `+Inf` tail, the Prometheus-compatible estimate is the last
finite bound, with `tail_limited` resolution and no finite upper bound. It
cannot evaluate the finite latency predicate. A genuine positive population
entirely in the zero bucket can have a zero-millisecond recorded estimate.

## Operator predicate and compatibility

`ROCKETMQ_SRE_SEND_LATENCY_POLICIES_JSON` optionally supplies an array of exact
`{cluster_id, topic, p99_millis}` records. It defaults to no policy. The array is
bounded to 8192 UTF-8 bytes and 64 entries; clusters must be configured, topics
must be literal, duplicate scopes are rejected, and thresholds must be integer
milliseconds in `1..=3600000`. Unknown fields, wildcards and unconfigured
clusters fail configuration validation. No UI, model or arbitrary query can
change this predicate.

The comparison is strictly greater than the operator-declared estimate
threshold. Its provenance is `operator_configuration`, its scope is explicit,
and its runtime generation is unknown. This is not a Broker-enforced timeout,
SLO, segment root cause or production validation. The old catalog's nominal
500 ms value was not executed by Boolean v1 and is not adopted as a default.

Exact `send-latency.v1`/`1.0.0` Boolean behavior and saved references remain
readable. Ordinary latency inspections, symptom selection and the default
`send_latency_ha` scenario use v2. Only an authorized saved primary revision
whose pack is exactly v1 pins that evidence chain back to v1 and its actual
compiled requirements. The explicit `send_latency_legacy_v1` inspection
template preserves the frozen Boolean fixture baseline. It adds no generic
pack selection or mutation permission. Migration 0063 and the generated
inspection enum preserve every earlier template value.

The complete compiled catalog has 34 major-qualified IDs; active selection has
32 and the frozen qualification fixture baseline still contains its original
32 versions/96 synthetic scenarios. V2 has separate focused and actual-source
engineering evidence, not inherited production qualification.

Progress comparison ignores capture/as-of clocks, derived eligibility and HTTP
statistics. Actual scrape times, counter values, data span, source presence and
operator policy remain facts. Re-capturing identical data under new Evidence
IDs does not admit another evaluation merely because time passed.

## Reproducible isolated-source regression

The ignored test
`sources::live_send_latency::live_send_latency_uses_fixed_canonical_read_and_owned_shutdown`
uses the normal `ConnectorConfig`, SourceComposer, canonical resolver and owned
runtime. It requires explicit `ROCKETMQ_SRE_LIVE_SEND_LATENCY=1`, completed
`ROCKETMQ_SRE_LIVE_SEND_LATENCY_START`/`END` timestamps, a private CA-verified
HTTPS MCP fixture and the fixed isolated `Local-c03-latency` /
`ProducerSimpleTest` scope. Its data path is Prometheus HTTP; MCP checks
authenticated compatibility and cluster activity. The output must be a new
file outside the repository. `ROCKETMQ_SRE_LIVE_SEND_LATENCY_EXPECT` selects the
explicit expected Broker availability, never a qualification override.

The source-only gate does not exercise normal reverse-channel startup inventory
upload. Normal-process validation also covers registration, inventory, an
actual canonical command and graceful termination. The inventory and common
MCP/Admin adapter awaits keep large child states on the heap, preserving the
same admitted session, deadlines, cancellation, audit and `RuntimeOwner` with
the default worker stack. No detached work or alternative runtime is introduced.

Inventory path coverage indexes the observed typed endpoints and directed edges
once per render. Unresolved associations or trimmed participants keep the affected
path partial; a complete remaining subgraph cannot erase those gaps. Byte-bound
trimming conservatively halves edge prefixes before asset prefixes, retaining the
Cluster and measuring the actual encoding after each coverage update. It does
not assume encoded size is monotone or promise maximum retention. One absolute
budget and the real parent cancellation cover readiness, admitted reads and the
final authenticated upload. Cancellation still settles admitted-read audits;
canceling an upload wait does not assert that the remote upload was unaccepted.
Channel readiness requires a real heartbeat acknowledgement after the initial
inventory attempt, rather than registration alone.

These samples demonstrate the configured same-tree read path and truthful
boundaries. They do not establish independent organizational/SRE adjudication,
all-traffic measurement, real IdP revocation, whole-path causality or production
execution permission.
