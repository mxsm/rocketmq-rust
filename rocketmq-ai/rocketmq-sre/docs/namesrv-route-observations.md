# Current NameServer route observations

`namesrv-route.v2` compares two bounded rounds from explicitly configured,
direct NameServer peers. Its canonical read is
`admin-query:namesrv-route/<topic>` and its exposure is `AdminRpc`. The normal
Connector still verifies the public MCP TLS/protocol/capability prerequisites;
MCP's pooled Topic view is not the data source for this comparison.

The public read primitive uses only `GetNamesrvConfig` and
`GetRouteinfoByTopic` at the configured exact endpoint. It never invokes
`QueryDataVersion`: that request can update the NameServer's Broker heartbeat
metadata. There is no Broker reachability probe, fallback endpoint, arbitrary
query or target mutation.

## Trusted configuration and read bounds

`ROCKETMQ_SRE_NAMESRV_ROUTE_CONFIG_JSON` is a trusted operator JSON array. Each
entry binds `cluster_id` to `physical_cluster`, an opaque `revision` and
`deployment_reference`, an explicit `expected_view`, and two to four `peers`.
Each peer has `peer_id`, `instance_reference` and its private `endpoint`.
Endpoints must be distinct canonical targets already present in the readonly
Admin source's configured NameServer allowlist. Logical MCP cluster aliases
are allowed; unspecified IP literals such as `0.0.0.0`, `::` and mapped
unspecified IPv4 are rejected as direct remote peer identities. Configured DNS
names remain operator declarations rather than server attestation. Logical names
can differ from the physical Broker cluster; authority is bound by cluster UUID.

All three expected view fields are explicit Booleans:
`cluster_test` must be false, and `order_message_enable` and
`support_acting_master` must match the intended native view. In each round the
collector reads configuration from every eligible peer before starting route
reads. A missing, malformed, denied or mismatched configuration skips that
peer's route read in that round. In particular, a known `clusterTest=true`
never starts a route request that could enter its external-cluster fallback.

The collector plans `2*N` configuration and at most `2*N` route RPCs for `N`
peers (eight total for the ordinary two-peer fixture). Started RPC counts,
known received body bytes, accepted bytes and unknown counts are reported
separately. There is one admitted canonical query and one final gateway audit;
physical RPC attempts are not mislabeled as tool admissions or remote ACKs.
Both rounds share the caller's absolute deadline, parent/query cancellation,
accepted-body byte limit and row limit. Per-RPC timeout is the smaller of the
configured readonly Admin timeout and remaining caller budget. There are no
implicit retries or per-round budget resets.

Full private configuration entries count against the read budget before they
are discarded. Only the three flags and bounded status/count metadata survive.
The transport already receives a separately bounded remoting frame before the
profile checks its remaining accepted-body limit; this is not a claim that the
profile's Evidence byte limit is a socket frame limit. Oversize/unknown outcomes
do not become empty routes or zero consumption. An output trimmed by a generic
sanitizer is rejected rather than retained as a complete comparison proof.
Before capture is accepted, the complete typed response envelope is measured
with a maximum-width sequence and must fit the existing 512 KiB channel limit.
Increasing the source body allowance cannot bypass that limit or trim a proof.

## Route equality and missing data

The private decoder is duplicate-aware and preserves missing versus explicit
null fields, including compatible legacy numeric map keys. It does not use
`BrokerData::Eq`, which omits some route-view fields. The safe comparison covers
Broker membership, Broker IDs and endpoint membership, physical cluster, zone,
acting-master presence/value, queue counts, permission and Topic system flags,
ordered Topic configuration, filter-server associations and static mapping
scope/epoch/IDs. Native signed and unsigned numeric domains remain distinct.

Broker and filter endpoints are replaced by scoped keyed IDs before evidence
is constructed. Foreign/mixed-cluster routes and malformed/duplicate records
are rejected with closed reasons; original endpoints, configuration bodies and
remote remarks are never returned or logged by this profile.

Topic absence requires an actual `TopicNotExist` response. Permission failure,
disconnection or a skipped request cannot establish absence or a route
difference. Unknown flags becoming readable do not establish a configuration
change. Stable two-round differences, actual observed transitions and
incomparable reads are separate outcomes. A round longer than two seconds or
an acquisition span longer than five seconds is incomparable. These are fixed
engineering comparison limits, not registration thresholds or atomicity proof.

## Time and saved investigation revisions

The payload keeps the original requested window, actual read spans, collection
completion and source age separately. `observed_at` is collection completion;
source age starts at the earliest actual RPC. Source age plus elapsed storage
time must remain within the compiled 300-second limit. Future, malformed or
out-of-scope payloads are excluded. Normal PostgreSQL truncation of outer times
to microseconds is accepted only when the original inner timestamp truncates
to exactly that value; arbitrary same-millisecond substitutions are rejected.

The native protocol returns current state. It cannot establish state throughout
an ended historical investigation window. Only this exact compiled v2 permits
validated `current_context_only` evidence as Partial rule input. An ordinary
ended-window Run can therefore retain the actual EID, an Inconclusive current
observation/difference advisory, and a real saved Run revision. Old instant
adapters and other pack versions keep their existing temporal rules.

Nine facts remain unconditionally missing: first/last registration time, last
heartbeat time, source generation time/version/instance identity, atomic local
origin, scope-bound Broker reachability and historical window coverage. A caller
`partial=false` or `coverage=available` declaration cannot remove them. Current
equality is not Healthy; current differences are not a historical root cause.
The whole pack stays Inconclusive/Partial, cannot establish production
verification and grants no execution authority.

## Stable identity and no progress

`ROCKETMQ_SRE_NAMESRV_ROUTE_KEY_FILE` names an absolute, operator-managed regular
file containing exactly 32 bytes. The same opened handle is inspected and read
with a 33-byte maximum including the EOF probe. The key is separate from JWT,
ACL, internal channel tokens and TLS material. It must persist across normal
restarts and credential rotations. Missing configuration/key is unconfigured;
the collector never substitutes a process-random or authentication-derived key.

The `hmac-sha256-v1` identity domain includes tenant, cluster and an explicit
role. Route endpoints use the same role/domain across every peer and round.
Peer target identities use a separate role. Administrative revision, display
peer/instance/deployment references and collection clocks are excluded from the
semantic fact digest. Actual endpoint membership, route/view/presence and raw
completeness changes remain facts.

While exact v2 and its canonical binding remain effective, planning pins the
domain from original supplied/successful-read references. The service loads a
bounded, currently authorized metadata pool using the original read provenance
and retention checks. Business freshness expiry does not discard an otherwise
accessible domain anchor. Its old business content is never passed to current
rules, model explanation, SavedR citations or finalization. Missing retained
metadata, bad integrity/scope or conflicting/changed domains stops progress
before another Evaluate/model admission. A stale original anchor plus fresh,
same-domain changed route can progress; identical facts remain NoProgress.
Removing v2 does not make its old anchors block unrelated remaining packs.

## Compatibility and validation

Exact `namesrv-route.v1` and persisted references retain their Boolean contract.
The existing MCP `TopicDescribe` conversation operation also pins v1: it still
collects the old pooled single-result projection. It is not presented as a
direct-peer v2 comparison merely by changing a diagnostic label.
Ordinary inspections and route scenarios use active v2. The explicit readonly
`namesrv_route_legacy_v1` inspection template pins only v1 for the frozen
32-version/96-scenario qualification dataset. It does not qualify v2. The
compiled catalog has 35 retained major-qualified IDs and 32 active IDs.
An authorized saved primary v1 revision pins the routing scenario to its
original contract; a public caller cannot request a version override.

Maintained tests cover strict normalization, configuration gating, bounded
attempts/cancellation, false absence/change, freshness and precision, current
SavedR citations, domain continuity, Viewer rejection and old-version reads.
The opt-in source regression
`sources::live_namesrv_route::live_namesrv_route_uses_fixed_peer_reads_and_owned_shutdown`
requires `ROCKETMQ_SRE_LIVE_NAMESRV_ROUTE=1`, verified public MCP TLS, fixed
`Local-c04-namesrv` / `ProducerSimpleTest`, two separately owned native peers,
one closed expected comparison state, and an exclusive output outside the repo.
It uses the normal Source composer and default owned worker, not a private
collector bypass. Source protocol and engineering fixtures are distinct from
independent SRE adjudication or organizational production qualification.

Registration times and heartbeat observations remain an upstream source gap
tracked in [#12645](https://github.com/mxsm/rocketmq-rust/issues/12645).
Source version/instance attestation and per-Topic local-origin provenance are
tracked in [#12646](https://github.com/mxsm/rocketmq-rust/issues/12646).
Those signals and historical route coverage remain Unknown. Double sampling,
operator-declared independent peers and collection timestamps do not invent
those signals. An unrelated publish, a partial chain or timing coincidence
cannot establish a causal disconnection finding.
