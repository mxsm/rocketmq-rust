# Selected remediation validation results

This record covers the scenarios selected for W18 from the September 8 remediation
plan. It includes results executed during the preceding implementation phases;
those unchanged tests were not rerun solely to populate this record. Commands and
details are in the [P0](2026-09-08-p0.md), [P1](2026-09-08-p1.md),
[P2](2026-09-08-p2.md), and [deployment](2026-09-08-p3.md) records.

## Compatibility

The existing `scripts/m09_compatibility_matrix.py` and
`scripts/v1-capability-manifest.json` provide the feature/wire scenario inventory.
Their historical completion labels do not establish a new execution result. This
remediation selected the directly affected consumers and boundaries:

| Boundary | Executed result |
| --- | --- |
| Consumer retry and Proxy request ownership | P0: 61 consumption, 4 scheduler, 8 send-back, 29 Local, 56 gRPC, and 3 network-send tests passed |
| Store/Broker legacy statuses and append evidence | P1: 11 capability conformance tests, actual single/batch append fence and flush-timeout tests, and replica-timeout/skip-HA cases passed |
| Authentication provider replacement | P2: 375 Auth unit tests, 9 Java alignment tests, and 10 Proxy authentication tests passed |
| Admin and Client consumers | P2: Admin Core 10, CLI deletion 3, TUI Topic 7, and Client pagination 5 tests passed; Client/Admin Core without default features compiled |
| Health and activation | P2: health 5, Cluster deadline/read-only 2, Runtime precedence 2, startup cleanup 1, real embedded Broker readiness/shutdown 1, feature-absence activation 2, Broker config acknowledgement 1, and NameServer config activation 3 tests passed |
| Controller rollout observation | P3: shared DTO validation, Client rejection of old/invalid observations, 2 CLI parsing tests, and request-processor metadata/rejection tests passed |
| Message and remoting wire contracts | P3: 7 message codec compatibility tests and 5 remoting wire golden tests passed |
| Rollout feature boundaries | P3: Client/Admin Core without default features and Client with only `admin-read` compiled; scoped formatting passed for all eight affected Rust packages |

P3 also ran an actual Java process against a Rust Controller remoting listener.
The Java client used the local Apache RocketMQ **5.5.0** distribution jars and
Oracle JDK **25.0.1** on Windows; Rust used **1.95.0**. A single-voter in-memory
Controller and ephemeral loopback ports isolated the protocol test. Both JSON and
ROCKETMQ binary request encodings passed: Java decoded the leader ID/address,
leader flag, and peer list, preserved request correlation, and observed an empty
body for ordinary metadata. Invalid and single-voter rollout requests returned
non-success responses. The Java process and Controller were shut down afterward.
The test passed in 5.19 seconds. Netty emitted a JDK Unsafe deprecation warning;
the assertions and process exit succeeded.

Reproduce with Java 11+ and the selected Java distribution's library classpath:

```sh
export ROCKETMQ_JAVA_CLASSPATH='/opt/rocketmq/lib/*'
cargo test -p rocketmq-controller --test controller_request_processor_contract_test \
  java_process_controller_metadata_compatibility -- --ignored --nocapture
```

`scripts/java/ControllerMetadataCompatibilitySmoke.java` uses the actual Java
`NettyRemotingClient` and `GetMetaDataResponseHeader`. The new quorum JSON body is
opt-in; a legacy Java server can ignore the request extension, so the Rust rollout
client rejects its ordinary empty-body response. The test does not certify all
Java versions, Broker send/consume interoperability, ACL/TLS interoperability, or
Java Controller support for the Rust-only quorum observation.

## Selected capacity and fault scenarios

| Scenario and configuration | Observed result and limit |
| --- | --- |
| Data admission saturated by count or retained bytes, with a Local control reserve | W11 tests admitted route/ACK control work and completed the blocked data requests after releasing their owners. This establishes entry-budget progress, not a throughput guarantee for every Broker resource. |
| Compressed gRPC batches with retained body aliases | W04 tests exercised bounded decompression arenas and permit retention. The last alias releases its budget; the result is a bounded-ownership regression, not a peak-RSS benchmark. |
| Disk observation unavailable or an active root unhealthy | W06 tests distinguished Unknown from known capacity and preserved independent flush/write refusal. A healthy spare did not authorize the unhealthy active segment. |
| Three managed segments, expired retention, and an active alias | W08 `managed_cleanup_reports_submission_before_reaper_completion` submitted two eligible retirements while retaining the alias; after release and actual reaper progress, pending work and namespace removal reflected completion. The last segment remained. Five retirement integration tests also passed. No claim is made about physical free-byte reclamation timing. |
| A Controller lease changes after a real single/batch append | W15 preserved the completed append range/watermark through the post-append fence. Flush and replica timeouts retained existing wire statuses and separate execution evidence. An ambiguous business send was not treated as safely retryable. |
| Transaction operation and Schedule persistence work outlive a caller | P1's 22 transaction queue and 22 Schedule tests passed, including cancellation/drain and retained final persistence. Role transitions did not publish completion after failed persistence. |
| One of three Controller voters becomes unreachable | P3's native gRPC regression required fresh acknowledgements after ReadIndex. The leader refused restarting another healthy voter despite old matched offsets, while replacing the failed voter remained allowed. The test passed in 6.10 seconds and drained its owned RPC task groups. This is quorum admission evidence, not crash-recovery RTO. |
| Controller rollout API refusal and Pod replacement observation | Seven deterministic operator tests passed. Missing/denied/wrong-cluster observations did not evict; PDB refusal was not retried or bypassed; the old Pod or a terminating replacement could not satisfy completion. Kubernetes calls were mocked. |

The existing ACK/failover schema in
`distribution/config/ack-failover-evidence-schema.json` separates Memory, Local,
and Replicated durability and requires deployment faults for corresponding
reliability claims. The selected component tests do **not** qualify those profiles
or establish zero loss, RPO, or RTO. Their results must not be entered as a completed
deployment evidence bundle.

## Untested scope

Docker Desktop could not initialize its local daemon, so no Kubernetes deployment,
real eviction, Pod/PVC recovery, or container fault matrix was executed. Helm
rendering, actual configuration loading, native Raft networking, and Java remoting
results remain independently valid.

Dual Tiered/Timeline pin advancement and withdrawal, slow/offline consumers or
replicas beyond the retention window, prolonged storage pressure, disk-fsync
jitter, and sustained throughput/latency/RSS comparisons remain deployment or
performance follow-up scenarios. No consumer/replica pin policy, retention
guarantee, performance threshold, or production reliability claim was added.
Loom, Miri, extended fuzzing, and the full feature matrix were not rerun.

Known unrelated failures from earlier broader probes remain recorded in the phase
documents: the HA fixture omitting a mandatory Controller write lease, historical
CLI error-text assertions, and unguarded Local/Cluster imports in Proxy unit tests
without default features. Focused affected regressions passed; production checks
and error semantics were not relaxed to accommodate those fixtures.
