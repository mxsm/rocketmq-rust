# Architecture public API and compatibility evidence

This record binds the current workspace surface and compatibility commands to
the repository-owned guards. It replaces evidence that referenced crates and
paths removed by the architecture migration.

## Public API snapshot

- Scope: every core-release library target (`library_targets=26`); excluded
  Dashboard, MCP, and SRE projects are not part of this denominator
- Structural profiles: `profiles=50` (26 workspace defaults plus the 24 frozen
  public-feature matrix entries)
- Snapshot comparison: `differences=0`
- Current classified source-export counts:

  | Package | Classified exports |
  |---|---:|
  | `rocketmq-client-rust` | 224 |
  | `rocketmq-runtime` | 134 |
  | `rocketmq-transport` | 219 |
  | `rocketmq-store` | 211 |

- The current Transport surface has one unversioned `api` module, one
  `RequestProcessor` contract, one authorized dispatcher facade, and one
  response-lifecycle observation callback. Migration-only aliases and the
  duplicate versioned public modules were removed before release.

Previously approved source cleanups remain in force: `ClientRuntime::new` was
removed and callers use fallible `ClientRuntime::try_new`; `ClusterConfig` owns mandatory bounded-execution
fields without changing Serde backward reads; and `MappedBuffer::read_zero_copy`
was replaced by the accurately named `MappedBuffer::read_copy`.

The package count is derived from `cargo metadata`; the guard rejects a
baseline that is missing a current library target or retains a removed one.

## Canonical-path cutover inventory

PR-13 treats Rust source paths as an architecture boundary. The cutover was
started from an executable red baseline rather than by regenerating the intent
manifest: the current repository produced 73 individually reported findings
(the older plan recorded 22 before the PR-11/PR-12 capability additions).
`cargo public-api` was not available in the repository toolchain, so the
repository-owned intent and rustdoc snapshot guards remain the acceptance
authority.

The following table is the deletion and caller-migration ledger. Entries in the
old-symbol column are exhaustive for the compatibility aliases and hidden
Client/Transport/Store root exports removed by this cutover.

| Old symbol or path | Caller before cutover | Canonical target |
|---|---|---|
| `rocketmq_client_rust::proxy_adapter_compat::{ClientInstanceHandle, ClientRpcHook, client_config_for_managed_domain, rpc_hook_from_outbound_signer}` | `rocketmq-proxy-cluster` | Client-owned capability exported once at the Client root; the implementation module is private |
| `rocketmq_client_rust::proxy_adapter_compat::{TopicMessageType, BoundaryType, ExpressionType, MessageExt, MessageId, MessageQueue, MessageQueueAssignment, Message, MessageConst, MessageTrait, LOGICAL_QUEUE_MOCK_BROKER_PREFIX, MASTER_ID, MessageSysFlag, PullSysFlag}` | `rocketmq-proxy-cluster` | Direct `rocketmq-model` imports |
| `rocketmq_client_rust::proxy_adapter_compat::{MessageDecoder, BrokerDataExt}` | `rocketmq-proxy-cluster` | Direct `rocketmq-protocol` imports |
| `rocketmq_client_rust::proxy_adapter_compat::current_millis` | `rocketmq-proxy-cluster` | Direct `rocketmq-runtime` import |
| `MQClientAPIExt`, `MqClientAdminImpl` | No repository caller | Deleted; callers use scoped Client capabilities |
| `run_concurrent_clean_expire_lifecycle_probe`, `ConcurrentCleanExpireLifecycleProbe`, `run_orderly_lock_periodic_lifecycle_probe`, `OrderlyLockPeriodicLifecycleProbe`, `run_pop_orderly_lock_refresh_lifecycle_probe`, `PopOrderlyLockRefreshLifecycleProbe`, `run_lite_pull_assignment_registry_probe`, `run_lite_pull_task_lifecycle_probe`, `LitePullAssignmentRegistryProbe`, `LitePullTaskLifecycleProbe` | Client integration tests and lifecycle benchmarks | `rocketmq_client_rust::test_support::*` with explicit `test-support` |
| `run_process_queue_has_temp_message_probe`, `run_process_queue_max_span_only_probe`, `run_process_queue_put_probe`, `run_process_queue_remove_probe`, `run_process_queue_take_probe`, `ProcessQueue`, `ProcessQueueOperationFixture` | Client integration tests and hot-path benchmarks | `rocketmq_client_rust::test_support::*` with explicit `test-support` |
| `run_pull_message_service_lifecycle_probe`, `PullMessageService`, `PullMessageServiceLifecycleProbe`, `PullMessageServiceShardSnapshot`, `PullRequest`, `run_rebalance_service_lifecycle_probe`, `RebalanceServiceLifecycleProbe` | Client integration tests and scheduler benchmarks | `rocketmq_client_rust::test_support::*` with explicit `test-support` |
| `run_local_file_offset_store_lifecycle_probe`, `LocalFileOffsetStoreLifecycleProbe`, `run_connection_event_listener_lifecycle_probe`, `run_heartbeat_route_index_probe`, `run_route_refresh_concurrent_stale_guard_probe`, `run_route_refresh_shard_probe`, `ConnectionEventListenerLifecycleProbe`, `HeartbeatRouteIndexProbe`, `MQClientInstance`, `RouteRefreshConcurrentProbe`, `RouteRefreshShardProbe` | Client integration tests and lifecycle benchmarks | `rocketmq_client_rust::test_support::*` with explicit `test-support` |
| `run_namesrv_refresh_lifecycle_probe`, `NamesrvRefreshLifecycleProbe`, `run_consumer_stats_manager_lifecycle_probe`, `ConsumerStatsManagerLifecycleProbe`, `run_latency_fault_detector_lifecycle_probe`, `LatencyFaultDetectorLifecycleProbe` | Client integration tests and lifecycle benchmarks | `rocketmq_client_rust::test_support::*` with explicit `test-support` |
| `run_produce_accumulator_guard_lifecycle_probe`, `ProduceAccumulatorGuardLifecycleProbe`, `with_timeout`, `with_timeout_all`, `TopicPublishInfo`, `run_request_future_holder_lifecycle_probe`, `run_request_future_holder_scan_probe`, `RequestFutureHolderLifecycleProbe`, `RequestFutureHolderScanProbe` | Client tests, doctests, and producer benchmarks | Test/benchmark helpers move to `rocketmq_client_rust::test_support::*`; timeout helpers remain internal |
| `run_trace_queue_depth_accounting_probe`, `run_trace_worker_lifecycle_probe`, `TraceWorkerLifecycleProbe` | Client integration tests and trace benchmarks | `rocketmq_client_rust::test_support::*` with explicit `test-support` |
| `rocketmq_transport::LocalRequestHarness` | Historical Broker and NameServer processor tests | Removed with the compatibility processor graph; channel lifecycle tests use `rocketmq_transport::test_support::LocalChannelHarness`, while processor tests use the socket-free `EmbeddedRequestHarness` |
| `rocketmq_transport::LocalResponseSink` | No external caller | Private Transport dispatch implementation |
| `rocketmq_store::bench_support::*` | Store benchmarks and Store contract tests | `rocketmq_store::test_support::*` with explicit `test-support` |

The cutover does not change request codes, headers, Serde names/defaults, or
persisted layouts. Because the project has not reached its first production
release, no deprecated forwarding aliases are retained.

The repository owner explicitly approved removal of obsolete internal crates,
facades, module paths, and source-level contracts on 2026-07-29. That decision
is the compatibility classification authority for this change; it does not
waive protocol, wire, persisted-layout, or implemented-behavior compatibility.
The same authority explicitly approved the `ClusterConfig` source-shape change:
the obsolete fixed-lane execution contract is not retained.
It also approved typed blocking-profile additions; they do not change RocketMQ
wire, storage, or recovery contracts.
The same source-compatibility decision applies to the mapped-buffer read
cleanup: old internal names are not retained when they misstate allocation and
ownership.

## Compatibility matrix

- Result: `40/40`
- Feature profiles: `feature=24/24`
- Wire and ingress contracts: `wire=6/6`
- Storage layouts and engines: `storage=10/10`

The wire group is owned by the current canonical boundaries:
`rocketmq-protocol`, `rocketmq-transport`, `rocketmq-proxy`, and
`rocketmq-runtime`. Storage checks exercise current capability and component
contracts rather than old re-export facades. The matrix intentionally does not
recreate the removed `rocketmq-common`, `rocketmq-remoting`, or workspace
facade packages.
