# Remoting Processor V2 Migration Ledger

This ledger is the live inventory for removing production dependencies on the
V1 `RequestProcessor` contract, `ConnectionHandlerContext`, and raw `Channel`
parameters. It is an implementation aid, not a source fingerprint or a release
gate. Update the affected rows whenever a migration changes ownership or status.

## Status and ownership model

| Status | Meaning |
|---|---|
| `V1` | The production implementation still uses the V1 processor/context contract. |
| `Dual` | A V2 implementation exists, but a production V1 route or compatibility path remains. |
| `V2 infrastructure` | The code already uses the V2 contract and supports a later production cutover. |
| `Compatibility` | The code is retained temporarily to serve or drain an accepted V1 request. |

Migration owners are the implementation batches defined by the connection
handler context plan:

- `MIG-02`: Transport, Client, Namesrv, and Controller processors.
- `MIG-03`: ordinary Broker leaf processors and session identity registries.
- `MIG-04`: response-heavy Broker leaves and their result/deferred seams.
- `MIG-05`: Broker aggregate/router production cutover and bounded V1 drain.
- `MIG-06`: Admin nested handlers, Auth, and Proxy.
- `MIG-07`: final production V1 removal and compatibility cleanup.

## Transport and Client

| Area | Production implementation or consumer | Current state | Owner and completion condition |
|---|---|---|---|
| Transport | `rocketmq-transport/src/request_processor/default_request_processor.rs` — `DefaultRequestProcessor` | `V1`; the channel and context parameters are unused. | `MIG-02`; return an explicit V2 reply outcome and migrate its direct tests. |
| Client | `rocketmq-client/src/implementation/client_remoting_processor.rs` — `ClientRemotingProcessor` | `V1`; dispatch and several callback helpers accept `Channel` or `ConnectionHandlerContext`. | `MIG-02`; map reply, one-way, and protocol-no-response callbacks to explicit V2 outcomes and migrate the inline transport-backed tests. |
| Transport | `rocketmq-transport/src/clients/` and `rocketmq-transport/src/clients/client.rs` — `TransportClient<PR>`, `RemotingClient<PR>`, and `ClientInner<PR>` | The production client inbound route is generically bound to V1 `RequestProcessor`; it creates a context for each session. | `MIG-02`; provide a V2 inbound client owner/coexistence seam that does not expose raw channel/context authority to the processor. Keep the V1 facade compatibility-only until `MIG-07`. |
| Transport | `rocketmq-transport/src/remoting.rs` — `RemotingGeneralHandler<RP>` | V1 generic inbound router; invokes the V1 processor and writes through the context channel. | `MIG-07`; retain only as an explicit compatibility owner after application routes move to V2. |
| Transport | `rocketmq-transport/src/remoting_server/rocketmq_tokio_server.rs` and its `launch.rs`/`connection_handler.rs` path | `TransportServer<RP>` is the V1 generic production listener and connection route. | Application owners move to `TransportServerV2` in `MIG-02`/`MIG-05`; remove the unused V1 production route in `MIG-07`. |
| Transport | `rocketmq-transport/src/dispatch/authorized_dispatcher.rs` and `legacy_processor_adapter.rs` | The V1 dispatcher is bridged through `LegacyProcessorAdapter`; V2 dispatcher/server owners already exist. | Compatibility-only; do not expand the adapter, and remove it in `MIG-07` after all callers move. |
| Client | `rocketmq-client/src/factory/mq_client_instance.rs`, `implementation/mq_client_api_impl.rs`, and `implementation/mq_client_api_impl/transport.rs` | Production composition constructs `ClientRemotingProcessor` behind the V1 `RemotingClient<PR>` bound. | `MIG-02`; compose the new V2 inbound client owner and retain outbound request behavior. |

Transport already contains `RequestProcessorV2`, `AuthorizedCommandDispatcherV2`,
`TransportServerV2`, response-plan, and deferred-registration infrastructure.
Its V2-only unit and network processors are coverage for that infrastructure,
not pending production leaf migrations. The transport V1 server tests and V1
adapter tests remain compatibility coverage until `MIG-07` removes the adapter.
The reserved V2 extension guard must continue rejecting `Channel` and
`ConnectionHandlerContext`; migration must not bypass it to preserve a V1 side
contract.

## Namesrv

| Production implementation | Kind | Current state | Owner and completion condition |
|---|---|---|---|
| `rocketmq-namesrv/src/processor/default_request_processor.rs` — `DefaultRequestProcessor` | Leaf | `V1`; the context is unused, but broker registration passes the `Channel` to `RouteInfoManager`. | `MIG-02`; migrate before the wrapper/router and replace connection ownership with the narrow session-registration/lifecycle port. |
| `rocketmq-namesrv/src/processor/client_request_processor.rs` — `ClientRequestProcessor` | Leaf | `V1`; both transport parameters are unused. | `MIG-02`; migrate before the wrapper/router. |
| `rocketmq-namesrv/src/processor/cluster_test_request_processor.rs` — `ClusterTestRequestProcessor` | Leaf | `V1`; both transport parameters are unused. | `MIG-02`; preserve the route lookup behavior and integration coverage. |
| `rocketmq-namesrv/src/processor.rs` — `NameServerRequestProcessorWrapper` | Wrapper enum | `V1`; reconstructs child calls with a raw channel and ignores the supplied context. | `MIG-02`; dispatch a typed request to V2 children after every leaf is migrated. |
| `rocketmq-namesrv/src/processor.rs` — `NameServerRequestProcessor` | Aggregate/router | `V1`; auth/admission consumes the context and remote address, then the router forwards channel/context to a selected wrapper. | `MIG-02`; migrate last and retain real request/response, auth-denial, admission, and metrics tests. |
| `rocketmq-namesrv/src/bootstrap.rs` | Production composition | Builds `TransportServer<NameServerRequestProcessor>` and registers route/default wrappers. | `MIG-02`; switch the production owner to `TransportServerV2` after leaves and wrapper are V2. |

The `RouteProcessor` and `BlockingRouteProcessor` fixtures in
`rocketmq-namesrv/src/processor/cluster_test_request_processor/route_lookup.rs`
implement the separate transport `SessionProcessor` test contract. They do not
use `ConnectionHandlerContext`; keep them aligned with the route-lookup tests,
but do not mechanically convert them as V1 production processors.

## Controller

| Production implementation or consumer | Kind | Current state | Owner and completion condition |
|---|---|---|---|
| `rocketmq-controller/src/processor/controller_request_processor.rs` — `ControllerRequestProcessor` | Leaf/command router | `V1`; context is unused, but maintenance auth reads channel identity and heartbeat passes the full channel to its manager. | `MIG-02`; replace those side contracts with typed identity and the narrow session-registration/lifecycle port. |
| `rocketmq-controller/src/processor.rs` — `ControllerRequestProcessorWrapper` | Wrapper enum | `V1`; forwards channel/context to the leaf. | `MIG-02`; migrate after the leaf. |
| `rocketmq-controller/src/processor.rs` — `ControllerServerRequestProcessor` | Aggregate/default router | `V1`; forwards channel/context to the registered wrapper, but the current production manager does not wire this aggregate. | `MIG-02`; either prove and register a V2 purpose for it or classify it as dead compatibility code for deletion in `MIG-07`; retain its unsupported-request test until that decision. |
| `rocketmq-controller/src/controller/controller_manager.rs` | Production composition/context forwarding | Builds `TransportServer<ControllerRequestProcessor>` directly; it does not use `ControllerServerRequestProcessor`. | `MIG-02`; register the V2 leaf/server owner without synthesizing a legacy context and preserve heartbeat/session lifecycle behavior. |

Inline tests that construct `ConnectionHandlerContextWrapper` belong to their
production processor or wrapper and must migrate in the same change.

Namesrv broker registration and Controller heartbeat currently hand a raw
`Channel` to a longer-lived owner. They must not place that capability in a V2
request extension. `MIG-02` must move registration/cleanup to a session owner or
introduce the narrowest typed registration port so disconnect cleanup remains
correct without leaking arbitrary write authority.

## Broker ordinary leaves and session registries

All rows in this section are owned by `MIG-03`. Their inline tests migrate with
the production file. A leaf is complete only when it returns an explicit V2
outcome and no longer receives a raw context solely for dispatch compatibility.

| Production implementation | Current state | Notable migration responsibility |
|---|---|---|
| `rocketmq-broker/src/processor/ack_message_processor.rs` — `AckMessageProcessor<MS>` | `Dual`: V2 leaf + V1 aggregate projection | Uses a typed network/embedded source label; the core receives no transport handle. |
| `rocketmq-broker/src/processor/change_invisible_time_processor.rs` — `ChangeInvisibleTimeProcessor<MS>` | `Dual`: V2 leaf + V1 aggregate projection | Returns a typed response plan without direct response writes. |
| `rocketmq-broker/src/processor/client_manage_processor.rs` — `ClientManageProcessor<MS>` | `Dual`: V2 leaf + isolated V1 wrapper | V2 registration uses canonical `SessionId`; raw `Channel` remains confined to `LegacyClientManageProcessor`. |
| `rocketmq-broker/src/processor/consumer_manage_processor.rs` — `ConsumerManageProcessor<MS>` | `Dual`: V2 leaf + V1 aggregate projection | Uses typed request-source metadata and the live SessionId registry. |
| `rocketmq-broker/src/processor/end_transaction_processor.rs` — `EndTransactionProcessor<TM, MS>` | `Dual`: V2 leaf + V1 aggregate projection | Returns a typed reply/error plan and makes the legacy no-response branch explicit. |
| `rocketmq-broker/src/processor/lite_manager_processor.rs` — `LiteManagerProcessor<MS>` | `Dual`: V2 leaf + V1 aggregate projection | Has no transport-context dependency and preserves protocol behavior. |
| `rocketmq-broker/src/processor/lite_subscription_ctl_processor.rs` — `LiteSubscriptionCtlProcessor<MS>` | `Dual`: V2 leaf + V1 aggregate projection | Removed the unused Channel cache from `LiteSubscriptionRegistry`. |
| `rocketmq-broker/src/processor/maintenance_request_processor.rs` — `MaintenanceRequestProcessor` | `Dual`: V2 leaf + isolated V1 wrapper | V2 authorization uses the authenticated principal; legacy channel authentication stays in the wrapper. |
| `rocketmq-broker/src/processor/peek_message_processor.rs` — `PeekMessageProcessor<MS>` | `Dual`: V2 leaf + V1 aggregate projection | Uses typed source metadata and returns a typed response plan. |
| `rocketmq-broker/src/processor/polling_info_processor.rs` — `PollingInfoProcessor` | `Dual`: V2 leaf + V1 aggregate projection | Embedded callers are labeled explicitly rather than assigned a synthetic address. |
| `rocketmq-broker/src/processor/query_assignment_processor.rs` — `QueryAssignmentProcessor` | `Dual`: V2 leaf + V1 aggregate projection | The core receives only a diagnostic source label and returns a typed reply. |
| `rocketmq-broker/src/processor/recall_message_processor.rs` — `RecallMessageProcessor<MS>` | `Dual`: V2 leaf + V1 aggregate projection | Requires a trusted network origin for the persisted born host and fails closed for embedded calls. |
| `rocketmq-broker/src/client/manager/producer_manager.rs` and `consumer_manager.rs` | Dual registries: V1 `Channel` + V2 `SessionId` | Stable SessionId tables, reverse indexes, ordered typed consumer-member notifications, merged snapshots, and the live Pull client-identity seam coexist with V1 drain tables until Broker cutover. |

The manager migration must not retain `RemotingRequest` or `SessionView` as an
arbitrary send handle. Existing source-text assertions are not completion
evidence and must not be expanded into a fingerprint gate.

## Broker response-heavy leaves

These rows are owned by `MIG-04`. Existing BRK seams are prerequisites and V2
infrastructure; they do not by themselves mark a production leaf complete.

| Production implementation or seam | Current state | Completion condition |
|---|---|---|
| `rocketmq-broker/src/processor/send_message_processor.rs` — `SendMessageProcessor<MS, TS>` | `Dual`: formal V2 leaf delegates to the structured Send seam; the V1 projection remains reachable. | `MIG-04` complete. `MIG-05` switches the production route after lifecycle composition is ready. |
| `rocketmq-broker/src/processor/reply_message_processor.rs` — `ReplyMessageProcessor<MS, TS>` | `Dual`: formal V2 leaf returns owned plans; V1 and the narrow `BrokerReplyPushPort` compatibility sender remain reachable. | `MIG-04` complete. `XCT-02` must provide the stable push sender before `MIG-05` removes the compatibility route. |
| `rocketmq-broker/src/processor/query_message_processor.rs` — `QueryMessageProcessor<S>` | `Dual`: formal V2 leaf owns heap, segmented, and file-region response plans; V1 remains reachable. | `MIG-04` complete. Retain V1 only until the `MIG-05` aggregate cutover. |
| `rocketmq-broker/src/processor/pull_message_processor.rs` — `PullMessageProcessor<MS>` | `Dual`: formal V2 leaf uses the existing Pull deferred service and canonical network `SessionId`; legacy `PullRequestProcessor` remains reachable. | `MIG-04` complete. `BRK-06` owns production service installation and `MIG-05` owns route cutover/drain. |
| `rocketmq-broker/src/processor/pop_message_processor.rs` — `PopMessageProcessor<MS>` | `Dual`: V1 and V2 share one typed initial core; only the V2 leaf registers sealed Pop deferred waits. | `MIG-04` complete. `BRK-06` owns production service installation and `MIG-05` owns route cutover/drain. |
| `rocketmq-broker/src/processor/pop_lite_message_processor.rs` — `PopLiteMessageProcessor<MS>` | `Dual`: formal V2 leaf preserves the client event gate, reservation, deadline, and typed PopLite deferred service; V1 remains reachable. | `MIG-04` complete. `BRK-06` owns production service installation and `MIG-05` owns route cutover/drain. |
| `rocketmq-broker/src/processor/notification_processor.rs` — `NotificationProcessor<MS>` | `Dual`: formal V2 leaf preserves filter-before-claim and born-time normalization through the typed Notification deferred service; V1 remains reachable. | `MIG-04` complete. `BRK-06` owns production service installation and `MIG-05` owns route cutover/drain. |
| `rocketmq-broker/src/processor/default_pull_message_result_handler.rs` | Channel-free typed heap/segment/file-region result handling is connected through the V2 Pull leaf. | `MIG-04` complete; retain the V1 projection only for `MIG-05` drain compatibility. |
| `rocketmq-broker/src/processor/pull_message_result_handler.rs` | The typed response-plan contract is the shared Pull result boundary for the formal V2 leaf. | `MIG-04` complete; preserve the contract through aggregate cutover. |

The V2-only processors under `long_polling/*_deferred/acceptance_tests`,
`processor/fast_failure_dispatch/tests.rs`, the structured Send/Reply tests, and
`processor/response_plan/tests` are already V2 infrastructure tests. Preserve
them as regression coverage for `MIG-04` and the later production cutover.

## Broker aggregate, router, and V1 drain

| Production implementation or compatibility consumer | Current state | Owner and completion condition |
|---|---|---|---|
| `rocketmq-broker/src/processor.rs` — `BrokerProcessorType<MS, TS>` | `V1` generic wrapper enum. | `MIG-05`; route typed requests to completed V2 leaves. |
| `rocketmq-broker/src/processor.rs` — `BrokerRequestProcessor<MS, TS>` | `V1` generic aggregate/router. | `MIG-05`; publish the V2 production route only after leaves and lifecycle ownership are ready. |
| `rocketmq-broker/src/broker_runtime.rs` and `rocketmq-broker/src/broker_runtime/request_pipeline.rs` | Production composition still registers the Broker V1 aggregate. | `MIG-05`; perform the coordinated route cutover and keep old owners alive through bounded drain. |
| `rocketmq-broker/src/processor/admin_broker_processor.rs` — `AdminBrokerProcessor<MS>` | `V1` aggregate with nested context consumers. | `MIG-05` removes its aggregate V1 adapter only after the `MIG-06` nested handlers are typed. |
| `rocketmq-broker/src/long_polling/pull_request.rs` and `long_polling/long_polling_service/pull_request_hold_service.rs` | `Compatibility`; legacy Pull waiter/service stores or forwards `ConnectionHandlerContext`. | `MIG-05` seals acceptance and drains accepted work; `MIG-07` removes the empty legacy path. |
| `rocketmq-broker/src/long_polling/pop_request.rs`, `pop_long_polling_service.rs`, and `pop_lite_long_polling_service.rs` | `Compatibility`; legacy Pop/Notification/PopLite waiter/services store or forward the context. | `MIG-05` seals acceptance and drains accepted work; `MIG-07` removes the empty legacy path. |

The legacy long-poll test processors (`TestPullProcessor`, `FailingProcessor`,
`ControlledProcessor`, `LegacyNotificationProcessor`, and Pop/PopLite
`TestProcessor` fixtures) belong to these compatibility services. Update them
with the owning service during cutover/drain; do not treat them as production
processor inventory.

## Admin nested handlers

`MIG-06` owns every nested handler below. Most accept an unused `_ctx`; delete
that parameter rather than replacing it with an unused whole request. Their
inline context-wrapper tests migrate with the owning handler and
`AdminBrokerProcessor` aggregate.

| Directory | Nested handler files |
|---|---|
| `rocketmq-broker/src/processor/admin_broker_processor/` | `batch_mq_handler.rs`, `broker_config_request_handler.rs`, `broker_epoch_cache_handler.rs`, `broker_stats_handler.rs`, `consumer_request_handler.rs`, `create_acl_request_handler.rs`, `create_user_request_handler.rs`, `delete_acl_request_handler.rs`, `delete_user_request_handler.rs` |
| Same directory | `get_acl_request_handler.rs`, `get_broker_ha_status_handler.rs`, `get_user_request_handler.rs`, `list_acl_request_handler.rs`, `list_users_request_handler.rs`, `message_related_handler.rs`, `notify_broker_role_change_handler.rs`, `notify_min_broker_id_handler.rs`, `offset_request_handler.rs` |
| Same directory | `producer_request_handler.rs`, `reset_master_flusg_offset_handler.rs`, `subscription_group_handler.rs`, `topic_request_handler.rs`, `update_acl_request_handler.rs`, `update_broker_ha_handler.rs`, `update_cold_data_flow_ctr_group_config.rs`, `update_global_white_addrs_config_request_handler.rs`, `update_user_request_handler.rs` |

## Auth and Proxy

| Production implementation or consumer | Current state | Owner and completion condition |
|---|---|---|
| `rocketmq-auth/src/runtime.rs` | Accepts `Any` channel context and probes `ConnectionHandlerContext`, its wrapper, and `Channel` for source IP/channel identity. | `MIG-06`; consume a trusted typed request/session/auth view and fail closed when required provenance is absent. |
| `rocketmq-auth/src/authorization/builder/default_authorization_context_builder.rs` | Repeats the three-way runtime downcast for source IP. | `MIG-06`; accept typed provenance directly and remove the legacy downcasts. |
| `rocketmq-proxy/src/remoting.rs` — `ProxyRequestProcessor<P>` | `V1` generic production processor; derives proxy context from a raw `Channel`. | `MIG-06`; use `dispatch_embedded_v2` with `EmbeddedCaller::BrokerProxy` and consume the local V2 result. |
| `rocketmq-proxy/src/auth.rs` | Probes context alias, wrapper, and `Channel` for source IP. | `MIG-06`; consume only trusted typed provenance and preserve fail-closed tests. |

Proxy remoting inline tests that call the V1 `process_request` entry point belong
to `ProxyRequestProcessor` and migrate in the same change.

## Explicit exclusions

The scan intentionally excludes these lookalikes from the production migration
count while retaining them as review evidence:

- Business methods named `process_request` that do not implement either
  remoting processor trait, including `rocketmq-broker/src/proxy_facade.rs`.
- The transport `SessionProcessor` contract used by local/network test servers;
  it is typed and does not carry `ConnectionHandlerContext`.
- V2-only fixture processors used to exercise response plans, provenance,
  deferred registration, deadlines, lifecycle, and wire delivery.
- Outbound client-side uses of `DefaultRequestProcessor`, such as Broker outer
  API composition; they are consumers of the Transport migration, not Broker
  server leaves.
- Legacy long-poll business traits recorded in the compatibility section; they
  are drain-owned consumers, not top-level `RequestProcessor` implementations.

## Re-scan and update checklist

Run a path-based search when beginning each migration batch, then classify every
new match as production, compatibility, test fixture, or ordinary same-name
code. Do not record file hashes and do not generate a fingerprint baseline.

```powershell
rg -n --glob '*.rs' `
  'ConnectionHandlerContext|ConnectionHandlerContextWrapper|impl.*RequestProcessor|process_request\(' `
  rocketmq-transport/src rocketmq-broker/src rocketmq-client/src `
  rocketmq-namesrv/src rocketmq-controller/src rocketmq-proxy/src rocketmq-auth/src
```

- [x] All seven primary crates are represented.
- [x] Generic production implementations and wrapper/aggregate implementations are recorded.
- [x] All 27 Admin nested handler files are recorded.
- [x] Test fixtures have an owning production processor, compatibility service, or explicit V2-infrastructure classification.
- [x] Ordinary same-name business methods are excluded from mechanical migration.
- [x] No source fingerprint, stored hash, or generated baseline is used.
