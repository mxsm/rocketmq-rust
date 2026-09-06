# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- **docs(store):** Add the missing Apache 2.0 header to the store context compile-test fixture ([#10069](https://github.com/mxsm/rocketmq-rust/issues/10069)).
- **docs(namesrv):** Add the missing Apache 2.0 header to `broker_addr_info.rs` ([#10059](https://github.com/mxsm/rocketmq-rust/issues/10059))
- **docs:** Add missing Apache 2.0 headers to the observability broker metrics example and configuration resolution tests ([#10060](https://github.com/mxsm/rocketmq-rust/issues/10060)).
- **fix(sre-ui):** Pin TypeScript to 5.9.3 so clean installs satisfy the OpenAPI generator and ESLint peer dependencies ([#10033](https://github.com/mxsm/rocketmq-rust/issues/10033))
- **fix(sre):** Include Required Signals manifest owners in capability coverage validation so standalone MCP ownership is accepted while invalid mappings remain rejected ([#10032](https://github.com/mxsm/rocketmq-rust/issues/10032))
- **docs:** Fix Rustdoc generic type markup, broken public links, and references to private items across the workspace ([#10030](https://github.com/mxsm/rocketmq-rust/issues/10030))
- **fix(transport):** Remove unused `RuntimeConfig` imports that break Linux sendfile test and benchmark compilation with warnings denied ([#10026](https://github.com/mxsm/rocketmq-rust/issues/10026))

### Added

- **test(remoting):** Add comprehensive test coverage for `QueryMessageResponseHeader` including integration with RemotingCommand, boundary checks, and error handling
- **feat(tools):** Add `broker` command group with `GetBrokerConfigSubCommand` for querying broker configuration by broker address or cluster, with optional `--keyPattern` regex filtering
- **feat(tools):** Add `CleanExpiredCQSubCommand` under broker commands with broker/cluster/topic target scan, dry-run preview, and cleanup summary reporting
- **feat(tools):** Add `UpdateBrokerConfigSubCommand` under broker commands with single/multi key updates, value validation, broker or cluster targeting, old/new diff display, and rollback on partial failures
- **feat(tools):** Add `CommitLogSetReadAheadSubCommand` under broker commands with Java-compatible mode (`0/1`) plus `--enable/--disable`, optional `--readAheadSize`, broker or cluster targeting, and current/updated read-ahead config display
- **test(remoting):** Add comprehensive test coverage for `GetMaxOffsetRequestHeader` including required fields, optional nested headers, trait implementation methods, and edge cases
- **feat(tools):** Add `SetConsumeModeSubCommand` for setting consumer group consumption mode (PULL/POP) ([#5650](https://github.com/mxsm/rocketmq-rust/issues/5650))
- **feat(tools):** Add `ListAclSubCommand` for ACL enumeration and subject filtering ([#5663](https://github.com/mxsm/rocketmq-rust/issues/5663))
- **feat(tools):** Add `UpdateAclSubCommand` ([#5665](https://github.com/mxsm/rocketmq-rust/issues/5665))
- **feat(tools):** Add `UpdateSubGroupListSubCommand` for batch subscription group updates ([#5652](https://github.com/mxsm/rocketmq-rust/issues/5652))
- **feat(tools):** Add `UpdateSubGroupSubCommand` in update_sub_group_sub_command.rs ([#5653](https://github.com/mxsm/rocketmq-rust/issues/5653))
- **feat(tools):** Add `GetControllerMetadataSubCommand` ([#5624](https://github.com/mxsm/rocketmq-rust/issues/5624))
- **feat(common):** Add `filter_type` module to filter.rs in rocketmq-common crate ([#5454](https://github.com/mxsm/rocketmq-rust/issues/5454))
- **feat(tools):** Add `consumerProgress` command for querying consumer consumption progress and metrics, supporting both detailed single-group and summary all-groups views
- **feat(common):** Add test coverage for `MessageQueueAssignment` struct to in rocketmq-common crate ([#5752](https://github.com/mxsm/rocketmq-rust/issues/5752))

### Changed

- **fix(namesrv):** Default the legacy public-listener compatibility setting to enabled while preserving an
  explicit secure opt-out and the secure-enforced profile rejection ([#10097](https://github.com/mxsm/rocketmq-rust/issues/10097)).
- **docs(admin-cli):** Add missing copyright and Apache License 2.0 headers to three Rust files ([#10070](https://github.com/mxsm/rocketmq-rust/issues/10070)).
- **refactor(store):** Close the `rocketmq-store-local` public error surface behind `StoreError` (E3-3 breaking cut). Append admission, mapped-file acquisition, and namespace verification become caller-owned outcomes (`AppendAdmissionOutcome<T>`, `AcquireTransitionOutcome`, `LifecycleAcquireOutcome<T>`, and the existing `NamespaceTransitionOutcome`); the 28 deterministic contracts are renamed to typed `*Violation` identities; the 36 private leaves become crate-private and are promoted exactly once into `StoreError` at their subsystem owners with typed sources preserved; all 11 `ErrorKind` taxonomies, the `MappedFileResult`/`TransferResult` aliases, the two cfg(test)-only wrappers, and the `HAConnectionError` leaf are deleted. `rocketmq-store` consumers, including the mapped-file builder, HA connections, transfer engines, and the extended timeline, now speak `StoreError` with the established 12-descriptor catalog, operations, and components; no descriptor, operation, or component was added ([#9957](https://github.com/mxsm/rocketmq-rust/issues/9957)).
- **docs(macros):** Record RequestHeaderCodec retirement readiness: V1 and V2 remain deprecated public 1.x derives/adapters, V3 is recommended, and the migration guard reports 152 registered, 152 V3, 0 V2, 0 pending, and 0 production legacy derive uses. Existing compatibility, wire behavior, helper attributes, fixtures, and intentional deprecated-use allows remain. Any future removal requires the complete release cycle, an explicit 2.0 breaking window, and an individual exact reviewed post-freeze approval for each frozen derive; this change creates no approval and does not announce or approve 2.0 ([#9730](https://github.com/mxsm/rocketmq-rust/issues/9730)).
- **docs(filter):** Document `Filter::try_compile` and `FilterCompileError` as the typed filter path while retaining the deprecated 1.x `Filter::compile` and local string `FilterError` facades. Any future deletion requires the complete release cycle, an explicit 2.0 breaking window, and individual reviewed post-freeze approvals for every affected frozen public item; this change does not authorize deletion or announce a 2.0 release ([#9728](https://github.com/mxsm/rocketmq-rust/issues/9728)).
- **refactor(auth):** Establish `rocketmq-security-api` as the direct owner of runtime-neutral security contracts and add the canonical `rocketmq-auth` policy-model names `PolicyDecision`, `PolicyResource`, and `AuthorizationRequest`. The frozen `Decision`, `Resource`, and `RequestContext` names and all twelve maintenance aliases remain source-compatible; `SecurityPrincipal` and `SecurityResource` are deprecated 1.x compatibility re-exports. Any future 2.0 removal remains subject to compatibility, migration, and release gates. This creates no deletion approval, does not announce or approve a 2.0 release, and changes no defaults, wire or Serde representations, error behavior, fail-closed behavior, whitelist/profile behavior, or `AuthorizationHandlerChain` first-success semantics ([#9726](https://github.com/mxsm/rocketmq-rust/issues/9726)).
- **docs(runtime):** Document `RocketMQRuntime` as a deprecated 1.x compatibility API with removal intended only for a future 2.0 source-compatibility boundary, subject to the full release cycle, a 2.0 breaking window, and exact reviewed post-freeze repository-owner approval for every affected frozen public item; this change grants no removal approval and existing public APIs remain available ([#9724](https://github.com/mxsm/rocketmq-rust/issues/9724)).
- **fix(namesrv):** Exclude the lifecycle handle from shutdown-report tracing instrumentation ([#9169](https://github.com/mxsm/rocketmq-rust/issues/9169))
- **refactor(error):** Preserve typed header, JSON, and authorization sources through Controller maintenance handling while retaining the existing error kinds and redacted boundary projections.
- **fix(controller):** Route online consensus membership changes through `apply_membership_change`, which requires a maintenance authorization grant, optimistic membership version, idempotency key, quorum checks, and audit facts. Direct public `add_learner` and `change_membership` mutation methods are no longer exposed; embedders must migrate online changes to the authorized boundary.
- **refactor(proxy):** Remove the hidden `rocketmq_broker::proxy_adapter_compat` re-export surface. Embedded proxy integrations must import model, protocol, store, transport, and observability types from their owning crates and use `ProxyBrokerFacade` for Broker use cases.
- **refactor(namesrv):** Make embedded Controller support opt-in through the `embedded-controller` feature. Applications using `Builder::set_controller_config` or `Builder::set_controller_config_opt` must enable this feature; default builds reject `enableControllerInNamesrv=true` and no longer include Controller, OpenRaft, or RocksDB.
- **chore(broker):** Remove commented-out dead logging code in `pull_request_hold_service.rs` ([#6579](https://github.com/mxsm/rocketmq-rust/issues/6579))
- **refactor(remoting/tools):** Return references from `TopicStatsTable::get_offset_table` and add `into_offset_table`/`get_offset_table_mut` to avoid unnecessary `HashMap` cloning in topic status flows
- **refactor(common):** Rename foundational `MessageTrait` methods to the idiomatic Rust naming: `get_property` to `property` and `get_property_ref` to `property_ref` (other getters like `get_topic`, `get_flag`, etc.. will be renamed in subsequent commits)
- **refactor(client):** Refactor `default_mq_producer::start` in `default_mq_producer.rs` removing repeated `as_mut().unwrap()`([#5576](https://github.com/mxsm/rocketmq-rust/issues/5576))
- **refactor(tui):** Reformat `rocketmq-tui` using `taplo`([#5242](https://github.com/mxsm/rocketmq-rust/issues/5242))
- **refactor(error):** Reformat `Cargo.toml` using `taplo` with entry alignment ([#5232](https://github.com/mxsm/rocketmq-rust/issues/5232))
- **refactor(client):** Change `MQProducer::send_to_queue` return type to `RocketMQResult<Option<SendResult>>` in `mq_producer.rs`, `default_mq_producer.rs` and `transaction_mq_producer.rs` ([#5169](https://github.com/mxsm/rocketmq-rust/issues/5169))
- **refactor(client):** Replace `lazy_static!` with `std::sync::LazyLock` in `trace_view.rs` ([#5092](https://github.com/mxsm/rocketmq-rust/issues/5092))
- **refactor(store):** Replace `lazy_static!` with `std::sync::LazyLock` in `delivery.rs` and remove `lazy_static` dependency from `rocketmq-store` ([#5091](https://github.com/mxsm/rocketmq-rust/issues/5091))
- **refactor(common):** Replace `lazy_static!` with `std::sync::LazyLock` in `name_server_address_utils.rs` ([#5068](https://github.com/mxsm/rocketmq-rust/issues/5068))
- **refactor(common):** Replace `lazy_static!` with `std::sync::LazyLock` in `broker_config.rs` ([#5056](https://github.com/mxsm/rocketmq-rust/issues/5056))
- **refactor(remoting):** Replace `lazy_static!` with `std::sync::LazyLock` in `remoting_command.rs` and remove `lazy_static` dependency from `rocketmq-remoting` ([#5060](https://github.com/mxsm/rocketmq-rust/issues/5060))
- **perf(ArcMut):** Add the `#[inline]` attribute to the `mut_from_ref`, `downgrade`, and `get_inner` methods for `ArcMut`, improving performance ([#2876](https://github.com/mxsm/rocketmq-rust/pull/2876))
- **chore(controller):** Update default controller listen address to use port 60109 ([#5527](https://github.com/mxsm/rocketmq-rust/issues/5527))

### Removed

- **docs:** Remove the Star History chart from `README.md` ([#9337](https://github.com/mxsm/rocketmq-rust/issues/9337))
- **test(store):** Remove obsolete phase3_integration_tests.rs integration test file ([#6649](https://github.com/mxsm/rocketmq-rust/issues/6649))
- **refactor(broker):** Update ProducerManager to use ProducerGroupName type alias for producer group mapping ([#6638](https://github.com/mxsm/rocketmq-rust/issues/6638))
- **test(store):** Remove obsolete io_uring_integration_tests.rs integration test file ([#6620](https://github.com/mxsm/rocketmq-rust/issues/6620))
