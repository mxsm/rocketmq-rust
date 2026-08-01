// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use bytes::Buf;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;
use std::net::TcpListener;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use crate::config::store_runtime_config::StoreRuntimeConfig;
use crate::store_error::StoreComponent;
use crate::store_error::StoreErrorKind;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_model::common::attribute::cleanup_policy::CleanupPolicy;
use rocketmq_model::common::attribute::cq_type::CQType;
use rocketmq_model::common::attribute::topic_attributes::TopicAttributes;
use rocketmq_model::common::attribute::Attribute;
use rocketmq_model::common::boundary_type::BoundaryType;
use rocketmq_model::common::broker::broker_role::BrokerRole;
use rocketmq_model::common::config::TopicConfig;
use rocketmq_model::common::message::message_batch::MessageExtBatch;
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_model::common::message::MessageConst;
use rocketmq_model::common::message::MessageTrait;
use rocketmq_model::common::message::MessageVersion;
use rocketmq_model::common::mix_all;
use rocketmq_model::common::running::running_stats::RunningStats;
use rocketmq_model::common::topic::TopicValidator;
use rocketmq_model::utils::crc32_utils::crc32;
use rocketmq_runtime::ShutdownDeadline;
#[cfg(feature = "tieredstore")]
use rocketmq_tieredstore::fetcher::TieredGetMessageStatus;
#[cfg(feature = "tieredstore")]
use rocketmq_tieredstore::TieredDispatchRequest;
#[cfg(feature = "tieredstore")]
use rocketmq_tieredstore::TieredDispatcher;
#[cfg(feature = "tieredstore")]
use rocketmq_tieredstore::TieredLifecycle;
#[cfg(feature = "tieredstore")]
use rocketmq_tieredstore::TieredMessageFetcher;
#[cfg(feature = "tieredstore")]
use rocketmq_tieredstore::TieredStorageLevel;
#[cfg(feature = "tieredstore")]
use rocketmq_tieredstore::TieredStoreConfig;
use tempfile::tempdir;
use tokio_util::sync::CancellationToken;

use super::run_blocking_scheduled_task;
use super::BackgroundIndexRebuildService;
use super::BackgroundIndexRebuildState;
use super::CleanCommitLogService;
use super::CommitLogDispatcherDefault;
use super::DiskCleanDecision;
use super::LocalFileMessageStore;
use super::ReputMessageService;
use super::ReputMessageServiceInner;
use crate::base::backend_ops::BackendOps;
use crate::base::commit_log_dispatcher::CommitLogDispatcher;
use crate::base::dispatch_request::DispatchRequest;
use crate::base::message_arriving_listener::MessageArrivingListener;
use crate::base::message_result::PutMessageResult;
use crate::base::message_status_enum::GetMessageStatus;
use crate::base::message_status_enum::PutMessageStatus;
use crate::base::store_checkpoint::StoreCheckpoint;
use crate::base::store_enum::StoreType;
use crate::config::flush_disk_type::FlushDiskType;
use crate::config::message_store_config::LinuxMemoryLockMode;
use crate::config::message_store_config::MessageStoreConfig;
use crate::config::message_store_config::RecoveryMode;
use crate::filter::MessageFilter;
use crate::hook::put_message_hook::PutMessageHook;
use crate::hook::send_message_back_hook::SendMessageBackHook;
use crate::kv::compaction_service::CompactionService;
use crate::log_file::mapped_file::default_mapped_file_impl::DefaultMappedFile;
use crate::log_file::mapped_file::MappedFile;
use crate::message_encoder::message_ext_encoder::MessageExtEncoder;
use crate::message_store::recovery::RecoveryCrcPolicy;
use crate::message_store::recovery::RecoveryExit;
use crate::message_store::recovery::RecoveryIndexRepairPolicy;
use crate::message_store::recovery::RecoveryPhase;
use crate::message_store::recovery::RecoveryPhaseStatus;
use crate::message_store::recovery::RecoveryReportStats;
use crate::queue::consume_queue::ConsumeQueueTrait;
use crate::queue::consume_queue_store::ConsumeQueueStoreTrait;
use crate::store_error::StoreError;
use crate::store_path_config_helper::get_store_checkpoint;
use rocketmq_store_local::message_store::lifecycle::LocalStoreState;

fn local_store_production_source() -> String {
    [
        include_str!("../../../src/message_store/local_file_message_store.rs"),
        include_str!("../../../src/message_store/local_file_message_store/composition.rs"),
        include_str!("../../../src/message_store/local_file_message_store/read_path.rs"),
        include_str!("../../../src/message_store/local_file_message_store/write_path.rs"),
        include_str!("../../../src/message_store/local_file_message_store/dispatch.rs"),
        include_str!("../../../src/message_store/local_file_message_store/recovery.rs"),
        include_str!("../../../src/message_store/local_file_message_store/health.rs"),
        include_str!("../../../src/message_store/local_file_message_store/lifecycle.rs"),
        "#[cfg(test)]\nmod tests",
    ]
    .join("\n")
    .replace("\r\n", "\n")
}

fn commit_log_production_source() -> String {
    [
        include_str!("../../../src/log_file/commit_log.rs"),
        include_str!("../../../src/log_file/commit_log/context.rs"),
        include_str!("../../../src/log_file/commit_log/handles.rs"),
        include_str!("../../../src/log_file/commit_log/append_sequencer.rs"),
    ]
    .into_iter()
    .map(|source| {
        let source = source.replace("\r\n", "\n");
        source
            .split_once("#[cfg(test)]\nmod tests")
            .map_or(source.as_str(), |(production, _)| production)
            .to_string()
    })
    .collect::<Vec<_>>()
    .join("\n")
}

fn new_test_store(temp_dir: &tempfile::TempDir) -> LocalFileMessageStore {
    new_configured_test_store(temp_dir, MessageStoreConfig::default())
}

fn allocate_local_test_port() -> u16 {
    TcpListener::bind(("127.0.0.1", 0))
        .expect("allocate local test port")
        .local_addr()
        .expect("read local test port")
        .port()
}

#[tokio::test]
async fn scheduled_blocking_task_reports_completion_and_join_failure() {
    let runtime_scope = crate::runtime::test_scope("scheduled-blocking-task-test");
    let completed = Arc::new(AtomicBool::new(false));
    let completed_task = Arc::clone(&completed);

    assert!(
        run_blocking_scheduled_task(&runtime_scope, "test scheduled success", move || {
            completed_task.store(true, Ordering::Release);
        })
        .await
    );
    assert!(completed.load(Ordering::Acquire));

    assert!(
        !run_blocking_scheduled_task(&runtime_scope, "test scheduled panic", || panic!(
            "scheduled task panic"
        ))
        .await,
        "panic in a scheduled blocking task should stop that scheduled loop"
    );
}

fn new_configured_test_store(
    temp_dir: &tempfile::TempDir,
    message_store_config: MessageStoreConfig,
) -> LocalFileMessageStore {
    new_owned_test_store_with_broker(temp_dir, message_store_config, StoreRuntimeConfig::default())
}

fn new_owned_test_store_with_broker(
    temp_dir: &tempfile::TempDir,
    mut message_store_config: MessageStoreConfig,
    broker_config: StoreRuntimeConfig,
) -> LocalFileMessageStore {
    message_store_config.store_path_root_dir = temp_dir.path().to_string_lossy().to_string().into();
    message_store_config.timer_wheel_enable = false;
    let mut store = LocalFileMessageStore::new(
        Arc::new(message_store_config),
        Arc::new(broker_config),
        Arc::new(DashMap::<CheetahString, Arc<TopicConfig>>::new()),
        None,
        false,
        crate::runtime::test_service_context("local-file-store-test"),
    );
    store
        .wire_owned_root_dependencies()
        .expect("LocalFile tests should wire owned Store capabilities");
    store
}

fn new_configured_test_store_with_broker(
    temp_dir: &tempfile::TempDir,
    mut message_store_config: MessageStoreConfig,
    broker_config: StoreRuntimeConfig,
) -> LocalFileMessageStore {
    message_store_config.store_path_root_dir = temp_dir.path().to_string_lossy().to_string().into();
    let mut store = LocalFileMessageStore::new(
        Arc::new(message_store_config),
        Arc::new(broker_config),
        Arc::new(DashMap::<CheetahString, Arc<TopicConfig>>::new()),
        None,
        false,
        crate::runtime::test_service_context("configured-local-file-store-test"),
    );
    assert!(store.get_timer_message_store().is_none());
    store
        .wire_owned_root_dependencies()
        .expect("LocalFile tests should wire owned Store capabilities");
    store
}

fn new_controller_test_store(
    temp_dir: &tempfile::TempDir,
    mut message_store_config: MessageStoreConfig,
) -> LocalFileMessageStore {
    message_store_config.enable_controller_mode = true;
    new_owned_test_store_with_broker(
        temp_dir,
        message_store_config,
        StoreRuntimeConfig {
            enable_controller_mode: true,
            ..StoreRuntimeConfig::default()
        },
    )
}

#[test]
fn commitlog_uses_allocate_mapped_file_service_for_file_creation() {
    let temp_dir = tempdir().unwrap();
    let store = new_configured_test_store(&temp_dir, MessageStoreConfig::default());

    assert!(store.commit_log.has_allocate_mapped_file_service());
}

#[test]
fn unlock_mapped_file_is_safe_for_unlocked_and_repeated_calls() {
    let store_dir = tempdir().unwrap();
    let mapped_file_dir = tempdir().unwrap();
    let store = new_configured_test_store(&store_dir, MessageStoreConfig::default());
    let mapped_file_path = mapped_file_dir.path().join("00000000000000000000");
    let mapped_file = DefaultMappedFile::new(CheetahString::from(mapped_file_path.to_string_lossy().as_ref()), 4096);

    store.unlock_mapped_file(&mapped_file);
    store.unlock_mapped_file(&mapped_file);
}

fn new_unwired_test_store(temp_dir: &tempfile::TempDir) -> LocalFileMessageStore {
    let message_store_config = MessageStoreConfig {
        store_path_root_dir: temp_dir.path().to_string_lossy().to_string().into(),
        ..MessageStoreConfig::default()
    };
    LocalFileMessageStore::new(
        Arc::new(message_store_config),
        Arc::new(StoreRuntimeConfig::default()),
        Arc::new(DashMap::<CheetahString, Arc<TopicConfig>>::new()),
        None,
        false,
        crate::runtime::test_service_context("unwired-local-file-store-test"),
    )
}

#[tokio::test]
async fn recovery_without_owned_root_wiring_returns_without_panicking() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_unwired_test_store(&temp_dir);

    store.recover_normally(0).await;
    store.recover_abnormally(0).await;
}

#[tokio::test]
async fn init_without_root_dependency_wiring_fails_closed() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_unwired_test_store(&temp_dir);

    let error = store.init().await.expect_err("unwired Local store must not initialize");

    assert_eq!(error.kind(), StoreErrorKind::Internal);
    assert_eq!(
        error.detail(),
        Some("message store root dependencies are not wired; call wire_owned_root_dependencies before init")
    );
}

fn new_owned_wiring_test_store(
    temp_dir: &tempfile::TempDir,
    mut message_store_config: MessageStoreConfig,
    broker_config: StoreRuntimeConfig,
) -> LocalFileMessageStore {
    message_store_config.store_path_root_dir = temp_dir.path().to_string_lossy().to_string().into();
    LocalFileMessageStore::new(
        Arc::new(message_store_config),
        Arc::new(broker_config),
        Arc::new(DashMap::<CheetahString, Arc<TopicConfig>>::new()),
        None,
        false,
        crate::runtime::test_service_context("owned-wiring-local-file-store-test"),
    )
}

#[test]
fn owned_root_wiring_constructs_ha_from_replica_capability() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_owned_wiring_test_store(
        &temp_dir,
        MessageStoreConfig {
            timer_wheel_enable: false,
            ..MessageStoreConfig::default()
        },
        StoreRuntimeConfig::default(),
    );

    store
        .wire_owned_root_dependencies()
        .expect("HA can use an independently owned replica capability");

    assert!(store.root_dependencies_wired);
    assert!(store.pending_ha_service.is_some());
}

#[test]
fn owned_root_wiring_constructs_timer_from_store_capabilities() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_owned_wiring_test_store(
        &temp_dir,
        MessageStoreConfig {
            duplication_enable: true,
            timer_wheel_enable: true,
            ..MessageStoreConfig::default()
        },
        StoreRuntimeConfig {
            duplication_enable: true,
            ..StoreRuntimeConfig::default()
        },
    );

    store
        .wire_owned_root_dependencies()
        .expect("Timer can use independently owned Store capabilities");

    assert!(store.root_dependencies_wired);
    assert!(store.get_timer_message_store().is_some());
}

#[test]
fn owned_root_wiring_allows_duplication_without_timer() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_owned_wiring_test_store(
        &temp_dir,
        MessageStoreConfig {
            duplication_enable: true,
            timer_wheel_enable: false,
            ..MessageStoreConfig::default()
        },
        StoreRuntimeConfig {
            duplication_enable: true,
            ..StoreRuntimeConfig::default()
        },
    );

    store
        .wire_owned_root_dependencies()
        .expect("duplication without timer can use an exclusively owned store");

    assert!(store.root_dependencies_wired);
}

#[tokio::test]
async fn ha_replica_handle_rejects_append_after_shutdown() {
    let temp_dir = tempdir().unwrap();
    let store = new_owned_wiring_test_store(
        &temp_dir,
        MessageStoreConfig {
            duplication_enable: true,
            timer_wheel_enable: false,
            ..MessageStoreConfig::default()
        },
        StoreRuntimeConfig {
            duplication_enable: true,
            ..StoreRuntimeConfig::default()
        },
    );
    let handle = store.ha_replica_store_handle();
    store.shutdown.store(true, Ordering::Release);

    let appended = handle
        .append_replica_data(0, &[1, 2, 3, 4], 0, 4)
        .await
        .expect("shutdown replica append should fail closed without an error");

    assert!(!appended);
    assert_eq!(handle.get_max_phy_offset(), 0);
}

#[tokio::test]
async fn ha_replica_handle_shares_transfer_and_replication_progress() {
    let temp_dir = tempdir().unwrap();
    let store = new_owned_wiring_test_store(
        &temp_dir,
        MessageStoreConfig {
            duplication_enable: true,
            timer_wheel_enable: false,
            ..MessageStoreConfig::default()
        },
        StoreRuntimeConfig {
            duplication_enable: true,
            ..StoreRuntimeConfig::default()
        },
    );
    let handle = store.ha_replica_store_handle();

    assert!(handle
        .append_replica_data(0, &[1, 2, 3, 4], 0, 4)
        .await
        .expect("append replica data"));
    handle.publish_confirm_offset(4);
    store.master_flushed_offset.store(3, Ordering::SeqCst);
    store.alive_replica_num_in_group.store(2, Ordering::SeqCst);

    let segments = handle
        .select_segments(0, 4, false)
        .expect("select replica transfer segment");
    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0].segment().global_offset, 0);
    assert_eq!(handle.get_confirm_offset(), 4);
    assert_eq!(handle.get_confirm_offset_directly(), 4);
    assert_eq!(handle.get_master_flushed_offset(), 3);
    assert_eq!(handle.get_alive_replica_num_in_group(), 2);
    assert!(!handle.is_shutdown());
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RecordedArrival {
    topic: CheetahString,
    queue_id: i32,
    logic_offset: i64,
    filter_bit_map: Option<Vec<u8>>,
}

struct RecordingArrivingListener {
    arrivals: Arc<std::sync::Mutex<Vec<RecordedArrival>>>,
}

impl MessageArrivingListener for RecordingArrivingListener {
    fn arriving(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        logic_offset: i64,
        _tags_code: Option<i64>,
        _msg_store_time: i64,
        filter_bit_map: Option<Vec<u8>>,
        _properties: Option<&HashMap<CheetahString, CheetahString>>,
    ) {
        self.arrivals.lock().unwrap().push(RecordedArrival {
            topic: topic.clone(),
            queue_id,
            logic_offset,
            filter_bit_map,
        });
    }
}

fn recording_arriving_listener() -> (
    super::MessageArrivingListenerHandle,
    Arc<std::sync::Mutex<Vec<RecordedArrival>>>,
) {
    let arrivals = Arc::new(std::sync::Mutex::new(Vec::new()));
    let listener: Box<dyn MessageArrivingListener + Sync + Send + 'static> = Box::new(RecordingArrivingListener {
        arrivals: Arc::clone(&arrivals),
    });
    (Arc::new(listener), arrivals)
}

fn install_recording_arriving_listener(
    store: &mut LocalFileMessageStore,
) -> Arc<std::sync::Mutex<Vec<RecordedArrival>>> {
    let (listener, arrivals) = recording_arriving_listener();
    store.set_message_arriving_listener(Some(listener));
    arrivals
}

fn reput_inner_for_store(store: &LocalFileMessageStore) -> ReputMessageServiceInner {
    let policy = store.composition.reput();
    ReputMessageServiceInner {
        reput_from_offset: Arc::new(AtomicI64::new(0)),
        commit_log: store.commit_log.read_handle(),
        policy,
        dispatcher: store.dispatcher.handle(),
        notify_message_arrive_in_batch: false,
        runtime_context: store.reput_runtime_context(),
    }
}

#[test]
fn reput_background_boundary_uses_owned_store_capabilities() {
    let temp_dir = tempdir().unwrap();
    let store = new_configured_test_store(&temp_dir, MessageStoreConfig::default());

    let inner = reput_inner_for_store(&store);

    assert_eq!(inner.commit_log.get_max_offset(), store.get_max_phy_offset());
    drop(inner);
    assert_eq!(store.get_max_phy_offset(), 0);
}

#[test]
fn reput_message_arrival_capability_observes_replace_and_clear() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_configured_test_store(
        &temp_dir,
        MessageStoreConfig {
            enable_lmq: true,
            ..MessageStoreConfig::default()
        },
    );
    let first_arrivals = install_recording_arriving_listener(&mut store);
    let runtime_context = store.reput_runtime_context();
    let (replacement, replacement_arrivals) = recording_arriving_listener();
    store.set_message_arriving_listener(Some(replacement));
    let mut properties = HashMap::new();
    properties.insert(
        CheetahString::from_static_str(MessageConst::PROPERTY_INNER_MULTI_DISPATCH),
        CheetahString::from_static_str("%LMQ%replacement"),
    );
    properties.insert(
        CheetahString::from_static_str(MessageConst::PROPERTY_INNER_MULTI_QUEUE_OFFSET),
        CheetahString::from_static_str("2"),
    );
    let mut dispatch_request = DispatchRequest {
        topic: CheetahString::from_static_str("message-arrival-capability-topic"),
        queue_id: 1,
        properties_map: Some(properties),
        ..DispatchRequest::default()
    };

    runtime_context.notify_message_arrive_for_multi_queue(&mut dispatch_request);

    assert!(first_arrivals.lock().unwrap().is_empty());
    assert_eq!(replacement_arrivals.lock().unwrap().len(), 1);

    store.set_message_arriving_listener(None);
    runtime_context.notify_message_arrive_for_multi_queue(&mut dispatch_request);
    assert_eq!(replacement_arrivals.lock().unwrap().len(), 1);
}

#[test]
fn local_background_source_contract_excludes_direct_store_owner() {
    let source = local_store_production_source();
    let reput = source
        .split_once("impl ReputMessageService {")
        .and_then(|(_, source)| source.split_once("async fn run_blocking_scheduled_task"))
        .map(|(source, _)| source)
        .expect("ReputMessageService production section");
    assert!(!reput.contains("ArcMut<LocalFileMessageStore>"));

    let scheduled = source
        .split_once("fn add_schedule_task(&mut self)")
        .and_then(|(_, source)| source.split_once("async fn shutdown_schedule_tasks"))
        .map(|(source, _)| source)
        .expect("scheduled task production section");
    assert!(!scheduled.contains("message_store.clone()"));
}

#[test]
fn clean_commit_log_source_contract_uses_narrow_cleanup_capability() {
    let source = local_store_production_source();
    let cleanup = source
        .split_once("impl CleanCommitLogService {")
        .and_then(|(_, source)| source.split_once("struct CleanConsumeQueueService"))
        .map(|(source, _)| source)
        .expect("CleanCommitLogService production section");
    assert!(!cleanup.contains("mut_from_ref"));
    assert!(cleanup.contains("commit_log: CommitLogCleanupHandle"));
    assert!(!cleanup.contains("ArcMut<CommitLog>"));

    let commit_log_source = commit_log_production_source();
    assert!(commit_log_source.contains("pub(crate) struct CommitLogCleanupHandle"));
    assert!(commit_log_source.contains("pub(crate) fn cleanup_handle(&self) -> CommitLogCleanupHandle"));
    assert!(commit_log_source.contains("pub fn delete_expired_files_by_time_before(\n        &mut self,"));
    assert!(commit_log_source.contains("pub fn retry_delete_first_file(&mut self,"));
    assert!(!commit_log_source.contains("get_mapped_files().store"));

    let mapped_file_queue_source =
        include_str!("../../../src/consume_queue/mapped_file_queue.rs").replace("\r\n", "\n");
    let production = mapped_file_queue_source
        .rsplit_once("#[cfg(test)]")
        .map(|(source, _)| source)
        .expect("MappedFileQueue production section");
    assert!(production.contains("pub(crate) struct MappedFileQueueCleanupHandle"));
    assert!(production.contains("pub fn delete_expired_file_by_time_before(\n        &self,"));
    assert!(production.contains("pub fn retry_delete_first_file(&self,"));
    assert!(production.contains("self.mapped_files.rcu(|current| update(current.as_slice()))"));
    assert!(production.contains("pub(crate) fn replace_mapped_files_exclusive(&mut self,"));
    assert_eq!(production.matches(".store(").count(), 1);
}

#[test]
fn dispatcher_handle_observes_ordered_registry_publication() {
    struct RecordingDispatcher {
        id: i32,
        calls: Arc<StdMutex<Vec<i32>>>,
    }

    impl CommitLogDispatcher for RecordingDispatcher {
        fn dispatch(&self, _dispatch_request: &mut DispatchRequest) {
            self.calls.lock().unwrap().push(self.id);
        }
    }

    let calls = Arc::new(StdMutex::new(Vec::new()));
    let mut dispatcher = CommitLogDispatcherDefault::new();
    let handle = dispatcher.handle();
    dispatcher.add_dispatcher(Arc::new(RecordingDispatcher {
        id: 2,
        calls: Arc::clone(&calls),
    }));
    dispatcher.add_first_dispatcher(Arc::new(RecordingDispatcher {
        id: 1,
        calls: Arc::clone(&calls),
    }));

    handle.dispatch(&mut DispatchRequest::default());

    assert_eq!(*calls.lock().unwrap(), vec![1, 2]);
}

#[test]
fn commit_log_child_source_contract_uses_narrow_dispatch_and_owned_flush_manager() {
    let source = local_store_production_source().replace("\r\n", "\n");
    let production = source
        .split_once("#[cfg(test)]\nmod tests")
        .map(|(source, _)| source)
        .expect("LocalFileMessageStore production section");
    assert!(!production.contains("ArcMut<CommitLogDispatcherDefault>"));
    assert!(production.contains("dispatcher: CommitLogDispatcherDefault"));
    assert!(production.contains("dispatcher: CommitLogDispatchHandle"));
    assert!(production.contains("published: Arc<ArcSwap<Vec<Arc<dyn CommitLogDispatcher>>>>"));

    let commit_log_source = commit_log_production_source();
    assert!(commit_log_source.contains("dispatcher: super::CommitLogDispatchHandle"));
    assert!(commit_log_source.contains("flush_manager: super::DefaultFlushManager"));
    assert!(!commit_log_source.contains("ArcMut<super::CommitLogDispatcherDefault>"));
    assert!(!commit_log_source.contains("ArcMut<super::DefaultFlushManager>"));
    assert!(!commit_log_source.contains("ArcMut::new(DefaultFlushManager::new"));
}

#[test]
fn commit_log_store_context_source_contract_removes_local_root_back_reference() {
    let local_source = local_store_production_source().replace("\r\n", "\n");
    let local_production = local_source
        .split_once("#[cfg(test)]\nmod tests")
        .map(|(source, _)| source)
        .expect("LocalFileMessageStore production section");
    assert!(local_production.contains("delay_level_table: Arc<BTreeMap<i32"));
    assert!(!local_production.contains("delay_level_table: ArcMut<BTreeMap<i32"));
    assert!(!local_production.contains("set_local_file_message_store"));

    let commit_log_production = commit_log_production_source();
    assert!(commit_log_production.contains("pub(crate) struct CommitLogStoreContext"));
    assert!(commit_log_production.contains("ha_service: Arc<ArcSwapOption<GeneralHAService>>"));
    assert!(commit_log_production.contains("store_context: super::CommitLogStoreContext"));
    assert!(commit_log_production.contains("$commit_log.consume_queue_store.truncate_dirty(process_offset)"));
    assert!(!commit_log_production.contains("use rocketmq_rust::ArcMut;"));
    assert!(!commit_log_production.contains("pub(super) local_file_message_store:"));
    assert!(!commit_log_production.contains("message_store: ArcMut<LocalFileMessageStore>"));
}

#[test]
fn commit_log_long_lived_readers_use_narrow_capability() {
    let local_source = local_store_production_source().replace("\r\n", "\n");
    let local_production = local_source
        .split_once("#[cfg(test)]\nmod tests")
        .map(|(source, _)| source)
        .expect("LocalFileMessageStore production section");
    assert_eq!(local_production.matches("ArcMut<CommitLog>").count(), 0);
    assert!(local_production.contains("commit_log: CommitLog,"));
    assert!(local_production.contains("commit_log: CommitLogReadHandle"));
    assert!(local_production.contains("self.commit_log.read_handle()"));
    assert!(!local_production.contains("self_check_commit_log = self.commit_log.clone()"));

    let commit_log_production = commit_log_production_source();
    assert!(commit_log_production.contains("pub(crate) struct CommitLogReadHandle"));
    assert!(commit_log_production.contains("mapped_file_queue: MappedFileQueueReadHandle"));
    assert!(commit_log_production.contains("runtime_state: Arc<CommitLogRuntimeState>"));

    let mapped_file_queue_source =
        include_str!("../../../src/consume_queue/mapped_file_queue.rs").replace("\r\n", "\n");
    assert!(mapped_file_queue_source.contains("pub(crate) struct MappedFileQueueReadHandle"));
}

#[test]
fn commit_log_maintenance_avoids_shared_reference_mutation() {
    let local_source = local_store_production_source().replace("\r\n", "\n");
    let local_production = local_source
        .split_once("#[cfg(test)]\nmod tests")
        .map(|(source, _)| source)
        .expect("LocalFileMessageStore production section");
    assert!(!local_production.contains(".mut_from_ref()"));
    assert!(local_production.contains("self.commit_log.reset_offset(phy_offset)"));
    assert!(local_production.contains("self.commit_log.truncate_dirty_files(offset_to_truncate)"));
    assert!(local_production.contains("self.commit_log.get_last_mapped_file(start_offset)"));

    let commit_log_source = include_str!("../../../src/log_file/commit_log.rs").replace("\r\n", "\n");
    assert!(commit_log_source.contains("pub fn reset_offset(&self, offset: i64)"));
    assert!(commit_log_source.contains("pub fn truncate_dirty_files(&self, offset_to_truncate: i64)"));
    assert!(commit_log_source.contains("pub fn get_last_mapped_file(&self, start_offset: i64)"));

    let queue_source = include_str!("../../../src/consume_queue/mapped_file_queue.rs").replace("\r\n", "\n");
    assert!(queue_source.contains("pub fn reset_offset(&self, offset: i64)"));
    assert!(queue_source.contains("pub fn truncate_dirty_files(&self, offset: i64)"));
    assert!(queue_source.contains("pub fn try_create_mapped_file(&self, create_offset: u64)"));
    assert!(queue_source.matches("self.runtime_state.commit_lock().lock()").count() >= 3);
}

#[test]
fn commit_log_owner_remains_exclusive_to_the_local_store() {
    let source = local_store_production_source().replace("\r\n", "\n");
    let production = source
        .split_once("#[cfg(test)]\nmod tests")
        .map(|(source, _)| source)
        .expect("LocalFileMessageStore production section");

    assert!(production.contains("commit_log: CommitLog,"));
    assert!(!production.contains("commit_log: ArcMut<CommitLog>"));
    assert!(!production.contains("ArcMut::new(commit_log)"));
    assert!(production.contains("let commit_log_read = commit_log.read_handle();"));
    assert!(production.contains("let commit_log_cleanup = commit_log.cleanup_handle();"));
    assert!(production.contains("commit_log,"));
}

#[test]
fn local_store_owned_wiring_does_not_retain_its_complete_root_handle() {
    let source = local_store_production_source().replace("\r\n", "\n");
    let production = source
        .split_once("#[cfg(test)]\nmod tests")
        .map(|(source, _)| source)
        .expect("LocalFileMessageStore production section");
    let wiring = production
        .split_once("pub fn wire_owned_root_dependencies")
        .and_then(|(_, source)| source.split_once("pub fn delay_level_table"))
        .map(|(source, _)| source)
        .expect("Local root wiring function");
    let init = production
        .split_once("pub(super) async fn initialize_store(&mut self)")
        .and_then(|(_, source)| source.split_once("pub(super) async fn shutdown_store_gracefully(&mut self)"))
        .map(|(source, _)| source)
        .expect("Local init function");

    assert!(production.contains("root_dependencies_wired: bool,"));
    assert!(!production.contains("message_store_arc: Option<ArcMut<LocalFileMessageStore>>"));
    assert!(!production.contains("fn message_store_arc_or_error"));
    assert!(!production.contains("self.message_store_arc"));
    assert!(wiring.contains("self.consume_queue_store.set_context(self.consume_queue_context());"));
    assert!(wiring.contains("TimerMessageStore::new_with_store_context("));
    assert!(wiring.contains("self.timer_store_context()"));
    assert!(wiring.contains("DefaultHAService::new_with_store_metrics("));
    assert!(wiring.contains("self.runtime_scope.clone()"));
    assert!(wiring.contains("self.telemetry.store().clone()"));
    assert!(wiring.contains("PendingHAService::AutoSwitch"));
    assert!(wiring.contains("PendingHAService::Default"));
    assert!(wiring.contains("PendingHAService::Default(Box::new("));
    assert!(wiring.contains("self.root_dependencies_wired = true;"));
    assert!(!wiring.contains("ArcMut"));
    assert!(!wiring.contains("message_store_arc"));
    assert!(!init.contains("DefaultHAService::new"));
    assert_eq!(init.matches("ArcMut::new(service)").count(), 0);
    assert_eq!(init.matches("ArcMut::new(*service)").count(), 0);
    assert!(init.contains("GeneralHAService::new_with_default_ha_service(*service)"));
    assert!(init.contains("GeneralHAService::new_with_auto_switch_ha_service(service)"));
    assert!(init.contains("self.ensure_root_dependencies_wired(\"init\")?;"));
    assert!(init.contains("self.pending_ha_service.take()"));
    assert!(init.contains("let _ = ha_service.init();"));
    assert!(init.contains("self.ha_service = Some(ha_service);"));
}

#[test]
fn owned_root_wiring_scheduled_lifecycle_probe_has_no_shared_owner() {
    let lib_source = include_str!("../../../src/lib.rs").replace("\r\n", "\n");
    let probe = lib_source
        .split_once("pub async fn run_store_local_file_scheduled_lifecycle_probe")
        .and_then(|(_, source)| source.split_once("#[cfg(feature = \"rocksdb_store\")]"))
        .map(|(source, _)| source)
        .expect("local file scheduled lifecycle probe");
    assert!(probe.contains("LocalFileMessageStore::new("));
    assert!(probe.contains("wire_owned_root_dependencies()"));
    assert!(probe.contains("duplication_enable: true"));
    assert!(!probe.contains("ArcMut"));
    assert!(!probe.contains("set_message_store_arc"));
    assert!(!lib_source.contains("local_file_shared_owner"));
}

#[tokio::test]
async fn reput_shutdown_wait_uses_dispatch_progress_notification() {
    let temp_dir = tempdir().unwrap();
    let store = new_configured_test_store(&temp_dir, MessageStoreConfig::default());
    let mut inner = reput_inner_for_store(&store);
    inner.set_reput_from_offset(-1);

    let service = ReputMessageService {
        shutdown_token: CancellationToken::new(),
        new_message_notify: Arc::new(tokio::sync::Notify::new()),
        dispatch_progress_notify: Arc::new(tokio::sync::Notify::new()),
        pending_messages: Arc::new(AtomicI64::new(0)),
        inflight_dispatch_batches: Arc::new(AtomicU64::new(0)),
        reput_from_offset: None,
        dispatch_tx: None,
        inner: None,
        task_group: None,
    };
    let reput_from_offset = inner.reput_from_offset.clone();
    let dispatch_progress_notify = service.dispatch_progress_notify.clone();

    let (dispatched, _) = tokio::time::timeout(Duration::from_millis(100), async {
        tokio::join!(
            service.wait_until_commit_log_dispatched(&inner, Duration::from_secs(5)),
            async move {
                tokio::task::yield_now().await;
                reput_from_offset.store(0, Ordering::Release);
                dispatch_progress_notify.notify_waiters();
            }
        )
    })
    .await
    .expect("dispatch progress notification should wake shutdown wait");

    assert!(dispatched);
}

#[tokio::test]
async fn release_checkpoint_waits_for_inflight_dispatch_batch_completion() {
    let temp_dir = tempdir().unwrap();
    let store = new_configured_test_store(&temp_dir, MessageStoreConfig::default());
    let inflight = Arc::new(AtomicU64::new(1));
    let progress = Arc::new(tokio::sync::Notify::new());
    let service = ReputMessageService {
        shutdown_token: CancellationToken::new(),
        new_message_notify: Arc::new(tokio::sync::Notify::new()),
        dispatch_progress_notify: Arc::clone(&progress),
        pending_messages: Arc::new(AtomicI64::new(0)),
        inflight_dispatch_batches: Arc::clone(&inflight),
        reput_from_offset: None,
        dispatch_tx: None,
        inner: Some(reput_inner_for_store(&store)),
        task_group: None,
    };

    let (drained, ()) = tokio::join!(
        service.wait_until_release_checkpoint_drained(ShutdownDeadline::after(Duration::from_secs(1))),
        async move {
            tokio::task::yield_now().await;
            inflight.store(0, Ordering::Release);
            progress.notify_waiters();
        }
    );

    assert!(drained);
}

#[tokio::test]
async fn reput_shutdown_releases_the_runtime_context() {
    let temp_dir = tempdir().unwrap();
    let store = new_configured_test_store(&temp_dir, MessageStoreConfig::default());
    let reput_from_offset = Arc::new(AtomicI64::new(0));
    let mut service = ReputMessageService {
        shutdown_token: CancellationToken::new(),
        new_message_notify: Arc::new(tokio::sync::Notify::new()),
        dispatch_progress_notify: Arc::new(tokio::sync::Notify::new()),
        pending_messages: Arc::new(AtomicI64::new(0)),
        inflight_dispatch_batches: Arc::new(AtomicU64::new(0)),
        reput_from_offset: Some(reput_from_offset.clone()),
        dispatch_tx: None,
        inner: Some(ReputMessageServiceInner {
            reput_from_offset,
            ..reput_inner_for_store(&store)
        }),
        task_group: None,
    };

    service.shutdown().await;

    assert!(service.inner.is_none());
    assert!(service.reput_from_offset.is_some());
}

#[tokio::test]
async fn reput_service_start_wakes_existing_commitlog_backlog() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_configured_test_store(
        &temp_dir,
        MessageStoreConfig {
            read_uncommitted: true,
            ..MessageStoreConfig::default()
        },
    );
    let topic = CheetahString::from_static_str("reput-start-backlog-topic");
    let msg_size = append_encoded_test_message(&mut store, &topic, 0, 1_000, Bytes::from_static(b"backlog-body")).await;

    store.reput_message_service.set_reput_from_offset(0);
    let commit_log = store.commit_log.read_handle();
    let reput_policy = store.composition.reput();
    let dispatcher = store.dispatcher.handle();
    let runtime_context = store.reput_runtime_context();
    store.reput_message_service.start(
        &store.runtime_scope,
        commit_log,
        reput_policy,
        dispatcher,
        false,
        runtime_context,
    );

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let offset = store
                .reput_message_service
                .reput_from_offset
                .as_ref()
                .expect("reput offset should exist")
                .load(Ordering::Acquire);
            if offset >= i64::from(msg_size) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("initial reput wakeup should process existing backlog");

    store.reput_message_service.shutdown().await;
}

fn new_async_flush_test_store(temp_dir: &tempfile::TempDir) -> LocalFileMessageStore {
    new_configured_test_store(
        temp_dir,
        MessageStoreConfig {
            flush_disk_type: FlushDiskType::AsyncFlush,
            ..MessageStoreConfig::default()
        },
    )
}

fn decode_cq_bytes(bytes: Bytes) -> (i64, i32, i64) {
    let mut bytes = bytes;
    let commit_log_offset = bytes.get_i64();
    let msg_size = bytes.get_i32();
    let tags_code = bytes.get_i64();
    (commit_log_offset, msg_size, tags_code)
}

fn build_test_message(topic: &CheetahString, body: Bytes) -> MessageExtBrokerInner {
    let mut msg = MessageExtBrokerInner::default();
    msg.set_topic(topic.clone());
    msg.message_ext_inner.set_queue_id(0);
    msg.set_body(body);
    msg
}

#[cfg(feature = "tieredstore")]
fn encode_tiered_test_message(
    store: &LocalFileMessageStore,
    topic: &CheetahString,
    queue_offset: i64,
    commit_log_offset: i64,
    store_timestamp: i64,
    body: Bytes,
    key: Option<CheetahString>,
) -> Bytes {
    let mut msg = build_test_message(topic, body);
    msg.with_version(MessageVersion::V1);
    msg.message_ext_inner.set_queue_offset(queue_offset);
    msg.message_ext_inner.set_commit_log_offset(commit_log_offset);
    msg.message_ext_inner.set_store_timestamp(store_timestamp);
    if let Some(key) = key {
        msg.set_keys(key);
    }

    let mut encoder = MessageExtEncoder::new(store.message_store_config());
    assert!(encoder.encode(&msg).is_none());
    Bytes::copy_from_slice(encoder.byte_buf().as_ref())
}

async fn append_encoded_test_message(
    store: &mut LocalFileMessageStore,
    topic: &CheetahString,
    commit_log_offset: i64,
    store_timestamp: i64,
    body: Bytes,
) -> i32 {
    append_encoded_test_message_with_key(store, topic, commit_log_offset, store_timestamp, body, None).await
}

async fn append_encoded_test_message_with_key(
    store: &mut LocalFileMessageStore,
    topic: &CheetahString,
    commit_log_offset: i64,
    store_timestamp: i64,
    body: Bytes,
    key: Option<CheetahString>,
) -> i32 {
    let mut msg = build_test_message(topic, body);
    msg.with_version(MessageVersion::V1);
    msg.message_ext_inner.set_store_timestamp(store_timestamp);
    if let Some(key) = key {
        msg.set_keys(key);
    }

    let mut encoder = MessageExtEncoder::new(store.message_store_config());
    assert!(encoder.encode(&msg).is_none());
    let encoded = encoder.byte_buf();
    let msg_size = encoded.len() as i32;
    let appended = store
        .append_to_commit_log(commit_log_offset, encoded.as_ref(), 0, msg_size)
        .await
        .expect("append encoded commitlog message");
    assert!(appended);
    msg_size
}

fn build_test_batch(topic: &CheetahString, bodies: &[Bytes]) -> MessageExtBatch {
    let mut batch_body = BytesMut::new();
    for body in bodies {
        let record_size = 4 + 4 + 4 + 4 + 4 + body.len() + 2;
        batch_body.put_i32(record_size as i32);
        batch_body.put_i32(0);
        batch_body.put_i32(crc32(body.as_ref()) as i32);
        batch_body.put_i32(0);
        batch_body.put_i32(body.len() as i32);
        batch_body.put_slice(body.as_ref());
        batch_body.put_i16(0);
    }

    let mut inner = MessageExtBrokerInner::default();
    inner.set_topic(topic.clone());
    inner.message_ext_inner.set_queue_id(0);
    inner.set_body(batch_body.freeze());

    MessageExtBatch {
        message_ext_broker_inner: inner,
        is_inner_batch: false,
        encoded_buff: None,
    }
}

#[test]
fn owned_root_wiring_initializes_timer_message_store_when_timer_wheel_enabled() {
    let temp_dir = tempdir().unwrap();
    let store = new_configured_test_store_with_broker(
        &temp_dir,
        MessageStoreConfig {
            timer_wheel_enable: true,
            ..MessageStoreConfig::default()
        },
        StoreRuntimeConfig::default(),
    );

    assert!(store.root_dependencies_wired);
    assert!(store.get_timer_message_store().is_some());
    assert!(store.pending_ha_service.is_some());
    assert!(store.get_ha_service().is_none());
}

#[tokio::test]
async fn init_rejects_dledger_commit_log_configuration() {
    let temp_dir = tempdir().unwrap();

    for message_store_config in [
        MessageStoreConfig {
            enable_dledger_commit_log: true,
            ..MessageStoreConfig::default()
        },
        MessageStoreConfig {
            enable_dleger_commit_log: true,
            ..MessageStoreConfig::default()
        },
    ] {
        let mut store = new_configured_test_store(&temp_dir, message_store_config);
        let error = store.init().await.expect_err("DLedger should be rejected explicitly");
        assert_eq!(error.component(), StoreComponent::DLedger);
        assert!(error
            .detail()
            .is_some_and(|detail| detail.contains("DLedger commit log")));
    }
}

#[tokio::test]
async fn init_rejects_rocksdb_specific_flags_without_rocksdb_store_type() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_configured_test_store(
        &temp_dir,
        MessageStoreConfig {
            clean_rocksdb_dirty_cq_interval_min: 1,
            stat_rocksdb_cq_interval_sec: 2,
            real_time_persist_rocksdb_config: true,
            enable_rocksdb_log: true,
            rocksdb_cq_double_write_enable: true,
            trans_rocksdb_enable: true,
            ..MessageStoreConfig::default()
        },
    );

    let error = store
        .init()
        .await
        .expect_err("local file store should reject rocksdb-only configuration");
    assert_eq!(error.kind(), StoreErrorKind::InvalidRequest);
    let message = error.detail().expect("configuration detail");
    assert!(message.contains("store_type=RocksDB"));
    assert!(message.contains("clean_rocksdb_dirty_cq_interval_min"));
    assert!(message.contains("stat_rocksdb_cq_interval_sec"));
    assert!(message.contains("real_time_persist_rocksdb_config"));
    assert!(message.contains("enable_rocksdb_log"));
    assert!(message.contains("rocksdb_cq_double_write_enable"));
    assert!(message.contains("trans_rocksdb_enable"));
}

#[tokio::test]
async fn init_rejects_strict_active_file_memory_lock_without_explicit_budget() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_configured_test_store(
        &temp_dir,
        MessageStoreConfig {
            linux_memory_lock_mode: LinuxMemoryLockMode::ActiveFile,
            linux_memory_lock_budget_bytes: 0,
            linux_memory_lock_warn_only: false,
            ..MessageStoreConfig::default()
        },
    );

    let error = store
        .init()
        .await
        .expect_err("strict active_file memory locking should require explicit budget");
    assert_eq!(error.kind(), StoreErrorKind::InvalidRequest);
    let message = error.detail().expect("configuration detail");
    assert!(message.contains("active_file"));
    assert!(message.contains("linux_memory_lock_budget_bytes"));
    assert!(message.contains("linux_memory_lock_warn_only=true"));

    let warn_only_dir = tempdir().unwrap();
    let mut warn_only_store = new_configured_test_store(
        &warn_only_dir,
        MessageStoreConfig {
            linux_memory_lock_mode: LinuxMemoryLockMode::ActiveFile,
            linux_memory_lock_budget_bytes: 0,
            linux_memory_lock_warn_only: true,
            ..MessageStoreConfig::default()
        },
    );
    warn_only_store
        .init()
        .await
        .expect("warn-only active_file memory locking should degrade without explicit budget");

    let explicit_budget_dir = tempdir().unwrap();
    let mut explicit_budget_store = new_configured_test_store(
        &explicit_budget_dir,
        MessageStoreConfig {
            linux_memory_lock_mode: LinuxMemoryLockMode::ActiveFile,
            linux_memory_lock_budget_bytes: 64 * 1024 * 1024,
            linux_memory_lock_warn_only: false,
            ..MessageStoreConfig::default()
        },
    );
    explicit_budget_store
        .init()
        .await
        .expect("strict active_file memory locking should accept an explicit budget");
}

#[tokio::test]
async fn init_allows_rocksdb_specific_flags_when_store_type_is_rocksdb() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_configured_test_store(
        &temp_dir,
        MessageStoreConfig {
            store_type: StoreType::RocksDB,
            enable_rocksdb_log: true,
            ..MessageStoreConfig::default()
        },
    );

    store
        .init()
        .await
        .expect("rocksdb-typed store should accept rocksdb flags");
}

#[cfg(feature = "tieredstore")]
#[tokio::test]
async fn tieredstore_write_path_dispatches_commitlog_reput_messages() {
    let temp_dir = tempdir().unwrap();
    let topic = CheetahString::from_static_str("tieredstore-write-path-topic");
    let body = Bytes::from_static(b"tiered-write-body");
    let mut store = new_configured_test_store(
        &temp_dir,
        MessageStoreConfig {
            duplication_enable: true,
            flush_disk_type: FlushDiskType::AsyncFlush,
            read_uncommitted: true,
            timer_wheel_enable: false,
            tiered_store_config: Some(TieredStoreConfig {
                storage_level: TieredStorageLevel::Force,
                backend_provider: "memory".to_string(),
                store_path_root_dir: temp_dir.path().join("tieredstore"),
                max_pending_tasks: 16,
                ..TieredStoreConfig::default()
            }),
            ..MessageStoreConfig::default()
        },
    );

    store.init().await.expect("init tieredstore-enabled store");
    assert!(store.load().await, "load tieredstore-enabled store");
    let tiered_store = store
        .tiered_store
        .as_ref()
        .expect("tieredstore should be initialized")
        .clone();
    tiered_store.start().await.expect("start tieredstore dispatcher");

    let put_result = store.put_message(build_test_message(&topic, body.clone())).await;
    assert_eq!(put_result.put_message_status(), PutMessageStatus::PutOk);
    let append_result = put_result
        .append_message_result()
        .expect("put result should include append result");
    let queue_offset = append_result.logics_offset;
    store
        .reput_message_service
        .set_reput_from_offset(append_result.wrote_offset);
    store.reput_once().await;
    tiered_store.shutdown().await.expect("shutdown tieredstore dispatcher");

    let fetched = tiered_store
        .inner()
        .fetcher()
        .get_message(topic.to_string(), 0, queue_offset, 1)
        .await
        .expect("fetch tiered message after store shutdown drains dispatcher");
    assert_eq!(fetched.status, TieredGetMessageStatus::Found);
    assert_eq!(fetched.messages.len(), 1);
    assert!(fetched.messages[0]
        .windows(body.len())
        .any(|window| window == body.as_ref()));
    assert_eq!(tiered_store.metrics().dispatch_requests(), 1);
    assert_eq!(tiered_store.metrics().messages_dispatch_total(), 1);
}

#[cfg(feature = "tieredstore")]
#[tokio::test]
async fn tieredstore_read_path_falls_back_when_local_queue_is_missing() {
    let temp_dir = tempdir().unwrap();
    let topic = CheetahString::from_static_str("tieredstore-read-fallback-topic");
    let group = CheetahString::from_static_str("tieredstore-read-fallback-group");
    let key = CheetahString::from_static_str("tiered-fallback-key");
    let body = Bytes::from_static(b"tiered-read-fallback-body");
    let queue_offset = 7;
    let store_timestamp = 1_700_000;
    let mut store = new_configured_test_store(
        &temp_dir,
        MessageStoreConfig {
            duplication_enable: true,
            timer_wheel_enable: false,
            tiered_store_config: Some(TieredStoreConfig {
                storage_level: TieredStorageLevel::Force,
                backend_provider: "memory".to_string(),
                store_path_root_dir: temp_dir.path().join("tieredstore"),
                delete_file_enable: false,
                max_pending_tasks: 16,
                ..TieredStoreConfig::default()
            }),
            ..MessageStoreConfig::default()
        },
    );

    store.init().await.expect("init tieredstore-enabled store");
    assert!(store.load().await, "load tieredstore-enabled store");
    store.start().await.expect("start tieredstore-enabled store");

    let encoded = encode_tiered_test_message(
        &store,
        &topic,
        queue_offset,
        0,
        store_timestamp,
        body.clone(),
        Some(key.clone()),
    );
    let tiered_store = store
        .tiered_store
        .as_ref()
        .expect("tieredstore should be initialized")
        .clone();
    tiered_store
        .inner()
        .dispatcher()
        .dispatch(TieredDispatchRequest {
            topic: topic.to_string(),
            queue_id: 0,
            queue_offset,
            commit_log_offset: 0,
            message_size: encoded.len() as i32,
            tags_code: 0,
            store_timestamp,
            keys: Some(key.to_string()),
            uniq_key: None,
            offset_id: None,
            sys_flag: 0,
            body: Some(encoded),
        })
        .await
        .expect("dispatch directly to tieredstore");
    tiered_store
        .shutdown()
        .await
        .expect("shutdown tieredstore dispatcher after direct dispatch");

    let mut get_result = None;
    for _ in 0..50 {
        if let Some(result) = store.get_message(&group, &topic, 0, queue_offset, 1, None).await {
            if result.status() == Some(GetMessageStatus::Found) {
                get_result = Some(result);
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let get_result = get_result.expect("tieredstore get_message fallback should find message");
    assert_eq!(get_result.message_count(), 1);
    assert!(get_result.message_mapped_list()[0]
        .get_buffer()
        .windows(body.len())
        .any(|window| window == body.as_ref()));
    assert!(tiered_store.metrics().get_message_fallback_total() >= 1);
    assert!(tiered_store.metrics().messages_out_total() >= 1);

    let timestamp = store
        .get_message_store_timestamp_async(&topic, 0, queue_offset)
        .await
        .expect("tieredstore timestamp fallback");
    assert_eq!(timestamp, store_timestamp);

    assert_eq!(
        store
            .get_offset_in_queue_by_time_async(&topic, 0, store_timestamp - 1)
            .await
            .expect("tieredstore offset lower fallback"),
        queue_offset
    );
    assert_eq!(
        store
            .get_offset_in_queue_by_time_with_boundary_async(&topic, 0, store_timestamp, BoundaryType::Upper)
            .await
            .expect("tieredstore offset upper fallback"),
        queue_offset
    );
    assert_eq!(
        store
            .get_offset_in_queue_by_time_async(&topic, 0, store_timestamp + 1)
            .await
            .expect("tieredstore offset overflow fallback"),
        queue_offset + 1
    );

    let mut query_data = None;
    for _ in 0..50 {
        if let Some(result) = store.query_message(&topic, &key, 10, 0, i64::MAX).await {
            if let Some(data) = result.get_message_data() {
                query_data = Some(data);
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let query_data = query_data.expect("tieredstore query fallback data");
    assert!(query_data.windows(body.len()).any(|window| window == body.as_ref()));

    store.shutdown().await;
}

#[tokio::test]
async fn init_rejects_timer_rocksdb_backend_until_native_store_exists() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_configured_test_store(
        &temp_dir,
        MessageStoreConfig {
            timer_rocksdb_enable: true,
            ..MessageStoreConfig::default()
        },
    );

    let error = store
        .init()
        .await
        .expect_err("timer rocksdb backend is not implemented");
    assert_eq!(error.kind(), StoreErrorKind::Unsupported);
    let message = error.detail().expect("unsupported detail");
    assert!(message.contains("Timer RocksDB backend"));
    assert!(message.contains("timer_rocksdb_enable=false"));
}

#[tokio::test]
async fn init_allows_timer_rocksdb_backend_when_store_type_is_rocksdb() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_configured_test_store(
        &temp_dir,
        MessageStoreConfig {
            store_type: StoreType::RocksDB,
            timer_rocksdb_enable: true,
            ..MessageStoreConfig::default()
        },
    );

    store
        .init()
        .await
        .expect("rocksdb-typed store should accept timer rocksdb flag");
}

#[tokio::test]
async fn compaction_topic_dispatches_and_reads_from_compaction_store() {
    let temp_dir = tempdir().unwrap();
    let topic = CheetahString::from_static_str("compaction-dispatch-topic");
    let group = CheetahString::from_static_str("compaction-dispatch-group");
    let mut store = new_configured_test_store(
        &temp_dir,
        MessageStoreConfig {
            enable_compaction: true,
            flush_disk_type: FlushDiskType::AsyncFlush,
            ..MessageStoreConfig::default()
        },
    );
    let mut topic_config = TopicConfig::new(topic.clone());
    topic_config.attributes.insert(
        TopicAttributes::cleanup_policy_attribute().name().clone(),
        CleanupPolicy::COMPACTION.to_string().into(),
    );
    store.topic_config_table.insert(topic.clone(), Arc::new(topic_config));

    store.init().await.expect("init compaction-enabled store");
    assert!(store.load().await, "load compaction-enabled store");

    let put_result = store
        .put_message(build_test_message(
            &topic,
            Bytes::from_static(b"compaction-fallback-body"),
        ))
        .await;
    assert_eq!(put_result.put_message_status(), PutMessageStatus::PutOk);
    store.reput_once().await;

    let result = store
        .get_message(&group, &topic, 0, 0, 32, None)
        .await
        .expect("compaction topic should read from compaction store");
    assert_eq!(result.status(), Some(GetMessageStatus::Found));
    assert_eq!(result.message_count(), 1);
    assert_eq!(store.compaction_store.message_count(&topic, 0), 1);
    assert!(result.message_mapped_list()[0].mapped_file.is_none());
    assert!(result.message_mapped_list()[0].get_bytes_ref().is_some());
}

#[tokio::test]
async fn rocksdb_store_type_with_rocksdb_cq_topic_uses_compat_local_queue_path() {
    let temp_dir = tempdir().unwrap();
    let topic = CheetahString::from_static_str("rocksdb-compat-topic");
    let group = CheetahString::from_static_str("rocksdb-compat-group");
    let mut store = new_configured_test_store(
        &temp_dir,
        MessageStoreConfig {
            store_type: StoreType::RocksDB,
            rocksdb_cq_double_write_enable: true,
            flush_disk_type: FlushDiskType::AsyncFlush,
            ..MessageStoreConfig::default()
        },
    );
    let mut topic_config = TopicConfig::new(topic.clone());
    topic_config.attributes.insert(
        TopicAttributes::queue_type_attribute().name().clone(),
        CQType::RocksDBCQ.to_string().into(),
    );
    store.topic_config_table.insert(topic.clone(), Arc::new(topic_config));

    store.init().await.expect("init rocksdb compatibility store");
    assert!(store.load().await, "load rocksdb compatibility store");
    let put_result = store
        .put_message(build_test_message(&topic, Bytes::from_static(b"rocksdb-compat-body")))
        .await;
    assert_eq!(put_result.put_message_status(), PutMessageStatus::PutOk);
    store.reput_once().await;

    let queue = store
        .get_consume_queue(&topic, 0)
        .expect("compat queue should be present");
    assert_eq!(queue.read().get_cq_type(), CQType::SimpleCQ);

    let result = store
        .get_message(&group, &topic, 0, 0, 32, None)
        .await
        .expect("rocksdb compatibility get result");
    assert_eq!(result.status(), Some(GetMessageStatus::Found));
    assert_eq!(result.message_count(), 1);
    assert_eq!(result.next_begin_offset(), 1);

    let runtime_info = store.get_runtime_info();
    assert_eq!(runtime_info["storeType"], "RocksDB");
    assert_eq!(runtime_info["rocksdbCqDoubleWriteEnable"], "true");
    assert_eq!(runtime_info["rocksdbCompatibilityMode"], "local_file_compat");
}

#[test]
fn runtime_info_reports_linux_storage_lifecycle_fields() {
    let temp_dir = tempdir().unwrap();
    let store = new_configured_test_store(
        &temp_dir,
        MessageStoreConfig {
            mapped_file_size_commit_log: 1024,
            ..MessageStoreConfig::default()
        },
    );
    assert!(store.get_last_mapped_file(0));
    let mapped_file = store
        .commit_log
        .last_mapped_file_for_testing()
        .expect("runtime info test should create commitlog mapped file");
    mapped_file
        .get_metrics()
        .expect("mapped file metrics should be enabled")
        .record_warm_with_latency(4096, Duration::from_millis(12));

    let runtime_info = store.get_runtime_info();

    assert!(runtime_info.contains_key("linuxStorageOs"));
    assert!(runtime_info.contains_key("linuxStoragePageSize"));
    assert!(runtime_info.contains_key("linuxStorageMemoryLockLimitBytes"));
    let platform_capability = crate::platform::current_store_platform_capability();
    assert_eq!(
        runtime_info["storePlatformIoHintBranch"],
        platform_capability.optimization.io_hint_branch.as_str()
    );
    assert_eq!(
        runtime_info["storePlatformMmapAdviceSupported"],
        platform_capability.optimization.mmap_advice_supported.to_string()
    );
    assert_eq!(
        runtime_info["storePlatformFilePrefetchSupported"],
        platform_capability.optimization.file_prefetch_supported.to_string()
    );
    assert_eq!(
        runtime_info["storePlatformLazyMmapSupported"],
        platform_capability.optimization.lazy_mmap_supported.to_string()
    );
    assert_eq!(runtime_info["storePlatformIoHintFailureAffectsCorrectness"], "false");
    assert_eq!(runtime_info["storeIoHintEnable"], "false");
    assert_eq!(runtime_info["storeLazyMmapEnable"], "false");
    assert_eq!(runtime_info["storeEffectiveIoHintEnable"], "false");
    assert_eq!(runtime_info["storeEffectiveLazyMmapEnable"], "false");
    assert_eq!(runtime_info["transientStorePoolLockAttempts"], "0");
    assert_eq!(runtime_info["transientStorePoolLockedBuffers"], "0");
    assert_eq!(runtime_info["transientStorePoolLockFailedBuffers"], "0");
    assert_eq!(runtime_info["transientStorePoolLockSkippedBuffers"], "0");
    assert_eq!(runtime_info["transientStorePoolLockedBytes"], "0");
    assert_eq!(runtime_info["warmMappedFileEnable"], "false");
    assert_eq!(runtime_info["linuxStorageProfile"], "balanced");
    assert_eq!(runtime_info["linuxStorageTransferEngine"], "vectored");
    assert_eq!(runtime_info["linuxStorageMappedFileWarmMode"], "madvise");
    assert_eq!(runtime_info["linuxStorageMappedFileWarmOperations"], "1");
    assert_eq!(runtime_info["linuxStorageMappedFileWarmBytes"], "4096");
    assert_eq!(runtime_info["linuxStorageMappedFileWarmTotalMillis"], "12");
    assert_eq!(runtime_info["linuxStorageMappedFileWarmLastMillis"], "12");
    assert_eq!(runtime_info["storeLazyMmapEligibleFiles"], "0");
    assert_eq!(runtime_info["storeLazyMmapMappedFiles"], "0");
    assert_eq!(runtime_info["storeLazyMmapOperations"], "0");
    assert_eq!(runtime_info["storeLazyMmapFailures"], "0");
    assert_eq!(runtime_info["storeLazyMmapTotalMillis"], "0");
    assert_eq!(runtime_info["storeLazyMmapLastMillis"], "0");
    assert_eq!(runtime_info["linuxStorageMemoryLockMode"], "off");
    assert_eq!(runtime_info["linuxStorageRecoveryFadvise"], "disabled");
    assert_eq!(runtime_info["linuxStorageRecoveryMmapAdvice"], "disabled");
    assert_eq!(runtime_info["linuxStorageRecoveryMmapAdviceAttempts"], "0");
    assert_eq!(runtime_info["linuxStorageRecoveryMmapAdviceSuccesses"], "0");
    assert_eq!(runtime_info["linuxStorageRecoveryMmapAdviceFailures"], "0");
    assert_eq!(runtime_info["linuxStorageRecoveryMmapAdviceElapsedMs"], "0");
    assert_eq!(runtime_info["windowsStorageRecoveryFilePrefetch"], "disabled");
    assert_eq!(runtime_info["windowsStorageRecoveryFilePrefetchAttempts"], "0");
    assert_eq!(runtime_info["windowsStorageRecoveryFilePrefetchSuccesses"], "0");
    assert_eq!(runtime_info["windowsStorageRecoveryFilePrefetchFailures"], "0");
    assert_eq!(runtime_info["windowsStorageRecoveryFilePrefetchElapsedMs"], "0");
    assert_eq!(runtime_info["linuxStorageHaSendfileEnable"], "false");
    assert_eq!(runtime_info["linuxStorageIoUringEnable"], "false");
}

#[test]
fn runtime_info_reports_effective_linux_memory_lock_budget() {
    let temp_dir = tempdir().unwrap();
    let store = new_configured_test_store(
        &temp_dir,
        MessageStoreConfig {
            linux_memory_lock_budget_bytes: 4096,
            ..Default::default()
        },
    );

    let runtime_info = store.get_runtime_info();

    assert_eq!(runtime_info["linuxStorageEffectiveMemoryLockBudgetBytes"], "4096");
}

#[tokio::test]
async fn start_requires_init_before_services_begin() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_configured_test_store(
        &temp_dir,
        MessageStoreConfig {
            duplication_enable: true,
            ..MessageStoreConfig::default()
        },
    );

    let error = store.start().await.expect_err("start should require init first");

    assert_eq!(error.kind(), StoreErrorKind::Internal);
    assert!(error
        .detail()
        .is_some_and(|message| message.contains("initialized before start")));
    assert!(!temp_dir.path().join("lock").exists());
}

#[tokio::test]
async fn start_holds_store_root_lock_until_shutdown() {
    let temp_dir = tempdir().unwrap();
    let config = MessageStoreConfig {
        duplication_enable: true,
        ..MessageStoreConfig::default()
    };

    let mut first = new_configured_test_store(&temp_dir, config.clone());
    first.init().await.expect("init first store");
    first.start().await.expect("start first store");
    assert!(temp_dir.path().join("lock").exists());

    let mut second = new_configured_test_store(&temp_dir, config);
    second.init().await.expect("init second store");
    let error = second
        .start()
        .await
        .expect_err("second store should not start while lock is held");
    assert_eq!(error.kind(), StoreErrorKind::Storage);
    assert!(error
        .detail()
        .is_some_and(|message| message.contains("lock file is held")));

    first.shutdown().await;

    second.start().await.expect("lock should be reusable after shutdown");
    second.shutdown().await;
}

#[tokio::test]
async fn started_store_reput_dispatches_schedule_topic_messages_after_normal_message() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_configured_test_store(
        &temp_dir,
        MessageStoreConfig {
            flush_disk_type: FlushDiskType::AsyncFlush,
            ha_listen_port: allocate_local_test_port() as usize,
            ..MessageStoreConfig::default()
        },
    );
    let normal_topic = CheetahString::from_static_str("started-store-normal-before-schedule-topic");
    let schedule_topic = CheetahString::from_static_str(TopicValidator::RMQ_SYS_SCHEDULE_TOPIC);
    let schedule_queue_id = 2;
    let normal_message = build_test_message(&normal_topic, Bytes::from_static(b"normal-before-schedule-body"));
    let mut schedule_message = build_test_message(&schedule_topic, Bytes::from_static(b"scheduled-retry-body"));
    schedule_message.message_ext_inner.set_queue_id(schedule_queue_id);
    schedule_message.set_delay_time_level(3);

    store.init().await.expect("init store");
    assert!(store.load().await, "load store");
    store.start().await.expect("start store");

    let normal_put_result = store.put_message(normal_message).await;
    assert_eq!(normal_put_result.put_message_status(), PutMessageStatus::PutOk);
    let schedule_put_result = store.put_message(schedule_message).await;
    assert_eq!(schedule_put_result.put_message_status(), PutMessageStatus::PutOk);

    let mut normal_max_offset = 0;
    let mut max_offset = 0;
    for _ in 0..100 {
        normal_max_offset = store.get_max_offset_in_queue(&normal_topic, 0);
        max_offset = store.get_max_offset_in_queue(&schedule_topic, schedule_queue_id);
        if normal_max_offset == 1 && max_offset == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    store.shutdown().await;

    assert_eq!(
        normal_max_offset, 1,
        "started store should dispatch the normal message before the scheduled message"
    );
    assert_eq!(
        max_offset, 1,
        "started store should dispatch scheduled messages into SCHEDULE_TOPIC_XXXX consume queue"
    );
}

#[tokio::test]
async fn sync_flush_store_dispatches_wait_false_schedule_topic_messages() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_configured_test_store(
        &temp_dir,
        MessageStoreConfig {
            ha_listen_port: allocate_local_test_port() as usize,
            ..MessageStoreConfig::default()
        },
    );
    let normal_topic = CheetahString::from_static_str("sync-flush-normal-before-schedule-topic");
    let schedule_topic = CheetahString::from_static_str(TopicValidator::RMQ_SYS_SCHEDULE_TOPIC);
    let schedule_queue_id = 2;
    let normal_message = build_test_message(&normal_topic, Bytes::from_static(b"sync-flush-normal-body"));
    let mut schedule_message = build_test_message(&schedule_topic, Bytes::from_static(b"sync-flush-schedule-body"));
    schedule_message.message_ext_inner.set_queue_id(schedule_queue_id);
    schedule_message.set_delay_time_level(3);
    schedule_message.set_wait_store_msg_ok(false);

    store.init().await.expect("init store");
    assert!(store.load().await, "load store");
    store.start().await.expect("start store");

    let normal_put_result = store.put_message(normal_message).await;
    assert_eq!(normal_put_result.put_message_status(), PutMessageStatus::PutOk);
    let schedule_put_result = store.put_message(schedule_message).await;
    assert_eq!(schedule_put_result.put_message_status(), PutMessageStatus::PutOk);

    let mut normal_max_offset = 0;
    let mut schedule_max_offset = 0;
    for _ in 0..100 {
        normal_max_offset = store.get_max_offset_in_queue(&normal_topic, 0);
        schedule_max_offset = store.get_max_offset_in_queue(&schedule_topic, schedule_queue_id);
        if normal_max_offset == 1 && schedule_max_offset == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    store.shutdown().await;

    assert_eq!(
        normal_max_offset, 1,
        "sync flush store should dispatch the preceding normal message"
    );
    assert_eq!(
        schedule_max_offset, 1,
        "sync flush store should dispatch wait=false scheduled messages after wakeup flush"
    );
}

#[tokio::test]
async fn shutdown_waits_for_stats_and_timer_background_tasks() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_configured_test_store_with_broker(
        &temp_dir,
        MessageStoreConfig {
            duplication_enable: true,
            enable_compaction: true,
            timer_wheel_enable: true,
            ..MessageStoreConfig::default()
        },
        StoreRuntimeConfig::default(),
    );

    store.init().await.expect("init store");
    store.start().await.expect("start store");

    assert!(store.has_scheduled_task_group());
    let mut snapshots = store.scheduled_task_snapshot();
    for _ in 0..100 {
        if snapshots
            .iter()
            .any(|snapshot| snapshot.runs > 0 && snapshot.active_runs == 0)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        snapshots = store.scheduled_task_snapshot();
    }
    assert_eq!(snapshots.len(), 4, "{snapshots:?}");
    assert!(store.scheduled_task_count() >= 4);
    assert!(snapshots.iter().map(|snapshot| snapshot.runs).sum::<u64>() > 0);
    assert_eq!(snapshots.iter().map(|snapshot| snapshot.overlaps).sum::<u64>(), 0);
    assert_eq!(snapshots.iter().map(|snapshot| snapshot.failures).sum::<u64>(), 0);
    assert!(store.store_stats_service.has_worker_handle());
    assert!(store
        .compaction_service
        .as_ref()
        .is_some_and(CompactionService::has_worker_handle));
    assert!(store.reput_message_service.has_task_group());
    assert!(store
        .timer_message_store
        .as_ref()
        .expect("timer store should be initialized")
        .has_scheduler_handle());

    store.shutdown().await;

    assert!(!store.has_scheduled_task_group());
    assert_eq!(store.scheduled_task_count(), 0);
    assert!(!store.store_stats_service.has_worker_handle());
    assert!(store
        .compaction_service
        .as_ref()
        .is_some_and(|service| !service.has_worker_handle()));
    assert!(!store.reput_message_service.has_task_group());
    assert!(!store
        .timer_message_store
        .as_ref()
        .expect("timer store should be initialized")
        .has_scheduler_handle());
}

#[tokio::test]
async fn read_and_write_are_rejected_after_shutdown() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_test_store(&temp_dir);
    store.shutdown().await;

    let topic = CheetahString::from_static_str("shutdown-io-topic");
    let group = CheetahString::from_static_str("shutdown-io-group");

    let result = store
        .put_message(build_test_message(&topic, Bytes::from_static(b"after-shutdown")))
        .await;

    assert_eq!(result.put_message_status(), PutMessageStatus::ServiceNotAvailable);
    assert!(store.get_message(&group, &topic, 0, 0, 32, None).await.is_none());
}

#[tokio::test]
async fn read_and_write_are_rejected_while_recovering() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_test_store(&temp_dir);
    store.set_lifecycle_state(LocalStoreState::RecoveringCommitLog);

    let topic = CheetahString::from_static_str("recovering-io-topic");
    let group = CheetahString::from_static_str("recovering-io-group");
    let result = store
        .put_message(build_test_message(&topic, Bytes::from_static(b"during-recovery")))
        .await;

    assert_eq!(result.put_message_status(), PutMessageStatus::ServiceNotAvailable);
    assert!(store.get_message(&group, &topic, 0, 0, 32, None).await.is_none());
}

#[tokio::test]
async fn recover_restores_lifecycle_state_after_recovery_phases() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_test_store(&temp_dir);

    assert_eq!(store.lifecycle_state(), LocalStoreState::Created);
    store.recover(true).await;
    assert_eq!(store.lifecycle_state(), LocalStoreState::Created);

    store.init().await.expect("init store");
    store.recover(false).await;
    assert_eq!(store.lifecycle_state(), LocalStoreState::Initialized);
}

#[tokio::test]
async fn recover_records_structured_recovery_report() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_configured_test_store(
        &temp_dir,
        MessageStoreConfig {
            max_recovery_commit_log_files: 7,
            recovery_mode: RecoveryMode::Strict,
            check_crc_on_recover: true,
            force_verify_prop_crc: true,
            enable_local_file_consume_queue_recovery_concurrently: true,
            local_file_consume_queue_recovery_parallelism: 4,
            ..Default::default()
        },
    );

    store.recover(false).await;

    let report = store.last_recovery_report().expect("recovery report");
    assert_eq!(report.plan.mode, RecoveryMode::Strict);
    assert_eq!(report.plan.exit, RecoveryExit::Abnormal);
    assert!(!report.plan.recover_concurrently);
    assert_eq!(report.plan.max_recovery_commit_log_files, 7);
    assert_eq!(report.plan.scan_range.file_count_limit, Some(7));
    assert!(report.plan.dispatch_recovery_offset.is_some());
    assert_eq!(
        report.plan.offsets.dispatch_recovery_offset,
        report.plan.dispatch_recovery_offset
    );
    assert_eq!(
        report.plan.scan_range.start_offset,
        report.plan.dispatch_recovery_offset
    );
    assert!(report.plan.offsets.commit_log_min_offset.is_some());
    assert!(report.plan.offsets.commit_log_max_offset.is_some());
    assert!(report.plan.offsets.confirm_offset.is_some());
    assert_eq!(report.plan.offsets.index_safe_offset, Some(0));
    assert_eq!(
        report.plan.scan_range.end_offset,
        report.plan.offsets.commit_log_max_offset
    );
    assert_eq!(report.plan.crc_policy, RecoveryCrcPolicy::new(true, true));
    assert_eq!(report.plan.index_repair_policy, RecoveryIndexRepairPolicy::Synchronous);
    assert!(report.plan.consume_queue_recovery_concurrency.local_file_enabled);
    assert_eq!(report.plan.consume_queue_recovery_concurrency.local_file_parallelism, 4);
    assert_eq!(report.phases.len(), 3);
    assert!(report.phase_duration_ms(RecoveryPhase::ConsumeQueue).is_some());
    assert!(report.phase_duration_ms(RecoveryPhase::CommitLog).is_some());
    assert!(report.phase_duration_ms(RecoveryPhase::TopicQueueTable).is_some());
    assert!(report
        .phases
        .iter()
        .all(|phase| phase.status == RecoveryPhaseStatus::Success));
    assert_eq!(report.stats, RecoveryReportStats::default());
    assert_eq!(
        report.total_duration_ms,
        report.phases.iter().map(|phase| phase.duration_ms).sum()
    );
    let runtime_info = store.get_runtime_info();
    assert_eq!(runtime_info["recoveryReportAvailable"], "true");
    assert_eq!(runtime_info["recoveryPhaseCount"], report.phases.len().to_string());
    assert_eq!(runtime_info["recoveryFailedPhaseCount"], "0");
    assert_eq!(runtime_info["recoveryFallbackPhaseCount"], "0");
    assert_eq!(runtime_info["recoveryFallbackReasonPresent"], "false");
    assert!(!runtime_info.contains_key("recoveryFallbackReason"));
    assert_eq!(runtime_info["tieredStoreConfigured"], "false");
}

#[test]
fn current_index_safe_offset_is_bounded_by_confirm_offset() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_owned_test_store_with_broker(
        &temp_dir,
        MessageStoreConfig::default(),
        StoreRuntimeConfig {
            duplication_enable: true,
            ..StoreRuntimeConfig::default()
        },
    );
    let checkpoint = store.store_checkpoint.as_ref().expect("checkpoint");

    checkpoint.set_index_safe_phy_offset(512);
    store.set_confirm_offset(128);

    assert_eq!(store.current_index_safe_offset(), 128);
}

#[tokio::test]
async fn background_index_rebuild_pause_resume_and_shutdown_update_state() {
    let mut service = BackgroundIndexRebuildService::new();

    assert_eq!(service.snapshot().state, BackgroundIndexRebuildState::Idle);

    service.pause();
    assert_eq!(service.snapshot().state, BackgroundIndexRebuildState::Paused);

    service.resume();
    assert_eq!(service.snapshot().state, BackgroundIndexRebuildState::Idle);

    service.shutdown().await;
    assert_eq!(service.snapshot().state, BackgroundIndexRebuildState::Shutdown);
}

#[test]
fn background_index_rebuild_is_disabled_by_default_for_store() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_test_store(&temp_dir);
    let commit_log = store.commit_log.read_handle();
    let message_store_config = store.message_store_config.clone();
    let index_service = store.index_service.clone();
    let delay_level_table = store.delay_level_table_ref().clone();
    let max_delay_level = store.max_delay_level;

    store.background_index_rebuild_service.start(
        &store.runtime_scope,
        commit_log,
        message_store_config,
        index_service,
        delay_level_table,
        max_delay_level,
    );

    let snapshot = store.background_index_rebuild_snapshot();
    assert_eq!(snapshot.state, BackgroundIndexRebuildState::Idle);
    assert!(!store.background_index_rebuild_service.has_task_group());

    let runtime_info = store.get_runtime_info();
    assert_eq!(runtime_info["backgroundIndexRebuildState"], "idle");
    assert_eq!(runtime_info["backgroundIndexRebuildEffectiveEnable"], "false");
    assert_eq!(runtime_info["backgroundIndexRebuildGrayMode"], "disabled");
    assert_eq!(
        runtime_info["backgroundIndexRebuildRollbackHint"],
        MessageStoreConfig::background_index_rebuild_rollback_hint()
    );
    assert_eq!(runtime_info["backgroundIndexRebuildQueryDegradationTotal"], "0");
    assert_eq!(runtime_info["backgroundIndexRebuildBacklogBytes"], "0");
}

#[test]
fn background_index_rebuild_strict_mode_blocks_gray_start() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_configured_test_store(
        &temp_dir,
        MessageStoreConfig {
            enable_background_index_rebuild: true,
            recovery_mode: RecoveryMode::Strict,
            ..MessageStoreConfig::default()
        },
    );
    store.set_confirm_offset(128);

    let commit_log = store.commit_log.read_handle();
    let message_store_config = store.message_store_config.clone();
    let index_service = store.index_service.clone();
    let delay_level_table = store.delay_level_table_ref().clone();
    let max_delay_level = store.max_delay_level;
    store.background_index_rebuild_service.start(
        &store.runtime_scope,
        commit_log,
        message_store_config,
        index_service,
        delay_level_table,
        max_delay_level,
    );

    let snapshot = store.background_index_rebuild_snapshot();
    assert_eq!(snapshot.state, BackgroundIndexRebuildState::Idle);
    assert!(!store.background_index_rebuild_service.has_task_group());

    let runtime_info = store.get_runtime_info();
    assert_eq!(runtime_info["backgroundIndexRebuildEffectiveEnable"], "false");
    assert_eq!(runtime_info["backgroundIndexRebuildGrayMode"], "strict_blocked");
    assert_eq!(runtime_info["backgroundIndexRebuildBacklogBytes"], "0");
}

#[tokio::test]
async fn background_index_rebuild_completes_and_indexes_commitlog_messages() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_configured_test_store(
        &temp_dir,
        MessageStoreConfig {
            flush_disk_type: FlushDiskType::AsyncFlush,
            enable_background_index_rebuild: true,
            recovery_mode: RecoveryMode::Balanced,
            background_index_rebuild_batch_size: 1,
            background_index_rebuild_bytes_per_second: 0,
            background_index_rebuild_max_retries: 1,
            ..MessageStoreConfig::default()
        },
    );
    let topic = CheetahString::from_static_str("background-index-rebuild-topic");
    let key = CheetahString::from_static_str("background-index-rebuild-key");
    let msg_size = append_encoded_test_message_with_key(
        &mut store,
        &topic,
        0,
        1_000,
        Bytes::from_static(b"background-index-rebuild-body"),
        Some(key.clone()),
    )
    .await;
    let target_offset = i64::from(msg_size);
    let checkpoint = store.store_checkpoint.as_ref().expect("store checkpoint");
    checkpoint.set_index_safe_phy_offset(0);

    let before_rebuild = store
        .query_message(&topic, &key, 10, 0, i64::MAX)
        .await
        .expect("query result before background rebuild");
    assert!(
        before_rebuild.message_maped_list.is_empty(),
        "raw commitlog append should not populate the index before background rebuild"
    );
    assert!(!before_rebuild.index_query_safe);
    assert_eq!(before_rebuild.index_safe_phyoffset, 0);
    assert_eq!(before_rebuild.index_confirm_phyoffset, target_offset);
    assert_eq!(
        store.get_runtime_info()["backgroundIndexRebuildQueryDegradationTotal"],
        "1"
    );

    let commit_log = store.commit_log.read_handle();
    let message_store_config = store.message_store_config.clone();
    let index_service = store.index_service.clone();
    let delay_level_table = store.delay_level_table_ref().clone();
    let max_delay_level = store.max_delay_level;
    store.background_index_rebuild_service.start(
        &store.runtime_scope,
        commit_log,
        message_store_config,
        index_service,
        delay_level_table,
        max_delay_level,
    );

    let snapshot = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let snapshot = store.background_index_rebuild_snapshot();
            if snapshot.state == BackgroundIndexRebuildState::Completed {
                break snapshot;
            }
            assert_ne!(
                snapshot.state,
                BackgroundIndexRebuildState::Failed,
                "background index rebuild failed: {:?}",
                snapshot.last_error
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("background index rebuild should complete");

    assert!(snapshot.current_safe_offset >= target_offset);
    assert_eq!(snapshot.target_offset, target_offset);
    assert_eq!(snapshot.backlog_bytes, 0);
    assert_eq!(snapshot.rebuilt_messages, 1);
    assert!(snapshot.rebuilt_bytes > 0);
    assert_eq!(
        store.index_service.index_safe_phy_offset(),
        snapshot.current_safe_offset as u64
    );

    let query_result = store
        .query_message(&topic, &key, 10, 0, i64::MAX)
        .await
        .expect("query result after background rebuild");
    assert_eq!(query_result.message_maped_list.len(), 1);
    assert!(query_result.index_query_safe);
    assert_eq!(query_result.index_safe_phyoffset, target_offset);
    assert_eq!(query_result.index_confirm_phyoffset, target_offset);

    let runtime_info = store.get_runtime_info();
    assert_eq!(runtime_info["backgroundIndexRebuildState"], "completed");
    assert_eq!(runtime_info["backgroundIndexRebuildEffectiveEnable"], "true");
    assert_eq!(runtime_info["backgroundIndexRebuildGrayMode"], "balanced_gray");
    assert_eq!(
        runtime_info["backgroundIndexRebuildCurrentSafeOffset"],
        snapshot.current_safe_offset.to_string()
    );
    assert_eq!(runtime_info["backgroundIndexRebuildBacklogBytes"], "0");
    assert_eq!(runtime_info["backgroundIndexRebuildFailureCount"], "0");
    assert_eq!(runtime_info["backgroundIndexRebuildQueryDegradationTotal"], "1");

    store.background_index_rebuild_service.shutdown().await;
}

#[tokio::test]
async fn query_message_marks_empty_result_unsafe_when_index_safe_offset_lags_confirm() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_configured_test_store(
        &temp_dir,
        MessageStoreConfig {
            flush_disk_type: FlushDiskType::AsyncFlush,
            ..MessageStoreConfig::default()
        },
    );
    let topic = CheetahString::from_static_str("index-query-safe-range-topic");
    let key = CheetahString::from_static_str("index-query-safe-range-key");
    let msg_size = append_encoded_test_message_with_key(
        &mut store,
        &topic,
        0,
        1_000,
        Bytes::from_static(b"index-query-safe-range-body"),
        Some(key.clone()),
    )
    .await;

    let result = store
        .query_message(&topic, &key, 10, 0, i64::MAX)
        .await
        .expect("query result");

    assert!(result.message_maped_list.is_empty());
    assert!(!result.index_query_safe);
    assert_eq!(result.index_safe_phyoffset, 0);
    assert_eq!(result.index_confirm_phyoffset, i64::from(msg_size));
    assert_eq!(
        store.get_runtime_info()["backgroundIndexRebuildQueryDegradationTotal"],
        "1"
    );
}

#[tokio::test]
async fn background_index_rebuild_retries_then_fails_when_commitlog_data_missing() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_owned_test_store_with_broker(
        &temp_dir,
        MessageStoreConfig {
            enable_background_index_rebuild: true,
            recovery_mode: RecoveryMode::Balanced,
            background_index_rebuild_bytes_per_second: 0,
            background_index_rebuild_max_retries: 1,
            ..MessageStoreConfig::default()
        },
        StoreRuntimeConfig {
            duplication_enable: true,
            ..StoreRuntimeConfig::default()
        },
    );
    store.set_confirm_offset(128);

    let commit_log = store.commit_log.read_handle();
    let message_store_config = store.message_store_config.clone();
    let index_service = store.index_service.clone();
    let delay_level_table = store.delay_level_table_ref().clone();
    let max_delay_level = store.max_delay_level;
    store.background_index_rebuild_service.start(
        &store.runtime_scope,
        commit_log,
        message_store_config,
        index_service,
        delay_level_table,
        max_delay_level,
    );

    let snapshot = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let snapshot = store.background_index_rebuild_snapshot();
            if snapshot.state == BackgroundIndexRebuildState::Failed {
                break snapshot;
            }
            assert_ne!(
                snapshot.state,
                BackgroundIndexRebuildState::Completed,
                "background index rebuild should not complete without commitlog data"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("background index rebuild should fail after retry budget");

    assert!(snapshot.failure_count >= 2);
    assert_eq!(snapshot.target_offset, 128);
    assert_eq!(snapshot.current_safe_offset, 0);
    assert_eq!(snapshot.backlog_bytes, 128);
    assert!(snapshot
        .last_error
        .as_deref()
        .is_some_and(|error| error.contains("commitlog data unavailable")));

    let runtime_info = store.get_runtime_info();
    assert_eq!(runtime_info["backgroundIndexRebuildState"], "failed");
    assert_eq!(runtime_info["backgroundIndexRebuildEffectiveEnable"], "true");
    assert_eq!(runtime_info["backgroundIndexRebuildGrayMode"], "balanced_gray");
    assert_eq!(
        runtime_info["backgroundIndexRebuildFailureCount"],
        snapshot.failure_count.to_string()
    );
    assert!(runtime_info["backgroundIndexRebuildLastError"].contains("commitlog data unavailable"));

    store.background_index_rebuild_service.shutdown().await;
}

#[tokio::test]
async fn recover_enables_local_file_consume_queue_concurrency_when_configured() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_owned_test_store_with_broker(
        &temp_dir,
        MessageStoreConfig {
            enable_local_file_consume_queue_recovery_concurrently: true,
            local_file_consume_queue_recovery_parallelism: 2,
            ..Default::default()
        },
        StoreRuntimeConfig {
            recover_concurrently: true,
            ..Default::default()
        },
    );

    store.recover(false).await;

    let report = store.last_recovery_report().expect("recovery report");
    assert!(report.plan.recover_concurrently);
    assert!(report.plan.consume_queue_recovery_concurrency.local_file_enabled);
    assert_eq!(report.plan.consume_queue_recovery_concurrency.local_file_parallelism, 2);
}

#[tokio::test]
async fn start_and_init_are_rejected_while_recovering() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_test_store(&temp_dir);

    store.set_lifecycle_state(LocalStoreState::RecoveringConsumeQueue);
    let start_error = store
        .start()
        .await
        .expect_err("start should be rejected during recovery");
    assert_eq!(start_error.kind(), StoreErrorKind::Internal);
    assert!(start_error
        .detail()
        .is_some_and(|message| message.contains("recovering")));

    store.set_lifecycle_state(LocalStoreState::RecoveringTopicQueueTable);
    let init_error = store.init().await.expect_err("init should be rejected during recovery");
    assert_eq!(init_error.kind(), StoreErrorKind::Internal);
    assert!(init_error
        .detail()
        .is_some_and(|message| message.contains("recovering")));
}

#[test]
fn sync_broker_role_updates_timer_dequeue_state() {
    let temp_dir = tempdir().unwrap();
    let store = new_configured_test_store_with_broker(
        &temp_dir,
        MessageStoreConfig {
            timer_wheel_enable: true,
            ..MessageStoreConfig::default()
        },
        StoreRuntimeConfig::default(),
    );

    let timer_message_store = store
        .get_timer_message_store()
        .cloned()
        .expect("timer message store should exist");

    assert!(!timer_message_store.is_should_running_dequeue());

    store.sync_broker_role(BrokerRole::SyncMaster);
    assert_eq!(store.current_broker_role(), BrokerRole::SyncMaster);
    assert!(timer_message_store.is_should_running_dequeue());

    store.sync_broker_role(BrokerRole::Slave);
    assert_eq!(store.current_broker_role(), BrokerRole::Slave);
    assert!(!timer_message_store.is_should_running_dequeue());

    store.sync_broker_role(BrokerRole::AsyncMaster);
    assert!(timer_message_store.is_should_running_dequeue());
}

#[test]
fn set_master_flushed_offset_updates_store_checkpoint() {
    let temp_dir = tempdir().unwrap();
    let store = new_test_store(&temp_dir);
    let checkpoint_path = get_store_checkpoint(store.message_store_config_ref().store_path_root_dir.as_str());

    store.set_master_flushed_offset(1024);
    store
        .store_checkpoint
        .as_ref()
        .expect("store checkpoint")
        .flush()
        .expect("flush checkpoint");

    let checkpoint = StoreCheckpoint::new(&checkpoint_path).expect("reload checkpoint");
    assert_eq!(checkpoint.master_flushed_offset(), 1024);
}

#[tokio::test]
async fn sync_broker_role_in_controller_mode_refreshes_confirm_offset_for_master() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_controller_test_store(
        &temp_dir,
        MessageStoreConfig {
            all_ack_in_sync_state_set: true,
            ..MessageStoreConfig::default()
        },
    );
    store.init().await.expect("init store");
    store.sync_controller_sync_state_set(7, &HashSet::from([7_i64]));

    store
        .get_commit_log_mut()
        .append_data(0, &[1, 2, 3, 4], 0, 4)
        .await
        .expect("append data");
    store.set_confirm_offset(0);
    store.sync_broker_role(BrokerRole::Slave);
    assert_eq!(store.get_commit_log().get_confirm_offset_directly(), 0);

    store.sync_broker_role(BrokerRole::SyncMaster);

    assert_eq!(store.get_commit_log().get_confirm_offset_directly(), 4);
    assert_eq!(store.get_confirm_offset(), 4);
}

#[test]
fn master_store_in_process_round_trips_concrete_store_reference() {
    let temp_dir = tempdir().unwrap();
    let store = new_test_store(&temp_dir);
    let master_store = Arc::new(LocalFileMessageStore::new(
        Arc::new(MessageStoreConfig {
            store_path_root_dir: temp_dir
                .path()
                .join("master-store")
                .to_string_lossy()
                .to_string()
                .into(),
            ..MessageStoreConfig::default()
        }),
        Arc::new(StoreRuntimeConfig::default()),
        Arc::new(DashMap::<CheetahString, Arc<TopicConfig>>::new()),
        None,
        false,
        crate::runtime::test_service_context("master-local-file-store-test"),
    ));

    store.set_master_store_in_process(master_store.clone());

    let restored = store
        .get_master_store_in_process::<LocalFileMessageStore>()
        .expect("master store should be present");

    assert!(Arc::ptr_eq(&master_store, &restored));
}

#[test]
fn send_message_back_hook_round_trips_registered_hook() {
    struct MockSendBackHook;

    impl SendMessageBackHook for MockSendBackHook {
        fn execute_send_message_back(
            &self,
            _msg_list: &mut [MessageExt],
            broker_name: &CheetahString,
            broker_addr: &CheetahString,
        ) -> bool {
            !broker_name.is_empty() && !broker_addr.is_empty()
        }
    }

    let temp_dir = tempdir().unwrap();
    let store = new_test_store(&temp_dir);
    let hook = Arc::new(MockSendBackHook) as Arc<dyn SendMessageBackHook>;

    store.set_send_message_back_hook(hook.clone());

    let restored = store
        .get_send_message_back_hook()
        .expect("send message back hook should be present");

    assert!(Arc::ptr_eq(&hook, &restored));
}

#[test]
fn clean_unused_topic_deletes_only_non_retained_non_system_non_lmq_topics() {
    let temp_dir = tempdir().unwrap();
    let store = new_test_store(&temp_dir);
    let deletable_topic = CheetahString::from_static_str("delete-me");
    let retained_topic = CheetahString::from_static_str("retain-me");
    let system_topic = CheetahString::from_static_str(TopicValidator::RMQ_SYS_TRACE_TOPIC);
    let lmq_topic = CheetahString::from_static_str("%LMQ%lite-group");

    store
        .consume_queue_store
        .find_or_create_consume_queue(&deletable_topic, 0);
    store
        .consume_queue_store
        .find_or_create_consume_queue(&retained_topic, 0);
    store.consume_queue_store.find_or_create_consume_queue(&system_topic, 0);
    store.consume_queue_store.find_or_create_consume_queue(&lmq_topic, 0);

    let mut retain_topics = HashSet::new();
    retain_topics.insert(retained_topic.to_string());

    let deleted_count = store.clean_unused_topic(&retain_topics);

    assert_eq!(deleted_count, 1);
    assert!(store
        .consume_queue_store
        .find_consume_queue_map(&deletable_topic)
        .is_none());
    assert!(store
        .consume_queue_store
        .find_consume_queue_map(&retained_topic)
        .is_some());
    assert!(store
        .consume_queue_store
        .find_consume_queue_map(&system_topic)
        .is_some());
    assert!(store.consume_queue_store.find_consume_queue_map(&lmq_topic).is_some());
}

#[test]
fn get_commit_log_offset_in_queue_returns_consume_queue_entry_position() {
    let temp_dir = tempdir().unwrap();
    let store = new_test_store(&temp_dir);
    let topic = CheetahString::from_static_str("offset-topic");

    store
        .consume_queue_store
        .put_message_position_info_wrapper(&DispatchRequest {
            topic: topic.clone(),
            queue_id: 1,
            commit_log_offset: 123,
            msg_size: 32,
            consume_queue_offset: 0,
            success: true,
            ..DispatchRequest::default()
        });

    assert_eq!(store.get_commit_log_offset_in_queue(&topic, 1, 0), 123);
}

#[test]
fn assign_offset_and_increase_offset_delegate_to_consume_queue_store() {
    let temp_dir = tempdir().unwrap();
    let store = new_test_store(&temp_dir);
    let topic = CheetahString::from_static_str("assign-offset-topic");

    let mut first = MessageExtBrokerInner::default();
    first.set_topic(topic.clone());
    first.message_ext_inner.set_queue_id(2);
    first.set_body(Bytes::from_static(b"first"));

    store.assign_offset(&mut first).unwrap();
    assert_eq!(first.queue_offset(), 0);

    store.increase_offset(&first, 1);

    let mut second = MessageExtBrokerInner::default();
    second.set_topic(topic);
    second.message_ext_inner.set_queue_id(2);
    second.set_body(Bytes::from_static(b"second"));

    store.assign_offset(&mut second).unwrap();
    assert_eq!(second.queue_offset(), 1);
}

#[test]
fn commit_log_accessors_copy_data_and_allow_offset_reset() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_test_store(&temp_dir);
    let payload = b"rust";

    assert!(store.is_mapped_files_empty());

    let appended = crate::runtime::test_runtime_owner()
        .block_on(store.append_to_commit_log(0, payload, 0, payload.len() as i32))
        .unwrap();

    assert!(appended);
    assert!(!store.is_mapped_files_empty());
    assert_eq!(store.get_last_file_from_offset(), 0);
    assert!(store.get_last_mapped_file(0));

    let mut buffer = BytesMut::new();
    assert!(store.get_data(0, payload.len() as i32, &mut buffer));
    assert_eq!(buffer.as_ref(), payload);

    let flushed_where = store.flush();
    assert!(flushed_where >= payload.len() as i64);
    assert_eq!(store.get_flushed_where(), flushed_where);

    assert!(store.reset_write_offset(2));

    let mut truncated = BytesMut::new();
    assert!(!store.get_data(0, payload.len() as i32, &mut truncated));

    let mut remaining = BytesMut::new();
    assert!(store.get_data(0, 2, &mut remaining));
    assert_eq!(remaining.as_ref(), &payload[..2]);
}

#[test]
fn failed_canonical_flush_marks_store_unwriteable_and_legacy_flush_keeps_watermark() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_test_store(&temp_dir);
    let payload = b"pending-durable-message";
    assert!(crate::runtime::test_runtime_owner()
        .block_on(store.append_to_commit_log(0, payload, 0, payload.len() as i32))
        .unwrap());
    let durable_before = store.get_flushed_where();
    let mapped_file = store
        .get_commit_log()
        .last_mapped_file_for_testing()
        .expect("append should create a mapped file");
    MappedFile::shutdown(mapped_file.as_ref(), 0);

    let error = store
        .try_flush()
        .expect_err("unavailable mapped file must fail canonical flush");

    assert_eq!(error.component(), StoreComponent::MappedFile);
    let health = store.health_snapshot();
    assert!(!health.writeable);
    assert_eq!(
        health.last_flush_error.as_ref().map(|error| error.kind),
        Some(crate::store_error::StoreErrorKind::Storage)
    );
    assert!(health
        .last_flush_error
        .as_ref()
        .is_some_and(|error| error.detail.contains("mapped_file")));
    assert_eq!(store.flush(), durable_before);
    assert_eq!(store.get_flushed_where(), durable_before);
}

#[tokio::test]
async fn graceful_shutdown_reports_typed_final_flush_failure() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_test_store(&temp_dir);
    store.init().await.expect("initialize message store");
    let payload = b"pending-shutdown-message";
    assert!(store
        .append_to_commit_log(0, payload, 0, payload.len() as i32)
        .await
        .expect("append pending message"));
    let mapped_file = store
        .get_commit_log()
        .last_mapped_file_for_testing()
        .expect("append should create a mapped file");
    MappedFile::shutdown(mapped_file.as_ref(), 0);

    let error = store
        .shutdown_gracefully()
        .await
        .expect_err("final fsync failure must be exposed to the shutdown caller");

    assert_eq!(error.component(), StoreComponent::MappedFile);
    let health = store.health_snapshot();
    assert!(!health.writeable);
    assert_eq!(
        health.last_flush_error.as_ref().map(|error| error.kind),
        Some(crate::store_error::StoreErrorKind::Storage)
    );
}

#[test]
fn get_put_message_hook_list_returns_registered_hooks() {
    struct TestHook;

    impl PutMessageHook for TestHook {
        fn hook_name(&self) -> &'static str {
            "test-hook"
        }

        fn execute_before_put_message(&self, _msg: &mut dyn MessageTrait) -> Option<PutMessageResult> {
            None
        }
    }

    let temp_dir = tempdir().unwrap();
    let mut store = new_test_store(&temp_dir);
    store.set_put_message_hook(Box::new(TestHook));

    let hooks = store.get_put_message_hook_list();

    assert_eq!(hooks.len(), 1);
    assert_eq!(hooks[0].hook_name(), "test-hook");
}

#[test]
fn consume_queue_reports_basic_runtime_metadata() {
    let temp_dir = tempdir().unwrap();
    let store = new_test_store(&temp_dir);
    let topic = CheetahString::from_static_str("consume-queue-metadata-topic");

    store
        .consume_queue_store
        .put_message_position_info_wrapper(&DispatchRequest {
            topic: topic.clone(),
            queue_id: 1,
            commit_log_offset: 123,
            msg_size: 32,
            consume_queue_offset: 0,
            store_timestamp: 5678,
            success: true,
            ..DispatchRequest::default()
        });

    let consume_queue = store.consume_queue_store.find_or_create_consume_queue(&topic, 1);
    let expected_roll = store.message_store_config_ref().get_mapped_file_size_consume_queue() as i64
        / consume_queue.read().get_unit_size() as i64;

    assert_eq!(consume_queue.read().get_message_total_in_queue(), 1);
    assert_eq!(consume_queue.read().get_last_offset(), 155);
    assert_eq!(consume_queue.read().roll_next_file(1), expected_roll);
    assert!(consume_queue.read().is_first_file_exist());
    assert!(consume_queue.read().is_first_file_available());
    assert_eq!(
        consume_queue.read().get_total_size(),
        store.message_store_config_ref().get_mapped_file_size_consume_queue() as i64
    );
}

#[tokio::test]
async fn flush_consume_queue_service_start_persists_logic_checkpoint_to_disk() {
    let temp_dir = tempdir().unwrap();
    let store = new_configured_test_store(
        &temp_dir,
        MessageStoreConfig {
            flush_interval_consume_queue: 10,
            flush_consume_queue_thorough_interval: 10,
            ..MessageStoreConfig::default()
        },
    );

    let expected_timestamp = 4321i64;
    store
        .consume_queue_store
        .put_message_position_info_wrapper(&DispatchRequest {
            topic: CheetahString::from_static_str("flush-cq-service-topic"),
            queue_id: 0,
            commit_log_offset: 128,
            msg_size: 32,
            consume_queue_offset: 0,
            store_timestamp: expected_timestamp,
            success: true,
            ..DispatchRequest::default()
        });

    let checkpoint_path = get_store_checkpoint(store.message_store_config_ref().store_path_root_dir.as_str());
    assert_eq!(
        StoreCheckpoint::new(&checkpoint_path).unwrap().logics_msg_timestamp(),
        0
    );

    store.flush_consume_queue_service.start();

    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let checkpoint = StoreCheckpoint::new(&checkpoint_path).unwrap();
        if checkpoint.logics_msg_timestamp() == expected_timestamp as u64 {
            break;
        }

        assert!(
            Instant::now() < deadline,
            "flush consume queue service did not persist checkpoint in time"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    store.flush_consume_queue_service.shutdown().await;
}

#[tokio::test]
async fn default_local_path_supports_consume_queue_time_queries_and_estimation() {
    struct MatchAllFilter;

    impl MessageFilter for MatchAllFilter {
        fn is_matched_by_consume_queue(
            &self,
            _tags_code: Option<i64>,
            _cq_ext_unit: Option<&crate::consume_queue::cq_ext_unit::CqExtUnit>,
        ) -> bool {
            true
        }

        fn is_matched_by_commit_log(
            &self,
            _msg_buffer: Option<&[u8]>,
            _properties: Option<&HashMap<CheetahString, CheetahString>>,
        ) -> bool {
            true
        }
    }

    let temp_dir = tempdir().unwrap();
    let mut store = new_async_flush_test_store(&temp_dir);
    let topic = CheetahString::from_static_str("consume-queue-time-query-topic");

    let mut first = MessageExtBrokerInner::default();
    first.set_topic(topic.clone());
    first.message_ext_inner.set_queue_id(0);
    first.set_body(Bytes::from_static(b"first-body"));

    let first_result = store.put_message(first).await;
    assert_eq!(first_result.put_message_status(), PutMessageStatus::PutOk);

    tokio::time::sleep(Duration::from_millis(5)).await;

    let mut second = MessageExtBrokerInner::default();
    second.set_topic(topic.clone());
    second.message_ext_inner.set_queue_id(0);
    second.set_body(Bytes::from_static(b"second-body"));

    let second_result = store.put_message(second).await;
    assert_eq!(second_result.put_message_status(), PutMessageStatus::PutOk);

    store.reput_once().await;

    let first_store_time = store.get_message_store_timestamp(&topic, 0, 0);
    let second_store_time = store.get_message_store_timestamp(&topic, 0, 1);

    assert!(first_store_time > 0);
    assert!(second_store_time >= first_store_time);
    assert_eq!(store.get_earliest_message_time(&topic, 0), first_store_time);
    assert_eq!(store.get_offset_in_queue_by_time(&topic, 0, first_store_time), 0);

    let consume_queue = store.consume_queue_store.find_or_create_consume_queue(&topic, 0);
    assert_eq!(
        consume_queue.read().get_offset_in_queue_by_time(second_store_time),
        if second_store_time == first_store_time { 0 } else { 1 }
    );
    assert_eq!(store.estimate_message_count(&topic, 0, 0, 1, &MatchAllFilter), 2);

    let latest = consume_queue.read().get_latest_unit().expect("latest cq unit");
    assert_eq!(latest.queue_offset, 1);
}

#[tokio::test]
async fn consume_queue_time_query_resolves_duplicate_timestamp_boundaries() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_test_store(&temp_dir);
    let topic = CheetahString::from_static_str("consume-queue-duplicate-time-topic");

    let mut next_commit_log_offset = 0;
    for (queue_offset, store_timestamp, body) in [
        (0_i64, 1_000_i64, Bytes::from_static(b"first")),
        (1, 1_000, Bytes::from_static(b"second")),
        (2, 2_000, Bytes::from_static(b"third")),
    ] {
        let msg_size =
            append_encoded_test_message(&mut store, &topic, next_commit_log_offset, store_timestamp, body).await;
        store
            .consume_queue_store
            .put_message_position_info_wrapper(&DispatchRequest {
                topic: topic.clone(),
                queue_id: 0,
                commit_log_offset: next_commit_log_offset,
                msg_size,
                consume_queue_offset: queue_offset,
                store_timestamp,
                success: true,
                ..DispatchRequest::default()
            });
        next_commit_log_offset += msg_size as i64;
    }

    let consume_queue = store.consume_queue_store.find_or_create_consume_queue(&topic, 0);

    assert_eq!(
        consume_queue
            .read()
            .get_offset_in_queue_by_time_with_boundary(1_000, BoundaryType::Lower),
        0
    );
    assert_eq!(
        consume_queue
            .read()
            .get_offset_in_queue_by_time_with_boundary(1_000, BoundaryType::Upper),
        1
    );
    assert_eq!(
        consume_queue
            .read()
            .get_offset_in_queue_by_time_with_boundary(1_500, BoundaryType::Lower),
        2
    );
    assert_eq!(
        consume_queue
            .read()
            .get_offset_in_queue_by_time_with_boundary(1_500, BoundaryType::Upper),
        1
    );
}

#[tokio::test]
async fn local_file_consume_queue_store_range_query_and_get_return_encoded_units() {
    let temp_dir = tempdir().unwrap();
    let store = new_test_store(&temp_dir);
    let topic = CheetahString::from_static_str("local-cq-store-range-query-topic");

    for (queue_offset, commit_log_offset, tags_code) in [(0_i64, 100_i64, 11_i64), (1, 132, 22), (2, 164, 33)] {
        store
            .consume_queue_store
            .put_message_position_info_wrapper(&DispatchRequest {
                topic: topic.clone(),
                queue_id: 0,
                commit_log_offset,
                msg_size: 32,
                tags_code,
                consume_queue_offset: queue_offset,
                store_timestamp: 1000 + queue_offset,
                success: true,
                ..DispatchRequest::default()
            });
    }

    let batch = store.consume_queue_store.range_query(&topic, 0, 1, 2).await;
    assert_eq!(batch.len(), 2);
    assert_eq!(decode_cq_bytes(batch[0].clone()), (132, 32, 22));
    assert_eq!(decode_cq_bytes(batch[1].clone()), (164, 32, 33));

    let single = store.consume_queue_store.get(&topic, 0, 0).await;
    assert_eq!(decode_cq_bytes(single), (100, 32, 11));
}

#[tokio::test]
async fn get_message_returns_dispatched_messages_after_reput() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_async_flush_test_store(&temp_dir);
    let topic = CheetahString::from_static_str("get-message-after-reput-topic");
    let group = CheetahString::from_static_str("test-group");

    for body in [Bytes::from_static(b"first-body"), Bytes::from_static(b"second-body")] {
        let mut msg = MessageExtBrokerInner::default();
        msg.set_topic(topic.clone());
        msg.message_ext_inner.set_queue_id(0);
        msg.set_body(body);

        let result = store.put_message(msg).await;
        assert_eq!(result.put_message_status(), PutMessageStatus::PutOk);
    }

    store.reput_once().await;

    let result = store
        .get_message(&group, &topic, 0, 0, 32, None)
        .await
        .expect("get message result");

    assert_eq!(result.status(), Some(GetMessageStatus::Found));
    assert_eq!(result.message_count(), 2);
    assert_eq!(result.message_queue_offset(), &vec![0, 1]);
    assert_eq!(result.next_begin_offset(), 2);
    assert_eq!(result.min_offset(), 0);
    assert_eq!(result.max_offset(), 2);
    assert_eq!(result.message_mapped_list().len(), 2);
    assert_eq!(result.message_mapped_capacity(), 32);

    let limited = tokio::time::timeout(Duration::from_secs(1), store.get_message(&group, &topic, 0, 0, 1, None))
        .await
        .expect("limited pull should not spin after exhausting its iterator")
        .expect("limited get message result");
    assert_eq!(limited.status(), Some(GetMessageStatus::Found));
    assert_eq!(limited.message_count(), 1);
    assert_eq!(limited.next_begin_offset(), 1);
    assert_eq!(limited.message_mapped_list().len(), 1);
}

#[tokio::test]
async fn store_stats_records_single_put_append_totals() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_async_flush_test_store(&temp_dir);
    let topic = CheetahString::from_static_str("single-put-store-stats-topic");
    let stats = store.get_store_stats_service();

    let put_result = store
        .put_message(build_test_message(&topic, Bytes::from_static(b"single-put-body")))
        .await;

    assert_eq!(put_result.put_message_status(), PutMessageStatus::PutOk);
    let append_result = put_result.append_message_result().expect("single put append result");
    assert_eq!(append_result.msg_num, 1);
    assert_eq!(stats.get_put_message_times_total(), append_result.msg_num as u64);
    assert_eq!(stats.get_put_message_size_total(), append_result.wrote_bytes as u64);

    let runtime_info = stats.get_runtime_info();
    assert_eq!(runtime_info["putMessageTimesTotal"], "1");
    assert_eq!(
        runtime_info["putMessageSizeTotal"],
        append_result.wrote_bytes.to_string()
    );
}

#[tokio::test]
async fn store_stats_records_batch_put_append_totals() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_async_flush_test_store(&temp_dir);
    let topic = CheetahString::from_static_str("batch-put-store-stats-topic");
    let stats = store.get_store_stats_service();
    let batch = build_test_batch(
        &topic,
        &[
            Bytes::from_static(b"batch-body-1"),
            Bytes::from_static(b"batch-body-2"),
            Bytes::from_static(b"batch-body-3"),
        ],
    );

    let put_result = store.put_messages(batch).await;

    assert_eq!(put_result.put_message_status(), PutMessageStatus::PutOk);
    let append_result = put_result.append_message_result().expect("batch put append result");
    assert_eq!(append_result.msg_num, 3);
    assert_eq!(stats.get_put_message_times_total(), 3);
    assert_eq!(stats.get_put_message_size_total(), append_result.wrote_bytes as u64);
}

#[tokio::test]
async fn shared_put_message_micro_batches_concurrent_appends() {
    let temp_dir = tempdir().unwrap();
    let store = new_async_flush_test_store(&temp_dir);
    let topic = CheetahString::from_static_str("shared-put-message-topic");

    let first = store.put_message_shared(build_test_message(&topic, Bytes::from_static(b"first")));
    let second = store.put_message_shared(build_test_message(&topic, Bytes::from_static(b"second")));
    let (first, second) = tokio::join!(first, second);

    assert_eq!(first.put_message_status(), PutMessageStatus::PutOk);
    assert_eq!(second.put_message_status(), PutMessageStatus::PutOk);
    assert_ne!(
        first.append_message_result().expect("first append").wrote_offset,
        second.append_message_result().expect("second append").wrote_offset
    );
    assert_eq!(store.get_runtime_info()["putMessageLockAcquireTotal"], "1");
}

#[tokio::test]
async fn single_put_retries_encoded_buffer_after_commitlog_eof_roll() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_configured_test_store(
        &temp_dir,
        MessageStoreConfig {
            mapped_file_size_commit_log: 512,
            flush_disk_type: FlushDiskType::AsyncFlush,
            ..MessageStoreConfig::default()
        },
    );
    let topic = CheetahString::from_static_str("af0s");

    let seed = store
        .put_message(build_test_message(&topic, Bytes::from(vec![1_u8; 245])))
        .await;
    assert_eq!(seed.put_message_status(), PutMessageStatus::PutOk);
    assert_eq!(seed.append_message_result().expect("seed append").wrote_bytes, 340);

    let retried = store
        .put_message(build_test_message(&topic, Bytes::from(vec![2_u8; 75])))
        .await;

    assert_eq!(retried.put_message_status(), PutMessageStatus::PutOk);
    let append = retried.append_message_result().expect("retried append");
    assert_eq!(append.wrote_offset, 512);
    assert_eq!(append.wrote_bytes, 170);
    assert_eq!(append.msg_num, 1);
    assert_eq!(store.get_runtime_info()["putMessageLockAcquireTotal"], "2");
}

#[tokio::test]
async fn batch_put_retries_full_encoded_buffer_after_partial_frame_eof_roll() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_configured_test_store(
        &temp_dir,
        MessageStoreConfig {
            mapped_file_size_commit_log: 512,
            flush_disk_type: FlushDiskType::AsyncFlush,
            ..MessageStoreConfig::default()
        },
    );
    let topic = CheetahString::from_static_str("af0b");

    let seed = store
        .put_message(build_test_message(&topic, Bytes::from(vec![3_u8; 75])))
        .await;
    assert_eq!(seed.put_message_status(), PutMessageStatus::PutOk);
    assert_eq!(seed.append_message_result().expect("seed append").wrote_bytes, 170);

    let batch = build_test_batch(&topic, &[Bytes::from(vec![4_u8; 75]), Bytes::from(vec![5_u8; 75])]);
    let retried = store.put_messages(batch).await;

    assert_eq!(retried.put_message_status(), PutMessageStatus::PutOk);
    let append = retried.append_message_result().expect("retried batch append");
    assert_eq!(append.wrote_offset, 512);
    assert_eq!(append.wrote_bytes, 340);
    assert_eq!(append.msg_num, 2);
    assert_eq!(store.get_runtime_info()["putMessageLockAcquireTotal"], "2");
}

#[tokio::test]
async fn store_stats_records_get_found_miss_and_transferred_counts() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_async_flush_test_store(&temp_dir);
    let topic = CheetahString::from_static_str("get-store-stats-topic");
    let group = CheetahString::from_static_str("get-store-stats-group");
    let stats = store.get_store_stats_service();

    for body in [Bytes::from_static(b"first-body"), Bytes::from_static(b"second-body")] {
        let put_result = store.put_message(build_test_message(&topic, body)).await;
        assert_eq!(put_result.put_message_status(), PutMessageStatus::PutOk);
    }
    store.reput_once().await;

    let found_result = store
        .get_message(&group, &topic, 0, 0, 32, None)
        .await
        .expect("found get result");
    assert_eq!(found_result.status(), Some(GetMessageStatus::Found));
    assert_eq!(found_result.message_count(), 2);
    assert_eq!(stats.get_message_times_total_found().load(Ordering::Relaxed), 1);
    assert_eq!(stats.get_message_times_total_miss().load(Ordering::Relaxed), 0);
    assert_eq!(stats.get_message_transferred_msg_count().load(Ordering::Relaxed), 2);

    let miss_result = store
        .get_message(&group, &topic, 0, 2, 32, None)
        .await
        .expect("miss get result");
    assert_ne!(miss_result.status(), Some(GetMessageStatus::Found));
    assert_eq!(stats.get_message_times_total_found().load(Ordering::Relaxed), 1);
    assert_eq!(stats.get_message_times_total_miss().load(Ordering::Relaxed), 1);
    assert_eq!(stats.get_message_transferred_msg_count().load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn runtime_info_includes_store_offsets_and_timer_defaults_when_timer_disabled() {
    let temp_dir = tempdir().unwrap();
    let store = new_configured_test_store(
        &temp_dir,
        MessageStoreConfig {
            timer_wheel_enable: false,
            ..MessageStoreConfig::default()
        },
    );

    let runtime_info = store.get_runtime_info();

    assert!(runtime_info.contains_key("putMessageTimesTotal"));
    assert!(runtime_info.contains_key(RunningStats::CommitLogMinOffset.as_str()));
    assert!(runtime_info.contains_key(RunningStats::CommitLogMaxOffset.as_str()));
    assert_eq!(runtime_info["storeType"], "LocalFile");
    assert_eq!(runtime_info["rocksdbCqDoubleWriteEnable"], "false");
    assert_eq!(runtime_info["rocksdbCompatibilityMode"], "disabled");
    assert!(runtime_info.contains_key("ioUringBackendStatus"));
    assert_eq!(runtime_info["timerReadBehind"], "0");
    assert_eq!(runtime_info["timerOffsetBehind"], "0");
    assert_eq!(runtime_info["timerCongestNum"], "0");
    assert_eq!(runtime_info["timerEnqueueTps"], "0.0");
    assert_eq!(runtime_info["timerDequeueTps"], "0.0");
    assert_eq!(runtime_info["timerTopicBacklogDistribution"], "{}");
    assert_eq!(runtime_info["timerBacklogDistribution"], "{}");
    assert_eq!(runtime_info["putMessageLockAcquireTotal"], "0");
    assert_eq!(runtime_info["putMessageLockWaitTotalMillis"], "0");
    assert_eq!(runtime_info["putMessageLockWaitMaxMillis"], "0");
    assert_eq!(runtime_info["putMessageLockHoldTotalMillis"], "0");
    assert_eq!(runtime_info["putMessageLockHoldMaxMillis"], "0");
    assert_eq!(runtime_info["syncFlushQueueDepth"], "0");
    assert_eq!(runtime_info["syncFlushEnqueueTotal"], "0");
    assert_eq!(runtime_info["syncFlushCompletedTotal"], "0");
    assert_eq!(runtime_info["syncFlushTimeoutTotal"], "0");
    assert_eq!(runtime_info["syncFlushOldestWaitMillis"], "0");
    assert_eq!(runtime_info["syncFlushMaxWaitMillis"], "0");
    assert_eq!(runtime_info["syncFlushWaitTotalMillis"], "0");
    assert_eq!(runtime_info["reputDispatchBehindBytes"], "0");
    assert_eq!(runtime_info["reputDispatchBatchCountTotal"], "0");
    assert_eq!(runtime_info["reputDispatchRequestTotal"], "0");
    assert_eq!(runtime_info["reputDispatchBatchSizeMax"], "0");
    assert_eq!(runtime_info["reputDispatchDurationTotalMillis"], "0");
    assert_eq!(runtime_info["reputDispatchDurationMaxMillis"], "0");
}

#[tokio::test]
async fn get_ha_runtime_info_reports_current_commitlog_max_offset() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_configured_test_store(
        &temp_dir,
        MessageStoreConfig {
            mapped_file_size_commit_log: 32,
            ..MessageStoreConfig::default()
        },
    );
    store.init().await.expect("init store");

    let data = b"ha";
    assert!(store
        .get_commit_log_mut()
        .append_data(0, data, 0, data.len() as i32)
        .await
        .expect("append commitlog data"));

    let ha_runtime_info = store.get_ha_runtime_info().expect("HA service should be initialized");

    assert!(ha_runtime_info.master);
    assert_eq!(ha_runtime_info.master_commit_log_max_offset, data.len() as u64);
    assert_eq!(ha_runtime_info.in_sync_slave_nums, 0);
}

#[tokio::test]
async fn query_message_returns_indexed_message_after_reput() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_async_flush_test_store(&temp_dir);
    let topic = CheetahString::from_static_str("query-message-after-reput-topic");
    let key = CheetahString::from_static_str("lookup-key");

    let mut msg = MessageExtBrokerInner::default();
    msg.set_topic(topic.clone());
    msg.message_ext_inner.set_queue_id(0);
    msg.set_body(Bytes::from_static(b"query-body"));
    msg.set_keys(key.clone());

    let put_result = store.put_message(msg).await;
    assert_eq!(put_result.put_message_status(), PutMessageStatus::PutOk);

    store.reput_once().await;

    let result = store
        .query_message(&topic, &key, 10, 0, i64::MAX)
        .await
        .expect("query message result");

    assert_eq!(result.message_maped_list.len(), 1);
    assert!(result.buffer_total_size > 0);
    assert!(result.index_last_update_timestamp > 0);
    assert!(result.get_message_data().is_some());
}

#[tokio::test]
async fn reput_once_records_dispatch_batch_runtime_info() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_async_flush_test_store(&temp_dir);
    let topic = CheetahString::from_static_str("reput-dispatch-runtime-topic");

    for index in 0..33 {
        let body = Bytes::from(format!("reput-dispatch-body-{index}"));
        let put_result = store.put_message(build_test_message(&topic, body)).await;
        assert_eq!(put_result.put_message_status(), PutMessageStatus::PutOk);
    }

    store.reput_once().await;

    let runtime_info = store.get_runtime_info();
    assert_eq!(runtime_info["reputDispatchBehindBytes"], "0");
    assert_eq!(runtime_info["reputDispatchBatchCountTotal"], "2");
    assert_eq!(runtime_info["reputDispatchRequestTotal"], "33");
    assert_eq!(runtime_info["reputDispatchBatchSizeMax"], "32");
    assert!(runtime_info.contains_key("reputDispatchDurationTotalMillis"));
    assert!(runtime_info.contains_key("reputDispatchDurationMaxMillis"));
}

#[test]
fn reput_once_initializes_from_minimum_dispatcher_progress() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_test_store(&temp_dir);
    let topic = CheetahString::from_static_str("reput-init-offset-topic");

    for (queue_offset, commit_log_offset) in [(0_i64, 100_i64), (1, 132_i64)] {
        store
            .consume_queue_store
            .put_message_position_info_wrapper(&DispatchRequest {
                topic: topic.clone(),
                queue_id: 0,
                commit_log_offset,
                msg_size: 32,
                consume_queue_offset: queue_offset,
                store_timestamp: 1000 + queue_offset,
                success: true,
                ..DispatchRequest::default()
            });
    }

    store.index_service.build_index(&DispatchRequest {
        topic,
        queue_id: 0,
        commit_log_offset: 100,
        msg_size: 32,
        store_timestamp: 1000,
        keys: CheetahString::from_static_str("lookup-key"),
        success: true,
        ..DispatchRequest::default()
    });

    crate::runtime::test_runtime_owner().block_on(store.reput_once());

    let reput_from_offset = store
        .reput_message_service
        .reput_from_offset
        .as_ref()
        .expect("reput offset should exist")
        .load(Ordering::SeqCst);
    assert_eq!(reput_from_offset, 100);
}

#[test]
fn do_recheck_reput_offset_from_dispatchers_rewinds_to_dispatched_commitlog_offset() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_controller_test_store(&temp_dir, MessageStoreConfig::default());
    let topic = CheetahString::from_static_str("recheck-reput-offset-topic");

    store.set_confirm_offset(256);
    store.reput_message_service.set_reput_from_offset(256);

    for (queue_offset, commit_log_offset) in [(0_i64, 100_i64), (1, 132_i64)] {
        store
            .consume_queue_store
            .put_message_position_info_wrapper(&DispatchRequest {
                topic: topic.clone(),
                queue_id: 0,
                commit_log_offset,
                msg_size: 32,
                consume_queue_offset: queue_offset,
                store_timestamp: 1000 + queue_offset,
                success: true,
                ..DispatchRequest::default()
            });
    }

    store.do_recheck_reput_offset_from_dispatchers();

    let reput_from_offset = store
        .reput_message_service
        .reput_from_offset
        .as_ref()
        .expect("reput offset should exist")
        .load(Ordering::SeqCst);
    assert_eq!(reput_from_offset, 164);
}

#[test]
fn do_recheck_reput_offset_from_dispatchers_uses_minimum_progress_across_cq_and_index() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_controller_test_store(&temp_dir, MessageStoreConfig::default());
    let topic = CheetahString::from_static_str("recheck-reput-offset-with-index-topic");

    store.set_confirm_offset(256);
    store.reput_message_service.set_reput_from_offset(256);

    for (queue_offset, commit_log_offset) in [(0_i64, 100_i64), (1, 132_i64)] {
        store
            .consume_queue_store
            .put_message_position_info_wrapper(&DispatchRequest {
                topic: topic.clone(),
                queue_id: 0,
                commit_log_offset,
                msg_size: 32,
                consume_queue_offset: queue_offset,
                store_timestamp: 1000 + queue_offset,
                success: true,
                ..DispatchRequest::default()
            });
    }

    store.index_service.build_index(&DispatchRequest {
        topic,
        queue_id: 0,
        commit_log_offset: 100,
        msg_size: 32,
        store_timestamp: 1000,
        keys: CheetahString::from_static_str("lookup-key"),
        success: true,
        ..DispatchRequest::default()
    });

    store.do_recheck_reput_offset_from_dispatchers();

    let reput_from_offset = store
        .reput_message_service
        .reput_from_offset
        .as_ref()
        .expect("reput offset should exist")
        .load(Ordering::SeqCst);
    assert_eq!(reput_from_offset, 100);
}

#[test]
fn get_dispatch_recovery_offset_respects_controller_epoch_start_offset() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_controller_test_store(&temp_dir, MessageStoreConfig::default());

    store.set_controller_epoch_start_offset(180);

    assert_eq!(store.get_dispatch_recovery_offset(), 180);
}

#[test]
fn do_recheck_reput_offset_from_dispatchers_respects_controller_epoch_start_offset_floor() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_controller_test_store(&temp_dir, MessageStoreConfig::default());
    let topic = CheetahString::from_static_str("recheck-reput-offset-epoch-floor-topic");

    store.set_confirm_offset(256);
    store.set_controller_epoch_start_offset(180);
    store.reput_message_service.set_reput_from_offset(256);

    for (queue_offset, commit_log_offset) in [(0_i64, 100_i64), (1, 132_i64)] {
        store
            .consume_queue_store
            .put_message_position_info_wrapper(&DispatchRequest {
                topic: topic.clone(),
                queue_id: 0,
                commit_log_offset,
                msg_size: 32,
                consume_queue_offset: queue_offset,
                store_timestamp: 1000 + queue_offset,
                success: true,
                ..DispatchRequest::default()
            });
    }

    store.do_recheck_reput_offset_from_dispatchers();

    let reput_from_offset = store
        .reput_message_service
        .reput_from_offset
        .as_ref()
        .expect("reput offset should exist")
        .load(Ordering::SeqCst);
    assert_eq!(reput_from_offset, 180);
}

#[tokio::test]
async fn calc_delta_checksum_matches_truncated_range() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_async_flush_test_store(&temp_dir);
    let topic = CheetahString::from_static_str("delta-checksum-truncate-topic");

    let mut first = MessageExtBrokerInner::default();
    first.set_topic(topic.clone());
    first.message_ext_inner.set_queue_id(0);
    first.set_keys(CheetahString::from_static_str("first-key"));
    first.set_body(Bytes::from_static(b"first-checksum-body"));

    let first_result = store.put_message(first).await;
    assert_eq!(first_result.put_message_status(), PutMessageStatus::PutOk);
    let first_append = first_result.append_message_result().expect("first append result");
    let first_end = first_append.wrote_offset + first_append.wrote_bytes as i64;

    let mut second = MessageExtBrokerInner::default();
    second.set_topic(topic);
    second.message_ext_inner.set_queue_id(0);
    second.set_keys(CheetahString::from_static_str("second-key"));
    second.set_body(Bytes::from_static(b"second-checksum-body"));

    let second_result = store.put_message(second).await;
    assert_eq!(second_result.put_message_status(), PutMessageStatus::PutOk);
    let second_append = second_result.append_message_result().expect("second append result");
    assert_eq!(second_append.wrote_offset, first_end);

    let checksum_before_truncate = store.calc_delta_checksum(0, second_append.wrote_offset);
    assert!(!checksum_before_truncate.is_empty());

    assert!(store
        .truncate_files(second_append.wrote_offset)
        .expect("truncate succeeds"));

    let checksum_after_truncate = store.calc_delta_checksum(0, store.get_max_phy_offset());
    assert_eq!(store.get_max_phy_offset(), second_append.wrote_offset);
    assert_eq!(checksum_before_truncate, checksum_after_truncate);
}

#[tokio::test]
async fn truncate_files_keeps_checksum_boundary_consistent() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_async_flush_test_store(&temp_dir);
    let topic = CheetahString::from_static_str("truncate-checksum-boundary-topic");

    for (index, body) in [
        Bytes::from_static(b"boundary-body-1"),
        Bytes::from_static(b"boundary-body-2"),
    ]
    .into_iter()
    .enumerate()
    {
        let mut msg = MessageExtBrokerInner::default();
        msg.set_topic(topic.clone());
        msg.message_ext_inner.set_queue_id(0);
        msg.set_keys(CheetahString::from_string(format!("boundary-key-{index}")));
        msg.set_body(body);

        let result = store.put_message(msg).await;
        assert_eq!(result.put_message_status(), PutMessageStatus::PutOk);
    }

    let max_phy_offset_before_truncate = store.get_max_phy_offset();
    let first_message_end = store
        .look_message_by_offset(0)
        .map(|message| message.commit_log_offset + message.store_size as i64)
        .expect("first message");

    let full_checksum = store.calc_delta_checksum(0, max_phy_offset_before_truncate);
    let first_range_checksum = store.calc_delta_checksum(0, first_message_end);
    assert!(!full_checksum.is_empty());
    assert!(!first_range_checksum.is_empty());
    assert_ne!(full_checksum, first_range_checksum);

    assert!(store.truncate_files(first_message_end).expect("truncate succeeds"));

    let truncated_checksum = store.calc_delta_checksum(0, store.get_max_phy_offset());
    assert_eq!(store.get_max_phy_offset(), first_message_end);
    assert_eq!(truncated_checksum, first_range_checksum);
}

#[tokio::test]
async fn put_message_with_multi_dispatch_properties_dispatches_lmq_queues_after_reput() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_configured_test_store(
        &temp_dir,
        MessageStoreConfig {
            enable_multi_dispatch: true,
            enable_lmq: true,
            flush_disk_type: FlushDiskType::AsyncFlush,
            ..MessageStoreConfig::default()
        },
    );
    let source_topic = CheetahString::from_static_str("multi-dispatch-source-topic");
    let lmq_alpha = CheetahString::from_static_str("%LMQ%alpha");
    let lmq_beta = CheetahString::from_static_str("%LMQ%beta");

    let mut msg = MessageExtBrokerInner::default();
    msg.set_topic(source_topic);
    msg.message_ext_inner.set_queue_id(0);
    msg.set_body(Bytes::from_static(b"multi-dispatch-body"));
    msg.put_property(
        CheetahString::from_static_str(MessageConst::PROPERTY_INNER_MULTI_DISPATCH),
        CheetahString::from_static_str("%LMQ%alpha,%LMQ%beta"),
    );

    let put_result = store.put_message(msg).await;
    assert_eq!(put_result.put_message_status(), PutMessageStatus::PutOk);

    store.reput_once().await;

    let alpha_queue = store.consume_queue_store.find_or_create_consume_queue(&lmq_alpha, 0);
    let beta_queue = store.consume_queue_store.find_or_create_consume_queue(&lmq_beta, 0);

    assert_eq!(alpha_queue.read().get_message_total_in_queue(), 1);
    assert_eq!(beta_queue.read().get_message_total_in_queue(), 1);
    assert_eq!(store.consume_queue_store.get_lmq_queue_offset("%LMQ%alpha-0"), 1);
    assert_eq!(store.consume_queue_store.get_lmq_queue_offset("%LMQ%beta-0"), 1);
}

#[tokio::test]
async fn put_message_with_existing_multi_queue_offsets_still_updates_lmq_offsets() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_configured_test_store(
        &temp_dir,
        MessageStoreConfig {
            enable_multi_dispatch: true,
            enable_lmq: true,
            flush_disk_type: FlushDiskType::AsyncFlush,
            ..MessageStoreConfig::default()
        },
    );
    let source_topic = CheetahString::from_static_str("multi-dispatch-existing-offset-topic");

    let mut msg = MessageExtBrokerInner::default();
    msg.set_topic(source_topic);
    msg.message_ext_inner.set_queue_id(0);
    msg.set_body(Bytes::from_static(b"multi-dispatch-existing-offset-body"));
    msg.put_property(
        CheetahString::from_static_str(MessageConst::PROPERTY_INNER_MULTI_DISPATCH),
        CheetahString::from_static_str("%LMQ%alpha,%LMQ%beta"),
    );
    msg.put_property(
        CheetahString::from_static_str(MessageConst::PROPERTY_INNER_MULTI_QUEUE_OFFSET),
        CheetahString::from_static_str("3,5"),
    );

    let put_result = store.put_message(msg).await;

    assert_eq!(put_result.put_message_status(), PutMessageStatus::PutOk);
    assert_eq!(store.consume_queue_store.get_lmq_queue_offset("%LMQ%alpha-0"), 1);
    assert_eq!(store.consume_queue_store.get_lmq_queue_offset("%LMQ%beta-0"), 1);
}

#[tokio::test]
async fn put_message_with_existing_mixed_multi_queue_offsets_updates_lmq_offsets() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_configured_test_store(
        &temp_dir,
        MessageStoreConfig {
            enable_multi_dispatch: true,
            enable_lmq: true,
            flush_disk_type: FlushDiskType::AsyncFlush,
            ..MessageStoreConfig::default()
        },
    );
    let source_topic = CheetahString::from_static_str("multi-dispatch-mixed-offset-topic");

    let mut msg = MessageExtBrokerInner::default();
    msg.set_topic(source_topic);
    msg.message_ext_inner.set_queue_id(0);
    msg.set_body(Bytes::from_static(b"multi-dispatch-mixed-offset-body"));
    msg.put_property(
        CheetahString::from_static_str(MessageConst::PROPERTY_INNER_MULTI_DISPATCH),
        CheetahString::from_static_str("%LMQ%alpha,normal-queue"),
    );
    msg.put_property(
        CheetahString::from_static_str(MessageConst::PROPERTY_INNER_MULTI_QUEUE_OFFSET),
        CheetahString::from_static_str("3,5"),
    );

    let put_result = store.put_message(msg).await;

    assert_eq!(put_result.put_message_status(), PutMessageStatus::PutOk);
    assert_eq!(store.consume_queue_store.get_lmq_queue_offset("%LMQ%alpha-0"), 1);
    assert_eq!(store.consume_queue_store.get_lmq_queue_offset("normal-queue-0"), 0);
}

#[test]
fn multi_dispatch_arrival_uses_multi_queue_offsets_without_allocating_vectors() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_configured_test_store(
        &temp_dir,
        MessageStoreConfig {
            enable_lmq: true,
            ..MessageStoreConfig::default()
        },
    );
    let arrivals = install_recording_arriving_listener(&mut store);
    let inner = reput_inner_for_store(&store);
    let mut properties = HashMap::new();
    properties.insert(
        CheetahString::from_static_str(MessageConst::PROPERTY_INNER_MULTI_DISPATCH),
        CheetahString::from_static_str("%LMQ%alpha,%LMQ%beta"),
    );
    properties.insert(
        CheetahString::from_static_str(MessageConst::PROPERTY_INNER_MULTI_QUEUE_OFFSET),
        CheetahString::from_static_str("3,5"),
    );
    let mut dispatch_request = DispatchRequest {
        topic: CheetahString::from_static_str("multi-arrival-source-topic"),
        queue_id: 7,
        tags_code: 11,
        store_timestamp: 99,
        bit_map: Some(vec![1, 2]),
        properties_map: Some(properties),
        ..DispatchRequest::default()
    };

    inner
        .runtime_context
        .notify_message_arrive_for_multi_queue(&mut dispatch_request);

    let arrivals = arrivals.lock().unwrap();
    assert_eq!(arrivals.len(), 2);
    assert_eq!(arrivals[0].topic, CheetahString::from_static_str("%LMQ%alpha"));
    assert_eq!(arrivals[0].queue_id, 0);
    assert_eq!(arrivals[0].logic_offset, 4);
    assert_eq!(arrivals[0].filter_bit_map, Some(vec![1, 2]));
    assert_eq!(arrivals[1].topic, CheetahString::from_static_str("%LMQ%beta"));
    assert_eq!(arrivals[1].queue_id, 0);
    assert_eq!(arrivals[1].logic_offset, 6);
}

#[test]
fn multi_dispatch_arrival_ignores_invalid_offsets_without_panicking() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_configured_test_store(&temp_dir, MessageStoreConfig::default());
    let arrivals = install_recording_arriving_listener(&mut store);
    let inner = reput_inner_for_store(&store);
    let mut properties = HashMap::new();
    properties.insert(
        CheetahString::from_static_str(MessageConst::PROPERTY_INNER_MULTI_DISPATCH),
        CheetahString::from_static_str("queue-a,queue-b"),
    );
    properties.insert(
        CheetahString::from_static_str(MessageConst::PROPERTY_INNER_MULTI_QUEUE_OFFSET),
        CheetahString::from_static_str("1,not-a-number"),
    );
    let mut dispatch_request = DispatchRequest {
        topic: CheetahString::from_static_str("multi-arrival-invalid-offset-topic"),
        queue_id: 3,
        properties_map: Some(properties),
        ..DispatchRequest::default()
    };

    inner
        .runtime_context
        .notify_message_arrive_for_multi_queue(&mut dispatch_request);

    assert!(arrivals.lock().unwrap().is_empty());
}

#[tokio::test]
async fn clean_commit_log_service_run_deletes_expired_files_and_advances_min_offset() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_configured_test_store(
        &temp_dir,
        MessageStoreConfig {
            mapped_file_size_commit_log: 32,
            file_reserved_time: 0,
            delete_commit_log_files_interval: 0,
            destroy_mapped_file_interval_forcibly: 0,
            redelete_hanged_file_interval: 0,
            delete_file_batch_max: 10,
            delete_when: "99".to_string(),
            disk_max_used_space_ratio: 95,
            clean_file_forcibly_enable: false,
            ..MessageStoreConfig::default()
        },
    );
    store.init().await.expect("init store");

    for (offset, fill) in [(0_i64, b'A'), (32, b'B'), (64, b'C')] {
        let data = vec![fill; 32];
        assert!(store
            .get_commit_log_mut()
            .append_data(offset, &data, 0, data.len() as i32)
            .await
            .expect("append commitlog file"));
    }

    assert_eq!(store.get_min_phy_offset(), 0);
    assert_eq!(store.get_last_file_from_offset(), 64);

    store.clean_commit_log_service.execute_delete_files_manually();
    store.clean_commit_log_service.run();

    assert_eq!(store.get_min_phy_offset(), 64);
    assert_eq!(store.get_last_file_from_offset(), 64);
    assert_eq!(store.get_commit_log().get_max_offset(), 96);
}

#[tokio::test]
async fn clean_commit_log_service_never_crosses_derived_wal_pin() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_configured_test_store(
        &temp_dir,
        MessageStoreConfig {
            mapped_file_size_commit_log: 32,
            file_reserved_time: 0,
            delete_commit_log_files_interval: 0,
            destroy_mapped_file_interval_forcibly: 0,
            redelete_hanged_file_interval: 0,
            delete_file_batch_max: 10,
            delete_when: "99".to_string(),
            disk_max_used_space_ratio: 95,
            clean_file_forcibly_enable: false,
            ..MessageStoreConfig::default()
        },
    );
    store.init().await.expect("init store");
    for (offset, fill) in [(0_i64, b'A'), (32, b'B'), (64, b'C')] {
        let data = vec![fill; 32];
        assert!(store
            .get_commit_log_mut()
            .append_data(offset, &data, 0, data.len() as i32)
            .await
            .expect("append commitlog file"));
    }
    let cleanup_policy =
        super::LocalCleanupPolicy::new(store.message_store_config.normalized_local_backend_config().cleanup);
    store.clean_commit_log_service = Arc::new(CleanCommitLogService::new(
        store.message_store_config.clone(),
        store.commit_log.cleanup_handle(),
        store.running_flags.clone(),
        cleanup_policy,
        Some(Arc::new(|| Some(32))),
    ));

    store.clean_commit_log_service.execute_delete_files_manually();
    store.clean_commit_log_service.run();

    assert_eq!(store.get_min_phy_offset(), 32);
    assert_eq!(store.get_last_file_from_offset(), 64);
}

#[tokio::test]
async fn clean_commit_log_service_run_skips_expired_files_outside_delete_window() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_configured_test_store(
        &temp_dir,
        MessageStoreConfig {
            mapped_file_size_commit_log: 32,
            file_reserved_time: 0,
            delete_commit_log_files_interval: 0,
            destroy_mapped_file_interval_forcibly: 0,
            redelete_hanged_file_interval: 0,
            delete_file_batch_max: 10,
            delete_when: "99".to_string(),
            disk_max_used_space_ratio: 95,
            ..MessageStoreConfig::default()
        },
    );
    store.init().await.expect("init store");

    for (offset, fill) in [(0_i64, b'A'), (32, b'B'), (64, b'C')] {
        let data = vec![fill; 32];
        assert!(store
            .get_commit_log_mut()
            .append_data(offset, &data, 0, data.len() as i32)
            .await
            .expect("append commitlog file"));
    }

    store
        .clean_commit_log_service
        .set_disk_clean_decision_override(Some(DiskCleanDecision::default()));
    store.clean_commit_log_service.run();

    assert_eq!(store.get_min_phy_offset(), 0);
    assert_eq!(store.get_last_file_from_offset(), 64);
    assert_eq!(store.get_commit_log().get_max_offset(), 96);
}

#[test]
fn clean_commit_log_service_clamps_disk_cleanup_ratios_like_java() {
    let temp_dir = tempdir().unwrap();
    let store = new_configured_test_store(
        &temp_dir,
        MessageStoreConfig {
            disk_space_warning_level_ratio: 1,
            disk_space_clean_forcibly_ratio: 100,
            disk_max_used_space_ratio: 1,
            ..MessageStoreConfig::default()
        },
    );

    assert_eq!(store.clean_commit_log_service.disk_space_warning_level_ratio(), 0.35);
    assert_eq!(store.clean_commit_log_service.disk_space_clean_forcibly_ratio(), 0.85);
    assert_eq!(store.clean_commit_log_service.disk_max_used_space_ratio(), 0.10);
}

#[test]
fn clean_commit_log_service_manual_delete_uses_java_retry_budget() {
    let temp_dir = tempdir().unwrap();
    let store = new_test_store(&temp_dir);

    store.clean_commit_log_service.execute_delete_files_manually();

    assert_eq!(
        store.clean_commit_log_service.remaining_manual_delete_requests(),
        CleanCommitLogService::MAX_MANUAL_DELETE_FILE_TIMES
    );
    assert!(store.clean_commit_log_service.consume_manual_delete_request());
    assert_eq!(
        store.clean_commit_log_service.remaining_manual_delete_requests(),
        CleanCommitLogService::MAX_MANUAL_DELETE_FILE_TIMES - 1
    );
}

#[test]
fn clean_commit_log_service_uses_lowest_commitlog_path_ratio_like_java() {
    let temp_dir = tempdir().unwrap();
    let store = new_test_store(&temp_dir);
    let missing_path = temp_dir.path().join("missing-commitlog");
    let existing_path = temp_dir.path().join("existing-commitlog");
    fs::create_dir_all(&existing_path).expect("create existing commitlog path");
    let store_path_commit_log = format!(
        "{}{}{}",
        missing_path.display(),
        mix_all::MULTI_PATH_SPLITTER.as_str(),
        existing_path.display()
    );
    let message_store_config = Arc::new(MessageStoreConfig {
        store_path_root_dir: temp_dir.path().to_string_lossy().to_string().into(),
        store_path_commit_log: Some(store_path_commit_log.into()),
        ..MessageStoreConfig::default()
    });
    let cleanup_policy = super::LocalCleanupPolicy::new(message_store_config.normalized_local_backend_config().cleanup);
    let service = CleanCommitLogService::new(
        message_store_config,
        store.commit_log.cleanup_handle(),
        store.running_flags.clone(),
        cleanup_policy,
        None,
    );

    let (ratio, selected_path) = service.min_physic_disk_ratio();

    assert!(ratio < 0.0);
    assert_eq!(selected_path, Some(missing_path.to_string_lossy().into_owned()));
}

#[tokio::test]
async fn clean_commit_log_service_run_deletes_when_disk_usage_is_unavailable() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_configured_test_store(
        &temp_dir,
        MessageStoreConfig {
            mapped_file_size_commit_log: 32,
            file_reserved_time: 0,
            delete_commit_log_files_interval: 0,
            destroy_mapped_file_interval_forcibly: 0,
            redelete_hanged_file_interval: 0,
            delete_file_batch_max: 10,
            delete_when: "99".to_string(),
            disk_max_used_space_ratio: 95,
            clean_file_forcibly_enable: false,
            ..MessageStoreConfig::default()
        },
    );
    store.init().await.expect("init store");
    let logic_path = LocalFileMessageStore::get_store_path_logic(&store.message_store_config);
    fs::remove_dir_all(logic_path).expect("remove logic store path");

    for (offset, fill) in [(0_i64, b'A'), (32, b'B'), (64, b'C')] {
        let data = vec![fill; 32];
        assert!(store
            .get_commit_log_mut()
            .append_data(offset, &data, 0, data.len() as i32)
            .await
            .expect("append commitlog file"));
    }

    store.clean_commit_log_service.run();

    assert_eq!(store.get_min_phy_offset(), 64);
    assert_eq!(store.get_last_file_from_offset(), 64);
}

#[tokio::test]
async fn correct_logic_offset_service_run_updates_min_offset_after_commitlog_cleanup() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_configured_test_store(
        &temp_dir,
        MessageStoreConfig {
            mapped_file_size_commit_log: 32,
            mapped_file_size_consume_queue: 40,
            file_reserved_time: 0,
            delete_commit_log_files_interval: 0,
            destroy_mapped_file_interval_forcibly: 0,
            redelete_hanged_file_interval: 0,
            delete_file_batch_max: 10,
            delete_when: "99".to_string(),
            disk_max_used_space_ratio: 95,
            clean_file_forcibly_enable: false,
            ..MessageStoreConfig::default()
        },
    );
    store.init().await.expect("init store");

    for (offset, fill) in [(0_i64, b'A'), (32, b'B'), (64, b'C')] {
        let data = vec![fill; 32];
        assert!(store
            .get_commit_log_mut()
            .append_data(offset, &data, 0, data.len() as i32)
            .await
            .expect("append commitlog file"));
    }

    let topic = CheetahString::from_static_str("correct-logic-offset-topic");
    for (queue_offset, commit_log_offset) in [(0_i64, 0_i64), (1, 32), (2, 64)] {
        store
            .consume_queue_store
            .put_message_position_info_wrapper(&DispatchRequest {
                topic: topic.clone(),
                queue_id: 0,
                commit_log_offset,
                msg_size: 32,
                consume_queue_offset: queue_offset,
                ..Default::default()
            });
    }

    let consume_queue = store.consume_queue_store.find_or_create_consume_queue(&topic, 0);
    assert_eq!(consume_queue.read().get_min_offset_in_queue(), 0);

    store.clean_commit_log_service.execute_delete_files_manually();
    store.clean_commit_log_service.run();
    assert_eq!(store.get_min_phy_offset(), 64);

    store.correct_logic_offset_service.run();

    let consume_queue = store.consume_queue_store.find_or_create_consume_queue(&topic, 0);
    assert_eq!(consume_queue.read().get_min_offset_in_queue(), 2);
    assert_eq!(consume_queue.read().get_message_total_in_queue(), 1);
}

#[tokio::test]
async fn clean_consume_queue_service_run_cleans_files_and_removes_fully_expired_queue() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_configured_test_store(
        &temp_dir,
        MessageStoreConfig {
            mapped_file_size_commit_log: 32,
            mapped_file_size_consume_queue: 40,
            file_reserved_time: 0,
            delete_commit_log_files_interval: 0,
            destroy_mapped_file_interval_forcibly: 0,
            redelete_hanged_file_interval: 0,
            delete_file_batch_max: 10,
            delete_when: "99".to_string(),
            disk_max_used_space_ratio: 95,
            clean_file_forcibly_enable: false,
            ..MessageStoreConfig::default()
        },
    );
    store.init().await.expect("init store");

    for (offset, fill) in [(0_i64, b'A'), (32, b'B'), (64, b'C')] {
        let data = vec![fill; 32];
        assert!(store
            .get_commit_log_mut()
            .append_data(offset, &data, 0, data.len() as i32)
            .await
            .expect("append commitlog file"));
    }

    let active_topic = CheetahString::from_static_str("active-clean-topic");
    for (queue_offset, commit_log_offset) in [(0_i64, 0_i64), (1, 32), (2, 64)] {
        store
            .consume_queue_store
            .put_message_position_info_wrapper(&DispatchRequest {
                topic: active_topic.clone(),
                queue_id: 0,
                commit_log_offset,
                msg_size: 32,
                consume_queue_offset: queue_offset,
                ..Default::default()
            });
    }

    let expired_topic = CheetahString::from_static_str("expired-clean-topic");
    store
        .consume_queue_store
        .put_message_position_info_wrapper(&DispatchRequest {
            topic: expired_topic.clone(),
            queue_id: 0,
            commit_log_offset: 0,
            msg_size: 32,
            consume_queue_offset: 0,
            ..Default::default()
        });

    store.clean_commit_log_service.execute_delete_files_manually();
    store.clean_commit_log_service.run();
    assert_eq!(store.get_min_phy_offset(), 64);

    store.clean_consume_queue_service.run();

    let active_consume_queue = store.consume_queue_store.find_or_create_consume_queue(&active_topic, 0);
    assert_eq!(active_consume_queue.read().get_min_offset_in_queue(), 2);
    assert_eq!(active_consume_queue.read().get_message_total_in_queue(), 1);
    assert!(store
        .consume_queue_store
        .find_consume_queue_map(&expired_topic)
        .is_none());
}

#[tokio::test]
async fn clean_expired_removes_trimmed_queue_directly() {
    let temp_dir = tempdir().unwrap();
    let mut store = new_configured_test_store(
        &temp_dir,
        MessageStoreConfig {
            mapped_file_size_commit_log: 32,
            mapped_file_size_consume_queue: 40,
            file_reserved_time: 0,
            delete_commit_log_files_interval: 0,
            destroy_mapped_file_interval_forcibly: 0,
            redelete_hanged_file_interval: 0,
            delete_file_batch_max: 10,
            delete_when: "99".to_string(),
            disk_max_used_space_ratio: 95,
            clean_file_forcibly_enable: false,
            ..MessageStoreConfig::default()
        },
    );
    store.init().await.expect("init store");

    for (offset, fill) in [(0_i64, b'A'), (32, b'B'), (64, b'C')] {
        let data = vec![fill; 32];
        assert!(store
            .get_commit_log_mut()
            .append_data(offset, &data, 0, data.len() as i32)
            .await
            .expect("append commitlog file"));
    }

    let expired_topic = CheetahString::from_static_str("expired-clean-direct-topic");
    store
        .consume_queue_store
        .put_message_position_info_wrapper(&DispatchRequest {
            topic: expired_topic.clone(),
            queue_id: 0,
            commit_log_offset: 0,
            msg_size: 32,
            consume_queue_offset: 0,
            ..Default::default()
        });

    store.clean_commit_log_service.execute_delete_files_manually();
    store.clean_commit_log_service.run();
    store.correct_logic_offset_service.run();

    let expired_consume_queue = store
        .consume_queue_store
        .find_or_create_consume_queue(&expired_topic, 0);
    assert_eq!(expired_consume_queue.read().get_min_offset_in_queue(), 1);
    assert_eq!(expired_consume_queue.read().get_message_total_in_queue(), 0);

    store.consume_queue_store.clean_expired(64).await;

    assert!(store
        .consume_queue_store
        .find_consume_queue_map(&expired_topic)
        .is_none());
}
