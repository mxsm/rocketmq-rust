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

//! Canonical capability projections for the composed message store.
//!
//! Broker request paths depend on the smallest capability that serves their
//! use case. The broad backend adapter remains crate-internal implementation
//! glue and is not part of the Broker-facing API.

use std::any::Any;
use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use rocketmq_model::common::boundary_type::BoundaryType;
use rocketmq_model::common::broker::broker_role::BrokerRole;
use rocketmq_model::common::message::message_batch::MessageExtBatch;
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_protocol::protocol::body::ha_runtime_info::HARuntimeInfo;
use rocketmq_store_api::AppendReceipt;
use rocketmq_store_api::AppendReceiptError;
use rocketmq_store_api::AppendStatus;
use rocketmq_store_api::Durability;
use rocketmq_store_api::FlushBacklog as ApiFlushBacklog;
use rocketmq_store_api::GetResult;
use rocketmq_store_api::GetStatus;
use rocketmq_store_api::LeasedBytes;
use rocketmq_store_api::MessageReader;
use rocketmq_store_api::QueryResult;
use rocketmq_store_api::ReadCacheState;
use rocketmq_store_api::SelectResult;
use rocketmq_store_api::StoreComponent;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreErrorKind;
use rocketmq_store_api::StoreHealth;
use rocketmq_store_api::StoreHealthSnapshot as ApiStoreHealthSnapshot;
use rocketmq_store_api::TimerRecallRequest;
use rocketmq_store_api::TimerRecallStatus;

use crate::base::backend_ops::BackendOps;
use crate::base::backend_ops::MessageStoreShutdownReport;
use crate::base::backend_ops::PutMessagePreflight;
use crate::base::backend_ops::StateMachineVersionView;
use crate::base::backend_ops::StoreHealthSnapshot as BackendStoreHealthSnapshot;
use crate::base::commit_log_dispatcher::CommitLogDispatcher;
use crate::base::get_message_result::GetMessageResult;
use crate::base::message_result::PutMessageResult;
use crate::base::message_status_enum::GetMessageStatus;
use crate::base::message_status_enum::PutMessageStatus;
use crate::base::query_message_result::QueryMessageResult;
use crate::base::select_result::SelectMappedBufferCacheState;
use crate::base::select_result::SelectMappedBufferResult;
use crate::base::store_checkpoint::StoreCheckpoint;
use crate::config::message_store_config::MessageStoreConfig;
use crate::consume_queue::mapped_file_queue::FlushProgress;
use crate::filter::ArcMessageFilter;
use crate::ha::general_ha_service::GeneralHAService;
use crate::hook::put_message_hook::BoxedPutMessageHook;
use crate::log_file::commit_log::CommitLog;
use crate::queue::ArcConsumeQueue;
use crate::stats::broker_stats_manager::BrokerStatsManager;
use crate::store::running_flags::RunningFlags;
use crate::timer::timer_message_store::TimerMessageStore;

/// Supported CommitLog access patterns exposed through the Store administration capability.
///
/// Numeric values remain only at the remoting protocol boundary. Store callers use this enum so
/// arbitrary platform memory-advice integers cannot cross the safe capability API.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CommitLogReadMode {
    /// Use the platform's normal access heuristic and enable sequential read-ahead.
    Normal,
    /// Prefer random-access behavior and disable sequential read-ahead.
    Random,
}

impl CommitLogReadMode {
    const NORMAL_WIRE_VALUE: i32 = 0;
    const RANDOM_WIRE_VALUE: i32 = 1;

    /// Converts the legacy administration request value into a supported mode.
    pub const fn from_wire_value(value: i32) -> Option<Self> {
        match value {
            Self::NORMAL_WIRE_VALUE => Some(Self::Normal),
            Self::RANDOM_WIRE_VALUE => Some(Self::Random),
            _ => None,
        }
    }

    /// Returns the stable value used by the existing administration protocol.
    pub const fn wire_value(self) -> i32 {
        match self {
            Self::Normal => Self::NORMAL_WIRE_VALUE,
            Self::Random => Self::RANDOM_WIRE_VALUE,
        }
    }
}

/// Sealed adapter from public Broker capabilities to the private backend implementation contract.
///
/// The trait is intentionally not re-exported. Capability users can call only the operations
/// declared by their bound, while Store implementations retain one internal forwarding surface.
#[doc(hidden)]
pub trait BackendAccess: Send + Sync + 'static {
    type Backend: BackendOps;

    fn backend(&self) -> &Self::Backend;

    fn backend_mut(&mut self) -> &mut Self::Backend;
}

impl<T> BackendAccess for T
where
    T: BackendOps,
{
    type Backend = T;

    fn backend(&self) -> &Self::Backend {
        self
    }

    fn backend_mut(&mut self) -> &mut Self::Backend {
        self
    }
}

/// Broker composition and lifecycle ownership boundary.
///
/// Request paths should prefer one of the narrower use-case markers below.
pub trait BrokerStorePort: BrokerAdminStore + BrokerReplicationStore {
    fn load(&mut self) -> impl Future<Output = bool> + Send {
        BackendOps::load(self.backend_mut())
    }

    fn start(&mut self) -> impl Future<Output = Result<(), crate::store_error::StoreError>> + Send {
        BackendOps::start(self.backend_mut())
    }

    fn init(&mut self) -> impl Future<Output = Result<(), crate::store_error::StoreError>> + Send {
        BackendOps::init(self.backend_mut())
    }

    fn shutdown_gracefully(
        &mut self,
    ) -> impl Future<Output = Result<MessageStoreShutdownReport, crate::store_error::StoreError>> + Send {
        BackendOps::shutdown_gracefully(self.backend_mut())
    }

    fn shutdown(&mut self) -> impl Future<Output = ()> + Send {
        BackendOps::shutdown(self.backend_mut())
    }

    fn destroy(&mut self) {
        BackendOps::destroy(self.backend_mut());
    }

    fn set_timer_message_store(&mut self, timer_message_store: Arc<TimerMessageStore>) {
        BackendOps::set_timer_message_store(self.backend_mut(), timer_message_store);
    }

    fn get_commit_log_mut(&mut self) -> &mut CommitLog {
        BackendOps::get_commit_log_mut(self.backend_mut())
    }

    fn recover_topic_queue_table(&mut self) {
        BackendOps::recover_topic_queue_table(self.backend_mut());
    }

    fn add_first_dispatcher(&mut self, dispatcher: Arc<dyn CommitLogDispatcher>) {
        BackendOps::add_first_dispatcher(self.backend_mut(), dispatcher);
    }

    fn set_put_message_hook(&mut self, put_message_hook: BoxedPutMessageHook) {
        BackendOps::set_put_message_hook(self.backend_mut(), put_message_hook);
    }
}

/// Broker message-read and logical-offset capability set.
pub trait BrokerReadStore: BackendAccess {
    fn get_message(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        max_msg_nums: i32,
        message_filter: Option<ArcMessageFilter>,
    ) -> impl Future<Output = Option<GetMessageResult>> + Send {
        BackendOps::get_message(
            self.backend(),
            group,
            topic,
            queue_id,
            offset,
            max_msg_nums,
            message_filter,
        )
    }

    #[allow(clippy::too_many_arguments, reason = "preserves the Store read contract")]
    fn get_message_with_size_limit(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        max_msg_nums: i32,
        max_total_msg_size: i32,
        message_filter: Option<ArcMessageFilter>,
    ) -> impl Future<Output = Option<GetMessageResult>> + Send {
        BackendOps::get_message_with_size_limit(
            self.backend(),
            group,
            topic,
            queue_id,
            offset,
            max_msg_nums,
            max_total_msg_size,
            message_filter,
        )
    }

    fn query_message(
        &self,
        topic: &CheetahString,
        key: &CheetahString,
        max_num: i32,
        begin: i64,
        end: i64,
    ) -> impl Future<Output = Option<QueryMessageResult>> + Send {
        BackendOps::query_message(self.backend(), topic, key, max_num, begin, end)
    }

    fn get_max_offset_in_queue(&self, topic: &CheetahString, queue_id: i32) -> i64 {
        BackendOps::get_max_offset_in_queue(self.backend(), topic, queue_id)
    }

    fn get_max_offset_in_queue_committed(&self, topic: &CheetahString, queue_id: i32, committed: bool) -> i64 {
        BackendOps::get_max_offset_in_queue_committed(self.backend(), topic, queue_id, committed)
    }

    fn get_min_offset_in_queue(&self, topic: &CheetahString, queue_id: i32) -> i64 {
        BackendOps::get_min_offset_in_queue(self.backend(), topic, queue_id)
    }

    fn get_commit_log_offset_in_queue(&self, topic: &CheetahString, queue_id: i32, consume_queue_offset: i64) -> i64 {
        BackendOps::get_commit_log_offset_in_queue(self.backend(), topic, queue_id, consume_queue_offset)
    }

    fn get_offset_in_queue_by_time_async(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        timestamp: i64,
    ) -> impl Future<Output = Result<i64, crate::store_error::StoreError>> + Send {
        BackendOps::get_offset_in_queue_by_time_async(self.backend(), topic, queue_id, timestamp)
    }

    fn get_offset_in_queue_by_time(&self, topic: &CheetahString, queue_id: i32, timestamp: i64) -> i64 {
        BackendOps::get_offset_in_queue_by_time(self.backend(), topic, queue_id, timestamp)
    }

    fn get_offset_in_queue_by_time_with_boundary(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        timestamp: i64,
        boundary_type: BoundaryType,
    ) -> i64 {
        BackendOps::get_offset_in_queue_by_time_with_boundary(self.backend(), topic, queue_id, timestamp, boundary_type)
    }

    fn get_offset_in_queue_by_time_with_boundary_async(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        timestamp: i64,
        boundary_type: BoundaryType,
    ) -> impl Future<Output = Result<i64, crate::store_error::StoreError>> + Send {
        BackendOps::get_offset_in_queue_by_time_with_boundary_async(
            self.backend(),
            topic,
            queue_id,
            timestamp,
            boundary_type,
        )
    }

    fn look_message_by_offset(&self, commit_log_offset: i64) -> Option<MessageExt> {
        BackendOps::look_message_by_offset(self.backend(), commit_log_offset)
    }

    fn look_message_by_offset_with_size(&self, commit_log_offset: i64, size: i32) -> Option<MessageExt> {
        BackendOps::look_message_by_offset_with_size(self.backend(), commit_log_offset, size)
    }

    fn select_one_message_by_offset(&self, commit_log_offset: i64) -> Option<SelectMappedBufferResult> {
        BackendOps::select_one_message_by_offset(self.backend(), commit_log_offset)
    }

    fn select_one_message_by_offset_with_size(
        &self,
        commit_log_offset: i64,
        msg_size: i32,
    ) -> Option<SelectMappedBufferResult> {
        BackendOps::select_one_message_by_offset_with_size(self.backend(), commit_log_offset, msg_size)
    }

    fn get_running_data_info(&self) -> String {
        BackendOps::get_running_data_info(self.backend())
    }

    fn get_timing_message_count(&self, topic: &CheetahString) -> i64 {
        BackendOps::get_timing_message_count(self.backend(), topic)
    }

    fn get_runtime_info(&self) -> HashMap<String, String> {
        BackendOps::get_runtime_info(self.backend())
    }

    fn get_max_phy_offset(&self) -> i64 {
        BackendOps::get_max_phy_offset(self.backend())
    }

    fn get_min_phy_offset(&self) -> i64 {
        BackendOps::get_min_phy_offset(self.backend())
    }

    fn get_earliest_message_time(&self, topic: &CheetahString, queue_id: i32) -> i64 {
        BackendOps::get_earliest_message_time(self.backend(), topic, queue_id)
    }

    fn get_earliest_message_time_store(&self) -> i64 {
        BackendOps::get_earliest_message_time_store(self.backend())
    }

    fn get_message_store_timestamp(&self, topic: &CheetahString, queue_id: i32, consume_queue_offset: i64) -> i64 {
        BackendOps::get_message_store_timestamp(self.backend(), topic, queue_id, consume_queue_offset)
    }

    fn get_message_store_timestamp_async(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        consume_queue_offset: i64,
    ) -> impl Future<Output = Result<i64, crate::store_error::StoreError>> + Send {
        BackendOps::get_message_store_timestamp_async(self.backend(), topic, queue_id, consume_queue_offset)
    }

    fn get_message_total_in_queue(&self, topic: &CheetahString, queue_id: i32) -> i64 {
        BackendOps::get_message_total_in_queue(self.backend(), topic, queue_id)
    }

    fn get_commit_log_data(&self, offset: i64) -> Option<SelectMappedBufferResult> {
        BackendOps::get_commit_log_data(self.backend(), offset)
    }

    fn get_bulk_commit_log_data(&self, offset: i64, size: i32) -> Option<Vec<SelectMappedBufferResult>> {
        BackendOps::get_bulk_commit_log_data(self.backend(), offset, size)
    }

    fn now(&self) -> u64 {
        BackendOps::now(self.backend())
    }

    fn check_in_mem_by_consume_offset(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        consume_offset: i64,
        batch_size: i32,
    ) -> bool {
        BackendOps::check_in_mem_by_consume_offset(self.backend(), topic, queue_id, consume_offset, batch_size)
    }

    fn check_in_store_by_consume_offset(&self, topic: &CheetahString, queue_id: i32, consume_offset: i64) -> bool {
        BackendOps::check_in_store_by_consume_offset(self.backend(), topic, queue_id, consume_offset)
    }

    fn dispatch_behind_bytes(&self) -> i64 {
        BackendOps::dispatch_behind_bytes(self.backend())
    }

    fn get_flushed_where(&self) -> i64 {
        BackendOps::get_flushed_where(self.backend())
    }

    fn get_confirm_offset(&self) -> i64 {
        BackendOps::get_confirm_offset(self.backend())
    }

    fn is_os_page_cache_busy(&self) -> bool {
        BackendOps::is_os_page_cache_busy(self.backend())
    }

    fn put_message_preflight(&self) -> PutMessagePreflight {
        BackendOps::put_message_preflight(self.backend())
    }

    fn backend_health_snapshot(&self) -> BackendStoreHealthSnapshot {
        BackendOps::health_snapshot(self.backend())
    }

    fn lock_time_millis(&self) -> i64 {
        BackendOps::lock_time_millis(self.backend())
    }

    fn is_transient_store_pool_deficient(&self) -> bool {
        BackendOps::is_transient_store_pool_deficient(self.backend())
    }

    fn get_consume_queue(&self, topic: &CheetahString, queue_id: i32) -> Option<ArcConsumeQueue> {
        BackendOps::get_consume_queue(self.backend(), topic, queue_id)
    }

    fn find_consume_queue(&self, topic: &CheetahString, queue_id: i32) -> Option<ArcConsumeQueue> {
        BackendOps::find_consume_queue(self.backend(), topic, queue_id)
    }

    fn get_broker_stats_manager(&self) -> Option<&Arc<BrokerStatsManager>> {
        BackendOps::get_broker_stats_manager(self.backend())
    }

    fn get_message_store_config(&self) -> &MessageStoreConfig {
        BackendOps::get_message_store_config(self.backend())
    }

    fn current_broker_role(&self) -> BrokerRole {
        BackendOps::current_broker_role(self.backend())
    }

    fn data_read_ahead_enabled(&self) -> bool {
        BackendOps::data_read_ahead_enabled(self.backend())
    }

    fn get_commit_log(&self) -> &CommitLog {
        BackendOps::get_commit_log(self.backend())
    }

    fn get_store_checkpoint(&self) -> &StoreCheckpoint {
        BackendOps::get_store_checkpoint(self.backend())
    }

    fn get_queue_store(&self) -> &dyn Any {
        BackendOps::get_queue_store(self.backend())
    }

    fn get_running_flags(&self) -> &RunningFlags {
        BackendOps::get_running_flags(self.backend())
    }

    fn is_sync_disk_flush(&self) -> bool {
        BackendOps::is_sync_disk_flush(self.backend())
    }

    fn is_sync_master(&self) -> bool {
        BackendOps::is_sync_master(self.backend())
    }

    fn get_data(&self, offset: i64, size: i32, byte_buffer: &mut BytesMut) -> bool {
        BackendOps::get_data(self.backend(), offset, size, byte_buffer)
    }

    fn get_master_flushed_offset(&self) -> i64 {
        BackendOps::get_master_flushed_offset(self.backend())
    }

    fn get_broker_init_max_offset(&self) -> i64 {
        BackendOps::get_broker_init_max_offset(self.backend())
    }

    fn get_state_machine_version(&self) -> i64 {
        BackendOps::get_state_machine_version(self.backend())
    }

    fn state_machine_version_view(&self) -> StateMachineVersionView {
        BackendOps::state_machine_version_view(self.backend())
    }

    fn remain_transient_store_buffer_numbs(&self) -> i32 {
        BackendOps::remain_transient_store_buffer_numbs(self.backend())
    }

    fn remain_how_many_data_to_commit(&self) -> i64 {
        BackendOps::remain_how_many_data_to_commit(self.backend())
    }

    fn remain_how_many_data_to_flush(&self) -> i64 {
        BackendOps::remain_how_many_data_to_flush(self.backend())
    }

    fn is_shutdown(&self) -> bool {
        BackendOps::is_shutdown(self.backend())
    }

    fn get_timer_message_store(&self) -> Option<&Arc<TimerMessageStore>> {
        BackendOps::get_timer_message_store(self.backend())
    }
}

/// Broker primary append capability set.
pub trait BrokerWriteStore: BrokerReadStore {
    fn put_message(&mut self, message: MessageExtBrokerInner) -> impl Future<Output = PutMessageResult> + Send {
        BackendOps::put_message(self.backend_mut(), message)
    }

    fn put_messages(&mut self, batch: MessageExtBatch) -> impl Future<Output = PutMessageResult> + Send {
        BackendOps::put_messages(self.backend_mut(), batch)
    }

    fn assign_offset(&self, message: &mut MessageExtBrokerInner) -> Result<(), crate::store_error::StoreError> {
        BackendOps::assign_offset(self.backend(), message)
    }

    fn increase_offset(&self, message: &MessageExtBrokerInner, message_num: i16) {
        BackendOps::increase_offset(self.backend(), message, message_num);
    }

    fn recall_extended_timer(
        &self,
        request: &TimerRecallRequest,
    ) -> Result<TimerRecallStatus, crate::store_error::StoreError> {
        BackendOps::recall_extended_timer(self.backend(), request)
    }
}

/// Broker paths that atomically combine reads, offsets, and writes.
pub trait BrokerReadWriteStore: BrokerReadStore + BrokerWriteStore {
    /// Confirms that the value is admitted to combined read/write paths.
    fn broker_read_write_store(&self) {}
}

/// Broker capability for updating both HA and logical master addresses.
pub trait BrokerMasterAddressStore: BrokerReadStore {
    fn update_logical_master_address(&self, new_addr: &CheetahString) {
        BackendOps::update_master_address(self.backend(), new_addr);
    }

    fn update_master_address(&self, new_addr: &CheetahString) -> impl Future<Output = ()> + Send {
        self.update_master_addresses(new_addr.as_str(), new_addr)
    }

    fn update_master_addresses(
        &self,
        ha_address: &str,
        logical_address: &CheetahString,
    ) -> impl Future<Output = ()> + Send {
        async move {
            BackendOps::update_ha_master_address(self.backend(), ha_address).await;
            self.update_logical_master_address(logical_address);
        }
    }
}

/// Broker administrative and configuration capability set.
pub trait BrokerAdminStore: BrokerReplicationStore {
    fn health_snapshot(&self) -> BackendStoreHealthSnapshot {
        BackendOps::health_snapshot(self.backend())
    }

    fn delete_topics(&self, delete_topics: Vec<&CheetahString>) -> i32 {
        BackendOps::delete_topics(self.backend(), delete_topics)
    }

    fn clean_unused_topic(&self, retain_topics: &HashSet<String>) -> i32 {
        BackendOps::clean_unused_topic(self.backend(), retain_topics)
    }

    fn clean_expired_consumer_queue(&self) {
        BackendOps::clean_expired_consumer_queue(self.backend());
    }

    fn execute_delete_files_manually(&self) {
        BackendOps::execute_delete_files_manually(self.backend());
    }

    fn set_commitlog_read_mode(
        &self,
        read_ahead_mode: CommitLogReadMode,
    ) -> Result<(), crate::store_error::StoreError> {
        BackendOps::set_commitlog_read_mode(self.backend(), read_ahead_mode)
    }

    fn flush(&self) -> i64 {
        BackendOps::flush(self.backend())
    }

    fn try_flush(&self) -> Result<FlushProgress, crate::store_error::StoreError> {
        BackendOps::try_flush(self.backend())
    }

    fn get_dispatcher_list(&self) -> &[Arc<dyn CommitLogDispatcher>] {
        BackendOps::get_dispatcher_list(self.backend())
    }

    fn truncate_dirty_logic_files(&self, phy_offset: i64) {
        BackendOps::truncate_dirty_logic_files(self.backend(), phy_offset);
    }
}

/// Broker HA and replication-control capability set.
pub trait BrokerReplicationStore: BrokerReadWriteStore + BrokerMasterAddressStore {
    fn append_to_commit_log(
        &mut self,
        start_offset: i64,
        data: &[u8],
        data_start: i32,
        data_length: i32,
    ) -> impl Future<Output = Result<bool, crate::store_error::StoreError>> + Send {
        BackendOps::append_to_commit_log(self.backend_mut(), start_offset, data, data_start, data_length)
    }

    fn update_ha_master_address(&self, new_addr: &str) -> impl Future<Output = ()> + Send {
        BackendOps::update_ha_master_address(self.backend(), new_addr)
    }

    fn set_confirm_offset(&mut self, phy_offset: i64) {
        BackendOps::set_confirm_offset(self.backend_mut(), phy_offset);
    }

    fn set_alive_replica_num_in_group(&self, alive_replica_nums: i32) {
        BackendOps::set_alive_replica_num_in_group(self.backend(), alive_replica_nums);
    }

    fn get_alive_replica_num_in_group(&self) -> i32 {
        BackendOps::get_alive_replica_num_in_group(self.backend())
    }

    fn sync_controller_sync_state_set(&self, local_broker_id: i64, sync_state_set: &HashSet<i64>) {
        BackendOps::sync_controller_sync_state_set(self.backend(), local_broker_id, sync_state_set);
    }

    fn wakeup_ha_client(&self) {
        BackendOps::wakeup_ha_client(self.backend());
    }

    fn set_master_flushed_offset(&self, master_flushed_offset: i64) {
        BackendOps::set_master_flushed_offset(self.backend(), master_flushed_offset);
    }

    fn sync_broker_role(&self, broker_role: BrokerRole) {
        BackendOps::sync_broker_role(self.backend(), broker_role);
    }

    fn sync_broker_role_with_term(
        &self,
        broker_role: BrokerRole,
        external_term: u64,
    ) -> Result<(), crate::store_error::StoreError> {
        BackendOps::sync_broker_role_with_term(self.backend(), broker_role, external_term)
    }

    fn get_ha_service(&self) -> Option<&GeneralHAService> {
        BackendOps::get_ha_service(self.backend())
    }

    fn get_ha_runtime_info(&self) -> Option<HARuntimeInfo> {
        BackendOps::get_ha_runtime_info(self.backend())
    }
}

impl<T> BrokerStorePort for T where T: BackendOps {}
impl<T> BrokerReadStore for T where T: BackendOps {}
impl<T> BrokerWriteStore for T where T: BackendOps {}
impl<T> BrokerReadWriteStore for T where T: BrokerReadStore + BrokerWriteStore {}
impl<T> BrokerMasterAddressStore for T where T: BackendOps {}
impl<T> BrokerAdminStore for T where T: BackendOps {}
impl<T> BrokerReplicationStore for T where T: BackendOps {}

/// Backend append output plus independent append and durable progress.
#[derive(Clone)]
pub struct StoreAppendReceipt {
    result: PutMessageResult,
    canonical: Result<AppendReceipt, AppendReceiptError>,
    appended_watermark: i64,
    durable_watermark: i64,
}

impl StoreAppendReceipt {
    /// Returns the backend append result.
    pub const fn result(&self) -> &PutMessageResult {
        &self.result
    }

    /// Returns the canonical backend-neutral receipt projection.
    pub fn canonical(&self) -> Result<&AppendReceipt, &AppendReceiptError> {
        self.canonical.as_ref()
    }

    /// Returns the exclusive primary-log append watermark observed after the operation.
    pub const fn appended_watermark(&self) -> i64 {
        self.appended_watermark
    }

    /// Returns the exclusive durable watermark observed after the operation.
    pub const fn durable_watermark(&self) -> i64 {
        self.durable_watermark
    }
}

/// Builds the canonical receipt without interpreting or rewriting backend status fields.
pub fn store_append_receipt(
    result: PutMessageResult,
    appended_watermark: i64,
    durable_watermark: i64,
) -> StoreAppendReceipt {
    let status = put_status_to_append_status(result.put_message_status());
    let canonical = if status.is_accepted() {
        match result.append_message_result() {
            Some(append) => {
                let start = append.wrote_offset;
                let end = start.saturating_add(i64::from(append.wrote_bytes));
                let durability = if durable_watermark >= end {
                    Durability::Local
                } else {
                    Durability::Memory
                };
                AppendReceipt::try_new(status, start..end, appended_watermark, durable_watermark, durability)
            }
            None => AppendReceipt::try_rejected(status, appended_watermark, durable_watermark),
        }
    } else {
        AppendReceipt::try_rejected(status, appended_watermark, durable_watermark)
    };
    StoreAppendReceipt {
        result,
        canonical,
        appended_watermark,
        durable_watermark,
    }
}

/// Maps every backend append outcome to a distinct neutral status.
pub const fn put_status_to_append_status(status: PutMessageStatus) -> AppendStatus {
    match status {
        PutMessageStatus::PutOk => AppendStatus::PutOk,
        PutMessageStatus::FlushDiskTimeout => AppendStatus::FlushDiskTimeout,
        PutMessageStatus::FlushSlaveTimeout => AppendStatus::FlushReplicaTimeout,
        PutMessageStatus::SlaveNotAvailable => AppendStatus::ReplicaUnavailable,
        PutMessageStatus::ServiceNotAvailable => AppendStatus::ServiceUnavailable,
        PutMessageStatus::CreateMappedFileFailed => AppendStatus::StorageUnavailable,
        PutMessageStatus::MessageIllegal => AppendStatus::InvalidMessage,
        PutMessageStatus::PropertiesSizeExceeded => AppendStatus::PropertiesTooLarge,
        PutMessageStatus::OsPageCacheBusy => AppendStatus::PageCacheBusy,
        PutMessageStatus::UnknownError => AppendStatus::Unknown,
        PutMessageStatus::InSyncReplicasNotEnough => AppendStatus::InsufficientReplicas,
        PutMessageStatus::PutToRemoteBrokerFail => AppendStatus::RemoteAppendFailed,
        PutMessageStatus::LmqConsumeQueueNumExceeded => AppendStatus::QueueLimitExceeded,
        PutMessageStatus::WheelTimerFlowControl => AppendStatus::ScheduleFlowControl,
        PutMessageStatus::WheelTimerMsgIllegal => AppendStatus::ScheduleMessageIllegal,
        PutMessageStatus::WheelTimerNotEnable => AppendStatus::ScheduleDisabled,
    }
}

/// Maps every backend get outcome without changing its semantics.
pub const fn get_status_to_api(status: GetMessageStatus) -> GetStatus {
    match status {
        GetMessageStatus::Found => GetStatus::Found,
        GetMessageStatus::NoMatchedMessage => GetStatus::NoMatchedMessage,
        GetMessageStatus::MessageWasRemoving => GetStatus::MessageWasRemoving,
        GetMessageStatus::OffsetFoundNull => GetStatus::OffsetFoundNull,
        GetMessageStatus::OffsetOverflowBadly => GetStatus::OffsetOverflowBadly,
        GetMessageStatus::OffsetOverflowOne => GetStatus::OffsetOverflowOne,
        GetMessageStatus::OffsetTooSmall => GetStatus::OffsetTooSmall,
        GetMessageStatus::NoMatchedLogicQueue => GetStatus::NoMatchedLogicQueue,
        GetMessageStatus::NoMessageInQueue => GetStatus::NoMessageInQueue,
        GetMessageStatus::OffsetReset => GetStatus::OffsetReset,
    }
}

/// Exact backend health error retained by the Broker admission projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StoreHealthError {
    kind: StoreErrorKind,
    component: StoreComponent,
}

impl StoreHealthError {
    /// Creates an exact projection from the backend kind.
    pub const fn new(kind: StoreErrorKind) -> Self {
        Self {
            kind,
            component: StoreComponent::Store,
        }
    }

    /// Creates a health projection with an exact component token.
    pub const fn in_component(kind: StoreErrorKind, component: StoreComponent) -> Self {
        Self { kind, component }
    }

    /// Creates an exact projection from one canonical store error.
    pub const fn from_error(error: &crate::base::backend_ops::StoreHealthError) -> Self {
        Self {
            kind: error.kind,
            component: error.component,
        }
    }

    /// Returns the original low-cardinality backend token.
    pub const fn backend_token(self) -> &'static str {
        match self.component {
            StoreComponent::Store | StoreComponent::Configuration => self.kind.as_str(),
            component => component.as_str(),
        }
    }
}

/// Compact sync-flush pressure retained by the Broker admission projection.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StoreFlushBacklog {
    pub queue_depth: u64,
    pub oldest_wait_millis: u64,
}

/// Exact health data consumed by Broker admission behavior.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreHealthSnapshot {
    pub writable: bool,
    pub last_error: Option<StoreHealthError>,
    pub page_cache_busy: bool,
    pub transient_pool_deficient: bool,
    pub flush_backlog: StoreFlushBacklog,
    pub dispatch_behind_bytes: i64,
    pub shutdown: bool,
    pub replication_pending_count: u64,
    pub replication_oldest_wait_millis: u64,
    pub appended_watermark: i64,
    pub durable_watermark: i64,
}

impl Default for StoreHealthSnapshot {
    fn default() -> Self {
        Self {
            writable: true,
            last_error: None,
            page_cache_busy: false,
            transient_pool_deficient: false,
            flush_backlog: StoreFlushBacklog::default(),
            dispatch_behind_bytes: 0,
            shutdown: false,
            replication_pending_count: 0,
            replication_oldest_wait_millis: 0,
            appended_watermark: 0,
            durable_watermark: 0,
        }
    }
}

impl StoreHealthSnapshot {
    /// Returns the backend-neutral health result while retaining exact backend data in this value.
    pub fn canonical(&self) -> ApiStoreHealthSnapshot {
        ApiStoreHealthSnapshot {
            writable: self.writable,
            last_error: self.last_error.map(|error| error.kind),
            page_cache_busy: self.page_cache_busy,
            transient_pool_deficient: self.transient_pool_deficient,
            flush_backlog: ApiFlushBacklog {
                queue_depth: self.flush_backlog.queue_depth,
                oldest_wait_millis: self.flush_backlog.oldest_wait_millis,
            },
            dispatch_behind_bytes: self.dispatch_behind_bytes,
            shutdown: self.shutdown,
            replication_pending_count: self.replication_pending_count,
            replication_oldest_wait_millis: self.replication_oldest_wait_millis,
            appended_watermark: self.appended_watermark,
            durable_watermark: self.durable_watermark,
        }
    }
}

/// Read-only capability for admission paths that only need store health.
pub struct MessageStoreHealthCapability<'a, MS> {
    store: &'a MS,
}

impl<'a, MS> MessageStoreHealthCapability<'a, MS> {
    /// Wraps an immutable store view without changing ownership.
    pub fn new(store: &'a MS) -> Self {
        Self { store }
    }
}

/// Narrow port for message-store read methods used by [`MessageReader`].
///
/// Filtered reads remain on the backend trait. This canonical read port intentionally
/// forwards only unfiltered reads, so its hot request path has no dynamic filter allocation.
/// Test doubles can implement these four operations without copying `BackendOps`.
pub(crate) trait MessageStoreReadPort: Sync {
    fn get_message(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        max_messages: i32,
    ) -> impl Future<Output = Option<GetMessageResult>> + Send;

    fn get_message_with_size_limit(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        max_messages: i32,
        max_total_size: i32,
    ) -> impl Future<Output = Option<GetMessageResult>> + Send;

    fn query_message(
        &self,
        topic: &CheetahString,
        key: &CheetahString,
        max_messages: i32,
        begin: i64,
        end: i64,
    ) -> impl Future<Output = Option<QueryMessageResult>> + Send;

    fn select_message(&self, physical_offset: i64, size: Option<i32>) -> Option<SelectMappedBufferResult>;
}

impl<MS: BackendOps> MessageStoreReadPort for MS {
    async fn get_message(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        max_messages: i32,
    ) -> Option<GetMessageResult> {
        BackendOps::get_message(self, group, topic, queue_id, offset, max_messages, None).await
    }

    async fn get_message_with_size_limit(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        max_messages: i32,
        max_total_size: i32,
    ) -> Option<GetMessageResult> {
        BackendOps::get_message_with_size_limit(
            self,
            group,
            topic,
            queue_id,
            offset,
            max_messages,
            max_total_size,
            None,
        )
        .await
    }

    async fn query_message(
        &self,
        topic: &CheetahString,
        key: &CheetahString,
        max_messages: i32,
        begin: i64,
        end: i64,
    ) -> Option<QueryMessageResult> {
        BackendOps::query_message(self, topic, key, max_messages, begin, end).await
    }

    fn select_message(&self, physical_offset: i64, size: Option<i32>) -> Option<SelectMappedBufferResult> {
        match size {
            Some(size) => BackendOps::select_one_message_by_offset_with_size(self, physical_offset, size),
            None => BackendOps::select_one_message_by_offset(self, physical_offset),
        }
    }
}

/// Read-only capability that forwards backend reads through [`MessageReader`].
pub struct MessageStoreReadCapability<'a, MS> {
    store: &'a MS,
}

impl<'a, MS> MessageStoreReadCapability<'a, MS> {
    /// Wraps an immutable store view without changing ownership.
    pub const fn new(store: &'a MS) -> Self {
        Self { store }
    }
}

/// Message-store read calls represented as one closed request enum.
pub enum MessageReadRequest {
    Get {
        group: CheetahString,
        topic: CheetahString,
        queue_id: i32,
        offset: i64,
        max_messages: i32,
        max_total_size: Option<i32>,
    },
    Query {
        topic: CheetahString,
        key: CheetahString,
        max_messages: i32,
        begin: i64,
        end: i64,
    },
    Select {
        physical_offset: i64,
        size: Option<i32>,
    },
}

/// Neutral result variants corresponding to get, query, and selected-buffer reads.
pub enum MessageReadResult {
    Get(GetResult<MessageReadLease>),
    Query(QueryResult<MessageReadLease>),
    Select(SelectResult<MessageReadLease>),
}

/// Lease guard that keeps the backend selected result alive behind the capability boundary.
///
/// The native selected-buffer type remains private and is released by its existing `Drop`
/// implementation when the neutral result is dropped.
pub struct MessageReadLease {
    _selected: SelectMappedBufferResult,
}

/// Converts a backend selected buffer into neutral leased bytes.
pub fn selected_result(selected: SelectMappedBufferResult) -> SelectResult<MessageReadLease> {
    let start_offset = selected.start_offset();
    let cache_state = match selected.cache_state() {
        SelectMappedBufferCacheState::Unknown => ReadCacheState::Unknown,
        SelectMappedBufferCacheState::Hot => ReadCacheState::Hot,
        SelectMappedBufferCacheState::Cold => ReadCacheState::Cold,
    };
    let bytes = selected
        .get_bytes()
        .unwrap_or_else(|| Bytes::copy_from_slice(selected.get_buffer()));
    SelectResult::new(
        start_offset,
        LeasedBytes::new(bytes, MessageReadLease { _selected: selected }),
        cache_state,
    )
}

/// Converts a backend logical get result without changing navigation or accounting fields.
pub fn get_result(result: GetMessageResult) -> GetResult<MessageReadLease> {
    let status = result.status().map(get_status_to_api);
    let queue_offsets = result.message_queue_offset().clone();
    let next_begin_offset = result.next_begin_offset();
    let min_offset = result.min_offset();
    let max_offset = result.max_offset();
    let buffer_total_size = result.buffer_total_size();
    let message_count = result.message_count();
    let suggest_pulling_from_replica = result.suggest_pulling_from_slave();
    let commercial_message_count = result.msg_count4_commercial();
    let commercial_size_per_message = result.commercial_size_per_msg();
    let cold_data_sum = result.cold_data_sum();
    let records = result.message_mapped_vec().into_iter().map(selected_result).collect();
    GetResult {
        records,
        queue_offsets,
        status,
        next_begin_offset,
        min_offset,
        max_offset,
        buffer_total_size,
        message_count,
        suggest_pulling_from_replica,
        commercial_message_count,
        commercial_size_per_message,
        cold_data_sum,
    }
}

/// Converts a backend key-query result without changing its index-safety projection.
pub fn query_result(result: QueryMessageResult) -> QueryResult<MessageReadLease> {
    let QueryMessageResult {
        message_maped_list,
        index_last_update_timestamp,
        index_last_update_phyoffset,
        buffer_total_size,
        index_query_safe,
        index_safe_phyoffset,
        index_confirm_phyoffset,
    } = result;
    QueryResult {
        records: message_maped_list.into_iter().map(selected_result).collect(),
        index_last_update_timestamp,
        index_last_update_physical_offset: index_last_update_phyoffset,
        buffer_total_size,
        index_query_safe,
        index_safe_physical_offset: index_safe_phyoffset,
        index_confirm_physical_offset: index_confirm_phyoffset,
    }
}

impl<MS> MessageReader for MessageStoreReadCapability<'_, MS>
where
    MS: MessageStoreReadPort,
{
    type Request = MessageReadRequest;
    type Output = Option<MessageReadResult>;
    type Error = StoreError;

    async fn read(&self, request: Self::Request) -> Result<Self::Output, Self::Error> {
        let result = match request {
            MessageReadRequest::Get {
                group,
                topic,
                queue_id,
                offset,
                max_messages,
                max_total_size,
            } => {
                let result = match max_total_size {
                    Some(max_total_size) => {
                        self.store
                            .get_message_with_size_limit(&group, &topic, queue_id, offset, max_messages, max_total_size)
                            .await
                    }
                    None => {
                        self.store
                            .get_message(&group, &topic, queue_id, offset, max_messages)
                            .await
                    }
                };
                result.map(get_result).map(MessageReadResult::Get)
            }
            MessageReadRequest::Query {
                topic,
                key,
                max_messages,
                begin,
                end,
            } => self
                .store
                .query_message(&topic, &key, max_messages, begin, end)
                .await
                .map(query_result)
                .map(MessageReadResult::Query),
            MessageReadRequest::Select { physical_offset, size } => self
                .store
                .select_message(physical_offset, size)
                .map(selected_result)
                .map(MessageReadResult::Select),
        };
        Ok(result)
    }
}

impl<MS: BrokerReadStore> StoreHealth for MessageStoreHealthCapability<'_, MS> {
    type Snapshot = StoreHealthSnapshot;

    fn health_snapshot(&self) -> Self::Snapshot {
        store_health_snapshot(self.store)
    }
}

pub fn store_health_snapshot<MS: BrokerReadStore>(store: &MS) -> StoreHealthSnapshot {
    let backend = store.backend_health_snapshot();
    StoreHealthSnapshot {
        writable: backend.writeable,
        last_error: backend.last_flush_error.as_ref().map(StoreHealthError::from_error),
        page_cache_busy: backend.os_page_cache_busy,
        transient_pool_deficient: backend.transient_store_pool_deficient,
        flush_backlog: StoreFlushBacklog {
            queue_depth: backend.sync_flush.queue_depth,
            oldest_wait_millis: backend.sync_flush.oldest_wait_millis,
        },
        dispatch_behind_bytes: backend.dispatch_behind_bytes,
        shutdown: backend.shutdown,
        replication_pending_count: backend.ha_pending_request_count,
        replication_oldest_wait_millis: backend.ha_pending_oldest_wait_millis,
        appended_watermark: store.get_max_phy_offset(),
        durable_watermark: store.get_flushed_where(),
    }
}

#[cfg(test)]
#[path = "capability_test.rs"]
mod tests;
