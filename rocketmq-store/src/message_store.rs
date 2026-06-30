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

pub mod local_file_message_store;
pub mod recovery;

#[cfg(feature = "rocksdb_store")]
pub mod rocksdb_message_store;

use std::any::Any;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use rocketmq_common::common::boundary_type::BoundaryType;
use rocketmq_common::common::broker::broker_role::BrokerRole;
use rocketmq_common::common::message::message_batch::MessageExtBatch;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::system_clock::SystemClock;
use rocketmq_remoting::protocol::body::ha_runtime_info::HARuntimeInfo;
use rocketmq_rust::ArcMut;

use crate::base::allocate_mapped_file_service::AllocateMappedFileService;
use crate::base::commit_log_dispatcher::CommitLogDispatcher;
use crate::base::dispatch_request::DispatchRequest;
use crate::base::get_message_result::GetMessageResult;
use crate::base::message_arriving_listener::MessageArrivingListener;
use crate::base::message_result::AppendMessageResult;
use crate::base::message_result::PutMessageResult;
use crate::base::message_store::MessageStore;
use crate::base::query_message_result::QueryMessageResult;
use crate::base::select_result::SelectMappedBufferResult;
use crate::base::store_checkpoint::StoreCheckpoint;
use crate::base::store_stats_service::StoreStatsService;
use crate::base::transient_store_pool::TransientStorePool;
use crate::config::message_store_config::MessageStoreConfig;
use crate::filter::ArcMessageFilter;
use crate::filter::MessageFilter;
use crate::ha::general_ha_service::GeneralHAService;
use crate::hook::put_message_hook::BoxedPutMessageHook;
use crate::hook::put_message_hook::PutMessageHook;
use crate::hook::send_message_back_hook::SendMessageBackHook;
use crate::log_file::commit_log::CommitLog;
use crate::log_file::mapped_file::MappedFile;
use crate::queue::local_file_consume_queue_store::ConsumeQueueStore;
use crate::queue::ArcConsumeQueue;
use crate::stats::broker_stats_manager::BrokerStatsManager;
use crate::store::running_flags::RunningFlags;
use crate::store_error::StoreError;
use crate::timer::timer_message_store::TimerMessageStore;

pub enum GenericMessageStore {
    #[cfg(feature = "local_file_store")]
    LocalFileStore(ArcMut<local_file_message_store::LocalFileMessageStore>),

    #[cfg(feature = "rocksdb_store")]
    RocksDBStore(ArcMut<rocksdb_message_store::RocksDBMessageStore>),
}

impl GenericMessageStore {
    #[cfg(feature = "local_file_store")]
    pub fn local_file(store: ArcMut<local_file_message_store::LocalFileMessageStore>) -> Self {
        Self::LocalFileStore(store)
    }

    #[cfg(feature = "rocksdb_store")]
    pub fn rocksdb(store: ArcMut<rocksdb_message_store::RocksDBMessageStore>) -> Self {
        Self::RocksDBStore(store)
    }

    pub fn set_message_arriving_listener(
        &mut self,
        message_arriving_listener: Option<Arc<Box<dyn MessageArrivingListener + Sync + Send + 'static>>>,
    ) {
        match self {
            #[cfg(feature = "local_file_store")]
            Self::LocalFileStore(store) => store.set_message_arriving_listener(message_arriving_listener),
            #[cfg(feature = "rocksdb_store")]
            Self::RocksDBStore(store) => store
                .local_file_store_mut()
                .set_message_arriving_listener(message_arriving_listener),
        }
    }

    pub async fn reput_once(&mut self) {
        match self {
            #[cfg(feature = "local_file_store")]
            Self::LocalFileStore(store) => store.reput_once().await,
            #[cfg(feature = "rocksdb_store")]
            Self::RocksDBStore(store) => store.local_file_store_mut().reput_once().await,
        }
    }

    pub fn consume_queue_store_mut(&mut self) -> &mut ConsumeQueueStore {
        match self {
            #[cfg(feature = "local_file_store")]
            Self::LocalFileStore(store) => store.consume_queue_store_mut(),
            #[cfg(feature = "rocksdb_store")]
            Self::RocksDBStore(store) => store.local_file_store_mut().consume_queue_store_mut(),
        }
    }

    #[cfg(feature = "rocksdb_store")]
    pub fn rocksdb_ticker_metrics(&self) -> Option<rocketmq_observability::metrics::rocksdb::RocksDbTickerMetrics> {
        match self {
            #[cfg(feature = "local_file_store")]
            Self::LocalFileStore(_) => None,
            Self::RocksDBStore(store) => Some(store.rocksdb_store().ticker_metrics()),
        }
    }

    #[cfg(feature = "tieredstore")]
    pub fn tiered_store_metrics(
        &self,
    ) -> Option<Arc<rocketmq_observability::metrics::tiered_store::TieredStoreMetrics>> {
        match self {
            #[cfg(feature = "local_file_store")]
            Self::LocalFileStore(store) => store.tiered_store_metrics(),
            #[cfg(feature = "rocksdb_store")]
            Self::RocksDBStore(store) => store.local_file_store().tiered_store_metrics(),
        }
    }
}

macro_rules! delegate_store {
    ($self:expr, $method:ident($($arg:expr),* $(,)?)) => {
        match $self {
            #[cfg(feature = "local_file_store")]
            GenericMessageStore::LocalFileStore(store) => store.$method($($arg),*),
            #[cfg(feature = "rocksdb_store")]
            GenericMessageStore::RocksDBStore(store) => store.$method($($arg),*),
        }
    };
}

macro_rules! delegate_store_async {
    ($self:expr, $method:ident($($arg:expr),* $(,)?)) => {
        match $self {
            #[cfg(feature = "local_file_store")]
            GenericMessageStore::LocalFileStore(store) => store.$method($($arg),*).await,
            #[cfg(feature = "rocksdb_store")]
            GenericMessageStore::RocksDBStore(store) => store.$method($($arg),*).await,
        }
    };
}

impl MessageStore for GenericMessageStore {
    async fn load(&mut self) -> bool {
        delegate_store_async!(self, load())
    }

    async fn start(&mut self) -> Result<(), StoreError> {
        delegate_store_async!(self, start())
    }

    async fn init(&mut self) -> Result<(), StoreError> {
        delegate_store_async!(self, init())
    }

    async fn shutdown(&mut self) {
        delegate_store_async!(self, shutdown());
    }

    fn destroy(&mut self) {
        delegate_store!(self, destroy());
    }

    async fn put_message(&mut self, msg: MessageExtBrokerInner) -> PutMessageResult {
        delegate_store_async!(self, put_message(msg))
    }

    async fn put_messages(&mut self, message_ext_batch: MessageExtBatch) -> PutMessageResult {
        delegate_store_async!(self, put_messages(message_ext_batch))
    }

    async fn get_message(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        max_msg_nums: i32,
        message_filter: Option<ArcMessageFilter>,
    ) -> Option<GetMessageResult> {
        delegate_store_async!(
            self,
            get_message(group, topic, queue_id, offset, max_msg_nums, message_filter)
        )
    }

    async fn get_message_with_size_limit(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        max_msg_nums: i32,
        max_total_msg_size: i32,
        message_filter: Option<ArcMessageFilter>,
    ) -> Option<GetMessageResult> {
        delegate_store_async!(
            self,
            get_message_with_size_limit(
                group,
                topic,
                queue_id,
                offset,
                max_msg_nums,
                max_total_msg_size,
                message_filter
            )
        )
    }

    fn get_max_offset_in_queue(&self, topic: &CheetahString, queue_id: i32) -> i64 {
        delegate_store!(self, get_max_offset_in_queue(topic, queue_id))
    }

    fn get_max_offset_in_queue_committed(&self, topic: &CheetahString, queue_id: i32, committed: bool) -> i64 {
        delegate_store!(self, get_max_offset_in_queue_committed(topic, queue_id, committed))
    }

    fn get_min_offset_in_queue(&self, topic: &CheetahString, queue_id: i32) -> i64 {
        delegate_store!(self, get_min_offset_in_queue(topic, queue_id))
    }

    fn get_timer_message_store(&self) -> Option<&Arc<TimerMessageStore>> {
        delegate_store!(self, get_timer_message_store())
    }

    fn set_timer_message_store(&mut self, timer_message_store: Arc<TimerMessageStore>) {
        delegate_store!(self, set_timer_message_store(timer_message_store));
    }

    fn get_commit_log_offset_in_queue(&self, topic: &CheetahString, queue_id: i32, consume_queue_offset: i64) -> i64 {
        delegate_store!(
            self,
            get_commit_log_offset_in_queue(topic, queue_id, consume_queue_offset)
        )
    }

    fn get_offset_in_queue_by_time(&self, topic: &CheetahString, queue_id: i32, timestamp: i64) -> i64 {
        delegate_store!(self, get_offset_in_queue_by_time(topic, queue_id, timestamp))
    }

    fn get_offset_in_queue_by_time_with_boundary(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        timestamp: i64,
        boundary_type: BoundaryType,
    ) -> i64 {
        delegate_store!(
            self,
            get_offset_in_queue_by_time_with_boundary(topic, queue_id, timestamp, boundary_type)
        )
    }

    async fn get_offset_in_queue_by_time_async(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        timestamp: i64,
    ) -> Result<i64, StoreError> {
        delegate_store_async!(self, get_offset_in_queue_by_time_async(topic, queue_id, timestamp))
    }

    async fn get_offset_in_queue_by_time_with_boundary_async(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        timestamp: i64,
        boundary_type: BoundaryType,
    ) -> Result<i64, StoreError> {
        delegate_store_async!(
            self,
            get_offset_in_queue_by_time_with_boundary_async(topic, queue_id, timestamp, boundary_type)
        )
    }

    fn look_message_by_offset(&self, commit_log_offset: i64) -> Option<MessageExt> {
        delegate_store!(self, look_message_by_offset(commit_log_offset))
    }

    fn look_message_by_offset_with_size(&self, commit_log_offset: i64, size: i32) -> Option<MessageExt> {
        delegate_store!(self, look_message_by_offset_with_size(commit_log_offset, size))
    }

    fn select_one_message_by_offset(&self, commit_log_offset: i64) -> Option<SelectMappedBufferResult> {
        delegate_store!(self, select_one_message_by_offset(commit_log_offset))
    }

    fn select_one_message_by_offset_with_size(
        &self,
        commit_log_offset: i64,
        msg_size: i32,
    ) -> Option<SelectMappedBufferResult> {
        delegate_store!(
            self,
            select_one_message_by_offset_with_size(commit_log_offset, msg_size)
        )
    }

    fn get_running_data_info(&self) -> String {
        delegate_store!(self, get_running_data_info())
    }

    fn get_timing_message_count(&self, topic: &CheetahString) -> i64 {
        delegate_store!(self, get_timing_message_count(topic))
    }

    fn get_runtime_info(&self) -> HashMap<String, String> {
        delegate_store!(self, get_runtime_info())
    }

    fn get_max_phy_offset(&self) -> i64 {
        delegate_store!(self, get_max_phy_offset())
    }

    fn get_min_phy_offset(&self) -> i64 {
        delegate_store!(self, get_min_phy_offset())
    }

    fn get_earliest_message_time(&self, topic: &CheetahString, queue_id: i32) -> i64 {
        delegate_store!(self, get_earliest_message_time(topic, queue_id))
    }

    fn get_earliest_message_time_store(&self) -> i64 {
        delegate_store!(self, get_earliest_message_time_store())
    }

    fn get_message_store_timestamp(&self, topic: &CheetahString, queue_id: i32, consume_queue_offset: i64) -> i64 {
        delegate_store!(self, get_message_store_timestamp(topic, queue_id, consume_queue_offset))
    }

    async fn get_message_store_timestamp_async(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        consume_queue_offset: i64,
    ) -> Result<i64, StoreError> {
        delegate_store_async!(
            self,
            get_message_store_timestamp_async(topic, queue_id, consume_queue_offset)
        )
    }

    fn get_message_total_in_queue(&self, topic: &CheetahString, queue_id: i32) -> i64 {
        delegate_store!(self, get_message_total_in_queue(topic, queue_id))
    }

    fn get_commit_log_data(&self, offset: i64) -> Option<SelectMappedBufferResult> {
        delegate_store!(self, get_commit_log_data(offset))
    }

    fn get_bulk_commit_log_data(&self, offset: i64, size: i32) -> Option<Vec<SelectMappedBufferResult>> {
        delegate_store!(self, get_bulk_commit_log_data(offset, size))
    }

    async fn append_to_commit_log(
        &mut self,
        start_offset: i64,
        data: &[u8],
        data_start: i32,
        data_length: i32,
    ) -> Result<bool, StoreError> {
        delegate_store_async!(self, append_to_commit_log(start_offset, data, data_start, data_length))
    }

    fn execute_delete_files_manually(&self) {
        delegate_store!(self, execute_delete_files_manually());
    }

    async fn query_message(
        &self,
        topic: &CheetahString,
        key: &CheetahString,
        max_num: i32,
        begin: i64,
        end: i64,
    ) -> Option<QueryMessageResult> {
        delegate_store_async!(self, query_message(topic, key, max_num, begin, end))
    }

    async fn update_ha_master_address(&self, new_addr: &str) {
        delegate_store_async!(self, update_ha_master_address(new_addr));
    }

    fn update_master_address(&self, new_addr: &CheetahString) {
        delegate_store!(self, update_master_address(new_addr));
    }

    fn slave_fall_behind_much(&self) -> i64 {
        delegate_store!(self, slave_fall_behind_much())
    }

    fn delete_topics(&mut self, delete_topics: Vec<&CheetahString>) -> i32 {
        delegate_store!(self, delete_topics(delete_topics))
    }

    fn clean_unused_topic(&self, retain_topics: &HashSet<String>) -> i32 {
        delegate_store!(self, clean_unused_topic(retain_topics))
    }

    fn clean_expired_consumer_queue(&self) {
        delegate_store!(self, clean_expired_consumer_queue());
    }

    fn check_in_mem_by_consume_offset(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        consume_offset: i64,
        batch_size: i32,
    ) -> bool {
        delegate_store!(
            self,
            check_in_mem_by_consume_offset(topic, queue_id, consume_offset, batch_size)
        )
    }

    fn check_in_store_by_consume_offset(&self, topic: &CheetahString, queue_id: i32, consume_offset: i64) -> bool {
        delegate_store!(self, check_in_store_by_consume_offset(topic, queue_id, consume_offset))
    }

    fn dispatch_behind_bytes(&self) -> i64 {
        delegate_store!(self, dispatch_behind_bytes())
    }

    fn flush(&self) -> i64 {
        delegate_store!(self, flush())
    }

    fn get_flushed_where(&self) -> i64 {
        delegate_store!(self, get_flushed_where())
    }

    fn reset_write_offset(&self, phy_offset: i64) -> bool {
        delegate_store!(self, reset_write_offset(phy_offset))
    }

    fn get_confirm_offset(&self) -> i64 {
        delegate_store!(self, get_confirm_offset())
    }

    fn set_confirm_offset(&mut self, phy_offset: i64) {
        delegate_store!(self, set_confirm_offset(phy_offset));
    }

    fn is_os_page_cache_busy(&self) -> bool {
        delegate_store!(self, is_os_page_cache_busy())
    }

    fn lock_time_millis(&self) -> i64 {
        delegate_store!(self, lock_time_millis())
    }

    fn is_transient_store_pool_deficient(&self) -> bool {
        delegate_store!(self, is_transient_store_pool_deficient())
    }

    fn get_dispatcher_list(&self) -> &[Arc<dyn CommitLogDispatcher>] {
        delegate_store!(self, get_dispatcher_list())
    }

    fn add_dispatcher(&mut self, dispatcher: Arc<dyn CommitLogDispatcher>) {
        delegate_store!(self, add_dispatcher(dispatcher));
    }

    fn add_first_dispatcher(&mut self, dispatcher: Arc<dyn CommitLogDispatcher>) {
        delegate_store!(self, add_first_dispatcher(dispatcher));
    }

    fn get_consume_queue(&self, topic: &CheetahString, queue_id: i32) -> Option<ArcConsumeQueue> {
        delegate_store!(self, get_consume_queue(topic, queue_id))
    }

    fn find_consume_queue(&self, topic: &CheetahString, queue_id: i32) -> Option<ArcConsumeQueue> {
        delegate_store!(self, find_consume_queue(topic, queue_id))
    }

    fn get_broker_stats_manager(&self) -> Option<&Arc<BrokerStatsManager>> {
        delegate_store!(self, get_broker_stats_manager())
    }

    fn on_commit_log_append<MF: MappedFile>(
        &self,
        msg: &MessageExtBrokerInner,
        result: &AppendMessageResult,
        commit_log_file: &MF,
    ) {
        delegate_store!(self, on_commit_log_append(msg, result, commit_log_file));
    }

    fn on_commit_log_dispatch<MF: MappedFile>(
        &self,
        dispatch_request: &DispatchRequest,
        do_dispatch: bool,
        commit_log_file: &MF,
        is_recover: bool,
        is_file_end: bool,
    ) -> Result<(), StoreError> {
        delegate_store!(
            self,
            on_commit_log_dispatch(dispatch_request, do_dispatch, commit_log_file, is_recover, is_file_end)
        )
    }

    fn finish_commit_log_dispatch(&self) {
        delegate_store!(self, finish_commit_log_dispatch());
    }

    fn get_message_store_config(&self) -> &MessageStoreConfig {
        match self {
            #[cfg(feature = "local_file_store")]
            GenericMessageStore::LocalFileStore(store) => store.message_store_config_ref(),
            #[cfg(feature = "rocksdb_store")]
            GenericMessageStore::RocksDBStore(store) => MessageStore::get_message_store_config(store.as_ref()),
        }
    }

    fn get_store_stats_service(&self) -> Arc<StoreStatsService> {
        delegate_store!(self, get_store_stats_service())
    }

    fn get_store_checkpoint(&self) -> &StoreCheckpoint {
        delegate_store!(self, get_store_checkpoint())
    }

    fn get_store_checkpoint_arc(&self) -> Arc<StoreCheckpoint> {
        delegate_store!(self, get_store_checkpoint_arc())
    }

    fn get_system_clock(&self) -> Arc<SystemClock> {
        delegate_store!(self, get_system_clock())
    }

    fn get_commit_log(&self) -> &CommitLog {
        delegate_store!(self, get_commit_log())
    }

    fn get_commit_log_mut_from_ref(&self) -> &mut CommitLog {
        delegate_store!(self, get_commit_log_mut_from_ref())
    }

    fn get_commit_log_mut(&mut self) -> &mut CommitLog {
        delegate_store!(self, get_commit_log_mut())
    }

    fn set_commitlog_read_mode(&mut self, read_ahead_mode: i32) -> Result<(), StoreError> {
        delegate_store!(self, set_commitlog_read_mode(read_ahead_mode))
    }

    fn get_running_flags(&self) -> &RunningFlags {
        delegate_store!(self, get_running_flags())
    }

    fn get_running_flags_arc(&self) -> Arc<RunningFlags> {
        delegate_store!(self, get_running_flags_arc())
    }

    fn get_transient_store_pool(&self) -> Arc<TransientStorePool> {
        delegate_store!(self, get_transient_store_pool())
    }

    fn get_allocate_mapped_file_service(&self) -> Arc<AllocateMappedFileService> {
        delegate_store!(self, get_allocate_mapped_file_service())
    }

    fn truncate_dirty_logic_files(&self, phy_offset: i64) {
        delegate_store!(self, truncate_dirty_logic_files(phy_offset));
    }

    fn unlock_mapped_file<MF: MappedFile>(&self, unlock_mapped_file: &MF) {
        delegate_store!(self, unlock_mapped_file(unlock_mapped_file));
    }

    fn get_queue_store(&self) -> &dyn Any {
        delegate_store!(self, get_queue_store())
    }

    fn is_sync_disk_flush(&self) -> bool {
        delegate_store!(self, is_sync_disk_flush())
    }

    fn is_sync_master(&self) -> bool {
        delegate_store!(self, is_sync_master())
    }

    fn assign_offset(&self, msg: &mut MessageExtBrokerInner) -> Result<(), StoreError> {
        delegate_store!(self, assign_offset(msg))
    }

    fn increase_offset(&self, msg: &MessageExtBrokerInner, message_num: i16) {
        delegate_store!(self, increase_offset(msg, message_num));
    }

    fn get_master_store_in_process<M: MessageStore + Send + Sync + 'static>(&self) -> Option<Arc<M>> {
        match self {
            #[cfg(feature = "local_file_store")]
            GenericMessageStore::LocalFileStore(store) => store.get_master_store_in_process::<M>(),
            #[cfg(feature = "rocksdb_store")]
            GenericMessageStore::RocksDBStore(store) => store.get_master_store_in_process::<M>(),
        }
    }

    fn set_master_store_in_process<M: MessageStore + Send + Sync + 'static>(&self, master_store_in_process: Arc<M>) {
        match self {
            #[cfg(feature = "local_file_store")]
            GenericMessageStore::LocalFileStore(store) => store.set_master_store_in_process(master_store_in_process),
            #[cfg(feature = "rocksdb_store")]
            GenericMessageStore::RocksDBStore(store) => store.set_master_store_in_process(master_store_in_process),
        }
    }

    fn get_data(&self, offset: i64, size: i32, byte_buffer: &mut BytesMut) -> bool {
        delegate_store!(self, get_data(offset, size, byte_buffer))
    }

    fn set_alive_replica_num_in_group(&self, alive_replica_nums: i32) {
        delegate_store!(self, set_alive_replica_num_in_group(alive_replica_nums));
    }

    fn get_alive_replica_num_in_group(&self) -> i32 {
        delegate_store!(self, get_alive_replica_num_in_group())
    }

    fn sync_controller_sync_state_set(&self, local_broker_id: i64, sync_state_set: &HashSet<i64>) {
        delegate_store!(self, sync_controller_sync_state_set(local_broker_id, sync_state_set));
    }

    fn wakeup_ha_client(&self) {
        delegate_store!(self, wakeup_ha_client());
    }

    fn get_master_flushed_offset(&self) -> i64 {
        delegate_store!(self, get_master_flushed_offset())
    }

    fn get_broker_init_max_offset(&self) -> i64 {
        delegate_store!(self, get_broker_init_max_offset())
    }

    fn set_master_flushed_offset(&self, master_flushed_offset: i64) {
        delegate_store!(self, set_master_flushed_offset(master_flushed_offset));
    }

    fn set_broker_init_max_offset(&mut self, broker_init_max_offset: i64) {
        delegate_store!(self, set_broker_init_max_offset(broker_init_max_offset));
    }

    fn sync_broker_role(&mut self, broker_role: BrokerRole) {
        delegate_store!(self, sync_broker_role(broker_role));
    }

    fn calc_delta_checksum(&self, from: i64, to: i64) -> Vec<u8> {
        delegate_store!(self, calc_delta_checksum(from, to))
    }

    fn truncate_files(&self, offset_to_truncate: i64) -> Result<bool, StoreError> {
        delegate_store!(self, truncate_files(offset_to_truncate))
    }

    fn is_offset_aligned(&self, offset: i64) -> bool {
        delegate_store!(self, is_offset_aligned(offset))
    }

    fn get_put_message_hook_list(&self) -> Vec<Arc<dyn PutMessageHook>> {
        delegate_store!(self, get_put_message_hook_list())
    }

    fn set_send_message_back_hook(&self, send_message_back_hook: Arc<dyn SendMessageBackHook>) {
        delegate_store!(self, set_send_message_back_hook(send_message_back_hook));
    }

    fn get_send_message_back_hook(&self) -> Option<Arc<dyn SendMessageBackHook>> {
        delegate_store!(self, get_send_message_back_hook())
    }

    fn get_last_file_from_offset(&self) -> i64 {
        delegate_store!(self, get_last_file_from_offset())
    }

    fn get_last_mapped_file(&self, start_offset: i64) -> bool {
        delegate_store!(self, get_last_mapped_file(start_offset))
    }

    fn set_physical_offset(&self, phy_offset: i64) {
        delegate_store!(self, set_physical_offset(phy_offset));
    }

    fn is_mapped_files_empty(&self) -> bool {
        delegate_store!(self, is_mapped_files_empty())
    }

    fn get_state_machine_version(&self) -> i64 {
        delegate_store!(self, get_state_machine_version())
    }

    fn check_message_and_return_size(
        &self,
        bytes: &mut Bytes,
        check_crc: bool,
        check_dup_info: bool,
        read_body: bool,
    ) -> DispatchRequest {
        delegate_store!(
            self,
            check_message_and_return_size(bytes, check_crc, check_dup_info, read_body)
        )
    }

    fn remain_transient_store_buffer_numbs(&self) -> i32 {
        delegate_store!(self, remain_transient_store_buffer_numbs())
    }

    fn remain_how_many_data_to_commit(&self) -> i64 {
        delegate_store!(self, remain_how_many_data_to_commit())
    }

    fn remain_how_many_data_to_flush(&self) -> i64 {
        delegate_store!(self, remain_how_many_data_to_flush())
    }

    fn is_shutdown(&self) -> bool {
        delegate_store!(self, is_shutdown())
    }

    fn estimate_message_count(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        from: i64,
        to: i64,
        filter: &dyn MessageFilter,
    ) -> i64 {
        delegate_store!(self, estimate_message_count(topic, queue_id, from, to, filter))
    }

    fn recover_topic_queue_table(&mut self) {
        delegate_store!(self, recover_topic_queue_table());
    }

    fn notify_message_arrive_if_necessary(&self, dispatch_request: &mut DispatchRequest) {
        delegate_store!(self, notify_message_arrive_if_necessary(dispatch_request));
    }

    fn set_put_message_hook(&mut self, put_message_hook: BoxedPutMessageHook) {
        delegate_store!(self, set_put_message_hook(put_message_hook));
    }

    fn get_ha_service(&self) -> Option<&GeneralHAService> {
        delegate_store!(self, get_ha_service())
    }

    fn get_ha_runtime_info(&self) -> Option<HARuntimeInfo> {
        delegate_store!(self, get_ha_runtime_info())
    }
}
