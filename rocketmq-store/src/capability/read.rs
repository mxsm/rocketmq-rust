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

//! Logical reads and queue metadata projections. Backend objects stay inside Store.

use super::*;
use crate::base::backend_read_ops::BackendReadOps;

/// Backend queue inventory used by Broker administration.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ConsumeQueueStatistics {
    pub lite_queues: i32,
    pub consume_queues: i32,
}

/// Broker message-read and logical-offset capability set.
pub trait BrokerReadStore: BackendAccess {
    fn is_message_in_cold_area(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        queue_offset: i64,
    ) -> bool {
        BackendReadOps::is_message_in_cold_area(self.backend(), group, topic, queue_id, queue_offset)
    }

    fn pickup_store_timestamp(&self, offset: i64, size: i32) -> i64 {
        BackendReadOps::pickup_store_timestamp(self.backend(), offset, size)
    }

    fn get_lmq_max_offset(&self, topic: &CheetahString) -> i64 {
        let offset = BackendReadOps::lmq_queue_offset(self.backend(), topic);
        if offset > 0 {
            offset
        } else {
            self.get_max_offset_in_queue(topic, 0)
        }
    }

    fn is_lmq_exist(&self, topic: &CheetahString) -> bool {
        BackendReadOps::contains_lmq(self.backend(), topic) || self.get_max_offset_in_queue(topic, 0) > 0
    }

    fn get_lmq_topic_names(&self) -> Vec<CheetahString> {
        BackendReadOps::get_lmq_topic_names(self.backend())
    }

    fn get_message(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        max_msg_nums: i32,
        message_filter: Option<ArcMessageFilter>,
    ) -> impl Future<Output = Option<GetMessageResult>> + Send {
        BackendReadOps::get_message(
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
        BackendReadOps::get_message_with_size_limit(
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
        BackendReadOps::query_message(self.backend(), topic, key, max_num, begin, end)
    }

    fn query_message_with_options(
        &self,
        request: &QueryMessageRequest,
    ) -> impl Future<Output = Option<QueryMessageResult>> + Send {
        BackendReadOps::query_message_with_options(self.backend(), request)
    }

    fn get_max_offset_in_queue(&self, topic: &CheetahString, queue_id: i32) -> i64 {
        BackendReadOps::get_max_offset_in_queue(self.backend(), topic, queue_id)
    }

    fn get_max_offset_in_queue_committed(&self, topic: &CheetahString, queue_id: i32, committed: bool) -> i64 {
        BackendReadOps::get_max_offset_in_queue_committed(self.backend(), topic, queue_id, committed)
    }

    fn get_min_offset_in_queue(&self, topic: &CheetahString, queue_id: i32) -> i64 {
        BackendReadOps::get_min_offset_in_queue(self.backend(), topic, queue_id)
    }

    fn get_commit_log_offset_in_queue(&self, topic: &CheetahString, queue_id: i32, consume_queue_offset: i64) -> i64 {
        BackendReadOps::get_commit_log_offset_in_queue(self.backend(), topic, queue_id, consume_queue_offset)
    }

    fn get_offset_in_queue_by_time_async(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        timestamp: i64,
    ) -> impl Future<Output = Result<i64, crate::store_error::StoreError>> + Send {
        BackendReadOps::get_offset_in_queue_by_time_async(self.backend(), topic, queue_id, timestamp)
    }

    fn get_offset_in_queue_by_time(&self, topic: &CheetahString, queue_id: i32, timestamp: i64) -> i64 {
        BackendReadOps::get_offset_in_queue_by_time(self.backend(), topic, queue_id, timestamp)
    }

    fn get_offset_in_queue_by_time_with_boundary(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        timestamp: i64,
        boundary_type: BoundaryType,
    ) -> i64 {
        BackendReadOps::get_offset_in_queue_by_time_with_boundary(
            self.backend(),
            topic,
            queue_id,
            timestamp,
            boundary_type,
        )
    }

    fn get_offset_in_queue_by_time_with_boundary_async(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        timestamp: i64,
        boundary_type: BoundaryType,
    ) -> impl Future<Output = Result<i64, crate::store_error::StoreError>> + Send {
        BackendReadOps::get_offset_in_queue_by_time_with_boundary_async(
            self.backend(),
            topic,
            queue_id,
            timestamp,
            boundary_type,
        )
    }

    fn look_message_by_offset(&self, commit_log_offset: i64) -> Option<MessageExt> {
        BackendReadOps::look_message_by_offset(self.backend(), commit_log_offset)
    }

    fn look_message_by_offset_with_size(&self, commit_log_offset: i64, size: i32) -> Option<MessageExt> {
        BackendReadOps::look_message_by_offset_with_size(self.backend(), commit_log_offset, size)
    }

    fn select_one_message_by_offset(&self, commit_log_offset: i64) -> Option<SelectMappedBufferResult> {
        BackendReadOps::select_one_message_by_offset(self.backend(), commit_log_offset)
    }

    fn select_one_message_by_offset_with_size(
        &self,
        commit_log_offset: i64,
        msg_size: i32,
    ) -> Option<SelectMappedBufferResult> {
        BackendReadOps::select_one_message_by_offset_with_size(self.backend(), commit_log_offset, msg_size)
    }

    fn get_timing_message_count(&self, topic: &CheetahString) -> i64 {
        BackendReadOps::get_timing_message_count(self.backend(), topic)
    }

    fn get_max_phy_offset(&self) -> i64 {
        BackendOps::get_max_phy_offset(self.backend())
    }

    fn get_min_phy_offset(&self) -> i64 {
        BackendOps::get_min_phy_offset(self.backend())
    }

    fn get_earliest_message_time(&self, topic: &CheetahString, queue_id: i32) -> i64 {
        BackendReadOps::get_earliest_message_time(self.backend(), topic, queue_id)
    }

    fn get_earliest_message_time_store(&self) -> i64 {
        BackendReadOps::get_earliest_message_time_store(self.backend())
    }

    fn get_message_store_timestamp(&self, topic: &CheetahString, queue_id: i32, consume_queue_offset: i64) -> i64 {
        BackendReadOps::get_message_store_timestamp(self.backend(), topic, queue_id, consume_queue_offset)
    }

    fn get_message_store_timestamp_async(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        consume_queue_offset: i64,
    ) -> impl Future<Output = Result<i64, crate::store_error::StoreError>> + Send {
        BackendReadOps::get_message_store_timestamp_async(self.backend(), topic, queue_id, consume_queue_offset)
    }

    fn get_message_total_in_queue(&self, topic: &CheetahString, queue_id: i32) -> i64 {
        BackendReadOps::get_message_total_in_queue(self.backend(), topic, queue_id)
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
        BackendReadOps::check_in_mem_by_consume_offset(self.backend(), topic, queue_id, consume_offset, batch_size)
    }

    fn check_in_store_by_consume_offset(&self, topic: &CheetahString, queue_id: i32, consume_offset: i64) -> bool {
        BackendReadOps::check_in_store_by_consume_offset(self.backend(), topic, queue_id, consume_offset)
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

    fn health_snapshot(&self) -> StoreHealthSnapshot {
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

    fn current_broker_role(&self) -> BrokerRole {
        BackendOps::current_broker_role(self.backend())
    }

    fn data_read_ahead_enabled(&self) -> bool {
        BackendOps::data_read_ahead_enabled(self.backend())
    }

    fn get_store_checkpoint(&self) -> &StoreCheckpoint {
        BackendOps::get_store_checkpoint(self.backend())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::store_runtime_config::StoreRuntimeConfig;
    use crate::message_store::local_file_message_store::LocalFileMessageStore;
    use crate::queue::consume_queue_store::ConsumeQueueStoreTrait;
    use rocketmq_model::common::config::TopicConfig;

    fn read_lmq(store: &impl BrokerReadStore, topic: &CheetahString) -> (i64, bool, Vec<CheetahString>) {
        (
            store.get_lmq_max_offset(topic),
            store.is_lmq_exist(topic),
            store.get_lmq_topic_names(),
        )
    }

    #[test]
    fn logical_read_capability_observes_lmq_updates_without_exposing_backend_objects() {
        let directory = tempfile::tempdir().unwrap();
        let config = MessageStoreConfig {
            store_path_root_dir: directory.path().to_string_lossy().as_ref().into(),
            enable_lmq: true,
            timer_wheel_enable: false,
            ..Default::default()
        };
        let mut store = LocalFileMessageStore::new(
            Arc::new(config),
            rocketmq_store_local::commit_log::append::micro_batch::MicroBatchPolicy::disabled(1).unwrap(),
            Arc::new(StoreRuntimeConfig::default()),
            Arc::new(dashmap::DashMap::<CheetahString, Arc<TopicConfig>>::new()),
            None,
            false,
            crate::runtime::test_service_context("read-capability-lmq"),
        )
        .unwrap()
        .unwrap();
        store.wire_owned_root_dependencies().unwrap();
        let topic = CheetahString::from_static_str("%LMQ%parent%child");
        assert_eq!(read_lmq(&store, &topic), (0, false, Vec::new()));
        let queue_store = BackendOps::get_queue_store(&store)
            .downcast_ref::<crate::queue::local_file_consume_queue_store::ConsumeQueueStore>()
            .unwrap();
        queue_store.increase_lmq_offset(&format!("{topic}-0"), 7);
        assert_eq!(read_lmq(&store, &topic), (7, true, vec![topic.clone()]));
        let counts = BackendReadOps::consume_queue_statistics(&store);
        assert_eq!(counts.lite_queues, 1);
        assert_eq!(counts.consume_queues, 1);
        let queue = BrokerReadStore::find_consume_queue(&store, &CheetahString::from("ordinary"), 2).unwrap();
        assert_eq!(queue.read().get_queue_id(), 2);
        assert_eq!(BackendReadOps::consume_queue_statistics(&store).consume_queues, 2);
        assert_eq!(BrokerReadStore::pickup_store_timestamp(&store, 0, 0), -1);
    }
}
