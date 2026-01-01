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

use std::any::Any;
use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use rocketmq_common::common::boundary_type::BoundaryType;
use rocketmq_common::common::message::message_batch::MessageExtBatch;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::system_clock::SystemClock;
use rocketmq_common::TimeUtils::get_current_millis;

use crate::base::allocate_mapped_file_service::AllocateMappedFileService;
use crate::base::commit_log_dispatcher::CommitLogDispatcher;
use crate::base::dispatch_request::DispatchRequest;
use crate::base::get_message_result::GetMessageResult;
use crate::base::message_result::AppendMessageResult;
use crate::base::message_result::PutMessageResult;
use crate::base::query_message_result::QueryMessageResult;
use crate::base::select_result::SelectMappedBufferResult;
use crate::base::store_checkpoint::StoreCheckpoint;
use crate::base::store_stats_service::StoreStatsService;
use crate::base::transient_store_pool::TransientStorePool;
use crate::config::message_store_config::MessageStoreConfig;
use crate::filter::MessageFilter;
use crate::ha::general_ha_service::GeneralHAService;
use crate::hook::put_message_hook::BoxedPutMessageHook;
use crate::hook::put_message_hook::PutMessageHook;
use crate::hook::send_message_back_hook::SendMessageBackHook;
use crate::log_file::commit_log::CommitLog;
use crate::log_file::mapped_file::MappedFile;
use crate::queue::ArcConsumeQueue;
use crate::stats::broker_stats_manager::BrokerStatsManager;
use crate::store::running_flags::RunningFlags;
use crate::store_error::StoreError;
use crate::timer::timer_message_store::TimerMessageStore;

type AsyncResult<T> = Pin<Box<dyn Future<Output = Result<T, StoreError>> + Send>>;

#[trait_variant::make(MessageStore: Send)]
pub trait MessageStoreInner: Sync + 'static {
    /// Load previously stored messages.
    ///
    /// Returns true if successful, false otherwise.
    async fn load(&mut self) -> bool;

    /// Launch this message store.
    async fn start(&mut self) -> Result<(), StoreError>;

    /// Initialize this message store.
    async fn init(&mut self) -> Result<(), StoreError>;

    /// Shutdown this message store.
    async fn shutdown(&mut self);

    /// Destroy this message store.
    /// Generally, all persistent files should be removed after invocation.
    fn destroy(&mut self);
    /*
    /// Store a message into the store in async manner.
    ///
    /// # Parameters
    /// * `msg` - Message instance to store
    ///
    /// # Returns
    /// A Future with the result of the store operation
    async fn async_put_message(
        &mut self,
        msg: MessageExtBrokerInner,
    ) -> PutMessageResult;*/

    /*    /// Store a batch of messages in async manner.
    ///
    /// # Parameters
    /// * `message_ext_batch` - The message batch
    ///
    /// # Returns
    /// A Future with the result of the store operation
    async fn async_put_messages(
        &self,
        message_ext_batch: MessageExtBatch,
    ) -> Result<PutMessageResult, StoreError>;*/

    /// Store a message into store.
    ///
    /// # Parameters
    /// * `msg` - Message instance to store
    ///
    /// # Returns
    /// Result of store operation
    async fn put_message(&mut self, msg: MessageExtBrokerInner) -> PutMessageResult;

    /// Store a batch of messages.
    ///
    /// # Parameters
    /// * `message_ext_batch` - Message batch
    ///
    /// # Returns
    /// Result of storing batch messages
    async fn put_messages(&mut self, message_ext_batch: MessageExtBatch) -> PutMessageResult;

    /// Query messages belonging to a topic at a queue starting from given offset.
    ///
    /// # Parameters
    /// * `group` - Consumer group that launches this query
    /// * `topic` - Topic to query
    /// * `queue_id` - Queue ID to query
    /// * `offset` - Logical offset to start from
    /// * `max_msg_nums` - Maximum count of messages to query
    /// * `message_filter` - Message filter used to screen desired messages
    ///
    /// # Returns
    /// Matched messages
    async fn get_message(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        max_msg_nums: i32,
        message_filter: Option<Arc<Box<dyn MessageFilter>>>,
    ) -> Option<GetMessageResult>;

    /*    /// Asynchronous get message
    async fn get_message_async(
         &self,
         group: &str,
         topic: &str,
         queue_id: i32,
         offset: i64,
         max_msg_nums: i32,
         message_filter: &dyn MessageFilter,
     ) -> Result<GetMessageResult, StoreError>;*/

    /// Get message with size constraint
    async fn get_message_with_size_limit(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        max_msg_nums: i32,
        max_total_msg_size: i32,
        message_filter: Option<Arc<Box<dyn MessageFilter>>>,
    ) -> Option<GetMessageResult>;

    /*    /// Asynchronous get message with size constraint
    async fn get_message_with_size_limit_async(
        &self,
        group: &str,
        topic: &str,
        queue_id: i32,
        offset: i64,
        max_msg_nums: i32,
        max_total_msg_size: i32,
        message_filter: &dyn MessageFilter,
    ) -> Result<GetMessageResult, StoreError>;*/

    /// Get maximum offset of the topic queue.
    fn get_max_offset_in_queue(&self, topic: &CheetahString, queue_id: i32) -> i64;

    /// Get maximum offset of the topic queue.
    fn get_max_offset_in_queue_committed(&self, topic: &CheetahString, queue_id: i32, committed: bool) -> i64;

    /// Get the minimum offset of the topic queue.
    fn get_min_offset_in_queue(&self, topic: &CheetahString, queue_id: i32) -> i64;

    /// Get the timer message store.
    fn get_timer_message_store(&self) -> Option<&Arc<TimerMessageStore>>;

    /// Set the timer message store.
    fn set_timer_message_store(&mut self, timer_message_store: Arc<TimerMessageStore>);

    /// Get the offset of the message in the commit log (physical offset).
    fn get_commit_log_offset_in_queue(&self, topic: &CheetahString, queue_id: i32, consume_queue_offset: i64) -> i64;

    /// Look up the physical offset of the message by timestamp.
    fn get_offset_in_queue_by_time(&self, topic: &CheetahString, queue_id: i32, timestamp: i64) -> i64;

    /// Look up the physical offset of the message by timestamp with boundary type.
    fn get_offset_in_queue_by_time_with_boundary(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        timestamp: i64,
        boundary_type: BoundaryType,
    ) -> i64;

    /// Look up the message by given commit log offset.
    fn look_message_by_offset(&self, commit_log_offset: i64) -> Option<MessageExt>;

    /// Look up the message by given commit log offset and size.
    fn look_message_by_offset_with_size(&self, commit_log_offset: i64, size: i32) -> Option<MessageExt>;

    /// Get one message from the specified commit log offset.
    fn select_one_message_by_offset(&self, commit_log_offset: i64) -> Option<SelectMappedBufferResult>;

    /// Get one message from the specified commit log offset and message size.
    fn select_one_message_by_offset_with_size(
        &self,
        commit_log_offset: i64,
        msg_size: i32,
    ) -> Option<SelectMappedBufferResult>;

    /// Get the running information of this store.
    fn get_running_data_info(&self) -> String;

    /// Get timing message count for a topic.
    fn get_timing_message_count(&self, topic: &CheetahString) -> i64;

    /// Message store runtime information.
    fn get_runtime_info(&self) -> HashMap<String, String>;

    //fn get_ha_runtime_info(&self) -> HARuntimeInfo;

    /// Get the maximum commit log offset.
    fn get_max_phy_offset(&self) -> i64;

    /// Get the minimum commit log offset.
    fn get_min_phy_offset(&self) -> i64;

    /// Get the store time of the earliest message in the given queue.
    fn get_earliest_message_time(&self, topic: &CheetahString, queue_id: i32) -> i64;

    /// Get the store time of the earliest message in this store.
    fn get_earliest_message_time_store(&self) -> i64;

    /*    /// Asynchronous get the store time of the earliest message in this store.
    async fn get_earliest_message_time_async(
        &self,
        topic: &str,
        queue_id: i32,
    ) -> Result<i64, StoreError>;*/

    /// Get the store time of the message specified.
    fn get_message_store_timestamp(&self, topic: &CheetahString, queue_id: i32, consume_queue_offset: i64) -> i64;

    /// Asynchronous get the store time of the message specified.
    async fn get_message_store_timestamp_async(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        consume_queue_offset: i64,
    ) -> Result<i64, StoreError>;

    /// Get the total number of the messages in the specified queue.
    fn get_message_total_in_queue(&self, topic: &CheetahString, queue_id: i32) -> i64;

    /// Get the raw commit log data starting from the given offset.
    fn get_commit_log_data(&self, offset: i64) -> Option<SelectMappedBufferResult>;

    /// Get the raw commit log data starting from the given offset, across multiple mapped files.
    fn get_bulk_commit_log_data(&self, offset: i64, size: i32) -> Option<Vec<SelectMappedBufferResult>>;

    /// Append data to commit log.
    async fn append_to_commit_log(
        &mut self,
        start_offset: i64,
        data: &[u8],
        data_start: i32,
        data_length: i32,
    ) -> Result<bool, StoreError>;

    /// Execute file deletion manually.
    fn execute_delete_files_manually(&self);

    /// Query messages by given key.
    async fn query_message(
        &self,
        topic: &CheetahString,
        key: &CheetahString,
        max_num: i32,
        begin: i64,
        end: i64,
    ) -> Option<QueryMessageResult>;

    /*/// Asynchronous query messages by given key.
    async fn query_message_async(
        &self,
        topic: &str,
        key: &str,
        max_num: i32,
        begin: i64,
        end: i64,
    ) -> Result<QueryMessageResult, StoreError>;*/

    /// Update HA master address.
    async fn update_ha_master_address(&self, new_addr: &str);

    /// Update master address.
    fn update_master_address(&self, new_addr: &CheetahString);

    /// Return how much the slave falls behind.
    fn slave_fall_behind_much(&self) -> i64;

    /// Return the current timestamp of the store.
    #[inline(always)]
    fn now(&self) -> u64 {
        get_current_millis()
    }

    /// Delete topic's consume queue file and unused stats.
    fn delete_topics(&mut self, delete_topics: Vec<&CheetahString>) -> i32;

    /// Clean unused topics which not in retain topic name set.
    fn clean_unused_topic(&self, retain_topics: &HashSet<String>) -> i32;

    /// Clean expired consume queues.
    fn clean_expired_consumer_queue(&self);

    /// Check if the given message is in the page cache.
    fn check_in_mem_by_consume_offset(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        consume_offset: i64,
        batch_size: i32,
    ) -> bool;

    /// Check if the given message is in store.
    fn check_in_store_by_consume_offset(&self, topic: &CheetahString, queue_id: i32, consume_offset: i64) -> bool;

    /// Get number of the bytes that have been stored in commit log and not yet dispatched.
    fn dispatch_behind_bytes(&self) -> i64;

    /// Flush the message store to persist all data.
    fn flush(&self) -> i64;

    /// Get the current flushed offset.
    fn get_flushed_where(&self) -> i64;

    /// Reset written offset.
    fn reset_write_offset(&self, phy_offset: i64) -> bool;

    /// Get confirm offset.
    fn get_confirm_offset(&self) -> i64;

    /// Set confirm offset.
    fn set_confirm_offset(&mut self, phy_offset: i64);

    /// Check if the operating system page cache is busy.
    fn is_os_page_cache_busy(&self) -> bool;

    /// Get lock time in milliseconds of the store.
    fn lock_time_millis(&self) -> i64;

    /// Check if the transient store pool is deficient.
    fn is_transient_store_pool_deficient(&self) -> bool;

    /// Get the dispatcher list.
    fn get_dispatcher_list(&self) -> &[Arc<dyn CommitLogDispatcher>];

    /// Add dispatcher.
    fn add_dispatcher(&mut self, dispatcher: Arc<dyn CommitLogDispatcher>);

    /// Add the first dispatcher.
    fn add_first_dispatcher(&mut self, dispatcher: Arc<dyn CommitLogDispatcher>);

    /// Get consume queue of the topic/queue. If not exist, returns None.
    fn get_consume_queue(&self, topic: &CheetahString, queue_id: i32) -> Option<ArcConsumeQueue>;

    /// Get consume queue of the topic/queue. If not exist, creates one.
    fn find_consume_queue(&self, topic: &CheetahString, queue_id: i32) -> Option<ArcConsumeQueue>;

    /// Get BrokerStatsManager of the messageStore.
    fn get_broker_stats_manager(&self) -> Option<&Arc<BrokerStatsManager>>;

    /// Will be triggered when a new message is appended to commit log.
    fn on_commit_log_append<MF: MappedFile>(
        &self,
        msg: &MessageExtBrokerInner,
        result: &AppendMessageResult,
        commit_log_file: &MF,
    );

    /// Will be triggered when a new dispatch request is sent to message store.
    fn on_commit_log_dispatch<MF: MappedFile>(
        &self,
        dispatch_request: &DispatchRequest,
        do_dispatch: bool,
        commit_log_file: &MF,
        is_recover: bool,
        is_file_end: bool,
    ) -> Result<(), StoreError>;

    /// Only used in rocksdb mode, because we build consumeQueue in batch
    /// It will be triggered when batch dispatch is finished
    fn finish_commit_log_dispatch(&self);

    /// Get the message store config
    fn get_message_store_config(&self) -> &MessageStoreConfig;

    /// Get the statistics service
    fn get_store_stats_service(&self) -> Arc<StoreStatsService>;

    /// Get the store checkpoint component
    fn get_store_checkpoint(&self) -> &StoreCheckpoint;

    fn get_store_checkpoint_arc(&self) -> Arc<StoreCheckpoint>;

    /// Get the system clock
    fn get_system_clock(&self) -> Arc<SystemClock>;

    /// Get the commit log
    fn get_commit_log(&self) -> &CommitLog;

    /// Get mutable commit log
    #[allow(clippy::mut_from_ref)]
    fn get_commit_log_mut_from_ref(&self) -> &mut CommitLog;

    fn get_commit_log_mut(&mut self) -> &mut CommitLog;

    /// Get running flags
    fn get_running_flags(&self) -> &RunningFlags;

    fn get_running_flags_arc(&self) -> Arc<RunningFlags>;

    /// Get the transient store pool
    fn get_transient_store_pool(&self) -> Arc<TransientStorePool>;

    // fn get_ha_service(&self) -> Arc<dyn HAService>;

    /// Get the allocate-mappedFile service
    fn get_allocate_mapped_file_service(&self) -> Arc<AllocateMappedFileService>;

    /// Truncate dirty logic files
    // fn truncate_dirty_logic_files(&self, phy_offset: i64) -> Result<(), StoreError>;
    fn truncate_dirty_logic_files(&self, phy_offset: i64);

    /// Unlock mappedFile
    fn unlock_mapped_file<MF: MappedFile>(&self, unlock_mapped_file: &MF);

    //fn get_perf_counter(&self) -> Arc<PerfCounterTicks>;

    /// Get the queue store
    fn get_queue_store(&self) -> &dyn Any;

    /// If 'sync disk flush' is configured in this message store
    fn is_sync_disk_flush(&self) -> bool;

    /// If this message store is sync master role
    fn is_sync_master(&self) -> bool;

    /// Assign a message to queue offset. If there is a race condition, you need to lock/unlock this
    /// method yourself.
    fn assign_offset(&self, msg: &mut MessageExtBrokerInner) -> Result<(), StoreError>;

    /// Increase queue offset in memory table. If there is a race condition, you need to lock/unlock
    /// this method
    fn increase_offset(&self, msg: &MessageExtBrokerInner, message_num: i16);

    /// Get master broker message store in process in broker container
    fn get_master_store_in_process<M: MessageStore>(&self) -> Option<Arc<M>>;

    /// Set master broker message store in process
    fn set_master_store_in_process<M: MessageStore>(&self, master_store_in_process: Arc<M>);

    /// Use FileChannel to get data
    fn get_data(&self, offset: i64, size: i32, byte_buffer: &mut BytesMut) -> bool;

    /// Set the number of alive replicas in group.
    fn set_alive_replica_num_in_group(&self, alive_replica_nums: i32);

    /// Get the number of alive replicas in group.
    fn get_alive_replica_num_in_group(&self) -> i32;

    /// Wake up AutoRecoverHAClient to start HA connection.
    fn wakeup_ha_client(&self);

    /// Get master flushed offset.
    fn get_master_flushed_offset(&self) -> i64;

    /// Get broker init max offset.
    fn get_broker_init_max_offset(&self) -> i64;

    /// Set master flushed offset.
    fn set_master_flushed_offset(&self, master_flushed_offset: i64);

    /// Set broker init max offset.
    fn set_broker_init_max_offset(&mut self, broker_init_max_offset: i64);

    /// Calculate the checksum of a certain range of data.
    fn calc_delta_checksum(&self, from: i64, to: i64) -> Vec<u8>;

    /// Truncate commitLog and consume queue to certain offset.
    fn truncate_files(&self, offset_to_truncate: i64) -> Result<bool, StoreError>;

    /// Check if the offset is aligned with one message.
    fn is_offset_aligned(&self, offset: i64) -> bool;

    /// Get put message hook list
    fn get_put_message_hook_list(&self) -> Vec<Arc<dyn PutMessageHook>>;

    /// Set send message back hook
    fn set_send_message_back_hook(&self, send_message_back_hook: Arc<dyn SendMessageBackHook>);

    /// Get send message back hook
    fn get_send_message_back_hook(&self) -> Option<Arc<dyn SendMessageBackHook>>;

    /// Get last mapped file and return last file first Offset
    fn get_last_file_from_offset(&self) -> i64;

    /// Get last mapped file
    fn get_last_mapped_file(&self, start_offset: i64) -> bool;

    /// Set physical offset
    fn set_physical_offset(&self, phy_offset: i64);

    /// Return whether mapped file is empty
    fn is_mapped_files_empty(&self) -> bool;

    /// Get state machine version
    fn get_state_machine_version(&self) -> i64;

    /// Check message and return size
    fn check_message_and_return_size(
        &self,
        bytes: &mut Bytes,
        check_crc: bool,
        check_dup_info: bool,
        read_body: bool,
    ) -> DispatchRequest;

    /// Get remain transientStoreBuffer numbers
    fn remain_transient_store_buffer_numbs(&self) -> i32;

    /// Get remain how many data to commit
    fn remain_how_many_data_to_commit(&self) -> i64;

    /// Get remain how many data to flush
    fn remain_how_many_data_to_flush(&self) -> i64;

    /// Get whether message store is shutdown
    fn is_shutdown(&self) -> bool;

    /// Estimate number of messages, within [from, to], which match given filter
    fn estimate_message_count(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        from: i64,
        to: i64,
        filter: &dyn MessageFilter,
    ) -> i64;

    // fn get_metrics_view(&self) -> Vec<Pair<InstrumentSelector, ViewBuilder>>;

    // fn init_metrics(&self, meter: &Meter, attributes_builder_supplier: Arc<dyn Fn() ->
    // AttributesBuilder>);

    /// Recover the topic queue table.
    ///
    /// This function is responsible for recovering the topic queue table
    /// to ensure that the message store can correctly manage and track
    /// the offsets and states of various topic queues.
    fn recover_topic_queue_table(&mut self);

    /// Notify message arrive if necessary
    fn notify_message_arrive_if_necessary(&self, dispatch_request: &mut DispatchRequest);

    /// Set a put message hook.
    ///
    /// # Arguments
    ///
    /// * `put_message_hook` - The hook to set.
    fn set_put_message_hook(&mut self, put_message_hook: BoxedPutMessageHook);

    /// Get the HA (High Availability) service associated with the message store.
    ///
    /// This function provides access to the `GeneralHAService` instance, which is responsible
    /// for managing high availability features such as replication and synchronization
    /// between master and slave nodes.
    ///
    /// # Returns
    /// A reference to the `GeneralHAService` instance.
    fn get_ha_service(&self) -> Option<&GeneralHAService>;
}
