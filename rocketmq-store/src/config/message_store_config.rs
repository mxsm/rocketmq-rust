/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::collections::HashMap;
use std::path::PathBuf;

use cheetah_string::CheetahString;
use lazy_static::lazy_static;
use rocketmq_common::common::broker::broker_role::BrokerRole;
use serde::Deserialize;

use crate::base::store_enum::StoreType;
use crate::config::flush_disk_type::FlushDiskType;
use crate::queue::single_consume_queue::CQ_STORE_UNIT_SIZE;

lazy_static! {
    static ref USER_HOME: PathBuf = dirs::home_dir().unwrap();
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MessageStoreConfig {
    pub store_path_root_dir: CheetahString,
    pub store_path_commit_log: Option<CheetahString>,
    pub store_path_dledger_commit_log: Option<CheetahString>,
    pub store_path_epoch_file: Option<CheetahString>,
    pub store_path_broker_identity: Option<CheetahString>,
    pub read_only_commit_log_store_paths: Option<CheetahString>,
    pub mapped_file_size_commit_log: usize,
    pub compaction_mapped_file_size: usize,
    pub compaction_cq_mapped_file_size: usize,
    pub compaction_schedule_internal: usize,
    pub max_offset_map_size: usize,
    pub compaction_thread_num: usize,
    pub enable_compaction: bool,
    pub mapped_file_size_timer_log: usize,
    pub timer_precision_ms: u64,
    pub timer_roll_window_slot: usize,
    pub timer_flush_interval_ms: usize,
    pub timer_get_message_thread_num: usize,
    pub timer_put_message_thread_num: usize,
    pub timer_enable_disruptor: bool,
    pub timer_enable_check_metrics: bool,
    pub timer_intercept_delay_level: bool,
    pub timer_max_delay_sec: u64,
    pub timer_wheel_enable: bool,
    /**
     * 1. Register to broker after (startTime + disappearTimeAfterStart)
     * 2. Internal msg exchange will start after (startTime + disappearTimeAfterStart)
     * A. PopReviveService
     * B. TimerDequeueGetService
     */
    pub disappear_time_after_start: i64,
    pub timer_stop_enqueue: bool,
    pub timer_check_metrics_when: String,
    pub timer_skip_unknown_error: bool,
    pub timer_warm_enable: bool,
    pub timer_stop_dequeue: bool,
    pub timer_congest_num_each_slot: usize,
    pub timer_metric_small_threshold: usize,
    pub timer_progress_log_interval_ms: usize,
    pub store_type: StoreType,
    pub mapped_file_size_consume_queue: usize,
    pub enable_consume_queue_ext: bool,
    pub mapped_file_size_consume_queue_ext: usize,
    pub mapper_file_size_batch_consume_queue: usize,
    pub bit_map_length_consume_queue_ext: usize,
    pub flush_interval_commit_log: i32,
    pub commit_interval_commit_log: u64,
    pub max_recovery_commit_log_files: usize,
    pub disk_space_warning_level_ratio: usize,
    pub disk_space_clean_forcibly_ratio: usize,
    pub use_reentrant_lock_when_put_message: bool,
    pub flush_commit_log_timed: bool,
    pub flush_interval_consume_queue: usize,
    pub clean_resource_interval: usize,
    pub delete_commit_log_files_interval: usize,
    pub delete_consume_queue_files_interval: usize,
    pub destroy_mapped_file_interval_forcibly: usize,
    pub redelete_hanged_file_interval: usize,
    pub delete_when: String,
    pub disk_max_used_space_ratio: usize,
    pub file_reserved_time: usize,
    pub delete_file_batch_max: usize,
    pub put_msg_index_hight_water: usize,
    pub max_message_size: i32,
    pub check_crc_on_recover: bool,
    pub flush_commit_log_least_pages: i32,
    pub commit_commit_log_least_pages: i32,
    pub flush_least_pages_when_warm_mapped_file: usize,
    pub flush_consume_queue_least_pages: usize,
    pub flush_commit_log_thorough_interval: i32,
    pub commit_commit_log_thorough_interval: u64,
    pub flush_consume_queue_thorough_interval: usize,
    pub max_transfer_bytes_on_message_in_memory: u64,
    pub max_transfer_count_on_message_in_memory: u64,
    pub max_transfer_bytes_on_message_in_disk: u64,
    pub max_transfer_count_on_message_in_disk: u64,
    pub access_message_in_memory_max_ratio: usize,
    pub message_index_enable: bool,
    pub max_hash_slot_num: u32,
    pub max_index_num: u32,
    pub max_msgs_num_batch: usize,
    pub message_index_safe: bool,
    pub ha_listen_port: usize,
    pub ha_send_heartbeat_interval: usize,
    pub ha_housekeeping_interval: usize,
    pub ha_transfer_batch_size: usize,
    pub ha_master_address: Option<String>,
    pub ha_max_gap_not_in_sync: usize,
    pub broker_role: BrokerRole,
    pub flush_disk_type: FlushDiskType,
    pub sync_flush_timeout: u64,
    pub put_message_timeout: usize,
    pub slave_timeout: usize,
    pub message_delay_level: String,
    pub flush_delay_offset_interval: usize,
    pub clean_file_forcibly_enable: bool,
    pub warm_mapped_file_enable: bool,
    pub offset_check_in_slave: bool,
    pub debug_lock_enable: bool,
    pub duplication_enable: bool,
    pub disk_fall_recorded: bool,
    pub os_page_cache_busy_timeout_mills: u64,
    pub default_query_max_num: usize,
    pub transient_store_pool_enable: bool,
    pub transient_store_pool_size: usize,
    pub fast_fail_if_no_buffer_in_store_pool: bool,
    pub enable_dledger_commit_log: bool,
    pub dledger_group: Option<String>,
    pub dledger_peers: Option<String>,
    pub dledger_self_id: Option<String>,
    pub preferred_leader_id: Option<String>,
    pub enable_batch_push: bool,
    pub enable_schedule_message_stats: bool,
    pub enable_lmq: bool,
    pub enable_multi_dispatch: bool,
    pub max_lmq_consume_queue_num: usize,
    pub enable_schedule_async_deliver: bool,
    pub schedule_async_deliver_max_pending_limit: usize,
    pub schedule_async_deliver_max_resend_num2_blocked: usize,
    pub max_batch_delete_files_num: usize,
    pub dispatch_cq_threads: usize,
    pub dispatch_cq_cache_num: usize,
    pub enable_async_reput: bool,
    pub recheck_reput_offset_from_cq: bool,
    pub max_topic_length: usize,
    pub auto_message_version_on_topic_len: bool,
    pub enabled_append_prop_crc: bool,
    pub force_verify_prop_crc: bool,
    pub travel_cq_file_num_when_get_message: usize,
    pub correct_logic_min_offset_sleep_interval: usize,
    pub correct_logic_min_offset_force_interval: usize,
    pub mapped_file_swap_enable: bool,
    pub commit_log_force_swap_map_interval: usize,
    pub commit_log_swap_map_interval: usize,
    pub commit_log_swap_map_reserve_file_num: usize,
    pub logic_queue_force_swap_map_interval: usize,
    pub logic_queue_swap_map_interval: usize,
    pub clean_swapped_map_interval: usize,
    pub logic_queue_swap_map_reserve_file_num: usize,
    pub search_bcq_by_cache_enable: bool,
    pub dispatch_from_sender_thread: bool,
    pub wake_commit_when_put_message: bool,
    pub wake_flush_when_put_message: bool,
    pub enable_clean_expired_offset: bool,
    pub max_async_put_message_requests: usize,
    pub pull_batch_max_message_count: usize,
    pub total_replicas: usize,
    pub in_sync_replicas: u32,
    pub min_in_sync_replicas: usize,
    pub all_ack_in_sync_state_set: bool,
    pub enable_auto_in_sync_replicas: bool,
    pub ha_flow_control_enable: bool,
    pub max_ha_transfer_byte_in_second: usize,
    pub ha_max_time_slave_not_catchup: usize,
    pub sync_master_flush_offset_when_startup: bool,
    pub max_checksum_range: usize,
    pub replicas_per_disk_partition: usize,
    pub logical_disk_space_clean_forcibly_threshold: f64,
    pub max_slave_resend_length: usize,
    pub sync_from_last_file: bool,
    pub async_learner: bool,
    pub max_consume_queue_scan: usize,
    pub sample_count_threshold: usize,
    pub cold_data_flow_control_enable: bool,
    pub cold_data_scan_enable: bool,
    pub data_read_ahead_enable: bool,
    pub timer_cold_data_check_interval_ms: usize,
    pub sample_steps: usize,
    pub access_message_in_memory_hot_ratio: usize,
    pub enable_build_consume_queue_concurrently: bool,
    pub batch_dispatch_request_thread_pool_nums: usize,
    pub clean_rocksdb_dirty_cq_interval_min: usize,
    pub stat_rocksdb_cq_interval_sec: usize,
    pub mem_table_flush_interval_ms: usize,
    pub real_time_persist_rocksdb_config: bool,
    pub enable_rocksdb_log: bool,
    pub topic_queue_lock_num: usize,
    pub max_filter_message_size: i32,
}

impl Default for MessageStoreConfig {
    fn default() -> Self {
        let store_path_root_dir = USER_HOME
            .clone()
            .join("store")
            .to_string_lossy()
            .to_string();
        Self {
            store_path_root_dir: store_path_root_dir.into(),
            store_path_commit_log: None,
            store_path_dledger_commit_log: None,
            store_path_epoch_file: None,
            store_path_broker_identity: None,
            read_only_commit_log_store_paths: None,
            mapped_file_size_commit_log: 1024 * 1024 * 1024, //CommitLog file size,default is 1G
            compaction_mapped_file_size: 100 * 1024 * 1024,
            /* CompactinLog file size, default
             * is 100M */
            compaction_cq_mapped_file_size: 10 * 1024 * 1024,
            /* CompactionLog consumeQueue file
             * size, default is 10M */
            compaction_schedule_internal: 15 * 60 * 1000,
            max_offset_map_size: 100 * 1024 * 1024,
            compaction_thread_num: 6,
            enable_compaction: false,
            mapped_file_size_timer_log: 100 * 1024 * 1024, // TimerLog file size, default is 100M
            timer_precision_ms: 1000,
            timer_roll_window_slot: 3600 * 24 * 2,
            timer_flush_interval_ms: 1000,
            timer_get_message_thread_num: 3,
            timer_put_message_thread_num: 3,
            timer_enable_disruptor: false,
            timer_enable_check_metrics: false,
            timer_intercept_delay_level: false,
            timer_max_delay_sec: 0,
            timer_wheel_enable: false,
            disappear_time_after_start: -1,
            timer_stop_enqueue: false,
            timer_check_metrics_when: "".to_string(),
            timer_skip_unknown_error: false,
            timer_warm_enable: false,
            timer_stop_dequeue: false,
            timer_congest_num_each_slot: 0,
            timer_metric_small_threshold: 0,
            timer_progress_log_interval_ms: 0,
            store_type: Default::default(),
            mapped_file_size_consume_queue: 300000 * 20,
            enable_consume_queue_ext: false,
            mapped_file_size_consume_queue_ext: 48 * 1024 * 1024,
            mapper_file_size_batch_consume_queue: 300000 * 46,
            bit_map_length_consume_queue_ext: 64,
            flush_interval_commit_log: 500,
            commit_interval_commit_log: 200,
            max_recovery_commit_log_files: 0,
            disk_space_warning_level_ratio: 0,
            disk_space_clean_forcibly_ratio: 0,
            use_reentrant_lock_when_put_message: false,
            flush_commit_log_timed: true,
            flush_interval_consume_queue: 1000,
            clean_resource_interval: 10000,
            delete_commit_log_files_interval: 100,
            delete_consume_queue_files_interval: 100,
            destroy_mapped_file_interval_forcibly: 1000 * 120,
            redelete_hanged_file_interval: 1000 * 120,
            delete_when: "04".to_string(),
            disk_max_used_space_ratio: 75,
            file_reserved_time: 0,
            delete_file_batch_max: 0,
            put_msg_index_hight_water: 0,
            max_message_size: 1024 * 1024 * 4,
            check_crc_on_recover: false,
            flush_commit_log_least_pages: 0,
            commit_commit_log_least_pages: 4,
            flush_least_pages_when_warm_mapped_file: 0,
            flush_consume_queue_least_pages: 0,
            flush_commit_log_thorough_interval: 1000 * 10,
            commit_commit_log_thorough_interval: 200,
            flush_consume_queue_thorough_interval: 0,
            max_transfer_bytes_on_message_in_memory: 1024 * 256,
            max_transfer_count_on_message_in_memory: 32,
            max_transfer_bytes_on_message_in_disk: 1024 * 64,
            max_transfer_count_on_message_in_disk: 8,
            access_message_in_memory_max_ratio: 40,
            message_index_enable: true,
            max_hash_slot_num: 5000000,
            max_index_num: 5000000 * 4,
            max_msgs_num_batch: 64,
            message_index_safe: false,
            ha_listen_port: 0,
            ha_send_heartbeat_interval: 0,
            ha_housekeeping_interval: 0,
            ha_transfer_batch_size: 0,
            ha_master_address: None,
            ha_max_gap_not_in_sync: 0,
            broker_role: Default::default(),
            flush_disk_type: FlushDiskType::SyncFlush,
            sync_flush_timeout: 1000 * 5,
            put_message_timeout: 0,
            slave_timeout: 0,
            message_delay_level: "".to_string(),
            flush_delay_offset_interval: 0,
            clean_file_forcibly_enable: false,
            warm_mapped_file_enable: false,
            offset_check_in_slave: false,
            debug_lock_enable: false,
            duplication_enable: false,
            disk_fall_recorded: false,
            os_page_cache_busy_timeout_mills: 1000,
            default_query_max_num: 0,
            transient_store_pool_enable: false,
            transient_store_pool_size: 0,
            fast_fail_if_no_buffer_in_store_pool: false,
            enable_dledger_commit_log: false,
            dledger_group: None,
            dledger_peers: None,
            dledger_self_id: None,
            preferred_leader_id: None,
            enable_batch_push: false,
            enable_schedule_message_stats: false,
            enable_lmq: false,
            enable_multi_dispatch: false,
            max_lmq_consume_queue_num: 0,
            enable_schedule_async_deliver: false,
            schedule_async_deliver_max_pending_limit: 0,
            schedule_async_deliver_max_resend_num2_blocked: 0,
            max_batch_delete_files_num: 0,
            dispatch_cq_threads: 0,
            dispatch_cq_cache_num: 0,
            enable_async_reput: false,
            recheck_reput_offset_from_cq: false,
            max_topic_length: 0,
            auto_message_version_on_topic_len: true,
            enabled_append_prop_crc: false,
            force_verify_prop_crc: false,
            travel_cq_file_num_when_get_message: 1,
            correct_logic_min_offset_sleep_interval: 0,
            correct_logic_min_offset_force_interval: 0,
            mapped_file_swap_enable: false,
            commit_log_force_swap_map_interval: 0,
            commit_log_swap_map_interval: 0,
            commit_log_swap_map_reserve_file_num: 0,
            logic_queue_force_swap_map_interval: 0,
            logic_queue_swap_map_interval: 0,
            clean_swapped_map_interval: 0,
            logic_queue_swap_map_reserve_file_num: 0,
            search_bcq_by_cache_enable: false,
            dispatch_from_sender_thread: false,
            wake_commit_when_put_message: false,
            wake_flush_when_put_message: false,
            enable_clean_expired_offset: false,
            max_async_put_message_requests: 0,
            pull_batch_max_message_count: 0,
            total_replicas: 1,
            in_sync_replicas: 1,
            min_in_sync_replicas: 0,
            all_ack_in_sync_state_set: false,
            enable_auto_in_sync_replicas: false,
            ha_flow_control_enable: false,
            max_ha_transfer_byte_in_second: 0,
            ha_max_time_slave_not_catchup: 0,
            sync_master_flush_offset_when_startup: false,
            max_checksum_range: 0,
            replicas_per_disk_partition: 0,
            logical_disk_space_clean_forcibly_threshold: 0.0,
            max_slave_resend_length: 0,
            sync_from_last_file: false,
            async_learner: false,
            max_consume_queue_scan: 0,
            sample_count_threshold: 0,
            cold_data_flow_control_enable: false,
            cold_data_scan_enable: false,
            data_read_ahead_enable: false,
            timer_cold_data_check_interval_ms: 0,
            sample_steps: 0,
            access_message_in_memory_hot_ratio: 0,
            enable_build_consume_queue_concurrently: false,
            batch_dispatch_request_thread_pool_nums: 0,
            clean_rocksdb_dirty_cq_interval_min: 0,
            stat_rocksdb_cq_interval_sec: 0,
            mem_table_flush_interval_ms: 0,
            real_time_persist_rocksdb_config: false,
            enable_rocksdb_log: false,
            topic_queue_lock_num: 32,
            max_filter_message_size: 16000,
        }
    }
}

impl MessageStoreConfig {
    pub fn get_store_path_commit_log(&self) -> String {
        if self.store_path_commit_log.is_none() {
            return PathBuf::from(self.store_path_root_dir.to_string())
                .join("commitlog")
                .to_string_lossy()
                .to_string();
        }
        self.store_path_commit_log.clone().unwrap().to_string()
    }

    pub fn is_enable_rocksdb_store(&self) -> bool {
        self.store_type == StoreType::RocksDB
    }

    pub fn get_mapped_file_size_consume_queue(&self) -> i32 {
        let factor = (self.mapped_file_size_consume_queue as f64 / (CQ_STORE_UNIT_SIZE as f64))
            .ceil() as i32;
        factor * CQ_STORE_UNIT_SIZE
    }
    pub fn is_timer_wheel_enable(&self) -> bool {
        self.timer_wheel_enable
    }

    pub fn get_properties(&self) -> HashMap<CheetahString, CheetahString> {
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert(
            "storePathRootDir".to_string(),
            self.store_path_root_dir.clone().to_string(),
        );
        properties.insert(
            "storePathCommitLog".to_string(),
            self.store_path_commit_log
                .clone()
                .unwrap_or_default()
                .to_string(),
        );
        properties.insert(
            "storePathDledgerCommitLog".to_string(),
            self.store_path_dledger_commit_log
                .clone()
                .unwrap_or_default()
                .to_string(),
        );
        properties.insert(
            "storePathEpochFile".to_string(),
            self.store_path_epoch_file
                .clone()
                .unwrap_or_default()
                .to_string(),
        );
        properties.insert(
            "storePathBrokerIdentity".to_string(),
            self.store_path_broker_identity
                .clone()
                .unwrap_or_default()
                .to_string(),
        );
        properties.insert(
            "readOnlyCommitLogStorePaths".to_string(),
            self.read_only_commit_log_store_paths
                .clone()
                .unwrap_or_default()
                .to_string(),
        );
        properties.insert(
            "mappedFileSizeCommitLog".to_string(),
            self.mapped_file_size_commit_log.to_string(),
        );
        properties.insert(
            "compactionMappedFileSize".to_string(),
            self.compaction_mapped_file_size.to_string(),
        );
        properties.insert(
            "compactionCqMappedFileSize".to_string(),
            self.compaction_cq_mapped_file_size.to_string(),
        );
        properties.insert(
            "compactionScheduleInternal".to_string(),
            self.compaction_schedule_internal.to_string(),
        );
        properties.insert(
            "maxOffsetMapSize".to_string(),
            self.max_offset_map_size.to_string(),
        );
        properties.insert(
            "compactionThreadNum".to_string(),
            self.compaction_thread_num.to_string(),
        );
        properties.insert(
            "enableCompaction".to_string(),
            self.enable_compaction.to_string(),
        );
        properties.insert(
            "mappedFileSizeTimerLog".to_string(),
            self.mapped_file_size_timer_log.to_string(),
        );
        properties.insert(
            "timerPrecisionMs".to_string(),
            self.timer_precision_ms.to_string(),
        );
        properties.insert(
            "timerRollWindowSlot".to_string(),
            self.timer_roll_window_slot.to_string(),
        );
        properties.insert(
            "timerFlushIntervalMs".to_string(),
            self.timer_flush_interval_ms.to_string(),
        );
        properties.insert(
            "timerGetMessageThreadNum".to_string(),
            self.timer_get_message_thread_num.to_string(),
        );
        properties.insert(
            "timerPutMessageThreadNum".to_string(),
            self.timer_put_message_thread_num.to_string(),
        );
        properties.insert(
            "timerEnableDisruptor".to_string(),
            self.timer_enable_disruptor.to_string(),
        );
        properties.insert(
            "timerEnableCheckMetrics".to_string(),
            self.timer_enable_check_metrics.to_string(),
        );
        properties.insert(
            "timerInterceptDelayLevel".to_string(),
            self.timer_intercept_delay_level.to_string(),
        );
        properties.insert(
            "timerMaxDelaySec".to_string(),
            self.timer_max_delay_sec.to_string(),
        );
        properties.insert(
            "timerWheelEnable".to_string(),
            self.timer_wheel_enable.to_string(),
        );
        properties.insert(
            "disappearTimeAfterStart".to_string(),
            self.disappear_time_after_start.to_string(),
        );
        properties.insert(
            "timerStopEnqueue".to_string(),
            self.timer_stop_enqueue.to_string(),
        );
        properties.insert(
            "timerCheckMetricsWhen".to_string(),
            self.timer_check_metrics_when.clone(),
        );
        properties.insert(
            "timerSkipUnknownError".to_string(),
            self.timer_skip_unknown_error.to_string(),
        );
        properties.insert(
            "timerWarmEnable".to_string(),
            self.timer_warm_enable.to_string(),
        );
        properties.insert(
            "timerStopDequeue".to_string(),
            self.timer_stop_dequeue.to_string(),
        );
        properties.insert(
            "timerCongestNumEachSlot".to_string(),
            self.timer_congest_num_each_slot.to_string(),
        );
        properties.insert(
            "timerMetricSmallThreshold".to_string(),
            self.timer_metric_small_threshold.to_string(),
        );
        properties.insert(
            "timerProgressLogIntervalMs".to_string(),
            self.timer_progress_log_interval_ms.to_string(),
        );
        properties.insert(
            "storeType".to_string(),
            self.store_type.get_store_type().to_string(),
        );
        properties.insert(
            "mappedFileSizeConsumeQueue".to_string(),
            self.mapped_file_size_consume_queue.to_string(),
        );
        properties.insert(
            "enableConsumeQueueExt".to_string(),
            self.enable_consume_queue_ext.to_string(),
        );
        properties.insert(
            "mappedFileSizeConsumeQueueExt".to_string(),
            self.mapped_file_size_consume_queue_ext.to_string(),
        );
        properties.insert(
            "mapperFileSizeBatchConsumeQueue".to_string(),
            self.mapper_file_size_batch_consume_queue.to_string(),
        );
        properties.insert(
            "bitMapLengthConsumeQueueExt".to_string(),
            self.bit_map_length_consume_queue_ext.to_string(),
        );
        properties.insert(
            "flushIntervalCommitLog".to_string(),
            self.flush_interval_commit_log.to_string(),
        );
        properties.insert(
            "commitIntervalCommitLog".to_string(),
            self.commit_interval_commit_log.to_string(),
        );
        properties.insert(
            "maxRecoveryCommitLogFiles".to_string(),
            self.max_recovery_commit_log_files.to_string(),
        );
        properties.insert(
            "diskSpaceWarningLevelRatio".to_string(),
            self.disk_space_warning_level_ratio.to_string(),
        );
        properties.insert(
            "diskSpaceCleanForciblyRatio".to_string(),
            self.disk_space_clean_forcibly_ratio.to_string(),
        );
        properties.insert(
            "useReentrantLockWhenPutMessage".to_string(),
            self.use_reentrant_lock_when_put_message.to_string(),
        );
        properties.insert(
            "flushCommitLogTimed".to_string(),
            self.flush_commit_log_timed.to_string(),
        );
        properties.insert(
            "flushIntervalConsumeQueue".to_string(),
            self.flush_interval_consume_queue.to_string(),
        );
        properties.insert(
            "cleanResourceInterval".to_string(),
            self.clean_resource_interval.to_string(),
        );
        properties.insert(
            "deleteCommitLogFilesInterval".to_string(),
            self.delete_commit_log_files_interval.to_string(),
        );
        properties.insert(
            "deleteConsumeQueueFilesInterval".to_string(),
            self.delete_consume_queue_files_interval.to_string(),
        );
        properties.insert(
            "destroyMappedFileIntervalForcibly".to_string(),
            self.destroy_mapped_file_interval_forcibly.to_string(),
        );
        properties.insert(
            "redeleteHangedFileInterval".to_string(),
            self.redelete_hanged_file_interval.to_string(),
        );
        properties.insert("deleteWhen".to_string(), self.delete_when.clone());
        properties.insert(
            "diskMaxUsedSpaceRatio".to_string(),
            self.disk_max_used_space_ratio.to_string(),
        );
        properties.insert(
            "fileReservedTime".to_string(),
            self.file_reserved_time.to_string(),
        );
        properties.insert(
            "deleteFileBatchMax".to_string(),
            self.delete_file_batch_max.to_string(),
        );
        properties.insert(
            "putMsgIndexHightWater".to_string(),
            self.put_msg_index_hight_water.to_string(),
        );
        properties.insert(
            "maxMessageSize".to_string(),
            self.max_message_size.to_string(),
        );
        properties.insert(
            "checkCrcOnRecover".to_string(),
            self.check_crc_on_recover.to_string(),
        );
        properties.insert(
            "flushCommitLogLeastPages".to_string(),
            self.flush_commit_log_least_pages.to_string(),
        );
        properties.insert(
            "commitCommitLogLeastPages".to_string(),
            self.commit_commit_log_least_pages.to_string(),
        );
        properties.insert(
            "flushLeastPagesWhenWarmMappedFile".to_string(),
            self.flush_least_pages_when_warm_mapped_file.to_string(),
        );
        properties.insert(
            "flushConsumeQueueLeastPages".to_string(),
            self.flush_consume_queue_least_pages.to_string(),
        );
        properties.insert(
            "flushCommitLogThoroughInterval".to_string(),
            self.flush_commit_log_thorough_interval.to_string(),
        );
        properties.insert(
            "commitCommitLogThoroughInterval".to_string(),
            self.commit_commit_log_thorough_interval.to_string(),
        );
        properties.insert(
            "flushConsumeQueueThoroughInterval".to_string(),
            self.flush_consume_queue_thorough_interval.to_string(),
        );
        properties.insert(
            "maxTransferBytesOnMessageInMemory".to_string(),
            self.max_transfer_bytes_on_message_in_memory.to_string(),
        );
        properties.insert(
            "maxTransferCountOnMessageInMemory".to_string(),
            self.max_transfer_count_on_message_in_memory.to_string(),
        );
        properties.insert(
            "maxTransferBytesOnMessageInDisk".to_string(),
            self.max_transfer_bytes_on_message_in_disk.to_string(),
        );
        properties.insert(
            "maxTransferCountOnMessageInDisk".to_string(),
            self.max_transfer_count_on_message_in_disk.to_string(),
        );
        properties.insert(
            "accessMessageInMemoryMaxRatio".to_string(),
            self.access_message_in_memory_max_ratio.to_string(),
        );
        properties.insert(
            "messageIndexEnable".to_string(),
            self.message_index_enable.to_string(),
        );
        properties.insert(
            "maxHashSlotNum".to_string(),
            self.max_hash_slot_num.to_string(),
        );
        properties.insert("maxIndexNum".to_string(), self.max_index_num.to_string());
        properties.insert(
            "maxMsgsNumBatch".to_string(),
            self.max_msgs_num_batch.to_string(),
        );
        properties.insert(
            "messageIndexSafe".to_string(),
            self.message_index_safe.to_string(),
        );
        properties.insert("haListenPort".to_string(), self.ha_listen_port.to_string());
        properties.insert(
            "haSendHeartbeatInterval".to_string(),
            self.ha_send_heartbeat_interval.to_string(),
        );
        properties.insert(
            "haHousekeepingInterval".to_string(),
            self.ha_housekeeping_interval.to_string(),
        );
        properties.insert(
            "haTransferBatchSize".to_string(),
            self.ha_transfer_batch_size.to_string(),
        );
        properties.insert(
            "haMasterAddress".to_string(),
            self.ha_master_address.clone().unwrap_or_default(),
        );
        properties.insert(
            "haMaxGapNotInSync".to_string(),
            self.ha_max_gap_not_in_sync.to_string(),
        );
        properties.insert(
            "brokerRole".to_string(),
            self.broker_role.get_broker_role().to_string(),
        );
        properties.insert(
            "flushDiskType".to_string(),
            self.flush_disk_type.get_flush_disk_type().to_string(),
        );
        properties.insert(
            "syncFlushTimeout".to_string(),
            self.sync_flush_timeout.to_string(),
        );
        properties.insert(
            "putMessageTimeout".to_string(),
            self.put_message_timeout.to_string(),
        );
        properties.insert("slaveTimeout".to_string(), self.slave_timeout.to_string());
        properties.insert(
            "messageDelayLevel".to_string(),
            self.message_delay_level.clone(),
        );
        properties.insert(
            "flushDelayOffsetInterval".to_string(),
            self.flush_delay_offset_interval.to_string(),
        );
        properties.insert(
            "cleanFileForciblyEnable".to_string(),
            self.clean_file_forcibly_enable.to_string(),
        );
        properties.insert(
            "warmMappedFileEnable".to_string(),
            self.warm_mapped_file_enable.to_string(),
        );
        properties.insert(
            "offsetCheckInSlave".to_string(),
            self.offset_check_in_slave.to_string(),
        );
        properties.insert(
            "debugLockEnable".to_string(),
            self.debug_lock_enable.to_string(),
        );
        properties.insert(
            "duplicationEnable".to_string(),
            self.duplication_enable.to_string(),
        );
        properties.insert(
            "diskFallRecorded".to_string(),
            self.disk_fall_recorded.to_string(),
        );
        properties.insert(
            "osPageCacheBusyTimeoutMills".to_string(),
            self.os_page_cache_busy_timeout_mills.to_string(),
        );
        properties.insert(
            "defaultQueryMaxNum".to_string(),
            self.default_query_max_num.to_string(),
        );
        properties.insert(
            "transientStorePoolEnable".to_string(),
            self.transient_store_pool_enable.to_string(),
        );
        properties.insert(
            "transientStorePoolSize".to_string(),
            self.transient_store_pool_size.to_string(),
        );
        properties.insert(
            "fastFailIfNoBufferInStorePool".to_string(),
            self.fast_fail_if_no_buffer_in_store_pool.to_string(),
        );
        properties.insert(
            "enableDledgerCommitLog".to_string(),
            self.enable_dledger_commit_log.to_string(),
        );
        properties.insert(
            "dledgerGroup".to_string(),
            self.dledger_group.clone().unwrap_or_default(),
        );
        properties.insert(
            "dledgerPeers".to_string(),
            self.dledger_peers.clone().unwrap_or_default(),
        );
        properties.insert(
            "dledgerSelfId".to_string(),
            self.dledger_self_id.clone().unwrap_or_default(),
        );
        properties.insert(
            "preferredLeaderId".to_string(),
            self.preferred_leader_id.clone().unwrap_or_default(),
        );
        properties.insert(
            "enableBatchPush".to_string(),
            self.enable_batch_push.to_string(),
        );
        properties.insert(
            "enableScheduleMessageStats".to_string(),
            self.enable_schedule_message_stats.to_string(),
        );
        properties.insert("enableLmq".to_string(), self.enable_lmq.to_string());
        properties.insert(
            "enableMultiDispatch".to_string(),
            self.enable_multi_dispatch.to_string(),
        );
        properties.insert(
            "maxLmqConsumeQueueNum".to_string(),
            self.max_lmq_consume_queue_num.to_string(),
        );
        properties.insert(
            "enableScheduleAsyncDeliver".to_string(),
            self.enable_schedule_async_deliver.to_string(),
        );
        properties.insert(
            "scheduleAsyncDeliverMaxPendingLimit".to_string(),
            self.schedule_async_deliver_max_pending_limit.to_string(),
        );
        properties.insert(
            "scheduleAsyncDeliverMaxResendNum2Blocked".to_string(),
            self.schedule_async_deliver_max_resend_num2_blocked
                .to_string(),
        );
        properties.insert(
            "maxBatchDeleteFilesNum".to_string(),
            self.max_batch_delete_files_num.to_string(),
        );
        properties.insert(
            "dispatchCqThreads".to_string(),
            self.dispatch_cq_threads.to_string(),
        );
        properties.insert(
            "dispatchCqCacheNum".to_string(),
            self.dispatch_cq_cache_num.to_string(),
        );
        properties.insert(
            "enableAsyncReput".to_string(),
            self.enable_async_reput.to_string(),
        );
        properties.insert(
            "recheckReputOffsetFromCq".to_string(),
            self.recheck_reput_offset_from_cq.to_string(),
        );
        properties.insert(
            "maxTopicLength".to_string(),
            self.max_topic_length.to_string(),
        );
        properties.insert(
            "autoMessageVersionOnTopicLen".to_string(),
            self.auto_message_version_on_topic_len.to_string(),
        );
        properties.insert(
            "enabledAppendPropCrc".to_string(),
            self.enabled_append_prop_crc.to_string(),
        );
        properties.insert(
            "forceVerifyPropCrc".to_string(),
            self.force_verify_prop_crc.to_string(),
        );
        properties.insert(
            "travelCqFileNumWhenGetMessage".to_string(),
            self.travel_cq_file_num_when_get_message.to_string(),
        );
        properties.insert(
            "correctLogicMinOffsetSleepInterval".to_string(),
            self.correct_logic_min_offset_sleep_interval.to_string(),
        );
        properties.insert(
            "correctLogicMinOffsetForceInterval".to_string(),
            self.correct_logic_min_offset_force_interval.to_string(),
        );
        properties.insert(
            "mappedFileSwapEnable".to_string(),
            self.mapped_file_swap_enable.to_string(),
        );
        properties.insert(
            "commitLogForceSwapMapInterval".to_string(),
            self.commit_log_force_swap_map_interval.to_string(),
        );
        properties.insert(
            "commitLogSwapMapInterval".to_string(),
            self.commit_log_swap_map_interval.to_string(),
        );
        properties.insert(
            "commitLogSwapMapReserveFileNum".to_string(),
            self.commit_log_swap_map_reserve_file_num.to_string(),
        );
        properties.insert(
            "logicQueueForceSwapMapInterval".to_string(),
            self.logic_queue_force_swap_map_interval.to_string(),
        );
        properties.insert(
            "logicQueueSwapMapInterval".to_string(),
            self.logic_queue_swap_map_interval.to_string(),
        );
        properties.insert(
            "cleanSwappedMapInterval".to_string(),
            self.clean_swapped_map_interval.to_string(),
        );
        properties.insert(
            "logicQueueSwapMapReserveFileNum".to_string(),
            self.logic_queue_swap_map_reserve_file_num.to_string(),
        );
        properties.insert(
            "searchBcqByCacheEnable".to_string(),
            self.search_bcq_by_cache_enable.to_string(),
        );
        properties.insert(
            "dispatchFromSenderThread".to_string(),
            self.dispatch_from_sender_thread.to_string(),
        );
        properties.insert(
            "wakeCommitWhenPutMessage".to_string(),
            self.wake_commit_when_put_message.to_string(),
        );
        properties.insert(
            "wakeFlushWhenPutMessage".to_string(),
            self.wake_flush_when_put_message.to_string(),
        );
        properties.insert(
            "enableCleanExpiredOffset".to_string(),
            self.enable_clean_expired_offset.to_string(),
        );
        properties.insert(
            "maxAsyncPutMessageRequests".to_string(),
            self.max_async_put_message_requests.to_string(),
        );
        properties.insert(
            "pullBatchMaxMessageCount".to_string(),
            self.pull_batch_max_message_count.to_string(),
        );
        properties.insert("totalReplicas".to_string(), self.total_replicas.to_string());
        properties.insert(
            "inSyncReplicas".to_string(),
            self.in_sync_replicas.to_string(),
        );
        properties.insert(
            "minInSyncReplicas".to_string(),
            self.min_in_sync_replicas.to_string(),
        );
        properties.insert(
            "allAckInSyncStateSet".to_string(),
            self.all_ack_in_sync_state_set.to_string(),
        );
        properties.insert(
            "enableAutoInSyncReplicas".to_string(),
            self.enable_auto_in_sync_replicas.to_string(),
        );
        properties.insert(
            "haFlowControlEnable".to_string(),
            self.ha_flow_control_enable.to_string(),
        );
        properties.insert(
            "maxHaTransferByteInSecond".to_string(),
            self.max_ha_transfer_byte_in_second.to_string(),
        );
        properties.insert(
            "haMaxTimeSlaveNotCatchup".to_string(),
            self.ha_max_time_slave_not_catchup.to_string(),
        );
        properties.insert(
            "syncMasterFlushOffsetWhenStartup".to_string(),
            self.sync_master_flush_offset_when_startup.to_string(),
        );
        properties.insert(
            "maxChecksumRange".to_string(),
            self.max_checksum_range.to_string(),
        );
        properties.insert(
            "replicasPerDiskPartition".to_string(),
            self.replicas_per_disk_partition.to_string(),
        );
        properties.insert(
            "logicalDiskSpaceCleanForciblyThreshold".to_string(),
            self.logical_disk_space_clean_forcibly_threshold.to_string(),
        );
        properties.insert(
            "maxSlaveResendLength".to_string(),
            self.max_slave_resend_length.to_string(),
        );
        properties.insert(
            "syncFromLastFile".to_string(),
            self.sync_from_last_file.to_string(),
        );
        properties.insert("asyncLearner".to_string(), self.async_learner.to_string());
        properties.insert(
            "maxConsumeQueueScan".to_string(),
            self.max_consume_queue_scan.to_string(),
        );
        properties.insert(
            "sampleCountThreshold".to_string(),
            self.sample_count_threshold.to_string(),
        );
        properties.insert(
            "coldDataFlowControlEenable".to_string(),
            self.cold_data_flow_control_enable.to_string(),
        );
        properties.insert(
            "coldDataScanEnable".to_string(),
            self.cold_data_scan_enable.to_string(),
        );
        properties.insert(
            "dataReadAheadEnable".to_string(),
            self.data_read_ahead_enable.to_string(),
        );
        properties.insert(
            "timerColdDataCheckIntervalMs".to_string(),
            self.timer_cold_data_check_interval_ms.to_string(),
        );
        properties.insert("sampleSteps".to_string(), self.sample_steps.to_string());
        properties.insert(
            "accessMessageInMemoryHotRatio".to_string(),
            self.access_message_in_memory_hot_ratio.to_string(),
        );
        properties.insert(
            "enableBuildConsumeQueueConcurrently".to_string(),
            self.enable_build_consume_queue_concurrently.to_string(),
        );
        properties.insert(
            "batchDispatchRequestThreadPoolNums".to_string(),
            self.batch_dispatch_request_thread_pool_nums.to_string(),
        );
        properties.insert(
            "cleanRocksdbDirtyCqIntervalMin".to_string(),
            self.clean_rocksdb_dirty_cq_interval_min.to_string(),
        );
        properties.insert(
            "statRocksdbCqIntervalSec".to_string(),
            self.stat_rocksdb_cq_interval_sec.to_string(),
        );
        properties.insert(
            "memTableFlushIntervalMs".into(),
            self.mem_table_flush_interval_ms.to_string(),
        );
        properties.insert(
            "realTimePersistRocksdbConfig".into(),
            self.real_time_persist_rocksdb_config.to_string(),
        );
        properties.insert(
            "enableRocksdbLog".into(),
            self.enable_rocksdb_log.to_string(),
        );
        properties.insert(
            "topicQueueLockNum".into(),
            self.topic_queue_lock_num.to_string(),
        );
        properties.insert(
            "maxFilterMessageSize".into(),
            self.max_filter_message_size.to_string(),
        );
        properties
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect::<HashMap<CheetahString, CheetahString>>()
    }
}
