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

use std::any::Any;
use std::collections::HashMap;
use std::path::PathBuf;

use lazy_static::lazy_static;
use serde::Deserialize;

use crate::base::store_enum::StoreType;
use crate::config::broker_role::BrokerRole;
use crate::config::flush_disk_type::FlushDiskType;
use crate::queue::single_consume_queue::CQ_STORE_UNIT_SIZE;

lazy_static! {
    static ref USER_HOME: PathBuf = dirs::home_dir().unwrap();
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MessageStoreConfig {
    pub store_path_root_dir: String,
    pub store_path_commit_log: Option<String>,
    pub store_path_dledger_commit_log: Option<String>,
    pub store_path_epoch_file: Option<String>,
    pub store_path_broker_identity: Option<String>,
    pub read_only_commit_log_store_paths: Option<String>,
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
            store_path_root_dir,
            store_path_commit_log: None,
            store_path_dledger_commit_log: None,
            store_path_epoch_file: None,
            store_path_broker_identity: None,
            read_only_commit_log_store_paths: None,
            mapped_file_size_commit_log: 1024 * 1024 * 1024, //CommitLog file size,default is 1G
            compaction_mapped_file_size: 100 * 1024 * 1024,  /* CompactinLog file size, default
                                                              * is 100M */
            compaction_cq_mapped_file_size: 10 * 1024 * 1024, /* CompactionLog consumeQueue file
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
            total_replicas: 0,
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
            return PathBuf::from(self.store_path_root_dir.clone())
                .join("commitlog")
                .to_string_lossy()
                .to_string();
        }
        self.store_path_commit_log.clone().unwrap()
    }

    pub fn is_enable_rocksdb_store(&self) -> bool {
        self.store_type == StoreType::RocksDB
    }

    pub fn get_mapped_file_size_consume_queue(&self) -> i32 {
        let factor = (self.mapped_file_size_consume_queue as f64 / (CQ_STORE_UNIT_SIZE as f64))
            .ceil() as i32;
        factor * CQ_STORE_UNIT_SIZE
    }

    pub fn get_properties(&self) -> HashMap<String,Box<dyn Any>> {
        let mut properties:HashMap<String,Box<dyn Any>>   = HashMap::new();
        properties.insert("store_path_root_dir".to_string(), Box::new(self.store_path_root_dir.clone()) as Box<dyn Any>);
        properties.insert("store_path_commit_log".to_string(), Box::new(self.store_path_commit_log.clone()) as Box<dyn Any>);
        properties.insert("store_path_dledger_commit_log".to_string(), Box::new(self.store_path_dledger_commit_log.clone()) as Box<dyn Any>);
        properties.insert("store_path_epoch_file".to_string(), Box::new(self.store_path_epoch_file.clone()) as Box<dyn Any>);
        properties.insert("store_path_broker_identity".to_string(), Box::new(self.store_path_broker_identity.clone()) as Box<dyn Any>);
        properties.insert("read_only_commit_log_store_paths".to_string(), Box::new(self.read_only_commit_log_store_paths.clone()) as Box<dyn Any>);
        properties.insert("mapped_file_size_commit_log".to_string(), Box::new(self.mapped_file_size_commit_log) as Box<dyn Any>);
        properties.insert("compaction_mapped_file_size".to_string(), Box::new(self.compaction_mapped_file_size) as Box<dyn Any>);
        properties.insert("compaction_cq_mapped_file_size".to_string(), Box::new(self.compaction_cq_mapped_file_size) as Box<dyn Any>);
        properties.insert("compaction_schedule_internal".to_string(), Box::new(self.compaction_schedule_internal) as Box<dyn Any>);
        properties.insert("max_offset_map_size".to_string(), Box::new(self.max_offset_map_size) as Box<dyn Any>);
        properties.insert("compaction_thread_num".to_string(), Box::new(self.compaction_thread_num) as Box<dyn Any>);
        properties.insert("enable_compaction".to_string(), Box::new(self.enable_compaction) as Box<dyn Any>);
        properties.insert("mapped_file_size_timer_log".to_string(), Box::new(self.mapped_file_size_timer_log) as Box<dyn Any>);
        properties.insert("timer_precision_ms".to_string(), Box::new(self.timer_precision_ms) as Box<dyn Any>);
        properties.insert("timer_roll_window_slot".to_string(), Box::new(self.timer_roll_window_slot) as Box<dyn Any>);
        properties.insert("timer_flush_interval_ms".to_string(), Box::new(self.timer_flush_interval_ms) as Box<dyn Any>);
        properties.insert("timer_get_message_thread_num".to_string(), Box::new(self.timer_get_message_thread_num) as Box<dyn Any>);
        properties.insert("timer_put_message_thread_num".to_string(), Box::new(self.timer_put_message_thread_num) as Box<dyn Any>);
        properties.insert("timer_enable_disruptor".to_string(), Box::new(self.timer_enable_disruptor) as Box<dyn Any>);
        properties.insert("timer_enable_check_metrics".to_string(), Box::new(self.timer_enable_check_metrics) as Box<dyn Any>);
        properties.insert("timer_intercept_delay_level".to_string(), Box::new(self.timer_intercept_delay_level) as Box<dyn Any>);
        properties.insert("timer_max_delay_sec".to_string(), Box::new(self.timer_max_delay_sec) as Box<dyn Any>);
        properties.insert("timer_wheel_enable".to_string(), Box::new(self.timer_wheel_enable) as Box<dyn Any>);
        properties.insert("disappear_time_after_start".to_string(), Box::new(self.disappear_time_after_start) as Box<dyn Any>);
        properties.insert("timer_stop_enqueue".to_string(), Box::new(self.timer_stop_enqueue) as Box<dyn Any>);
        properties.insert("timer_check_metrics_when".to_string(), Box::new(self.timer_check_metrics_when.clone()) as Box<dyn Any>);
        properties.insert("timer_skip_unknown_error".to_string(), Box::new(self.timer_skip_unknown_error) as Box<dyn Any>);
        properties.insert("timer_warm_enable".to_string(), Box::new(self.timer_warm_enable) as Box<dyn Any>);
        properties.insert("timer_stop_dequeue".to_string(), Box::new(self.timer_stop_dequeue) as Box<dyn Any>);
        properties.insert("timer_congest_num_each_slot".to_string(), Box::new(self.timer_congest_num_each_slot) as Box<dyn Any>);
        properties.insert("timer_metric_small_threshold".to_string(), Box::new(self.timer_metric_small_threshold) as Box<dyn Any>);
        properties.insert("timer_progress_log_interval_ms".to_string(), Box::new(self.timer_progress_log_interval_ms) as Box<dyn Any>);
        properties.insert("store_type".to_string(), Box::new(self.store_type.clone()) as Box<dyn Any>);
        properties.insert("mapped_file_size_consume_queue".to_string(), Box::new(self.mapped_file_size_consume_queue) as Box<dyn Any>);
        properties.insert("enable_consume_queue_ext".to_string(), Box::new(self.enable_consume_queue_ext) as Box<dyn Any>);
        properties.insert("mapped_file_size_consume_queue_ext".to_string(), Box::new(self.mapped_file_size_consume_queue_ext) as Box<dyn Any>);
        properties.insert("mapper_file_size_batch_consume_queue".to_string(), Box::new(self.mapper_file_size_batch_consume_queue) as Box<dyn Any>);
        properties.insert("bit_map_length_consume_queue_ext".to_string(), Box::new(self.bit_map_length_consume_queue_ext) as Box<dyn Any>);
        properties.insert("flush_interval_commit_log".to_string(), Box::new(self.flush_interval_commit_log) as Box<dyn Any>);
        properties.insert("commit_interval_commit_log".to_string(), Box::new(self.commit_interval_commit_log) as Box<dyn Any>);
        properties.insert("max_recovery_commit_log_files".to_string(), Box::new(self.max_recovery_commit_log_files) as Box<dyn Any>);
        properties.insert("disk_space_warning_level_ratio".to_string(), Box::new(self.disk_space_warning_level_ratio) as Box<dyn Any>);
        properties.insert("disk_space_clean_forcibly_ratio".to_string(), Box::new(self.disk_space_clean_forcibly_ratio) as Box<dyn Any>);
        properties.insert("use_reentrant_lock_when_put_message".to_string(), Box::new(self.use_reentrant_lock_when_put_message) as Box<dyn Any>);
        properties.insert("flush_commit_log_timed".to_string(), Box::new(self.flush_commit_log_timed) as Box<dyn Any>);
        properties.insert("flush_interval_consume_queue".to_string(), Box::new(self.flush_interval_consume_queue) as Box<dyn Any>);
        properties.insert("clean_resource_interval".to_string(), Box::new(self.clean_resource_interval) as Box<dyn Any>);
        properties.insert("delete_commit_log_files_interval".to_string(), Box::new(self.delete_commit_log_files_interval) as Box<dyn Any>);
        properties.insert("delete_consume_queue_files_interval".to_string(), Box::new(self.delete_consume_queue_files_interval) as Box<dyn Any>);
        properties.insert("destroy_mapped_file_interval_forcibly".to_string(), Box::new(self.destroy_mapped_file_interval_forcibly) as Box<dyn Any>);
        properties.insert("redelete_hanged_file_interval".to_string(), Box::new(self.redelete_hanged_file_interval) as Box<dyn Any>);
        properties.insert("delete_when".to_string(), Box::new(self.delete_when.clone()) as Box<dyn Any>);
        properties.insert("disk_max_used_space_ratio".to_string(), Box::new(self.disk_max_used_space_ratio) as Box<dyn Any>);
        properties.insert("file_reserved_time".to_string(), Box::new(self.file_reserved_time) as Box<dyn Any>);
        properties.insert("delete_file_batch_max".to_string(), Box::new(self.delete_file_batch_max) as Box<dyn Any>);
        properties.insert("put_msg_index_hight_water".to_string(), Box::new(self.put_msg_index_hight_water) as Box<dyn Any>);
        properties.insert("max_message_size".to_string(), Box::new(self.max_message_size) as Box<dyn Any>);
        properties.insert("check_crc_on_recover".to_string(), Box::new(self.check_crc_on_recover) as Box<dyn Any>);
        properties.insert("flush_commit_log_least_pages".to_string(), Box::new(self.flush_commit_log_least_pages) as Box<dyn Any>);
        properties.insert("commit_commit_log_least_pages".to_string(), Box::new(self.commit_commit_log_least_pages) as Box<dyn Any>);
        properties.insert("flush_least_pages_when_warm_mapped_file".to_string(), Box::new(self.flush_least_pages_when_warm_mapped_file) as Box<dyn Any>);
        properties.insert("flush_consume_queue_least_pages".to_string(), Box::new(self.flush_consume_queue_least_pages) as Box<dyn Any>);
        properties.insert("flush_commit_log_thorough_interval".to_string(), Box::new(self.flush_commit_log_thorough_interval) as Box<dyn Any>);
        properties.insert("commit_commit_log_thorough_interval".to_string(), Box::new(self.commit_commit_log_thorough_interval) as Box<dyn Any>);
        properties.insert("flush_consume_queue_thorough_interval".to_string(), Box::new(self.flush_consume_queue_thorough_interval) as Box<dyn Any>);
        properties.insert("max_transfer_bytes_on_message_in_memory".to_string(), Box::new(self.max_transfer_bytes_on_message_in_memory) as Box<dyn Any>);
        properties.insert("max_transfer_count_on_message_in_memory".to_string(), Box::new(self.max_transfer_count_on_message_in_memory) as Box<dyn Any>);
        properties.insert("max_transfer_bytes_on_message_in_disk".to_string(), Box::new(self.max_transfer_bytes_on_message_in_disk) as Box<dyn Any>);
        properties.insert("max_transfer_count_on_message_in_disk".to_string(), Box::new(self.max_transfer_count_on_message_in_disk) as Box<dyn Any>);
        properties.insert("access_message_in_memory_max_ratio".to_string(), Box::new(self.access_message_in_memory_max_ratio) as Box<dyn Any>);
        properties.insert("message_index_enable".to_string(), Box::new(self.message_index_enable) as Box<dyn Any>);
        properties.insert("max_hash_slot_num".to_string(), Box::new(self.max_hash_slot_num) as Box<dyn Any>);
        properties.insert("max_index_num".to_string(), Box::new(self.max_index_num) as Box<dyn Any>);
        properties.insert("max_msgs_num_batch".to_string(), Box::new(self.max_msgs_num_batch) as Box<dyn Any>);
        properties.insert("message_index_safe".to_string(), Box::new(self.message_index_safe) as Box<dyn Any>);
        properties.insert("ha_listen_port".to_string(), Box::new(self.ha_listen_port) as Box<dyn Any>);
        properties.insert("ha_send_heartbeat_interval".to_string(), Box::new(self.ha_send_heartbeat_interval) as Box<dyn Any>);
        properties.insert("ha_housekeeping_interval".to_string(), Box::new(self.ha_housekeeping_interval) as Box<dyn Any>);
        properties.insert("ha_transfer_batch_size".to_string(), Box::new(self.ha_transfer_batch_size) as Box<dyn Any>);
        properties.insert("ha_master_address".to_string(), Box::new(self.ha_master_address.clone()) as Box<dyn Any>);
        properties.insert("ha_max_gap_not_in_sync".to_string(), Box::new(self.ha_max_gap_not_in_sync) as Box<dyn Any>);
        properties.insert("broker_role".to_string(), Box::new(self.broker_role.clone()) as Box<dyn Any>);
        properties.insert("flush_disk_type".to_string(), Box::new(self.flush_disk_type.clone()) as Box<dyn Any>);
        properties.insert("sync_flush_timeout".to_string(), Box::new(self.sync_flush_timeout) as Box<dyn Any>);
        properties.insert("put_message_timeout".to_string(), Box::new(self.put_message_timeout) as Box<dyn Any>);
        properties.insert("slave_timeout".to_string(), Box::new(self.slave_timeout) as Box<dyn Any>);
        properties.insert("message_delay_level".to_string(), Box::new(self.message_delay_level.clone()) as Box<dyn Any>);
        properties.insert("flush_delay_offset_interval".to_string(), Box::new(self.flush_delay_offset_interval) as Box<dyn Any>);
        properties.insert("clean_file_forcibly_enable".to_string(), Box::new(self.clean_file_forcibly_enable) as Box<dyn Any>);
        properties.insert("warm_mapped_file_enable".to_string(), Box::new(self.warm_mapped_file_enable) as Box<dyn Any>);
        properties.insert("offset_check_in_slave".to_string(), Box::new(self.offset_check_in_slave) as Box<dyn Any>);
        properties.insert("debug_lock_enable".to_string(), Box::new(self.debug_lock_enable) as Box<dyn Any>);
        properties.insert("duplication_enable".to_string(), Box::new(self.duplication_enable) as Box<dyn Any>);
        properties.insert("disk_fall_recorded".to_string(), Box::new(self.disk_fall_recorded) as Box<dyn Any>);
        properties.insert("os_page_cache_busy_timeout_mills".to_string(), Box::new(self.os_page_cache_busy_timeout_mills) as Box<dyn Any>);
        properties.insert("default_query_max_num".to_string(), Box::new(self.default_query_max_num) as Box<dyn Any>);
        properties.insert("transient_store_pool_enable".to_string(), Box::new(self.transient_store_pool_enable) as Box<dyn Any>);
        properties.insert("transient_store_pool_size".to_string(), Box::new(self.transient_store_pool_size) as Box<dyn Any>);
        properties.insert("fast_fail_if_no_buffer_in_store_pool".to_string(), Box::new(self.fast_fail_if_no_buffer_in_store_pool) as Box<dyn Any>);
        properties.insert("enable_dledger_commit_log".to_string(), Box::new(self.enable_dledger_commit_log) as Box<dyn Any>);
        properties.insert("dledger_group".to_string(), Box::new(self.dledger_group.clone()) as Box<dyn Any>);
        properties.insert("dledger_peers".to_string(), Box::new(self.dledger_peers.clone()) as Box<dyn Any>);
        properties.insert("dledger_self_id".to_string(), Box::new(self.dledger_self_id.clone()) as Box<dyn Any>);
        properties.insert("preferred_leader_id".to_string(), Box::new(self.preferred_leader_id.clone()) as Box<dyn Any>);
        properties.insert("enable_batch_push".to_string(), Box::new(self.enable_batch_push) as Box<dyn Any>);
        properties.insert("enable_schedule_message_stats".to_string(), Box::new(self.enable_schedule_message_stats) as Box<dyn Any>);
        properties.insert("enable_lmq".to_string(), Box::new(self.enable_lmq) as Box<dyn Any>);
        properties.insert("enable_multi_dispatch".to_string(), Box::new(self.enable_multi_dispatch) as Box<dyn Any>);
        properties.insert("max_lmq_consume_queue_num".to_string(), Box::new(self.max_lmq_consume_queue_num) as Box<dyn Any>);
        properties.insert("enable_schedule_async_deliver".to_string(), Box::new(self.enable_schedule_async_deliver) as Box<dyn Any>);
        properties.insert("schedule_async_deliver_max_pending_limit".to_string(), Box::new(self.schedule_async_deliver_max_pending_limit) as Box<dyn Any>);
        properties.insert("schedule_async_deliver_max_resend_num2_blocked".to_string(), Box::new(self.schedule_async_deliver_max_resend_num2_blocked) as Box<dyn Any>);
        properties.insert("max_batch_delete_files_num".to_string(), Box::new(self.max_batch_delete_files_num) as Box<dyn Any>);
        properties.insert("dispatch_cq_threads".to_string(), Box::new(self.dispatch_cq_threads) as Box<dyn Any>);
        properties.insert("dispatch_cq_cache_num".to_string(), Box::new(self.dispatch_cq_cache_num) as Box<dyn Any>);
        properties.insert("enable_async_reput".to_string(), Box::new(self.enable_async_reput) as Box<dyn Any>);
        properties.insert("recheck_reput_offset_from_cq".to_string(), Box::new(self.recheck_reput_offset_from_cq) as Box<dyn Any>);
        properties.insert("max_topic_length".to_string(), Box::new(self.max_topic_length) as Box<dyn Any>);
        properties.insert("auto_message_version_on_topic_len".to_string(), Box::new(self.auto_message_version_on_topic_len) as Box<dyn Any>);
        properties.insert("enabled_append_prop_crc".to_string(), Box::new(self.enabled_append_prop_crc) as Box<dyn Any>);
        properties.insert("force_verify_prop_crc".to_string(), Box::new(self.force_verify_prop_crc) as Box<dyn Any>);
        properties.insert("travel_cq_file_num_when_get_message".to_string(), Box::new(self.travel_cq_file_num_when_get_message) as Box<dyn Any>);
        properties.insert("correct_logic_min_offset_sleep_interval".to_string(), Box::new(self.correct_logic_min_offset_sleep_interval) as Box<dyn Any>);
        properties.insert("correct_logic_min_offset_force_interval".to_string(), Box::new(self.correct_logic_min_offset_force_interval) as Box<dyn Any>);
        properties.insert("mapped_file_swap_enable".to_string(), Box::new(self.mapped_file_swap_enable) as Box<dyn Any>);
        properties.insert("commit_log_force_swap_map_interval".to_string(), Box::new(self.commit_log_force_swap_map_interval) as Box<dyn Any>);
        properties.insert("commit_log_swap_map_interval".to_string(), Box::new(self.commit_log_swap_map_interval) as Box<dyn Any>);
        properties.insert("commit_log_swap_map_reserve_file_num".to_string(), Box::new(self.commit_log_swap_map_reserve_file_num) as Box<dyn Any>);
        properties.insert("logic_queue_force_swap_map_interval".to_string(), Box::new(self.logic_queue_force_swap_map_interval) as Box<dyn Any>);
        properties.insert("logic_queue_swap_map_interval".to_string(), Box::new(self.logic_queue_swap_map_interval) as Box<dyn Any>);
        properties.insert("clean_swapped_map_interval".to_string(), Box::new(self.clean_swapped_map_interval) as Box<dyn Any>);
        properties.insert("logic_queue_swap_map_reserve_file_num".to_string(), Box::new(self.logic_queue_swap_map_reserve_file_num) as Box<dyn Any>);
        properties.insert("search_bcq_by_cache_enable".to_string(), Box::new(self.search_bcq_by_cache_enable) as Box<dyn Any>);
        properties.insert("dispatch_from_sender_thread".to_string(), Box::new(self.dispatch_from_sender_thread) as Box<dyn Any>);
        properties.insert("wake_commit_when_put_message".to_string(), Box::new(self.wake_commit_when_put_message) as Box<dyn Any>);
        properties.insert("wake_flush_when_put_message".to_string(), Box::new(self.wake_flush_when_put_message) as Box<dyn Any>);
        properties.insert("enable_clean_expired_offset".to_string(), Box::new(self.enable_clean_expired_offset) as Box<dyn Any>);
        properties.insert("max_async_put_message_requests".to_string(), Box::new(self.max_async_put_message_requests) as Box<dyn Any>);
        properties.insert("pull_batch_max_message_count".to_string(), Box::new(self.pull_batch_max_message_count) as Box<dyn Any>);
        properties.insert("total_replicas".to_string(), Box::new(self.total_replicas) as Box<dyn Any>);
        properties.insert("in_sync_replicas".to_string(), Box::new(self.in_sync_replicas) as Box<dyn Any>);
        properties.insert("min_in_sync_replicas".to_string(), Box::new(self.min_in_sync_replicas) as Box<dyn Any>);
        properties.insert("all_ack_in_sync_state_set".to_string(), Box::new(self.all_ack_in_sync_state_set) as Box<dyn Any>);
        properties.insert("enable_auto_in_sync_replicas".to_string(), Box::new(self.enable_auto_in_sync_replicas) as Box<dyn Any>);
        properties.insert("ha_flow_control_enable".to_string(), Box::new(self.ha_flow_control_enable) as Box<dyn Any>);
        properties.insert("max_ha_transfer_byte_in_second".to_string(), Box::new(self.max_ha_transfer_byte_in_second) as Box<dyn Any>);
        properties.insert("ha_max_time_slave_not_catchup".to_string(), Box::new(self.ha_max_time_slave_not_catchup) as Box<dyn Any>);
        properties.insert("sync_master_flush_offset_when_startup".to_string(), Box::new(self.sync_master_flush_offset_when_startup) as Box<dyn Any>);
        properties.insert("max_checksum_range".to_string(), Box::new(self.max_checksum_range) as Box<dyn Any>);
        properties.insert("replicas_per_disk_partition".to_string(), Box::new(self.replicas_per_disk_partition) as Box<dyn Any>);
        properties.insert("logical_disk_space_clean_forcibly_threshold".to_string(), Box::new(self.logical_disk_space_clean_forcibly_threshold) as Box<dyn Any>);
        properties.insert("max_slave_resend_length".to_string(), Box::new(self.max_slave_resend_length) as Box<dyn Any>);
        properties.insert("sync_from_last_file".to_string(), Box::new(self.sync_from_last_file) as Box<dyn Any>);
        properties.insert("async_learner".to_string(), Box::new(self.async_learner) as Box<dyn Any>);
        properties.insert("max_consume_queue_scan".to_string(), Box::new(self.max_consume_queue_scan) as Box<dyn Any>);
        properties.insert("sample_count_threshold".to_string(), Box::new(self.sample_count_threshold) as Box<dyn Any>);
        properties.insert("cold_data_flow_control_enable".to_string(), Box::new(self.cold_data_flow_control_enable) as Box<dyn Any>);
        properties.insert("cold_data_scan_enable".to_string(), Box::new(self.cold_data_scan_enable) as Box<dyn Any>);
        properties.insert("data_read_ahead_enable".to_string(), Box::new(self.data_read_ahead_enable) as Box<dyn Any>);
        properties.insert("timer_cold_data_check_interval_ms".to_string(), Box::new(self.timer_cold_data_check_interval_ms) as Box<dyn Any>);
        properties.insert("sample_steps".to_string(), Box::new(self.sample_steps) as Box<dyn Any>);
        properties.insert("access_message_in_memory_hot_ratio".to_string(), Box::new(self.access_message_in_memory_hot_ratio) as Box<dyn Any>);
        properties.insert("enable_build_consume_queue_concurrently".to_string(), Box::new(self.enable_build_consume_queue_concurrently) as Box<dyn Any>);
        properties.insert("batch_dispatch_request_thread_pool_nums".to_string(), Box::new(self.batch_dispatch_request_thread_pool_nums) as Box<dyn Any>);
        properties.insert("clean_rocksdb_dirty_cq_interval_min".to_string(), Box::new(self.clean_rocksdb_dirty_cq_interval_min) as Box<dyn Any>);
        properties.insert("stat_rocksdb_cq_interval_sec".to_string(), Box::new(self.stat_rocksdb_cq_interval_sec) as Box<dyn Any>);
        properties.insert("mem_table_flush_interval_ms".to_string(), Box::new(self.mem_table_flush_interval_ms) as Box<dyn Any>);
        properties.insert("real_time_persist_rocksdb_config".to_string(), Box::new(self.real_time_persist_rocksdb_config) as Box<dyn Any>);
        properties.insert("enable_rocksdb_log".to_string(), Box::new(self.enable_rocksdb_log) as Box<dyn Any>);
        properties.insert("topic_queue_lock_num".to_string(), Box::new(self.topic_queue_lock_num) as Box<dyn Any>);
        properties.insert("max_filter_message_size".to_string(), Box::new(self.max_filter_message_size) as Box<dyn Any>);
        properties
    }
}
