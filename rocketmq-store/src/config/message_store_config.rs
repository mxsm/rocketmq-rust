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

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::LazyLock;

use cheetah_string::CheetahString;
use rocketmq_common::common::broker::broker_role::BrokerRole;
use serde::Deserialize;

use crate::base::store_enum::StoreType;
use crate::config::flush_disk_type::FlushDiskType;
use crate::queue::single_consume_queue::CQ_STORE_UNIT_SIZE;

static USER_HOME: LazyLock<PathBuf> = LazyLock::new(|| dirs::home_dir().unwrap());

/// Default value functions for Serde deserialization
mod defaults {
    use super::*;

    pub fn store_path_root_dir() -> CheetahString {
        USER_HOME.clone().join("store").to_string_lossy().to_string().into()
    }

    pub fn mapped_file_size_commit_log() -> usize {
        1024 * 1024 * 1024 // 1GB
    }

    pub fn compaction_mapped_file_size() -> usize {
        100 * 1024 * 1024 // 100MB
    }

    pub fn compaction_cq_mapped_file_size() -> usize {
        10 * 1024 * 1024 // 10MB
    }

    pub fn compaction_schedule_internal() -> usize {
        15 * 60 * 1000 // 15 minutes
    }

    pub fn max_offset_map_size() -> usize {
        100 * 1024 * 1024 // 100MB
    }

    pub fn compaction_thread_num() -> usize {
        6
    }

    pub fn mapped_file_size_timer_log() -> usize {
        100 * 1024 * 1024 // 100MB
    }

    pub fn timer_precision_ms() -> u64 {
        1000 // 1 second
    }

    pub fn timer_roll_window_slot() -> usize {
        3600 * 24 * 2 // 2 days
    }

    pub fn timer_flush_interval_ms() -> usize {
        1000 // 1 second
    }

    pub fn timer_get_message_thread_num() -> usize {
        3
    }

    pub fn timer_put_message_thread_num() -> usize {
        3
    }

    pub fn timer_wheel_enable() -> bool {
        true
    }

    pub fn disappear_time_after_start() -> i64 {
        -1
    }

    pub fn timer_check_metrics_when() -> String {
        "".to_string()
    }

    pub fn store_type() -> StoreType {
        StoreType::default()
    }

    pub fn mapped_file_size_consume_queue() -> usize {
        300000 * 20
    }

    pub fn mapped_file_size_consume_queue_ext() -> usize {
        48 * 1024 * 1024 // 48MB
    }

    pub fn mapper_file_size_batch_consume_queue() -> usize {
        300000 * 46
    }

    pub fn bit_map_length_consume_queue_ext() -> usize {
        64
    }

    pub fn flush_interval_commit_log() -> i32 {
        500
    }

    pub fn commit_interval_commit_log() -> u64 {
        200
    }

    pub fn flush_commit_log_timed() -> bool {
        true
    }

    pub fn flush_interval_consume_queue() -> usize {
        1000
    }

    pub fn clean_resource_interval() -> usize {
        10000
    }

    pub fn delete_commit_log_files_interval() -> usize {
        100
    }

    pub fn delete_consume_queue_files_interval() -> usize {
        100
    }

    pub fn destroy_mapped_file_interval_forcibly() -> usize {
        1000 * 120 // 2 minutes
    }

    pub fn redelete_hanged_file_interval() -> usize {
        1000 * 120 // 2 minutes
    }

    pub fn delete_when() -> String {
        "04".to_string()
    }

    pub fn disk_max_used_space_ratio() -> usize {
        75
    }

    pub fn max_message_size() -> i32 {
        1024 * 1024 * 4 // 4MB
    }

    pub fn commit_commit_log_least_pages() -> i32 {
        4
    }

    pub fn flush_commit_log_thorough_interval() -> i32 {
        1000 * 10 // 10 seconds
    }

    pub fn commit_commit_log_thorough_interval() -> u64 {
        200
    }

    pub fn max_transfer_bytes_on_message_in_memory() -> u64 {
        1024 * 256 // 256KB
    }

    pub fn max_transfer_count_on_message_in_memory() -> u64 {
        32
    }

    pub fn max_transfer_bytes_on_message_in_disk() -> u64 {
        1024 * 64 // 64KB
    }

    pub fn max_transfer_count_on_message_in_disk() -> u64 {
        8
    }

    pub fn access_message_in_memory_max_ratio() -> usize {
        40
    }

    pub fn message_index_enable() -> bool {
        true
    }

    pub fn max_hash_slot_num() -> u32 {
        5000000
    }

    pub fn max_index_num() -> u32 {
        5000000 * 4
    }

    pub fn max_msgs_num_batch() -> usize {
        64
    }

    pub fn broker_role() -> BrokerRole {
        BrokerRole::default()
    }

    pub fn flush_disk_type() -> FlushDiskType {
        FlushDiskType::SyncFlush
    }

    pub fn sync_flush_timeout() -> u64 {
        1000 * 5 // 5 seconds
    }

    pub fn message_delay_level() -> String {
        "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h".to_string()
    }

    pub fn flush_delay_offset_interval() -> u64 {
        10_000 // 10 seconds
    }

    pub fn os_page_cache_busy_timeout_mills() -> u64 {
        1000 // 1 second
    }

    pub fn auto_message_version_on_topic_len() -> bool {
        true
    }

    pub fn travel_cq_file_num_when_get_message() -> usize {
        1
    }

    pub fn total_replicas() -> usize {
        1
    }

    pub fn in_sync_replicas() -> i32 {
        1
    }

    pub fn topic_queue_lock_num() -> usize {
        32
    }

    pub const fn max_ha_transfer_byte_in_second() -> usize {
        100 * 1024 * 1024
    }

    pub fn max_filter_message_size() -> i32 {
        16000
    }
    pub fn ha_housekeeping_interval() -> u64 {
        1000 * 20
    }
    pub fn ha_send_heartbeat_interval() -> u64 {
        1000 * 5
    }
    pub fn ha_listen_port() -> usize {
        10912
    }
    pub fn default_query_max_num() -> usize {
        32
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MessageStoreConfig {
    #[serde(default = "defaults::store_path_root_dir")]
    pub store_path_root_dir: CheetahString,

    #[serde(default)]
    pub store_path_commit_log: Option<CheetahString>,

    #[serde(default)]
    pub store_path_dledger_commit_log: Option<CheetahString>,

    #[serde(default)]
    pub store_path_epoch_file: Option<CheetahString>,

    #[serde(default)]
    pub store_path_broker_identity: Option<CheetahString>,

    #[serde(default)]
    pub read_only_commit_log_store_paths: Option<CheetahString>,

    #[serde(default = "defaults::mapped_file_size_commit_log")]
    pub mapped_file_size_commit_log: usize,

    #[serde(default = "defaults::compaction_mapped_file_size")]
    pub compaction_mapped_file_size: usize,

    #[serde(default = "defaults::compaction_cq_mapped_file_size")]
    pub compaction_cq_mapped_file_size: usize,

    #[serde(default = "defaults::compaction_schedule_internal")]
    pub compaction_schedule_internal: usize,

    #[serde(default = "defaults::max_offset_map_size")]
    pub max_offset_map_size: usize,

    #[serde(default = "defaults::compaction_thread_num")]
    pub compaction_thread_num: usize,

    #[serde(default)]
    pub enable_compaction: bool,

    #[serde(default = "defaults::mapped_file_size_timer_log")]
    pub mapped_file_size_timer_log: usize,

    #[serde(default = "defaults::timer_precision_ms")]
    pub timer_precision_ms: u64,

    #[serde(default = "defaults::timer_roll_window_slot")]
    pub timer_roll_window_slot: usize,

    #[serde(default = "defaults::timer_flush_interval_ms")]
    pub timer_flush_interval_ms: usize,

    #[serde(default = "defaults::timer_get_message_thread_num")]
    pub timer_get_message_thread_num: usize,

    #[serde(default = "defaults::timer_put_message_thread_num")]
    pub timer_put_message_thread_num: usize,

    #[serde(default)]
    pub timer_enable_disruptor: bool,

    #[serde(default)]
    pub timer_enable_check_metrics: bool,

    #[serde(default)]
    pub timer_intercept_delay_level: bool,

    #[serde(default)]
    pub timer_max_delay_sec: u64,

    #[serde(default = "defaults::timer_wheel_enable")]
    pub timer_wheel_enable: bool,

    #[serde(default = "defaults::disappear_time_after_start")]
    pub disappear_time_after_start: i64,

    #[serde(default)]
    pub timer_stop_enqueue: bool,

    #[serde(default = "defaults::timer_check_metrics_when")]
    pub timer_check_metrics_when: String,

    #[serde(default)]
    pub timer_skip_unknown_error: bool,

    #[serde(default)]
    pub timer_warm_enable: bool,

    #[serde(default)]
    pub timer_stop_dequeue: bool,

    #[serde(default)]
    pub timer_congest_num_each_slot: usize,

    #[serde(default)]
    pub timer_metric_small_threshold: usize,

    #[serde(default)]
    pub timer_progress_log_interval_ms: usize,

    #[serde(default = "defaults::store_type")]
    pub store_type: StoreType,

    #[serde(default = "defaults::mapped_file_size_consume_queue")]
    pub mapped_file_size_consume_queue: usize,

    #[serde(default)]
    pub enable_consume_queue_ext: bool,

    #[serde(default = "defaults::mapped_file_size_consume_queue_ext")]
    pub mapped_file_size_consume_queue_ext: usize,

    #[serde(default = "defaults::mapper_file_size_batch_consume_queue")]
    pub mapper_file_size_batch_consume_queue: usize,

    #[serde(default = "defaults::bit_map_length_consume_queue_ext")]
    pub bit_map_length_consume_queue_ext: usize,

    #[serde(default = "defaults::flush_interval_commit_log")]
    pub flush_interval_commit_log: i32,

    #[serde(default = "defaults::commit_interval_commit_log")]
    pub commit_interval_commit_log: u64,

    #[serde(default)]
    pub max_recovery_commit_log_files: usize,

    #[serde(default)]
    pub disk_space_warning_level_ratio: usize,

    #[serde(default)]
    pub disk_space_clean_forcibly_ratio: usize,

    #[serde(default)]
    pub use_reentrant_lock_when_put_message: bool,

    #[serde(default = "defaults::flush_commit_log_timed")]
    pub flush_commit_log_timed: bool,

    #[serde(default = "defaults::flush_interval_consume_queue")]
    pub flush_interval_consume_queue: usize,

    #[serde(default = "defaults::clean_resource_interval")]
    pub clean_resource_interval: usize,

    #[serde(default = "defaults::delete_commit_log_files_interval")]
    pub delete_commit_log_files_interval: usize,

    #[serde(default = "defaults::delete_consume_queue_files_interval")]
    pub delete_consume_queue_files_interval: usize,

    #[serde(default = "defaults::destroy_mapped_file_interval_forcibly")]
    pub destroy_mapped_file_interval_forcibly: usize,

    #[serde(default = "defaults::redelete_hanged_file_interval")]
    pub redelete_hanged_file_interval: usize,

    #[serde(default = "defaults::delete_when")]
    pub delete_when: String,

    #[serde(default = "defaults::disk_max_used_space_ratio")]
    pub disk_max_used_space_ratio: usize,

    #[serde(default)]
    pub file_reserved_time: usize,

    #[serde(default)]
    pub delete_file_batch_max: usize,

    #[serde(default)]
    pub put_msg_index_hight_water: usize,

    #[serde(default = "defaults::max_message_size")]
    pub max_message_size: i32,

    #[serde(default)]
    pub check_crc_on_recover: bool,

    #[serde(default)]
    pub flush_commit_log_least_pages: i32,

    #[serde(default = "defaults::commit_commit_log_least_pages")]
    pub commit_commit_log_least_pages: i32,

    #[serde(default)]
    pub flush_least_pages_when_warm_mapped_file: usize,

    #[serde(default)]
    pub flush_consume_queue_least_pages: usize,

    #[serde(default = "defaults::flush_commit_log_thorough_interval")]
    pub flush_commit_log_thorough_interval: i32,

    #[serde(default = "defaults::commit_commit_log_thorough_interval")]
    pub commit_commit_log_thorough_interval: u64,

    #[serde(default)]
    pub flush_consume_queue_thorough_interval: usize,

    #[serde(default = "defaults::max_transfer_bytes_on_message_in_memory")]
    pub max_transfer_bytes_on_message_in_memory: u64,

    #[serde(default = "defaults::max_transfer_count_on_message_in_memory")]
    pub max_transfer_count_on_message_in_memory: u64,

    #[serde(default = "defaults::max_transfer_bytes_on_message_in_disk")]
    pub max_transfer_bytes_on_message_in_disk: u64,

    #[serde(default = "defaults::max_transfer_count_on_message_in_disk")]
    pub max_transfer_count_on_message_in_disk: u64,

    #[serde(default = "defaults::access_message_in_memory_max_ratio")]
    pub access_message_in_memory_max_ratio: usize,

    #[serde(default = "defaults::message_index_enable")]
    pub message_index_enable: bool,

    #[serde(default = "defaults::max_hash_slot_num")]
    pub max_hash_slot_num: u32,

    #[serde(default = "defaults::max_index_num")]
    pub max_index_num: u32,

    #[serde(default = "defaults::max_msgs_num_batch")]
    pub max_msgs_num_batch: usize,

    #[serde(default)]
    pub message_index_safe: bool,

    #[serde(default = "defaults::ha_listen_port")]
    pub ha_listen_port: usize,

    #[serde(default = "defaults::ha_send_heartbeat_interval")]
    pub ha_send_heartbeat_interval: u64,

    #[serde(default = "defaults::ha_housekeeping_interval")]
    pub ha_housekeeping_interval: u64,

    #[serde(default)]
    pub ha_transfer_batch_size: usize,

    #[serde(default)]
    pub ha_master_address: Option<String>,

    #[serde(default)]
    pub ha_max_gap_not_in_sync: usize,

    #[serde(default = "defaults::broker_role")]
    pub broker_role: BrokerRole,

    #[serde(default = "defaults::flush_disk_type")]
    pub flush_disk_type: FlushDiskType,

    #[serde(default = "defaults::sync_flush_timeout")]
    pub sync_flush_timeout: u64,

    #[serde(default)]
    pub put_message_timeout: usize,

    #[serde(default)]
    pub slave_timeout: usize,

    #[serde(default = "defaults::message_delay_level")]
    pub message_delay_level: String,

    #[serde(default = "defaults::flush_delay_offset_interval")]
    pub flush_delay_offset_interval: u64,

    #[serde(default)]
    pub clean_file_forcibly_enable: bool,

    #[serde(default)]
    pub warm_mapped_file_enable: bool,

    #[serde(default)]
    pub offset_check_in_slave: bool,

    #[serde(default)]
    pub debug_lock_enable: bool,

    #[serde(default)]
    pub duplication_enable: bool,

    #[serde(default)]
    pub disk_fall_recorded: bool,

    #[serde(default = "defaults::os_page_cache_busy_timeout_mills")]
    pub os_page_cache_busy_timeout_mills: u64,

    #[serde(default = "defaults::default_query_max_num")]
    pub default_query_max_num: usize,

    #[serde(default)]
    pub transient_store_pool_enable: bool,

    #[serde(default)]
    pub transient_store_pool_size: usize,

    #[serde(default)]
    pub fast_fail_if_no_buffer_in_store_pool: bool,

    #[serde(default)]
    pub enable_dledger_commit_log: bool,

    #[serde(default)]
    pub dledger_group: Option<String>,

    #[serde(default)]
    pub dledger_peers: Option<String>,

    #[serde(default)]
    pub dledger_self_id: Option<String>,

    #[serde(default)]
    pub preferred_leader_id: Option<String>,

    #[serde(default)]
    pub enable_batch_push: bool,

    #[serde(default)]
    pub enable_schedule_message_stats: bool,

    #[serde(default)]
    pub enable_lmq: bool,

    #[serde(default)]
    pub enable_multi_dispatch: bool,

    #[serde(default)]
    pub max_lmq_consume_queue_num: usize,

    #[serde(default)]
    pub enable_schedule_async_deliver: bool,

    #[serde(default)]
    pub schedule_async_deliver_max_pending_limit: usize,

    #[serde(default)]
    pub schedule_async_deliver_max_resend_num2_blocked: usize,

    #[serde(default)]
    pub max_batch_delete_files_num: usize,

    #[serde(default)]
    pub dispatch_cq_threads: usize,

    #[serde(default)]
    pub dispatch_cq_cache_num: usize,

    #[serde(default)]
    pub enable_async_reput: bool,

    #[serde(default)]
    pub recheck_reput_offset_from_cq: bool,

    #[serde(default)]
    pub max_topic_length: usize,

    #[serde(default = "defaults::auto_message_version_on_topic_len")]
    pub auto_message_version_on_topic_len: bool,

    #[serde(default)]
    pub enabled_append_prop_crc: bool,

    #[serde(default)]
    pub force_verify_prop_crc: bool,

    #[serde(default = "defaults::travel_cq_file_num_when_get_message")]
    pub travel_cq_file_num_when_get_message: usize,

    #[serde(default)]
    pub correct_logic_min_offset_sleep_interval: usize,

    #[serde(default)]
    pub correct_logic_min_offset_force_interval: usize,

    #[serde(default)]
    pub mapped_file_swap_enable: bool,

    #[serde(default)]
    pub commit_log_force_swap_map_interval: usize,

    #[serde(default)]
    pub commit_log_swap_map_interval: usize,

    #[serde(default)]
    pub commit_log_swap_map_reserve_file_num: usize,

    #[serde(default)]
    pub logic_queue_force_swap_map_interval: usize,

    #[serde(default)]
    pub logic_queue_swap_map_interval: usize,

    #[serde(default)]
    pub clean_swapped_map_interval: usize,

    #[serde(default)]
    pub logic_queue_swap_map_reserve_file_num: usize,

    #[serde(default)]
    pub search_bcq_by_cache_enable: bool,

    #[serde(default)]
    pub dispatch_from_sender_thread: bool,

    #[serde(default)]
    pub wake_commit_when_put_message: bool,

    #[serde(default)]
    pub wake_flush_when_put_message: bool,

    #[serde(default)]
    pub enable_clean_expired_offset: bool,

    #[serde(default)]
    pub max_async_put_message_requests: usize,

    #[serde(default)]
    pub pull_batch_max_message_count: usize,

    #[serde(default = "defaults::total_replicas")]
    pub total_replicas: usize,

    #[serde(default = "defaults::in_sync_replicas")]
    pub in_sync_replicas: i32,

    #[serde(default)]
    pub min_in_sync_replicas: usize,

    #[serde(default)]
    pub all_ack_in_sync_state_set: bool,

    #[serde(default)]
    pub enable_auto_in_sync_replicas: bool,

    #[serde(default)]
    pub ha_flow_control_enable: bool,

    #[serde(default = "defaults::topic_queue_lock_num")]
    pub max_ha_transfer_byte_in_second: usize,

    #[serde(default)]
    pub ha_max_time_slave_not_catchup: usize,

    #[serde(default)]
    pub sync_master_flush_offset_when_startup: bool,

    #[serde(default)]
    pub max_checksum_range: usize,

    #[serde(default)]
    pub replicas_per_disk_partition: usize,

    #[serde(default)]
    pub logical_disk_space_clean_forcibly_threshold: f64,

    #[serde(default)]
    pub max_slave_resend_length: usize,

    #[serde(default)]
    pub sync_from_last_file: bool,

    #[serde(default)]
    pub async_learner: bool,

    #[serde(default)]
    pub max_consume_queue_scan: usize,

    #[serde(default)]
    pub sample_count_threshold: usize,

    #[serde(default)]
    pub cold_data_flow_control_enable: bool,

    #[serde(default)]
    pub cold_data_scan_enable: bool,

    #[serde(default)]
    pub data_read_ahead_enable: bool,

    #[serde(default)]
    pub timer_cold_data_check_interval_ms: usize,

    #[serde(default)]
    pub sample_steps: usize,

    #[serde(default)]
    pub access_message_in_memory_hot_ratio: usize,

    #[serde(default)]
    pub enable_build_consume_queue_concurrently: bool,

    #[serde(default)]
    pub batch_dispatch_request_thread_pool_nums: usize,

    #[serde(default)]
    pub clean_rocksdb_dirty_cq_interval_min: usize,

    #[serde(default)]
    pub stat_rocksdb_cq_interval_sec: usize,

    #[serde(default)]
    pub mem_table_flush_interval_ms: usize,

    #[serde(default)]
    pub real_time_persist_rocksdb_config: bool,

    #[serde(default)]
    pub enable_rocksdb_log: bool,

    #[serde(default = "defaults::topic_queue_lock_num")]
    pub topic_queue_lock_num: usize,

    #[serde(default = "defaults::max_filter_message_size")]
    pub max_filter_message_size: i32,

    #[serde(default)]
    pub enable_dleger_commit_log: bool,

    #[serde(default)]
    pub rocksdb_cq_double_write_enable: bool,

    #[serde(default)]
    pub read_uncommitted: bool,

    #[serde(default)]
    pub enable_controller_mode: bool,
}

impl Default for MessageStoreConfig {
    fn default() -> Self {
        let store_path_root_dir = USER_HOME.clone().join("store").to_string_lossy().to_string();
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
            timer_wheel_enable: true,
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
            file_reserved_time: 72,
            delete_file_batch_max: 10,
            put_msg_index_hight_water: 600000,
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
            ha_listen_port: 10912,
            ha_send_heartbeat_interval: 1000 * 5,
            ha_housekeeping_interval: 1000 * 20,
            ha_transfer_batch_size: 0,
            ha_master_address: None,
            ha_max_gap_not_in_sync: 0,
            broker_role: Default::default(),
            flush_disk_type: FlushDiskType::SyncFlush,
            sync_flush_timeout: 1000 * 5,
            put_message_timeout: 0,
            slave_timeout: 0,
            message_delay_level: "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h".to_string(),
            flush_delay_offset_interval: 10_000,
            clean_file_forcibly_enable: false,
            warm_mapped_file_enable: false,
            offset_check_in_slave: false,
            debug_lock_enable: false,
            duplication_enable: false,
            disk_fall_recorded: false,
            os_page_cache_busy_timeout_mills: 1000,
            default_query_max_num: 32,
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
            max_ha_transfer_byte_in_second: 100 * 1024 * 1024,
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
            enable_dleger_commit_log: false,
            rocksdb_cq_double_write_enable: false,
            read_uncommitted: false,
            enable_controller_mode: false,
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
        let factor = (self.mapped_file_size_consume_queue as f64 / (CQ_STORE_UNIT_SIZE as f64)).ceil() as i32;
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
            self.store_path_commit_log.clone().unwrap_or_default().to_string(),
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
            self.store_path_epoch_file.clone().unwrap_or_default().to_string(),
        );
        properties.insert(
            "storePathBrokerIdentity".to_string(),
            self.store_path_broker_identity.clone().unwrap_or_default().to_string(),
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
        properties.insert("maxOffsetMapSize".to_string(), self.max_offset_map_size.to_string());
        properties.insert(
            "compactionThreadNum".to_string(),
            self.compaction_thread_num.to_string(),
        );
        properties.insert("enableCompaction".to_string(), self.enable_compaction.to_string());
        properties.insert(
            "mappedFileSizeTimerLog".to_string(),
            self.mapped_file_size_timer_log.to_string(),
        );
        properties.insert("timerPrecisionMs".to_string(), self.timer_precision_ms.to_string());
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
        properties.insert("timerMaxDelaySec".to_string(), self.timer_max_delay_sec.to_string());
        properties.insert("timerWheelEnable".to_string(), self.timer_wheel_enable.to_string());
        properties.insert(
            "disappearTimeAfterStart".to_string(),
            self.disappear_time_after_start.to_string(),
        );
        properties.insert("timerStopEnqueue".to_string(), self.timer_stop_enqueue.to_string());
        properties.insert(
            "timerCheckMetricsWhen".to_string(),
            self.timer_check_metrics_when.clone(),
        );
        properties.insert(
            "timerSkipUnknownError".to_string(),
            self.timer_skip_unknown_error.to_string(),
        );
        properties.insert("timerWarmEnable".to_string(), self.timer_warm_enable.to_string());
        properties.insert("timerStopDequeue".to_string(), self.timer_stop_dequeue.to_string());
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
        properties.insert("storeType".to_string(), self.store_type.get_store_type().to_string());
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
        properties.insert("fileReservedTime".to_string(), self.file_reserved_time.to_string());
        properties.insert("deleteFileBatchMax".to_string(), self.delete_file_batch_max.to_string());
        properties.insert(
            "putMsgIndexHightWater".to_string(),
            self.put_msg_index_hight_water.to_string(),
        );
        properties.insert("maxMessageSize".to_string(), self.max_message_size.to_string());
        properties.insert("checkCrcOnRecover".to_string(), self.check_crc_on_recover.to_string());
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
        properties.insert("messageIndexEnable".to_string(), self.message_index_enable.to_string());
        properties.insert("maxHashSlotNum".to_string(), self.max_hash_slot_num.to_string());
        properties.insert("maxIndexNum".to_string(), self.max_index_num.to_string());
        properties.insert("maxMsgsNumBatch".to_string(), self.max_msgs_num_batch.to_string());
        properties.insert("messageIndexSafe".to_string(), self.message_index_safe.to_string());
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
        properties.insert("haMaxGapNotInSync".to_string(), self.ha_max_gap_not_in_sync.to_string());
        properties.insert("brokerRole".to_string(), self.broker_role.get_broker_role().to_string());
        properties.insert(
            "flushDiskType".to_string(),
            self.flush_disk_type.get_flush_disk_type().to_string(),
        );
        properties.insert("syncFlushTimeout".to_string(), self.sync_flush_timeout.to_string());
        properties.insert("putMessageTimeout".to_string(), self.put_message_timeout.to_string());
        properties.insert("slaveTimeout".to_string(), self.slave_timeout.to_string());
        properties.insert("messageDelayLevel".to_string(), self.message_delay_level.clone());
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
        properties.insert("offsetCheckInSlave".to_string(), self.offset_check_in_slave.to_string());
        properties.insert("debugLockEnable".to_string(), self.debug_lock_enable.to_string());
        properties.insert("duplicationEnable".to_string(), self.duplication_enable.to_string());
        properties.insert("diskFallRecorded".to_string(), self.disk_fall_recorded.to_string());
        properties.insert(
            "osPageCacheBusyTimeoutMills".to_string(),
            self.os_page_cache_busy_timeout_mills.to_string(),
        );
        properties.insert("defaultQueryMaxNum".to_string(), self.default_query_max_num.to_string());
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
        properties.insert("enableBatchPush".to_string(), self.enable_batch_push.to_string());
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
            self.schedule_async_deliver_max_resend_num2_blocked.to_string(),
        );
        properties.insert(
            "maxBatchDeleteFilesNum".to_string(),
            self.max_batch_delete_files_num.to_string(),
        );
        properties.insert("dispatchCqThreads".to_string(), self.dispatch_cq_threads.to_string());
        properties.insert("dispatchCqCacheNum".to_string(), self.dispatch_cq_cache_num.to_string());
        properties.insert("enableAsyncReput".to_string(), self.enable_async_reput.to_string());
        properties.insert(
            "recheckReputOffsetFromCq".to_string(),
            self.recheck_reput_offset_from_cq.to_string(),
        );
        properties.insert("maxTopicLength".to_string(), self.max_topic_length.to_string());
        properties.insert(
            "autoMessageVersionOnTopicLen".to_string(),
            self.auto_message_version_on_topic_len.to_string(),
        );
        properties.insert(
            "enabledAppendPropCrc".to_string(),
            self.enabled_append_prop_crc.to_string(),
        );
        properties.insert("forceVerifyPropCrc".to_string(), self.force_verify_prop_crc.to_string());
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
        properties.insert("inSyncReplicas".to_string(), self.in_sync_replicas.to_string());
        properties.insert("minInSyncReplicas".to_string(), self.min_in_sync_replicas.to_string());
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
        properties.insert("maxChecksumRange".to_string(), self.max_checksum_range.to_string());
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
        properties.insert("syncFromLastFile".to_string(), self.sync_from_last_file.to_string());
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
        properties.insert("coldDataScanEnable".to_string(), self.cold_data_scan_enable.to_string());
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
        properties.insert("enableRocksdbLog".into(), self.enable_rocksdb_log.to_string());
        properties.insert("topicQueueLockNum".into(), self.topic_queue_lock_num.to_string());
        properties.insert("maxFilterMessageSize".into(), self.max_filter_message_size.to_string());
        properties
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect::<HashMap<CheetahString, CheetahString>>()
    }
}
