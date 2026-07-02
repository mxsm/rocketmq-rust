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
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::time::Duration;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

use crate::common::broker::broker_role::BrokerRole;
use crate::common::constant::PermName;
use crate::common::message::message_enum::MessageRequestMode;
use crate::common::metrics::LogExporterType;
use crate::common::metrics::MetricsExporterType;
use crate::common::metrics::TraceExporterType;
use crate::common::mix_all;
use crate::common::mix_all::NAMESRV_ADDR_PROPERTY;
use crate::common::server::config::ServerConfig;
use crate::common::topic::TopicValidator;

const DEFAULT_CLUSTER_NAME: &str = "DefaultCluster";

pub static LOCAL_HOST_NAME: std::sync::LazyLock<Option<String>> = std::sync::LazyLock::new(|| match hostname::get() {
    Ok(hostname) => Some(hostname.to_string_lossy().to_string()),
    Err(_) => None,
});

pub static NAMESRV_ADDR: std::sync::LazyLock<Option<String>> =
    std::sync::LazyLock::new(|| std::env::var(NAMESRV_ADDR_PROPERTY).map_or(Some("127.0.0.1:9876".to_string()), Some));

/// Default value functions for Serde deserialization
mod defaults {
    use super::*;

    // BrokerIdentity defaults
    pub fn broker_name() -> CheetahString {
        default_broker_name().into()
    }

    pub fn broker_cluster_name() -> CheetahString {
        DEFAULT_CLUSTER_NAME.to_string().into()
    }

    pub fn broker_id() -> u64 {
        mix_all::MASTER_ID
    }

    // BrokerConfig defaults
    pub fn broker_identity() -> BrokerIdentity {
        BrokerIdentity::new()
    }

    pub fn topic_queue_config() -> TopicQueueConfig {
        TopicQueueConfig::default()
    }

    pub fn timer_wheel_config() -> TimerWheelConfig {
        TimerWheelConfig::default()
    }

    pub fn broker_server_config() -> ServerConfig {
        ServerConfig::default()
    }

    pub fn broker_ip1() -> CheetahString {
        match local_ip_address::local_ip() {
            Ok(local_ip) => local_ip.to_string().into(),
            Err(_) => "127.0.0.1".to_string().into(),
        }
    }

    pub fn broker_ip2() -> Option<CheetahString> {
        match local_ip_address::local_ip() {
            Ok(local_ip) => Some(local_ip.to_string().into()),
            Err(_) => None,
        }
    }

    pub fn listen_port() -> u32 {
        10911
    }

    pub fn msg_trace_topic_name() -> CheetahString {
        CheetahString::from_static_str(TopicValidator::RMQ_SYS_TRACE_TOPIC)
    }

    pub fn region_id() -> CheetahString {
        CheetahString::from_static_str(mix_all::DEFAULT_TRACE_REGION_ID)
    }

    pub fn observability_environment() -> CheetahString {
        "dev".into()
    }

    pub fn trace_on() -> bool {
        true
    }

    pub fn metrics_export_interval_millis() -> u64 {
        30_000
    }

    pub fn metrics_cardinality_limit() -> usize {
        10_000
    }

    pub fn metrics_label_enabled() -> bool {
        true
    }

    pub fn otlp_exporter_endpoint() -> CheetahString {
        "http://127.0.0.1:4317".into()
    }

    pub fn otlp_exporter_timeout_millis() -> u64 {
        3_000
    }

    pub fn metrics_prom_exporter_host() -> CheetahString {
        "127.0.0.1".into()
    }

    pub fn metrics_prom_exporter_port() -> u16 {
        5557
    }

    pub fn metrics_prom_exporter_path() -> CheetahString {
        "/metrics".into()
    }

    pub fn trace_sample_ratio() -> f64 {
        0.01
    }

    pub fn trace_propagate_context() -> bool {
        true
    }

    pub fn trace_record_body_size() -> bool {
        true
    }

    pub fn broker_permission() -> u32 {
        PermName::PERM_WRITE | PermName::PERM_READ
    }

    pub fn store_path_root_dir() -> CheetahString {
        dirs::home_dir()
            .unwrap_or_default()
            .join("store")
            .to_string_lossy()
            .into_owned()
            .into()
    }

    pub fn use_single_rocksdb_for_all_configs() -> bool {
        false
    }

    pub fn split_registration_size() -> i32 {
        800
    }

    pub fn register_broker_timeout_mills() -> i32 {
        24000
    }

    pub fn commercial_size_per_msg() -> i32 {
        4 * 1024
    }

    pub fn auto_create_topic_enable() -> bool {
        true
    }

    pub fn enable_single_topic_register() -> bool {
        true
    }

    pub fn broker_topic_enable() -> bool {
        true
    }

    pub fn cluster_topic_enable() -> bool {
        true
    }

    pub fn revive_queue_num() -> u32 {
        8
    }

    pub fn enable_detail_stat() -> bool {
        true
    }

    pub fn flush_consumer_offset_interval() -> u64 {
        1000 * 5
    }

    pub fn force_register() -> bool {
        true
    }

    pub fn enable_register_producer() -> bool {
        true
    }

    pub fn register_name_server_period() -> u64 {
        1000 * 30
    }

    pub fn namesrv_addr() -> Option<CheetahString> {
        NAMESRV_ADDR.clone().map(|addr| addr.into())
    }

    pub fn lite_pull_message_enable() -> bool {
        true
    }

    pub fn enable_lite_event_mode() -> bool {
        true
    }

    pub fn lite_event_check_interval() -> u64 {
        10_000
    }

    pub fn lite_ttl_check_interval() -> u64 {
        120_000
    }

    pub fn lite_subscription_check_interval() -> u64 {
        120_000
    }

    pub fn lite_subscription_check_timeout_mills() -> u64 {
        180_000
    }

    pub fn max_lite_subscription_count() -> u64 {
        100_000
    }

    pub fn enable_lite_pop_log() -> bool {
        false
    }

    pub fn max_client_event_count() -> i32 {
        100
    }

    pub fn lite_event_full_dispatch_delay_time() -> u64 {
        10_000
    }

    pub fn lite_lag_latency_collect_enable() -> bool {
        false
    }

    pub fn lite_lag_latency_metrics_enable() -> bool {
        false
    }

    pub fn lite_lag_count_metrics_enable() -> bool {
        false
    }

    pub fn lite_lag_latency_top_k() -> i32 {
        50
    }

    pub fn auto_create_subscription_group() -> bool {
        true
    }

    pub fn channel_expired_timeout() -> u64 {
        1000 * 120
    }

    pub fn subscription_expired_timeout() -> u64 {
        1000 * 60 * 10
    }

    pub fn use_server_side_reset_offset() -> bool {
        true
    }

    pub fn consumer_offset_update_version_step() -> i64 {
        500
    }

    pub fn enable_broadcast_offset_store() -> bool {
        true
    }

    pub fn transfer_msg_by_heap() -> bool {
        true
    }

    pub fn short_polling_time_mills() -> u64 {
        1000
    }

    pub fn long_polling_enable() -> bool {
        true
    }

    pub fn max_error_rate_of_bloom_filter() -> i32 {
        20
    }

    pub fn expect_consumer_num_use_filter() -> i32 {
        32
    }

    pub fn bit_map_length_consume_queue_ext() -> i32 {
        64
    }

    pub fn forward_timeout() -> u64 {
        3 * 1000
    }

    pub fn validate_system_topic_when_update_topic() -> bool {
        true
    }

    pub fn store_reply_message_enable() -> bool {
        true
    }

    pub fn recall_message_enable() -> bool {
        true
    }

    pub fn allow_recall_when_broker_not_writeable() -> bool {
        false
    }

    pub fn transaction_timeout() -> u64 {
        6_000
    }

    pub fn transaction_op_msg_max_size() -> i32 {
        4096
    }

    pub fn default_message_request_mode() -> MessageRequestMode {
        MessageRequestMode::Pull
    }

    pub fn default_pop_share_queue_num() -> i32 {
        -1
    }

    pub fn load_balance_poll_name_server_interval() -> u64 {
        30_000
    }

    pub fn server_load_balancer_enable() -> bool {
        true
    }

    pub fn retrieve_message_from_pop_retry_topic_v1() -> bool {
        true
    }

    pub fn pop_from_retry_probability() -> i32 {
        20
    }

    pub fn pop_consumer_fs_service_init() -> bool {
        true
    }

    pub fn pop_consumer_kv_service_log() -> bool {
        false
    }

    pub fn pop_consumer_kv_service_init() -> bool {
        false
    }

    pub fn pop_consumer_kv_service_enable() -> bool {
        false
    }

    pub fn init_pop_offset_by_check_msg_in_mem() -> bool {
        true
    }

    pub fn pop_ck_stay_buffer_time_out() -> u64 {
        3_000
    }

    pub fn pop_ck_stay_buffer_time() -> u64 {
        10_000
    }

    pub fn broker_role() -> BrokerRole {
        BrokerRole::AsyncMaster
    }

    pub fn revive_interval() -> u64 {
        1000
    }

    pub fn revive_max_slow() -> u64 {
        3
    }

    pub fn revive_scan_time() -> u64 {
        10_000
    }

    pub fn commercial_base_count() -> i32 {
        1
    }

    pub fn broker_not_active_timeout_millis() -> i64 {
        10_000
    }

    pub fn sync_broker_member_group_period() -> u64 {
        1_000
    }

    pub fn pop_polling_map_size() -> usize {
        100000
    }

    pub fn max_pop_polling_size() -> u64 {
        100000
    }

    pub fn pop_polling_size() -> usize {
        1024
    }

    pub fn pop_inflight_message_threshold() -> i64 {
        10_000
    }

    pub fn pop_ck_max_buffer_size() -> i64 {
        200_000
    }

    pub fn pop_ck_offset_max_queue_size() -> u64 {
        20_000
    }

    pub fn delay_offset_update_version_step() -> u64 {
        200
    }

    pub fn revive_ack_wait_ms() -> u64 {
        Duration::from_secs(3 * 60).as_millis() as u64
    }

    pub fn os_page_cache_busy_timeout_mills() -> u64 {
        1000
    }

    pub fn default_topic_queue_nums() -> u32 {
        8
    }

    pub fn auth_config_path() -> CheetahString {
        dirs::home_dir()
            .unwrap_or_default()
            .join("config")
            .join("auth")
            .to_string_lossy()
            .into_owned()
            .into()
    }

    pub fn acl_file() -> CheetahString {
        CheetahString::new()
    }

    pub fn acl_file_watch_enabled() -> bool {
        false
    }

    pub fn acl_file_watch_interval_millis() -> u64 {
        5_000
    }

    pub fn authentication_enabled() -> bool {
        false
    }

    pub fn authentication_provider() -> CheetahString {
        CheetahString::new()
    }

    pub fn authentication_metadata_provider() -> CheetahString {
        CheetahString::new()
    }

    pub fn authentication_strategy() -> CheetahString {
        CheetahString::new()
    }

    pub fn authorization_enabled() -> bool {
        false
    }

    pub fn authorization_provider() -> CheetahString {
        CheetahString::new()
    }

    pub fn authorization_metadata_provider() -> CheetahString {
        CheetahString::new()
    }

    pub fn authorization_strategy() -> CheetahString {
        CheetahString::new()
    }

    pub fn authentication_whitelist() -> CheetahString {
        CheetahString::new()
    }

    pub fn authorization_whitelist() -> CheetahString {
        CheetahString::new()
    }

    pub fn init_authentication_user() -> CheetahString {
        CheetahString::new()
    }

    pub fn inner_client_authentication_credentials() -> CheetahString {
        CheetahString::new()
    }

    pub fn signature_algorithm() -> CheetahString {
        CheetahString::from_static_str("HmacSHA1")
    }

    pub fn request_timestamp_expired_millis() -> u64 {
        0
    }

    pub fn migrate_auth_from_v1_enabled() -> bool {
        false
    }

    pub fn user_cache_max_num() -> u32 {
        1000
    }

    pub fn user_cache_expired_second() -> u32 {
        600
    }

    pub fn user_cache_refresh_second() -> u32 {
        60
    }

    pub fn acl_cache_max_num() -> u32 {
        1000
    }

    pub fn acl_cache_expired_second() -> u32 {
        600
    }

    pub fn acl_cache_refresh_second() -> u32 {
        60
    }

    pub fn stateful_authentication_cache_max_num() -> u32 {
        10000
    }

    pub fn stateful_authentication_cache_expired_second() -> u32 {
        60
    }

    pub fn stateful_authorization_cache_max_num() -> u32 {
        10000
    }

    pub fn stateful_authorization_cache_expired_second() -> u32 {
        60
    }

    pub fn transaction_check_interval() -> u64 {
        30_000
    }

    pub fn transaction_check_max() -> u32 {
        15
    }

    pub fn transaction_op_batch_interval() -> u64 {
        3000
    }

    pub fn compatible_with_old_name_srv() -> bool {
        true
    }

    #[inline]
    pub fn broker_heartbeat_interval() -> u64 {
        1000
    }

    pub fn controller_addr() -> CheetahString {
        CheetahString::new()
    }

    pub const fn send_heartbeat_timeout_millis() -> u64 {
        1000
    }

    pub const fn broker_fast_failure_enable() -> bool {
        true
    }

    pub const fn send_request_executor_detached_enable() -> bool {
        false
    }

    pub const fn wait_time_mills_in_send_queue() -> u64 {
        200
    }

    pub const fn sync_flush_backlog_reject_depth() -> u64 {
        0
    }

    pub const fn sync_flush_backlog_reject_wait_millis() -> u64 {
        0
    }

    pub const fn ha_pending_reject_count() -> u64 {
        0
    }

    pub const fn ha_pending_reject_wait_millis() -> u64 {
        0
    }

    pub const fn reput_lag_reject_bytes() -> i64 {
        0
    }

    pub const fn wait_time_mills_in_pull_queue() -> u64 {
        5_000
    }

    pub const fn wait_time_mills_in_lite_pull_queue() -> u64 {
        5_000
    }

    pub const fn wait_time_mills_in_heartbeat_queue() -> u64 {
        31_000
    }

    pub const fn wait_time_mills_in_transaction_queue() -> u64 {
        3_000
    }

    pub const fn wait_time_mills_in_ack_queue() -> u64 {
        3_000
    }

    pub const fn wait_time_mills_in_admin_broker_queue() -> u64 {
        5_000
    }

    pub const fn controller_heartbeat_timeout_mills() -> i64 {
        10 * 1000
    }

    pub const fn sync_broker_metadata_period() -> u64 {
        5 * 1000
    }

    pub const fn sync_controller_metadata_period() -> u64 {
        10 * 1000
    }

    pub const fn broker_election_priority() -> i32 {
        i32::MAX
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BrokerIdentity {
    #[serde(default = "defaults::broker_name")]
    pub broker_name: CheetahString,

    #[serde(default = "defaults::broker_cluster_name")]
    pub broker_cluster_name: CheetahString,

    #[serde(default = "defaults::broker_id")]
    pub broker_id: u64,

    #[serde(default)]
    pub is_broker_container: bool,

    #[serde(default)]
    pub is_in_broker_container: bool,
}

impl Default for BrokerIdentity {
    fn default() -> Self {
        BrokerIdentity::new()
    }
}

impl BrokerIdentity {
    pub fn new() -> Self {
        let broker_name = default_broker_name();
        let broker_cluster_name = String::from(DEFAULT_CLUSTER_NAME);
        let broker_id = mix_all::MASTER_ID;
        let is_broker_container = false;

        BrokerIdentity {
            broker_name: CheetahString::from_string(broker_name),
            broker_cluster_name: CheetahString::from_string(broker_cluster_name),
            broker_id,
            is_broker_container,
            is_in_broker_container: false,
        }
    }

    fn new_with_container(is_broker_container: bool) -> Self {
        let mut identity = BrokerIdentity::new();
        identity.is_broker_container = is_broker_container;
        identity
    }

    fn new_with_params(broker_cluster_name: String, broker_name: String, broker_id: u64) -> Self {
        BrokerIdentity {
            broker_name: CheetahString::from_string(broker_name),
            broker_cluster_name: CheetahString::from_string(broker_cluster_name),
            broker_id,
            is_broker_container: false,
            is_in_broker_container: false,
        }
    }

    fn new_with_container_params(
        broker_cluster_name: String,
        broker_name: String,
        broker_id: u64,
        is_in_broker_container: bool,
    ) -> Self {
        BrokerIdentity {
            broker_name: CheetahString::from_string(broker_name),
            broker_cluster_name: CheetahString::from_string(broker_cluster_name),
            broker_id,
            is_broker_container: true,
            is_in_broker_container,
        }
    }

    pub fn get_canonical_name(&self) -> String {
        match self.is_broker_container {
            true => "BrokerContainer".to_string(),
            false => {
                format!("{}_{}_{}", self.broker_cluster_name, self.broker_name, self.broker_id)
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BrokerConfig {
    #[serde(default = "defaults::broker_identity")]
    pub broker_identity: BrokerIdentity,

    #[serde(default = "defaults::topic_queue_config")]
    pub topic_queue_config: TopicQueueConfig,

    #[serde(default = "defaults::timer_wheel_config")]
    pub timer_wheel_config: TimerWheelConfig,

    #[serde(default = "defaults::broker_server_config")]
    pub broker_server_config: ServerConfig,

    #[serde(default = "defaults::broker_ip1")]
    pub broker_ip1: CheetahString,

    #[serde(default = "defaults::broker_ip2")]
    pub broker_ip2: Option<CheetahString>,

    #[serde(default = "defaults::listen_port")]
    pub listen_port: u32,

    #[serde(default)]
    pub trace_topic_enable: bool,

    #[serde(default = "defaults::msg_trace_topic_name")]
    pub msg_trace_topic_name: CheetahString,

    #[serde(default)]
    pub enable_controller_mode: bool,

    #[serde(default = "defaults::controller_addr")]
    pub controller_addr: CheetahString,

    #[serde(default = "defaults::sync_broker_metadata_period")]
    pub sync_broker_metadata_period: u64,

    #[serde(default = "defaults::sync_controller_metadata_period")]
    pub sync_controller_metadata_period: u64,

    #[serde(default = "defaults::region_id")]
    pub region_id: CheetahString,

    #[serde(default = "defaults::observability_environment")]
    pub observability_environment: CheetahString,

    #[serde(default)]
    pub observability_service_instance_id: CheetahString,

    #[serde(default)]
    pub observability_resource_attributes: CheetahString,

    #[serde(default = "defaults::trace_on")]
    pub trace_on: bool,

    #[serde(default)]
    pub metrics_exporter_type: MetricsExporterType,

    #[serde(default = "defaults::metrics_export_interval_millis")]
    pub metrics_export_interval_millis: u64,

    #[serde(default = "defaults::metrics_cardinality_limit")]
    pub metrics_cardinality_limit: usize,

    #[serde(default = "defaults::metrics_label_enabled")]
    pub metrics_topic_label_enabled: bool,

    #[serde(default = "defaults::metrics_label_enabled")]
    pub metrics_consumer_group_label_enabled: bool,

    #[serde(default = "defaults::otlp_exporter_endpoint")]
    pub otlp_exporter_endpoint: CheetahString,

    #[serde(default)]
    pub otlp_exporter_headers: CheetahString,

    #[serde(default = "defaults::otlp_exporter_timeout_millis")]
    pub otlp_exporter_timeout_millis: u64,

    #[serde(default = "defaults::metrics_prom_exporter_host")]
    pub metrics_prom_exporter_host: CheetahString,

    #[serde(default = "defaults::metrics_prom_exporter_port")]
    pub metrics_prom_exporter_port: u16,

    #[serde(default = "defaults::metrics_prom_exporter_path")]
    pub metrics_prom_exporter_path: CheetahString,

    #[serde(default)]
    pub trace_exporter_type: TraceExporterType,

    #[serde(default = "defaults::trace_sample_ratio")]
    pub trace_sample_ratio: f64,

    #[serde(default = "defaults::trace_propagate_context")]
    pub trace_propagate_context: bool,

    #[serde(default)]
    pub trace_record_message_id: bool,

    #[serde(default)]
    pub trace_record_message_keys: bool,

    #[serde(default = "defaults::trace_record_body_size")]
    pub trace_record_body_size: bool,

    #[serde(default)]
    pub log_exporter_type: LogExporterType,

    #[serde(default = "defaults::broker_permission")]
    pub broker_permission: u32,

    #[serde(default)]
    pub async_send_enable: bool, //not used in rust version,only for Java compatibility

    #[serde(default = "defaults::store_path_root_dir")]
    pub store_path_root_dir: CheetahString,

    #[serde(default = "defaults::use_single_rocksdb_for_all_configs")]
    pub use_single_rocksdb_for_all_configs: bool,

    #[serde(default)]
    pub enable_split_registration: bool,

    #[serde(default = "defaults::split_registration_size")]
    pub split_registration_size: i32,

    #[serde(default = "defaults::register_broker_timeout_mills")]
    pub register_broker_timeout_mills: i32,

    #[serde(default)]
    pub is_in_broker_container: bool,

    #[serde(default = "defaults::commercial_size_per_msg")]
    pub commercial_size_per_msg: i32,

    #[serde(default)]
    pub recover_concurrently: bool,

    #[serde(default)]
    pub duplication_enable: bool,

    #[serde(default)]
    pub start_accept_send_request_time_stamp: i64,

    #[serde(default = "defaults::auto_create_topic_enable")]
    pub auto_create_topic_enable: bool,

    #[serde(default = "defaults::enable_single_topic_register")]
    pub enable_single_topic_register: bool,

    #[serde(default = "defaults::broker_topic_enable")]
    pub broker_topic_enable: bool,

    #[serde(default = "defaults::cluster_topic_enable")]
    pub cluster_topic_enable: bool,

    #[serde(default = "defaults::revive_queue_num")]
    pub revive_queue_num: u32,

    #[serde(default)]
    pub enable_slave_acting_master: bool,

    /// Enable producer registration. When false with reject_transaction_message=true,
    /// only existing producers can send heartbeats, new producers cannot register.
    #[serde(default = "defaults::enable_register_producer")]
    pub enable_register_producer: bool,

    #[serde(default)]
    pub reject_transaction_message: bool,

    #[serde(default = "defaults::enable_detail_stat")]
    pub enable_detail_stat: bool,

    #[serde(default = "defaults::flush_consumer_offset_interval")]
    pub flush_consumer_offset_interval: u64,

    #[serde(default = "defaults::force_register")]
    pub force_register: bool,

    #[serde(default = "defaults::register_name_server_period")]
    pub register_name_server_period: u64,

    #[serde(default = "defaults::send_heartbeat_timeout_millis")]
    pub send_heartbeat_timeout_millis: u64,

    #[serde(default = "defaults::broker_fast_failure_enable")]
    pub broker_fast_failure_enable: bool,

    #[serde(default = "defaults::send_request_executor_detached_enable")]
    pub send_request_executor_detached_enable: bool,

    #[serde(default = "defaults::wait_time_mills_in_send_queue")]
    pub wait_time_mills_in_send_queue: u64,

    #[serde(default = "defaults::sync_flush_backlog_reject_depth")]
    pub sync_flush_backlog_reject_depth: u64,

    #[serde(default = "defaults::sync_flush_backlog_reject_wait_millis")]
    pub sync_flush_backlog_reject_wait_millis: u64,

    #[serde(default = "defaults::ha_pending_reject_count")]
    pub ha_pending_reject_count: u64,

    #[serde(default = "defaults::ha_pending_reject_wait_millis")]
    pub ha_pending_reject_wait_millis: u64,

    #[serde(default = "defaults::reput_lag_reject_bytes")]
    pub reput_lag_reject_bytes: i64,

    #[serde(default = "defaults::wait_time_mills_in_pull_queue")]
    pub wait_time_mills_in_pull_queue: u64,

    #[serde(default = "defaults::wait_time_mills_in_lite_pull_queue")]
    pub wait_time_mills_in_lite_pull_queue: u64,

    #[serde(default = "defaults::wait_time_mills_in_heartbeat_queue")]
    pub wait_time_mills_in_heartbeat_queue: u64,

    #[serde(default = "defaults::wait_time_mills_in_transaction_queue")]
    pub wait_time_mills_in_transaction_queue: u64,

    #[serde(default = "defaults::wait_time_mills_in_ack_queue")]
    pub wait_time_mills_in_ack_queue: u64,

    #[serde(default = "defaults::wait_time_mills_in_admin_broker_queue")]
    pub wait_time_mills_in_admin_broker_queue: u64,

    #[serde(default)]
    pub skip_pre_online: bool,

    #[serde(default = "defaults::namesrv_addr")]
    pub namesrv_addr: Option<CheetahString>,

    #[serde(default)]
    pub fetch_name_srv_addr_by_dns_lookup: bool,

    #[serde(default = "defaults::lite_pull_message_enable")]
    pub lite_pull_message_enable: bool,

    #[serde(default = "defaults::enable_lite_event_mode")]
    pub enable_lite_event_mode: bool,

    #[serde(default = "defaults::lite_event_check_interval")]
    pub lite_event_check_interval: u64,

    #[serde(default = "defaults::lite_ttl_check_interval")]
    pub lite_ttl_check_interval: u64,

    #[serde(default = "defaults::lite_subscription_check_interval")]
    pub lite_subscription_check_interval: u64,

    #[serde(default = "defaults::lite_subscription_check_timeout_mills")]
    pub lite_subscription_check_timeout_mills: u64,

    #[serde(default = "defaults::max_lite_subscription_count")]
    pub max_lite_subscription_count: u64,

    #[serde(default = "defaults::enable_lite_pop_log")]
    pub enable_lite_pop_log: bool,

    #[serde(default = "defaults::max_client_event_count")]
    pub max_client_event_count: i32,

    #[serde(default = "defaults::lite_event_full_dispatch_delay_time")]
    pub lite_event_full_dispatch_delay_time: u64,

    #[serde(default = "defaults::lite_lag_latency_collect_enable")]
    pub lite_lag_latency_collect_enable: bool,

    #[serde(default = "defaults::lite_lag_latency_metrics_enable")]
    pub lite_lag_latency_metrics_enable: bool,

    #[serde(default = "defaults::lite_lag_count_metrics_enable")]
    pub lite_lag_count_metrics_enable: bool,

    #[serde(default = "defaults::lite_lag_latency_top_k")]
    pub lite_lag_latency_top_k: i32,

    #[serde(default = "defaults::auto_create_subscription_group")]
    pub auto_create_subscription_group: bool,

    #[serde(default = "defaults::channel_expired_timeout")]
    pub channel_expired_timeout: u64,

    #[serde(default = "defaults::subscription_expired_timeout")]
    pub subscription_expired_timeout: u64,

    #[serde(default)]
    pub enable_property_filter: bool,

    #[serde(default)]
    pub filter_support_retry: bool,

    #[serde(default = "defaults::use_server_side_reset_offset")]
    pub use_server_side_reset_offset: bool,

    #[serde(default)]
    pub slave_read_enable: bool,

    #[serde(default = "defaults::commercial_base_count")]
    pub commercial_base_count: i32,

    #[serde(default)]
    pub reject_pull_consumer_enable: bool,

    #[serde(default = "defaults::consumer_offset_update_version_step")]
    pub consumer_offset_update_version_step: i64,

    #[serde(default = "defaults::enable_broadcast_offset_store")]
    pub enable_broadcast_offset_store: bool,

    #[serde(default = "defaults::transfer_msg_by_heap")]
    pub transfer_msg_by_heap: bool,

    #[serde(default = "defaults::short_polling_time_mills")]
    pub short_polling_time_mills: u64,

    #[serde(default = "defaults::long_polling_enable")]
    pub long_polling_enable: bool,

    #[serde(default = "defaults::max_error_rate_of_bloom_filter")]
    pub max_error_rate_of_bloom_filter: i32,

    #[serde(default = "defaults::expect_consumer_num_use_filter")]
    pub expect_consumer_num_use_filter: i32,

    #[serde(default = "defaults::bit_map_length_consume_queue_ext")]
    pub bit_map_length_consume_queue_ext: i32,

    #[serde(default = "defaults::validate_system_topic_when_update_topic")]
    pub validate_system_topic_when_update_topic: bool,

    #[serde(default)]
    pub enable_mixed_message_type: bool,

    #[serde(default)]
    pub auto_delete_unused_stats: bool,

    #[serde(default = "defaults::forward_timeout")]
    pub forward_timeout: u64,

    #[serde(default = "defaults::store_reply_message_enable")]
    pub store_reply_message_enable: bool,

    #[serde(default = "defaults::recall_message_enable")]
    pub recall_message_enable: bool,

    #[serde(default = "defaults::allow_recall_when_broker_not_writeable")]
    pub allow_recall_when_broker_not_writeable: bool,

    #[serde(default)]
    pub lock_in_strict_mode: bool,

    #[serde(default = "defaults::transaction_timeout")]
    pub transaction_timeout: u64,

    #[serde(default = "defaults::transaction_op_msg_max_size")]
    pub transaction_op_msg_max_size: i32,

    #[serde(default = "defaults::default_message_request_mode")]
    pub default_message_request_mode: MessageRequestMode,

    #[serde(default = "defaults::default_pop_share_queue_num")]
    pub default_pop_share_queue_num: i32,

    #[serde(default = "defaults::load_balance_poll_name_server_interval")]
    pub load_balance_poll_name_server_interval: u64,

    #[serde(default = "defaults::server_load_balancer_enable")]
    pub server_load_balancer_enable: bool,

    #[serde(default)]
    pub enable_remote_escape: bool,

    #[serde(default)]
    pub enable_pop_log: bool,

    #[serde(default)]
    pub enable_retry_topic_v2: bool,

    #[serde(default = "defaults::retrieve_message_from_pop_retry_topic_v1")]
    pub retrieve_message_from_pop_retry_topic_v1: bool,

    #[serde(default = "defaults::pop_from_retry_probability")]
    pub pop_from_retry_probability: i32,

    #[serde(default = "defaults::pop_consumer_fs_service_init")]
    pub pop_consumer_fs_service_init: bool,

    #[serde(default = "defaults::pop_consumer_kv_service_log")]
    pub pop_consumer_kv_service_log: bool,

    #[serde(default = "defaults::pop_consumer_kv_service_init")]
    pub pop_consumer_kv_service_init: bool,

    #[serde(default = "defaults::pop_consumer_kv_service_enable")]
    pub pop_consumer_kv_service_enable: bool,

    #[serde(default)]
    pub pop_response_return_actual_retry_topic: bool,

    #[serde(default = "defaults::init_pop_offset_by_check_msg_in_mem")]
    pub init_pop_offset_by_check_msg_in_mem: bool,

    #[serde(default)]
    pub enable_pop_buffer_merge: bool,

    #[serde(default = "defaults::pop_ck_stay_buffer_time_out")]
    pub pop_ck_stay_buffer_time_out: u64,

    #[serde(default = "defaults::pop_ck_stay_buffer_time")]
    pub pop_ck_stay_buffer_time: u64,

    #[serde(default = "defaults::broker_role")]
    pub broker_role: BrokerRole,

    #[serde(default)]
    pub enable_pop_batch_ack: bool,

    #[serde(default = "defaults::revive_interval")]
    pub revive_interval: u64,

    #[serde(default = "defaults::revive_max_slow")]
    pub revive_max_slow: u64,

    #[serde(default = "defaults::revive_scan_time")]
    pub revive_scan_time: u64,

    #[serde(default)]
    pub enable_skip_long_awaiting_ack: bool,

    #[serde(default)]
    pub skip_when_ck_re_put_reach_max_times: bool,

    #[serde(default)]
    pub compressed_register: bool,

    #[serde(default = "defaults::broker_not_active_timeout_millis")]
    pub broker_not_active_timeout_millis: i64,

    #[serde(default = "defaults::sync_broker_member_group_period")]
    pub sync_broker_member_group_period: u64,

    #[serde(default = "defaults::pop_polling_map_size")]
    pub pop_polling_map_size: usize,

    #[serde(default = "defaults::max_pop_polling_size")]
    pub max_pop_polling_size: u64,

    #[serde(default = "defaults::pop_polling_size")]
    pub pop_polling_size: usize,

    #[serde(default)]
    pub enable_pop_message_threshold: bool,

    #[serde(default = "defaults::pop_inflight_message_threshold")]
    pub pop_inflight_message_threshold: i64,

    #[serde(default = "defaults::pop_ck_max_buffer_size")]
    pub pop_ck_max_buffer_size: i64,

    #[serde(default = "defaults::pop_ck_offset_max_queue_size")]
    pub pop_ck_offset_max_queue_size: u64,

    #[serde(default = "defaults::delay_offset_update_version_step")]
    pub delay_offset_update_version_step: u64,

    #[serde(default = "defaults::revive_ack_wait_ms")]
    pub revive_ack_wait_ms: u64,

    #[serde(default)]
    pub enable_calc_filter_bit_map: bool,

    #[serde(default = "defaults::transaction_check_interval")]
    pub transaction_check_interval: u64,

    #[serde(default = "defaults::transaction_check_max")]
    pub transaction_check_max: u32,

    #[serde(default = "defaults::transaction_op_batch_interval")]
    pub transaction_op_batch_interval: u64,

    #[serde(default = "defaults::compatible_with_old_name_srv")]
    pub compatible_with_old_name_srv: bool,

    #[serde(default = "defaults::broker_heartbeat_interval")]
    pub broker_heartbeat_interval: u64,

    #[serde(default = "defaults::controller_heartbeat_timeout_mills")]
    pub controller_heartbeat_timeout_mills: i64,

    #[serde(default = "defaults::broker_election_priority")]
    pub broker_election_priority: i32,

    /// Enable fast channel event processing by maintaining channel-to-group mapping
    ///
    /// When enabled, channel close events use O(1) lookup instead of O(n) traversal
    /// of all consumer groups, providing 50-100x performance improvement in high
    /// connection scenarios (1000+ consumer groups).
    #[serde(default)]
    pub enable_fast_channel_event_process: bool,

    #[serde(default = "defaults::auth_config_path")]
    pub auth_config_path: CheetahString,

    #[serde(default = "defaults::acl_file")]
    pub acl_file: CheetahString,

    #[serde(default = "defaults::acl_file_watch_enabled")]
    pub acl_file_watch_enabled: bool,

    #[serde(default = "defaults::acl_file_watch_interval_millis")]
    pub acl_file_watch_interval_millis: u64,

    #[serde(default = "defaults::authentication_enabled")]
    pub authentication_enabled: bool,

    #[serde(default = "defaults::authentication_provider")]
    pub authentication_provider: CheetahString,

    #[serde(default = "defaults::authentication_metadata_provider")]
    pub authentication_metadata_provider: CheetahString,

    #[serde(default = "defaults::authentication_strategy")]
    pub authentication_strategy: CheetahString,

    #[serde(default = "defaults::authorization_enabled")]
    pub authorization_enabled: bool,

    #[serde(default = "defaults::authorization_provider")]
    pub authorization_provider: CheetahString,

    #[serde(default = "defaults::authorization_metadata_provider")]
    pub authorization_metadata_provider: CheetahString,

    #[serde(default = "defaults::authorization_strategy")]
    pub authorization_strategy: CheetahString,

    #[serde(default = "defaults::authentication_whitelist")]
    pub authentication_whitelist: CheetahString,

    #[serde(default = "defaults::authorization_whitelist")]
    pub authorization_whitelist: CheetahString,

    #[serde(default = "defaults::init_authentication_user")]
    pub init_authentication_user: CheetahString,

    #[serde(default = "defaults::inner_client_authentication_credentials")]
    pub inner_client_authentication_credentials: CheetahString,

    #[serde(default = "defaults::signature_algorithm")]
    pub signature_algorithm: CheetahString,

    #[serde(default = "defaults::request_timestamp_expired_millis")]
    pub request_timestamp_expired_millis: u64,

    #[serde(default = "defaults::migrate_auth_from_v1_enabled")]
    pub migrate_auth_from_v1_enabled: bool,

    #[serde(default = "defaults::user_cache_max_num")]
    pub user_cache_max_num: u32,

    #[serde(default = "defaults::user_cache_expired_second")]
    pub user_cache_expired_second: u32,

    #[serde(default = "defaults::user_cache_refresh_second")]
    pub user_cache_refresh_second: u32,

    #[serde(default = "defaults::acl_cache_max_num")]
    pub acl_cache_max_num: u32,

    #[serde(default = "defaults::acl_cache_expired_second")]
    pub acl_cache_expired_second: u32,

    #[serde(default = "defaults::acl_cache_refresh_second")]
    pub acl_cache_refresh_second: u32,

    #[serde(default = "defaults::stateful_authentication_cache_max_num")]
    pub stateful_authentication_cache_max_num: u32,

    #[serde(default = "defaults::stateful_authentication_cache_expired_second")]
    pub stateful_authentication_cache_expired_second: u32,

    #[serde(default = "defaults::stateful_authorization_cache_max_num")]
    pub stateful_authorization_cache_max_num: u32,

    #[serde(default = "defaults::stateful_authorization_cache_expired_second")]
    pub stateful_authorization_cache_expired_second: u32,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        let broker_identity = BrokerIdentity::new();
        let local_ip = local_ip_address::local_ip().unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST));
        let broker_ip1 = local_ip.to_string().into();
        let broker_ip2 = Some(local_ip.to_string().into());
        let listen_port = 10911;

        BrokerConfig {
            broker_identity,
            topic_queue_config: TopicQueueConfig::default(),
            timer_wheel_config: TimerWheelConfig::default(),
            broker_server_config: Default::default(),
            broker_ip1,
            broker_ip2,
            listen_port,
            trace_topic_enable: false,
            msg_trace_topic_name: CheetahString::from_static_str(TopicValidator::RMQ_SYS_TRACE_TOPIC),
            enable_controller_mode: false,
            controller_addr: CheetahString::new(),
            sync_broker_metadata_period: 5_000,
            sync_controller_metadata_period: 10_000,
            region_id: CheetahString::from_static_str(mix_all::DEFAULT_TRACE_REGION_ID),
            observability_environment: defaults::observability_environment(),
            observability_service_instance_id: CheetahString::new(),
            observability_resource_attributes: CheetahString::new(),
            trace_on: true,
            metrics_exporter_type: MetricsExporterType::Disable,
            metrics_export_interval_millis: defaults::metrics_export_interval_millis(),
            metrics_cardinality_limit: defaults::metrics_cardinality_limit(),
            metrics_topic_label_enabled: true,
            metrics_consumer_group_label_enabled: true,
            otlp_exporter_endpoint: defaults::otlp_exporter_endpoint(),
            otlp_exporter_headers: CheetahString::new(),
            otlp_exporter_timeout_millis: defaults::otlp_exporter_timeout_millis(),
            metrics_prom_exporter_host: defaults::metrics_prom_exporter_host(),
            metrics_prom_exporter_port: defaults::metrics_prom_exporter_port(),
            metrics_prom_exporter_path: defaults::metrics_prom_exporter_path(),
            trace_exporter_type: TraceExporterType::Disable,
            trace_sample_ratio: defaults::trace_sample_ratio(),
            trace_propagate_context: defaults::trace_propagate_context(),
            trace_record_message_id: false,
            trace_record_message_keys: false,
            trace_record_body_size: defaults::trace_record_body_size(),
            log_exporter_type: LogExporterType::Disable,
            broker_permission: PermName::PERM_WRITE | PermName::PERM_READ,
            async_send_enable: false,
            store_path_root_dir: defaults::store_path_root_dir(),
            use_single_rocksdb_for_all_configs: false,
            enable_split_registration: false,
            split_registration_size: 800,
            register_broker_timeout_mills: 24000,
            is_in_broker_container: false,
            commercial_size_per_msg: 4 * 1024,
            recover_concurrently: false,
            duplication_enable: false,
            start_accept_send_request_time_stamp: 0,
            auto_create_topic_enable: true,
            enable_single_topic_register: true,
            broker_topic_enable: true,
            cluster_topic_enable: true,
            revive_queue_num: 8,
            enable_slave_acting_master: false,
            enable_register_producer: true,
            reject_transaction_message: false,
            enable_detail_stat: true,
            flush_consumer_offset_interval: 1000 * 5,
            force_register: true,
            register_name_server_period: 1000 * 30,
            send_heartbeat_timeout_millis: 1000,
            broker_fast_failure_enable: defaults::broker_fast_failure_enable(),
            send_request_executor_detached_enable: defaults::send_request_executor_detached_enable(),
            wait_time_mills_in_send_queue: defaults::wait_time_mills_in_send_queue(),
            sync_flush_backlog_reject_depth: defaults::sync_flush_backlog_reject_depth(),
            sync_flush_backlog_reject_wait_millis: defaults::sync_flush_backlog_reject_wait_millis(),
            ha_pending_reject_count: defaults::ha_pending_reject_count(),
            ha_pending_reject_wait_millis: defaults::ha_pending_reject_wait_millis(),
            reput_lag_reject_bytes: defaults::reput_lag_reject_bytes(),
            wait_time_mills_in_pull_queue: defaults::wait_time_mills_in_pull_queue(),
            wait_time_mills_in_lite_pull_queue: defaults::wait_time_mills_in_lite_pull_queue(),
            wait_time_mills_in_heartbeat_queue: defaults::wait_time_mills_in_heartbeat_queue(),
            wait_time_mills_in_transaction_queue: defaults::wait_time_mills_in_transaction_queue(),
            wait_time_mills_in_ack_queue: defaults::wait_time_mills_in_ack_queue(),
            wait_time_mills_in_admin_broker_queue: defaults::wait_time_mills_in_admin_broker_queue(),
            skip_pre_online: false,
            namesrv_addr: NAMESRV_ADDR.clone().map(|addr| addr.into()),
            fetch_name_srv_addr_by_dns_lookup: false,
            lite_pull_message_enable: true,
            enable_lite_event_mode: defaults::enable_lite_event_mode(),
            lite_event_check_interval: defaults::lite_event_check_interval(),
            lite_ttl_check_interval: defaults::lite_ttl_check_interval(),
            lite_subscription_check_interval: defaults::lite_subscription_check_interval(),
            lite_subscription_check_timeout_mills: defaults::lite_subscription_check_timeout_mills(),
            max_lite_subscription_count: defaults::max_lite_subscription_count(),
            enable_lite_pop_log: defaults::enable_lite_pop_log(),
            max_client_event_count: defaults::max_client_event_count(),
            lite_event_full_dispatch_delay_time: defaults::lite_event_full_dispatch_delay_time(),
            lite_lag_latency_collect_enable: defaults::lite_lag_latency_collect_enable(),
            lite_lag_latency_metrics_enable: defaults::lite_lag_latency_metrics_enable(),
            lite_lag_count_metrics_enable: defaults::lite_lag_count_metrics_enable(),
            lite_lag_latency_top_k: defaults::lite_lag_latency_top_k(),
            auto_create_subscription_group: true,
            channel_expired_timeout: 1000 * 120,
            subscription_expired_timeout: 1000 * 60 * 10,
            enable_property_filter: false,
            filter_support_retry: false,
            use_server_side_reset_offset: true,
            slave_read_enable: false,
            commercial_base_count: 1,
            reject_pull_consumer_enable: false,
            consumer_offset_update_version_step: 500,
            enable_broadcast_offset_store: true,
            transfer_msg_by_heap: true,
            short_polling_time_mills: 1000,
            long_polling_enable: true,
            max_error_rate_of_bloom_filter: 20,
            expect_consumer_num_use_filter: 32,
            bit_map_length_consume_queue_ext: 64,
            forward_timeout: 3 * 1000,
            validate_system_topic_when_update_topic: true,
            enable_mixed_message_type: false,
            auto_delete_unused_stats: false,
            store_reply_message_enable: true,
            lock_in_strict_mode: false,
            transaction_timeout: 6_000,
            transaction_op_msg_max_size: 4096,
            default_message_request_mode: MessageRequestMode::Pull,
            default_pop_share_queue_num: -1,
            load_balance_poll_name_server_interval: 30_000,
            server_load_balancer_enable: true,
            enable_remote_escape: false,
            enable_pop_log: false,
            enable_retry_topic_v2: false,
            retrieve_message_from_pop_retry_topic_v1: true,
            pop_from_retry_probability: 20,
            pop_consumer_fs_service_init: true,
            pop_consumer_kv_service_log: false,
            pop_consumer_kv_service_init: false,
            pop_consumer_kv_service_enable: false,
            pop_response_return_actual_retry_topic: false,
            init_pop_offset_by_check_msg_in_mem: true,
            enable_pop_buffer_merge: false,
            pop_ck_stay_buffer_time_out: 3_000,
            pop_ck_stay_buffer_time: 10_000,
            broker_role: BrokerRole::AsyncMaster,
            enable_pop_batch_ack: false,
            revive_interval: 1000,
            revive_max_slow: 3,
            revive_scan_time: 10_000,
            enable_skip_long_awaiting_ack: false,
            skip_when_ck_re_put_reach_max_times: false,
            compressed_register: false,
            broker_not_active_timeout_millis: 10_000,
            sync_broker_member_group_period: 1_000,
            pop_polling_map_size: 100000,
            max_pop_polling_size: 100000,
            pop_polling_size: 1024,
            enable_pop_message_threshold: false,
            pop_inflight_message_threshold: 10_000,
            pop_ck_max_buffer_size: 200_000,
            pop_ck_offset_max_queue_size: 20_000,
            delay_offset_update_version_step: 200,
            revive_ack_wait_ms: Duration::from_secs(3 * 60).as_millis() as u64,
            enable_calc_filter_bit_map: false,
            transaction_check_interval: 30_000,
            transaction_check_max: 15,
            transaction_op_batch_interval: 3_000,
            compatible_with_old_name_srv: true,
            broker_heartbeat_interval: 1000,
            controller_heartbeat_timeout_mills: 10_000,
            broker_election_priority: i32::MAX,
            recall_message_enable: true,
            allow_recall_when_broker_not_writeable: false,
            enable_fast_channel_event_process: false,
            auth_config_path: defaults::auth_config_path(),
            acl_file: defaults::acl_file(),
            acl_file_watch_enabled: defaults::acl_file_watch_enabled(),
            acl_file_watch_interval_millis: defaults::acl_file_watch_interval_millis(),
            authentication_enabled: defaults::authentication_enabled(),
            authentication_provider: defaults::authentication_provider(),
            authentication_metadata_provider: defaults::authentication_metadata_provider(),
            authentication_strategy: defaults::authentication_strategy(),
            authorization_enabled: defaults::authorization_enabled(),
            authorization_provider: defaults::authorization_provider(),
            authorization_metadata_provider: defaults::authorization_metadata_provider(),
            authorization_strategy: defaults::authorization_strategy(),
            authentication_whitelist: defaults::authentication_whitelist(),
            authorization_whitelist: defaults::authorization_whitelist(),
            init_authentication_user: defaults::init_authentication_user(),
            inner_client_authentication_credentials: defaults::inner_client_authentication_credentials(),
            signature_algorithm: defaults::signature_algorithm(),
            request_timestamp_expired_millis: defaults::request_timestamp_expired_millis(),
            migrate_auth_from_v1_enabled: defaults::migrate_auth_from_v1_enabled(),
            user_cache_max_num: defaults::user_cache_max_num(),
            user_cache_expired_second: defaults::user_cache_expired_second(),
            user_cache_refresh_second: defaults::user_cache_refresh_second(),
            acl_cache_max_num: defaults::acl_cache_max_num(),
            acl_cache_expired_second: defaults::acl_cache_expired_second(),
            acl_cache_refresh_second: defaults::acl_cache_refresh_second(),
            stateful_authentication_cache_max_num: defaults::stateful_authentication_cache_max_num(),
            stateful_authentication_cache_expired_second: defaults::stateful_authentication_cache_expired_second(),
            stateful_authorization_cache_max_num: defaults::stateful_authorization_cache_max_num(),
            stateful_authorization_cache_expired_second: defaults::stateful_authorization_cache_expired_second(),
        }
    }
}

impl BrokerConfig {
    pub fn broker_name(&self) -> &CheetahString {
        &self.broker_identity.broker_name
    }

    pub fn broker_ip1(&self) -> CheetahString {
        self.broker_ip1.clone()
    }

    pub fn broker_ip2(&self) -> Option<CheetahString> {
        self.broker_ip2.clone()
    }

    pub fn listen_port(&self) -> u32 {
        self.listen_port
    }

    pub fn trace_topic_enable(&self) -> bool {
        self.trace_topic_enable
    }

    pub fn broker_server_config(&self) -> &ServerConfig {
        &self.broker_server_config
    }

    pub fn region_id(&self) -> &str {
        self.region_id.as_str()
    }

    #[inline]
    pub fn broker_permission(&self) -> u32 {
        self.broker_permission
    }

    pub fn get_broker_addr(&self) -> String {
        format!("{}:{}", self.broker_ip1, self.listen_port)
    }

    pub fn get_start_accept_send_request_time_stamp(&self) -> i64 {
        self.start_accept_send_request_time_stamp
    }

    pub fn get_properties(&self) -> HashMap<CheetahString, CheetahString> {
        let mut properties = HashMap::new();
        properties.insert("brokerName".into(), self.broker_identity.broker_name.clone());
        properties.insert(
            "brokerClusterName".into(),
            self.broker_identity.broker_cluster_name.clone(),
        );
        properties.insert("brokerId".into(), self.broker_identity.broker_id.to_string().into());
        properties.insert(
            "isBrokerContainer".into(),
            self.broker_identity.is_broker_container.to_string().into(),
        );
        properties.insert(
            "defaultTopicQueueNums".into(),
            self.topic_queue_config.default_topic_queue_nums.to_string().into(),
        );
        properties.insert(
            "timerWheelEnable".into(),
            self.timer_wheel_config.timer_wheel_enable.to_string().into(),
        );
        properties.insert(
            "bindAddress".into(),
            self.broker_server_config.bind_address.clone().into(),
        );
        properties.insert("brokerIp1".into(), self.broker_ip1.clone().to_string().into());
        properties.insert("brokerIp2".into(), self.broker_ip2.clone().unwrap_or_default());
        properties.insert("listenPort".into(), self.listen_port.to_string().into());
        properties.insert("traceTopicEnable".into(), self.trace_topic_enable.to_string().into());
        properties.insert("msgTraceTopicName".into(), self.msg_trace_topic_name.clone());
        properties.insert(
            "metricsExporterType".into(),
            self.metrics_exporter_type.to_string().into(),
        );
        properties.insert(
            "metricsExportIntervalMillis".into(),
            self.metrics_export_interval_millis.to_string().into(),
        );
        properties.insert(
            "metricsCardinalityLimit".into(),
            self.metrics_cardinality_limit.to_string().into(),
        );
        properties.insert(
            "metricsTopicLabelEnabled".into(),
            self.metrics_topic_label_enabled.to_string().into(),
        );
        properties.insert(
            "metricsConsumerGroupLabelEnabled".into(),
            self.metrics_consumer_group_label_enabled.to_string().into(),
        );
        properties.insert("otlpExporterEndpoint".into(), self.otlp_exporter_endpoint.clone());
        properties.insert("otlpExporterHeaders".into(), self.otlp_exporter_headers.clone());
        properties.insert(
            "otlpExporterTimeoutMillis".into(),
            self.otlp_exporter_timeout_millis.to_string().into(),
        );
        properties.insert(
            "metricsPromExporterHost".into(),
            self.metrics_prom_exporter_host.clone(),
        );
        properties.insert(
            "metricsPromExporterPort".into(),
            self.metrics_prom_exporter_port.to_string().into(),
        );
        properties.insert(
            "metricsPromExporterPath".into(),
            self.metrics_prom_exporter_path.clone(),
        );
        properties.insert("traceExporterType".into(), self.trace_exporter_type.to_string().into());
        properties.insert("traceSampleRatio".into(), self.trace_sample_ratio.to_string().into());
        properties.insert(
            "tracePropagateContext".into(),
            self.trace_propagate_context.to_string().into(),
        );
        properties.insert(
            "traceRecordMessageId".into(),
            self.trace_record_message_id.to_string().into(),
        );
        properties.insert(
            "traceRecordMessageKeys".into(),
            self.trace_record_message_keys.to_string().into(),
        );
        properties.insert(
            "traceRecordBodySize".into(),
            self.trace_record_body_size.to_string().into(),
        );
        properties.insert("logExporterType".into(), self.log_exporter_type.to_string().into());
        properties.insert(
            "enableControllerMode".into(),
            self.enable_controller_mode.to_string().into(),
        );
        properties.insert("controllerAddr".into(), self.controller_addr.clone());
        properties.insert(
            "syncBrokerMetadataPeriod".into(),
            self.sync_broker_metadata_period.to_string().into(),
        );
        properties.insert(
            "syncControllerMetadataPeriod".into(),
            self.sync_controller_metadata_period.to_string().into(),
        );
        properties.insert("regionId".into(), self.region_id.clone());
        properties.insert(
            "observabilityEnvironment".into(),
            self.observability_environment.clone(),
        );
        properties.insert(
            "observabilityServiceInstanceId".into(),
            self.observability_service_instance_id.clone(),
        );
        properties.insert(
            "observabilityResourceAttributes".into(),
            self.observability_resource_attributes.clone(),
        );
        properties.insert("brokerName".into(), self.broker_identity.broker_name.clone());
        properties.insert("traceOn".into(), self.trace_on.to_string().into());
        properties.insert("brokerPermission".into(), self.broker_permission.to_string().into());
        properties.insert("asyncSendEnable".into(), self.async_send_enable.to_string().into());
        properties.insert("storePathRootDir".into(), self.store_path_root_dir.clone());
        properties.insert(
            "useSingleRocksDBForAllConfigs".into(),
            self.use_single_rocksdb_for_all_configs.to_string().into(),
        );
        properties.insert(
            "enableSplitRegistration".into(),
            self.enable_split_registration.to_string().into(),
        );
        properties.insert(
            "splitRegistrationSize".into(),
            self.split_registration_size.to_string().into(),
        );
        properties.insert(
            "registerBrokerTimeoutMills".into(),
            self.register_broker_timeout_mills.to_string().into(),
        );
        properties.insert(
            "isInBrokerContainer".into(),
            self.is_in_broker_container.to_string().into(),
        );
        properties.insert(
            "commercialSizePerMsg".into(),
            self.commercial_size_per_msg.to_string().into(),
        );
        properties.insert(
            "recoverConcurrently".into(),
            self.recover_concurrently.to_string().into(),
        );
        properties.insert("duplicationEnable".into(), self.duplication_enable.to_string().into());
        properties.insert(
            "startAcceptSendRequestTimeStamp".into(),
            self.start_accept_send_request_time_stamp.to_string().into(),
        );
        properties.insert(
            "autoCreateTopicEnable".into(),
            self.auto_create_topic_enable.to_string().into(),
        );
        properties.insert(
            "enableSingleTopicRegister".into(),
            self.enable_single_topic_register.to_string().into(),
        );
        properties.insert("brokerTopicEnable".into(), self.broker_topic_enable.to_string().into());
        properties.insert(
            "clusterTopicEnable".into(),
            self.cluster_topic_enable.to_string().into(),
        );
        properties.insert("reviveQueueNum".into(), self.revive_queue_num.to_string().into());
        properties.insert(
            "enableSlaveActingMaster".into(),
            self.enable_slave_acting_master.to_string().into(),
        );
        properties.insert(
            "rejectTransactionMessage".into(),
            self.reject_transaction_message.to_string().into(),
        );
        properties.insert("enableDetailStat".into(), self.enable_detail_stat.to_string().into());
        properties.insert(
            "flushConsumerOffsetInterval".into(),
            self.flush_consumer_offset_interval.to_string().into(),
        );
        properties.insert("forceRegister".into(), self.force_register.to_string().into());
        properties.insert(
            "registerNameServerPeriod".into(),
            self.register_name_server_period.to_string().into(),
        );
        properties.insert(
            "sendHeartbeatTimeoutMillis".into(),
            self.send_heartbeat_timeout_millis.to_string().into(),
        );
        properties.insert(
            "brokerFastFailureEnable".into(),
            self.broker_fast_failure_enable.to_string().into(),
        );
        properties.insert(
            "sendRequestExecutorDetachedEnable".into(),
            self.send_request_executor_detached_enable.to_string().into(),
        );
        properties.insert(
            "waitTimeMillsInSendQueue".into(),
            self.wait_time_mills_in_send_queue.to_string().into(),
        );
        properties.insert(
            "syncFlushBacklogRejectDepth".into(),
            self.sync_flush_backlog_reject_depth.to_string().into(),
        );
        properties.insert(
            "syncFlushBacklogRejectWaitMillis".into(),
            self.sync_flush_backlog_reject_wait_millis.to_string().into(),
        );
        properties.insert(
            "haPendingRejectCount".into(),
            self.ha_pending_reject_count.to_string().into(),
        );
        properties.insert(
            "haPendingRejectWaitMillis".into(),
            self.ha_pending_reject_wait_millis.to_string().into(),
        );
        properties.insert(
            "reputLagRejectBytes".into(),
            self.reput_lag_reject_bytes.to_string().into(),
        );
        properties.insert(
            "waitTimeMillsInPullQueue".into(),
            self.wait_time_mills_in_pull_queue.to_string().into(),
        );
        properties.insert(
            "waitTimeMillsInLitePullQueue".into(),
            self.wait_time_mills_in_lite_pull_queue.to_string().into(),
        );
        properties.insert(
            "waitTimeMillsInHeartbeatQueue".into(),
            self.wait_time_mills_in_heartbeat_queue.to_string().into(),
        );
        properties.insert(
            "waitTimeMillsInTransactionQueue".into(),
            self.wait_time_mills_in_transaction_queue.to_string().into(),
        );
        properties.insert(
            "waitTimeMillsInAckQueue".into(),
            self.wait_time_mills_in_ack_queue.to_string().into(),
        );
        properties.insert(
            "waitTimeMillsInAdminBrokerQueue".into(),
            self.wait_time_mills_in_admin_broker_queue.to_string().into(),
        );
        properties.insert("skipPreOnline".into(), self.skip_pre_online.to_string().into());
        properties.insert("namesrvAddr".into(), self.namesrv_addr.clone().unwrap_or_default());
        properties.insert(
            "fetchNameSrvAddrByDnsLookup".into(),
            self.fetch_name_srv_addr_by_dns_lookup.to_string().into(),
        );
        properties.insert(
            "litePullMessageEnable".into(),
            self.lite_pull_message_enable.to_string().into(),
        );
        properties.insert(
            "enableLiteEventMode".into(),
            self.enable_lite_event_mode.to_string().into(),
        );
        properties.insert(
            "liteEventCheckInterval".into(),
            self.lite_event_check_interval.to_string().into(),
        );
        properties.insert(
            "liteTtlCheckInterval".into(),
            self.lite_ttl_check_interval.to_string().into(),
        );
        properties.insert(
            "liteSubscriptionCheckInterval".into(),
            self.lite_subscription_check_interval.to_string().into(),
        );
        properties.insert(
            "liteSubscriptionCheckTimeoutMills".into(),
            self.lite_subscription_check_timeout_mills.to_string().into(),
        );
        properties.insert(
            "maxLiteSubscriptionCount".into(),
            self.max_lite_subscription_count.to_string().into(),
        );
        properties.insert("enableLitePopLog".into(), self.enable_lite_pop_log.to_string().into());
        properties.insert(
            "maxClientEventCount".into(),
            self.max_client_event_count.to_string().into(),
        );
        properties.insert(
            "liteEventFullDispatchDelayTime".into(),
            self.lite_event_full_dispatch_delay_time.to_string().into(),
        );
        properties.insert(
            "liteLagLatencyCollectEnable".into(),
            self.lite_lag_latency_collect_enable.to_string().into(),
        );
        properties.insert(
            "liteLagLatencyMetricsEnable".into(),
            self.lite_lag_latency_metrics_enable.to_string().into(),
        );
        properties.insert(
            "liteLagCountMetricsEnable".into(),
            self.lite_lag_count_metrics_enable.to_string().into(),
        );
        properties.insert(
            "liteLagLatencyTopK".into(),
            self.lite_lag_latency_top_k.to_string().into(),
        );
        properties.insert(
            "autoCreateSubscriptionGroup".into(),
            self.auto_create_subscription_group.to_string().into(),
        );
        properties.insert(
            "channelExpiredTimeout".into(),
            self.channel_expired_timeout.to_string().into(),
        );
        properties.insert(
            "subscriptionExpiredTimeout".into(),
            self.subscription_expired_timeout.to_string().into(),
        );
        properties.insert(
            "enablePropertyFilter".into(),
            self.enable_property_filter.to_string().into(),
        );
        properties.insert(
            "filterSupportRetry".into(),
            self.filter_support_retry.to_string().into(),
        );
        properties.insert(
            "useServerSideResetOffset".into(),
            self.use_server_side_reset_offset.to_string().into(),
        );
        properties.insert("slaveReadEnable".into(), self.slave_read_enable.to_string().into());
        properties.insert(
            "commercialBaseCount".into(),
            self.commercial_base_count.to_string().into(),
        );
        properties.insert(
            "rejectPullConsumerEnable".into(),
            self.reject_pull_consumer_enable.to_string().into(),
        );
        properties.insert(
            "consumerOffsetUpdateVersionStep".into(),
            self.consumer_offset_update_version_step.to_string().into(),
        );
        properties.insert(
            "enableBroadcastOffsetStore".into(),
            self.enable_broadcast_offset_store.to_string().into(),
        );
        properties.insert("transferMsgByHeap".into(), self.transfer_msg_by_heap.to_string().into());
        properties.insert(
            "shortPollingTimeMills".into(),
            self.short_polling_time_mills.to_string().into(),
        );
        properties.insert("longPollingEnable".into(), self.long_polling_enable.to_string().into());
        properties.insert(
            "maxErrorRateOfBloomFilter".into(),
            self.max_error_rate_of_bloom_filter.to_string().into(),
        );
        properties.insert(
            "expectConsumerNumUseFilter".into(),
            self.expect_consumer_num_use_filter.to_string().into(),
        );
        properties.insert(
            "bitMapLengthConsumeQueueExt".into(),
            self.bit_map_length_consume_queue_ext.to_string().into(),
        );
        properties.insert(
            "popConsumerFSServiceInit".into(),
            self.pop_consumer_fs_service_init.to_string().into(),
        );
        properties.insert(
            "popConsumerKVServiceLog".into(),
            self.pop_consumer_kv_service_log.to_string().into(),
        );
        properties.insert(
            "popConsumerKVServiceInit".into(),
            self.pop_consumer_kv_service_init.to_string().into(),
        );
        properties.insert(
            "popConsumerKVServiceEnable".into(),
            self.pop_consumer_kv_service_enable.to_string().into(),
        );
        properties.insert(
            "enablePopMessageThreshold".into(),
            self.enable_pop_message_threshold.to_string().into(),
        );
        properties.insert(
            "validateSystemTopicWhenUpdateTopic".into(),
            self.validate_system_topic_when_update_topic.to_string().into(),
        );
        properties.insert(
            "brokerHeartbeatInterval".into(),
            self.broker_heartbeat_interval.to_string().into(),
        );
        properties.insert(
            "controllerHeartBeatTimeoutMills".into(),
            self.controller_heartbeat_timeout_mills.to_string().into(),
        );
        properties.insert(
            "brokerElectionPriority".into(),
            self.broker_election_priority.to_string().into(),
        );
        properties.insert(
            "enableMixedMessageType".into(),
            self.enable_mixed_message_type.to_string().into(),
        );
        properties.insert(
            "autoDeleteUnusedStats".into(),
            self.auto_delete_unused_stats.to_string().into(),
        );
        properties.insert("forwardTimeout".into(), self.forward_timeout.to_string().into());
        properties.insert("authConfigPath".into(), self.auth_config_path.clone());
        properties.insert("aclFile".into(), self.acl_file.clone());
        properties.insert(
            "aclFileWatchEnabled".into(),
            self.acl_file_watch_enabled.to_string().into(),
        );
        properties.insert(
            "aclFileWatchIntervalMillis".into(),
            self.acl_file_watch_interval_millis.to_string().into(),
        );
        properties.insert(
            "authenticationEnabled".into(),
            self.authentication_enabled.to_string().into(),
        );
        properties.insert("authenticationProvider".into(), self.authentication_provider.clone());
        properties.insert(
            "authenticationMetadataProvider".into(),
            self.authentication_metadata_provider.clone(),
        );
        properties.insert("authenticationStrategy".into(), self.authentication_strategy.clone());
        properties.insert(
            "authorizationEnabled".into(),
            self.authorization_enabled.to_string().into(),
        );
        properties.insert("authorizationProvider".into(), self.authorization_provider.clone());
        properties.insert(
            "authorizationMetadataProvider".into(),
            self.authorization_metadata_provider.clone(),
        );
        properties.insert("authorizationStrategy".into(), self.authorization_strategy.clone());
        properties.insert("authenticationWhitelist".into(), self.authentication_whitelist.clone());
        properties.insert("authorizationWhitelist".into(), self.authorization_whitelist.clone());
        properties.insert("initAuthenticationUser".into(), self.init_authentication_user.clone());
        properties.insert(
            "innerClientAuthenticationCredentials".into(),
            self.inner_client_authentication_credentials.clone(),
        );
        properties.insert("signatureAlgorithm".into(), self.signature_algorithm.clone());
        properties.insert(
            "requestTimestampExpiredMillis".into(),
            self.request_timestamp_expired_millis.to_string().into(),
        );
        properties.insert(
            "migrateAuthFromV1Enabled".into(),
            self.migrate_auth_from_v1_enabled.to_string().into(),
        );
        properties.insert("userCacheMaxNum".into(), self.user_cache_max_num.to_string().into());
        properties.insert(
            "userCacheExpiredSecond".into(),
            self.user_cache_expired_second.to_string().into(),
        );
        properties.insert(
            "userCacheRefreshSecond".into(),
            self.user_cache_refresh_second.to_string().into(),
        );
        properties.insert("aclCacheMaxNum".into(), self.acl_cache_max_num.to_string().into());
        properties.insert(
            "aclCacheExpiredSecond".into(),
            self.acl_cache_expired_second.to_string().into(),
        );
        properties.insert(
            "aclCacheRefreshSecond".into(),
            self.acl_cache_refresh_second.to_string().into(),
        );
        properties.insert(
            "statefulAuthenticationCacheMaxNum".into(),
            self.stateful_authentication_cache_max_num.to_string().into(),
        );
        properties.insert(
            "statefulAuthenticationCacheExpiredSecond".into(),
            self.stateful_authentication_cache_expired_second.to_string().into(),
        );
        properties.insert(
            "statefulAuthorizationCacheMaxNum".into(),
            self.stateful_authorization_cache_max_num.to_string().into(),
        );
        properties.insert(
            "statefulAuthorizationCacheExpiredSecond".into(),
            self.stateful_authorization_cache_expired_second.to_string().into(),
        );
        for (key, value) in self.broker_server_config.tls_config.java_property_entries() {
            properties.insert(key.into(), value.into());
        }
        properties
    }
}

pub fn default_broker_name() -> String {
    LOCAL_HOST_NAME.clone().unwrap_or_else(|| "DEFAULT_BROKER".to_string())
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TopicQueueConfig {
    #[serde(default = "defaults::default_topic_queue_nums")]
    pub default_topic_queue_nums: u32,
}

impl Default for TopicQueueConfig {
    fn default() -> Self {
        TopicQueueConfig {
            default_topic_queue_nums: 8,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TimerWheelConfig {
    #[serde(default)]
    pub timer_wheel_enable: bool,
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::BrokerConfig;
    use crate::common::metrics::LogExporterType;
    use crate::common::metrics::MetricsExporterType;
    use crate::common::metrics::TraceExporterType;

    #[test]
    fn default_broker_config_uses_java_lite_defaults() {
        let config = BrokerConfig::default();

        assert!(config.lite_pull_message_enable);
        assert!(config.enable_lite_event_mode);
        assert_eq!(config.lite_event_check_interval, 10_000);
        assert_eq!(config.lite_ttl_check_interval, 120_000);
        assert_eq!(config.lite_subscription_check_interval, 120_000);
        assert_eq!(config.lite_subscription_check_timeout_mills, 180_000);
        assert_eq!(config.max_lite_subscription_count, 100_000);
        assert!(!config.enable_lite_pop_log);
        assert_eq!(config.max_client_event_count, 100);
        assert_eq!(config.lite_event_full_dispatch_delay_time, 10_000);
        assert!(!config.lite_lag_latency_collect_enable);
        assert!(!config.lite_lag_latency_metrics_enable);
        assert!(!config.lite_lag_count_metrics_enable);
        assert_eq!(config.lite_lag_latency_top_k, 50);
    }

    #[test]
    fn default_broker_config_disables_metrics_exporter() {
        let config = BrokerConfig::default();

        assert_eq!(config.metrics_exporter_type, MetricsExporterType::Disable);
        assert_eq!(config.metrics_export_interval_millis, 30_000);
        assert_eq!(config.metrics_cardinality_limit, 10_000);
        assert!(config.metrics_topic_label_enabled);
        assert!(config.metrics_consumer_group_label_enabled);
        assert_eq!(config.otlp_exporter_endpoint, "http://127.0.0.1:4317");
        assert_eq!(config.otlp_exporter_headers, "");
        assert_eq!(config.otlp_exporter_timeout_millis, 3_000);
        assert_eq!(config.metrics_prom_exporter_host, "127.0.0.1");
        assert_eq!(config.metrics_prom_exporter_port, 5557);
        assert_eq!(config.metrics_prom_exporter_path, "/metrics");
        assert_eq!(config.trace_exporter_type, TraceExporterType::Disable);
        assert!((config.trace_sample_ratio - 0.01).abs() < f64::EPSILON);
        assert!(config.trace_propagate_context);
        assert_eq!(config.observability_environment, "dev");
        assert_eq!(config.observability_service_instance_id, "");
        assert_eq!(config.observability_resource_attributes, "");
        assert!(!config.trace_record_message_id);
        assert!(!config.trace_record_message_keys);
        assert!(config.trace_record_body_size);
        assert_eq!(config.log_exporter_type, LogExporterType::Disable);

        let properties = config.get_properties();
        assert_eq!(
            properties.get("metricsExporterType").map(CheetahString::as_str),
            Some("disable")
        );
        assert_eq!(
            properties.get("metricsExportIntervalMillis").map(CheetahString::as_str),
            Some("30000")
        );
        assert_eq!(
            properties.get("metricsCardinalityLimit").map(CheetahString::as_str),
            Some("10000")
        );
        assert_eq!(
            properties.get("metricsTopicLabelEnabled").map(CheetahString::as_str),
            Some("true")
        );
        assert_eq!(
            properties
                .get("metricsConsumerGroupLabelEnabled")
                .map(CheetahString::as_str),
            Some("true")
        );
        assert_eq!(
            properties.get("otlpExporterEndpoint").map(CheetahString::as_str),
            Some("http://127.0.0.1:4317")
        );
        assert_eq!(
            properties.get("otlpExporterHeaders").map(CheetahString::as_str),
            Some("")
        );
        assert_eq!(
            properties.get("otlpExporterTimeoutMillis").map(CheetahString::as_str),
            Some("3000")
        );
        assert_eq!(
            properties.get("metricsPromExporterHost").map(CheetahString::as_str),
            Some("127.0.0.1")
        );
        assert_eq!(
            properties.get("metricsPromExporterPort").map(CheetahString::as_str),
            Some("5557")
        );
        assert_eq!(
            properties.get("metricsPromExporterPath").map(CheetahString::as_str),
            Some("/metrics")
        );
        assert_eq!(
            properties.get("traceExporterType").map(CheetahString::as_str),
            Some("disable")
        );
        assert_eq!(
            properties.get("traceSampleRatio").map(CheetahString::as_str),
            Some("0.01")
        );
        assert_eq!(
            properties.get("tracePropagateContext").map(CheetahString::as_str),
            Some("true")
        );
        assert_eq!(
            properties.get("observabilityEnvironment").map(CheetahString::as_str),
            Some("dev")
        );
        assert_eq!(
            properties
                .get("observabilityServiceInstanceId")
                .map(CheetahString::as_str),
            Some("")
        );
        assert_eq!(
            properties
                .get("observabilityResourceAttributes")
                .map(CheetahString::as_str),
            Some("")
        );
        assert_eq!(
            properties.get("traceRecordMessageId").map(CheetahString::as_str),
            Some("false")
        );
        assert_eq!(
            properties.get("traceRecordMessageKeys").map(CheetahString::as_str),
            Some("false")
        );
        assert_eq!(
            properties.get("traceRecordBodySize").map(CheetahString::as_str),
            Some("true")
        );
        assert_eq!(
            properties.get("logExporterType").map(CheetahString::as_str),
            Some("disable")
        );
    }

    #[test]
    fn default_broker_config_uses_java_fast_failure_defaults() {
        let config = BrokerConfig::default();

        assert!(config.broker_fast_failure_enable);
        assert!(!config.send_request_executor_detached_enable);
        assert_eq!(config.wait_time_mills_in_send_queue, 200);
        assert_eq!(config.sync_flush_backlog_reject_depth, 0);
        assert_eq!(config.sync_flush_backlog_reject_wait_millis, 0);
        assert_eq!(config.ha_pending_reject_count, 0);
        assert_eq!(config.ha_pending_reject_wait_millis, 0);
        assert_eq!(config.reput_lag_reject_bytes, 0);
        assert_eq!(config.wait_time_mills_in_pull_queue, 5_000);
        assert_eq!(config.wait_time_mills_in_lite_pull_queue, 5_000);
        assert_eq!(config.wait_time_mills_in_heartbeat_queue, 31_000);
        assert_eq!(config.wait_time_mills_in_transaction_queue, 3_000);
        assert_eq!(config.wait_time_mills_in_ack_queue, 3_000);
        assert_eq!(config.wait_time_mills_in_admin_broker_queue, 5_000);
    }

    #[test]
    fn default_broker_config_uses_java_pop_kv_defaults() {
        let config = BrokerConfig::default();

        assert!(config.pop_consumer_fs_service_init);
        assert!(!config.pop_consumer_kv_service_log);
        assert!(!config.pop_consumer_kv_service_init);
        assert!(!config.pop_consumer_kv_service_enable);
        assert!(!config.enable_pop_message_threshold);

        let properties = config.get_properties();
        assert_eq!(
            properties.get("popConsumerFSServiceInit").map(|value| value.as_str()),
            Some("true")
        );
        assert_eq!(
            properties.get("popConsumerKVServiceLog").map(|value| value.as_str()),
            Some("false")
        );
        assert_eq!(
            properties.get("popConsumerKVServiceInit").map(|value| value.as_str()),
            Some("false")
        );
        assert_eq!(
            properties.get("popConsumerKVServiceEnable").map(|value| value.as_str()),
            Some("false")
        );
        assert_eq!(
            properties.get("enablePopMessageThreshold").map(|value| value.as_str()),
            Some("false")
        );
    }

    #[test]
    fn get_properties_contains_java_lite_keys() {
        let config = BrokerConfig::default();
        let properties = config.get_properties();

        assert_eq!(
            properties.get("litePullMessageEnable").map(|value| value.as_str()),
            Some("true")
        );
        assert_eq!(
            properties.get("enableLiteEventMode").map(|value| value.as_str()),
            Some("true")
        );
        assert_eq!(
            properties.get("liteEventCheckInterval").map(|value| value.as_str()),
            Some("10000")
        );
        assert_eq!(
            properties.get("liteTtlCheckInterval").map(|value| value.as_str()),
            Some("120000")
        );
        assert_eq!(
            properties
                .get("liteSubscriptionCheckInterval")
                .map(|value| value.as_str()),
            Some("120000")
        );
        assert_eq!(
            properties
                .get("liteSubscriptionCheckTimeoutMills")
                .map(|value| value.as_str()),
            Some("180000")
        );
        assert_eq!(
            properties.get("maxLiteSubscriptionCount").map(|value| value.as_str()),
            Some("100000")
        );
        assert_eq!(
            properties.get("enableLitePopLog").map(|value| value.as_str()),
            Some("false")
        );
        assert_eq!(
            properties.get("maxClientEventCount").map(|value| value.as_str()),
            Some("100")
        );
        assert_eq!(
            properties
                .get("liteEventFullDispatchDelayTime")
                .map(|value| value.as_str()),
            Some("10000")
        );
        assert_eq!(
            properties
                .get("liteLagLatencyCollectEnable")
                .map(|value| value.as_str()),
            Some("false")
        );
        assert_eq!(
            properties
                .get("liteLagLatencyMetricsEnable")
                .map(|value| value.as_str()),
            Some("false")
        );
        assert_eq!(
            properties.get("liteLagCountMetricsEnable").map(|value| value.as_str()),
            Some("false")
        );
        assert_eq!(
            properties.get("liteLagLatencyTopK").map(|value| value.as_str()),
            Some("50")
        );
    }

    #[test]
    fn get_properties_contains_java_tls_keys() {
        let mut config = BrokerConfig::default();
        config.broker_server_config.tls_config.enable = true;
        config.broker_server_config.tls_config.server.cert_path = Some("/certs/server.pem".to_string());

        let properties = config.get_properties();

        assert_eq!(properties.get("tls.enable").map(|value| value.as_str()), Some("true"));
        assert_eq!(
            properties.get("tls.server.mode").map(|value| value.as_str()),
            Some("permissive")
        );
        assert_eq!(
            properties.get("tls.server.certPath").map(|value| value.as_str()),
            Some("/certs/server.pem")
        );
    }

    #[test]
    fn get_properties_contains_java_fast_failure_keys() {
        let config = BrokerConfig::default();
        let properties = config.get_properties();

        assert_eq!(
            properties.get("brokerFastFailureEnable").map(|value| value.as_str()),
            Some("true")
        );
        assert_eq!(
            properties
                .get("sendRequestExecutorDetachedEnable")
                .map(|value| value.as_str()),
            Some("false")
        );
        assert_eq!(
            properties.get("waitTimeMillsInSendQueue").map(|value| value.as_str()),
            Some("200")
        );
        assert_eq!(
            properties
                .get("syncFlushBacklogRejectDepth")
                .map(|value| value.as_str()),
            Some("0")
        );
        assert_eq!(
            properties
                .get("syncFlushBacklogRejectWaitMillis")
                .map(|value| value.as_str()),
            Some("0")
        );
        assert_eq!(
            properties.get("waitTimeMillsInPullQueue").map(|value| value.as_str()),
            Some("5000")
        );
        assert_eq!(
            properties
                .get("waitTimeMillsInLitePullQueue")
                .map(|value| value.as_str()),
            Some("5000")
        );
        assert_eq!(
            properties
                .get("waitTimeMillsInHeartbeatQueue")
                .map(|value| value.as_str()),
            Some("31000")
        );
        assert_eq!(
            properties
                .get("waitTimeMillsInTransactionQueue")
                .map(|value| value.as_str()),
            Some("3000")
        );
        assert_eq!(
            properties.get("waitTimeMillsInAckQueue").map(|value| value.as_str()),
            Some("3000")
        );
        assert_eq!(
            properties
                .get("waitTimeMillsInAdminBrokerQueue")
                .map(|value| value.as_str()),
            Some("5000")
        );
    }

    #[test]
    fn default_broker_config_uses_auth_acl_file_defaults() {
        let config = BrokerConfig::default();

        assert!(config.acl_file.is_empty());
        assert!(!config.acl_file_watch_enabled);
        assert_eq!(config.acl_file_watch_interval_millis, 5_000);
        assert_eq!(config.signature_algorithm.as_str(), "HmacSHA1");
        assert_eq!(config.request_timestamp_expired_millis, 0);
    }

    #[test]
    fn serde_accepts_auth_acl_file_camel_case_keys() {
        let config: BrokerConfig = serde_json::from_str(
            r#"{
                "aclFile": "conf/plain_acl.yml",
                "aclFileWatchEnabled": true,
                "aclFileWatchIntervalMillis": 250,
                "signatureAlgorithm": "HmacSHA256",
                "requestTimestampExpiredMillis": 300000
            }"#,
        )
        .expect("broker config should deserialize auth ACL file keys");

        assert_eq!(config.acl_file.as_str(), "conf/plain_acl.yml");
        assert!(config.acl_file_watch_enabled);
        assert_eq!(config.acl_file_watch_interval_millis, 250);
        assert_eq!(config.signature_algorithm.as_str(), "HmacSHA256");
        assert_eq!(config.request_timestamp_expired_millis, 300_000);
    }

    #[test]
    fn serde_accepts_java_fast_failure_camel_case_keys() {
        let config: BrokerConfig = serde_json::from_str(
            r#"{
                "brokerFastFailureEnable": false,
                "sendRequestExecutorDetachedEnable": true,
                "waitTimeMillsInSendQueue": 201,
                "syncFlushBacklogRejectDepth": 32,
                "syncFlushBacklogRejectWaitMillis": 150,
                "haPendingRejectCount": 16,
                "haPendingRejectWaitMillis": 250,
                "reputLagRejectBytes": 1048576,
                "waitTimeMillsInPullQueue": 5001,
                "waitTimeMillsInLitePullQueue": 5002,
                "waitTimeMillsInHeartbeatQueue": 31001,
                "waitTimeMillsInTransactionQueue": 3001,
                "waitTimeMillsInAckQueue": 3002,
                "waitTimeMillsInAdminBrokerQueue": 5003
            }"#,
        )
        .expect("broker config should deserialize Java fast failure keys");

        assert!(!config.broker_fast_failure_enable);
        assert!(config.send_request_executor_detached_enable);
        assert_eq!(config.wait_time_mills_in_send_queue, 201);
        assert_eq!(config.sync_flush_backlog_reject_depth, 32);
        assert_eq!(config.sync_flush_backlog_reject_wait_millis, 150);
        assert_eq!(config.ha_pending_reject_count, 16);
        assert_eq!(config.ha_pending_reject_wait_millis, 250);
        assert_eq!(config.reput_lag_reject_bytes, 1_048_576);
        assert_eq!(config.wait_time_mills_in_pull_queue, 5_001);
        assert_eq!(config.wait_time_mills_in_lite_pull_queue, 5_002);
        assert_eq!(config.wait_time_mills_in_heartbeat_queue, 31_001);
        assert_eq!(config.wait_time_mills_in_transaction_queue, 3_001);
        assert_eq!(config.wait_time_mills_in_ack_queue, 3_002);
        assert_eq!(config.wait_time_mills_in_admin_broker_queue, 5_003);
    }
}
