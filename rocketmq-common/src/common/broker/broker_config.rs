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
use std::time::Duration;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

use crate::common::broker::broker_role::BrokerRole;
use crate::common::constant::PermName;
use crate::common::message::message_enum::MessageRequestMode;
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

    pub fn trace_on() -> bool {
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

    pub fn register_name_server_period() -> u64 {
        1000 * 30
    }

    pub fn namesrv_addr() -> Option<CheetahString> {
        NAMESRV_ADDR.clone().map(|addr| addr.into())
    }

    pub fn lite_pull_message_enable() -> bool {
        true
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

    #[serde(default = "defaults::region_id")]
    pub region_id: CheetahString,

    #[serde(default = "defaults::trace_on")]
    pub trace_on: bool,

    #[serde(default = "defaults::broker_permission")]
    pub broker_permission: u32,

    #[serde(default)]
    pub async_send_enable: bool, //not used in rust version,only for Java compatibility

    #[serde(default = "defaults::store_path_root_dir")]
    pub store_path_root_dir: CheetahString,

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

    #[serde(default)]
    pub skip_pre_online: bool,

    #[serde(default = "defaults::namesrv_addr")]
    pub namesrv_addr: Option<CheetahString>,

    #[serde(default)]
    pub fetch_name_srv_addr_by_dns_lookup: bool,

    #[serde(default = "defaults::lite_pull_message_enable")]
    pub lite_pull_message_enable: bool,

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
}

impl Default for BrokerConfig {
    fn default() -> Self {
        let broker_identity = BrokerIdentity::new();
        let local_ip = local_ip_address::local_ip().unwrap();
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
            region_id: CheetahString::from_static_str(mix_all::DEFAULT_TRACE_REGION_ID),
            trace_on: true,
            broker_permission: PermName::PERM_WRITE | PermName::PERM_READ,
            async_send_enable: false,
            store_path_root_dir: dirs::home_dir()
                .unwrap()
                .join("store")
                .to_string_lossy()
                .into_owned()
                .into(),
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
            reject_transaction_message: false,
            enable_detail_stat: true,
            flush_consumer_offset_interval: 1000 * 5,
            force_register: true,
            register_name_server_period: 1000 * 30,
            skip_pre_online: false,
            namesrv_addr: NAMESRV_ADDR.clone().map(|addr| addr.into()),
            fetch_name_srv_addr_by_dns_lookup: false,
            lite_pull_message_enable: true,
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
            "enableControllerMode".into(),
            self.enable_controller_mode.to_string().into(),
        );
        properties.insert("regionId".into(), self.region_id.clone());
        properties.insert("brokerName".into(), self.broker_identity.broker_name.clone());
        properties.insert("traceOn".into(), self.trace_on.to_string().into());
        properties.insert("brokerPermission".into(), self.broker_permission.to_string().into());
        properties.insert("asyncSendEnable".into(), self.async_send_enable.to_string().into());
        properties.insert("storePathRootDir".into(), self.store_path_root_dir.clone());
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
            "validateSystemTopicWhenUpdateTopic".into(),
            self.validate_system_topic_when_update_topic.to_string().into(),
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
