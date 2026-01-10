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

use std::env;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::utils::name_server_address_utils::NameServerAddressUtils;
use rocketmq_common::utils::name_server_address_utils::NAMESRV_ENDPOINT_PATTERN;
use rocketmq_common::utils::network_util::NetworkUtil;
use rocketmq_common::utils::string_utils::StringUtils;
use rocketmq_common::TimeUtils::get_current_nano;
use rocketmq_remoting::protocol::namespace_util::NamespaceUtil;
use rocketmq_remoting::protocol::request_type::RequestType;
use rocketmq_remoting::protocol::LanguageCode;

use crate::base::access_channel::AccessChannel;

#[derive(Clone)]
pub struct ClientConfig {
    pub namesrv_addr: Option<CheetahString>,
    pub client_ip: Option<CheetahString>,
    pub instance_name: CheetahString,
    pub client_callback_executor_threads: usize,
    pub namespace: Option<CheetahString>,
    pub namespace_initialized: Arc<AtomicBool>,
    pub namespace_v2: Option<CheetahString>,
    pub access_channel: AccessChannel,
    pub poll_name_server_interval: u32,
    pub heartbeat_broker_interval: u32,
    pub persist_consumer_offset_interval: u32,
    pub pull_time_delay_millis_when_exception: u32,
    pub unit_mode: bool,
    pub unit_name: Option<CheetahString>,
    pub decode_read_body: bool,
    pub decode_decompress_body: bool,
    pub vip_channel_enabled: bool,
    pub use_heartbeat_v2: bool,
    pub use_tls: bool,
    pub socks_proxy_config: CheetahString,
    pub mq_client_api_timeout: u64,
    pub detect_timeout: u32,
    pub detect_interval: u32,
    pub language: LanguageCode,
    pub enable_stream_request_type: bool,
    pub send_latency_enable: bool,
    pub start_detector_enable: bool,
    pub enable_heartbeat_channel_event_listener: bool,
    pub enable_trace: bool,
    pub trace_topic: Option<CheetahString>,
    pub trace_msg_batch_num: usize,
    pub max_page_size_in_get_metadata: usize,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientConfig {
    pub const SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY: &'static str = "com.rocketmq.sendMessageWithVIPChannel";
    pub const SOCKS_PROXY_CONFIG: &'static str = "com.rocketmq.socks.proxy.config";
    pub const DECODE_READ_BODY: &'static str = "com.rocketmq.read.body";
    pub const DECODE_DECOMPRESS_BODY: &'static str = "com.rocketmq.decompress.body";
    pub const SEND_LATENCY_ENABLE: &'static str = "com.rocketmq.sendLatencyEnable";
    pub const START_DETECTOR_ENABLE: &'static str = "com.rocketmq.startDetectorEnable";
    pub const HEART_BEAT_V2: &'static str = "com.rocketmq.heartbeat.v2";

    pub fn new() -> Self {
        ClientConfig {
            namesrv_addr: NameServerAddressUtils::get_name_server_addresses().map(|addr| addr.into()),
            client_ip: NetworkUtil::get_local_address().map(|addr| addr.into()),
            instance_name: env::var("rocketmq.client.name")
                .unwrap_or_else(|_| "DEFAULT".to_string())
                .into(),
            client_callback_executor_threads: num_cpus::get(),
            namespace: None,
            namespace_initialized: Arc::new(AtomicBool::new(false)),
            namespace_v2: None,
            access_channel: AccessChannel::Local,
            poll_name_server_interval: Duration::from_secs(30).as_millis() as u32,
            heartbeat_broker_interval: Duration::from_secs(30).as_millis() as u32,
            persist_consumer_offset_interval: Duration::from_secs(5).as_millis() as u32,
            pull_time_delay_millis_when_exception: 1000,
            unit_mode: false,
            unit_name: None,
            decode_read_body: env::var(Self::DECODE_READ_BODY)
                .unwrap_or_else(|_| "true".to_string())
                .parse::<bool>()
                .unwrap_or(true),
            decode_decompress_body: env::var(Self::DECODE_DECOMPRESS_BODY)
                .unwrap_or_else(|_| "true".to_string())
                .parse::<bool>()
                .unwrap_or(true),
            vip_channel_enabled: env::var(Self::SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY)
                .unwrap_or_else(|_| "false".to_string())
                .parse::<bool>()
                .unwrap_or(false),
            use_heartbeat_v2: env::var(Self::HEART_BEAT_V2)
                .unwrap_or_else(|_| "false".to_string())
                .parse::<bool>()
                .unwrap_or(false),
            use_tls: false,
            socks_proxy_config: env::var(Self::SOCKS_PROXY_CONFIG)
                .unwrap_or_else(|_| "{}".to_string())
                .into(),
            mq_client_api_timeout: Duration::from_secs(3).as_millis() as u64,
            detect_timeout: 200,
            detect_interval: Duration::from_secs(2).as_millis() as u32,
            language: LanguageCode::RUST,
            enable_stream_request_type: false,
            send_latency_enable: env::var(Self::SEND_LATENCY_ENABLE)
                .unwrap_or_else(|_| "false".to_string())
                .parse::<bool>()
                .unwrap_or(false),
            start_detector_enable: env::var(Self::START_DETECTOR_ENABLE)
                .unwrap_or_else(|_| "false".to_string())
                .parse::<bool>()
                .unwrap_or(false),
            enable_heartbeat_channel_event_listener: true,
            enable_trace: false,
            trace_topic: None,
            trace_msg_batch_num: 10,
            max_page_size_in_get_metadata: 2000,
        }
    }
}

impl ClientConfig {
    #[inline]
    pub fn with_namespace(&mut self, resource: &str) -> CheetahString {
        NamespaceUtil::wrap_namespace(self.get_namespace().unwrap_or_default().as_str(), resource).into()
    }

    #[inline]
    pub fn queue_with_namespace(&mut self, mut queue: MessageQueue) -> MessageQueue {
        if let Some(namespace) = self.get_namespace() {
            if !namespace.is_empty() {
                let topic =
                    CheetahString::from_string(NamespaceUtil::wrap_namespace(namespace.as_str(), queue.get_topic()));
                queue.set_topic(topic);
                return queue;
            }
        }
        queue
    }

    #[inline]
    pub fn get_namespace(&mut self) -> Option<CheetahString> {
        let namespace_initialized = self.namespace_initialized.load(Ordering::Acquire);
        if namespace_initialized {
            return self.namespace.clone();
        }

        if let Some(ref namespace) = self.namespace {
            return Some(namespace.clone());
        }

        if let Some(ref namesrv_addr) = self.namesrv_addr {
            if NameServerAddressUtils::validate_instance_endpoint(namesrv_addr.as_ref()) {
                self.namespace =
                    NameServerAddressUtils::parse_instance_id_from_endpoint(namesrv_addr.as_ref()).map(|id| id.into());
            }
        }
        self.namespace_initialized.store(true, Ordering::Release);
        self.namespace.clone()
    }

    #[inline]
    pub fn change_instance_name_to_pid(&mut self) {
        if self.instance_name == "DEFAULT" {
            self.instance_name = format!("{}#{}", std::process::id(), get_current_nano()).into();
        }
    }
    #[inline]
    pub fn set_instance_name(&mut self, instance_name: CheetahString) {
        self.instance_name = instance_name;
    }
    #[inline]
    pub fn set_namesrv_addr(&mut self, namesrv_addr: CheetahString) {
        self.namesrv_addr = Some(namesrv_addr);
        self.namespace_initialized.store(false, Ordering::Release);
    }

    #[inline]
    pub fn build_mq_client_id(&self) -> String {
        // Pre-allocate capacity to avoid reallocations
        let estimated_capacity = self.client_ip.as_ref().map(|ip| ip.len()).unwrap_or(15)
            + self.instance_name.len()
            + self.unit_name.as_ref().map(|un| un.len() + 1).unwrap_or(0)
            + if self.enable_stream_request_type { 8 } else { 0 }
            + 3; // For '@' separators

        let mut sb = String::with_capacity(estimated_capacity);
        if let Some(ref client_ip) = self.client_ip {
            sb.push_str(client_ip.as_str());
        }

        sb.push('@');
        sb.push_str(self.instance_name.as_str());
        if let Some(ref unit_name) = self.unit_name {
            if !unit_name.is_empty() {
                sb.push('@');
                sb.push_str(unit_name.as_str());
            }
        }

        if self.enable_stream_request_type {
            sb.push('@');
            sb.push_str(RequestType::Stream.to_string().as_str());
        }

        sb
    }

    #[inline]
    pub fn get_namesrv_addr(&self) -> Option<CheetahString> {
        if StringUtils::is_not_empty_str(self.namesrv_addr.as_deref())
            && NAMESRV_ENDPOINT_PATTERN.is_match(self.namesrv_addr.as_ref().unwrap().as_str())
        {
            NameServerAddressUtils::get_name_srv_addr_from_namesrv_endpoint(
                self.namesrv_addr.as_ref().unwrap().as_str(),
            )
            .map(|addr| addr.into())
        } else {
            self.namesrv_addr.clone()
        }
    }

    // ============ Comprehensive Getters and Setters ============

    #[inline]
    pub fn get_client_ip(&self) -> Option<&CheetahString> {
        self.client_ip.as_ref()
    }

    #[inline]
    pub fn set_client_ip(&mut self, client_ip: CheetahString) {
        self.client_ip = Some(client_ip);
    }

    #[inline]
    pub fn get_instance_name(&self) -> &CheetahString {
        &self.instance_name
    }

    #[inline]
    pub fn get_client_callback_executor_threads(&self) -> usize {
        self.client_callback_executor_threads
    }

    #[inline]
    pub fn set_client_callback_executor_threads(&mut self, threads: usize) {
        self.client_callback_executor_threads = threads;
    }

    #[inline]
    pub fn get_namespace_v2(&self) -> Option<&CheetahString> {
        self.namespace_v2.as_ref()
    }

    #[inline]
    pub fn set_namespace_v2(&mut self, namespace_v2: CheetahString) {
        self.namespace_v2 = Some(namespace_v2);
    }

    #[inline]
    pub fn set_namespace(&mut self, namespace: CheetahString) {
        self.namespace = Some(namespace);
        self.namespace_initialized.store(true, Ordering::Release);
    }

    #[inline]
    pub fn get_access_channel(&self) -> AccessChannel {
        self.access_channel
    }

    #[inline]
    pub fn set_access_channel(&mut self, access_channel: AccessChannel) {
        self.access_channel = access_channel;
    }

    #[inline]
    pub fn get_poll_name_server_interval(&self) -> u32 {
        self.poll_name_server_interval
    }

    #[inline]
    pub fn set_poll_name_server_interval(&mut self, interval: u32) {
        self.poll_name_server_interval = interval;
    }

    #[inline]
    pub fn get_heartbeat_broker_interval(&self) -> u32 {
        self.heartbeat_broker_interval
    }

    #[inline]
    pub fn set_heartbeat_broker_interval(&mut self, interval: u32) {
        self.heartbeat_broker_interval = interval;
    }

    #[inline]
    pub fn get_persist_consumer_offset_interval(&self) -> u32 {
        self.persist_consumer_offset_interval
    }

    #[inline]
    pub fn set_persist_consumer_offset_interval(&mut self, interval: u32) {
        self.persist_consumer_offset_interval = interval;
    }

    #[inline]
    pub fn get_pull_time_delay_millis_when_exception(&self) -> u32 {
        self.pull_time_delay_millis_when_exception
    }

    #[inline]
    pub fn set_pull_time_delay_millis_when_exception(&mut self, delay: u32) {
        self.pull_time_delay_millis_when_exception = delay;
    }

    #[inline]
    pub fn get_unit_name(&self) -> Option<&CheetahString> {
        self.unit_name.as_ref()
    }

    #[inline]
    pub fn set_unit_name(&mut self, unit_name: CheetahString) {
        self.unit_name = Some(unit_name);
    }

    #[inline]
    pub fn is_unit_mode(&self) -> bool {
        self.unit_mode
    }

    #[inline]
    pub fn set_unit_mode(&mut self, unit_mode: bool) {
        self.unit_mode = unit_mode;
    }

    #[inline]
    pub fn is_decode_read_body(&self) -> bool {
        self.decode_read_body
    }

    #[inline]
    pub fn set_decode_read_body(&mut self, decode_read_body: bool) {
        self.decode_read_body = decode_read_body;
    }

    #[inline]
    pub fn is_decode_decompress_body(&self) -> bool {
        self.decode_decompress_body
    }

    #[inline]
    pub fn set_decode_decompress_body(&mut self, decode_decompress_body: bool) {
        self.decode_decompress_body = decode_decompress_body;
    }

    #[inline]
    pub fn is_vip_channel_enabled(&self) -> bool {
        self.vip_channel_enabled
    }

    #[inline]
    pub fn set_vip_channel_enabled(&mut self, enabled: bool) {
        self.vip_channel_enabled = enabled;
    }

    #[inline]
    pub fn is_use_heartbeat_v2(&self) -> bool {
        self.use_heartbeat_v2
    }

    #[inline]
    pub fn set_use_heartbeat_v2(&mut self, use_heartbeat_v2: bool) {
        self.use_heartbeat_v2 = use_heartbeat_v2;
    }

    #[inline]
    pub fn is_use_tls(&self) -> bool {
        self.use_tls
    }

    #[inline]
    pub fn set_use_tls(&mut self, use_tls: bool) {
        self.use_tls = use_tls;
    }

    #[inline]
    pub fn get_socks_proxy_config(&self) -> &CheetahString {
        &self.socks_proxy_config
    }

    #[inline]
    pub fn set_socks_proxy_config(&mut self, config: CheetahString) {
        self.socks_proxy_config = config;
    }

    #[inline]
    pub fn get_language(&self) -> LanguageCode {
        self.language
    }

    #[inline]
    pub fn set_language(&mut self, language: LanguageCode) {
        self.language = language;
    }

    #[inline]
    pub fn get_mq_client_api_timeout(&self) -> u64 {
        self.mq_client_api_timeout
    }

    #[inline]
    pub fn set_mq_client_api_timeout(&mut self, timeout: u64) {
        self.mq_client_api_timeout = timeout;
    }

    #[inline]
    pub fn get_detect_timeout(&self) -> u32 {
        self.detect_timeout
    }

    #[inline]
    pub fn set_detect_timeout(&mut self, timeout: u32) {
        self.detect_timeout = timeout;
    }

    #[inline]
    pub fn get_detect_interval(&self) -> u32 {
        self.detect_interval
    }

    #[inline]
    pub fn set_detect_interval(&mut self, interval: u32) {
        self.detect_interval = interval;
    }

    #[inline]
    pub fn is_enable_stream_request_type(&self) -> bool {
        self.enable_stream_request_type
    }

    #[inline]
    pub fn set_enable_stream_request_type(&mut self, enabled: bool) {
        self.enable_stream_request_type = enabled;
    }

    #[inline]
    pub fn is_send_latency_enable(&self) -> bool {
        self.send_latency_enable
    }

    #[inline]
    pub fn set_send_latency_enable(&mut self, enabled: bool) {
        self.send_latency_enable = enabled;
    }

    #[inline]
    pub fn is_start_detector_enable(&self) -> bool {
        self.start_detector_enable
    }

    #[inline]
    pub fn set_start_detector_enable(&mut self, enabled: bool) {
        self.start_detector_enable = enabled;
    }

    #[inline]
    pub fn is_enable_heartbeat_channel_event_listener(&self) -> bool {
        self.enable_heartbeat_channel_event_listener
    }

    #[inline]
    pub fn set_enable_heartbeat_channel_event_listener(&mut self, enabled: bool) {
        self.enable_heartbeat_channel_event_listener = enabled;
    }

    #[inline]
    pub fn is_enable_trace(&self) -> bool {
        self.enable_trace
    }

    #[inline]
    pub fn set_enable_trace(&mut self, enabled: bool) {
        self.enable_trace = enabled;
    }

    #[inline]
    pub fn get_trace_topic(&self) -> Option<&CheetahString> {
        self.trace_topic.as_ref()
    }

    #[inline]
    pub fn set_trace_topic(&mut self, topic: CheetahString) {
        self.trace_topic = Some(topic);
    }

    #[inline]
    pub fn get_trace_msg_batch_num(&self) -> usize {
        self.trace_msg_batch_num
    }

    #[inline]
    pub fn set_trace_msg_batch_num(&mut self, num: usize) {
        self.trace_msg_batch_num = num;
    }

    #[inline]
    pub fn get_max_page_size_in_get_metadata(&self) -> usize {
        self.max_page_size_in_get_metadata
    }

    #[inline]
    pub fn set_max_page_size_in_get_metadata(&mut self, size: usize) {
        self.max_page_size_in_get_metadata = size;
    }

    // ============ Utility Methods ============

    /// Clones the configuration
    #[inline]
    pub fn clone_client_config(&self) -> Self {
        self.clone()
    }

    /// Resets client config from another instance
    pub fn reset_client_config(&mut self, other: &ClientConfig) {
        self.namesrv_addr = other.namesrv_addr.clone();
        self.client_ip = other.client_ip.clone();
        self.instance_name = other.instance_name.clone();
        self.client_callback_executor_threads = other.client_callback_executor_threads;
        self.namespace = other.namespace.clone();
        self.namespace_v2 = other.namespace_v2.clone();
        self.access_channel = other.access_channel;
        self.poll_name_server_interval = other.poll_name_server_interval;
        self.heartbeat_broker_interval = other.heartbeat_broker_interval;
        self.persist_consumer_offset_interval = other.persist_consumer_offset_interval;
        self.pull_time_delay_millis_when_exception = other.pull_time_delay_millis_when_exception;
        self.unit_mode = other.unit_mode;
        self.unit_name = other.unit_name.clone();
        self.decode_read_body = other.decode_read_body;
        self.decode_decompress_body = other.decode_decompress_body;
        self.vip_channel_enabled = other.vip_channel_enabled;
        self.use_heartbeat_v2 = other.use_heartbeat_v2;
        self.use_tls = other.use_tls;
        self.socks_proxy_config = other.socks_proxy_config.clone();
        self.language = other.language;
        self.mq_client_api_timeout = other.mq_client_api_timeout;
        self.detect_timeout = other.detect_timeout;
        self.detect_interval = other.detect_interval;
        self.enable_stream_request_type = other.enable_stream_request_type;
        self.send_latency_enable = other.send_latency_enable;
        self.start_detector_enable = other.start_detector_enable;
        self.enable_heartbeat_channel_event_listener = other.enable_heartbeat_channel_event_listener;
        self.enable_trace = other.enable_trace;
        self.trace_topic = other.trace_topic.clone();
        self.trace_msg_batch_num = other.trace_msg_batch_num;
        self.max_page_size_in_get_metadata = other.max_page_size_in_get_metadata;
    }

    /// Deprecated: Use with_namespace instead
    #[inline]
    #[deprecated(note = "Use with_namespace for namespace wrapping")]
    pub fn without_namespace(&mut self, resource: &str) -> CheetahString {
        if let Some(namespace) = self.get_namespace().as_deref() {
            NamespaceUtil::without_namespace_with_namespace(resource, namespace).into()
        } else {
            NamespaceUtil::without_namespace(resource).into()
        }
    }
}

impl std::fmt::Display for ClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ClientConfig {{ namesrv_addr: {:?}, client_ip: {:?}, instance_name: {}, \
             client_callback_executor_threads: {}, namespace: {:?}, namespace_v2: {:?}, access_channel: {:?}, \
             poll_name_server_interval: {}, heartbeat_broker_interval: {}, persist_consumer_offset_interval: {}, \
             pull_time_delay_millis_when_exception: {}, unit_mode: {}, unit_name: {:?}, decode_read_body: {}, \
             decode_decompress_body: {}, vip_channel_enabled: {}, use_heartbeat_v2: {}, use_tls: {}, \
             socks_proxy_config: {}, mq_client_api_timeout: {}, detect_timeout: {}, detect_interval: {}, language: \
             {:?}, enable_stream_request_type: {}, send_latency_enable: {}, start_detector_enable: {}, \
             enable_heartbeat_channel_event_listener: {}, enable_trace: {}, trace_topic: {:?}, trace_msg_batch_num: \
             {}, max_page_size_in_get_metadata: {} }}",
            self.namesrv_addr,
            self.client_ip,
            self.instance_name,
            self.client_callback_executor_threads,
            self.namespace,
            self.namespace_v2,
            self.access_channel,
            self.poll_name_server_interval,
            self.heartbeat_broker_interval,
            self.persist_consumer_offset_interval,
            self.pull_time_delay_millis_when_exception,
            self.unit_mode,
            self.unit_name,
            self.decode_read_body,
            self.decode_decompress_body,
            self.vip_channel_enabled,
            self.use_heartbeat_v2,
            self.use_tls,
            self.socks_proxy_config,
            self.mq_client_api_timeout,
            self.detect_timeout,
            self.detect_interval,
            self.language,
            self.enable_stream_request_type,
            self.send_latency_enable,
            self.start_detector_enable,
            self.enable_heartbeat_channel_event_listener,
            self.enable_trace,
            self.trace_topic,
            self.trace_msg_batch_num,
            self.max_page_size_in_get_metadata
        )
    }
}
