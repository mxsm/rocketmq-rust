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
            send_latency_enable: env::var(Self::SEND_LATENCY_ENABLE).unwrap_or_else(|_| "false".to_string()) == "false",
            start_detector_enable: env::var(Self::START_DETECTOR_ENABLE).unwrap_or_else(|_| "false".to_string())
                == "false",
            enable_heartbeat_channel_event_listener: true,
            enable_trace: false,
            trace_topic: None,
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
        let mut sb = String::new();
        sb.push_str(self.client_ip.as_ref().unwrap());

        sb.push('@');
        sb.push_str(self.instance_name.as_str());
        if let Some(unit_name) = &self.unit_name {
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
}
