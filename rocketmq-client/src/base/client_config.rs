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
use std::env;
use std::time::Duration;

use rocketmq_common::utils::name_server_address_utils::NameServerAddressUtils;
use rocketmq_common::utils::network_util::NetworkUtil;
use rocketmq_remoting::protocol::LanguageCode;

use crate::base::access_channel::AccessChannel;

pub const SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY: &str = "com.rocketmq.sendMessageWithVIPChannel";
pub const SOCKS_PROXY_CONFIG: &str = "com.rocketmq.socks.proxy.config";
pub const DECODE_READ_BODY: &str = "com.rocketmq.read.body";
pub const DECODE_DECOMPRESS_BODY: &str = "com.rocketmq.decompress.body";
pub const SEND_LATENCY_ENABLE: &str = "com.rocketmq.sendLatencyEnable";
pub const START_DETECTOR_ENABLE: &str = "com.rocketmq.startDetectorEnable";
pub const HEART_BEAT_V2: &str = "com.rocketmq.heartbeat.v2";

pub struct ClientConfig {
    pub namesrv_addr: Option<String>,
    pub client_ip: Option<String>,
    pub instance_name: String,
    pub client_callback_executor_threads: usize,
    pub namespace: Option<String>,
    pub namespace_v2: Option<String>,
    pub access_channel: AccessChannel,
    pub poll_name_server_interval: u32,
    pub heartbeat_broker_interval: u32,
    pub persist_consumer_offset_interval: u32,
    pub pull_time_delay_millis_when_exception: u32,
    pub unit_mode: bool,
    pub unit_name: Option<String>,
    pub decode_read_body: bool,
    pub decode_decompress_body: bool,
    pub vip_channel_enabled: bool,
    pub use_heartbeat_v2: bool,
    pub use_tls: bool,
    pub socks_proxy_config: String,
    pub mq_client_api_timeout: u32,
    pub detect_timeout: u32,
    pub detect_interval: u32,
    pub language: LanguageCode,
    pub enable_stream_request_type: bool,
    pub send_latency_enable: bool,
    pub start_detector_enable: bool,
    pub enable_heartbeat_channel_event_listener: bool,
    pub enable_trace: bool,
    pub trace_topic: Option<String>,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientConfig {
    pub fn new() -> Self {
        ClientConfig {
            namesrv_addr: NameServerAddressUtils::get_name_server_addresses(),
            client_ip: NetworkUtil::get_local_address(),
            instance_name: env::var("rocketmq.client.name")
                .unwrap_or_else(|_| "DEFAULT".to_string()),
            client_callback_executor_threads: num_cpus::get(),
            namespace: None,
            namespace_v2: None,
            access_channel: AccessChannel::Local,
            poll_name_server_interval: Duration::from_secs(30).as_millis() as u32,
            heartbeat_broker_interval: Duration::from_secs(30).as_millis() as u32,
            persist_consumer_offset_interval: Duration::from_secs(5).as_millis() as u32,
            pull_time_delay_millis_when_exception: 1000,
            unit_mode: false,
            unit_name: None,
            decode_read_body: env::var(DECODE_READ_BODY).unwrap_or_else(|_| "true".to_string())
                == "true",
            decode_decompress_body: env::var(DECODE_DECOMPRESS_BODY)
                .unwrap_or_else(|_| "true".to_string())
                == "true",
            vip_channel_enabled: env::var(SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY)
                .unwrap_or_else(|_| "false".to_string())
                == "false",
            use_heartbeat_v2: env::var(HEART_BEAT_V2).unwrap_or_else(|_| "false".to_string())
                == "false",
            use_tls: false,
            socks_proxy_config: env::var(SOCKS_PROXY_CONFIG).unwrap_or_else(|_| "{}".to_string()),
            mq_client_api_timeout: Duration::from_secs(3).as_millis() as u32,
            detect_timeout: 200,
            detect_interval: Duration::from_secs(2).as_millis() as u32,
            language: LanguageCode::JAVA,
            enable_stream_request_type: false,
            send_latency_enable: env::var(SEND_LATENCY_ENABLE)
                .unwrap_or_else(|_| "false".to_string())
                == "false",
            start_detector_enable: env::var(START_DETECTOR_ENABLE)
                .unwrap_or_else(|_| "false".to_string())
                == "false",
            enable_heartbeat_channel_event_listener: true,
            enable_trace: false,
            trace_topic: None,
        }
    }
}
