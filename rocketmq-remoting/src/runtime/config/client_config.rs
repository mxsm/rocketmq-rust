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

use std::sync::LazyLock;

use crate::runtime::config::net_system_config::NetSystemConfig;

static NET_SYSTEM_CONFIG: LazyLock<NetSystemConfig> = LazyLock::new(NetSystemConfig::new);

#[derive(Clone)]
pub struct TokioClientConfig {
    // Worker thread number
    pub client_worker_threads: i32,
    pub client_callback_executor_threads: usize,
    pub client_oneway_semaphore_value: i32,
    pub client_async_semaphore_value: i32,
    pub connect_timeout_millis: i32,
    pub channel_not_active_interval: i64,
    pub client_channel_max_idle_time_seconds: i32,
    pub client_socket_snd_buf_size: i32,
    pub client_socket_rcv_buf_size: i32,
    pub client_pooled_byte_buf_allocator_enable: bool,
    pub client_close_socket_if_timeout: bool,
    //pub use_tls: bool,
    pub socks_proxy_config: String,
    pub write_buffer_high_water_mark: i32,
    pub write_buffer_low_water_mark: i32,
    pub disable_callback_executor: bool,
    pub disable_netty_worker_group: bool,
    pub max_reconnect_interval_time_seconds: i64,
    pub enable_reconnect_for_go_away: bool,
    pub enable_transparent_retry: bool,
}

impl Default for TokioClientConfig {
    fn default() -> Self {
        TokioClientConfig {
            client_worker_threads: NET_SYSTEM_CONFIG.client_worker_size,
            client_callback_executor_threads: num_cpus::get(),
            client_oneway_semaphore_value: NET_SYSTEM_CONFIG.client_oneway_semaphore_value,
            client_async_semaphore_value: NET_SYSTEM_CONFIG.client_async_semaphore_value,
            connect_timeout_millis: NET_SYSTEM_CONFIG.connect_timeout_millis,
            channel_not_active_interval: 1000 * 60,
            client_channel_max_idle_time_seconds: NET_SYSTEM_CONFIG.client_channel_max_idle_seconds,
            client_socket_snd_buf_size: NET_SYSTEM_CONFIG.socket_sndbuf_size,
            client_socket_rcv_buf_size: NET_SYSTEM_CONFIG.socket_rcvbuf_size,
            client_pooled_byte_buf_allocator_enable: false,
            client_close_socket_if_timeout: NET_SYSTEM_CONFIG.client_close_socket_if_timeout,
            //use_tls: TlsSystemConfig::is_tls_enabled(),
            socks_proxy_config: "{}".to_string(),
            write_buffer_high_water_mark: NET_SYSTEM_CONFIG.write_buffer_high_water_mark_value,
            write_buffer_low_water_mark: NET_SYSTEM_CONFIG.write_buffer_low_water_mark,
            disable_callback_executor: false,
            disable_netty_worker_group: false,
            max_reconnect_interval_time_seconds: 60,
            enable_reconnect_for_go_away: true,
            enable_transparent_retry: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let default_config = TokioClientConfig::default();

        assert_eq!(
            default_config.client_worker_threads,
            NET_SYSTEM_CONFIG.client_worker_size
        );
        assert_eq!(default_config.client_callback_executor_threads, num_cpus::get());
        assert_eq!(
            default_config.client_oneway_semaphore_value,
            NET_SYSTEM_CONFIG.client_oneway_semaphore_value
        );
    }
}
