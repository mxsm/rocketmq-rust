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

use crate::runtime::config::net_system_config::NetSystemConfig;

#[derive(Debug, Clone)]
struct NettyServerConfig {
    bind_address: String,
    listen_port: i32,
    server_worker_threads: i32,
    server_callback_executor_threads: i32,
    server_selector_threads: i32,
    server_oneway_semaphore_value: i32,
    server_async_semaphore_value: i32,
    server_channel_max_idle_time_seconds: i32,
    server_socket_snd_buf_size: i32,
    server_socket_rcv_buf_size: i32,
    write_buffer_high_water_mark: i32,
    write_buffer_low_water_mark: i32,
    server_socket_backlog: i32,
    server_pooled_byte_buf_allocator_enable: bool,
    enable_shutdown_gracefully: bool,
    shutdown_wait_time_seconds: i32,
    use_epoll_native_selector: bool,
}

impl NettyServerConfig {
    fn new() -> NettyServerConfig {
        Self::default()
    }
}

impl Default for NettyServerConfig {
    fn default() -> Self {
        let config = NetSystemConfig::new();
        NettyServerConfig {
            bind_address: "0.0.0.0".to_string(),
            listen_port: 0,
            server_worker_threads: 8,
            server_callback_executor_threads: 0,
            server_selector_threads: 3,
            server_oneway_semaphore_value: 256,
            server_async_semaphore_value: 64,
            server_channel_max_idle_time_seconds: 120,
            server_socket_snd_buf_size: config.socket_sndbuf_size,
            server_socket_rcv_buf_size: config.socket_rcvbuf_size,
            write_buffer_high_water_mark: config.write_buffer_high_water_mark_value,
            write_buffer_low_water_mark: config.write_buffer_low_water_mark,
            server_socket_backlog: config.socket_backlog,
            server_pooled_byte_buf_allocator_enable: true,
            enable_shutdown_gracefully: false,
            shutdown_wait_time_seconds: 30,
            use_epoll_native_selector: false,
        }
    }
}

impl NettyServerConfig {
    pub fn set_bind_address(&mut self, bind_address: String) {
        self.bind_address = bind_address;
    }

    pub fn set_listen_port(&mut self, listen_port: i32) {
        self.listen_port = listen_port;
    }

    pub fn set_server_worker_threads(&mut self, server_worker_threads: i32) {
        self.server_worker_threads = server_worker_threads;
    }

    pub fn set_server_callback_executor_threads(&mut self, server_callback_executor_threads: i32) {
        self.server_callback_executor_threads = server_callback_executor_threads;
    }

    pub fn set_server_selector_threads(&mut self, server_selector_threads: i32) {
        self.server_selector_threads = server_selector_threads;
    }

    pub fn set_server_oneway_semaphore_value(&mut self, server_oneway_semaphore_value: i32) {
        self.server_oneway_semaphore_value = server_oneway_semaphore_value;
    }

    pub fn set_server_async_semaphore_value(&mut self, server_async_semaphore_value: i32) {
        self.server_async_semaphore_value = server_async_semaphore_value;
    }

    pub fn set_server_channel_max_idle_time_seconds(&mut self, server_channel_max_idle_time_seconds: i32) {
        self.server_channel_max_idle_time_seconds = server_channel_max_idle_time_seconds;
    }

    pub fn set_server_socket_snd_buf_size(&mut self, server_socket_snd_buf_size: i32) {
        self.server_socket_snd_buf_size = server_socket_snd_buf_size;
    }

    pub fn set_server_socket_rcv_buf_size(&mut self, server_socket_rcv_buf_size: i32) {
        self.server_socket_rcv_buf_size = server_socket_rcv_buf_size;
    }

    pub fn set_write_buffer_high_water_mark(&mut self, write_buffer_high_water_mark: i32) {
        self.write_buffer_high_water_mark = write_buffer_high_water_mark;
    }

    pub fn set_write_buffer_low_water_mark(&mut self, write_buffer_low_water_mark: i32) {
        self.write_buffer_low_water_mark = write_buffer_low_water_mark;
    }

    pub fn set_server_socket_backlog(&mut self, server_socket_backlog: i32) {
        self.server_socket_backlog = server_socket_backlog;
    }

    pub fn set_server_pooled_byte_buf_allocator_enable(&mut self, server_pooled_byte_buf_allocator_enable: bool) {
        self.server_pooled_byte_buf_allocator_enable = server_pooled_byte_buf_allocator_enable;
    }

    pub fn set_enable_shutdown_gracefully(&mut self, enable_shutdown_gracefully: bool) {
        self.enable_shutdown_gracefully = enable_shutdown_gracefully;
    }

    pub fn set_shutdown_wait_time_seconds(&mut self, shutdown_wait_time_seconds: i32) {
        self.shutdown_wait_time_seconds = shutdown_wait_time_seconds;
    }

    pub fn set_use_epoll_native_selector(&mut self, use_epoll_native_selector: bool) {
        self.use_epoll_native_selector = use_epoll_native_selector;
    }
}

impl NettyServerConfig {
    pub fn bind_address(&self) -> &str {
        &self.bind_address
    }

    pub fn listen_port(&self) -> i32 {
        self.listen_port
    }

    pub fn server_worker_threads(&self) -> i32 {
        self.server_worker_threads
    }

    pub fn server_callback_executor_threads(&self) -> i32 {
        self.server_callback_executor_threads
    }

    pub fn server_selector_threads(&self) -> i32 {
        self.server_selector_threads
    }

    pub fn server_oneway_semaphore_value(&self) -> i32 {
        self.server_oneway_semaphore_value
    }

    pub fn server_async_semaphore_value(&self) -> i32 {
        self.server_async_semaphore_value
    }

    pub fn server_channel_max_idle_time_seconds(&self) -> i32 {
        self.server_channel_max_idle_time_seconds
    }

    pub fn server_socket_snd_buf_size(&self) -> i32 {
        self.server_socket_snd_buf_size
    }

    pub fn server_socket_rcv_buf_size(&self) -> i32 {
        self.server_socket_rcv_buf_size
    }

    pub fn write_buffer_high_water_mark(&self) -> i32 {
        self.write_buffer_high_water_mark
    }

    pub fn write_buffer_low_water_mark(&self) -> i32 {
        self.write_buffer_low_water_mark
    }

    pub fn server_socket_backlog(&self) -> i32 {
        self.server_socket_backlog
    }

    pub fn server_pooled_byte_buf_allocator_enable(&self) -> bool {
        self.server_pooled_byte_buf_allocator_enable
    }

    pub fn enable_shutdown_gracefully(&self) -> bool {
        self.enable_shutdown_gracefully
    }

    pub fn shutdown_wait_time_seconds(&self) -> i32 {
        self.shutdown_wait_time_seconds
    }

    pub fn use_epoll_native_selector(&self) -> bool {
        self.use_epoll_native_selector
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_netty_server_config_default_values() {
        let config = NettyServerConfig::default();

        assert_eq!("0.0.0.0", config.bind_address());
        assert_eq!(0, config.listen_port());
        assert_eq!(8, config.server_worker_threads());
    }

    #[test]
    fn test_netty_server_config_setters_and_getters() {
        let mut config = NettyServerConfig::new();

        config.set_bind_address("127.0.0.1".to_string());
        assert_eq!("127.0.0.1", config.bind_address());

        config.set_listen_port(8080);
        assert_eq!(8080, config.listen_port());
    }
}
