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

pub const POOLED_BYTE_BUF_ALLOCATOR_ENABLE: &str = "com.rocketmq.rocketmq-remoting.nettyPooledByteBufAllocatorEnable";
pub const SOCKET_SNDBUF_SIZE: &str = "com.rocketmq.rocketmq-remoting.socket.sndbuf.size";
pub const SOCKET_RCVBUF_SIZE: &str = "com.rocketmq.rocketmq-remoting.socket.rcvbuf.size";
pub const SOCKET_BACKLOG: &str = "com.rocketmq.rocketmq-remoting.socket.backlog";
pub const CLIENT_ASYNC_SEMAPHORE_VALUE: &str = "com.rocketmq.rocketmq-remoting.clientAsyncSemaphoreValue";
pub const CLIENT_ONEWAY_SEMAPHORE_VALUE: &str = "com.rocketmq.rocketmq-remoting.clientOnewaySemaphoreValue";
pub const CLIENT_WORKER_SIZE: &str = "com.rocketmq.rocketmq-remoting.client.worker.size";
pub const CLIENT_CONNECT_TIMEOUT: &str = "com.rocketmq.rocketmq-remoting.client.connect.timeout";
pub const CLIENT_CHANNEL_MAX_IDLE_SECONDS: &str = "com.rocketmq.rocketmq-remoting.client.channel.maxIdleTimeSeconds";
pub const CLIENT_CLOSE_SOCKET_IF_TIMEOUT: &str = "com.rocketmq.rocketmq-remoting.client.closeSocketIfTimeout";
pub const WRITE_BUFFER_HIGH_WATER_MARK_VALUE: &str = "com.rocketmq.rocketmq-remoting.write.buffer.high.water.mark";
pub const WRITE_BUFFER_LOW_WATER_MARK: &str = "com.rocketmq.rocketmq-remoting.write.buffer.low.water.mark";

pub(crate) struct NetSystemConfig {
    pub(crate) pooled_byte_buf_allocator_enable: bool,
    pub(crate) socket_sndbuf_size: i32,
    pub(crate) socket_rcvbuf_size: i32,
    pub(crate) socket_backlog: i32,
    pub(crate) client_async_semaphore_value: i32,
    pub(crate) client_oneway_semaphore_value: i32,
    pub(crate) client_worker_size: i32,
    pub(crate) connect_timeout_millis: i32,
    pub(crate) client_channel_max_idle_seconds: i32,
    pub(crate) client_close_socket_if_timeout: bool,
    pub(crate) write_buffer_high_water_mark_value: i32,
    pub(crate) write_buffer_low_water_mark: i32,
}

impl NetSystemConfig {
    pub(crate) fn new() -> NetSystemConfig {
        NetSystemConfig {
            pooled_byte_buf_allocator_enable: env::var(POOLED_BYTE_BUF_ALLOCATOR_ENABLE)
                .unwrap_or_else(|_| "false".to_string())
                .parse()
                .unwrap_or(false),
            socket_sndbuf_size: env::var(SOCKET_SNDBUF_SIZE)
                .unwrap_or_else(|_| "0".to_string())
                .parse()
                .unwrap_or(0),
            socket_rcvbuf_size: env::var(SOCKET_RCVBUF_SIZE)
                .unwrap_or_else(|_| "0".to_string())
                .parse()
                .unwrap_or(0),
            socket_backlog: env::var(SOCKET_BACKLOG)
                .unwrap_or_else(|_| "0".to_string())
                .parse()
                .unwrap_or(0),
            client_async_semaphore_value: env::var(CLIENT_ASYNC_SEMAPHORE_VALUE)
                .unwrap_or_else(|_| "65535".to_string())
                .parse()
                .unwrap_or(0),
            client_oneway_semaphore_value: env::var(CLIENT_ONEWAY_SEMAPHORE_VALUE)
                .unwrap_or_else(|_| "65535".to_string())
                .parse()
                .unwrap_or(0),
            client_worker_size: env::var(CLIENT_WORKER_SIZE)
                .unwrap_or_else(|_| "4".to_string())
                .parse()
                .unwrap_or(0),
            connect_timeout_millis: env::var(CLIENT_CONNECT_TIMEOUT)
                .unwrap_or_else(|_| "3000".to_string())
                .parse()
                .unwrap_or(0),
            client_channel_max_idle_seconds: env::var(CLIENT_CHANNEL_MAX_IDLE_SECONDS)
                .unwrap_or_else(|_| "120".to_string())
                .parse()
                .unwrap_or(0),
            client_close_socket_if_timeout: env::var(CLIENT_CLOSE_SOCKET_IF_TIMEOUT)
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),
            write_buffer_high_water_mark_value: env::var(WRITE_BUFFER_HIGH_WATER_MARK_VALUE)
                .unwrap_or_else(|_| "0".to_string())
                .parse()
                .unwrap_or(0),
            write_buffer_low_water_mark: env::var(WRITE_BUFFER_LOW_WATER_MARK)
                .unwrap_or_else(|_| "0".to_string())
                .parse()
                .unwrap_or(0),
        }
    }
}
