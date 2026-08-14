// Copyright 2026 The RocketMQ Rust Authors
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

use rocketmq_client_rust::ClientConfig;

#[test]
fn runtime_concurrency_and_metadata_limits_are_preserved() {
    let config = ClientConfig::builder()
        .client_callback_executor_threads(2)
        .enable_concurrent_heartbeat(true)
        .concurrent_heartbeat_thread_pool_size(2)
        .enable_heartbeat_channel_event_listener(false)
        .max_page_size_in_get_metadata(37)
        .build()
        .expect("valid runtime settings");

    assert_eq!(config.client_callback_executor_threads, 2);
    assert!(config.enable_concurrent_heartbeat);
    assert_eq!(config.concurrent_heartbeat_thread_pool_size, 2);
    assert!(!config.enable_heartbeat_channel_event_listener);
    assert_eq!(config.max_page_size_in_get_metadata, 37);
}

#[test]
fn zero_callback_concurrency_is_rejected() {
    let error = match ClientConfig::builder().client_callback_executor_threads(0).build() {
        Ok(_) => panic!("zero callback concurrency must fail"),
        Err(error) => error,
    };

    assert!(error.to_string().contains("client_callback_executor_threads"));
}

#[test]
fn enabled_concurrent_heartbeat_rejects_zero_capacity() {
    let error = match ClientConfig::builder()
        .enable_concurrent_heartbeat(true)
        .concurrent_heartbeat_thread_pool_size(0)
        .build()
    {
        Ok(_) => panic!("zero heartbeat concurrency must fail"),
        Err(error) => error,
    };

    assert!(error.to_string().contains("concurrent_heartbeat_thread_pool_size"));
}

#[test]
fn zero_metadata_page_size_is_rejected() {
    let error = match ClientConfig::builder().max_page_size_in_get_metadata(0).build() {
        Ok(_) => panic!("zero metadata page size must fail"),
        Err(error) => error,
    };

    assert!(error.to_string().contains("max_page_size_in_get_metadata"));
}
