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

use once_cell::sync::Lazy;

pub(crate) mod consume_message_concurrently_service;
pub(crate) mod consume_message_orderly_service;
pub(crate) mod consume_message_pop_concurrently_service;
pub(crate) mod consume_message_pop_orderly_service;
pub(crate) mod consume_message_service;
pub(crate) mod default_mq_push_consumer_impl;
pub(crate) mod message_request;
pub(crate) mod pop_process_queue;
pub(crate) mod pop_request;
pub mod process_queue;
pub(crate) mod pull_api_wrapper;
pub mod pull_message_service;
pub mod pull_request;
pub(crate) mod pull_request_ext;
pub(crate) mod re_balance;

pub(crate) static PULL_MAX_IDLE_TIME: Lazy<u64> = Lazy::new(|| {
    std::env::var("rocketmq.client.pull.pullMaxIdleTime")
        .unwrap_or_else(|_| "120000".into())
        .parse()
        .unwrap_or(120000)
});
