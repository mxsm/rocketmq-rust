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

use std::collections::VecDeque;
use std::sync::Arc;

use parking_lot::Mutex;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_store::filter::MessageFilter;
use tracing::warn;

pub const NO_SUSPEND_KEY: &str = "_noSuspend_";

/// Represents a suspended pull request for cold data flow control
#[allow(dead_code)]
pub struct ColdDataPullRequest {
    request: RemotingCommand,
    channel: Channel,
    suspend_timestamp: u64,
    timeout_millis: u64,
    queue_offset: i64,
    subscription_data: SubscriptionData,
    message_filter: Arc<Box<dyn MessageFilter>>,
}

impl ColdDataPullRequest {
    pub fn new(
        request: RemotingCommand,
        channel: Channel,
        timeout_millis: u64,
        suspend_timestamp: u64,
        queue_offset: i64,
        subscription_data: SubscriptionData,
        message_filter: Arc<Box<dyn MessageFilter>>,
    ) -> Self {
        Self {
            request,
            channel,
            suspend_timestamp,
            timeout_millis,
            queue_offset,
            subscription_data,
            message_filter,
        }
    }
}

#[derive(Default)]
pub struct ColdDataPullRequestHoldService {
    /// Queue of suspended cold data pull requests
    pull_request_queue: Mutex<VecDeque<ColdDataPullRequest>>,
}

impl ColdDataPullRequestHoldService {
    pub fn start(&mut self) {
        warn!("ColdDataPullRequestHoldService started not implemented");
    }

    /// Suspend a cold data read request for flow control
    ///
    /// When a consumer is reading cold data and the consume type is CONSUME_ACTIVELY (PULL),
    /// the request will be suspended and processed later with limited throughput.
    pub fn suspend_cold_data_read_request(&self, pull_request: ColdDataPullRequest) {
        let mut queue = self.pull_request_queue.lock();
        queue.push_back(pull_request);
        // TODO: Implement actual processing logic that wakes up suspended requests
        // after a delay and processes them with rate limiting
    }

    pub fn shutdown(&mut self) {
        warn!("ColdDataPullRequestHoldService shutdown not implemented");
    }
}
