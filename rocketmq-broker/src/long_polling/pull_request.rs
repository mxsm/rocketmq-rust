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

use std::sync::Arc;

use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_store::filter::MessageFilter;

#[derive(Clone)]
pub struct PullRequest {
    request_command: RemotingCommand,
    client_channel: Channel,
    ctx: ConnectionHandlerContext,
    timeout_millis: u64,
    suspend_timestamp: u64,
    pull_from_this_offset: i64,
    subscription_data: SubscriptionData,
    message_filter: Arc<Box<dyn MessageFilter>>,
}

impl PullRequest {
    pub fn new(
        request_command: RemotingCommand,
        client_channel: Channel,
        ctx: ConnectionHandlerContext,
        timeout_millis: u64,
        suspend_timestamp: u64,
        pull_from_this_offset: i64,
        subscription_data: SubscriptionData,
        message_filter: Arc<Box<dyn MessageFilter>>,
    ) -> Self {
        Self {
            request_command,
            client_channel,
            ctx,
            timeout_millis,
            suspend_timestamp,
            pull_from_this_offset,
            subscription_data,
            message_filter,
        }
    }

    pub fn request_command(&self) -> &RemotingCommand {
        &self.request_command
    }

    pub fn request_command_mut(&mut self) -> &mut RemotingCommand {
        &mut self.request_command
    }

    pub fn client_channel(&self) -> &Channel {
        &self.client_channel
    }

    pub fn pull_from_this_offset(&self) -> i64 {
        self.pull_from_this_offset
    }

    pub fn subscription_data(&self) -> &SubscriptionData {
        &self.subscription_data
    }

    pub fn message_filter(&self) -> Arc<Box<dyn MessageFilter>> {
        self.message_filter.clone()
    }

    pub fn timeout_millis(&self) -> u64 {
        self.timeout_millis
    }
    pub fn suspend_timestamp(&self) -> u64 {
        self.suspend_timestamp
    }

    pub fn connection_handler_context(&self) -> &ConnectionHandlerContext {
        &self.ctx
    }
}
