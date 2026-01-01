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

use std::any::Any;
use std::sync::Arc;

use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::header::pull_message_request_header::PullMessageRequestHeader;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_context::TopicQueueMappingContext;
use rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_store::base::get_message_result::GetMessageResult;
use rocketmq_store::filter::MessageFilter;

/// Trait defining the behavior for handling the result of a pull message request.
///
/// This trait is designed to be implemented by types that handle the result of a pull message
/// request in a RocketMQ broker. It provides a method for processing the result of a message
/// retrieval operation, along with various parameters related to the request and the broker's
/// state.
pub trait PullMessageResultHandler: Sync + Send + Any + 'static {
    /// Handles the result of a pull message request.
    ///
    /// This method processes the result of a message retrieval operation (`get_message_result`),
    /// using the provided request information, channel, context, subscription data, and other
    /// parameters to generate an appropriate response.
    ///
    /// # Parameters
    /// - `get_message_result`: The result of the message retrieval operation.
    /// - `request`: The original remoting command representing the pull message request.
    /// - `request_header`: The header of the pull message request, containing request-specific
    ///   information.
    /// - `channel`: The channel through which the request was received.
    /// - `ctx`: The connection handler context associated with the request.
    /// - `subscription_data`: Subscription data for the consumer making the request.
    /// - `subscription_group_config`: Configuration for the subscription group of the consumer.
    /// - `broker_allow_suspend`: Flag indicating whether the broker allows suspending the request.
    /// - `message_filter`: The message filter to apply to the retrieved messages.
    /// - `response`: The initial response remoting command to be potentially modified and returned.
    /// - `mapping_context`: Context for topic-queue mapping.
    /// - `begin_time_mills`: The timestamp (in milliseconds) when the request began processing.
    ///
    /// # Returns
    /// An optional `RemotingCommand` representing the response to the pull message request.
    /// If `None`, it indicates that no response should be sent back to the client.
    async fn handle(
        &self,
        get_message_result: GetMessageResult,
        request: &mut RemotingCommand,
        request_header: PullMessageRequestHeader,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        subscription_data: SubscriptionData,
        subscription_group_config: &SubscriptionGroupConfig,
        broker_allow_suspend: bool,
        message_filter: Arc<Box<dyn MessageFilter>>,
        response: RemotingCommand,
        mapping_context: TopicQueueMappingContext,
        begin_time_mills: u64,
    ) -> Option<RemotingCommand>;

    /// Returns a mutable reference to `self` as a trait object of type `Any`.
    ///
    /// This method is useful for downcasting the trait object to its concrete type.
    fn as_any_mut(&mut self) -> &mut dyn Any;

    /// Returns a reference to `self` as a trait object of type `Any`.
    ///
    /// This method is useful for downcasting the trait object to its concrete type.
    fn as_any(&self) -> &dyn Any;
}
