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
use std::net::SocketAddr;

use cheetah_string::CheetahString;
use rocketmq_protocol::protocol::header::pull_message_request_header::PullMessageRequestHeader;
use rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::static_topic::topic_queue_mapping_context::TopicQueueMappingContext;
use rocketmq_protocol::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use rocketmq_store::ArcMessageFilter;
use rocketmq_store::GetMessageResult;

use crate::long_polling::pull_deferred::PullHookMetadata;
use crate::long_polling::pull_deferred::PullSuspendTiming;
use crate::processor::response_assembly::BrokerResponseParts;

pub(crate) type PullBroadcastClientResolver<'a> =
    dyn Fn(&PullMessageRequestHeader) -> rocketmq_error::RocketMQResult<Option<CheetahString>> + Send + Sync + 'a;

/// Channel-free facts required to compose one Pull response.
pub(crate) struct PullResponseContext<'a> {
    pub(crate) effective_peer: SocketAddr,
    pub(crate) hook_metadata: &'a PullHookMetadata,
    pub(crate) broadcast_client_resolver: &'a PullBroadcastClientResolver<'a>,
    pub(crate) allow_suspend: bool,
    pub(crate) begin_time_millis: u64,
}

/// Affine data returned when the selected request boundary can transfer a suspended Pull.
pub(crate) struct PullSuspension {
    pub(crate) timing: PullSuspendTiming,
    pub(crate) request_header: PullMessageRequestHeader,
    pub(crate) subscription_data: SubscriptionData,
    pub(crate) message_filter: ArcMessageFilter,
    pub(crate) fallback: BrokerResponseParts,
}

/// The explicit outcome of composing a Pull response.
pub(crate) enum PullMessageResult {
    /// An immediate response whose head and affine body are ready for delivery.
    Reply(BrokerResponseParts),
    /// The request is eligible for an atomic transfer to the selected long-poll owner.
    Suspend(Box<PullSuspension>),
}

/// Trait defining the behavior for handling the result of a pull message request.
///
/// This trait is designed to be implemented by types that handle the result of a pull message
/// request in a RocketMQ broker. It provides a method for processing the result of a message
/// retrieval operation, along with various parameters related to the request and the broker's
/// state.
pub(crate) trait PullMessageResultHandler: Sync + Send + Any + 'static {
    /// Handles the result of a pull message request.
    ///
    /// This method processes the result of a message retrieval operation (`get_message_result`),
    /// using typed request facts, subscription data, and store results to generate an appropriate
    /// remoting response or suspension candidate.
    ///
    /// # Parameters
    /// - `get_message_result`: The result of the message retrieval operation.
    /// - `request_header`: The header of the pull message request, containing request-specific
    ///   information.
    /// - `subscription_data`: Subscription data for the consumer making the request.
    /// - `subscription_group_config`: Configuration for the subscription group of the consumer.
    /// - `message_filter`: The message filter to apply to the retrieved messages.
    /// - `response`: The initial response remoting command to be potentially modified and returned.
    /// - `mapping_context`: Context for topic-queue mapping.
    /// - `response_context`: Trusted peer, hook, broadcast identity, suspension policy, and timing
    ///   facts for the response.
    ///
    /// # Returns
    /// An explicit immediate response or suspension result.
    #[allow(
        clippy::too_many_arguments,
        reason = "existing pull result protocol context is tracked by the lint debt registry"
    )]
    async fn handle(
        &self,
        get_message_result: GetMessageResult,
        request_header: PullMessageRequestHeader,
        subscription_data: SubscriptionData,
        subscription_group_config: &SubscriptionGroupConfig,
        message_filter: ArcMessageFilter,
        response: RemotingCommand,
        mapping_context: TopicQueueMappingContext,
        response_context: PullResponseContext<'_>,
    ) -> rocketmq_error::RocketMQResult<PullMessageResult>;

    /// Returns a mutable reference to `self` as a trait object of type `Any`.
    ///
    /// This method is useful for downcasting the trait object to its concrete type.
    fn as_any_mut(&mut self) -> &mut dyn Any;

    /// Returns a reference to `self` as a trait object of type `Any`.
    ///
    /// This method is useful for downcasting the trait object to its concrete type.
    fn as_any(&self) -> &dyn Any;
}
