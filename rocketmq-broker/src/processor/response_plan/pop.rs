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

use bytes::Bytes;
use rocketmq_protocol::protocol::header::pop_message_response_header::PopMessageResponseHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_store::GetMessageResult;
use rocketmq_transport::api::v1::Channel;

use super::store_body_segments;
use super::BrokerResponseParts;
use super::LegacyResponseDelivery;

pub(crate) fn attach_pop_response_header(
    mut head: RemotingCommand,
    response_header: PopMessageResponseHeader,
) -> RemotingCommand {
    debug_assert!(head.body().is_none());
    head.set_command_custom_header_ref(response_header);
    head
}

pub(crate) fn pop_heap_response_parts(
    head: RemotingCommand,
    body: Option<Bytes>,
) -> rocketmq_error::RocketMQResult<BrokerResponseParts> {
    match body {
        Some(body) => BrokerResponseParts::bytes(head, body).map_err(Into::into),
        None => BrokerResponseParts::command(head).map_err(Into::into),
    }
}

pub(crate) fn pop_segmented_response_parts(
    head: RemotingCommand,
    body_segments: Vec<Bytes>,
) -> rocketmq_error::RocketMQResult<BrokerResponseParts> {
    BrokerResponseParts::segments(head, body_segments).map_err(Into::into)
}

pub(crate) fn take_pop_body_segments(get_message_result: GetMessageResult) -> Vec<Bytes> {
    store_body_segments(get_message_result.message_mapped_vec())
}

pub(crate) async fn deliver_pop_legacy(
    parts: BrokerResponseParts,
    channel: &Channel,
) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
    match parts.deliver_legacy(channel).await? {
        LegacyResponseDelivery::Command(command) => Ok(Some(command)),
        LegacyResponseDelivery::Written => Ok(None),
    }
}

#[cfg(test)]
#[path = "pop/tests.rs"]
mod tests;
