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

//! Exact advertised-target reads and consumption of an already resolved message.

use super::DefaultMQAdminExt;
use cheetah_string::CheetahString;
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_protocol::common::message::message_decoder;
use rocketmq_protocol::protocol::header::view_message_request_header::ViewMessageRequestHeader;

/// Lookup parameters whose transport target is independent of the address inside the Message ID.
pub struct MessageTargetLookup {
    pub broker_addr: CheetahString,
    pub topic: CheetahString,
    pub message_id: CheetahString,
}

/// Additive read capability for a Broker discovered through trusted route information.
///
/// The caller owns route authorization and verifies the returned Topic and physical Message ID.
/// This capability uses only the embedded commit-log offset; it never connects to the embedded address.
#[allow(
    async_fn_in_trait,
    reason = "These additive capabilities are called through concrete SDK sessions"
)]
pub trait MQAdminMessageTargetReadExt: Send {
    /// Reads the declared Topic and offset through the supplied advertised Broker address.
    ///
    /// # Errors
    /// Returns an error for an invalid physical ID, an unavailable session, or a failed Broker read.
    async fn view_message_at(&self, request: &MessageTargetLookup) -> crate::ClientResult<MessageExt>;
}
impl MQAdminMessageTargetReadExt for DefaultMQAdminExt {
    async fn view_message_at(&self, request: &MessageTargetLookup) -> crate::ClientResult<MessageExt> {
        let (broker_addr, header) = lookup_target(request)?;
        self.inner()
            .mq_client_api()?
            .view_message(&broker_addr, header, self.inner().remoting_timeout_millis()?)
            .await
    }
}
fn lookup_target(request: &MessageTargetLookup) -> crate::ClientResult<(CheetahString, ViewMessageRequestHeader)> {
    let decoded = message_decoder::decode_message_id(request.message_id.as_str())
        .map_err(|_| crate::ClientError::illegal_argument("Message ID must be a physical offset identifier"))?;
    Ok((
        request.broker_addr.clone(),
        ViewMessageRequestHeader {
            topic: Some(request.topic.clone()),
            offset: decoded.offset,
        },
    ))
}

#[cfg(feature = "admin-mutation")]
/// An explicitly selected Consumer and the message already verified at an advertised Broker.
/// This request owns its transient body and is intentionally neither Debug nor serializable.
pub struct ResolvedMessageConsumeRequest {
    /// The trusted route target that returned this message. The Broker forwards over its Consumer channel.
    pub broker_addr: CheetahString,
    pub consumer_group: CheetahString,
    pub client_id: Option<CheetahString>,
    pub message: MessageExt,
}
#[cfg(feature = "admin-mutation")]
/// Direct consumption without a second lookup through the address embedded in the Message ID.
#[allow(
    async_fn_in_trait,
    reason = "These additive capabilities are called through concrete SDK sessions"
)]
pub trait MQAdminResolvedMessageMutationExt: Send {
    /// Requests consumption through the verified Broker and its registered Consumer channel.
    ///
    /// # Errors
    /// Returns an error if the Consumer is unavailable, client selection is ambiguous, or execution fails.
    async fn consume_resolved_message(
        &self,
        request: ResolvedMessageConsumeRequest,
    ) -> crate::ClientResult<
        rocketmq_protocol::protocol::body::consume_message_directly_result::ConsumeMessageDirectlyResult,
    >;
}
#[cfg(feature = "admin-mutation")]
impl MQAdminResolvedMessageMutationExt for DefaultMQAdminExt {
    async fn consume_resolved_message(
        &self,
        request: ResolvedMessageConsumeRequest,
    ) -> crate::ClientResult<
        rocketmq_protocol::protocol::body::consume_message_directly_result::ConsumeMessageDirectlyResult,
    > {
        self.inner().consume_resolved_message(request).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn message_target_read_preserves_the_alias_and_uses_only_the_physical_offset() {
        let request = MessageTargetLookup {
            broker_addr: "broker.prod.example:10911".into(),
            topic: "orders".into(),
            message_id: "7F00000100002A9F0000000000000001".into(),
        };
        let (target, header) = lookup_target(&request).unwrap();
        assert_eq!(target.as_str(), "broker.prod.example:10911");
        assert_eq!(header.topic.as_deref(), Some("orders"));
        assert_eq!(header.offset, 1);
    }
}
