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

//! Backend-neutral contracts for the legacy Remoting ingress.
//!
//! Wire decoding and TCP lifecycle remain facade responsibilities. This ingress module
//! owns the stable request classification and backend port used after a facade
//! has decoded a canonical protocol command.

use rocketmq_model::result::SendResult;
use rocketmq_model::result::SendStatus;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_transport::api::EmbeddedDispatchOutcome;

use crate::proto::v2;
use crate::status::ProxyPayloadStatus;
use crate::ProxyServiceFuture;

/// Backend port for requests that must be handled by a facade-owned runtime.
///
/// The command is the canonical protocol DTO. Implementations may bridge it to
/// an embedded broker or another provider, but Core does not depend on those
/// implementations.
pub trait ProxyRemotingBackend: Send + Sync {
    /// Processes a canonical command through the channel-free Broker boundary.
    ///
    /// Local and remote implementations both return the affine dispatch result;
    /// remote providers wrap their response command in a `RemotingResponse`
    /// without introducing a reverse response-to-command adapter.
    fn process(&self, request: RemotingCommand) -> ProxyServiceFuture<'_, EmbeddedDispatchOutcome>;
}

/// Stable operations recognized by the Remoting ingress.
///
/// Several wire request codes intentionally share one operation. The facade
/// still receives the original command and can distinguish the exact encoding
/// while Core owns the support matrix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemotingIngressRoute {
    QueryRoute,
    QueryAssignment,
    SendMessage,
    Heartbeat,
    UnregisterClient,
    GetConsumerListByGroup,
    GetConsumerConnectionList,
    NotifyConsumerIdsChanged,
    NotifyUnsubscribeLite,
    LockBatchMessageQueue,
    UnlockBatchMessageQueue,
    CheckClientConfig,
    PullMessage,
    UpdateConsumerOffset,
    QueryConsumerOffset,
    GetMaxOffset,
    GetMinOffset,
    SearchOffsetByTimestamp,
    GetBrokerLiteInfo,
    GetParentTopicInfo,
    GetLiteTopicInfo,
    GetLiteGroupInfo,
    GetProxyDrainState,
    BeginProxyDrain,
    CancelProxyDrain,
    ForwardBackend,
    AuthAdminUnsupported,
    Unsupported,
}

/// Execution ownership selected for a Remoting request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemotingRouteClass {
    /// The Proxy decodes the request and invokes a backend-neutral service.
    Translate,
    /// The Proxy preserves the command and forwards it to exactly one Broker backend.
    ForwardBackend,
    /// The request is outside the supported Proxy surface.
    Unsupported,
}

/// One immutable entry in the Java-compatible Proxy Remoting policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RemotingRoutePolicy {
    request_code: RequestCode,
    java_request_name: &'static str,
    route: RemotingIngressRoute,
}

impl RemotingRoutePolicy {
    const fn new(request_code: RequestCode, java_request_name: &'static str, route: RemotingIngressRoute) -> Self {
        Self {
            request_code,
            java_request_name,
            route,
        }
    }

    /// Returns the numeric wire request code.
    pub const fn request_code(self) -> i32 {
        self.request_code.to_i32()
    }

    /// Returns the Java `RequestCode` constant name used by the generated inventory.
    pub const fn java_request_name(self) -> &'static str {
        self.java_request_name
    }

    /// Returns the selected ingress operation.
    pub const fn route(self) -> RemotingIngressRoute {
        self.route
    }

    /// Returns who owns execution for this route.
    pub const fn route_class(self) -> RemotingRouteClass {
        match self.route {
            RemotingIngressRoute::ForwardBackend
            | RemotingIngressRoute::LockBatchMessageQueue
            | RemotingIngressRoute::UnlockBatchMessageQueue => RemotingRouteClass::ForwardBackend,
            RemotingIngressRoute::AuthAdminUnsupported | RemotingIngressRoute::Unsupported => {
                RemotingRouteClass::Unsupported
            }
            _ => RemotingRouteClass::Translate,
        }
    }
}

const JAVA_PROXY_ACTIVE_ROUTE_POLICIES: &[RemotingRoutePolicy] = &[
    RemotingRoutePolicy::new(
        RequestCode::SendMessage,
        "SEND_MESSAGE",
        RemotingIngressRoute::SendMessage,
    ),
    RemotingRoutePolicy::new(
        RequestCode::SendMessageV2,
        "SEND_MESSAGE_V2",
        RemotingIngressRoute::SendMessage,
    ),
    RemotingRoutePolicy::new(
        RequestCode::SendBatchMessage,
        "SEND_BATCH_MESSAGE",
        RemotingIngressRoute::SendMessage,
    ),
    RemotingRoutePolicy::new(
        RequestCode::ConsumerSendMsgBack,
        "CONSUMER_SEND_MSG_BACK",
        RemotingIngressRoute::ForwardBackend,
    ),
    RemotingRoutePolicy::new(
        RequestCode::EndTransaction,
        "END_TRANSACTION",
        RemotingIngressRoute::ForwardBackend,
    ),
    RemotingRoutePolicy::new(
        RequestCode::RecallMessage,
        "RECALL_MESSAGE",
        RemotingIngressRoute::ForwardBackend,
    ),
    RemotingRoutePolicy::new(RequestCode::HeartBeat, "HEART_BEAT", RemotingIngressRoute::Heartbeat),
    RemotingRoutePolicy::new(
        RequestCode::UnregisterClient,
        "UNREGISTER_CLIENT",
        RemotingIngressRoute::UnregisterClient,
    ),
    RemotingRoutePolicy::new(
        RequestCode::CheckClientConfig,
        "CHECK_CLIENT_CONFIG",
        RemotingIngressRoute::CheckClientConfig,
    ),
    RemotingRoutePolicy::new(
        RequestCode::PullMessage,
        "PULL_MESSAGE",
        RemotingIngressRoute::PullMessage,
    ),
    RemotingRoutePolicy::new(
        RequestCode::LitePullMessage,
        "LITE_PULL_MESSAGE",
        RemotingIngressRoute::PullMessage,
    ),
    RemotingRoutePolicy::new(
        RequestCode::PopMessage,
        "POP_MESSAGE",
        RemotingIngressRoute::ForwardBackend,
    ),
    RemotingRoutePolicy::new(
        RequestCode::UpdateConsumerOffset,
        "UPDATE_CONSUMER_OFFSET",
        RemotingIngressRoute::UpdateConsumerOffset,
    ),
    RemotingRoutePolicy::new(
        RequestCode::AckMessage,
        "ACK_MESSAGE",
        RemotingIngressRoute::ForwardBackend,
    ),
    RemotingRoutePolicy::new(
        RequestCode::ChangeMessageInvisibleTime,
        "CHANGE_MESSAGE_INVISIBLETIME",
        RemotingIngressRoute::ForwardBackend,
    ),
    RemotingRoutePolicy::new(
        RequestCode::GetConsumerConnectionList,
        "GET_CONSUMER_CONNECTION_LIST",
        RemotingIngressRoute::GetConsumerConnectionList,
    ),
    RemotingRoutePolicy::new(
        RequestCode::GetConsumerListByGroup,
        "GET_CONSUMER_LIST_BY_GROUP",
        RemotingIngressRoute::GetConsumerListByGroup,
    ),
    RemotingRoutePolicy::new(
        RequestCode::GetMaxOffset,
        "GET_MAX_OFFSET",
        RemotingIngressRoute::GetMaxOffset,
    ),
    RemotingRoutePolicy::new(
        RequestCode::GetMinOffset,
        "GET_MIN_OFFSET",
        RemotingIngressRoute::GetMinOffset,
    ),
    RemotingRoutePolicy::new(
        RequestCode::QueryConsumerOffset,
        "QUERY_CONSUMER_OFFSET",
        RemotingIngressRoute::QueryConsumerOffset,
    ),
    RemotingRoutePolicy::new(
        RequestCode::SearchOffsetByTimestamp,
        "SEARCH_OFFSET_BY_TIMESTAMP",
        RemotingIngressRoute::SearchOffsetByTimestamp,
    ),
    RemotingRoutePolicy::new(
        RequestCode::LockBatchMq,
        "LOCK_BATCH_MQ",
        RemotingIngressRoute::LockBatchMessageQueue,
    ),
    RemotingRoutePolicy::new(
        RequestCode::UnlockBatchMq,
        "UNLOCK_BATCH_MQ",
        RemotingIngressRoute::UnlockBatchMessageQueue,
    ),
    RemotingRoutePolicy::new(
        RequestCode::GetRouteinfoByTopic,
        "GET_ROUTEINFO_BY_TOPIC",
        RemotingIngressRoute::QueryRoute,
    ),
];

/// Returns the complete active Java 5.5 Proxy route policy.
pub const fn java_proxy_active_route_policies() -> &'static [RemotingRoutePolicy] {
    JAVA_PROXY_ACTIVE_ROUTE_POLICIES
}

/// Returns the active Java policy for `code`, if the code belongs to that surface.
pub fn remoting_route_policy(code: i32) -> Option<RemotingRoutePolicy> {
    JAVA_PROXY_ACTIVE_ROUTE_POLICIES
        .iter()
        .copied()
        .find(|policy| policy.request_code() == code)
}

impl RemotingIngressRoute {
    /// Returns the low-cardinality observation name for a request code.
    pub fn rpc_name(code: i32) -> &'static str {
        match RequestCode::from(code) {
            RequestCode::GetRouteinfoByTopic => "RemotingGetRouteinfoByTopic",
            RequestCode::QueryAssignment => "RemotingQueryAssignment",
            RequestCode::SendMessage => "RemotingSendMessage",
            RequestCode::SendMessageV2 => "RemotingSendMessageV2",
            RequestCode::SendBatchMessage => "RemotingSendBatchMessage",
            RequestCode::HeartBeat => "RemotingHeartBeat",
            RequestCode::UnregisterClient => "RemotingUnregisterClient",
            RequestCode::GetConsumerListByGroup => "RemotingGetConsumerListByGroup",
            RequestCode::GetConsumerConnectionList => "RemotingGetConsumerConnectionList",
            RequestCode::NotifyConsumerIdsChanged => "RemotingNotifyConsumerIdsChanged",
            RequestCode::NotifyUnsubscribeLite => "RemotingNotifyUnsubscribeLite",
            RequestCode::LockBatchMq => "RemotingLockBatchMq",
            RequestCode::UnlockBatchMq => "RemotingUnlockBatchMq",
            RequestCode::CheckClientConfig => "RemotingCheckClientConfig",
            RequestCode::PullMessage => "RemotingPullMessage",
            RequestCode::LitePullMessage => "RemotingLitePullMessage",
            RequestCode::UpdateConsumerOffset => "RemotingUpdateConsumerOffset",
            RequestCode::QueryConsumerOffset => "RemotingQueryConsumerOffset",
            RequestCode::GetMaxOffset => "RemotingGetMaxOffset",
            RequestCode::GetMinOffset => "RemotingGetMinOffset",
            RequestCode::SearchOffsetByTimestamp => "RemotingSearchOffsetByTimestamp",
            RequestCode::GetBrokerLiteInfo => "RemotingGetBrokerLiteInfo",
            RequestCode::GetParentTopicInfo => "RemotingGetParentTopicInfo",
            RequestCode::GetLiteTopicInfo => "RemotingGetLiteTopicInfo",
            RequestCode::GetLiteGroupInfo => "RemotingGetLiteGroupInfo",
            RequestCode::GetProxyDrainState => "RemotingGetProxyDrainState",
            RequestCode::BeginProxyDrain => "RemotingBeginProxyDrain",
            RequestCode::CancelProxyDrain => "RemotingCancelProxyDrain",
            RequestCode::ConsumerSendMsgBack => "RemotingConsumerSendMsgBack",
            RequestCode::EndTransaction => "RemotingEndTransaction",
            RequestCode::RecallMessage => "RemotingRecallMessage",
            RequestCode::PopMessage => "RemotingPopMessage",
            RequestCode::AckMessage => "RemotingAckMessage",
            RequestCode::ChangeMessageInvisibleTime => "RemotingChangeMessageInvisibleTime",
            _ => "RemotingRequest",
        }
    }
}

/// Stateless Core dispatcher that converts a canonical command into a neutral
/// ingress operation.
///
/// It deliberately stops before custom-header or body decoding because those
/// codecs still belong to the compatibility facade.
pub struct RemotingIngressDispatcher;

impl RemotingIngressDispatcher {
    /// Returns the Core operation selected for `request`.
    pub fn route(request: &RemotingCommand) -> RemotingIngressRoute {
        classify_remoting_request(request.code())
    }
}

/// Classifies a wire request code without decoding facade-owned headers.
pub fn classify_remoting_request(code: i32) -> RemotingIngressRoute {
    if let Some(policy) = remoting_route_policy(code) {
        return policy.route();
    }
    match RequestCode::from(code) {
        RequestCode::QueryAssignment => RemotingIngressRoute::QueryAssignment,
        RequestCode::NotifyConsumerIdsChanged => RemotingIngressRoute::NotifyConsumerIdsChanged,
        RequestCode::NotifyUnsubscribeLite => RemotingIngressRoute::NotifyUnsubscribeLite,
        RequestCode::GetBrokerLiteInfo => RemotingIngressRoute::GetBrokerLiteInfo,
        RequestCode::GetParentTopicInfo => RemotingIngressRoute::GetParentTopicInfo,
        RequestCode::GetLiteTopicInfo => RemotingIngressRoute::GetLiteTopicInfo,
        RequestCode::GetLiteGroupInfo => RemotingIngressRoute::GetLiteGroupInfo,
        RequestCode::GetProxyDrainState => RemotingIngressRoute::GetProxyDrainState,
        RequestCode::BeginProxyDrain => RemotingIngressRoute::BeginProxyDrain,
        RequestCode::CancelProxyDrain => RemotingIngressRoute::CancelProxyDrain,
        RequestCode::AuthCreateUser
        | RequestCode::AuthUpdateUser
        | RequestCode::AuthDeleteUser
        | RequestCode::AuthGetUser
        | RequestCode::AuthListUsers
        | RequestCode::AuthCreateAcl
        | RequestCode::AuthUpdateAcl
        | RequestCode::AuthDeleteAcl
        | RequestCode::AuthGetAcl
        | RequestCode::AuthListAcl => RemotingIngressRoute::AuthAdminUnsupported,
        _ => RemotingIngressRoute::Unsupported,
    }
}

/// Maps Core results to canonical Remoting response codes.
pub struct RemotingStatusMapper;

impl RemotingStatusMapper {
    /// Maps a model send result to its legacy response code.
    pub fn from_send_result(result: &SendResult) -> ResponseCode {
        match result.send_status {
            SendStatus::SendOk => ResponseCode::Success,
            SendStatus::FlushDiskTimeout => ResponseCode::FlushDiskTimeout,
            SendStatus::FlushSlaveTimeout => ResponseCode::FlushSlaveTimeout,
            SendStatus::SlaveNotAvailable => ResponseCode::SlaveNotAvailable,
        }
    }

    /// Maps a Core send payload status to its legacy response code.
    pub fn from_send_payload(status: &ProxyPayloadStatus) -> ResponseCode {
        match payload_code(status) {
            v2::Code::Ok => ResponseCode::Success,
            v2::Code::MasterPersistenceTimeout => ResponseCode::FlushDiskTimeout,
            v2::Code::SlavePersistenceTimeout => ResponseCode::FlushSlaveTimeout,
            v2::Code::HaNotAvailable => ResponseCode::SlaveNotAvailable,
            v2::Code::TopicNotFound => ResponseCode::TopicNotExist,
            v2::Code::ConsumerGroupNotFound => ResponseCode::SubscriptionGroupNotExist,
            v2::Code::Forbidden | v2::Code::Unauthorized => ResponseCode::NoPermission,
            v2::Code::TooManyRequests => ResponseCode::SystemBusy,
            v2::Code::BadRequest
            | v2::Code::IllegalMessageId
            | v2::Code::IllegalMessageKey
            | v2::Code::IllegalMessageTag
            | v2::Code::IllegalMessageGroup
            | v2::Code::IllegalDeliveryTime
            | v2::Code::MessageBodyTooLarge
            | v2::Code::MessagePropertyConflictWithType => ResponseCode::MessageIllegal,
            v2::Code::NotImplemented | v2::Code::Unsupported | v2::Code::VersionUnsupported => {
                ResponseCode::RequestCodeNotSupported
            }
            _ => ResponseCode::SystemError,
        }
    }

    /// Maps a Core pull payload status to its legacy response code.
    pub fn from_pull_payload(status: &ProxyPayloadStatus) -> ResponseCode {
        match payload_code(status) {
            v2::Code::Ok => ResponseCode::Success,
            v2::Code::MessageNotFound => ResponseCode::PullNotFound,
            v2::Code::IllegalOffset => ResponseCode::PullOffsetMoved,
            v2::Code::TopicNotFound => ResponseCode::TopicNotExist,
            v2::Code::ConsumerGroupNotFound => ResponseCode::SubscriptionGroupNotExist,
            v2::Code::Forbidden | v2::Code::Unauthorized => ResponseCode::NoPermission,
            v2::Code::TooManyRequests => ResponseCode::SystemBusy,
            v2::Code::BadRequest | v2::Code::IllegalFilterExpression => ResponseCode::InvalidParameter,
            _ => ResponseCode::SystemError,
        }
    }

    /// Maps a Core offset payload status to its legacy response code.
    pub fn from_offset_payload(status: &ProxyPayloadStatus, not_found: ResponseCode) -> ResponseCode {
        match payload_code(status) {
            v2::Code::Ok => ResponseCode::Success,
            v2::Code::OffsetNotFound | v2::Code::MessageNotFound => not_found,
            v2::Code::TopicNotFound => ResponseCode::TopicNotExist,
            v2::Code::ConsumerGroupNotFound => ResponseCode::SubscriptionGroupNotExist,
            v2::Code::Forbidden | v2::Code::Unauthorized => ResponseCode::NoPermission,
            v2::Code::TooManyRequests => ResponseCode::SystemBusy,
            v2::Code::BadRequest | v2::Code::IllegalOffset => ResponseCode::InvalidParameter,
            v2::Code::NotImplemented | v2::Code::Unsupported | v2::Code::VersionUnsupported => {
                ResponseCode::RequestCodeNotSupported
            }
            _ => ResponseCode::SystemError,
        }
    }
}

fn payload_code(status: &ProxyPayloadStatus) -> v2::Code {
    v2::Code::try_from(status.code()).unwrap_or(v2::Code::InternalError)
}

#[cfg(test)]
mod tests {
    use rocketmq_model::result::SendResult;
    use rocketmq_model::result::SendStatus;
    use rocketmq_protocol::code::request_code::RequestCode;
    use rocketmq_protocol::code::response_code::ResponseCode;

    use super::classify_remoting_request;
    use super::RemotingIngressDispatcher;
    use super::RemotingIngressRoute;
    use super::RemotingStatusMapper;
    use crate::proto::v2;
    use crate::status::ProxyPayloadStatus;

    #[test]
    fn classification_groups_wire_variants_without_losing_observation_names() {
        for code in [
            RequestCode::SendMessage,
            RequestCode::SendMessageV2,
            RequestCode::SendBatchMessage,
        ] {
            assert_eq!(
                classify_remoting_request(code.to_i32()),
                RemotingIngressRoute::SendMessage
            );
        }
        assert_eq!(
            RemotingIngressRoute::rpc_name(RequestCode::SendMessageV2.to_i32()),
            "RemotingSendMessageV2"
        );

        let command = rocketmq_protocol::protocol::remoting_command::RemotingCommand::create_remoting_command(
            RequestCode::SendMessageV2,
        );
        assert_eq!(
            RemotingIngressDispatcher::route(&command),
            RemotingIngressRoute::SendMessage
        );
    }

    #[test]
    fn auth_admin_codes_are_explicitly_rejected_at_the_core_boundary() {
        assert_eq!(
            classify_remoting_request(RequestCode::AuthCreateUser.to_i32()),
            RemotingIngressRoute::AuthAdminUnsupported
        );
        assert_eq!(
            classify_remoting_request(RequestCode::UpdateBrokerConfig.to_i32()),
            RemotingIngressRoute::Unsupported
        );
        assert_eq!(
            classify_remoting_request(RequestCode::GetProxyDrainState.to_i32()),
            RemotingIngressRoute::GetProxyDrainState
        );
        assert_eq!(
            classify_remoting_request(RequestCode::BeginProxyDrain.to_i32()),
            RemotingIngressRoute::BeginProxyDrain
        );
        assert_eq!(
            classify_remoting_request(RequestCode::CancelProxyDrain.to_i32()),
            RemotingIngressRoute::CancelProxyDrain
        );
    }

    #[test]
    fn payload_statuses_map_to_legacy_response_codes() {
        assert_eq!(
            RemotingStatusMapper::from_send_payload(&ProxyPayloadStatus::new(
                v2::Code::MasterPersistenceTimeout as i32,
                "timeout"
            )),
            ResponseCode::FlushDiskTimeout
        );
        assert_eq!(
            RemotingStatusMapper::from_pull_payload(&ProxyPayloadStatus::new(
                v2::Code::MessageNotFound as i32,
                "empty"
            )),
            ResponseCode::PullNotFound
        );
        assert_eq!(
            RemotingStatusMapper::from_offset_payload(
                &ProxyPayloadStatus::new(v2::Code::OffsetNotFound as i32, "missing"),
                ResponseCode::QueryNotFound,
            ),
            ResponseCode::QueryNotFound
        );
    }

    #[test]
    fn send_result_mapping_preserves_all_legacy_statuses() {
        for (status, expected) in [
            (SendStatus::SendOk, ResponseCode::Success),
            (SendStatus::FlushDiskTimeout, ResponseCode::FlushDiskTimeout),
            (SendStatus::FlushSlaveTimeout, ResponseCode::FlushSlaveTimeout),
            (SendStatus::SlaveNotAvailable, ResponseCode::SlaveNotAvailable),
        ] {
            let result = SendResult {
                send_status: status,
                ..SendResult::default()
            };
            assert_eq!(RemotingStatusMapper::from_send_result(&result), expected);
        }
    }
}
