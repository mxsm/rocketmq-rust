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

use rocketmq_client_rust::producer::send_result::SendResult;
use rocketmq_client_rust::producer::send_status::SendStatus;
use rocketmq_error::RocketMQError;
use rocketmq_remoting::code::response_code::ResponseCode;
use tonic::Code as TonicCode;
use tonic::Status as TonicStatus;

use crate::error::ProxyError;
use crate::proto::v2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxyPayloadStatus {
    code: i32,
    message: String,
}

impl ProxyPayloadStatus {
    pub fn new(code: i32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn code(&self) -> i32 {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn is_ok(&self) -> bool {
        self.code == v2::Code::Ok as i32
    }
}

impl From<ProxyPayloadStatus> for v2::Status {
    fn from(value: ProxyPayloadStatus) -> Self {
        Self {
            code: value.code,
            message: value.message,
        }
    }
}

pub struct ProxyStatusMapper;

impl ProxyStatusMapper {
    pub fn ok_payload() -> ProxyPayloadStatus {
        Self::from_payload_code(v2::Code::Ok, "OK")
    }

    pub fn ok() -> v2::Status {
        Self::ok_payload().into()
    }

    pub fn from_payload_code(code: v2::Code, message: impl Into<String>) -> ProxyPayloadStatus {
        ProxyPayloadStatus::new(code as i32, message)
    }

    pub fn from_code(code: v2::Code, message: impl Into<String>) -> v2::Status {
        Self::from_payload_code(code, message).into()
    }

    pub fn from_send_result_payload(result: &SendResult) -> ProxyPayloadStatus {
        match result.send_status {
            SendStatus::SendOk => Self::ok_payload(),
            SendStatus::FlushDiskTimeout => {
                Self::from_payload_code(v2::Code::MasterPersistenceTimeout, "broker flush disk timed out")
            }
            SendStatus::FlushSlaveTimeout => {
                Self::from_payload_code(v2::Code::SlavePersistenceTimeout, "broker slave flush timed out")
            }
            SendStatus::SlaveNotAvailable => {
                Self::from_payload_code(v2::Code::HaNotAvailable, "slave broker not available")
            }
        }
    }

    pub fn from_error_payload(error: &ProxyError) -> ProxyPayloadStatus {
        let code = match error {
            ProxyError::ClientIdRequired => v2::Code::ClientIdRequired,
            ProxyError::UnrecognizedClientType(_) => v2::Code::UnrecognizedClientType,
            ProxyError::NotImplemented { .. } => v2::Code::NotImplemented,
            ProxyError::TooManyRequests { .. } => v2::Code::TooManyRequests,
            ProxyError::InvalidMetadata { .. } => v2::Code::BadRequest,
            ProxyError::Transport { .. } => v2::Code::InternalError,
            ProxyError::IllegalMessageId { .. } => v2::Code::IllegalMessageId,
            ProxyError::InvalidTransactionId { .. } => v2::Code::InvalidTransactionId,
            ProxyError::IllegalMessageGroup { .. } => v2::Code::IllegalMessageGroup,
            ProxyError::IllegalDeliveryTime { .. } => v2::Code::IllegalDeliveryTime,
            ProxyError::IllegalPollingTime { .. } => v2::Code::IllegalPollingTime,
            ProxyError::IllegalOffset { .. } => v2::Code::IllegalOffset,
            ProxyError::IllegalInvisibleTime { .. } => v2::Code::IllegalInvisibleTime,
            ProxyError::IllegalFilterExpression { .. } => v2::Code::IllegalFilterExpression,
            ProxyError::InvalidReceiptHandle { .. } => v2::Code::InvalidReceiptHandle,
            ProxyError::IllegalLiteTopic { .. } => v2::Code::IllegalLiteTopic,
            ProxyError::LiteSubscriptionQuotaExceeded { .. } => v2::Code::LiteSubscriptionQuotaExceeded,
            ProxyError::MessagePropertyConflictWithType { .. } => v2::Code::MessagePropertyConflictWithType,
            ProxyError::RocketMQ(inner) => Self::from_rocketmq_error(inner),
        };
        Self::from_payload_code(code, error.to_string())
    }

    pub fn from_error(error: &ProxyError) -> v2::Status {
        Self::from_error_payload(error).into()
    }

    pub fn to_tonic_status(error: &ProxyError) -> TonicStatus {
        let payload = Self::from_error(error);
        let tonic_code = match v2::Code::try_from(payload.code).unwrap_or(v2::Code::InternalError) {
            v2::Code::BadRequest
            | v2::Code::IllegalAccessPoint
            | v2::Code::IllegalTopic
            | v2::Code::IllegalConsumerGroup
            | v2::Code::IllegalMessageTag
            | v2::Code::IllegalMessageKey
            | v2::Code::IllegalMessageGroup
            | v2::Code::IllegalMessagePropertyKey
            | v2::Code::InvalidTransactionId
            | v2::Code::IllegalMessageId
            | v2::Code::IllegalFilterExpression
            | v2::Code::IllegalInvisibleTime
            | v2::Code::IllegalDeliveryTime
            | v2::Code::InvalidReceiptHandle
            | v2::Code::MessagePropertyConflictWithType
            | v2::Code::UnrecognizedClientType
            | v2::Code::ClientIdRequired
            | v2::Code::IllegalPollingTime
            | v2::Code::IllegalOffset
            | v2::Code::IllegalLiteTopic => TonicCode::InvalidArgument,
            v2::Code::Unauthorized => TonicCode::Unauthenticated,
            v2::Code::Forbidden => TonicCode::PermissionDenied,
            v2::Code::NotFound
            | v2::Code::MessageNotFound
            | v2::Code::TopicNotFound
            | v2::Code::ConsumerGroupNotFound
            | v2::Code::OffsetNotFound => TonicCode::NotFound,
            v2::Code::RequestTimeout | v2::Code::ProxyTimeout => TonicCode::DeadlineExceeded,
            v2::Code::TooManyRequests | v2::Code::LiteTopicQuotaExceeded | v2::Code::LiteSubscriptionQuotaExceeded => {
                TonicCode::ResourceExhausted
            }
            v2::Code::NotImplemented
            | v2::Code::Unsupported
            | v2::Code::VersionUnsupported
            | v2::Code::VerifyFifoMessageUnsupported => TonicCode::Unimplemented,
            _ => TonicCode::Internal,
        };

        TonicStatus::new(tonic_code, payload.message)
    }

    fn from_rocketmq_error(error: &RocketMQError) -> v2::Code {
        match error {
            RocketMQError::TopicNotExist { .. } | RocketMQError::RouteNotFound { .. } => v2::Code::TopicNotFound,
            RocketMQError::SubscriptionGroupNotExist { .. } => v2::Code::ConsumerGroupNotFound,
            RocketMQError::Authentication(_) => v2::Code::Unauthorized,
            RocketMQError::BrokerPermissionDenied { .. } | RocketMQError::TopicSendingForbidden { .. } => {
                v2::Code::Forbidden
            }
            RocketMQError::MessageTooLarge { .. } => v2::Code::MessageBodyTooLarge,
            RocketMQError::IllegalArgument(_)
            | RocketMQError::InvalidProperty(_)
            | RocketMQError::RequestBodyInvalid { .. }
            | RocketMQError::RequestHeaderError(_)
            | RocketMQError::ResponseProcessFailed { .. }
            | RocketMQError::MissingRequiredMessageProperty { .. } => v2::Code::BadRequest,
            RocketMQError::BrokerNotFound { .. }
            | RocketMQError::QueueNotExist { .. }
            | RocketMQError::ClusterNotFound { .. } => v2::Code::NotFound,
            RocketMQError::BrokerOperationFailed { code, .. } => match ResponseCode::from(*code) {
                ResponseCode::NoPermission => v2::Code::Forbidden,
                ResponseCode::TopicNotExist => v2::Code::TopicNotFound,
                ResponseCode::SubscriptionGroupNotExist => v2::Code::ConsumerGroupNotFound,
                ResponseCode::QueryNotFound => v2::Code::OffsetNotFound,
                ResponseCode::PullOffsetMoved => v2::Code::IllegalOffset,
                _ => v2::Code::InternalError,
            },
            RocketMQError::Timeout { .. } => v2::Code::ProxyTimeout,
            RocketMQError::Network(_) => v2::Code::RequestTimeout,
            _ => v2::Code::InternalError,
        }
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_error::RocketMQError;

    use super::ProxyStatusMapper;
    use crate::error::ProxyError;
    use crate::proto::v2;

    #[test]
    fn client_id_required_maps_to_expected_code() {
        let status = ProxyStatusMapper::from_error(&ProxyError::ClientIdRequired);
        assert_eq!(status.code, v2::Code::ClientIdRequired as i32);
    }

    #[test]
    fn route_not_found_maps_to_topic_not_found() {
        let status = ProxyStatusMapper::from_error(&ProxyError::RocketMQ(RocketMQError::route_not_found("TestTopic")));
        assert_eq!(status.code, v2::Code::TopicNotFound as i32);
    }
}
