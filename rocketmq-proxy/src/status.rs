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

use rocketmq_error::RocketMQError;
use tonic::Code as TonicCode;
use tonic::Status as TonicStatus;

use crate::error::ProxyError;
use crate::proto::v2;

pub struct ProxyStatusMapper;

impl ProxyStatusMapper {
    pub fn ok() -> v2::Status {
        Self::from_code(v2::Code::Ok, "OK")
    }

    pub fn from_code(code: v2::Code, message: impl Into<String>) -> v2::Status {
        v2::Status {
            code: code as i32,
            message: message.into(),
        }
    }

    pub fn from_error(error: &ProxyError) -> v2::Status {
        let code = match error {
            ProxyError::ClientIdRequired => v2::Code::ClientIdRequired,
            ProxyError::UnrecognizedClientType(_) => v2::Code::UnrecognizedClientType,
            ProxyError::NotImplemented { .. } => v2::Code::NotImplemented,
            ProxyError::TooManyRequests { .. } => v2::Code::TooManyRequests,
            ProxyError::InvalidMetadata { .. } => v2::Code::BadRequest,
            ProxyError::Transport { .. } => v2::Code::InternalError,
            ProxyError::RocketMQ(inner) => Self::from_rocketmq_error(inner),
        };
        Self::from_code(code, error.to_string())
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
