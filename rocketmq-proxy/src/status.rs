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

use rocketmq_error::GrpcPayloadCode;
use rocketmq_error::GrpcStatusCode;
use rocketmq_error::RocketMQError;
use rocketmq_model::result::SendResult;
use rocketmq_model::result::SendStatus;
use rocketmq_remoting::code::response_code::ResponseCode;
use tonic::Code as TonicCode;
use tonic::Status as TonicStatus;

use crate::error::ProxyError;
use crate::error::ProxyErrorKind;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProxyGrpcMapping {
    payload: v2::Code,
    status: TonicCode,
}

impl ProxyGrpcMapping {
    const fn new(payload: v2::Code, status: TonicCode) -> Self {
        Self { payload, status }
    }
}

pub struct ProxyStatusMapper;

impl ProxyStatusMapper {
    pub fn should_use_tonic_status(error: &ProxyError) -> bool {
        matches!(error, ProxyError::InvalidMetadata { .. } | ProxyError::Transport { .. })
    }

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
        let (code, message) = match error {
            ProxyError::RocketMQ(inner) => {
                let view = inner.boundary_view();
                (
                    Self::rocketmq_error_grpc_mapping(inner).payload,
                    view.message().to_owned(),
                )
            }
            local => (
                Self::local_error_grpc_mapping(
                    local
                        .local_kind()
                        .expect("non-RocketMQ proxy errors must expose local kind"),
                )
                .payload,
                local.to_string(),
            ),
        };
        Self::from_payload_code(code, message)
    }

    pub fn from_error(error: &ProxyError) -> v2::Status {
        Self::from_error_payload(error).into()
    }

    pub fn to_tonic_status(error: &ProxyError) -> TonicStatus {
        match error {
            ProxyError::InvalidMetadata { message } => {
                return TonicStatus::new(TonicCode::InvalidArgument, message.clone());
            }
            ProxyError::Transport { message } => {
                return TonicStatus::new(TonicCode::Unavailable, message.clone());
            }
            _ => {}
        }

        let payload = Self::from_error(error);
        let tonic_code = match error {
            ProxyError::RocketMQ(inner) => Self::rocketmq_error_grpc_mapping(inner).status,
            local => {
                Self::local_error_grpc_mapping(
                    local
                        .local_kind()
                        .expect("non-RocketMQ proxy errors must expose local kind"),
                )
                .status
            }
        };

        TonicStatus::new(tonic_code, payload.message)
    }

    fn local_error_grpc_mapping(kind: ProxyErrorKind) -> ProxyGrpcMapping {
        match kind {
            ProxyErrorKind::ClientIdRequired => {
                ProxyGrpcMapping::new(v2::Code::ClientIdRequired, TonicCode::InvalidArgument)
            }
            ProxyErrorKind::UnrecognizedClientType => {
                ProxyGrpcMapping::new(v2::Code::UnrecognizedClientType, TonicCode::InvalidArgument)
            }
            ProxyErrorKind::NotImplemented => ProxyGrpcMapping::new(v2::Code::NotImplemented, TonicCode::Unimplemented),
            ProxyErrorKind::TooManyRequests => {
                ProxyGrpcMapping::new(v2::Code::TooManyRequests, TonicCode::ResourceExhausted)
            }
            ProxyErrorKind::InvalidMetadata => ProxyGrpcMapping::new(v2::Code::BadRequest, TonicCode::InvalidArgument),
            ProxyErrorKind::Transport => ProxyGrpcMapping::new(v2::Code::InternalError, TonicCode::Unavailable),
            ProxyErrorKind::IllegalMessageId => {
                ProxyGrpcMapping::new(v2::Code::IllegalMessageId, TonicCode::InvalidArgument)
            }
            ProxyErrorKind::InvalidTransactionId => {
                ProxyGrpcMapping::new(v2::Code::InvalidTransactionId, TonicCode::InvalidArgument)
            }
            ProxyErrorKind::IllegalMessageGroup => {
                ProxyGrpcMapping::new(v2::Code::IllegalMessageGroup, TonicCode::InvalidArgument)
            }
            ProxyErrorKind::IllegalDeliveryTime => {
                ProxyGrpcMapping::new(v2::Code::IllegalDeliveryTime, TonicCode::InvalidArgument)
            }
            ProxyErrorKind::IllegalPollingTime => {
                ProxyGrpcMapping::new(v2::Code::IllegalPollingTime, TonicCode::InvalidArgument)
            }
            ProxyErrorKind::IllegalOffset => ProxyGrpcMapping::new(v2::Code::IllegalOffset, TonicCode::InvalidArgument),
            ProxyErrorKind::IllegalInvisibleTime => {
                ProxyGrpcMapping::new(v2::Code::IllegalInvisibleTime, TonicCode::InvalidArgument)
            }
            ProxyErrorKind::IllegalFilterExpression => {
                ProxyGrpcMapping::new(v2::Code::IllegalFilterExpression, TonicCode::InvalidArgument)
            }
            ProxyErrorKind::InvalidReceiptHandle => {
                ProxyGrpcMapping::new(v2::Code::InvalidReceiptHandle, TonicCode::InvalidArgument)
            }
            ProxyErrorKind::IllegalLiteTopic => {
                ProxyGrpcMapping::new(v2::Code::IllegalLiteTopic, TonicCode::InvalidArgument)
            }
            ProxyErrorKind::LiteSubscriptionQuotaExceeded => {
                ProxyGrpcMapping::new(v2::Code::LiteSubscriptionQuotaExceeded, TonicCode::ResourceExhausted)
            }
            ProxyErrorKind::MessagePropertyConflictWithType => {
                ProxyGrpcMapping::new(v2::Code::MessagePropertyConflictWithType, TonicCode::InvalidArgument)
            }
        }
    }

    fn rocketmq_error_grpc_mapping(error: &RocketMQError) -> ProxyGrpcMapping {
        Self::broker_response_payload_override(error).unwrap_or_else(|| {
            let grpc = error.boundary_view().grpc();
            ProxyGrpcMapping::new(
                Self::grpc_payload_to_code(grpc.payload),
                Self::grpc_status_to_tonic_code(grpc.status),
            )
        })
    }

    fn broker_response_payload_override(error: &RocketMQError) -> Option<ProxyGrpcMapping> {
        match error {
            RocketMQError::BrokerOperationFailed { code, .. } => match ResponseCode::from(*code) {
                ResponseCode::NoPermission => {
                    Some(ProxyGrpcMapping::new(v2::Code::Forbidden, TonicCode::PermissionDenied))
                }
                ResponseCode::TopicNotExist => {
                    Some(ProxyGrpcMapping::new(v2::Code::TopicNotFound, TonicCode::NotFound))
                }
                ResponseCode::SubscriptionGroupNotExist => Some(ProxyGrpcMapping::new(
                    v2::Code::ConsumerGroupNotFound,
                    TonicCode::NotFound,
                )),
                ResponseCode::UserNotExist | ResponseCode::PolicyNotExist => {
                    Some(ProxyGrpcMapping::new(v2::Code::NotFound, TonicCode::NotFound))
                }
                ResponseCode::QueryNotFound => {
                    Some(ProxyGrpcMapping::new(v2::Code::OffsetNotFound, TonicCode::NotFound))
                }
                ResponseCode::PullOffsetMoved => Some(ProxyGrpcMapping::new(
                    v2::Code::IllegalOffset,
                    TonicCode::InvalidArgument,
                )),
                ResponseCode::RequestCodeNotSupported => {
                    Some(ProxyGrpcMapping::new(v2::Code::Unsupported, TonicCode::Unimplemented))
                }
                _ => Some(ProxyGrpcMapping::new(v2::Code::InternalError, TonicCode::Internal)),
            },
            _ => None,
        }
    }

    fn grpc_payload_to_code(payload: GrpcPayloadCode) -> v2::Code {
        match payload {
            GrpcPayloadCode::InternalError => v2::Code::InternalError,
            GrpcPayloadCode::BadRequest => v2::Code::BadRequest,
            GrpcPayloadCode::Unauthorized => v2::Code::Unauthorized,
            GrpcPayloadCode::Forbidden => v2::Code::Forbidden,
            GrpcPayloadCode::NotFound => v2::Code::NotFound,
            GrpcPayloadCode::TopicNotFound => v2::Code::TopicNotFound,
            GrpcPayloadCode::ConsumerGroupNotFound => v2::Code::ConsumerGroupNotFound,
            GrpcPayloadCode::MessageNotFound => v2::Code::MessageNotFound,
            GrpcPayloadCode::MessageBodyTooLarge => v2::Code::MessageBodyTooLarge,
            GrpcPayloadCode::RequestTimeout => v2::Code::RequestTimeout,
            GrpcPayloadCode::ProxyTimeout => v2::Code::ProxyTimeout,
            GrpcPayloadCode::TooManyRequests => v2::Code::TooManyRequests,
            GrpcPayloadCode::Unsupported => v2::Code::Unsupported,
        }
    }

    fn grpc_status_to_tonic_code(status: GrpcStatusCode) -> TonicCode {
        match status {
            GrpcStatusCode::InvalidArgument => TonicCode::InvalidArgument,
            GrpcStatusCode::Unauthenticated => TonicCode::Unauthenticated,
            GrpcStatusCode::PermissionDenied => TonicCode::PermissionDenied,
            GrpcStatusCode::NotFound => TonicCode::NotFound,
            GrpcStatusCode::DeadlineExceeded => TonicCode::DeadlineExceeded,
            GrpcStatusCode::ResourceExhausted => TonicCode::ResourceExhausted,
            GrpcStatusCode::FailedPrecondition => TonicCode::FailedPrecondition,
            GrpcStatusCode::Unimplemented => TonicCode::Unimplemented,
            GrpcStatusCode::Unavailable => TonicCode::Unavailable,
            GrpcStatusCode::Internal => TonicCode::Internal,
        }
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_error::RocketMQError;
    use rocketmq_remoting::code::response_code::ResponseCode;

    use super::ProxyStatusMapper;
    use crate::error::ProxyError;
    use crate::error::ProxyErrorKind;
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

    #[test]
    fn route_not_found_requires_typed_error_instead_of_display_text() {
        let status = ProxyStatusMapper::from_error(&ProxyError::RocketMQ(RocketMQError::illegal_argument(
            "CODE: 17  DESC: No topic route info in name server for the topic: TestTopic",
        )));

        assert_eq!(status.code, v2::Code::BadRequest as i32);
        assert_eq!(
            ProxyStatusMapper::to_tonic_status(&ProxyError::RocketMQ(RocketMQError::illegal_argument(
                "CODE: 17  DESC: No topic route info in name server for the topic: TestTopic",
            )))
            .code(),
            tonic::Code::InvalidArgument
        );
    }

    #[test]
    fn invalid_metadata_prefers_tonic_status() {
        let error = ProxyError::invalid_metadata("grpc-timeout must be valid");
        assert!(ProxyStatusMapper::should_use_tonic_status(&error));
        assert_eq!(
            ProxyStatusMapper::to_tonic_status(&error).code(),
            tonic::Code::InvalidArgument
        );
    }

    #[test]
    fn transport_error_maps_to_unavailable_tonic_status() {
        let error = ProxyError::Transport {
            message: "cluster worker unavailable".to_owned(),
        };
        assert!(ProxyStatusMapper::should_use_tonic_status(&error));
        assert_eq!(
            ProxyStatusMapper::to_tonic_status(&error).code(),
            tonic::Code::Unavailable
        );
    }

    #[test]
    fn phase7_broker_auth_and_permission_response_codes_map_to_grpc_payload_codes() {
        for (response_code, expected_grpc_code) in [
            (ResponseCode::NoPermission, v2::Code::Forbidden),
            (ResponseCode::UserNotExist, v2::Code::NotFound),
            (ResponseCode::PolicyNotExist, v2::Code::NotFound),
        ] {
            let error = ProxyError::RocketMQ(RocketMQError::broker_operation_failed(
                "AUTH_ADMIN",
                response_code.to_i32(),
                "auth failed",
            ));
            let status = ProxyStatusMapper::from_error(&error);
            assert_eq!(status.code, expected_grpc_code as i32);
        }

        let authentication_error = ProxyError::RocketMQ(RocketMQError::authentication_failed("bad credentials"));
        let status = ProxyStatusMapper::from_error(&authentication_error);
        assert_eq!(status.code, v2::Code::Unauthorized as i32);
    }

    #[test]
    fn phase7_request_code_not_supported_maps_to_unsupported_and_unimplemented_transport() {
        let error = ProxyError::RocketMQ(RocketMQError::broker_operation_failed(
            "REMOTING",
            ResponseCode::RequestCodeNotSupported.to_i32(),
            "request code not supported",
        ));

        let payload_status = ProxyStatusMapper::from_error(&error);
        assert_eq!(payload_status.code, v2::Code::Unsupported as i32);
        assert_eq!(
            ProxyStatusMapper::to_tonic_status(&error).code(),
            tonic::Code::Unimplemented
        );
    }

    #[test]
    fn broker_response_overrides_have_explicit_payload_and_tonic_status() {
        for (response_code, expected_grpc_code, expected_tonic_code) in [
            (
                ResponseCode::QueryNotFound,
                v2::Code::OffsetNotFound,
                tonic::Code::NotFound,
            ),
            (
                ResponseCode::PullOffsetMoved,
                v2::Code::IllegalOffset,
                tonic::Code::InvalidArgument,
            ),
            (
                ResponseCode::RequestCodeNotSupported,
                v2::Code::Unsupported,
                tonic::Code::Unimplemented,
            ),
        ] {
            let error = ProxyError::RocketMQ(RocketMQError::broker_operation_failed(
                "BROKER",
                response_code.to_i32(),
                "broker response override",
            ));
            let payload_status = ProxyStatusMapper::from_error(&error);
            assert_eq!(payload_status.code, expected_grpc_code as i32);
            assert_eq!(ProxyStatusMapper::to_tonic_status(&error).code(), expected_tonic_code);
        }
    }

    #[test]
    fn auth_config_errors_map_to_bad_request_payload_and_transport_status() {
        for error in [
            ProxyError::RocketMQ(RocketMQError::ConfigInvalidValue {
                key: "auth.authorization",
                value: "local".to_owned(),
                reason: "provider not ready".to_owned(),
            }),
            ProxyError::RocketMQ(RocketMQError::auth_config_invalid(
                "auth.authorization",
                "provider not ready",
            )),
        ] {
            let payload_status = ProxyStatusMapper::from_error(&error);
            assert_eq!(payload_status.code, v2::Code::BadRequest as i32);
            assert_eq!(
                ProxyStatusMapper::to_tonic_status(&error).code(),
                tonic::Code::InvalidArgument
            );
        }
    }

    #[test]
    fn rocketmq_errors_use_central_grpc_boundary_spec() {
        let retry_exhausted = ProxyError::RocketMQ(RocketMQError::RetryLimitExceeded {
            group: "GID_test".to_owned(),
            current: 3,
            max: 3,
        });
        let retry_payload = ProxyStatusMapper::from_error(&retry_exhausted);
        assert_eq!(retry_payload.code, v2::Code::TooManyRequests as i32);
        assert_eq!(
            ProxyStatusMapper::to_tonic_status(&retry_exhausted).code(),
            tonic::Code::ResourceExhausted
        );

        let not_master = ProxyError::RocketMQ(RocketMQError::NotMasterBroker {
            master_address: "127.0.0.1:10911".to_owned(),
        });
        let not_master_payload = ProxyStatusMapper::from_error(&not_master);
        assert_eq!(not_master_payload.code, v2::Code::InternalError as i32);
        assert_eq!(
            ProxyStatusMapper::to_tonic_status(&not_master).code(),
            tonic::Code::FailedPrecondition
        );
    }

    #[test]
    fn rocketmq_error_payload_message_uses_public_message() {
        let inner = RocketMQError::ConfigInvalidValue {
            key: "auth.authorization",
            value: "local".to_owned(),
            reason: "provider not ready".to_owned(),
        };
        let expected_message = inner.public_message().to_owned();
        let error = ProxyError::RocketMQ(inner);

        let status = ProxyStatusMapper::from_error(&error);

        assert_eq!(status.message, expected_message);
    }

    #[test]
    fn local_proxy_errors_use_local_only_kind_mapping() {
        let lite_topic = ProxyError::illegal_lite_topic("not an LMQ");
        assert_eq!(lite_topic.local_kind(), Some(ProxyErrorKind::IllegalLiteTopic));
        let lite_topic_status = ProxyStatusMapper::from_error(&lite_topic);
        assert_eq!(lite_topic_status.code, v2::Code::IllegalLiteTopic as i32);
        assert_eq!(
            ProxyStatusMapper::to_tonic_status(&lite_topic).code(),
            tonic::Code::InvalidArgument
        );

        let quota = ProxyError::lite_subscription_quota_exceeded("subscription limit reached");
        assert_eq!(quota.local_kind(), Some(ProxyErrorKind::LiteSubscriptionQuotaExceeded));
        let quota_status = ProxyStatusMapper::from_error(&quota);
        assert_eq!(quota_status.code, v2::Code::LiteSubscriptionQuotaExceeded as i32);
        assert_eq!(
            ProxyStatusMapper::to_tonic_status(&quota).code(),
            tonic::Code::ResourceExhausted
        );
    }
}
