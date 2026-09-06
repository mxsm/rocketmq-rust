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
use rocketmq_model::result::SendResult;
use rocketmq_model::result::SendStatus;
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

pub struct ProxyStatusMapper;

impl ProxyStatusMapper {
    pub fn should_use_tonic_status(error: &ProxyError) -> bool {
        matches!(
            error.local_kind(),
            Some(ProxyErrorKind::InvalidMetadata | ProxyErrorKind::Transport | ProxyErrorKind::SettingsUnavailable)
        )
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
        let descriptor = error.descriptor();
        let grpc = descriptor.projection().grpc();
        Self::from_payload_code(Self::grpc_payload_to_code(grpc.payload), descriptor.public_message())
    }

    pub fn from_error(error: &ProxyError) -> v2::Status {
        Self::from_error_payload(error).into()
    }

    pub fn to_tonic_status(error: &ProxyError) -> TonicStatus {
        let descriptor = error.descriptor();
        let grpc = descriptor.projection().grpc();
        TonicStatus::new(
            Self::grpc_status_to_tonic_code(grpc.status),
            descriptor.public_message(),
        )
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
            GrpcPayloadCode::OffsetNotFound => v2::Code::OffsetNotFound,
            GrpcPayloadCode::IllegalOffset => v2::Code::IllegalOffset,
            GrpcPayloadCode::ClientIdRequired => v2::Code::ClientIdRequired,
            GrpcPayloadCode::UnrecognizedClientType => v2::Code::UnrecognizedClientType,
            GrpcPayloadCode::NotImplemented => v2::Code::NotImplemented,
            GrpcPayloadCode::IllegalMessageId => v2::Code::IllegalMessageId,
            GrpcPayloadCode::InvalidTransactionId => v2::Code::InvalidTransactionId,
            GrpcPayloadCode::IllegalMessageGroup => v2::Code::IllegalMessageGroup,
            GrpcPayloadCode::IllegalDeliveryTime => v2::Code::IllegalDeliveryTime,
            GrpcPayloadCode::IllegalPollingTime => v2::Code::IllegalPollingTime,
            GrpcPayloadCode::IllegalInvisibleTime => v2::Code::IllegalInvisibleTime,
            GrpcPayloadCode::IllegalFilterExpression => v2::Code::IllegalFilterExpression,
            GrpcPayloadCode::InvalidReceiptHandle => v2::Code::InvalidReceiptHandle,
            GrpcPayloadCode::IllegalLiteTopic => v2::Code::IllegalLiteTopic,
            GrpcPayloadCode::LiteSubscriptionQuotaExceeded => v2::Code::LiteSubscriptionQuotaExceeded,
            GrpcPayloadCode::MessagePropertyConflictWithType => v2::Code::MessagePropertyConflictWithType,
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
            GrpcStatusCode::AlreadyExists => TonicCode::AlreadyExists,
            GrpcStatusCode::Aborted => TonicCode::Aborted,
            GrpcStatusCode::Unimplemented => TonicCode::Unimplemented,
            GrpcStatusCode::Unavailable => TonicCode::Unavailable,
            GrpcStatusCode::DataLoss => TonicCode::DataLoss,
            GrpcStatusCode::Internal => TonicCode::Internal,
        }
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_error::GrpcStatusCode;
    use rocketmq_error::PublicErrorView;
    use rocketmq_error::RocketMQError;
    use rocketmq_protocol::code::response_code::ResponseCode;

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
            let error = ProxyError::from(RocketMQError::broker_operation_failed(
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
        let error = ProxyError::from(RocketMQError::broker_operation_failed(
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
    fn normalized_broker_responses_use_descriptor_payload_and_tonic_status() {
        for (response_code, expected_grpc_code, expected_tonic_code) in [
            (
                ResponseCode::NoPermission,
                v2::Code::Forbidden,
                tonic::Code::PermissionDenied,
            ),
            (
                ResponseCode::TopicNotExist,
                v2::Code::TopicNotFound,
                tonic::Code::NotFound,
            ),
            (
                ResponseCode::SubscriptionGroupNotExist,
                v2::Code::ConsumerGroupNotFound,
                tonic::Code::NotFound,
            ),
            (ResponseCode::UserNotExist, v2::Code::NotFound, tonic::Code::NotFound),
            (ResponseCode::PolicyNotExist, v2::Code::NotFound, tonic::Code::NotFound),
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
            (ResponseCode::SystemBusy, v2::Code::InternalError, tonic::Code::Internal),
        ] {
            let error = ProxyError::from(RocketMQError::broker_operation_failed(
                "BROKER",
                response_code.to_i32(),
                "secret broker response\r\nC:\\private\\broker.conf",
            ));
            let payload_status = ProxyStatusMapper::from_error(&error);
            assert_eq!(payload_status.code, expected_grpc_code as i32);
            assert_eq!(ProxyStatusMapper::to_tonic_status(&error).code(), expected_tonic_code);
            assert!(!payload_status.message.contains("secret"));
            assert!(!payload_status.message.contains("private"));
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
    fn data_loss_status_maps_to_tonic_data_loss() {
        assert_eq!(
            ProxyStatusMapper::grpc_status_to_tonic_code(GrpcStatusCode::DataLoss),
            tonic::Code::DataLoss
        );
    }

    #[test]
    fn newly_declared_statuses_map_exhaustively_to_tonic() {
        for (status, expected) in [
            (GrpcStatusCode::AlreadyExists, tonic::Code::AlreadyExists),
            (GrpcStatusCode::Aborted, tonic::Code::Aborted),
        ] {
            assert_eq!(ProxyStatusMapper::grpc_status_to_tonic_code(status), expected);
        }
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
    fn all_local_proxy_errors_use_fixed_catalog_output_and_preserve_wire_codes() {
        const MALICIOUS: &str = "secret-token\r\nC:\\private\\proxy.pem";

        let cases = vec![
            (
                ProxyError::ClientIdRequired,
                ProxyErrorKind::ClientIdRequired,
                v2::Code::ClientIdRequired,
                tonic::Code::InvalidArgument,
                1,
            ),
            (
                ProxyError::UnrecognizedClientType(31337),
                ProxyErrorKind::UnrecognizedClientType,
                v2::Code::UnrecognizedClientType,
                tonic::Code::InvalidArgument,
                1,
            ),
            (
                ProxyError::not_implemented(MALICIOUS),
                ProxyErrorKind::NotImplemented,
                v2::Code::NotImplemented,
                tonic::Code::Unimplemented,
                3,
            ),
            (
                ProxyError::too_many_requests(MALICIOUS),
                ProxyErrorKind::TooManyRequests,
                v2::Code::TooManyRequests,
                tonic::Code::ResourceExhausted,
                2,
            ),
            (
                ProxyError::Draining,
                ProxyErrorKind::Draining,
                v2::Code::InternalError,
                tonic::Code::Unavailable,
                1,
            ),
            (
                ProxyError::invalid_metadata(MALICIOUS),
                ProxyErrorKind::InvalidMetadata,
                v2::Code::BadRequest,
                tonic::Code::InvalidArgument,
                1,
            ),
            (
                ProxyError::Transport {
                    message: MALICIOUS.to_owned(),
                },
                ProxyErrorKind::Transport,
                v2::Code::InternalError,
                tonic::Code::Unavailable,
                1,
            ),
            (
                ProxyError::illegal_message_id(MALICIOUS),
                ProxyErrorKind::IllegalMessageId,
                v2::Code::IllegalMessageId,
                tonic::Code::InvalidArgument,
                13,
            ),
            (
                ProxyError::invalid_transaction_id(MALICIOUS),
                ProxyErrorKind::InvalidTransactionId,
                v2::Code::InvalidTransactionId,
                tonic::Code::InvalidArgument,
                1,
            ),
            (
                ProxyError::illegal_message_group(MALICIOUS),
                ProxyErrorKind::IllegalMessageGroup,
                v2::Code::IllegalMessageGroup,
                tonic::Code::InvalidArgument,
                13,
            ),
            (
                ProxyError::illegal_delivery_time(MALICIOUS),
                ProxyErrorKind::IllegalDeliveryTime,
                v2::Code::IllegalDeliveryTime,
                tonic::Code::InvalidArgument,
                13,
            ),
            (
                ProxyError::illegal_polling_time(MALICIOUS),
                ProxyErrorKind::IllegalPollingTime,
                v2::Code::IllegalPollingTime,
                tonic::Code::InvalidArgument,
                1,
            ),
            (
                ProxyError::illegal_offset(MALICIOUS),
                ProxyErrorKind::IllegalOffset,
                v2::Code::IllegalOffset,
                tonic::Code::InvalidArgument,
                21,
            ),
            (
                ProxyError::illegal_invisible_time(MALICIOUS),
                ProxyErrorKind::IllegalInvisibleTime,
                v2::Code::IllegalInvisibleTime,
                tonic::Code::InvalidArgument,
                1,
            ),
            (
                ProxyError::illegal_filter_expression(MALICIOUS),
                ProxyErrorKind::IllegalFilterExpression,
                v2::Code::IllegalFilterExpression,
                tonic::Code::InvalidArgument,
                23,
            ),
            (
                ProxyError::invalid_receipt_handle(MALICIOUS),
                ProxyErrorKind::InvalidReceiptHandle,
                v2::Code::InvalidReceiptHandle,
                tonic::Code::InvalidArgument,
                1,
            ),
            (
                ProxyError::illegal_lite_topic(MALICIOUS),
                ProxyErrorKind::IllegalLiteTopic,
                v2::Code::IllegalLiteTopic,
                tonic::Code::InvalidArgument,
                1,
            ),
            (
                ProxyError::lite_subscription_quota_exceeded(MALICIOUS),
                ProxyErrorKind::LiteSubscriptionQuotaExceeded,
                v2::Code::LiteSubscriptionQuotaExceeded,
                tonic::Code::ResourceExhausted,
                1,
            ),
            (
                ProxyError::message_property_conflict(MALICIOUS),
                ProxyErrorKind::MessagePropertyConflictWithType,
                v2::Code::MessagePropertyConflictWithType,
                tonic::Code::InvalidArgument,
                13,
            ),
            (
                ProxyError::settings_unavailable(MALICIOUS),
                ProxyErrorKind::SettingsUnavailable,
                v2::Code::InternalError,
                tonic::Code::FailedPrecondition,
                1,
            ),
        ];

        for (error, kind, expected_payload, expected_tonic, expected_remoting) in cases {
            assert_eq!(error.local_kind(), Some(kind));
            assert_eq!(error.descriptor(), kind.descriptor());
            assert_eq!(
                error.descriptor().projection().remoting().code.as_i32(),
                expected_remoting,
                "{kind:?}"
            );

            let context = error.context();
            let public = PublicErrorView::try_new(error.descriptor(), &context)
                .expect("Proxy local context must match its catalog descriptor");
            assert_eq!(public.fields().count(), 0, "{kind:?}");

            let payload = ProxyStatusMapper::from_error_payload(&error);
            let tonic = ProxyStatusMapper::to_tonic_status(&error);
            assert_eq!(payload.code(), expected_payload as i32, "{kind:?}");
            assert_eq!(tonic.code(), expected_tonic, "{kind:?}");
            assert_eq!(payload.message(), error.descriptor().public_message(), "{kind:?}");
            assert_eq!(tonic.message(), error.descriptor().public_message(), "{kind:?}");
            for output in [payload.message(), tonic.message()] {
                assert!(!output.contains("secret-token"), "{kind:?}");
                assert!(!output.contains("private"), "{kind:?}");
                assert!(!output.chars().any(char::is_control), "{kind:?}");
            }
            assert_eq!(
                ProxyStatusMapper::should_use_tonic_status(&error),
                matches!(
                    kind,
                    ProxyErrorKind::InvalidMetadata | ProxyErrorKind::Transport | ProxyErrorKind::SettingsUnavailable
                ),
                "{kind:?}"
            );
        }
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
