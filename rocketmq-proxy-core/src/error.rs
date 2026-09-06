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

use rocketmq_error::fields;
use rocketmq_error::Error as CanonicalError;
use rocketmq_error::ErrorDescriptor;
use rocketmq_error::RocketMQError;
use rocketmq_error::PROXY_BROKER_CONSUMER_GROUP_NOT_FOUND;
use rocketmq_error::PROXY_BROKER_OFFSET_INVALID;
use rocketmq_error::PROXY_BROKER_OFFSET_NOT_FOUND;
use rocketmq_error::PROXY_BROKER_PERMISSION_DENIED;
use rocketmq_error::PROXY_BROKER_REQUEST_UNSUPPORTED;
use rocketmq_error::PROXY_BROKER_RESOURCE_NOT_FOUND;
use rocketmq_error::PROXY_BROKER_RESPONSE_FAILED;
use rocketmq_error::PROXY_BROKER_TOPIC_NOT_FOUND;
use rocketmq_protocol::code::response_code::ResponseCode;
use thiserror::Error;

pub type ProxyResult<T> = std::result::Result<T, ProxyError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProxyErrorKind {
    ClientIdRequired,
    UnrecognizedClientType,
    NotImplemented,
    TooManyRequests,
    Draining,
    InvalidMetadata,
    Transport,
    IllegalMessageId,
    InvalidTransactionId,
    IllegalMessageGroup,
    IllegalDeliveryTime,
    IllegalPollingTime,
    IllegalOffset,
    IllegalInvisibleTime,
    IllegalFilterExpression,
    InvalidReceiptHandle,
    IllegalLiteTopic,
    LiteSubscriptionQuotaExceeded,
    MessagePropertyConflictWithType,
    SettingsUnavailable,
}

#[derive(Debug, Error)]
pub enum ProxyError {
    #[error("{0}")]
    RocketMQ(#[source] RocketMQError),

    #[error("{0}")]
    BrokerResponse(#[source] CanonicalError),

    #[error("gRPC client id is required")]
    ClientIdRequired,

    #[error("unrecognized client type: {0}")]
    UnrecognizedClientType(i32),

    #[error("proxy capability is not implemented yet: {feature}")]
    NotImplemented { feature: &'static str },

    #[error("request was rejected because '{resource}' is saturated")]
    TooManyRequests { resource: &'static str },

    #[error("Proxy is draining and does not accept new requests")]
    Draining,

    #[error("invalid gRPC metadata: {message}")]
    InvalidMetadata { message: String },

    #[error("transport error: {message}")]
    Transport { message: String },

    #[error("illegal message id: {message}")]
    IllegalMessageId { message: String },

    #[error("invalid transaction id: {message}")]
    InvalidTransactionId { message: String },

    #[error("illegal message group: {message}")]
    IllegalMessageGroup { message: String },

    #[error("illegal delivery time: {message}")]
    IllegalDeliveryTime { message: String },

    #[error("illegal polling time: {message}")]
    IllegalPollingTime { message: String },

    #[error("illegal offset: {message}")]
    IllegalOffset { message: String },

    #[error("illegal invisible time: {message}")]
    IllegalInvisibleTime { message: String },

    #[error("illegal filter expression: {message}")]
    IllegalFilterExpression { message: String },

    #[error("invalid receipt handle: {message}")]
    InvalidReceiptHandle { message: String },

    #[error("illegal lite topic: {message}")]
    IllegalLiteTopic { message: String },

    #[error("lite subscription quota exceeded: {message}")]
    LiteSubscriptionQuotaExceeded { message: String },

    #[error("message property conflicts with message type: {message}")]
    MessagePropertyConflictWithType { message: String },

    #[error("authoritative client settings are unavailable: {message}")]
    SettingsUnavailable { message: String },
}

impl ProxyError {
    pub fn local_kind(&self) -> Option<ProxyErrorKind> {
        Some(match self {
            Self::RocketMQ(_) | Self::BrokerResponse(_) => return None,
            Self::ClientIdRequired => ProxyErrorKind::ClientIdRequired,
            Self::UnrecognizedClientType(_) => ProxyErrorKind::UnrecognizedClientType,
            Self::NotImplemented { .. } => ProxyErrorKind::NotImplemented,
            Self::TooManyRequests { .. } => ProxyErrorKind::TooManyRequests,
            Self::Draining => ProxyErrorKind::Draining,
            Self::InvalidMetadata { .. } => ProxyErrorKind::InvalidMetadata,
            Self::Transport { .. } => ProxyErrorKind::Transport,
            Self::IllegalMessageId { .. } => ProxyErrorKind::IllegalMessageId,
            Self::InvalidTransactionId { .. } => ProxyErrorKind::InvalidTransactionId,
            Self::IllegalMessageGroup { .. } => ProxyErrorKind::IllegalMessageGroup,
            Self::IllegalDeliveryTime { .. } => ProxyErrorKind::IllegalDeliveryTime,
            Self::IllegalPollingTime { .. } => ProxyErrorKind::IllegalPollingTime,
            Self::IllegalOffset { .. } => ProxyErrorKind::IllegalOffset,
            Self::IllegalInvisibleTime { .. } => ProxyErrorKind::IllegalInvisibleTime,
            Self::IllegalFilterExpression { .. } => ProxyErrorKind::IllegalFilterExpression,
            Self::InvalidReceiptHandle { .. } => ProxyErrorKind::InvalidReceiptHandle,
            Self::IllegalLiteTopic { .. } => ProxyErrorKind::IllegalLiteTopic,
            Self::LiteSubscriptionQuotaExceeded { .. } => ProxyErrorKind::LiteSubscriptionQuotaExceeded,
            Self::MessagePropertyConflictWithType { .. } => ProxyErrorKind::MessagePropertyConflictWithType,
            Self::SettingsUnavailable { .. } => ProxyErrorKind::SettingsUnavailable,
        })
    }

    pub fn not_implemented(feature: &'static str) -> Self {
        Self::NotImplemented { feature }
    }

    pub fn too_many_requests(resource: &'static str) -> Self {
        Self::TooManyRequests { resource }
    }

    pub fn invalid_metadata(message: impl Into<String>) -> Self {
        Self::InvalidMetadata {
            message: message.into(),
        }
    }

    pub fn illegal_message_id(message: impl Into<String>) -> Self {
        Self::IllegalMessageId {
            message: message.into(),
        }
    }

    pub fn invalid_transaction_id(message: impl Into<String>) -> Self {
        Self::InvalidTransactionId {
            message: message.into(),
        }
    }

    pub fn illegal_message_group(message: impl Into<String>) -> Self {
        Self::IllegalMessageGroup {
            message: message.into(),
        }
    }

    pub fn illegal_delivery_time(message: impl Into<String>) -> Self {
        Self::IllegalDeliveryTime {
            message: message.into(),
        }
    }

    pub fn illegal_polling_time(message: impl Into<String>) -> Self {
        Self::IllegalPollingTime {
            message: message.into(),
        }
    }

    pub fn illegal_offset(message: impl Into<String>) -> Self {
        Self::IllegalOffset {
            message: message.into(),
        }
    }

    pub fn illegal_invisible_time(message: impl Into<String>) -> Self {
        Self::IllegalInvisibleTime {
            message: message.into(),
        }
    }

    pub fn illegal_filter_expression(message: impl Into<String>) -> Self {
        Self::IllegalFilterExpression {
            message: message.into(),
        }
    }

    pub fn invalid_receipt_handle(message: impl Into<String>) -> Self {
        Self::InvalidReceiptHandle {
            message: message.into(),
        }
    }

    pub fn illegal_lite_topic(message: impl Into<String>) -> Self {
        Self::IllegalLiteTopic {
            message: message.into(),
        }
    }

    pub fn lite_subscription_quota_exceeded(message: impl Into<String>) -> Self {
        Self::LiteSubscriptionQuotaExceeded {
            message: message.into(),
        }
    }

    pub fn message_property_conflict(message: impl Into<String>) -> Self {
        Self::MessagePropertyConflictWithType {
            message: message.into(),
        }
    }

    pub fn settings_unavailable(message: impl Into<String>) -> Self {
        Self::SettingsUnavailable {
            message: message.into(),
        }
    }
}

impl From<RocketMQError> for ProxyError {
    fn from(error: RocketMQError) -> Self {
        match error {
            source @ RocketMQError::BrokerOperationFailed { code, .. } => {
                let descriptor = proxy_broker_response_descriptor(code);
                let context = source.context().with_secret_presence(fields::SOURCE_PRESENT);
                Self::BrokerResponse(CanonicalError::caused_by(descriptor, source).with_context(context))
            }
            source => Self::RocketMQ(source),
        }
    }
}

fn proxy_broker_response_descriptor(code: i32) -> &'static ErrorDescriptor {
    match ResponseCode::from(code) {
        ResponseCode::NoPermission => &PROXY_BROKER_PERMISSION_DENIED,
        ResponseCode::TopicNotExist => &PROXY_BROKER_TOPIC_NOT_FOUND,
        ResponseCode::SubscriptionGroupNotExist => &PROXY_BROKER_CONSUMER_GROUP_NOT_FOUND,
        ResponseCode::UserNotExist | ResponseCode::PolicyNotExist => &PROXY_BROKER_RESOURCE_NOT_FOUND,
        ResponseCode::QueryNotFound => &PROXY_BROKER_OFFSET_NOT_FOUND,
        ResponseCode::PullOffsetMoved => &PROXY_BROKER_OFFSET_INVALID,
        ResponseCode::RequestCodeNotSupported => &PROXY_BROKER_REQUEST_UNSUPPORTED,
        _ => &PROXY_BROKER_RESPONSE_FAILED,
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as StdError;

    use rocketmq_error::ViewValueRef;

    use super::*;

    fn normalized_broker_error(code: ResponseCode, message: &str) -> ProxyError {
        RocketMQError::broker_operation_failed("BROKER_TEST", code.to_i32(), message)
            .with_broker_addr("127.0.0.1:10911")
            .into()
    }

    #[test]
    fn broker_response_codes_are_classified_once_at_proxy_ingress() {
        for (code, descriptor) in [
            (ResponseCode::NoPermission, &PROXY_BROKER_PERMISSION_DENIED),
            (ResponseCode::TopicNotExist, &PROXY_BROKER_TOPIC_NOT_FOUND),
            (
                ResponseCode::SubscriptionGroupNotExist,
                &PROXY_BROKER_CONSUMER_GROUP_NOT_FOUND,
            ),
            (ResponseCode::UserNotExist, &PROXY_BROKER_RESOURCE_NOT_FOUND),
            (ResponseCode::PolicyNotExist, &PROXY_BROKER_RESOURCE_NOT_FOUND),
            (ResponseCode::QueryNotFound, &PROXY_BROKER_OFFSET_NOT_FOUND),
            (ResponseCode::PullOffsetMoved, &PROXY_BROKER_OFFSET_INVALID),
            (ResponseCode::RequestCodeNotSupported, &PROXY_BROKER_REQUEST_UNSUPPORTED),
            (ResponseCode::SystemBusy, &PROXY_BROKER_RESPONSE_FAILED),
        ] {
            let ProxyError::BrokerResponse(error) = normalized_broker_error(code, "broker rejected request") else {
                panic!("BrokerOperationFailed must normalize at Proxy ingress");
            };
            assert_eq!(error.descriptor(), descriptor);
        }

        let ProxyError::BrokerResponse(error) = ProxyError::from(RocketMQError::broker_operation_failed(
            "BROKER_TEST",
            987_654,
            "unknown code",
        )) else {
            panic!("BrokerOperationFailed must normalize at Proxy ingress");
        };
        assert_eq!(error.descriptor(), &PROXY_BROKER_RESPONSE_FAILED);
    }

    #[test]
    fn normalized_broker_response_retains_typed_source_and_diagnostic_origin() {
        let proxy_error = normalized_broker_error(ResponseCode::TopicNotExist, "secret\r\nC:\\private\\broker.conf");
        let canonical = StdError::source(&proxy_error)
            .and_then(|source| source.downcast_ref::<CanonicalError>())
            .expect("ProxyError retains the normalized canonical carrier");
        let source = StdError::source(canonical)
            .and_then(|source| source.downcast_ref::<RocketMQError>())
            .expect("normalized response retains the original typed source");
        assert!(matches!(
            source,
            RocketMQError::BrokerOperationFailed {
                code,
                message,
                broker_addr: Some(addr),
                ..
            } if *code == ResponseCode::TopicNotExist.to_i32()
                && message == "secret\r\nC:\\private\\broker.conf"
                && addr == "127.0.0.1:10911"
        ));

        let ProxyError::BrokerResponse(error) = proxy_error else {
            panic!("BrokerOperationFailed must normalize at Proxy ingress");
        };

        let public = error.public_view().expect("descriptor-valid public view");
        assert_eq!(public.message(), PROXY_BROKER_TOPIC_NOT_FOUND.public_message());
        assert_eq!(public.fields().count(), 0);
        let rendered = error.to_string();
        assert!(!rendered.contains("secret"));
        assert!(!rendered.contains("private"));
        assert!(!rendered.contains("127.0.0.1"));

        let diagnostic = error.diagnostic_view().expect("descriptor-valid diagnostic view");
        let diagnostic_fields = diagnostic
            .fields()
            .map(|field| (field.name(), field.value()))
            .collect::<Vec<_>>();
        assert!(diagnostic_fields.contains(&("broker_code", ViewValueRef::I64(17))));
        assert!(diagnostic_fields.contains(&("broker_addr", ViewValueRef::Text("127.0.0.1:10911"))));
        assert!(diagnostic_fields.contains(&("message", ViewValueRef::Redacted)));
        assert!(diagnostic_fields.contains(&("source_present", ViewValueRef::Redacted)));
    }

    #[test]
    fn non_broker_rocketmq_errors_keep_the_existing_proxy_variant() {
        let error = ProxyError::from(RocketMQError::illegal_argument("invalid request"));
        assert!(matches!(error, ProxyError::RocketMQ(_)));
    }
}
