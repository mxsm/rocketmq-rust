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
use rocketmq_error::ErrorContext;
use rocketmq_error::ErrorDescriptor;
use rocketmq_error::SharedError;
use rocketmq_error::ViewValueRef;
use rocketmq_error::AUTH_CREDENTIALS_INVALID;
use rocketmq_error::AUTH_OPERATION_FAILED;
use rocketmq_error::AUTH_PERMISSION_DENIED;
use rocketmq_error::BROKER_LEADERSHIP_NOT_MASTER;
use rocketmq_error::BROKER_LOOKUP_NOT_FOUND;
use rocketmq_error::BROKER_OPERATION_FAILED;
use rocketmq_error::BROKER_QUEUE_ID_OUT_OF_RANGE;
use rocketmq_error::BROKER_QUEUE_NOT_FOUND;
use rocketmq_error::BROKER_SUBSCRIPTION_GROUP_NOT_FOUND;
use rocketmq_error::BROKER_TOPIC_NOT_FOUND;
use rocketmq_error::CLIENT_RETRY_BUDGET_EXHAUSTED;
use rocketmq_error::CORE_ARGUMENT_INVALID;
use rocketmq_error::CORE_CONFIGURATION_INVALID;
use rocketmq_error::CORE_CONFIGURATION_PARSE_FAILED;
use rocketmq_error::CORE_INTERNAL_FAILURE;
use rocketmq_error::CORE_OPERATION_TIMED_OUT;
use rocketmq_error::PROTOCOL_BODY_INVALID;
use rocketmq_error::PROXY_BROKER_CONSUMER_GROUP_NOT_FOUND;
use rocketmq_error::PROXY_BROKER_OFFSET_INVALID;
use rocketmq_error::PROXY_BROKER_OFFSET_NOT_FOUND;
use rocketmq_error::PROXY_BROKER_PERMISSION_DENIED;
use rocketmq_error::PROXY_BROKER_REQUEST_UNSUPPORTED;
use rocketmq_error::PROXY_BROKER_RESOURCE_NOT_FOUND;
use rocketmq_error::PROXY_BROKER_RESPONSE_FAILED;
use rocketmq_error::PROXY_BROKER_TOPIC_NOT_FOUND;
use rocketmq_error::PROXY_CAPABILITY_UNSUPPORTED;
use rocketmq_error::PROXY_CAPACITY_EXHAUSTED;
use rocketmq_error::PROXY_CLIENT_ID_REQUIRED;
use rocketmq_error::PROXY_CLIENT_TYPE_UNRECOGNIZED;
use rocketmq_error::PROXY_DELIVERY_TIME_INVALID;
use rocketmq_error::PROXY_FILTER_EXPRESSION_INVALID;
use rocketmq_error::PROXY_INVISIBLE_TIME_INVALID;
use rocketmq_error::PROXY_LITE_SUBSCRIPTION_QUOTA_EXCEEDED;
use rocketmq_error::PROXY_LITE_TOPIC_INVALID;
use rocketmq_error::PROXY_MESSAGE_GROUP_INVALID;
use rocketmq_error::PROXY_MESSAGE_ID_INVALID;
use rocketmq_error::PROXY_MESSAGE_PROPERTY_CONFLICT;
use rocketmq_error::PROXY_METADATA_INVALID;
use rocketmq_error::PROXY_OFFSET_INVALID;
use rocketmq_error::PROXY_POLLING_TIME_INVALID;
use rocketmq_error::PROXY_RECEIPT_HANDLE_INVALID;
use rocketmq_error::PROXY_REQUEST_DRAINING;
use rocketmq_error::PROXY_SETTINGS_UNAVAILABLE;
use rocketmq_error::PROXY_TRANSACTION_ID_INVALID;
use rocketmq_error::PROXY_TRANSPORT_UNAVAILABLE;
use rocketmq_error::ROUTE_TOPIC_NOT_FOUND;
use rocketmq_protocol::code::response_code::ResponseCode;
use thiserror::Error;

pub type ProxyResult<T> = std::result::Result<T, ProxyError>;

#[derive(Debug, Error)]
pub enum ProxyError {
    #[error("{0}")]
    Canonical(#[source] CanonicalError),

    #[error("{0}")]
    SharedCanonical(#[source] SharedError),

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
    /// Returns the single catalog descriptor that owns this error's boundary behavior.
    pub fn descriptor(&self) -> &'static ErrorDescriptor {
        match self {
            Self::Canonical(error) => error.descriptor(),
            Self::SharedCanonical(error) => error.descriptor(),
            Self::BrokerResponse(error) => error.descriptor(),
            Self::ClientIdRequired => &PROXY_CLIENT_ID_REQUIRED,
            Self::UnrecognizedClientType(_) => &PROXY_CLIENT_TYPE_UNRECOGNIZED,
            Self::NotImplemented { .. } => &PROXY_CAPABILITY_UNSUPPORTED,
            Self::TooManyRequests { .. } => &PROXY_CAPACITY_EXHAUSTED,
            Self::Draining => &PROXY_REQUEST_DRAINING,
            Self::InvalidMetadata { .. } => &PROXY_METADATA_INVALID,
            Self::Transport { .. } => &PROXY_TRANSPORT_UNAVAILABLE,
            Self::IllegalMessageId { .. } => &PROXY_MESSAGE_ID_INVALID,
            Self::InvalidTransactionId { .. } => &PROXY_TRANSACTION_ID_INVALID,
            Self::IllegalMessageGroup { .. } => &PROXY_MESSAGE_GROUP_INVALID,
            Self::IllegalDeliveryTime { .. } => &PROXY_DELIVERY_TIME_INVALID,
            Self::IllegalPollingTime { .. } => &PROXY_POLLING_TIME_INVALID,
            Self::IllegalOffset { .. } => &PROXY_OFFSET_INVALID,
            Self::IllegalInvisibleTime { .. } => &PROXY_INVISIBLE_TIME_INVALID,
            Self::IllegalFilterExpression { .. } => &PROXY_FILTER_EXPRESSION_INVALID,
            Self::InvalidReceiptHandle { .. } => &PROXY_RECEIPT_HANDLE_INVALID,
            Self::IllegalLiteTopic { .. } => &PROXY_LITE_TOPIC_INVALID,
            Self::LiteSubscriptionQuotaExceeded { .. } => &PROXY_LITE_SUBSCRIPTION_QUOTA_EXCEEDED,
            Self::MessagePropertyConflictWithType { .. } => &PROXY_MESSAGE_PROPERTY_CONFLICT,
            Self::SettingsUnavailable { .. } => &PROXY_SETTINGS_UNAVAILABLE,
        }
    }

    /// Builds descriptor-declared diagnostic context without retaining raw values in public output.
    pub fn context(&self) -> ErrorContext {
        match self {
            Self::Canonical(error) => error.context().clone(),
            Self::SharedCanonical(error) => error.context().clone(),
            Self::BrokerResponse(error) => error.context().clone(),
            Self::ClientIdRequired | Self::UnrecognizedClientType(_) | Self::Draining => ErrorContext::new(),
            Self::NotImplemented { feature } => ErrorContext::new().with_text(fields::OPERATION_DIAGNOSTIC, *feature),
            Self::TooManyRequests { resource } => {
                ErrorContext::new().with_text(fields::OPERATION_DIAGNOSTIC, *resource)
            }
            Self::InvalidMetadata { .. }
            | Self::Transport { .. }
            | Self::IllegalMessageId { .. }
            | Self::InvalidTransactionId { .. }
            | Self::IllegalMessageGroup { .. }
            | Self::IllegalDeliveryTime { .. }
            | Self::IllegalPollingTime { .. }
            | Self::IllegalOffset { .. }
            | Self::IllegalInvisibleTime { .. }
            | Self::IllegalFilterExpression { .. }
            | Self::InvalidReceiptHandle { .. }
            | Self::IllegalLiteTopic { .. }
            | Self::LiteSubscriptionQuotaExceeded { .. }
            | Self::MessagePropertyConflictWithType { .. }
            | Self::SettingsUnavailable { .. } => ErrorContext::new().with_secret_presence(fields::MESSAGE_PRESENT),
        }
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

    /// Normalizes a client-originated broker response while retaining the
    /// canonical client error as the direct typed source.
    pub fn from_client_error(error: CanonicalError) -> Self {
        if error.descriptor() != &rocketmq_error::BROKER_OPERATION_FAILED {
            return Self::Canonical(error);
        }

        let mut broker_code = None;
        let mut broker_addr = None;
        if let Ok(view) = error.diagnostic_view() {
            for field in view.fields() {
                match (field.name(), field.value()) {
                    ("broker_code", ViewValueRef::I64(value)) => broker_code = i32::try_from(value).ok(),
                    ("broker_addr", ViewValueRef::Text(value)) => broker_addr = Some(value.to_owned()),
                    _ => {}
                }
            }
        }

        let Some(broker_code) = broker_code else {
            return Self::Canonical(error);
        };
        let descriptor = proxy_broker_response_descriptor(broker_code);
        let mut context = ErrorContext::new()
            .with_text(fields::OPERATION_DIAGNOSTIC, "proxy_client")
            .with_i64(fields::BROKER_CODE, i64::from(broker_code));
        if let Some(broker_addr) = broker_addr.as_deref() {
            context = context.with_text(fields::BROKER_ADDR, broker_addr);
        }
        context = context
            .with_secret_presence(fields::MESSAGE_PRESENT)
            .with_secret_presence(fields::SOURCE_PRESENT);
        Self::BrokerResponse(CanonicalError::caused_by(descriptor, error).with_context(context))
    }
}

impl From<CanonicalError> for ProxyError {
    fn from(error: CanonicalError) -> Self {
        Self::Canonical(error)
    }
}

impl From<SharedError> for ProxyError {
    fn from(error: SharedError) -> Self {
        Self::SharedCanonical(error)
    }
}

/// Canonical constructors used by Proxy boundaries.
///
/// The Proxy intentionally retains no second domain-error hierarchy. Each
/// helper chooses a catalog descriptor and stores request detail only as a
/// typed, redacted source or catalog-approved diagnostic context.
pub mod canonical {
    use std::fmt;

    use super::*;

    #[derive(Debug)]
    struct DiagnosticMessage(String);

    impl fmt::Display for DiagnosticMessage {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str(&self.0)
        }
    }

    impl std::error::Error for DiagnosticMessage {}

    fn message(descriptor: &'static ErrorDescriptor, value: impl Into<String>) -> CanonicalError {
        CanonicalError::caused_by(descriptor, DiagnosticMessage(value.into()))
            .with_context(ErrorContext::new().with_secret_presence(fields::MESSAGE_PRESENT))
    }

    fn message_with_source(
        descriptor: &'static ErrorDescriptor,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> CanonicalError {
        CanonicalError::caused_by(descriptor, source)
            .with_context(ErrorContext::new().with_secret_presence(fields::MESSAGE_PRESENT))
    }

    pub fn argument(value: impl Into<String>) -> CanonicalError {
        message(&CORE_ARGUMENT_INVALID, value)
    }

    pub fn argument_with_source(source: impl std::error::Error + Send + Sync + 'static) -> CanonicalError {
        message_with_source(&CORE_ARGUMENT_INVALID, source)
    }

    pub fn transport_unavailable_with_source(source: impl std::error::Error + Send + Sync + 'static) -> CanonicalError {
        message_with_source(&PROXY_TRANSPORT_UNAVAILABLE, source)
    }

    pub fn invalid_metadata_with_source(source: impl std::error::Error + Send + Sync + 'static) -> CanonicalError {
        message_with_source(&PROXY_METADATA_INVALID, source)
    }

    pub fn message_id_invalid_with_source(source: impl std::error::Error + Send + Sync + 'static) -> CanonicalError {
        message_with_source(&PROXY_MESSAGE_ID_INVALID, source)
    }

    pub fn transaction_id_invalid_with_source(
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> CanonicalError {
        message_with_source(&PROXY_TRANSACTION_ID_INVALID, source)
    }

    pub fn delivery_time_invalid_with_source(source: impl std::error::Error + Send + Sync + 'static) -> CanonicalError {
        message_with_source(&PROXY_DELIVERY_TIME_INVALID, source)
    }

    pub fn receipt_handle_invalid_with_source(
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> CanonicalError {
        message_with_source(&PROXY_RECEIPT_HANDLE_INVALID, source)
    }

    pub fn request_body_invalid(operation: &'static str, reason: impl Into<String>) -> CanonicalError {
        CanonicalError::caused_by(&PROTOCOL_BODY_INVALID, DiagnosticMessage(reason.into())).with_context(
            ErrorContext::new()
                .with_text(fields::OPERATION_DIAGNOSTIC, operation)
                .with_secret_presence(fields::INVALID_VALUE_PRESENT)
                .with_secret_presence(fields::SOURCE_PRESENT),
        )
    }

    pub fn configuration_parse_failed(key: &'static str, reason: impl Into<String>) -> CanonicalError {
        CanonicalError::caused_by(&CORE_CONFIGURATION_PARSE_FAILED, DiagnosticMessage(reason.into())).with_context(
            ErrorContext::new()
                .with_text(fields::KEY, key)
                .with_secret_presence(fields::REASON_PRESENT),
        )
    }

    pub fn configuration_parse_failed_with_source(
        key: &'static str,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> CanonicalError {
        CanonicalError::caused_by(&CORE_CONFIGURATION_PARSE_FAILED, source).with_context(
            ErrorContext::new()
                .with_text(fields::KEY, key)
                .with_secret_presence(fields::REASON_PRESENT),
        )
    }

    pub fn configuration_invalid(key: &'static str, reason: impl Into<String>) -> CanonicalError {
        CanonicalError::caused_by(&CORE_CONFIGURATION_INVALID, DiagnosticMessage(reason.into())).with_context(
            ErrorContext::new()
                .with_text(fields::KEY, key)
                .with_secret_presence(fields::VALUE_PRESENT)
                .with_secret_presence(fields::REASON_PRESENT),
        )
    }

    pub fn configuration_invalid_with_source(
        key: &'static str,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> CanonicalError {
        CanonicalError::caused_by(&CORE_CONFIGURATION_INVALID, source).with_context(
            ErrorContext::new()
                .with_text(fields::KEY, key)
                .with_secret_presence(fields::VALUE_PRESENT)
                .with_secret_presence(fields::REASON_PRESENT),
        )
    }

    pub fn authentication_failed(_operation: &'static str, reason: impl Into<String>) -> CanonicalError {
        CanonicalError::caused_by(&AUTH_CREDENTIALS_INVALID, DiagnosticMessage(reason.into()))
            .with_context(ErrorContext::new().with_secret_presence(fields::CREDENTIALS_PRESENT))
    }

    pub fn authentication_failed_with_source(
        _operation: &'static str,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> CanonicalError {
        CanonicalError::caused_by(&AUTH_CREDENTIALS_INVALID, source)
            .with_context(ErrorContext::new().with_secret_presence(fields::CREDENTIALS_PRESENT))
    }

    pub fn authentication_operation_failed(operation: &'static str, reason: impl Into<String>) -> CanonicalError {
        CanonicalError::caused_by(&AUTH_OPERATION_FAILED, DiagnosticMessage(reason.into())).with_context(
            ErrorContext::new()
                .with_text(fields::OPERATION_DIAGNOSTIC, operation)
                .with_secret_presence(fields::SOURCE_PRESENT),
        )
    }

    pub fn authorization_denied(operation: &'static str) -> CanonicalError {
        CanonicalError::new(&AUTH_PERMISSION_DENIED)
            .with_context(ErrorContext::new().with_text(fields::OPERATION_DIAGNOSTIC, operation))
    }

    pub fn timed_out(operation: &'static str, timeout_ms: u64) -> CanonicalError {
        CanonicalError::new(&CORE_OPERATION_TIMED_OUT).with_context(
            ErrorContext::new()
                .with_text(fields::OPERATION_DIAGNOSTIC, operation)
                .with_u64(fields::TIMEOUT_MS, timeout_ms),
        )
    }

    pub fn broker_not_found(name: impl AsRef<str>) -> CanonicalError {
        CanonicalError::new(&BROKER_LOOKUP_NOT_FOUND)
            .with_context(ErrorContext::new().with_text(fields::BROKER, name.as_ref()))
    }

    pub fn topic_not_found(topic: impl AsRef<str>) -> CanonicalError {
        CanonicalError::new(&BROKER_TOPIC_NOT_FOUND)
            .with_context(ErrorContext::new().with_text(fields::TOPIC, topic.as_ref()))
    }

    pub fn route_not_found(topic: impl AsRef<str>) -> CanonicalError {
        CanonicalError::new(&ROUTE_TOPIC_NOT_FOUND)
            .with_context(ErrorContext::new().with_text(fields::TOPIC, topic.as_ref()))
    }

    pub fn subscription_group_not_found(group: impl AsRef<str>) -> CanonicalError {
        CanonicalError::new(&BROKER_SUBSCRIPTION_GROUP_NOT_FOUND)
            .with_context(ErrorContext::new().with_text(fields::GROUP, group.as_ref()))
    }

    pub fn queue_not_found(topic: impl AsRef<str>, queue_id: i32) -> CanonicalError {
        CanonicalError::new(&BROKER_QUEUE_NOT_FOUND).with_context(
            ErrorContext::new()
                .with_text(fields::TOPIC, topic.as_ref())
                .with_i64(fields::QUEUE_ID, i64::from(queue_id)),
        )
    }

    pub fn queue_id_out_of_range(topic: impl AsRef<str>, queue_id: i32, max_queue_id: i32) -> CanonicalError {
        CanonicalError::new(&BROKER_QUEUE_ID_OUT_OF_RANGE).with_context(
            ErrorContext::new()
                .with_text(fields::TOPIC, topic.as_ref())
                .with_i64(fields::QUEUE_ID, i64::from(queue_id))
                .with_i64(fields::MAX_QUEUE_ID, i64::from(max_queue_id)),
        )
    }

    pub fn retry_budget_exhausted(group: impl AsRef<str>, current: i32, max: i32) -> CanonicalError {
        CanonicalError::new(&CLIENT_RETRY_BUDGET_EXHAUSTED).with_context(
            ErrorContext::new()
                .with_text(fields::GROUP, group.as_ref())
                .with_i64(fields::CURRENT, i64::from(current))
                .with_i64(fields::MAX, i64::from(max)),
        )
    }

    pub fn not_master(master_address: impl AsRef<str>) -> CanonicalError {
        CanonicalError::new(&BROKER_LEADERSHIP_NOT_MASTER)
            .with_context(ErrorContext::new().with_text(fields::MASTER_ADDRESS, master_address.as_ref()))
    }

    pub fn internal(operation: &'static str, reason: impl Into<String>) -> CanonicalError {
        CanonicalError::caused_by(&CORE_INTERNAL_FAILURE, DiagnosticMessage(reason.into())).with_context(
            ErrorContext::new()
                .with_text(fields::OPERATION_DIAGNOSTIC, operation)
                .with_secret_presence(fields::SOURCE_PRESENT),
        )
    }

    pub fn internal_with_source(
        operation: &'static str,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> CanonicalError {
        CanonicalError::caused_by(&CORE_INTERNAL_FAILURE, source).with_context(
            ErrorContext::new()
                .with_text(fields::OPERATION_DIAGNOSTIC, operation)
                .with_secret_presence(fields::SOURCE_PRESENT),
        )
    }

    pub fn broker_response(
        operation: &'static str,
        code: i32,
        broker_addr: Option<&str>,
        message: impl Into<String>,
    ) -> CanonicalError {
        let descriptor = proxy_broker_response_descriptor(code);
        let mut context = ErrorContext::new()
            .with_text(fields::OPERATION_DIAGNOSTIC, operation)
            .with_i64(fields::BROKER_CODE, i64::from(code));
        if let Some(broker_addr) = broker_addr {
            context = context.with_text(fields::BROKER_ADDR, broker_addr);
        }
        context = context
            .with_secret_presence(fields::MESSAGE_PRESENT)
            .with_secret_presence(fields::SOURCE_PRESENT);
        CanonicalError::caused_by(descriptor, DiagnosticMessage(message.into())).with_context(context)
    }

    pub fn broker_operation(
        operation: &'static str,
        code: i32,
        broker_addr: Option<&str>,
        message: impl Into<String>,
    ) -> CanonicalError {
        let mut context = ErrorContext::new()
            .with_text(fields::OPERATION_DIAGNOSTIC, operation)
            .with_i64(fields::BROKER_CODE, i64::from(code))
            .with_secret_presence(fields::MESSAGE_PRESENT);
        if let Some(broker_addr) = broker_addr {
            context = context.with_text(fields::BROKER_ADDR, broker_addr);
        }
        CanonicalError::caused_by(&BROKER_OPERATION_FAILED, DiagnosticMessage(message.into())).with_context(context)
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
        ProxyError::BrokerResponse(canonical::broker_response(
            "BROKER_TEST",
            code.to_i32(),
            Some("127.0.0.1:10911"),
            message,
        ))
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
                panic!("broker response must normalize at Proxy ingress");
            };
            assert_eq!(error.descriptor(), descriptor);
        }

        let ProxyError::BrokerResponse(error) =
            ProxyError::BrokerResponse(canonical::broker_response("BROKER_TEST", 987_654, None, "unknown code"))
        else {
            panic!("broker response must normalize at Proxy ingress");
        };
        assert_eq!(error.descriptor(), &PROXY_BROKER_RESPONSE_FAILED);
    }

    #[test]
    fn normalized_broker_response_retains_typed_source_and_diagnostic_origin() {
        let proxy_error = normalized_broker_error(ResponseCode::TopicNotExist, "secret\r\nC:\\private\\broker.conf");
        let canonical = StdError::source(&proxy_error)
            .and_then(|source| source.downcast_ref::<CanonicalError>())
            .expect("ProxyError retains the normalized canonical carrier");
        assert!(
            StdError::source(canonical).is_some(),
            "normalized response retains its typed source"
        );

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
    fn non_broker_canonical_errors_keep_the_existing_proxy_variant() {
        let error = ProxyError::from(canonical::argument("invalid request"));
        assert!(matches!(error, ProxyError::Canonical(_)));
    }

    #[test]
    fn transport_source_preserves_its_typed_cause_and_projection() {
        let error = ProxyError::from(canonical::transport_unavailable_with_source(std::io::Error::other(
            "injected transport failure",
        )));

        assert_eq!(error.descriptor(), &PROXY_TRANSPORT_UNAVAILABLE);
        let ProxyError::Canonical(error) = error else {
            panic!("source-bearing transport errors must use the canonical proxy carrier");
        };
        assert!(StdError::source(&error)
            .and_then(|source| source.downcast_ref::<std::io::Error>())
            .is_some());
    }

    #[test]
    fn shared_canonical_errors_remain_in_the_standard_source_chain() {
        let physical = std::sync::Arc::new(CanonicalError::caused_by(
            &CORE_INTERNAL_FAILURE,
            std::io::Error::other("injected failure"),
        ));
        let proxy = ProxyError::SharedCanonical(std::sync::Arc::clone(&physical));

        let source = StdError::source(&proxy).expect("shared canonical error must be a source");
        assert_eq!(source.to_string(), physical.to_string());
        assert!(StdError::source(source)
            .and_then(|source| source.downcast_ref::<std::io::Error>())
            .is_some());
    }
}
