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

use std::error::Error as StdError;
use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

use rocketmq_error::fields;
use rocketmq_error::Error;
use rocketmq_error::ErrorContext;
use rocketmq_error::ErrorDescriptor;
use rocketmq_error::SharedError;
use rocketmq_error::ViewValueRef;

/// Opaque client boundary error backed by one shared canonical error.
///
/// Client operations cross asynchronous callbacks and retry coordinators, so
/// the canonical value is shared rather than copied or flattened into text.
#[derive(Clone)]
pub struct ClientError(SharedError);

/// Result returned by RocketMQ client operations.
pub type ClientResult<T> = std::result::Result<T, ClientError>;

impl ClientError {
    /// Promotes an owned canonical error into the client boundary.
    #[must_use]
    pub fn from_error(error: Error) -> Self {
        Self(Arc::new(error))
    }

    /// Retains an already shared canonical error at the client boundary.
    #[must_use]
    pub const fn from_shared(error: SharedError) -> Self {
        Self(error)
    }

    /// Returns the canonical error descriptor.
    #[must_use]
    pub fn descriptor(&self) -> &'static ErrorDescriptor {
        self.0.descriptor()
    }

    /// Returns whether this error has the supplied canonical identity.
    #[must_use]
    pub fn is(&self, descriptor: &'static ErrorDescriptor) -> bool {
        self.descriptor().code() == descriptor.code()
    }

    /// Returns the retained broker response code when this is a broker failure.
    #[must_use]
    pub fn broker_response_code(&self) -> Option<i32> {
        self.diagnostic_i64(fields::BROKER_CODE.schema().name())
            .and_then(|code| i32::try_from(code).ok())
    }

    /// Returns the bounded diagnostic broker address when present.
    #[must_use]
    pub fn broker_addr(&self) -> Option<&str> {
        self.diagnostic_text(fields::BROKER_ADDR.schema().name())
    }

    /// Borrows the shared canonical error.
    #[must_use]
    pub const fn shared_error(&self) -> &SharedError {
        &self.0
    }

    /// Finds a typed cause anywhere below the canonical client error.
    #[must_use]
    pub fn source_ref<T>(&self) -> Option<&T>
    where
        T: StdError + 'static,
    {
        let mut current: &(dyn StdError + 'static) = self.0.as_ref();
        while let Some(source) = current.source() {
            if let Some(found) = source.downcast_ref::<T>() {
                return Some(found);
            }
            current = source;
        }
        None
    }

    /// Returns the shared canonical error.
    #[must_use]
    pub fn into_shared_error(self) -> SharedError {
        self.0
    }

    /// Creates an invalid-argument failure without retaining arbitrary text.
    #[must_use]
    pub fn illegal_argument(message: impl Into<String>) -> Self {
        let message = message.into();
        let mut context = ErrorContext::new();
        if !message.is_empty() {
            context = context.with_secret_presence(fields::MESSAGE_PRESENT);
        }
        Self::from_error(Error::new(&rocketmq_error::CORE_ARGUMENT_INVALID).with_context(context))
    }

    /// Creates an invalid-argument failure with a typed source.
    #[must_use]
    pub fn illegal_argument_source(source: impl StdError + Send + Sync + 'static) -> Self {
        Self::from_error(
            Error::caused_by(&rocketmq_error::CORE_ARGUMENT_INVALID, source)
                .with_context(ErrorContext::new().with_secret_presence(fields::MESSAGE_PRESENT)),
        )
    }

    /// Creates a client-not-started lifecycle failure.
    #[must_use]
    pub fn not_started() -> Self {
        Self::from_error(Error::new(&rocketmq_error::CLIENT_LIFECYCLE_NOT_STARTED))
    }

    /// Creates a client-already-started lifecycle failure.
    #[must_use]
    pub fn already_started() -> Self {
        Self::from_error(Error::new(&rocketmq_error::CLIENT_LIFECYCLE_ALREADY_STARTED))
    }

    /// Creates a client-shutting-down lifecycle failure.
    #[must_use]
    pub fn shutting_down() -> Self {
        Self::from_error(Error::new(&rocketmq_error::CLIENT_LIFECYCLE_SHUTTING_DOWN))
    }

    /// Creates an invalid client-state failure.
    #[must_use]
    pub fn invalid_state(expected: impl AsRef<str>, actual: impl AsRef<str>) -> Self {
        let context = ErrorContext::new()
            .with_text(fields::EXPECTED_STATE, expected)
            .with_text(fields::ACTUAL_STATE, actual);
        Self::from_error(Error::new(&rocketmq_error::CLIENT_LIFECYCLE_INVALID_STATE).with_context(context))
    }

    /// Creates an unavailable client-component failure.
    #[must_use]
    pub fn component_unavailable(role: impl AsRef<str>) -> Self {
        let context = ErrorContext::new().with_text(fields::CLIENT_ROLE, role);
        Self::from_error(Error::new(&rocketmq_error::CLIENT_COMPONENT_UNAVAILABLE).with_context(context))
    }

    /// Creates a component-not-initialized failure.
    #[must_use]
    pub fn not_initialized(component: impl AsRef<str>) -> Self {
        let context = ErrorContext::new().with_text(fields::COMPONENT_NAME, component);
        Self::from_error(Error::new(&rocketmq_error::CORE_LIFECYCLE_NOT_INITIALIZED).with_context(context))
    }

    /// Creates a configuration-missing failure.
    #[must_use]
    pub fn config_missing(key: impl AsRef<str>) -> Self {
        let context = ErrorContext::new().with_text(fields::KEY, key);
        Self::from_error(Error::new(&rocketmq_error::CORE_CONFIGURATION_MISSING).with_context(context))
    }

    /// Creates an invalid-configuration failure.
    #[must_use]
    pub fn config_invalid(key: impl AsRef<str>, value: impl AsRef<str>, reason: impl AsRef<str>) -> Self {
        let mut context = ErrorContext::new().with_text(fields::KEY, key);
        if !value.as_ref().is_empty() {
            context = context.with_secret_presence(fields::VALUE_PRESENT);
        }
        if !reason.as_ref().is_empty() {
            context = context.with_secret_presence(fields::REASON_PRESENT);
        }
        Self::from_error(Error::new(&rocketmq_error::CORE_CONFIGURATION_INVALID).with_context(context))
    }

    /// Creates an invalid-configuration failure with a typed source.
    #[must_use]
    pub fn config_invalid_source(
        key: impl AsRef<str>,
        value_present: bool,
        source: impl StdError + Send + Sync + 'static,
    ) -> Self {
        let mut context = ErrorContext::new().with_text(fields::KEY, key);
        if value_present {
            context = context.with_secret_presence(fields::VALUE_PRESENT);
        }
        context = context.with_secret_presence(fields::REASON_PRESENT);
        Self::from_error(Error::caused_by(&rocketmq_error::CORE_CONFIGURATION_INVALID, source).with_context(context))
    }

    /// Creates a configuration-parse failure.
    #[must_use]
    pub fn config_parse_failed(key: impl AsRef<str>, reason: impl AsRef<str>) -> Self {
        let mut context = ErrorContext::new().with_text(fields::KEY, key);
        if !reason.as_ref().is_empty() {
            context = context.with_secret_presence(fields::REASON_PRESENT);
        }
        Self::from_error(Error::new(&rocketmq_error::CORE_CONFIGURATION_PARSE_FAILED).with_context(context))
    }

    /// Creates a configuration-parse failure with a typed source.
    #[must_use]
    pub fn config_parse_source(key: impl AsRef<str>, source: impl StdError + Send + Sync + 'static) -> Self {
        let context = ErrorContext::new()
            .with_text(fields::KEY, key)
            .with_secret_presence(fields::REASON_PRESENT);
        Self::from_error(
            Error::caused_by(&rocketmq_error::CORE_CONFIGURATION_PARSE_FAILED, source).with_context(context),
        )
    }

    /// Creates a response-processing failure.
    #[must_use]
    pub fn response_process_failed(operation: impl AsRef<str>, reason: impl AsRef<str>) -> Self {
        let mut context = ErrorContext::new().with_text(fields::OPERATION_DIAGNOSTIC, operation);
        if !reason.as_ref().is_empty() {
            context = context.with_secret_presence(fields::REASON_PRESENT);
        }
        Self::from_error(Error::new(&rocketmq_error::PROTOCOL_RESPONSE_FAILED).with_context(context))
    }

    /// Creates a response-processing failure with a typed source.
    #[must_use]
    pub fn response_process_source(operation: impl AsRef<str>, source: impl StdError + Send + Sync + 'static) -> Self {
        let context = ErrorContext::new()
            .with_text(fields::OPERATION_DIAGNOSTIC, operation)
            .with_secret_presence(fields::REASON_PRESENT);
        Self::from_error(Error::caused_by(&rocketmq_error::PROTOCOL_RESPONSE_FAILED, source).with_context(context))
    }

    /// Creates an operation-timeout failure.
    #[must_use]
    pub fn timeout(operation: impl AsRef<str>, timeout_ms: u64) -> Self {
        let context = ErrorContext::new()
            .with_text(fields::OPERATION_DIAGNOSTIC, operation)
            .with_u64(fields::TIMEOUT_MS, timeout_ms);
        Self::from_error(Error::new(&rocketmq_error::CORE_OPERATION_TIMED_OUT).with_context(context))
    }

    /// Creates an internal failure while retaining its typed source.
    #[must_use]
    pub fn internal(operation: impl AsRef<str>, source: impl StdError + Send + Sync + 'static) -> Self {
        let context = ErrorContext::new()
            .with_text(fields::OPERATION_DIAGNOSTIC, operation)
            .with_secret_presence(fields::SOURCE_PRESENT);
        Self::from_error(Error::caused_by(&rocketmq_error::CORE_INTERNAL_FAILURE, source).with_context(context))
    }

    /// Creates an invariant failure.
    #[must_use]
    pub fn invariant_violated(invariant: impl AsRef<str>) -> Self {
        let context = ErrorContext::new().with_text(fields::OPERATION_DIAGNOSTIC, invariant);
        Self::from_error(Error::new(&rocketmq_error::CORE_INTERNAL_FAILURE).with_context(context))
    }

    /// Creates a service lifecycle failure.
    #[must_use]
    pub fn service_failed(operation: impl AsRef<str>) -> Self {
        let context = ErrorContext::new().with_text(fields::OPERATION_DIAGNOSTIC, operation);
        Self::from_error(Error::new(&rocketmq_error::CORE_SERVICE_FAILED).with_context(context))
    }

    /// Creates a service lifecycle failure with a typed source.
    #[must_use]
    pub fn service_source(operation: impl AsRef<str>, source: impl StdError + Send + Sync + 'static) -> Self {
        let context = ErrorContext::new().with_text(fields::OPERATION_DIAGNOSTIC, operation);
        Self::from_error(Error::caused_by(&rocketmq_error::CORE_SERVICE_FAILED, source).with_context(context))
    }

    /// Creates an invalid-message-property failure.
    #[must_use]
    pub fn invalid_property(property: impl AsRef<str>) -> Self {
        let context = ErrorContext::new().with_text(fields::PROPERTY, property);
        Self::from_error(Error::new(&rocketmq_error::PROTOCOL_MESSAGE_PROPERTY_INVALID).with_context(context))
    }

    /// Creates a broker-lookup failure.
    #[must_use]
    pub fn broker_not_found(broker: impl AsRef<str>) -> Self {
        let context = ErrorContext::new().with_text(fields::BROKER, broker);
        Self::from_error(Error::new(&rocketmq_error::BROKER_LOOKUP_NOT_FOUND).with_context(context))
    }

    /// Creates a broker-operation failure.
    #[must_use]
    pub fn broker_operation_failed(operation: impl AsRef<str>, code: i32, message: impl AsRef<str>) -> Self {
        let mut context = ErrorContext::new()
            .with_text(fields::OPERATION_DIAGNOSTIC, operation)
            .with_i64(fields::BROKER_CODE, i64::from(code));
        if !message.as_ref().is_empty() {
            context = context.with_secret_presence(fields::MESSAGE_PRESENT);
        }
        Self::from_error(Error::new(&rocketmq_error::BROKER_OPERATION_FAILED).with_context(context))
    }

    /// Creates a broker-operation failure with an optional broker address.
    #[must_use]
    pub fn broker_operation_failed_at(
        operation: impl AsRef<str>,
        code: i32,
        message: impl AsRef<str>,
        broker_addr: Option<impl AsRef<str>>,
    ) -> Self {
        let error = Self::broker_operation_failed(operation, code, message);
        match broker_addr {
            Some(broker_addr) => error.with_broker_addr(broker_addr),
            None => error,
        }
    }

    /// Creates a broker-operation failure while retaining its typed source.
    #[must_use]
    pub fn broker_operation_source(
        operation: impl AsRef<str>,
        code: i32,
        broker_addr: Option<impl AsRef<str>>,
        source: impl StdError + Send + Sync + 'static,
    ) -> Self {
        let mut context = ErrorContext::new()
            .with_text(fields::OPERATION_DIAGNOSTIC, operation)
            .with_i64(fields::BROKER_CODE, i64::from(code))
            .with_secret_presence(fields::MESSAGE_PRESENT);
        if let Some(broker_addr) = broker_addr {
            context = context.with_text(fields::BROKER_ADDR, broker_addr);
        }
        Self::from_error(Error::caused_by(&rocketmq_error::BROKER_OPERATION_FAILED, source).with_context(context))
    }

    /// Adds a broker address to a newly created broker-operation failure.
    #[must_use]
    pub fn with_broker_addr(self, broker_addr: impl AsRef<str>) -> Self {
        match Arc::try_unwrap(self.0) {
            Ok(error) => {
                let context = error.context().clone().with_text(fields::BROKER_ADDR, broker_addr);
                Self::from_error(error.with_context(context))
            }
            Err(shared) => Self(shared),
        }
    }

    /// Creates a route-not-found failure.
    #[must_use]
    pub fn route_not_found(topic: impl AsRef<str>) -> Self {
        let context = ErrorContext::new().with_text(fields::TOPIC, topic);
        Self::from_error(Error::new(&rocketmq_error::ROUTE_TOPIC_NOT_FOUND).with_context(context))
    }

    /// Creates a DNS-resolution failure without exposing endpoint details.
    #[must_use]
    pub fn dns_failed() -> Self {
        Self::from_error(Error::new(&rocketmq_error::TRANSPORT_DNS_FAILED))
    }

    /// Creates a DNS-resolution failure while retaining its typed source.
    #[must_use]
    pub fn dns_failed_source(source: impl StdError + Send + Sync + 'static) -> Self {
        let context = ErrorContext::new().with_secret_presence(fields::SOURCE_PRESENT);
        Self::from_error(Error::caused_by(&rocketmq_error::TRANSPORT_DNS_FAILED, source).with_context(context))
    }

    fn diagnostic_i64(&self, name: &str) -> Option<i64> {
        self.0
            .diagnostic_view()
            .ok()?
            .fields()
            .find_map(|field| match (field.name() == name, field.value()) {
                (true, ViewValueRef::I64(value)) => Some(value),
                _ => None,
            })
    }

    fn diagnostic_text(&self, name: &str) -> Option<&str> {
        self.0
            .diagnostic_view()
            .ok()?
            .fields()
            .find_map(|field| match (field.name() == name, field.value()) {
                (true, ViewValueRef::Text(value)) => Some(value),
                _ => None,
            })
    }
}

impl From<Error> for ClientError {
    fn from(error: Error) -> Self {
        Self::from_error(error)
    }
}

impl From<SharedError> for ClientError {
    fn from(error: SharedError) -> Self {
        Self::from_shared(error)
    }
}

impl From<ClientError> for SharedError {
    fn from(error: ClientError) -> Self {
        error.into_shared_error()
    }
}

impl AsRef<Error> for ClientError {
    fn as_ref(&self) -> &Error {
        self.0.as_ref()
    }
}

impl Deref for ClientError {
    type Target = Error;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl fmt::Display for ClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl fmt::Debug for ClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl StdError for ClientError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(self.0.as_ref())
    }
}
