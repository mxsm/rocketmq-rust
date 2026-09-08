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

//! Broker-local constructors for the workspace canonical error envelope.

use std::error::Error as StdError;
use std::sync::Arc;

use rocketmq_auth::AuthServiceError;
use rocketmq_error::fields;
use rocketmq_error::Error;
use rocketmq_error::ErrorContext;
use rocketmq_error::SharedError;
use rocketmq_error::AUTH_CONFIGURATION_INVALID;
use rocketmq_error::AUTH_CONFIGURATION_RELOAD_FAILED;
use rocketmq_error::AUTH_CREDENTIALS_INVALID;
use rocketmq_error::AUTH_PERMISSION_DENIED;
use rocketmq_error::BROKER_OPERATION_FAILED;
use rocketmq_error::BROKER_TASK_FAILED;
use rocketmq_error::BROKER_TOPIC_NOT_FOUND;
use rocketmq_error::CLIENT_LIFECYCLE_INVALID_STATE;
use rocketmq_error::CLIENT_LIFECYCLE_NOT_STARTED;
use rocketmq_error::CORE_ARGUMENT_INVALID;
use rocketmq_error::CORE_CONFIGURATION_INVALID;
use rocketmq_error::CORE_INTERNAL_FAILURE;
use rocketmq_error::CORE_IO_FAILED;
use rocketmq_error::CORE_LIFECYCLE_NOT_INITIALIZED;
use rocketmq_error::CORE_OPERATION_TIMED_OUT;
use rocketmq_error::CORE_SERIALIZATION_FAILED;
use rocketmq_error::CORE_SERVICE_FAILED;
use rocketmq_error::PROTOCOL_BODY_INVALID;
use rocketmq_error::PROTOCOL_HEADER_INVALID;
use rocketmq_error::PROTOCOL_RESPONSE_FAILED;
use rocketmq_error::ROUTE_TOPIC_INCONSISTENT;
use rocketmq_error::STORAGE_CAPACITY_EXHAUSTED;
use rocketmq_error::STORAGE_READ_FAILED;
use rocketmq_error::STORAGE_WRITE_FAILED;

pub(crate) type BrokerResult<T> = std::result::Result<T, SharedError>;

fn shared(error: Error) -> SharedError {
    Arc::new(error)
}

pub(crate) fn from_shared(source: SharedError) -> SharedError {
    source
}

pub(crate) fn from_canonical(error: Error) -> SharedError {
    shared(error)
}

pub(crate) fn auth_service_error(error: AuthServiceError) -> SharedError {
    shared(Error::from(error))
}

pub(crate) fn client_not_started() -> SharedError {
    shared(Error::new(&CLIENT_LIFECYCLE_NOT_STARTED))
}

pub(crate) fn client_invalid_state(expected: &'static str, actual: impl AsRef<str>) -> SharedError {
    shared(
        Error::new(&CLIENT_LIFECYCLE_INVALID_STATE).with_context(
            ErrorContext::new()
                .with_text(fields::EXPECTED_STATE, expected)
                .with_text(fields::ACTUAL_STATE, actual),
        ),
    )
}

pub(crate) fn invalid_argument(_message: impl Into<String>) -> SharedError {
    shared(
        Error::new(&CORE_ARGUMENT_INVALID)
            .with_context(ErrorContext::new().with_secret_presence(fields::MESSAGE_PRESENT)),
    )
}

pub(crate) fn invalid_argument_source(source: impl StdError + Send + Sync + 'static) -> SharedError {
    shared(
        Error::caused_by(&CORE_ARGUMENT_INVALID, source)
            .with_context(ErrorContext::new().with_secret_presence(fields::MESSAGE_PRESENT)),
    )
}

pub(crate) fn internal(operation: &'static str, source: impl StdError + Send + Sync + 'static) -> SharedError {
    shared(
        Error::caused_by(&CORE_INTERNAL_FAILURE, source).with_context(
            ErrorContext::new()
                .with_text(fields::OPERATION_DIAGNOSTIC, operation)
                .with_secret_presence(fields::SOURCE_PRESENT),
        ),
    )
}

pub(crate) fn invariant_violated(_invariant: &'static str) -> SharedError {
    shared(
        Error::new(&CORE_INTERNAL_FAILURE)
            .with_context(ErrorContext::new().with_text(fields::OPERATION_DIAGNOSTIC, "invariant_violation")),
    )
}

pub(crate) fn configuration_invalid(key: &'static str) -> SharedError {
    shared(
        Error::new(&CORE_CONFIGURATION_INVALID).with_context(
            ErrorContext::new()
                .with_text(fields::KEY, key)
                .with_secret_presence(fields::VALUE_PRESENT)
                .with_secret_presence(fields::REASON_PRESENT),
        ),
    )
}

pub(crate) fn configuration_invalid_source(
    key: &'static str,
    source: impl StdError + Send + Sync + 'static,
) -> SharedError {
    shared(
        Error::caused_by(&CORE_CONFIGURATION_INVALID, source).with_context(
            ErrorContext::new()
                .with_text(fields::KEY, key)
                .with_secret_presence(fields::VALUE_PRESENT)
                .with_secret_presence(fields::REASON_PRESENT),
        ),
    )
}

pub(crate) fn io(source: std::io::Error) -> SharedError {
    shared(
        Error::caused_by(&CORE_IO_FAILED, source).with_context(
            ErrorContext::new()
                .with_text(fields::OPERATION_DIAGNOSTIC, "io")
                .with_secret_presence(fields::SOURCE_PRESENT),
        ),
    )
}

pub(crate) fn timeout(operation: &'static str, timeout_ms: u64) -> SharedError {
    shared(
        Error::new(&CORE_OPERATION_TIMED_OUT).with_context(
            ErrorContext::new()
                .with_text(fields::OPERATION_DIAGNOSTIC, operation)
                .with_u64(fields::TIMEOUT_MS, timeout_ms),
        ),
    )
}

pub(crate) fn service_failed(operation: &'static str) -> SharedError {
    shared(
        Error::new(&CORE_SERVICE_FAILED)
            .with_context(ErrorContext::new().with_text(fields::OPERATION_DIAGNOSTIC, operation)),
    )
}

pub(crate) fn not_initialized(component: impl AsRef<str>) -> SharedError {
    shared(
        Error::new(&CORE_LIFECYCLE_NOT_INITIALIZED).with_context(
            ErrorContext::new()
                .with_text(fields::COMPONENT_NAME, component)
                .with_secret_presence(fields::REASON_PRESENT),
        ),
    )
}

pub(crate) fn not_initialized_source(
    component: impl AsRef<str>,
    source: impl StdError + Send + Sync + 'static,
) -> SharedError {
    shared(
        Error::caused_by(&CORE_LIFECYCLE_NOT_INITIALIZED, source).with_context(
            ErrorContext::new()
                .with_text(fields::COMPONENT_NAME, component)
                .with_secret_presence(fields::REASON_PRESENT),
        ),
    )
}

pub(crate) fn serialization_failed(
    operation: &'static str,
    format: &'static str,
    source: impl StdError + Send + Sync + 'static,
) -> SharedError {
    shared(
        Error::caused_by(&CORE_SERIALIZATION_FAILED, source).with_context(
            ErrorContext::new()
                .with_text(fields::OPERATION_DIAGNOSTIC, operation)
                .with_text(fields::FORMAT, format)
                .with_secret_presence(fields::SOURCE_PRESENT),
        ),
    )
}

pub(crate) fn serialization_failure(operation: &'static str, format: impl AsRef<str>) -> SharedError {
    shared(
        Error::new(&CORE_SERIALIZATION_FAILED).with_context(
            ErrorContext::new()
                .with_text(fields::OPERATION_DIAGNOSTIC, operation)
                .with_text(fields::FORMAT, format)
                .with_secret_presence(fields::DETAIL_PRESENT),
        ),
    )
}

pub(crate) fn request_body_invalid(operation: &'static str, _reason: impl Into<String>) -> SharedError {
    shared(
        Error::new(&PROTOCOL_BODY_INVALID).with_context(
            ErrorContext::new()
                .with_text(fields::OPERATION_DIAGNOSTIC, operation)
                .with_secret_presence(fields::INVALID_VALUE_PRESENT),
        ),
    )
}

pub(crate) fn request_body_source(
    operation: &'static str,
    source: impl StdError + Send + Sync + 'static,
) -> SharedError {
    shared(
        Error::caused_by(&PROTOCOL_BODY_INVALID, source).with_context(
            ErrorContext::new()
                .with_text(fields::OPERATION_DIAGNOSTIC, operation)
                .with_secret_presence(fields::SOURCE_PRESENT),
        ),
    )
}

pub(crate) fn request_header_error(_message: impl Into<String>) -> SharedError {
    shared(
        Error::new(&PROTOCOL_HEADER_INVALID)
            .with_context(ErrorContext::new().with_secret_presence(fields::INVALID_VALUE_PRESENT)),
    )
}

pub(crate) fn request_header_source(
    operation: &'static str,
    source: impl StdError + Send + Sync + 'static,
) -> SharedError {
    shared(
        Error::caused_by(&PROTOCOL_HEADER_INVALID, source).with_context(
            ErrorContext::new()
                .with_text(fields::OPERATION_DIAGNOSTIC, operation)
                .with_secret_presence(fields::SOURCE_PRESENT),
        ),
    )
}

pub(crate) fn response_process_failed(operation: &'static str, _reason: impl Into<String>) -> SharedError {
    shared(
        Error::new(&PROTOCOL_RESPONSE_FAILED).with_context(
            ErrorContext::new()
                .with_text(fields::OPERATION_DIAGNOSTIC, operation)
                .with_secret_presence(fields::REASON_PRESENT),
        ),
    )
}

pub(crate) fn response_process_source(
    operation: &'static str,
    source: impl StdError + Send + Sync + 'static,
) -> SharedError {
    shared(
        Error::caused_by(&PROTOCOL_RESPONSE_FAILED, source).with_context(
            ErrorContext::new()
                .with_text(fields::OPERATION_DIAGNOSTIC, operation)
                .with_secret_presence(fields::REASON_PRESENT),
        ),
    )
}

pub(crate) fn broker_operation_failed(operation: &'static str, code: i32, _message: impl Into<String>) -> SharedError {
    broker_operation_failed_with_address(operation, code, _message, None)
}

pub(crate) fn broker_operation_failed_with_address(
    operation: &'static str,
    code: i32,
    _message: impl Into<String>,
    broker_addr: Option<String>,
) -> SharedError {
    let mut context = ErrorContext::new()
        .with_text(fields::OPERATION_DIAGNOSTIC, operation)
        .with_i64(fields::BROKER_CODE, i64::from(code))
        .with_secret_presence(fields::MESSAGE_PRESENT);
    if let Some(broker_addr) = broker_addr {
        context = context.with_text(fields::BROKER_ADDR, broker_addr);
    }
    shared(Error::new(&BROKER_OPERATION_FAILED).with_context(context))
}

pub(crate) fn broker_operation_source(
    operation: &'static str,
    code: i32,
    broker_addr: impl AsRef<str>,
    source: impl StdError + Send + Sync + 'static,
) -> SharedError {
    shared(
        Error::caused_by(&BROKER_OPERATION_FAILED, source).with_context(
            ErrorContext::new()
                .with_text(fields::OPERATION_DIAGNOSTIC, operation)
                .with_i64(fields::BROKER_CODE, i64::from(code))
                .with_text(fields::BROKER_ADDR, broker_addr)
                .with_secret_presence(fields::MESSAGE_PRESENT),
        ),
    )
}

pub(crate) fn broker_response_code(error: &Error) -> Option<i32> {
    if error.descriptor() != &BROKER_OPERATION_FAILED {
        return None;
    }
    error
        .diagnostic_view()
        .ok()?
        .fields()
        .find(|field| field.name() == fields::BROKER_CODE.schema().name())
        .and_then(|field| match field.value() {
            rocketmq_error::ViewValueRef::I64(code) => i32::try_from(code).ok(),
            _ => None,
        })
}

pub(crate) fn broker_task_failed(task: &'static str, source: impl StdError + Send + Sync + 'static) -> SharedError {
    shared(
        Error::caused_by(&BROKER_TASK_FAILED, source).with_context(
            ErrorContext::new()
                .with_text(fields::TASK, task)
                .with_secret_presence(fields::CONTEXT_PRESENT)
                .with_secret_presence(fields::SOURCE_PRESENT),
        ),
    )
}

pub(crate) fn permission_denied(operation: impl AsRef<str>) -> SharedError {
    shared(
        Error::new(&AUTH_PERMISSION_DENIED).with_context(ErrorContext::new().with_text(fields::OPERATION, operation)),
    )
}

pub(crate) fn authentication_failed(_reason: impl Into<String>) -> SharedError {
    shared(
        Error::new(&AUTH_CREDENTIALS_INVALID)
            .with_context(ErrorContext::new().with_secret_presence(fields::CREDENTIALS_PRESENT)),
    )
}

pub(crate) fn auth_configuration_invalid(key: &'static str, _reason: impl Into<String>) -> SharedError {
    shared(
        Error::new(&AUTH_CONFIGURATION_INVALID).with_context(
            ErrorContext::new()
                .with_text(fields::KEY, key)
                .with_secret_presence(fields::REASON_PRESENT),
        ),
    )
}

pub(crate) fn auth_reload_failed(_path: impl Into<String>, _reason: impl Into<String>) -> SharedError {
    shared(
        Error::new(&AUTH_CONFIGURATION_RELOAD_FAILED).with_context(
            ErrorContext::new()
                .with_secret_presence(fields::PATH_PRESENT)
                .with_secret_presence(fields::REASON_PRESENT),
        ),
    )
}

pub(crate) fn topic_not_found(topic: impl AsRef<str>) -> SharedError {
    shared(Error::new(&BROKER_TOPIC_NOT_FOUND).with_context(ErrorContext::new().with_text(fields::TOPIC, topic)))
}

pub(crate) fn storage_read_failed() -> SharedError {
    shared(
        Error::new(&STORAGE_READ_FAILED).with_context(
            ErrorContext::new()
                .with_text(fields::STORE_OPERATION, "read")
                .with_secret_presence(fields::STORE_DETAIL_PRESENT),
        ),
    )
}

pub(crate) fn storage_read_source(source: impl StdError + Send + Sync + 'static) -> SharedError {
    shared(
        Error::caused_by(&STORAGE_READ_FAILED, source).with_context(
            ErrorContext::new()
                .with_text(fields::STORE_OPERATION, "read")
                .with_secret_presence(fields::STORE_DETAIL_PRESENT),
        ),
    )
}

pub(crate) fn storage_write_failed(_path: impl Into<String>, _reason: impl Into<String>) -> SharedError {
    shared(
        Error::new(&STORAGE_WRITE_FAILED).with_context(
            ErrorContext::new()
                .with_text(fields::STORE_OPERATION, "write")
                .with_secret_presence(fields::STORE_DETAIL_PRESENT),
        ),
    )
}

pub(crate) fn storage_write_source(source: impl StdError + Send + Sync + 'static) -> SharedError {
    shared(
        Error::caused_by(&STORAGE_WRITE_FAILED, source).with_context(
            ErrorContext::new()
                .with_text(fields::STORE_OPERATION, "write")
                .with_secret_presence(fields::STORE_DETAIL_PRESENT),
        ),
    )
}

pub(crate) fn storage_exhausted() -> SharedError {
    shared(
        Error::new(&STORAGE_CAPACITY_EXHAUSTED).with_context(
            ErrorContext::new()
                .with_text(fields::STORE_OPERATION, "write")
                .with_secret_presence(fields::STORE_DETAIL_PRESENT),
        ),
    )
}

pub(crate) fn route_inconsistent(topic: impl AsRef<str>) -> SharedError {
    shared(
        Error::new(&ROUTE_TOPIC_INCONSISTENT).with_context(
            ErrorContext::new()
                .with_text(fields::TOPIC, topic)
                .with_secret_presence(fields::REASON_PRESENT),
        ),
    )
}
