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

use rocketmq_error::Error;
use rocketmq_error::ErrorContext;
use rocketmq_error::fields;

pub(crate) fn argument_invalid(detail: impl Into<String>) -> Error {
    let _ = detail.into();
    Error::new(&rocketmq_error::CORE_ARGUMENT_INVALID)
        .with_context(ErrorContext::new().with_secret_presence(fields::MESSAGE_PRESENT))
}

pub(crate) fn configuration_missing(key: &'static str) -> Error {
    Error::new(&rocketmq_error::CORE_CONFIGURATION_MISSING)
        .with_context(ErrorContext::new().with_text(fields::KEY, key))
}

pub(crate) fn topic_not_found(topic: impl AsRef<str>) -> Error {
    Error::new(&rocketmq_error::ROUTE_TOPIC_NOT_FOUND)
        .with_context(ErrorContext::new().with_text(fields::TOPIC, topic.as_ref()))
}

pub(crate) fn serialization_failed_by(
    format: &'static str,
    source: impl std::error::Error + Send + Sync + 'static,
) -> Error {
    Error::caused_by(&rocketmq_error::CORE_SERIALIZATION_FAILED, source).with_context(
        ErrorContext::new()
            .with_text(fields::FORMAT, format)
            .with_secret_presence(fields::SOURCE_PRESENT),
    )
}

pub(crate) fn service_failed_by(
    operation: &'static str,
    source: impl std::error::Error + Send + Sync + 'static,
) -> Error {
    Error::caused_by(&rocketmq_error::CORE_SERVICE_FAILED, source)
        .with_context(ErrorContext::new().with_text(fields::OPERATION_DIAGNOSTIC, operation))
}

pub(crate) fn io_failed_by(operation: &'static str, source: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::caused_by(&rocketmq_error::CORE_IO_FAILED, source).with_context(
        ErrorContext::new()
            .with_text(fields::OPERATION_DIAGNOSTIC, operation)
            .with_secret_presence(fields::SOURCE_PRESENT),
    )
}

pub(crate) fn storage_read_failed_by(
    component: &'static str,
    source: impl std::error::Error + Send + Sync + 'static,
) -> Error {
    Error::caused_by(&rocketmq_error::STORAGE_READ_FAILED, source).with_context(
        ErrorContext::new()
            .with_text(fields::STORE_OPERATION, "read")
            .with_text(fields::STORE_COMPONENT, component)
            .with_secret_presence(fields::SOURCE_PRESENT),
    )
}

pub(crate) fn storage_write_failed_by(
    component: &'static str,
    source: impl std::error::Error + Send + Sync + 'static,
) -> Error {
    Error::caused_by(&rocketmq_error::STORAGE_WRITE_FAILED, source).with_context(
        ErrorContext::new()
            .with_text(fields::STORE_OPERATION, "write")
            .with_text(fields::STORE_COMPONENT, component)
            .with_secret_presence(fields::SOURCE_PRESENT),
    )
}

pub(crate) fn broker_response_failed(operation: &'static str, code: i32) -> Error {
    Error::new(&rocketmq_error::BROKER_OPERATION_FAILED).with_context(
        ErrorContext::new()
            .with_text(fields::OPERATION_DIAGNOSTIC, operation)
            .with_i64(fields::BROKER_CODE, i64::from(code)),
    )
}

pub(crate) fn broker_permission_denied(operation: impl AsRef<str>) -> Error {
    Error::new(&rocketmq_error::AUTH_PERMISSION_DENIED)
        .with_context(ErrorContext::new().with_text(fields::OPERATION, operation.as_ref()))
}

pub(crate) fn internal_failed_by(
    operation: &'static str,
    source: impl std::error::Error + Send + Sync + 'static,
) -> Error {
    Error::caused_by(&rocketmq_error::CORE_INTERNAL_FAILURE, source)
        .with_context(ErrorContext::new().with_text(fields::OPERATION_DIAGNOSTIC, operation))
}

pub(crate) fn request_body_invalid_by(
    operation: &'static str,
    source: impl std::error::Error + Send + Sync + 'static,
) -> Error {
    Error::caused_by(&rocketmq_error::PROTOCOL_BODY_INVALID, source).with_context(
        ErrorContext::new()
            .with_text(fields::OPERATION_DIAGNOSTIC, operation)
            .with_secret_presence(fields::SOURCE_PRESENT),
    )
}
