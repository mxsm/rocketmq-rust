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

use rocketmq_error::fields;
use rocketmq_error::Error as CanonicalError;
use rocketmq_error::ErrorContext;
use rocketmq_error::ViewValueRef;

pub(crate) fn cluster_metadata_unavailable(reason: impl Into<String>) -> CanonicalError {
    let _ = reason.into();
    CanonicalError::new(&rocketmq_error::CORE_CONFIGURATION_INVALID)
        .with_context(ErrorContext::new().with_secret_presence(fields::REASON_PRESENT))
}

pub(crate) fn cluster_not_found(cluster: impl Into<String>) -> CanonicalError {
    let _ = cluster.into();
    CanonicalError::new(&rocketmq_error::ROUTE_CLUSTER_NOT_FOUND)
}

pub(crate) fn broker_metadata_unavailable(reason: impl Into<String>) -> CanonicalError {
    let _ = reason.into();
    CanonicalError::new(&rocketmq_error::BROKER_LOOKUP_NOT_FOUND)
}

pub(crate) fn broker_not_found(broker: impl Into<String>) -> CanonicalError {
    let _ = broker.into();
    CanonicalError::new(&rocketmq_error::BROKER_LOOKUP_NOT_FOUND)
}

pub(crate) fn broker_operation_failed(operation: &'static str, reason: impl Into<String>) -> CanonicalError {
    let _ = reason.into();
    CanonicalError::new(&rocketmq_error::BROKER_OPERATION_FAILED)
        .with_context(ErrorContext::new().with_text(fields::OPERATION_DIAGNOSTIC, operation))
}

pub(crate) fn broker_operation_failed_by(
    operation: &'static str,
    source: impl std::error::Error + Send + Sync + 'static,
) -> CanonicalError {
    CanonicalError::caused_by(&rocketmq_error::BROKER_OPERATION_FAILED, source)
        .with_context(ErrorContext::new().with_text(fields::OPERATION_DIAGNOSTIC, operation))
}

pub(crate) fn broker_response_code(error: &CanonicalError) -> Option<i32> {
    error
        .diagnostic_view()
        .ok()?
        .fields()
        .find_map(|field| match (field.name(), field.value()) {
            ("broker_code", ViewValueRef::I64(code)) => i32::try_from(code).ok(),
            _ => None,
        })
}

pub(crate) fn admin_operation_failed(operation: &'static str, reason: impl Into<String>) -> CanonicalError {
    let _ = reason.into();
    CanonicalError::new(&rocketmq_error::PROTOCOL_RESPONSE_FAILED).with_context(
        ErrorContext::new()
            .with_text(fields::OPERATION_DIAGNOSTIC, operation)
            .with_secret_presence(fields::REASON_PRESENT),
    )
}

pub(crate) fn admin_response_failed_by(
    operation: &'static str,
    source: impl std::error::Error + Send + Sync + 'static,
) -> CanonicalError {
    CanonicalError::caused_by(&rocketmq_error::PROTOCOL_RESPONSE_FAILED, source).with_context(
        ErrorContext::new()
            .with_text(fields::OPERATION_DIAGNOSTIC, operation)
            .with_secret_presence(fields::REASON_PRESENT),
    )
}

pub(crate) fn admin_operation_failed_by(
    operation: &'static str,
    source: impl std::error::Error + Send + Sync + 'static,
) -> CanonicalError {
    CanonicalError::caused_by(&rocketmq_error::TOOLS_OPERATION_FAILED, source)
        .with_context(ErrorContext::new().with_text(fields::OPERATION_DIAGNOSTIC, operation))
}

pub(crate) fn admin_validation_failed(field: impl Into<String>, reason: impl Into<String>) -> CanonicalError {
    let _ = field.into();
    let _ = reason.into();
    CanonicalError::new(&rocketmq_error::CORE_ARGUMENT_INVALID)
        .with_context(ErrorContext::new().with_secret_presence(fields::MESSAGE_PRESENT))
}

pub(crate) fn admin_validation_failed_by(
    field: impl Into<String>,
    source: impl std::error::Error + Send + Sync + 'static,
) -> CanonicalError {
    let _ = field.into();
    CanonicalError::caused_by(&rocketmq_error::CORE_ARGUMENT_INVALID, source)
        .with_context(ErrorContext::new().with_secret_presence(fields::MESSAGE_PRESENT))
}

pub(crate) fn admin_serialization_failed_by(
    format: &'static str,
    source: impl std::error::Error + Send + Sync + 'static,
) -> CanonicalError {
    CanonicalError::caused_by(&rocketmq_error::CORE_SERIALIZATION_FAILED, source).with_context(
        ErrorContext::new()
            .with_text(fields::FORMAT, format)
            .with_secret_presence(fields::SOURCE_PRESENT),
    )
}

pub(crate) fn storage_read_failed(component: &'static str) -> CanonicalError {
    CanonicalError::new(&rocketmq_error::STORAGE_READ_FAILED).with_context(
        ErrorContext::new()
            .with_text(fields::STORE_OPERATION, "read")
            .with_text(fields::STORE_COMPONENT, component),
    )
}

pub(crate) fn storage_read_failed_by(
    component: &'static str,
    source: impl std::error::Error + Send + Sync + 'static,
) -> CanonicalError {
    CanonicalError::caused_by(&rocketmq_error::STORAGE_READ_FAILED, source).with_context(
        ErrorContext::new()
            .with_text(fields::STORE_OPERATION, "read")
            .with_text(fields::STORE_COMPONENT, component)
            .with_secret_presence(fields::SOURCE_PRESENT),
    )
}

pub(crate) fn storage_write_failed(component: &'static str) -> CanonicalError {
    CanonicalError::new(&rocketmq_error::STORAGE_WRITE_FAILED).with_context(
        ErrorContext::new()
            .with_text(fields::STORE_OPERATION, "write")
            .with_text(fields::STORE_COMPONENT, component),
    )
}

pub(crate) fn storage_write_failed_by(
    component: &'static str,
    source: impl std::error::Error + Send + Sync + 'static,
) -> CanonicalError {
    CanonicalError::caused_by(&rocketmq_error::STORAGE_WRITE_FAILED, source).with_context(
        ErrorContext::new()
            .with_text(fields::STORE_OPERATION, "write")
            .with_text(fields::STORE_COMPONENT, component)
            .with_secret_presence(fields::SOURCE_PRESENT),
    )
}

pub(crate) fn topic_route_not_found(topic: impl Into<String>) -> CanonicalError {
    let _ = topic.into();
    CanonicalError::new(&rocketmq_error::ROUTE_TOPIC_NOT_FOUND)
}

pub(crate) fn topic_route_inconsistent(topic: impl Into<String>, reason: impl Into<String>) -> CanonicalError {
    let _ = topic.into();
    let _ = reason.into();
    CanonicalError::new(&rocketmq_error::ROUTE_TOPIC_INCONSISTENT)
        .with_context(ErrorContext::new().with_secret_presence(fields::REASON_PRESENT))
}

pub(crate) fn internal(reason: impl Into<String>) -> CanonicalError {
    let _ = reason.into();
    CanonicalError::new(&rocketmq_error::CORE_INTERNAL_FAILURE)
}

pub(crate) fn internal_by(source: impl std::error::Error + Send + Sync + 'static) -> CanonicalError {
    CanonicalError::caused_by(&rocketmq_error::CORE_INTERNAL_FAILURE, source)
}
