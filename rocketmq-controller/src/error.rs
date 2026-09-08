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

use rocketmq_error::fields;
use rocketmq_error::Error;
use rocketmq_error::ErrorContext;
use rocketmq_error::AUTH_CREDENTIALS_INVALID;
use rocketmq_error::AUTH_OPERATION_FAILED;
use rocketmq_error::AUTH_PERMISSION_DENIED;
use rocketmq_error::BROKER_MESSAGE_TOO_LARGE;
use rocketmq_error::CONTROLLER_CONFIGURATION_INVALID;
use rocketmq_error::CONTROLLER_CONSENSUS_FAILED;
use rocketmq_error::CONTROLLER_CONSENSUS_TIMED_OUT;
use rocketmq_error::CONTROLLER_INTERNAL_FAILURE;
use rocketmq_error::CONTROLLER_LIFECYCLE_NOT_INITIALIZED;
use rocketmq_error::CONTROLLER_REQUEST_INVALID;
use rocketmq_error::CORE_IO_FAILED;
use rocketmq_error::CORE_SERIALIZATION_FAILED;
use rocketmq_error::PROTOCOL_BODY_INVALID;
use rocketmq_error::PROTOCOL_HEADER_INVALID;

pub(crate) fn consensus_failed(operation: &'static str, source: impl StdError + Send + Sync + 'static) -> Error {
    Error::caused_by(&CONTROLLER_CONSENSUS_FAILED, source).with_context(
        ErrorContext::new()
            .with_text(fields::OPERATION_DIAGNOSTIC, operation)
            .with_secret_presence(fields::SOURCE_PRESENT),
    )
}

pub(crate) fn consensus_timed_out(operation: &'static str, timeout_ms: u64) -> Error {
    Error::new(&CONTROLLER_CONSENSUS_TIMED_OUT).with_context(
        ErrorContext::new()
            .with_text(fields::OPERATION_DIAGNOSTIC, operation)
            .with_u64(fields::TIMEOUT_MS, timeout_ms),
    )
}

pub(crate) fn request_invalid(operation: &'static str) -> Error {
    Error::new(&CONTROLLER_REQUEST_INVALID).with_context(
        ErrorContext::new()
            .with_text(fields::OPERATION_DIAGNOSTIC, operation)
            .with_secret_presence(fields::REASON_PRESENT),
    )
}

pub(crate) fn request_invalid_by(operation: &'static str, source: impl StdError + Send + Sync + 'static) -> Error {
    Error::caused_by(&CONTROLLER_REQUEST_INVALID, source).with_context(
        ErrorContext::new()
            .with_text(fields::OPERATION_DIAGNOSTIC, operation)
            .with_secret_presence(fields::REASON_PRESENT)
            .with_secret_presence(fields::SOURCE_PRESENT),
    )
}

pub(crate) fn configuration_invalid(key: &'static str) -> Error {
    Error::new(&CONTROLLER_CONFIGURATION_INVALID).with_context(
        ErrorContext::new()
            .with_text(fields::KEY, key)
            .with_secret_presence(fields::REASON_PRESENT),
    )
}

pub(crate) fn configuration_invalid_by(key: &'static str, source: impl StdError + Send + Sync + 'static) -> Error {
    Error::caused_by(&CONTROLLER_CONFIGURATION_INVALID, source).with_context(
        ErrorContext::new()
            .with_text(fields::KEY, key)
            .with_secret_presence(fields::REASON_PRESENT)
            .with_secret_presence(fields::SOURCE_PRESENT),
    )
}

pub(crate) fn not_initialized(component: &'static str) -> Error {
    Error::new(&CONTROLLER_LIFECYCLE_NOT_INITIALIZED).with_context(
        ErrorContext::new()
            .with_text(fields::COMPONENT_NAME, component)
            .with_secret_presence(fields::REASON_PRESENT),
    )
}

pub(crate) fn controller_internal(operation: &'static str) -> Error {
    Error::new(&CONTROLLER_INTERNAL_FAILURE)
        .with_context(ErrorContext::new().with_text(fields::OPERATION_DIAGNOSTIC, operation))
}

pub(crate) fn controller_internal_by(operation: &'static str, source: impl StdError + Send + Sync + 'static) -> Error {
    Error::caused_by(&CONTROLLER_INTERNAL_FAILURE, source).with_context(
        ErrorContext::new()
            .with_text(fields::OPERATION_DIAGNOSTIC, operation)
            .with_secret_presence(fields::SOURCE_PRESENT),
    )
}

pub(crate) fn request_header_invalid(operation: &'static str) -> Error {
    Error::new(&PROTOCOL_HEADER_INVALID).with_context(
        ErrorContext::new()
            .with_text(fields::OPERATION_DIAGNOSTIC, operation)
            .with_secret_presence(fields::INVALID_VALUE_PRESENT),
    )
}

pub(crate) fn request_header_invalid_by(
    operation: &'static str,
    source: impl StdError + Send + Sync + 'static,
) -> Error {
    Error::caused_by(&PROTOCOL_HEADER_INVALID, source).with_context(
        ErrorContext::new()
            .with_text(fields::OPERATION_DIAGNOSTIC, operation)
            .with_secret_presence(fields::SOURCE_PRESENT),
    )
}

pub(crate) fn request_body_invalid(operation: &'static str) -> Error {
    Error::new(&PROTOCOL_BODY_INVALID).with_context(
        ErrorContext::new()
            .with_text(fields::OPERATION_DIAGNOSTIC, operation)
            .with_secret_presence(fields::INVALID_VALUE_PRESENT),
    )
}

pub(crate) fn request_body_invalid_by(operation: &'static str, source: impl StdError + Send + Sync + 'static) -> Error {
    Error::caused_by(&PROTOCOL_BODY_INVALID, source).with_context(
        ErrorContext::new()
            .with_text(fields::OPERATION_DIAGNOSTIC, operation)
            .with_secret_presence(fields::INVALID_VALUE_PRESENT)
            .with_secret_presence(fields::SOURCE_PRESENT),
    )
}

pub(crate) fn authentication_failed() -> Error {
    Error::new(&AUTH_CREDENTIALS_INVALID)
        .with_context(ErrorContext::new().with_secret_presence(fields::CREDENTIALS_PRESENT))
}

pub(crate) fn permission_denied(operation: &'static str) -> Error {
    Error::new(&AUTH_PERMISSION_DENIED).with_context(ErrorContext::new().with_text(fields::OPERATION, operation))
}

pub(crate) fn auth_operation_failed(operation: &'static str) -> Error {
    Error::new(&AUTH_OPERATION_FAILED)
        .with_context(ErrorContext::new().with_text(fields::OPERATION_DIAGNOSTIC, operation))
}

pub(crate) fn message_too_large(actual: usize, limit: usize) -> Error {
    Error::new(&BROKER_MESSAGE_TOO_LARGE).with_context(
        ErrorContext::new()
            .with_u64(fields::ACTUAL_BYTES, actual as u64)
            .with_u64(fields::LIMIT_BYTES, limit as u64),
    )
}

pub(crate) fn serialization_failed(
    operation: &'static str,
    format: &'static str,
    source: impl StdError + Send + Sync + 'static,
) -> Error {
    Error::caused_by(&CORE_SERIALIZATION_FAILED, source).with_context(
        ErrorContext::new()
            .with_text(fields::OPERATION_DIAGNOSTIC, operation)
            .with_text(fields::FORMAT, format)
            .with_secret_presence(fields::SOURCE_PRESENT),
    )
}

pub(crate) fn serialization_invalid(operation: &'static str, format: &'static str) -> Error {
    Error::new(&CORE_SERIALIZATION_FAILED).with_context(
        ErrorContext::new()
            .with_text(fields::OPERATION_DIAGNOSTIC, operation)
            .with_text(fields::FORMAT, format)
            .with_secret_presence(fields::INVALID_VALUE_PRESENT),
    )
}

pub(crate) fn io_failed(operation: &'static str, source: impl StdError + Send + Sync + 'static) -> Error {
    Error::caused_by(&CORE_IO_FAILED, source).with_context(
        ErrorContext::new()
            .with_text(fields::OPERATION_DIAGNOSTIC, operation)
            .with_secret_presence(fields::SOURCE_PRESENT),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error as _;
    use std::io;

    #[test]
    fn canonical_controller_error_preserves_descriptor_and_typed_leaf() {
        let error = consensus_failed("append raft entry", io::Error::other("private raft detail"));

        assert_eq!(error.descriptor(), &CONTROLLER_CONSENSUS_FAILED);
        assert!(error
            .source()
            .and_then(|source| source.downcast_ref::<io::Error>())
            .is_some());
        assert_eq!(
            error.public_view().expect("schema-valid view").message(),
            "Controller consensus operation failed"
        );
    }

    #[test]
    fn canonical_error_is_shared_only_at_multi_owner_boundaries() {
        let canonical = std::sync::Arc::new(controller_internal("cross owner boundary"));
        let shared = std::sync::Arc::clone(&canonical);

        assert!(std::sync::Arc::ptr_eq(&canonical, &shared));
        assert_eq!(shared.descriptor(), &CONTROLLER_INTERNAL_FAILURE);
    }

    #[test]
    fn configuration_source_is_retained_but_public_view_is_fixed() {
        let error = configuration_invalid_by("controller.raft_address", io::Error::other("private address"));

        assert_eq!(error.descriptor(), &CONTROLLER_CONFIGURATION_INVALID);
        assert!(error.source().is_some());
        assert_eq!(
            error.public_view().expect("schema-valid view").message(),
            "Controller configuration is invalid"
        );
    }
}
