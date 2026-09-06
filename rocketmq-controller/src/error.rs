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
use rocketmq_error::CONTROLLER_CONFIGURATION_INVALID;
use rocketmq_error::CONTROLLER_CONSENSUS_FAILED;
use rocketmq_error::CONTROLLER_CONSENSUS_TIMED_OUT;
use rocketmq_error::CONTROLLER_INTERNAL_FAILURE;
use rocketmq_error::CONTROLLER_LIFECYCLE_NOT_INITIALIZED;
use rocketmq_error::CONTROLLER_REQUEST_INVALID;

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

#[cfg(test)]
mod tests {
    use std::error::Error as _;
    use std::io;
    use std::sync::Arc;

    use rocketmq_error::RocketMQError;

    use super::*;

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
    fn explicit_shared_carrier_keeps_the_same_canonical_allocation() {
        let canonical = Arc::new(controller_internal("cross legacy owner boundary"));
        let expected = Arc::clone(&canonical);
        let facade = RocketMQError::Shared(canonical);
        let RocketMQError::Shared(actual) = facade else {
            panic!("explicit controller carrier must remain shared");
        };

        assert!(Arc::ptr_eq(&expected, &actual));
        assert_eq!(actual.descriptor(), &CONTROLLER_INTERNAL_FAILURE);
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
