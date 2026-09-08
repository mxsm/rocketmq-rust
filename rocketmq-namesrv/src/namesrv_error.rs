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

//! NameServer-local constructors for the canonical error envelope.

use std::error::Error as StdError;
use std::sync::Arc;

use rocketmq_error::fields;
use rocketmq_error::Error;
use rocketmq_error::ErrorContext;
use rocketmq_error::SharedError;

/// Result returned by NameServer operations.
pub type NameServerResult<T> = std::result::Result<T, SharedError>;

pub(crate) fn shared(error: Error) -> SharedError {
    Arc::new(error)
}

pub(crate) fn from_error(error: Error) -> SharedError {
    shared(error)
}

pub(crate) fn invalid_configuration(key: impl AsRef<str>) -> SharedError {
    shared(
        Error::new(&rocketmq_error::CORE_CONFIGURATION_INVALID).with_context(
            ErrorContext::new()
                .with_text(fields::KEY, key)
                .with_secret_presence(fields::VALUE_PRESENT)
                .with_secret_presence(fields::REASON_PRESENT),
        ),
    )
}

pub(crate) fn invalid_configuration_source(
    key: impl AsRef<str>,
    source: impl StdError + Send + Sync + 'static,
) -> SharedError {
    shared(
        Error::caused_by(&rocketmq_error::CORE_CONFIGURATION_INVALID, source).with_context(
            ErrorContext::new()
                .with_text(fields::KEY, key)
                .with_secret_presence(fields::VALUE_PRESENT)
                .with_secret_presence(fields::REASON_PRESENT),
        ),
    )
}

pub(crate) fn io(operation: impl AsRef<str>, source: impl StdError + Send + Sync + 'static) -> SharedError {
    shared(
        Error::caused_by(&rocketmq_error::CORE_IO_FAILED, source).with_context(
            ErrorContext::new()
                .with_text(fields::OPERATION_DIAGNOSTIC, operation)
                .with_secret_presence(fields::SOURCE_PRESENT),
        ),
    )
}

pub(crate) fn storage_read(source: impl StdError + Send + Sync + 'static) -> SharedError {
    storage_failure(&rocketmq_error::STORAGE_READ_FAILED, "read", source)
}

pub(crate) fn storage_write(source: impl StdError + Send + Sync + 'static) -> SharedError {
    storage_failure(&rocketmq_error::STORAGE_WRITE_FAILED, "write", source)
}

fn storage_failure(
    descriptor: &'static rocketmq_error::ErrorDescriptor,
    operation: &'static str,
    source: impl StdError + Send + Sync + 'static,
) -> SharedError {
    shared(
        Error::caused_by(descriptor, source).with_context(
            ErrorContext::new()
                .with_text(fields::STORE_OPERATION, operation)
                .with_text(fields::STORE_COMPONENT, "namesrv-kv")
                .with_secret_presence(fields::STORE_DETAIL_PRESENT)
                .with_secret_presence(fields::SOURCE_PRESENT),
        ),
    )
}

pub(crate) fn serialization(
    operation: impl AsRef<str>,
    format: impl AsRef<str>,
    source: impl StdError + Send + Sync + 'static,
) -> SharedError {
    shared(
        Error::caused_by(&rocketmq_error::CORE_SERIALIZATION_FAILED, source).with_context(
            ErrorContext::new()
                .with_text(fields::OPERATION_DIAGNOSTIC, operation)
                .with_text(fields::FORMAT, format)
                .with_secret_presence(fields::SOURCE_PRESENT),
        ),
    )
}

pub(crate) fn not_initialized(component: impl AsRef<str>) -> SharedError {
    shared(
        Error::new(&rocketmq_error::CORE_LIFECYCLE_NOT_INITIALIZED).with_context(
            ErrorContext::new()
                .with_text(fields::COMPONENT_NAME, component)
                .with_secret_presence(fields::REASON_PRESENT),
        ),
    )
}

pub(crate) fn invariant(invariant: impl AsRef<str>) -> SharedError {
    shared(
        Error::new(&rocketmq_error::CORE_INTERNAL_FAILURE)
            .with_context(ErrorContext::new().with_text(fields::OPERATION_DIAGNOSTIC, invariant)),
    )
}

pub(crate) fn startup(operation: impl AsRef<str>, source: impl StdError + Send + Sync + 'static) -> SharedError {
    shared(
        Error::caused_by(&rocketmq_error::CORE_SERVICE_FAILED, source)
            .with_context(ErrorContext::new().with_text(fields::OPERATION_DIAGNOSTIC, operation)),
    )
}

pub(crate) fn startup_state(operation: impl AsRef<str>) -> SharedError {
    shared(
        Error::new(&rocketmq_error::CORE_SERVICE_FAILED)
            .with_context(ErrorContext::new().with_text(fields::OPERATION_DIAGNOSTIC, operation)),
    )
}

pub(crate) fn authentication_failed() -> SharedError {
    shared(
        Error::new(&rocketmq_error::AUTH_CREDENTIALS_INVALID)
            .with_context(ErrorContext::new().with_secret_presence(fields::CREDENTIALS_PRESENT)),
    )
}

pub(crate) fn permission_denied(operation: impl AsRef<str>) -> SharedError {
    shared(
        Error::new(&rocketmq_error::AUTH_PERMISSION_DENIED)
            .with_context(ErrorContext::new().with_text(fields::OPERATION, operation)),
    )
}

pub(crate) fn request_body_invalid(operation: impl AsRef<str>) -> SharedError {
    shared(
        Error::new(&rocketmq_error::PROTOCOL_BODY_INVALID).with_context(
            ErrorContext::new()
                .with_text(fields::OPERATION_DIAGNOSTIC, operation)
                .with_secret_presence(fields::INVALID_VALUE_PRESENT),
        ),
    )
}

pub(crate) fn request_body_source(
    operation: impl AsRef<str>,
    source: impl StdError + Send + Sync + 'static,
) -> SharedError {
    shared(
        Error::caused_by(&rocketmq_error::PROTOCOL_BODY_INVALID, source).with_context(
            ErrorContext::new()
                .with_text(fields::OPERATION_DIAGNOSTIC, operation)
                .with_secret_presence(fields::SOURCE_PRESENT),
        ),
    )
}

pub(crate) fn response_failed(operation: impl AsRef<str>) -> SharedError {
    shared(
        Error::new(&rocketmq_error::PROTOCOL_RESPONSE_FAILED).with_context(
            ErrorContext::new()
                .with_text(fields::OPERATION_DIAGNOSTIC, operation)
                .with_secret_presence(fields::REASON_PRESENT),
        ),
    )
}

pub(crate) fn response_source(
    operation: impl AsRef<str>,
    source: impl StdError + Send + Sync + 'static,
) -> SharedError {
    shared(
        Error::caused_by(&rocketmq_error::PROTOCOL_RESPONSE_FAILED, source).with_context(
            ErrorContext::new()
                .with_text(fields::OPERATION_DIAGNOSTIC, operation)
                .with_secret_presence(fields::REASON_PRESENT),
        ),
    )
}

pub(crate) fn rpc_response_failed(code: i32) -> SharedError {
    shared(
        Error::new(&rocketmq_error::RPC_RESPONSE_FAILED).with_context(
            ErrorContext::new()
                .with_i64(fields::REMOTE_CODE, i64::from(code))
                .with_secret_presence(fields::MESSAGE_PRESENT),
        ),
    )
}

pub(crate) fn cluster_not_found(cluster: impl AsRef<str>) -> SharedError {
    shared(
        Error::new(&rocketmq_error::ROUTE_CLUSTER_NOT_FOUND)
            .with_context(ErrorContext::new().with_text(fields::CLUSTER, cluster)),
    )
}
