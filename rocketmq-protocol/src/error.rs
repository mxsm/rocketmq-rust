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
use rocketmq_error::CORE_ARGUMENT_INVALID;
use rocketmq_error::CORE_CONFIGURATION_MISSING;
use rocketmq_error::CORE_IO_FAILED;
use rocketmq_error::CORE_SERIALIZATION_FAILED;
use rocketmq_error::PROTOCOL_BODY_INVALID;
use rocketmq_error::PROTOCOL_ENCODING_UNSUPPORTED;
use rocketmq_error::PROTOCOL_HEADER_INVALID;
use rocketmq_error::PROTOCOL_RESPONSE_FAILED;
use rocketmq_error::ROUTE_TOPIC_INCONSISTENT;

pub(crate) fn invalid_argument(_detail: impl Into<String>) -> Error {
    Error::new(&CORE_ARGUMENT_INVALID).with_context(ErrorContext::new().with_secret_presence(fields::MESSAGE_PRESENT))
}

pub(crate) fn serialization_failure(
    operation: &'static str,
    format: &'static str,
    _detail: impl Into<String>,
) -> Error {
    Error::new(&CORE_SERIALIZATION_FAILED).with_context(
        ErrorContext::new()
            .with_text(fields::OPERATION_DIAGNOSTIC, operation)
            .with_text(fields::FORMAT, format)
            .with_secret_presence(fields::DETAIL_PRESENT),
    )
}

pub(crate) fn serialization_encode_failed(format: &'static str, detail: impl Into<String>) -> Error {
    serialization_failure("encode", format, detail)
}

pub(crate) fn serialization_decode_failed(format: &'static str, detail: impl Into<String>) -> Error {
    serialization_failure("decode", format, detail)
}

pub(crate) fn serialization_source(
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

pub(crate) fn request_header_failure() -> Error {
    Error::new(&PROTOCOL_HEADER_INVALID)
        .with_context(ErrorContext::new().with_secret_presence(fields::INVALID_VALUE_PRESENT))
}

pub(crate) fn request_header_source(operation: &'static str, source: impl StdError + Send + Sync + 'static) -> Error {
    Error::caused_by(&PROTOCOL_HEADER_INVALID, source).with_context(
        ErrorContext::new()
            .with_text(fields::OPERATION_DIAGNOSTIC, operation)
            .with_secret_presence(fields::SOURCE_PRESENT),
    )
}

pub(crate) fn invalid_body(operation: &'static str, _detail: impl Into<String>) -> Error {
    Error::new(&PROTOCOL_BODY_INVALID).with_context(
        ErrorContext::new()
            .with_text(fields::OPERATION_DIAGNOSTIC, operation)
            .with_secret_presence(fields::INVALID_VALUE_PRESENT),
    )
}

pub(crate) fn invalid_body_source(operation: &'static str, source: impl StdError + Send + Sync + 'static) -> Error {
    Error::caused_by(&PROTOCOL_BODY_INVALID, source).with_context(
        ErrorContext::new()
            .with_text(fields::OPERATION_DIAGNOSTIC, operation)
            .with_secret_presence(fields::INVALID_VALUE_PRESENT)
            .with_secret_presence(fields::SOURCE_PRESENT),
    )
}

pub(crate) fn unsupported_encoding(serialize_type: u8) -> Error {
    Error::new(&PROTOCOL_ENCODING_UNSUPPORTED)
        .with_context(ErrorContext::new().with_u64(fields::SERIALIZATION_TYPE, u64::from(serialize_type)))
}

pub(crate) fn response_failure(operation: &'static str, _detail: impl Into<String>) -> Error {
    Error::new(&PROTOCOL_RESPONSE_FAILED).with_context(
        ErrorContext::new()
            .with_text(fields::OPERATION_DIAGNOSTIC, operation)
            .with_secret_presence(fields::REASON_PRESENT),
    )
}

pub(crate) fn route_inconsistent(topic: impl AsRef<str>, _detail: impl Into<String>) -> Error {
    Error::new(&ROUTE_TOPIC_INCONSISTENT).with_context(
        ErrorContext::new()
            .with_text(fields::TOPIC, topic)
            .with_secret_presence(fields::REASON_PRESENT),
    )
}

pub(crate) fn io_source(operation: &'static str, source: impl StdError + Send + Sync + 'static) -> Error {
    Error::caused_by(&CORE_IO_FAILED, source).with_context(
        ErrorContext::new()
            .with_text(fields::OPERATION_DIAGNOSTIC, operation)
            .with_secret_presence(fields::SOURCE_PRESENT),
    )
}

pub(crate) fn missing_configuration(key: &'static str) -> Error {
    Error::new(&CORE_CONFIGURATION_MISSING).with_context(ErrorContext::new().with_text(fields::KEY, key))
}
