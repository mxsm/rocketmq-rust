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
use rocketmq_error::CORE_CONFIGURATION_INVALID;
use rocketmq_error::CORE_SERIALIZATION_FAILED;
use rocketmq_error::PROTOCOL_MESSAGE_PROPERTY_INVALID;
use rocketmq_error::PROTOCOL_VERSION_UNSUPPORTED;

pub(crate) fn invalid_argument(_detail: impl Into<String>) -> Error {
    Error::new(&CORE_ARGUMENT_INVALID).with_context(ErrorContext::new().with_secret_presence(fields::MESSAGE_PRESENT))
}

pub(crate) fn invalid_property(property: impl AsRef<str>) -> Error {
    Error::new(&PROTOCOL_MESSAGE_PROPERTY_INVALID)
        .with_context(ErrorContext::new().with_text(fields::PROPERTY, property))
}

pub(crate) fn invalid_configuration(key: &'static str) -> Error {
    Error::new(&CORE_CONFIGURATION_INVALID).with_context(
        ErrorContext::new()
            .with_text(fields::KEY, key)
            .with_secret_presence(fields::VALUE_PRESENT)
            .with_secret_presence(fields::REASON_PRESENT),
    )
}

pub(crate) fn serialization_failure(operation: &'static str, format: &'static str) -> Error {
    Error::new(&CORE_SERIALIZATION_FAILED).with_context(
        ErrorContext::new()
            .with_text(fields::OPERATION_DIAGNOSTIC, operation)
            .with_text(fields::FORMAT, format)
            .with_secret_presence(fields::DETAIL_PRESENT),
    )
}

pub(crate) fn serialization_decode_failed(format: &'static str, _detail: impl Into<String>) -> Error {
    serialization_failure("decode", format)
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

pub(crate) fn unsupported_version(ordinal: u32) -> Error {
    Error::new(&PROTOCOL_VERSION_UNSUPPORTED)
        .with_context(ErrorContext::new().with_u64(fields::ORDINAL, u64::from(ordinal)))
}
