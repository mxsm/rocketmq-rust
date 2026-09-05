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

//! Crate-private canonical error construction owned by Transport.

use std::error::Error as StdError;
use std::sync::Arc;

use rocketmq_error::Error;
use rocketmq_error::ErrorContext;
use rocketmq_error::ErrorDescriptor;
use rocketmq_error::RocketMQError;
use rocketmq_error::SerializationError;
use rocketmq_error::SharedError;

/// Closed diagnostic stage captured by Transport producers.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum TransportStage {
    EndpointValidation,
    Connect,
    BeforeWrite,
    Writing,
    AwaitingResponse,
    Read,
    Write,
    Closed,
}

impl TransportStage {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::EndpointValidation => "endpoint_validation",
            Self::Connect => "connect",
            Self::BeforeWrite => "before_write",
            Self::Writing => "writing",
            Self::AwaitingResponse => "awaiting_response",
            Self::Read => "read",
            Self::Write => "write",
            Self::Closed => "closed",
        }
    }
}

#[track_caller]
fn source_free(descriptor: &'static ErrorDescriptor, context: ErrorContext) -> SharedError {
    Arc::new(Error::new(descriptor).with_context(context))
}

#[track_caller]
fn caused_by(
    descriptor: &'static ErrorDescriptor,
    context: ErrorContext,
    source: impl StdError + Send + Sync + 'static,
) -> SharedError {
    Arc::new(Error::caused_by(descriptor, source).with_context(context))
}

#[inline]
pub(crate) fn network(error: SharedError) -> RocketMQError {
    RocketMQError::Network(error)
}

#[track_caller]
pub(crate) fn endpoint_invalid(remote_addr_present: bool) -> SharedError {
    let context = if remote_addr_present {
        ErrorContext::new().with_secret_presence(rocketmq_error::fields::REMOTE_ADDR_PRESENT)
    } else {
        ErrorContext::new()
    };
    source_free(&rocketmq_error::TRANSPORT_ENDPOINT_INVALID, context)
}

#[track_caller]
pub(crate) fn dns_failed(source: impl StdError + Send + Sync + 'static) -> SharedError {
    caused_by(
        &rocketmq_error::TRANSPORT_DNS_FAILED,
        ErrorContext::new()
            .with_secret_presence(rocketmq_error::fields::HOST_PRESENT)
            .with_secret_presence(rocketmq_error::fields::SOURCE_PRESENT),
        source,
    )
}

#[track_caller]
pub(crate) fn dns_failed_without_source() -> SharedError {
    source_free(
        &rocketmq_error::TRANSPORT_DNS_FAILED,
        ErrorContext::new().with_secret_presence(rocketmq_error::fields::HOST_PRESENT),
    )
}

#[track_caller]
pub(crate) fn connection_failed(stage: TransportStage, source: impl StdError + Send + Sync + 'static) -> SharedError {
    caused_by(
        &rocketmq_error::TRANSPORT_CONNECTION_FAILED,
        ErrorContext::new()
            .with_text(rocketmq_error::fields::PHASE, stage.as_str())
            .with_secret_presence(rocketmq_error::fields::SOURCE_PRESENT),
        source,
    )
}

#[track_caller]
pub(crate) fn connection_failed_for_remote(
    _remote_addr: impl AsRef<str>,
    stage: TransportStage,
    source: impl StdError + Send + Sync + 'static,
) -> SharedError {
    caused_by(
        &rocketmq_error::TRANSPORT_CONNECTION_FAILED,
        ErrorContext::new()
            .with_text(rocketmq_error::fields::PHASE, stage.as_str())
            .with_secret_presence(rocketmq_error::fields::REMOTE_ADDR_PRESENT)
            .with_secret_presence(rocketmq_error::fields::SOURCE_PRESENT),
        source,
    )
}

#[track_caller]
pub(crate) fn connection_failed_without_source(stage: TransportStage) -> SharedError {
    source_free(
        &rocketmq_error::TRANSPORT_CONNECTION_FAILED,
        ErrorContext::new().with_text(rocketmq_error::fields::PHASE, stage.as_str()),
    )
}

#[track_caller]
pub(crate) fn connection_failed_without_source_for_remote(
    _remote_addr: impl AsRef<str>,
    stage: TransportStage,
) -> SharedError {
    source_free(
        &rocketmq_error::TRANSPORT_CONNECTION_FAILED,
        ErrorContext::new()
            .with_text(rocketmq_error::fields::PHASE, stage.as_str())
            .with_secret_presence(rocketmq_error::fields::REMOTE_ADDR_PRESENT),
    )
}

#[track_caller]
pub(crate) fn connection_timeout_caused_by(
    remote_addr: impl Into<String>,
    timeout_millis: u64,
    source: impl StdError + Send + Sync + 'static,
) -> SharedError {
    caused_by(
        &rocketmq_error::TRANSPORT_CONNECTION_TIMEOUT,
        ErrorContext::new()
            .with_u64(rocketmq_error::fields::TIMEOUT_MS, timeout_millis)
            .with_text(rocketmq_error::fields::REMOTE_ADDR, remote_addr.into())
            .with_secret_presence(rocketmq_error::fields::SOURCE_PRESENT),
        source,
    )
}

#[track_caller]
pub(crate) fn admission_queue_saturated(remote_addr: impl Into<String>) -> SharedError {
    source_free(
        &rocketmq_error::TRANSPORT_ADMISSION_QUEUE_SATURATED,
        ErrorContext::new().with_text(rocketmq_error::fields::REMOTE_ADDR, remote_addr.into()),
    )
}

#[track_caller]
pub(crate) fn write_timeout(stage: TransportStage, timeout_millis: u64) -> SharedError {
    debug_assert!(matches!(stage, TransportStage::BeforeWrite | TransportStage::Writing));
    source_free(
        &rocketmq_error::TRANSPORT_WRITE_TIMEOUT,
        ErrorContext::new()
            .with_text(rocketmq_error::fields::PHASE, stage.as_str())
            .with_u64(rocketmq_error::fields::TIMEOUT_MS, timeout_millis),
    )
}

#[track_caller]
pub(crate) fn write_timeout_caused_by(
    stage: TransportStage,
    timeout_millis: u64,
    source: impl StdError + Send + Sync + 'static,
) -> SharedError {
    debug_assert!(matches!(stage, TransportStage::BeforeWrite | TransportStage::Writing));
    caused_by(
        &rocketmq_error::TRANSPORT_WRITE_TIMEOUT,
        ErrorContext::new()
            .with_text(rocketmq_error::fields::PHASE, stage.as_str())
            .with_u64(rocketmq_error::fields::TIMEOUT_MS, timeout_millis)
            .with_secret_presence(rocketmq_error::fields::SOURCE_PRESENT),
        source,
    )
}

#[track_caller]
pub(crate) fn response_timeout(timeout_millis: u64) -> SharedError {
    source_free(
        &rocketmq_error::TRANSPORT_RESPONSE_TIMEOUT,
        ErrorContext::new()
            .with_text(rocketmq_error::fields::PHASE, TransportStage::AwaitingResponse.as_str())
            .with_u64(rocketmq_error::fields::TIMEOUT_MS, timeout_millis),
    )
}

#[track_caller]
pub(crate) fn response_timeout_caused_by(
    timeout_millis: u64,
    source: impl StdError + Send + Sync + 'static,
) -> SharedError {
    caused_by(
        &rocketmq_error::TRANSPORT_RESPONSE_TIMEOUT,
        ErrorContext::new()
            .with_text(rocketmq_error::fields::PHASE, TransportStage::AwaitingResponse.as_str())
            .with_u64(rocketmq_error::fields::TIMEOUT_MS, timeout_millis)
            .with_secret_presence(rocketmq_error::fields::SOURCE_PRESENT),
        source,
    )
}

#[track_caller]
pub(crate) fn response_timeout_for_remote(_remote_addr: impl AsRef<str>, timeout_millis: u64) -> SharedError {
    source_free(
        &rocketmq_error::TRANSPORT_RESPONSE_TIMEOUT,
        ErrorContext::new()
            .with_text(rocketmq_error::fields::PHASE, TransportStage::AwaitingResponse.as_str())
            .with_u64(rocketmq_error::fields::TIMEOUT_MS, timeout_millis)
            .with_secret_presence(rocketmq_error::fields::REMOTE_ADDR_PRESENT),
    )
}

#[track_caller]
pub(crate) fn response_timeout_caused_by_for_remote(
    _remote_addr: impl AsRef<str>,
    timeout_millis: u64,
    source: impl StdError + Send + Sync + 'static,
) -> SharedError {
    caused_by(
        &rocketmq_error::TRANSPORT_RESPONSE_TIMEOUT,
        ErrorContext::new()
            .with_text(rocketmq_error::fields::PHASE, TransportStage::AwaitingResponse.as_str())
            .with_u64(rocketmq_error::fields::TIMEOUT_MS, timeout_millis)
            .with_secret_presence(rocketmq_error::fields::REMOTE_ADDR_PRESENT)
            .with_secret_presence(rocketmq_error::fields::SOURCE_PRESENT),
        source,
    )
}

/// Create a decoding error
#[inline]
pub(crate) fn decoding_error(required: usize, available: usize) -> RocketMQError {
    RocketMQError::Serialization(SerializationError::DecodeFailed {
        format: "binary",
        message: format!("required {} bytes, got {}", required, available),
    })
}
