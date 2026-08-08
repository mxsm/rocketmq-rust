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

//! Error helper functions for RocketMQ transport.
//!
//! This module provides convenient helper functions to create unified errors
//! for common remoting scenarios.

use rocketmq_error::RocketMQError;
use rocketmq_error::SerializationError;

/// Create a remote error
#[inline]
pub(crate) fn remote_error(msg: impl Into<String>) -> RocketMQError {
    RocketMQError::network_connection_failed("remote", msg)
}

/// Create a decoding error
#[inline]
pub(crate) fn decoding_error(required: usize, available: usize) -> RocketMQError {
    RocketMQError::Serialization(SerializationError::DecodeFailed {
        format: "binary",
        message: format!("required {} bytes, got {}", required, available),
    })
}
