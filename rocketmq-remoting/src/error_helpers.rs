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

//! Error helper functions for rocketmq-remoting
//!
//! This module provides convenient helper functions to create unified errors
//! for common remoting scenarios.

use rocketmq_error::RocketMQError;

/// Create an I/O error from std::io::Error
#[inline]
pub fn io_error(err: std::io::Error) -> RocketMQError {
    RocketMQError::IO(err)
}

/// Create a connection invalid error
#[inline]
pub fn connection_invalid(msg: impl Into<String>) -> RocketMQError {
    RocketMQError::network_connection_failed("unknown", msg)
}

/// Create a remote error
#[inline]
pub fn remote_error(msg: impl Into<String>) -> RocketMQError {
    RocketMQError::network_connection_failed("remote", msg)
}

/// Create a remoting command decoder error
#[inline]
pub fn decoder_error(ext_fields_len: usize, header_len: usize) -> RocketMQError {
    RocketMQError::Protocol(rocketmq_error::unified::ProtocolError::DecodeError {
        ext_fields_len,
        header_len,
    })
}

/// Create a deserialize header error
#[inline]
pub fn deserialize_header_error(msg: impl Into<String>) -> RocketMQError {
    RocketMQError::Serialization(rocketmq_error::unified::SerializationError::DecodeFailed {
        format: "header",
        message: msg.into(),
    })
}

/// Create a decoding error
#[inline]
pub fn decoding_error(required: usize, available: usize) -> RocketMQError {
    RocketMQError::Serialization(rocketmq_error::unified::SerializationError::DecodeFailed {
        format: "binary",
        message: format!("required {} bytes, got {}", required, available),
    })
}

/// Create an unsupported serialize type error
#[inline]
pub fn unsupported_serialize_type(serialize_type: u8) -> RocketMQError {
    RocketMQError::Protocol(rocketmq_error::unified::ProtocolError::UnsupportedSerializationType { serialize_type })
}

/// Create an illegal argument error
#[inline]
pub fn illegal_argument(msg: impl Into<String>) -> RocketMQError {
    RocketMQError::illegal_argument(msg)
}

/// Create a channel send request failed error
#[inline]
pub fn channel_send_failed(msg: impl Into<String>) -> RocketMQError {
    RocketMQError::network_connection_failed("channel", format!("send failed: {}", msg.into()))
}

/// Create a channel receive request failed error
#[inline]
pub fn channel_recv_failed(msg: impl Into<String>) -> RocketMQError {
    RocketMQError::network_connection_failed("channel", format!("receive failed: {}", msg.into()))
}

/// Create an abort process error
#[inline]
pub fn abort_process_error(code: i32, msg: impl Into<String>) -> RocketMQError {
    RocketMQError::Internal(format!("Abort process error {}: {}", code, msg.into()))
}

/// Create a remoting command encoder error
#[inline]
pub fn encoder_error(msg: impl Into<String>) -> RocketMQError {
    RocketMQError::Serialization(rocketmq_error::unified::SerializationError::EncodeFailed {
        format: "command",
        message: msg.into(),
    })
}
