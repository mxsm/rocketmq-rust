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

//! RPC client errors with full context preservation

use thiserror::Error;

/// RPC client specific errors with full context preservation
#[derive(Error, Debug)]
pub enum RpcClientError {
    /// Broker address not found in client metadata
    #[error("Broker '{broker_name}' address not found in client metadata")]
    BrokerNotFound { broker_name: String },

    /// RPC request failed
    #[error("RPC request failed: addr={addr}, request_code={request_code}, timeout={timeout_ms}ms")]
    RequestFailed {
        addr: String,
        request_code: i32,
        timeout_ms: u64,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// Unexpected response code received
    #[error("Unexpected response code: {code} ({code_name})")]
    UnexpectedResponseCode { code: i32, code_name: String },

    /// Request code not supported by the handler
    #[error("Request code not supported: {code}")]
    UnsupportedRequestCode { code: i32 },

    /// RPC error from remote server
    #[error("RPC error from remote: code={0}, message={1}")]
    RemoteError(i32, String),
}

impl RpcClientError {
    /// Helper to construct a `BrokerNotFound` error.
    pub fn broker_not_found(broker_name: impl Into<String>) -> Self {
        RpcClientError::BrokerNotFound {
            broker_name: broker_name.into(),
        }
    }
    /// Helper to construct a `RequestFailed` error.
    pub fn request_failed<E>(addr: impl Into<String>, request_code: i32, timeout_ms: u64, source: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        RpcClientError::RequestFailed {
            addr: addr.into(),
            request_code,
            timeout_ms,
            source: Box::new(source),
        }
    }
    /// Helper to construct an `UnexpectedResponseCode` error.
    pub fn unexpected_response_code(code: i32, code_name: impl Into<String>) -> Self {
        RpcClientError::UnexpectedResponseCode {
            code,
            code_name: code_name.into(),
        }
    }
    /// Helper to construct an `UnsupportedRequestCode` error.
    pub fn unsupported_request_code(code: i32) -> Self {
        RpcClientError::UnsupportedRequestCode { code }
    }
    /// Helper to construct a `RemoteError`.
    pub fn remote_error(code: i32, message: impl Into<String>) -> Self {
        RpcClientError::RemoteError(code, message.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[test]
    fn test_rpc_client_error() {
        let err = RpcClientError::broker_not_found("broker-a");
        assert_eq!(
            err.to_string(),
            "Broker 'broker-a' address not found in client metadata"
        );

        let source = io::Error::other("network error");
        let err = RpcClientError::request_failed("127.0.0.1:10911", 10, 3000, source);
        assert_eq!(
            err.to_string(),
            "RPC request failed: addr=127.0.0.1:10911, request_code=10, timeout=3000ms"
        );

        let err = RpcClientError::unexpected_response_code(1, "SYSTEM_ERROR");
        assert_eq!(err.to_string(), "Unexpected response code: 1 (SYSTEM_ERROR)");

        let err = RpcClientError::unsupported_request_code(100);
        assert_eq!(err.to_string(), "Request code not supported: 100");

        let err = RpcClientError::remote_error(2, "topic not exist");
        assert_eq!(
            err.to_string(),
            "RPC error from remote: code=2, message=topic not exist"
        );
    }
}
