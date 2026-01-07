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

//! Network-related errors for RocketMQ operations

use thiserror::Error;

/// Network operation errors
#[derive(Debug, Error)]
pub enum NetworkError {
    /// Connection to remote address failed
    #[error("Connection failed to {addr}: {reason}")]
    ConnectionFailed { addr: String, reason: String },

    /// Connection timeout
    #[error("Connection timeout to {addr} after {timeout_ms}ms")]
    ConnectionTimeout { addr: String, timeout_ms: u64 },

    /// Connection was closed unexpectedly
    #[error("Connection closed: {addr}")]
    ConnectionClosed { addr: String },

    /// Failed to send data
    #[error("Send failed to {addr}: {reason}")]
    SendFailed { addr: String, reason: String },

    /// Failed to receive data
    #[error("Receive failed from {addr}: {reason}")]
    ReceiveFailed { addr: String, reason: String },

    /// Invalid address format
    #[error("Invalid address format: {addr}")]
    InvalidAddress { addr: String },

    /// DNS resolution failed
    #[error("DNS resolution failed for {host}: {reason}")]
    DnsResolutionFailed { host: String, reason: String },

    /// Too many requests (backpressure)
    #[error("Too many requests to {addr}, limit: {limit}")]
    TooManyRequests { addr: String, limit: usize },

    /// Request timeout
    #[error("Request timeout to {addr} after {timeout_ms}ms")]
    RequestTimeout { addr: String, timeout_ms: u64 },
}

impl NetworkError {
    /// Create a connection failed error
    #[inline]
    pub fn connection_failed(addr: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::ConnectionFailed {
            addr: addr.into(),
            reason: reason.into(),
        }
    }

    /// Create a connection timeout error
    #[inline]
    pub fn connection_timeout(addr: impl Into<String>, timeout_ms: u64) -> Self {
        Self::ConnectionTimeout {
            addr: addr.into(),
            timeout_ms,
        }
    }

    /// Create a send failed error
    #[inline]
    pub fn send_failed(addr: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::SendFailed {
            addr: addr.into(),
            reason: reason.into(),
        }
    }

    /// Create a request timeout error
    #[inline]
    pub fn request_timeout(addr: impl Into<String>, timeout_ms: u64) -> Self {
        Self::RequestTimeout {
            addr: addr.into(),
            timeout_ms,
        }
    }

    /// Get the associated address if available
    pub fn addr(&self) -> &str {
        match self {
            Self::ConnectionFailed { addr, .. }
            | Self::ConnectionTimeout { addr, .. }
            | Self::ConnectionClosed { addr }
            | Self::SendFailed { addr, .. }
            | Self::ReceiveFailed { addr, .. }
            | Self::InvalidAddress { addr }
            | Self::TooManyRequests { addr, .. }
            | Self::RequestTimeout { addr, .. } => addr,
            Self::DnsResolutionFailed { host, .. } => host,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_network_error_creation() {
        let err = NetworkError::connection_failed("127.0.0.1:9876", "timeout");
        assert_eq!(err.addr(), "127.0.0.1:9876");
        assert!(err.to_string().contains("Connection failed"));
    }

    #[test]
    fn test_network_error_display() {
        let err = NetworkError::ConnectionTimeout {
            addr: "localhost:10911".to_string(),
            timeout_ms: 3000,
        };
        assert_eq!(err.to_string(), "Connection timeout to localhost:10911 after 3000ms");
    }

    #[test]
    fn test_network_error_connection_closed() {
        let err = NetworkError::ConnectionClosed {
            addr: "localhost:10911".to_string(),
        };
        assert_eq!(err.to_string(), "Connection closed: localhost:10911");
    }

    #[test]
    fn test_network_error_send_failed() {
        let err = NetworkError::SendFailed {
            addr: "localhost:10911".to_string(),
            reason: "internal error".to_string(),
        };
        assert_eq!(err.to_string(), "Send failed to localhost:10911: internal error");
    }

    #[test]
    fn test_network_error_receive_failed() {
        let err = NetworkError::ReceiveFailed {
            addr: "localhost:10911".to_string(),
            reason: "internal error".to_string(),
        };
        assert_eq!(err.to_string(), "Receive failed from localhost:10911: internal error");
    }

    #[test]
    fn test_network_error_invalid_address() {
        let err = NetworkError::InvalidAddress {
            addr: "localhost:10911".to_string(),
        };
        assert_eq!(err.to_string(), "Invalid address format: localhost:10911");
    }

    #[test]
    fn test_network_error_too_many_requests() {
        let err = NetworkError::TooManyRequests {
            addr: "localhost:10911".to_string(),
            limit: 5,
        };
        assert_eq!(err.to_string(), "Too many requests to localhost:10911, limit: 5");
    }

    #[test]
    fn test_network_error_request_timeout() {
        let err = NetworkError::RequestTimeout {
            addr: "localhost:10911".to_string(),
            timeout_ms: 100,
        };
        assert_eq!(err.to_string(), "Request timeout to localhost:10911 after 100ms");
    }

    #[test]
    fn test_network_error_dns_resolution_failed() {
        let err = NetworkError::DnsResolutionFailed {
            host: "example.com".to_string(),
            reason: "host not found".to_string(),
        };
        assert_eq!(err.to_string(), "DNS resolution failed for example.com: host not found");
    }
}
