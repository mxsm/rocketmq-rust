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

//! # RocketMQ Error Handling System
//!
//! This crate provides a unified, semantic, and performant error handling system
//! for the RocketMQ Rust implementation.
//!
//! ## New Unified Error System (v0.7.0+)
//!
//! The new error system provides:
//! - **Semantic clarity**: Each error type clearly expresses what went wrong
//! - **Performance**: Minimal heap allocations, optimized for hot paths
//! - **Ergonomics**: Automatic error conversions via `From` trait
//! - **Debuggability**: Rich context for production debugging
//!
//! ### Usage
//!
//! ```rust
//! use rocketmq_error::RocketMQError;
//! use rocketmq_error::RocketMQResult;
//!
//! fn send_message(addr: &str) -> RocketMQResult<()> {
//!     if addr.is_empty() {
//!         return Err(RocketMQError::network_connection_failed(
//!             "localhost:9876",
//!             "invalid address",
//!         ));
//!     }
//!     Ok(())
//! }
//! # send_message("localhost:9876").unwrap();
//! ```
//!
//! ## Legacy Error System (Deprecated)
//!
//! The legacy `RocketmqError` enum is still available for backward compatibility
//! but will be removed in a future version. Please migrate to the new unified system.

// New unified error system
pub mod unified;

// Auth error module
pub mod auth_error;

// Controller error module
pub mod controller_error;

// Filter error module
pub mod filter_error;

// Re-export new error types as primary API
// Re-export auth error types from unified module
// Re-export controller error types
pub use controller_error::ControllerError;
pub use controller_error::ControllerResult;
// Re-export filter error types
pub use filter_error::FilterError;
pub use unified::AuthError;
pub use unified::NetworkError;
pub use unified::ProtocolError;
pub use unified::RocketMQError;
pub use unified::RpcClientError;
pub use unified::SerializationError;
// Re-export result types (but don't conflict with legacy ones below)
pub use unified::ServiceError as UnifiedServiceError;
pub use unified::ToolsError;

// Legacy error modules (deprecated but kept for compatibility)
#[deprecated(since = "0.7.0", note = "Use unified error system instead")]
mod cli_error;
#[deprecated(since = "0.7.0", note = "Use unified error system instead")]
mod client_error;
#[deprecated(since = "0.7.0", note = "Use unified error system instead")]
mod common_error;
#[deprecated(since = "0.7.0", note = "Use unified error system instead")]
mod name_srv_error;
#[deprecated(since = "0.7.0", note = "Use unified error system instead")]
mod remoting_error;
#[deprecated(since = "0.7.0", note = "Use unified error system instead")]
mod store_error;
#[deprecated(since = "0.7.0", note = "Use unified error system instead")]
mod tui_error;

use std::io;

use thiserror::Error;

// Legacy type aliases (deprecated - use RocketMQResult and Result from unified module)
// Kept for backward compatibility with existing code
#[deprecated(since = "0.7.0", note = "Use unified::RocketMQResult instead")]
pub type LegacyRocketMQResult<T> = std::result::Result<T, RocketmqError>;

#[deprecated(since = "0.7.0", note = "Use unified::Result instead")]
pub type LegacyResult<T> = anyhow::Result<T>;

// Re-export unified result types as the primary API
pub use unified::Result;
pub use unified::RocketMQResult;
// Import ServiceError for use in legacy RocketmqError enum
use unified::ServiceError;

#[derive(Debug, Error)]
pub enum RocketmqError {
    // remoting errors
    #[error("{0}")]
    RemoteError(String),

    #[error("{0}")]
    DeserializeHeaderError(String),

    #[error("connect to {0} failed")]
    RemotingConnectError(String),

    #[error("send request to < {0} > failed")]
    RemotingSendRequestError(String),

    #[error("wait response on the channel < {0}  >, timeout: {1}(ms)")]
    RemotingTimeoutError(String, u64),

    #[error("RemotingTooMuchRequestException: {0}")]
    RemotingTooMuchRequestError(String),

    #[error("RpcException: code: {0}, message: {1}")]
    RpcError(i32, String),

    #[error("{0}")]
    FromStrErr(String),

    #[error("{0:?}")]
    Io(#[from] io::Error),

    #[error("RocketMQ protocol decoding failed, extFields length: {0}, but header length: {1}")]
    DecodingError(usize, usize),

    #[error("UTF-8 decoding error: {0}")]
    Utf8Error(#[from] std::str::Utf8Error),

    #[error("RemotingCommandDecoderError:{0}")]
    RemotingCommandDecoderError(String),

    #[error("RemotingCommandEncoderError:{0}")]
    RemotingCommandEncoderError(String),

    #[error("Not support serialize type: {0}")]
    NotSupportSerializeType(u8),

    #[error("ConnectionInvalid: {0}")]
    ConnectionInvalid(String),

    #[error("AbortProcessError: {0}-{1}")]
    AbortProcessError(i32, String),

    #[error("Channel Send Request failed: {0}")]
    ChannelSendRequestFailed(String),

    #[error("Channel recv Request failed: {0}")]
    ChannelRecvRequestFailed(String),

    #[error("{0}")]
    IllegalArgument(String),

    //client error
    #[error("{0}")]
    MQClientErr(#[from] ClientErr),

    #[error("{0}")]
    MQClientBrokerError(#[from] MQBrokerErr),

    #[error("{0}")]
    RequestTimeoutError(#[from] RequestTimeoutErr),

    #[error("Client exception occurred: CODE:{0}, broker address:{1}, Message:{2}")]
    OffsetNotFoundError(i32, String, String),

    #[error("{0}")]
    IllegalArgumentError(String),

    #[error("{0}")]
    #[cfg(feature = "with_serde")]
    SerdeJsonError(#[from] serde_json::Error),

    #[error("{0}")]
    UnsupportedOperationException(String),

    #[error("{0}")]
    IpError(String),

    #[error("{0}")]
    ChannelError(String),

    #[error("Client exception occurred: CODE:{0}, broker address:{2}, Message:{1}")]
    MQBrokerError(i32, String, String),

    #[error("{0}")]
    NoneError(String),

    #[error("{0}")]
    TokioHandlerError(String),

    #[error("Config parse error: {0}")]
    #[cfg(feature = "with_config")]
    ConfigError(#[from] config::ConfigError),

    #[error("{0} command failed , {1}")]
    SubCommand(String, String),

    #[error("{0}")]
    ServiceTaskError(#[from] ServiceError),

    #[error("{0}")]
    StoreCustomError(String),
}

#[derive(Error, Debug)]
#[error("{message}")]
pub struct MQBrokerErr {
    response_code: i32,
    error_message: Option<String>,
    broker_addr: Option<String>,
    message: String,
}

impl MQBrokerErr {
    pub fn new(response_code: i32, error_message: impl Into<String>) -> Self {
        let error_message = error_message.into();
        /*let message = FAQUrl::attach_default_url(Some(
            format!("CODE: {}  DESC: {}", response_code, error_message,).as_str(),
        ));*/
        let message = "";
        Self {
            response_code,
            error_message: Some(error_message),
            broker_addr: None,
            message: String::from(message),
        }
    }

    pub fn new_with_broker(
        response_code: i32,
        error_message: impl Into<String>,
        broker_addr: impl Into<String>,
    ) -> Self {
        let broker_addr = broker_addr.into();
        let error_message = error_message.into();
        /*let message = FAQUrl::attach_default_url(Some(
            format!(
                "CODE: {}  DESC: {} BROKER: {}",
                response_code, error_message, broker_addr
            )
            .as_str(),
        ));*/
        let message = "";
        Self {
            response_code,
            error_message: Some(error_message),
            broker_addr: Some(broker_addr),
            message: String::from(message),
        }
    }

    pub fn response_code(&self) -> i32 {
        self.response_code
    }

    pub fn error_message(&self) -> Option<&String> {
        self.error_message.as_ref()
    }

    pub fn broker_addr(&self) -> Option<&String> {
        self.broker_addr.as_ref()
    }
}

#[macro_export]
macro_rules! client_broker_err {
    // Handle errors with a custom ResponseCode and formatted string
    ($response_code:expr, $error_message:expr, $broker_addr:expr) => {{
        std::result::Result::Err($crate::RocketmqError::MQClientBrokerError(
            $crate::MQBrokerErr::new_with_broker($response_code as i32, $error_message, $broker_addr),
        ))
    }};
    // Handle errors without a ResponseCode, using only the error message
    ($response_code:expr, $error_message:expr) => {{
        std::result::Result::Err($crate::RocketmqError::MQClientBrokerError($crate::MQBrokerErr::new(
            $response_code as i32,
            $error_message,
        )))
    }};
}

#[derive(Error, Debug)]
#[error("{message}")]
pub struct ClientErr {
    response_code: i32,
    error_message: Option<String>,
    message: String,
}

impl ClientErr {
    pub fn new(error_message: impl Into<String>) -> Self {
        let error_message = error_message.into();
        let message = "string";
        // let message = FAQUrl::attach_default_url(Some(error_message.as_str()));
        Self {
            response_code: -1,
            error_message: Some(error_message),
            message: String::from(message),
        }
    }

    pub fn new_with_code(response_code: i32, error_message: impl Into<String>) -> Self {
        let error_message = error_message.into();
        /*let message = FAQUrl::attach_default_url(Some(
            format!("CODE: {}  DESC: {}", response_code, error_message,).as_str(),
        ));*/
        let message = "";
        Self {
            response_code,
            error_message: Some(error_message),
            message: String::from(message),
        }
    }

    pub fn response_code(&self) -> i32 {
        self.response_code
    }

    pub fn error_message(&self) -> Option<&String> {
        self.error_message.as_ref()
    }
}

// Legacy macro - deprecated in favor of new unified error system
#[deprecated(
    since = "0.7.0",
    note = "Use unified error system and macros from rocketmq-client instead"
)]
#[macro_export]
macro_rules! mq_client_err_legacy {
    // Handle errors with a custom ResponseCode and formatted string
    ($response_code:expr, $fmt:expr, $($arg:expr),*) => {{
        let formatted_msg = format!($fmt, $($arg),*);
        std::result::Result::Err($crate::client_error::MQClientError::MQClientErr(
            $crate::client_error::ClientErr::new_with_code($response_code as i32, formatted_msg),
        ))
    }};

    ($response_code:expr, $error_message:expr) => {{
        std::result::Result::Err($crate::RocketmqError::MQClientErr(
            $crate::ClientErr::new_with_code(
                $response_code as i32,
                $error_message,
            ),
        ))
    }};

    // Handle errors without a ResponseCode, using only the error message
    ($error_message:expr) => {{
        std::result::Result::Err($crate::RocketmqError::MQClientErr(
            $crate::ClientErr::new($error_message),
        ))
    }};
}

#[derive(Error, Debug)]
#[error("{message}")]
pub struct RequestTimeoutErr {
    response_code: i32,
    error_message: Option<String>,
    message: String,
}

impl RequestTimeoutErr {
    pub fn new(error_message: impl Into<String>) -> Self {
        let error_message = error_message.into();
        //let message = FAQUrl::attach_default_url(Some(error_message.as_str()));
        let message = "FAQUrl::attach_default_url(Some(error_message.as_str()))";
        Self {
            response_code: -1,
            error_message: Some(error_message),
            message: String::from(message),
        }
    }

    pub fn new_with_code(response_code: i32, error_message: impl Into<String>) -> Self {
        let error_message = error_message.into();
        // let message = FAQUrl::attach_default_url(Some(
        //     format!("CODE: {}  DESC: {}", response_code, error_message,).as_str(),
        // ));
        let message =
            "FAQUrl::attach_default_url(Some(format!(\"CODE: {}  DESC: {}\", response_code, error_message,).as_str()))";
        Self {
            response_code,
            error_message: Some(error_message),
            message: String::from(message),
        }
    }

    pub fn response_code(&self) -> i32 {
        self.response_code
    }

    pub fn error_message(&self) -> Option<&String> {
        self.error_message.as_ref()
    }
}

#[macro_export]
macro_rules! request_timeout_err {
    // Handle errors with a custom ResponseCode and formatted string
    ($response_code:expr, $fmt:expr, $($arg:expr),*) => {{
        let formatted_msg = format!($fmt, $($arg),*);
        std::result::Result::Err($crate::RocketmqError::RequestTimeoutError(
            $crate::RequestTimeoutErr::new_with_code(
                $response_code as i32,
                formatted_msg,
            ),
        ))
    }};
    ($response_code:expr, $error_message:expr) => {{
        std::result::Result::Err($crate::RocketmqError::RequestTimeoutError(
            $crate::RequestTimeoutErr::new_with_code(
                $response_code as i32,
                $error_message,
            ),
        ))
    }};
    // Handle errors without a ResponseCode, using only the error message
    ($error_message:expr) => {{
        std::result::Result::Err($crate::RocketmqError::RequestTimeoutError(
            $crate::RequestTimeoutErr::new($error_message),
        ))
    }};
}

//------------------Legacy ServiceError (deprecated)------------------

/// Service error enumeration (LEGACY - deprecated)
///
/// Use `unified::ServiceError` instead
#[deprecated(since = "0.7.0", note = "Use unified::ServiceError instead")]
#[derive(Debug, thiserror::Error)]
pub enum LegacyServiceError {
    #[error("Service is already running")]
    AlreadyRunning,

    #[error("Service is not running")]
    NotRunning,

    #[error("Service startup failed: {0}")]
    StartupFailed(String),

    #[error("Service shutdown failed: {0}")]
    ShutdownFailed(String),

    #[error("Service operation timeout")]
    Timeout,

    #[error("Service interrupted")]
    Interrupted,
}

// Note: ServiceError is re-exported at the top of this file from unified module

//------------------Automatic conversion from legacy to unified------------------

/// Automatic conversion from legacy `RocketmqError` to unified `RocketMQError`
///
/// This allows legacy error-producing code to work with new error-expecting code
impl From<RocketmqError> for unified::RocketMQError {
    fn from(err: RocketmqError) -> Self {
        match err {
            // Network errors
            RocketmqError::RemoteError(msg) => Self::network_connection_failed("unknown", msg),
            RocketmqError::RemotingConnectError(addr) => Self::network_connection_failed(addr, "connection failed"),
            RocketmqError::RemotingSendRequestError(addr) => {
                Self::network_connection_failed(addr, "send request failed")
            }
            RocketmqError::RemotingTimeoutError(addr, timeout) => {
                Self::Network(unified::NetworkError::RequestTimeout {
                    addr,
                    timeout_ms: timeout,
                })
            }
            RocketmqError::RemotingTooMuchRequestError(msg) => Self::illegal_argument(msg),

            // Protocol errors
            RocketmqError::DeserializeHeaderError(msg) => {
                Self::Serialization(unified::SerializationError::DecodeFailed {
                    format: "header",
                    message: msg,
                })
            }
            RocketmqError::RemotingCommandDecoderError(_msg) => Self::Protocol(unified::ProtocolError::DecodeError {
                ext_fields_len: 0,
                header_len: 0,
            }),
            RocketmqError::DecodingError(required, available) => {
                Self::Serialization(unified::SerializationError::DecodeFailed {
                    format: "binary",
                    message: format!("required {} bytes, got {}", required, available),
                })
            }
            RocketmqError::NotSupportSerializeType(t) => {
                Self::Protocol(unified::ProtocolError::UnsupportedSerializationType { serialize_type: t })
            }

            // Broker errors
            RocketmqError::MQBrokerError(code, msg, addr) => {
                Self::broker_operation_failed("BROKER_OPERATION", code, msg).with_broker_addr(addr)
            }
            RocketmqError::MQClientBrokerError(err) => {
                let mut e = Self::broker_operation_failed(
                    "BROKER_OPERATION",
                    err.response_code(),
                    err.error_message().unwrap_or(&String::new()).clone(),
                );
                if let Some(addr) = err.broker_addr() {
                    e = e.with_broker_addr(addr.clone());
                }
                e
            }
            RocketmqError::OffsetNotFoundError(code, addr, msg) => {
                Self::broker_operation_failed("OFFSET_NOT_FOUND", code, msg).with_broker_addr(addr)
            }

            // Client errors
            RocketmqError::MQClientErr(err) => {
                Self::illegal_argument(err.error_message().unwrap_or(&String::new()).clone())
            }
            RocketmqError::RequestTimeoutError(_) => Self::Timeout {
                operation: "request",
                timeout_ms: 3000,
            },

            // System errors
            RocketmqError::Io(err) => Self::IO(err),
            RocketmqError::IllegalArgument(msg) | RocketmqError::IllegalArgumentError(msg) => {
                Self::illegal_argument(msg)
            }
            RocketmqError::UnsupportedOperationException(msg) => Self::illegal_argument(msg),
            RocketmqError::IpError(msg) => Self::illegal_argument(format!("IP error: {}", msg)),
            RocketmqError::ChannelError(msg) => Self::Internal(format!("Channel error: {}", msg)),
            RocketmqError::NoneError(msg) => Self::Internal(format!("None error: {}", msg)),
            RocketmqError::TokioHandlerError(msg) => Self::Internal(format!("Tokio handler error: {}", msg)),
            RocketmqError::SubCommand(cmd, msg) => Self::Internal(format!("{} command failed: {}", cmd, msg)),
            RocketmqError::StoreCustomError(msg) => Self::StorageReadFailed {
                path: "unknown".to_string(),
                reason: msg,
            },

            // Other errors
            RocketmqError::ServiceTaskError(err) => Self::Service(err),

            #[cfg(feature = "with_serde")]
            RocketmqError::SerdeJsonError(err) => {
                Self::Serialization(unified::SerializationError::JsonError(err.to_string()))
            }

            #[cfg(feature = "with_config")]
            RocketmqError::ConfigError(err) => Self::ConfigParseFailed {
                key: "unknown",
                reason: err.to_string(),
            },

            // Handle remaining variants
            RocketmqError::RpcError(code, msg) => Self::broker_operation_failed("RPC", code, msg),
            RocketmqError::FromStrErr(msg) => Self::illegal_argument(msg),
            RocketmqError::Utf8Error(err) => Self::Serialization(unified::SerializationError::Utf8Error(err)),
            RocketmqError::ConnectionInvalid(msg) => Self::network_connection_failed("unknown", msg),
            RocketmqError::AbortProcessError(code, msg) => {
                Self::Internal(format!("Abort process error {}: {}", code, msg))
            }
            RocketmqError::ChannelSendRequestFailed(msg) => Self::network_connection_failed("channel", msg),
            RocketmqError::ChannelRecvRequestFailed(msg) => Self::network_connection_failed("channel", msg),
            RocketmqError::RemotingCommandEncoderError(msg) => {
                Self::Serialization(unified::SerializationError::EncodeFailed {
                    format: "command",
                    message: msg,
                })
            }
        }
    }
}
