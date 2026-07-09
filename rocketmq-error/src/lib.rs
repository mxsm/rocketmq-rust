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
//! ## Public Error Surface
//!
//! The crate exports the typed `RocketMQError` enum and stable supporting
//! contracts only. Pre-typed compatibility aliases and enum variants are not
//! part of the public API.

// New unified error system
pub mod unified;

// Stable error taxonomy
pub mod boundary;
pub mod cli;
pub mod context;
pub mod kind;
pub mod policy;
pub mod spec;

// Auth error module
pub mod auth_error;

// Controller error module
pub mod controller_error;

// Filter error module
pub mod filter_error;

// Observability error module
pub mod observability_error;

// Client error module
pub mod client_error;

// Re-export new error types as primary API
// Re-export auth error types from unified module
// Re-export controller error types
pub use boundary::BoundaryErrorView;
pub use boundary::CliExitCode;
pub use boundary::CliSpec;
pub use boundary::GrpcPayloadCode;
pub use boundary::GrpcSpec;
pub use boundary::GrpcStatusCode;
pub use boundary::HttpSpec;
pub use boundary::HttpStatusCode;
pub use boundary::RemotingResponseCode;
pub use boundary::RemotingSpec;
pub use cli::CliErrorView;
pub use client_error::ClientError;
pub use context::ErrorContext;
pub use context::ErrorContextField;
pub use context::RedactionKind;
pub use context::RedactionPolicy;
pub use context::Sensitive;
pub use context::REDACTED;
pub use controller_error::ControllerError;
pub use controller_error::ControllerResult;
// Re-export filter error types
pub use filter_error::FilterError;
pub use kind::ErrorCategory;
pub use kind::ErrorCode;
pub use kind::ErrorKind;
pub use kind::ErrorScope;
pub use observability_error::ObservabilityError;
pub use policy::ErrorSeverity;
pub use policy::ObserveSpec;
pub use policy::RecoverySpec;
pub use policy::RetryClass;
pub use spec::error_spec;
pub use spec::ErrorSpec;
pub use spec::ALL_ERROR_SPECS;
pub use unified::AuthError;
pub use unified::NetworkError;
pub use unified::ProtocolError;
pub use unified::RocketMQError;
pub use unified::RocketMQResult;
pub use unified::RpcClientError;
pub use unified::SerializationError;
pub use unified::ServiceError as UnifiedServiceError;
pub use unified::ToolsError;
