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

#![deny(missing_docs)]

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

mod auth_error;
mod boundary;
mod catalog;
mod cli;
mod context;
mod controller_error;
mod descriptor;
mod domain;
mod field;
mod filter_error;
mod kind;
mod observability_error;
mod policy;
mod projection;
mod recovery;
mod shared;
mod spec;
mod unified;
mod view;

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
pub use catalog::descriptor_by_code;
pub use catalog::ALL_DESCRIPTORS;
pub use catalog::AUTH_CREDENTIALS_INVALID;
pub use catalog::AUTH_PERMISSION_DENIED;
pub use catalog::CONTROLLER_LEADERSHIP_NOT_LEADER;
pub use catalog::CORE_INTERNAL_FAILURE;
pub use catalog::PROTOCOL_HEADER_INVALID;
pub use catalog::PROTOCOL_VERSION_UNSUPPORTED;
pub use catalog::ROUTE_TOPIC_NOT_FOUND;
pub use catalog::STORAGE_COMMIT_LOG_CORRUPT_RECORD;
pub use catalog::TRANSPORT_ADMISSION_QUEUE_SATURATED;
pub use catalog::TRANSPORT_CONNECTION_TIMEOUT;
pub use cli::CliErrorView;
pub use context::ErrorContext;
pub use context::ErrorContextField;
pub use context::FieldValueRef;
pub use context::RedactionPolicy;
pub use context::Sensitive;
pub use context::REDACTED;
pub use controller_error::ControllerError;
pub use controller_error::ControllerResult;
pub use descriptor::ErrorCode;
pub use descriptor::ErrorDescriptor;
pub use domain::DomainError;
pub use field::fields;
pub use field::BoolField;
pub use field::ContextVisibility;
pub use field::FieldKey;
pub use field::FieldSchema;
pub use field::FieldValueKind;
pub use field::I64Field;
pub use field::SecretPresenceField;
pub use field::TextField;
pub use field::U64Field;
// Re-export filter error types
pub use filter_error::FilterCompileError;
pub use filter_error::FilterCompileErrorKind;
pub use filter_error::FilterCompileSource;
pub use filter_error::FilterCompileStage;
pub use filter_error::FilterError;
pub use kind::ErrorCategory;
pub use kind::ErrorKind;
pub use kind::ErrorScope;
pub use observability_error::ObservabilityError;
pub use policy::ErrorSeverity;
pub use policy::ObserveSpec;
pub use policy::RecoverySpec;
pub use policy::RetryClass;
pub use projection::ProjectionSpec;
pub use recovery::CanonicalCondition;
pub use recovery::RecoveryHint;
pub use shared::SharedRocketMQError;
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
pub use view::DiagnosticFields;
pub use view::DiagnosticView;
pub use view::PublicErrorView;
pub use view::PublicFields;
pub use view::ViewContextViolation;
pub use view::ViewFieldRef;
pub use view::ViewValueRef;
