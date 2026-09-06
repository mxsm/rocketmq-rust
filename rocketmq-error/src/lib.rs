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

//! # RocketMQ Error Handling
//!
//! This crate provides the canonical catalog, opaque [`Error`] envelope,
//! typed context, safe views, and boundary-neutral policy used by RocketMQ
//! components.
//!
//! Catalog descriptors are the sole owners of stable identity and policy.
//! Error instances retain one direct typed source and record the first caller
//! that promotes a failure into the canonical envelope. Formatting and safe
//! views never render source text, caller paths, or backtrace frames.
//!
//! ```rust
//! use std::io;
//! use rocketmq_error::{Error, Result, RUNTIME_IO_FAILED};
//!
//! fn read_metadata() -> Result<()> {
//!     Err(Error::caused_by(
//!         &RUNTIME_IO_FAILED,
//!         io::Error::other("metadata read failed"),
//!     ))
//! }
//!
//! let error = read_metadata().expect_err("example failure");
//! assert_eq!(error.code().as_str(), "runtime.io.failed");
//! ```
//!
//! [`RocketMQError`] remains the workspace-wide domain facade. Its `Shared`
//! variant carries a canonical [`SharedError`], while retained domain enums
//! preserve their explicitly owned variants and descriptor projections.

mod auth_error;
mod boundary;
mod catalog;
mod cli;
mod context;
mod descriptor;
mod domain;
mod error;
mod field;
mod filter_error;
mod observability_error;
mod projection;
mod recovery;
mod unified;
mod view;

// Re-export new error types as primary API
// Re-export auth error types from unified module
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
pub use catalog::*;
pub use cli::CliErrorView;
pub use cli::CliOutput;
pub use cli::CliVerbosity;
pub use context::ErrorContext;
pub use context::ErrorContextField;
pub use context::FieldValueRef;
pub use context::Sensitive;
pub use context::REDACTED;
pub use descriptor::BacktracePolicy;
pub use descriptor::ComponentId;
pub use descriptor::ErrorClass;
pub use descriptor::ErrorCode;
pub use descriptor::ErrorDescriptor;
pub use descriptor::ErrorSeverity;
pub use descriptor::Exposure;
pub use descriptor::FaultAttribution;
pub use domain::DomainError;
pub use error::Error;
pub use error::Result;
pub use error::SharedError;
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
pub use observability_error::ObservabilityError;
pub use projection::ProjectionSpec;
pub use recovery::CanonicalCondition;
pub use recovery::RecoveryHint;
pub use unified::AuthError;
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
