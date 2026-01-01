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

//! Authentication handler trait for Chain of Responsibility pattern.
//!
//! # Note on rocketmq-common::Handler
//!
//! While `rocketmq_common::common::chain::Handler<T, R>` provides a general-purpose
//! synchronous chain implementation, this module provides an authentication-specific
//! async handler optimized for the following requirements:
//!
//! - **Async operations**: Authentication requires async I/O (database, network)
//! - **Error propagation**: Uses `Result<(), AuthError>` with fail-fast semantics
//! - **Sequential execution**: Automatic chain progression without manual delegation
//! - **Type safety**: Specialized for `DefaultAuthenticationContext`
//!
//! The common Handler is better suited for synchronous request/response pipelines,
//! while this implementation is tailored for async authentication workflows.

use std::future::Future;
use std::pin::Pin;

use rocketmq_error::AuthError;

use crate::authentication::context::default_authentication_context::DefaultAuthenticationContext;

/// Authentication handler trait.
///
/// This trait defines the interface for handlers in the authentication chain.
/// Unlike `rocketmq_common::Handler`, this is async-first and designed for
/// fail-fast error propagation in authentication scenarios.
///
/// # Design Pattern
///
/// This implements the Chain of Responsibility pattern, allowing multiple
/// handlers to process authentication requests in sequence.
pub trait AuthenticationHandler: Send + Sync {
    /// Handle the authentication request.
    ///
    /// # Arguments
    ///
    /// * `context` - The authentication context containing request information
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Authentication succeeded
    /// * `Err(AuthError)` - Authentication failed
    fn handle<'a>(
        &'a self,
        context: &'a DefaultAuthenticationContext,
    ) -> Pin<Box<dyn Future<Output = Result<(), AuthError>> + Send + 'a>>;
}
