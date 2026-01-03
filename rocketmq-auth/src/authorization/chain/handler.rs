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

//! Authorization handler trait for Chain of Responsibility pattern.
//!
//! This module provides async authorization handlers optimized for:
//! - Async operations (metadata lookup, policy evaluation)
//! - Error propagation with fail-fast semantics
//! - Sequential chain execution
//! - Type safety with DefaultAuthorizationContext

use std::future::Future;
use std::pin::Pin;

use rocketmq_error::RocketMQError;

use crate::authorization::context::default_authorization_context::DefaultAuthorizationContext;

/// Authorization handler trait.
///
/// This trait defines the interface for handlers in the authorization chain.
/// Each handler processes an authorization request and can either:
/// - Grant access (return Ok(()))
/// - Deny access (return Err(RocketMQError))
///
/// # Design Pattern
///
/// Implements Chain of Responsibility, allowing multiple authorization
/// strategies (ACL, RBAC, ABAC) to be chained together.
///
/// # Thread Safety
///
/// Implementations must be Send + Sync for concurrent access.
pub trait AuthorizationHandler: Send + Sync {
    /// Handle the authorization request.
    ///
    /// # Arguments
    ///
    /// * `context` - The authorization context containing subject, resource, actions, etc.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Authorization granted
    /// * `Err(RocketMQError)` - Authorization denied with specific error
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use rocketmq_auth::authorization::chain::AuthorizationHandler;
    /// use rocketmq_auth::authorization::context::DefaultAuthorizationContext;
    ///
    /// struct MyHandler;
    ///
    /// impl AuthorizationHandler for MyHandler {
    ///     fn handle<'a>(
    ///         &'a self,
    ///         context: &'a DefaultAuthorizationContext,
    ///     ) -> Pin<Box<dyn Future<Output = Result<(), RocketMQError>> + Send + 'a>> {
    ///         Box::pin(async move {
    ///             // Authorization logic here
    ///             Ok(())
    ///         })
    ///     }
    /// }
    /// ```
    fn handle<'a>(
        &'a self,
        context: &'a DefaultAuthorizationContext,
    ) -> Pin<Box<dyn Future<Output = Result<(), RocketMQError>> + Send + 'a>>;
}
