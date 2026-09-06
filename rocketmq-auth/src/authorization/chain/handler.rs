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

use rocketmq_security_api::AuthorizationDecision;

use crate::authorization::context::default_authorization_context::DefaultAuthorizationContext;
use crate::AuthServiceResult;

/// Authorization handler trait.
///
/// This trait defines the interface for handlers in the authorization chain.
/// Each handler returns a final allow/deny decision. Operational failures remain
/// errors and stop the chain immediately.
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
    /// * `Ok(AuthorizationDecision)` - A final authorization decision
    /// * `Err(AuthServiceError)` - The handler could not make a decision
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use rocketmq_auth::AuthorizationHandler;
    /// use rocketmq_auth::DefaultAuthorizationContext;
    ///
    /// struct MyHandler;
    ///
    /// impl AuthorizationHandler for MyHandler {
    ///     fn handle<'a>(
    ///         &'a self,
    ///         context: &'a DefaultAuthorizationContext,
    ///     ) -> Pin<Box<dyn Future<Output = AuthServiceResult<AuthorizationDecision>> + Send + 'a>> {
    ///         Box::pin(async move {
    ///             // Authorization logic here
    ///             Ok(AuthorizationDecision::Allow)
    ///         })
    ///     }
    /// }
    /// ```
    fn handle<'a>(
        &'a self,
        context: &'a DefaultAuthorizationContext,
    ) -> Pin<Box<dyn Future<Output = AuthServiceResult<AuthorizationDecision>> + Send + 'a>>;
}
