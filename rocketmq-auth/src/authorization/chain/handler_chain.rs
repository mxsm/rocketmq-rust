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

//! Authorization handler chain implementation.
//!
//! This module provides a chain of authorization handlers that execute sequentially
//! until one grants access or all deny it.

use std::sync::Arc;

use rocketmq_error::RocketMQError;

use super::handler::AuthorizationHandler;
use crate::authorization::context::default_authorization_context::DefaultAuthorizationContext;

/// Authorization handler chain.
///
/// Executes handlers in sequence, implementing fail-fast semantics:
/// - If any handler returns Ok(()), authorization succeeds
/// - If all handlers return Err(...), authorization fails
/// - Execution stops at first Ok(())
///
/// # Example
///
/// ```rust,ignore
/// use rocketmq_auth::authorization::chain::{AuthorizationHandlerChain, AclAuthorizationHandler};
/// use std::sync::Arc;
///
/// let chain = AuthorizationHandlerChain::new()
///     .add_handler(Arc::new(AclAuthorizationHandler::new(provider)));
///
/// // Execute chain
/// chain.handle(&context).await?;
/// ```
pub struct AuthorizationHandlerChain {
    handlers: Vec<Arc<dyn AuthorizationHandler>>,
}

impl AuthorizationHandlerChain {
    /// Create an empty handler chain.
    pub fn new() -> Self {
        Self { handlers: Vec::new() }
    }

    /// Add a handler to the end of the chain.
    pub fn add_handler(mut self, handler: Arc<dyn AuthorizationHandler>) -> Self {
        self.handlers.push(handler);
        self
    }

    /// Execute the authorization chain.
    ///
    /// Handlers are executed in order until one succeeds.
    ///
    /// # Returns
    ///
    /// - `Ok(())` if any handler grants access
    /// - `Err(RocketMQError)` if all handlers deny access
    pub async fn handle(&self, context: &DefaultAuthorizationContext) -> Result<(), RocketMQError> {
        if self.handlers.is_empty() {
            return Err(RocketMQError::authentication_failed(
                "No authorization handlers configured",
            ));
        }

        let mut last_error = None;

        for handler in &self.handlers {
            match handler.handle(context).await {
                Ok(()) => return Ok(()), // Authorization granted
                Err(e) => {
                    last_error = Some(e);
                    // Continue to next handler
                }
            }
        }

        // All handlers failed
        Err(last_error
            .unwrap_or_else(|| RocketMQError::authentication_failed("All authorization handlers denied access")))
    }

    /// Get the number of handlers in the chain.
    pub fn len(&self) -> usize {
        self.handlers.len()
    }

    /// Check if the chain is empty.
    pub fn is_empty(&self) -> bool {
        self.handlers.is_empty()
    }
}

impl Default for AuthorizationHandlerChain {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use rocketmq_common::common::action::Action;
    use rocketmq_error::RocketMQError;

    use super::*;
    use crate::authentication::enums::subject_type::SubjectType;
    use crate::authorization::chain::handler::AuthorizationHandler;
    use crate::authorization::context::default_authorization_context::DefaultAuthorizationContext;
    use crate::authorization::model::resource::Resource;

    struct AllowHandler {
        call_count: Arc<AtomicUsize>,
    }

    impl AuthorizationHandler for AllowHandler {
        fn handle<'a>(
            &'a self,
            _context: &'a DefaultAuthorizationContext,
        ) -> Pin<Box<dyn Future<Output = Result<(), RocketMQError>> + Send + 'a>> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move { Ok(()) })
        }
    }

    struct DenyHandler {
        call_count: Arc<AtomicUsize>,
    }

    impl AuthorizationHandler for DenyHandler {
        fn handle<'a>(
            &'a self,
            _context: &'a DefaultAuthorizationContext,
        ) -> Pin<Box<dyn Future<Output = Result<(), RocketMQError>> + Send + 'a>> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move { Err(RocketMQError::authentication_failed("Access denied")) })
        }
    }

    #[tokio::test]
    async fn test_chain_single_handler_allow() {
        let counter = Arc::new(AtomicUsize::new(0));
        let handler = Arc::new(AllowHandler {
            call_count: counter.clone(),
        });

        let chain = AuthorizationHandlerChain::new().add_handler(handler);

        let context = DefaultAuthorizationContext::of(
            "user",
            SubjectType::User,
            Resource::of_topic("test"),
            Action::Pub,
            "127.0.0.1",
        );

        let result = chain.handle(&context).await;
        assert!(result.is_ok());
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_chain_single_handler_deny() {
        let counter = Arc::new(AtomicUsize::new(0));
        let handler = Arc::new(DenyHandler {
            call_count: counter.clone(),
        });

        let chain = AuthorizationHandlerChain::new().add_handler(handler);

        let context = DefaultAuthorizationContext::of(
            "user",
            SubjectType::User,
            Resource::of_topic("test"),
            Action::Pub,
            "127.0.0.1",
        );

        let result = chain.handle(&context).await;
        assert!(result.is_err());
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_chain_multiple_handlers_first_allow() {
        let counter1 = Arc::new(AtomicUsize::new(0));
        let counter2 = Arc::new(AtomicUsize::new(0));

        let chain = AuthorizationHandlerChain::new()
            .add_handler(Arc::new(AllowHandler {
                call_count: counter1.clone(),
            }))
            .add_handler(Arc::new(DenyHandler {
                call_count: counter2.clone(),
            }));

        let context = DefaultAuthorizationContext::of(
            "user",
            SubjectType::User,
            Resource::of_topic("test"),
            Action::Pub,
            "127.0.0.1",
        );

        let result = chain.handle(&context).await;
        assert!(result.is_ok());
        assert_eq!(counter1.load(Ordering::SeqCst), 1);
        assert_eq!(counter2.load(Ordering::SeqCst), 0); // Should not be called
    }

    #[tokio::test]
    async fn test_chain_multiple_handlers_second_allow() {
        let counter1 = Arc::new(AtomicUsize::new(0));
        let counter2 = Arc::new(AtomicUsize::new(0));

        let chain = AuthorizationHandlerChain::new()
            .add_handler(Arc::new(DenyHandler {
                call_count: counter1.clone(),
            }))
            .add_handler(Arc::new(AllowHandler {
                call_count: counter2.clone(),
            }));

        let context = DefaultAuthorizationContext::of(
            "user",
            SubjectType::User,
            Resource::of_topic("test"),
            Action::Pub,
            "127.0.0.1",
        );

        let result = chain.handle(&context).await;
        assert!(result.is_ok());
        assert_eq!(counter1.load(Ordering::SeqCst), 1);
        assert_eq!(counter2.load(Ordering::SeqCst), 1); // Should be called after first fails
    }

    #[tokio::test]
    async fn test_chain_all_deny() {
        let counter1 = Arc::new(AtomicUsize::new(0));
        let counter2 = Arc::new(AtomicUsize::new(0));

        let chain = AuthorizationHandlerChain::new()
            .add_handler(Arc::new(DenyHandler {
                call_count: counter1.clone(),
            }))
            .add_handler(Arc::new(DenyHandler {
                call_count: counter2.clone(),
            }));

        let context = DefaultAuthorizationContext::of(
            "user",
            SubjectType::User,
            Resource::of_topic("test"),
            Action::Pub,
            "127.0.0.1",
        );

        let result = chain.handle(&context).await;
        assert!(result.is_err());
        assert_eq!(counter1.load(Ordering::SeqCst), 1);
        assert_eq!(counter2.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_empty_chain() {
        let chain = AuthorizationHandlerChain::new();

        let context = DefaultAuthorizationContext::of(
            "user",
            SubjectType::User,
            Resource::of_topic("test"),
            Action::Pub,
            "127.0.0.1",
        );

        let result = chain.handle(&context).await;
        assert!(result.is_err());
    }
}
