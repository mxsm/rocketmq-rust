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

//! Authentication handler chain implementation.
//!
//! # Note on rocketmq-common::HandlerChain
//!
//! This module provides an authentication-specific handler chain that differs from
//! `rocketmq_common::common::chain::HandlerChain<T, R>` in the following ways:
//!
//! - **Async execution**: Supports async handlers with `.await`
//! - **Fail-fast semantics**: Stops at first error instead of using `Option<R>`
//! - **Simplified API**: Automatic sequential execution without manual chain delegation
//! - **Type specialization**: Optimized for `DefaultAuthenticationContext` and `AuthError`
//!
//! The common HandlerChain uses `Cell<usize>` for interior mutability in sync contexts,
//! while this implementation uses simple iteration for async handler execution.

use std::sync::Arc;

use rocketmq_error::AuthError;

use crate::authentication::chain::handler::AuthenticationHandler;
use crate::authentication::context::default_authentication_context::DefaultAuthenticationContext;

/// Authentication handler chain.
///
/// Manages a sequence of authentication handlers and executes them in order.
/// Unlike `rocketmq_common::HandlerChain`, this is async-first with fail-fast semantics.
pub struct AuthenticationHandlerChain {
    handlers: Vec<Arc<dyn AuthenticationHandler>>,
}

impl AuthenticationHandlerChain {
    /// Create a new empty handler chain.
    pub fn new() -> Self {
        Self { handlers: Vec::new() }
    }

    /// Create a handler chain with pre-configured handlers.
    pub fn with_handlers(handlers: Vec<Arc<dyn AuthenticationHandler>>) -> Self {
        Self { handlers }
    }

    /// Add a handler to the chain.
    pub fn add_handler(&mut self, handler: Arc<dyn AuthenticationHandler>) {
        self.handlers.push(handler);
    }

    /// Execute the handler chain.
    ///
    /// Handlers are executed in the order they were added.
    /// The chain stops at the first error.
    pub async fn execute(&self, context: &DefaultAuthenticationContext) -> Result<(), AuthError> {
        for handler in &self.handlers {
            handler.handle(context).await?;
        }
        Ok(())
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

impl Default for AuthenticationHandlerChain {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;

    use super::*;

    struct TestHandler {
        should_fail: bool,
    }

    impl AuthenticationHandler for TestHandler {
        fn handle<'a>(
            &'a self,
            _context: &'a DefaultAuthenticationContext,
        ) -> Pin<Box<dyn Future<Output = Result<(), AuthError>> + Send + 'a>> {
            let should_fail = self.should_fail;
            Box::pin(async move {
                if should_fail {
                    Err(AuthError::AuthenticationFailed("Test failure".to_string()))
                } else {
                    Ok(())
                }
            })
        }
    }

    #[tokio::test]
    async fn test_empty_chain() {
        let chain = AuthenticationHandlerChain::new();
        let context = DefaultAuthenticationContext::new();

        assert!(chain.is_empty());
        assert_eq!(chain.len(), 0);

        let result = chain.execute(&context).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_single_handler_success() {
        let mut chain = AuthenticationHandlerChain::new();
        chain.add_handler(Arc::new(TestHandler { should_fail: false }));

        let context = DefaultAuthenticationContext::new();
        let result = chain.execute(&context).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_single_handler_failure() {
        let mut chain = AuthenticationHandlerChain::new();
        chain.add_handler(Arc::new(TestHandler { should_fail: true }));

        let context = DefaultAuthenticationContext::new();
        let result = chain.execute(&context).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_multiple_handlers_all_success() {
        let mut chain = AuthenticationHandlerChain::new();
        chain.add_handler(Arc::new(TestHandler { should_fail: false }));
        chain.add_handler(Arc::new(TestHandler { should_fail: false }));
        chain.add_handler(Arc::new(TestHandler { should_fail: false }));

        let context = DefaultAuthenticationContext::new();
        let result = chain.execute(&context).await;

        assert_eq!(chain.len(), 3);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_chain_stops_at_first_error() {
        let mut chain = AuthenticationHandlerChain::new();
        chain.add_handler(Arc::new(TestHandler { should_fail: false }));
        chain.add_handler(Arc::new(TestHandler { should_fail: true }));
        chain.add_handler(Arc::new(TestHandler { should_fail: false }));

        let context = DefaultAuthenticationContext::new();
        let result = chain.execute(&context).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_with_handlers_constructor() {
        let handlers: Vec<Arc<dyn AuthenticationHandler>> = vec![
            Arc::new(TestHandler { should_fail: false }),
            Arc::new(TestHandler { should_fail: false }),
        ];

        let chain = AuthenticationHandlerChain::with_handlers(handlers);
        let context = DefaultAuthenticationContext::new();

        assert_eq!(chain.len(), 2);

        let result = chain.execute(&context).await;
        assert!(result.is_ok());
    }
}
