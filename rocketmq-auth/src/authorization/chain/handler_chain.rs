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
//! until one grants access, all deny it, or an operational failure stops evaluation.

use std::sync::Arc;

use rocketmq_security_api::AuthorizationDecision;
#[cfg(test)]
use rocketmq_security_api::AuthorizationDenial;

use super::handler::AuthorizationHandler;
use crate::authorization::context::default_authorization_context::DefaultAuthorizationContext;
use crate::authorization::provider::AuthorizationError;
use crate::authorization::provider::AuthorizationResult;

/// Authorization handler chain.
///
/// Executes handlers in sequence with fail-closed semantics. An allow grants
/// access, ordinary denials may be followed by another authorization source,
/// and operational errors stop evaluation immediately.
///
/// # Example
///
/// ```rust,ignore
/// use rocketmq_auth::{AclAuthorizationHandler, AuthorizationHandlerChain};
/// use std::sync::Arc;
///
/// let chain = AuthorizationHandlerChain::new()
///     .add_handler(Arc::new(AclAuthorizationHandler::new(provider)));
///
/// // Execute the chain and consume both normal outcomes.
/// match chain.handle(&context).await? {
///     AuthorizationDecision::Allow => proceed(),
///     AuthorizationDecision::Deny(reason) => reject(reason),
/// }
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
    /// Handlers are executed in order until one allows the request, every
    /// handler denies it, or an operational failure occurs.
    ///
    /// # Returns
    ///
    /// - `Ok(AuthorizationDecision)` for a final allow or deny
    /// - `Err(AuthorizationError)` when evaluation cannot produce a decision
    pub async fn handle(&self, context: &DefaultAuthorizationContext) -> AuthorizationResult<AuthorizationDecision> {
        if self.handlers.is_empty() {
            return Err(AuthorizationError::NotInitialized(
                "no authorization handlers configured".to_owned(),
            ));
        }

        let mut denial = None;
        for handler in &self.handlers {
            match handler.handle(context).await {
                Ok(AuthorizationDecision::Allow) => return Ok(AuthorizationDecision::Allow),
                Ok(AuthorizationDecision::Deny(reason)) => denial.get_or_insert(reason),
                Err(error) => return Err(error),
            };
        }

        denial.map_or_else(
            || {
                Err(AuthorizationError::NotInitialized(
                    "authorization handler chain did not produce a decision".to_owned(),
                ))
            },
            |reason| Ok(AuthorizationDecision::Deny(reason)),
        )
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

    use rocketmq_security_api::Action;

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
        ) -> Pin<Box<dyn Future<Output = AuthorizationResult<AuthorizationDecision>> + Send + 'a>> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move { Ok(AuthorizationDecision::Allow) })
        }
    }

    struct DenyHandler {
        call_count: Arc<AtomicUsize>,
    }

    impl AuthorizationHandler for DenyHandler {
        fn handle<'a>(
            &'a self,
            _context: &'a DefaultAuthorizationContext,
        ) -> Pin<Box<dyn Future<Output = AuthorizationResult<AuthorizationDecision>> + Send + 'a>> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move { Ok(AuthorizationDecision::Deny(AuthorizationDenial::PermissionDenied)) })
        }
    }

    struct FailureHandler {
        call_count: Arc<AtomicUsize>,
    }

    impl AuthorizationHandler for FailureHandler {
        fn handle<'a>(
            &'a self,
            _context: &'a DefaultAuthorizationContext,
        ) -> Pin<Box<dyn Future<Output = AuthorizationResult<AuthorizationDecision>> + Send + 'a>> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move { Err(AuthorizationError::InvalidContext("broken context".to_owned())) })
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
        assert_eq!(result.unwrap(), AuthorizationDecision::Allow);
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
        assert_eq!(
            result.unwrap(),
            AuthorizationDecision::Deny(AuthorizationDenial::PermissionDenied)
        );
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
        assert_eq!(result.unwrap(), AuthorizationDecision::Allow);
        assert_eq!(counter1.load(Ordering::SeqCst), 1);
        assert_eq!(counter2.load(Ordering::SeqCst), 0); // Should not be called
    }

    #[tokio::test]
    async fn test_chain_deny_can_be_followed_by_allow() {
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
        assert_eq!(result.unwrap(), AuthorizationDecision::Allow);
        assert_eq!(counter1.load(Ordering::SeqCst), 1);
        assert_eq!(counter2.load(Ordering::SeqCst), 1);
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
        assert_eq!(
            result.unwrap(),
            AuthorizationDecision::Deny(AuthorizationDenial::PermissionDenied)
        );
        assert_eq!(counter1.load(Ordering::SeqCst), 1);
        assert_eq!(counter2.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_chain_operational_failure_is_terminal() {
        let failure_count = Arc::new(AtomicUsize::new(0));
        let allow_count = Arc::new(AtomicUsize::new(0));
        let chain = AuthorizationHandlerChain::new()
            .add_handler(Arc::new(FailureHandler {
                call_count: failure_count.clone(),
            }))
            .add_handler(Arc::new(AllowHandler {
                call_count: allow_count.clone(),
            }));
        let context = DefaultAuthorizationContext::default();

        assert!(matches!(
            chain.handle(&context).await,
            Err(AuthorizationError::InvalidContext(_))
        ));
        assert_eq!(failure_count.load(Ordering::SeqCst), 1);
        assert_eq!(allow_count.load(Ordering::SeqCst), 0);
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
