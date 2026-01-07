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

//! Authorization Evaluator - Facade for authorization decision making
//!
//! This module provides the `AuthorizationEvaluator` struct, which serves as the main
//! entry point for authorization evaluation in RocketMQ. It coordinates with
//! `AuthorizationStrategy` to make authorization decisions based on request context.
//!
//! # Architecture
//!
//! ```text
//! AuthorizationEvaluator (Facade/Evaluator Pattern)
//! └── AuthorizationStrategy (Strategy Pattern)
//!     └── AuthorizationProvider (Provider Pattern)
//! ```
//!
//! # Design Principles
//!
//! - **Facade Pattern**: Simplifies authorization interface for clients
//! - **Strategy Pattern**: Delegates decision logic to pluggable strategies
//! - **Stateless**: Evaluator itself holds no mutable state
//! - **Fail-safe**: Early return on empty input, explicit error handling
//!
//! # Example
//!
//! ```rust,ignore
//! use rocketmq_auth::authorization::evaluator::AuthorizationEvaluator;
//! use rocketmq_auth::authorization::context::default_authorization_context::DefaultAuthorizationContext;
//! use rocketmq_auth::authorization::strategy::stateless_authorization_strategy::StatelessAuthorizationStrategy;
//! use rocketmq_auth::config::AuthConfig;
//!
//! let config = AuthConfig::default();
//! let strategy = StatelessAuthorizationStrategy::new(config, None);
//! let evaluator = AuthorizationEvaluator::new(strategy);
//!
//! let contexts = vec![/* authorization contexts */];
//! evaluator.evaluate(&contexts)?;
//! ```

use crate::authorization::context::default_authorization_context::DefaultAuthorizationContext;
use crate::authorization::provider::AuthorizationError;
use crate::authorization::strategy::abstract_authorization_strategy::AuthorizationStrategy;

/// Authorization evaluator result type
pub type EvaluatorResult<T> = Result<T, AuthorizationError>;

/// Authorization evaluator - main entry point for authorization decisions
///
/// The `AuthorizationEvaluator` acts as a facade that coordinates authorization
/// evaluation through a configured `AuthorizationStrategy`. It provides a simplified
/// interface for authorization checks across the RocketMQ system.
///
/// # Thread Safety
///
/// This struct is `Send + Sync` and can be safely shared across threads using `Arc`.
/// The underlying strategy is also `Send + Sync`.
///
/// # Behavior
///
/// - **Empty input**: Returns `Ok(())` immediately without evaluation
/// - **Strategy delegation**: Delegates each context to the configured strategy
/// - **Error propagation**: First authorization failure aborts evaluation
///
/// # Examples
///
/// ```rust,ignore
/// // With stateless strategy
/// let strategy = StatelessAuthorizationStrategy::new(config, None);
/// let evaluator = AuthorizationEvaluator::new(strategy);
///
/// let context = DefaultAuthorizationContext::of(subject, resource, action, source_ip);
/// evaluator.evaluate(&[context])?;
///
/// // With stateful (cached) strategy
/// let strategy = StatefulAuthorizationStrategy::new(config, None);
/// let evaluator = AuthorizationEvaluator::new(strategy);
/// evaluator.evaluate(&contexts)?;
/// ```
pub struct AuthorizationEvaluator<S>
where
    S: AuthorizationStrategy,
{
    /// The authorization strategy used for decision making
    authorization_strategy: S,
}

impl<S> AuthorizationEvaluator<S>
where
    S: AuthorizationStrategy,
{
    /// Create a new authorization evaluator with the given strategy
    ///
    /// # Arguments
    ///
    /// * `authorization_strategy` - The strategy to use for authorization decisions
    ///
    /// # Returns
    ///
    /// A new `AuthorizationEvaluator` instance
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use rocketmq_auth::authorization::evaluator::AuthorizationEvaluator;
    /// use rocketmq_auth::authorization::strategy::stateless_authorization_strategy::StatelessAuthorizationStrategy;
    ///
    /// let strategy = StatelessAuthorizationStrategy::new(config, None);
    /// let evaluator = AuthorizationEvaluator::new(strategy);
    /// ```
    pub fn new(authorization_strategy: S) -> Self {
        Self { authorization_strategy }
    }

    /// Evaluate a list of authorization contexts
    ///
    /// This is the main authorization entry point. It iterates through each context
    /// and delegates evaluation to the configured strategy. If any context fails
    /// authorization, the evaluation stops and returns the error.
    ///
    /// # Arguments
    ///
    /// * `contexts` - Slice of authorization contexts to evaluate
    ///
    /// # Returns
    ///
    /// * `Ok(())` - All contexts passed authorization
    /// * `Err(AuthorizationError)` - At least one context failed authorization
    ///
    /// # Behavior
    ///
    /// - Empty slice: Returns `Ok(())` immediately
    /// - Non-empty: Evaluates each context sequentially
    /// - Early termination: Stops at first authorization failure
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let contexts = vec![
    ///     DefaultAuthorizationContext::of(subject1, resource1, action1, source_ip1),
    ///     DefaultAuthorizationContext::of(subject2, resource2, action2, source_ip2),
    /// ];
    ///
    /// // All contexts must pass for Ok(())
    /// evaluator.evaluate(&contexts)?;
    /// ```
    ///
    /// # Errors
    ///
    /// Returns `AuthorizationError` if:
    /// - Subject does not have permission for the requested action
    /// - Policy evaluation fails
    /// - Resource or subject not found
    /// - Source IP not allowed
    /// - Decision is DENY
    pub fn evaluate(&self, contexts: &[DefaultAuthorizationContext]) -> EvaluatorResult<()> {
        // Early return on empty input
        if contexts.is_empty() {
            return Ok(());
        }

        // Evaluate each context through the strategy
        for context in contexts {
            self.authorization_strategy.evaluate(context)?;
        }

        Ok(())
    }

    /// Get a reference to the underlying authorization strategy
    ///
    /// This method allows access to the strategy for advanced use cases such as
    /// inspection or custom configuration.
    ///
    /// # Returns
    ///
    /// A reference to the authorization strategy
    pub fn strategy(&self) -> &S {
        &self.authorization_strategy
    }
}

// Implement Send + Sync for thread-safe usage
// Safety: AuthorizationStrategy is Send + Sync, so evaluator is too
unsafe impl<S> Send for AuthorizationEvaluator<S> where S: AuthorizationStrategy {}
unsafe impl<S> Sync for AuthorizationEvaluator<S> where S: AuthorizationStrategy {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authorization::strategy::abstract_authorization_strategy::StrategyResult;

    // Mock strategy for testing
    struct MockStrategy {
        should_fail: bool,
    }

    impl AuthorizationStrategy for MockStrategy {
        fn evaluate(&self, _context: &DefaultAuthorizationContext) -> StrategyResult<()> {
            if self.should_fail {
                Err(AuthorizationError::PermissionDenied {
                    subject: "test".to_string(),
                    resource: "test".to_string(),
                    reason: "mock failure".to_string(),
                })
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn test_evaluator_empty_contexts() {
        let strategy = MockStrategy { should_fail: false };
        let evaluator = AuthorizationEvaluator::new(strategy);

        let result = evaluator.evaluate(&[]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_evaluator_success() {
        let strategy = MockStrategy { should_fail: false };
        let evaluator = AuthorizationEvaluator::new(strategy);

        let context = DefaultAuthorizationContext::default();
        let result = evaluator.evaluate(&[context]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_evaluator_failure() {
        let strategy = MockStrategy { should_fail: true };
        let evaluator = AuthorizationEvaluator::new(strategy);

        let context = DefaultAuthorizationContext::default();
        let result = evaluator.evaluate(&[context]);
        assert!(result.is_err());
    }

    #[test]
    fn test_evaluator_multiple_contexts_success() {
        let strategy = MockStrategy { should_fail: false };
        let evaluator = AuthorizationEvaluator::new(strategy);

        let contexts = vec![
            DefaultAuthorizationContext::default(),
            DefaultAuthorizationContext::default(),
            DefaultAuthorizationContext::default(),
        ];
        let result = evaluator.evaluate(&contexts);
        assert!(result.is_ok());
    }

    #[test]
    fn test_evaluator_multiple_contexts_one_failure() {
        let strategy = MockStrategy { should_fail: true };
        let evaluator = AuthorizationEvaluator::new(strategy);

        let contexts = vec![
            DefaultAuthorizationContext::default(),
            DefaultAuthorizationContext::default(),
        ];
        let result = evaluator.evaluate(&contexts);
        assert!(result.is_err());
    }

    #[test]
    fn test_evaluator_get_strategy() {
        let strategy = MockStrategy { should_fail: false };
        let evaluator = AuthorizationEvaluator::new(strategy);

        let _strategy_ref = evaluator.strategy();
        // Just verify we can access the strategy
    }
}
