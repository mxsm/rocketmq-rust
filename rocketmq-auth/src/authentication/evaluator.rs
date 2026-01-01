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

//! Authentication evaluator - facade for authentication logic.
//!
//! This module provides the `AuthenticationEvaluator` which acts as the main entry point
//! for authentication evaluation, coordinating authentication strategies.

use rocketmq_error::AuthError;

use crate::authentication::strategy::AuthenticationStrategy;
use crate::authorization::context::authentication_context::AuthenticationContext;

/// Authentication evaluator - main entry point for authentication.
///
/// This struct acts as a facade, delegating authentication evaluation
/// to the configured authentication strategy.
///
/// # Design
///
/// - **Facade Pattern**: Simplifies authentication API for clients
/// - **Strategy Pattern**: Delegates to pluggable authentication strategies
/// - **Thread-safe**: Can be safely shared across threads
///
/// # Example
///
/// ```rust,ignore
/// use rocketmq_auth::authentication::evaluator::AuthenticationEvaluator;
/// use rocketmq_auth::authentication::strategy::StatelessAuthenticationStrategy;
/// use rocketmq_auth::config::AuthConfig;
///
/// let config = AuthConfig::default();
/// let strategy = StatelessAuthenticationStrategy::new(config, None);
/// let evaluator = AuthenticationEvaluator::new(strategy);
///
/// // Evaluate authentication
/// evaluator.evaluate(&context)?;
/// ```
pub struct AuthenticationEvaluator<S>
where
    S: AuthenticationStrategy,
{
    authentication_strategy: S,
}

impl<S> AuthenticationEvaluator<S>
where
    S: AuthenticationStrategy,
{
    /// Create a new authentication evaluator with the given strategy.
    ///
    /// # Arguments
    ///
    /// * `authentication_strategy` - The authentication strategy to use
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let strategy = StatelessAuthenticationStrategy::new(config, provider);
    /// let evaluator = AuthenticationEvaluator::new(strategy);
    /// ```
    pub fn new(authentication_strategy: S) -> Self {
        Self {
            authentication_strategy,
        }
    }

    /// Evaluate authentication for the given context.
    ///
    /// This method delegates to the underlying authentication strategy.
    /// Returns `Ok(())` if authentication succeeds, or an `AuthError` if it fails.
    ///
    /// # Arguments
    ///
    /// * `context` - The authentication context containing request information
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Authentication succeeded
    /// * `Err(AuthError)` - Authentication failed with a specific error
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let context = DefaultAuthenticationContext::new();
    /// evaluator.evaluate(&context)?;
    /// ```
    pub fn evaluate(&self, context: &dyn AuthenticationContext) -> Result<(), AuthError> {
        self.authentication_strategy.authenticate(context)
    }

    /// Get a reference to the underlying authentication strategy.
    pub fn strategy(&self) -> &S {
        &self.authentication_strategy
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authentication::context::default_authentication_context::DefaultAuthenticationContext;
    use crate::authentication::strategy::allow_all::AllowAllAuthenticationStrategy;

    struct TestStrategy {
        should_succeed: bool,
    }

    impl AuthenticationStrategy for TestStrategy {
        fn authenticate(&self, _context: &dyn AuthenticationContext) -> Result<(), AuthError> {
            if self.should_succeed {
                Ok(())
            } else {
                Err(AuthError::AuthenticationFailed(
                    "Test authentication failed".to_string(),
                ))
            }
        }
    }

    #[test]
    fn test_evaluator_creation() {
        let strategy = AllowAllAuthenticationStrategy;
        let evaluator = AuthenticationEvaluator::new(strategy);

        assert!(std::ptr::eq(&evaluator.authentication_strategy, evaluator.strategy()));
    }

    #[test]
    fn test_evaluate_success() {
        let strategy = TestStrategy { should_succeed: true };
        let evaluator = AuthenticationEvaluator::new(strategy);
        let context = DefaultAuthenticationContext::new();

        let result = evaluator.evaluate(&context);
        assert!(result.is_ok());
    }

    #[test]
    fn test_evaluate_failure() {
        let strategy = TestStrategy { should_succeed: false };
        let evaluator = AuthenticationEvaluator::new(strategy);
        let context = DefaultAuthenticationContext::new();

        let result = evaluator.evaluate(&context);
        assert!(result.is_err());

        if let Err(AuthError::AuthenticationFailed(msg)) = result {
            assert_eq!(msg, "Test authentication failed");
        } else {
            panic!("Expected AuthenticationFailed error");
        }
    }

    #[test]
    fn test_evaluate_with_allow_all_strategy() {
        let strategy = AllowAllAuthenticationStrategy;
        let evaluator = AuthenticationEvaluator::new(strategy);
        let context = DefaultAuthenticationContext::new();

        let result = evaluator.evaluate(&context);
        assert!(result.is_ok());
    }
}
