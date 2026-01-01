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

//! Allow-all authentication strategy.
//!
//! This strategy allows all authentication attempts to succeed.
//! It is intended for use in development and testing environments only.
//!
//! **WARNING: DO NOT USE IN PRODUCTION!**
//!
//! This strategy provides no security and should never be used in production
//! environments. It is provided solely for development convenience and testing.

use rocketmq_error::AuthError;

use crate::authentication::strategy::AuthenticationStrategy;
use crate::authorization::context::authentication_context::AuthenticationContext;

/// Authentication strategy that allows all requests.
///
/// This strategy unconditionally succeeds for all authentication attempts,
/// regardless of credentials or context. It is designed for:
/// - Development environments where authentication is not required
/// - Testing scenarios where authentication should be bypassed
/// - Debugging authentication-related issues
///
/// # Security Warning
///
/// **This strategy provides NO security whatsoever!**
///
/// Never use this strategy in production environments. All authentication
/// requests will succeed, effectively disabling authentication entirely.
///
/// # Examples
///
/// ```rust,ignore
/// use rocketmq_auth::authentication::strategy::allow_all::AllowAllAuthenticationStrategy;
/// use rocketmq_auth::authentication::strategy::AuthenticationStrategy;
/// use rocketmq_auth::authentication::context::DefaultAuthenticationContext;
///
/// // Create the strategy
/// let strategy = AllowAllAuthenticationStrategy::new();
///
/// // All authentication attempts succeed
/// let context = DefaultAuthenticationContext::new();
/// assert!(strategy.authenticate(&context).is_ok());
/// ```
#[derive(Debug, Clone, Default)]
pub struct AllowAllAuthenticationStrategy;

impl AllowAllAuthenticationStrategy {
    /// Creates a new `AllowAllAuthenticationStrategy`.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use rocketmq_auth::authentication::strategy::allow_all::AllowAllAuthenticationStrategy;
    ///
    /// let strategy = AllowAllAuthenticationStrategy::new();
    /// ```
    #[inline]
    pub fn new() -> Self {
        Self
    }
}

impl AuthenticationStrategy for AllowAllAuthenticationStrategy {
    /// Always returns `Ok(())`, allowing all authentication attempts.
    ///
    /// This implementation ignores the context entirely and always succeeds.
    ///
    /// # Arguments
    ///
    /// * `_context` - The authentication context (ignored)
    ///
    /// # Returns
    ///
    /// Always returns `Ok(())` indicating successful authentication.
    fn authenticate(&self, _context: &dyn AuthenticationContext) -> Result<(), AuthError> {
        // Allow all authentication attempts
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authentication::context::default_authentication_context::DefaultAuthenticationContext;
    use cheetah_string::CheetahString;
    use std::sync::Arc;

    #[test]
    fn test_new_creates_strategy() {
        let strategy = AllowAllAuthenticationStrategy::new();
        let context = DefaultAuthenticationContext::new();
        assert!(strategy.authenticate(&context).is_ok());
    }

    #[test]
    fn test_default_creates_strategy() {
        let strategy = AllowAllAuthenticationStrategy;
        let context = DefaultAuthenticationContext::new();
        assert!(strategy.authenticate(&context).is_ok());
    }

    #[test]
    fn test_allows_empty_context() {
        let strategy = AllowAllAuthenticationStrategy::new();
        let context = DefaultAuthenticationContext::new();

        let result = strategy.authenticate(&context);
        assert!(result.is_ok());
    }

    #[test]
    fn test_allows_context_with_credentials() {
        let strategy = AllowAllAuthenticationStrategy::new();
        let mut context = DefaultAuthenticationContext::new();
        context.set_username(CheetahString::from("test_user"));
        context.set_signature(CheetahString::from("invalid_signature"));

        let result = strategy.authenticate(&context);
        assert!(result.is_ok());
    }

    #[test]
    fn test_allows_context_with_invalid_data() {
        let strategy = AllowAllAuthenticationStrategy::new();
        let mut context = DefaultAuthenticationContext::new();
        context.set_username(CheetahString::from(""));
        context.set_content(vec![]);

        let result = strategy.authenticate(&context);
        assert!(result.is_ok());
    }

    #[test]
    fn test_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<AllowAllAuthenticationStrategy>();
    }

    #[test]
    fn test_can_be_used_as_trait_object() {
        let strategy: Arc<dyn AuthenticationStrategy> = Arc::new(AllowAllAuthenticationStrategy::new());
        let context = DefaultAuthenticationContext::new();

        let result = strategy.authenticate(&context);
        assert!(result.is_ok());
    }

    #[test]
    fn test_clone_works() {
        let strategy1 = AllowAllAuthenticationStrategy::new();
        let strategy2 = strategy1.clone();

        let context = DefaultAuthenticationContext::new();
        assert!(strategy1.authenticate(&context).is_ok());
        assert!(strategy2.authenticate(&context).is_ok());
    }

    #[test]
    fn test_debug_impl() {
        let strategy = AllowAllAuthenticationStrategy::new();
        let debug_string = format!("{:?}", strategy);
        assert_eq!(debug_string, "AllowAllAuthenticationStrategy");
    }
}
