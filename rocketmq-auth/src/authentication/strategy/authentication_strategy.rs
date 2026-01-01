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

//! # Authentication Strategy
//!
//! This module provides the core `AuthenticationStrategy` trait for implementing
//! pluggable authentication strategies in RocketMQ.
//!
//! ## Overview
//!
//! The `AuthenticationStrategy` trait defines a unified interface for authentication
//! mechanisms, supporting multiple authentication strategies such as:
//! - AK/SK (Access Key / Secret Key) authentication
//! - Token-based authentication
//! - mTLS (mutual TLS) authentication
//! - Custom authentication strategies
//!
//! ## Design
//!
//! The trait is designed to be:
//! - **Object-safe**: Can be used with `Arc<dyn AuthenticationStrategy>` for dynamic dispatch
//! - **Thread-safe**: Implementations should be `Send + Sync`
//! - **Async-ready**: Returns `Result` for synchronous or async authentication
//! - **Composable**: Multiple strategies can be combined via chain-of-responsibility pattern
//!
//! ## Example
//!
//! ```rust,ignore
//! use rocketmq_auth::authentication::strategy::AuthenticationStrategy;
//! use rocketmq_auth::authentication::context::AuthenticationContext;
//! use rocketmq_error::AuthError;
//!
//! struct AkSkAuthenticationStrategy;
//!
//! impl AuthenticationStrategy for AkSkAuthenticationStrategy {
//!     fn authenticate(&self, context: &dyn AuthenticationContext) -> Result<(), AuthError> {
//!         // Validate access key and secret key
//!         // ...
//!         Ok(())
//!     }
//! }
//! ```

use rocketmq_error::AuthError;

use crate::authorization::context::authentication_context::AuthenticationContext;

/// Authentication strategy trait.
///
/// This trait defines the contract for authentication strategies in RocketMQ.
/// Implementations of this trait provide specific authentication mechanisms
/// (e.g., AK/SK, token-based, mTLS).
///
/// # Thread Safety
///
/// Implementations must be thread-safe (`Send + Sync`) to allow concurrent
/// authentication requests.
///
/// # Object Safety
///
/// This trait is object-safe, meaning it can be used behind a trait object
/// (`Box<dyn AuthenticationStrategy>` or `Arc<dyn AuthenticationStrategy>`).
///
/// # Error Handling
///
/// Authentication failures should return an appropriate `AuthError` variant
/// with clear context about the failure reason. Never use `panic!` or `unwrap()`
/// in production implementations.
///
/// # Examples
///
/// ## Basic Implementation
///
/// ```rust,ignore
/// use rocketmq_auth::authentication::strategy::AuthenticationStrategy;
/// use rocketmq_error::AuthError;
///
/// struct AllowAllAuthenticationStrategy;
///
/// impl AuthenticationStrategy for AllowAllAuthenticationStrategy {
///     fn authenticate(&self, _context: &dyn AuthenticationContext) -> Result<(), AuthError> {
///         // Allow all authentication attempts (for testing only!)
///         Ok(())
///     }
/// }
/// ```
///
/// ## AK/SK Authentication
///
/// ```rust,ignore
/// use rocketmq_auth::authentication::strategy::AuthenticationStrategy;
/// use rocketmq_auth::authentication::context::DefaultAuthenticationContext;
/// use rocketmq_error::AuthError;
///
/// struct AkSkAuthenticationStrategy {
///     user_provider: Arc<dyn UserProvider>,
/// }
///
/// impl AuthenticationStrategy for AkSkAuthenticationStrategy {
///     fn authenticate(&self, context: &dyn AuthenticationContext) -> Result<(), AuthError> {
///         // Downcast to DefaultAuthenticationContext
///         let ctx = context.as_any()
///             .downcast_ref::<DefaultAuthenticationContext>()
///             .ok_or_else(|| AuthError::Other("Invalid context type".into()))?;
///
///         // Extract username and signature
///         let username = ctx.username()
///             .ok_or_else(|| AuthError::InvalidCredential("Missing username".into()))?;
///         let signature = ctx.signature()
///             .ok_or_else(|| AuthError::InvalidCredential("Missing signature".into()))?;
///
///         // Retrieve user from provider
///         let user = self.user_provider.get_user(username.as_str())
///             .map_err(|_| AuthError::UserNotFound(username.to_string()))?;
///
///         // Validate signature
///         if !verify_signature(ctx.content(), user.password(), signature) {
///             return Err(AuthError::InvalidSignature("Signature mismatch".into()));
///         }
///
///         Ok(())
///     }
/// }
/// ```
pub trait AuthenticationStrategy: Send + Sync {
    /// Authenticates a request based on the provided authentication context.
    ///
    /// This method performs authentication using credentials, metadata, and other
    /// information contained in the `AuthenticationContext`. Successful authentication
    /// returns `Ok(())`, while failures return an appropriate `AuthError`.
    ///
    /// # Arguments
    ///
    /// * `context` - The authentication context containing credentials, request metadata, and any
    ///   other information required for authentication.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Authentication succeeded
    /// * `Err(AuthError)` - Authentication failed with a specific error
    ///
    /// # Error Semantics
    ///
    /// Different error types indicate different failure modes:
    /// - `AuthError::UserNotFound` - User does not exist in the system
    /// - `AuthError::InvalidCredential` - Credentials are malformed or invalid
    /// - `AuthError::InvalidSignature` - Signature verification failed
    /// - `AuthError::InvalidUserStatus` - User account is disabled or invalid
    /// - `AuthError::AuthenticationFailed` - Generic authentication failure
    ///
    /// # Thread Safety
    ///
    /// This method must be safe to call concurrently from multiple threads.
    /// Implementations should either be stateless or use appropriate synchronization.
    ///
    /// # Performance Considerations
    ///
    /// - Minimize heap allocations in hot paths
    /// - Consider caching authenticated sessions (see `StatefulAuthenticationStrategy`)
    /// - Use constant-time comparison for security-critical operations
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use rocketmq_auth::authentication::strategy::AuthenticationStrategy;
    /// use rocketmq_auth::authentication::context::DefaultAuthenticationContext;
    /// use rocketmq_error::AuthError;
    ///
    /// fn authenticate_request(
    ///     strategy: &dyn AuthenticationStrategy,
    ///     context: &dyn AuthenticationContext,
    /// ) -> Result<(), AuthError> {
    ///     strategy.authenticate(context)?;
    ///     println!("Authentication succeeded");
    ///     Ok(())
    /// }
    /// ```
    fn authenticate(&self, context: &dyn AuthenticationContext) -> Result<(), AuthError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authentication::context::default_authentication_context::DefaultAuthenticationContext;
    use std::sync::Arc;

    /// Test implementation that always allows authentication
    struct AllowAllAuthenticationStrategy;

    impl AuthenticationStrategy for AllowAllAuthenticationStrategy {
        fn authenticate(&self, _context: &dyn AuthenticationContext) -> Result<(), AuthError> {
            Ok(())
        }
    }

    /// Test implementation that always denies authentication
    struct DenyAllAuthenticationStrategy;

    impl AuthenticationStrategy for DenyAllAuthenticationStrategy {
        fn authenticate(&self, _context: &dyn AuthenticationContext) -> Result<(), AuthError> {
            Err(AuthError::AuthenticationFailed("Denied by policy".to_string()))
        }
    }

    #[test]
    fn test_allow_all_strategy_succeeds() {
        let strategy = AllowAllAuthenticationStrategy;
        let context = DefaultAuthenticationContext::new();

        let result = strategy.authenticate(&context);
        assert!(result.is_ok());
    }

    #[test]
    fn test_deny_all_strategy_fails() {
        let strategy = DenyAllAuthenticationStrategy;
        let context = DefaultAuthenticationContext::new();

        let result = strategy.authenticate(&context);
        assert!(result.is_err());

        if let Err(AuthError::AuthenticationFailed(msg)) = result {
            assert_eq!(msg, "Denied by policy");
        } else {
            panic!("Expected AuthenticationFailed error");
        }
    }

    #[test]
    fn test_strategy_is_object_safe() {
        let strategy: Arc<dyn AuthenticationStrategy> = Arc::new(AllowAllAuthenticationStrategy);
        let context = DefaultAuthenticationContext::new();

        let result = strategy.authenticate(&context);
        assert!(result.is_ok());
    }

    #[test]
    fn test_strategy_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<AllowAllAuthenticationStrategy>();
        assert_send_sync::<DenyAllAuthenticationStrategy>();
    }

    #[test]
    fn test_multiple_strategies_can_coexist() {
        let allow_strategy: Arc<dyn AuthenticationStrategy> = Arc::new(AllowAllAuthenticationStrategy);
        let deny_strategy: Arc<dyn AuthenticationStrategy> = Arc::new(DenyAllAuthenticationStrategy);

        let context = DefaultAuthenticationContext::new();

        assert!(allow_strategy.authenticate(&context).is_ok());
        assert!(deny_strategy.authenticate(&context).is_err());
    }
}
