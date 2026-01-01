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

//! Default authentication handler implementation.
//!
//! This handler performs username/password authentication with signature verification.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use rocketmq_error::AuthError;

use crate::authentication::chain::acl_signer;
use crate::authentication::chain::handler::AuthenticationHandler;
use crate::authentication::context::default_authentication_context::DefaultAuthenticationContext;
use crate::authentication::enums::user_status::UserStatus;
use crate::authentication::model::user::User;
use crate::authentication::provider::AuthenticationMetadataProvider;

/// Default authentication handler.
///
/// This handler implements the core authentication logic:
/// 1. Retrieve user from metadata provider
/// 2. Check user status (enabled/disabled)
/// 3. Calculate signature and verify against provided signature
pub struct DefaultAuthenticationHandler<P: AuthenticationMetadataProvider> {
    authentication_metadata_provider: Arc<P>,
}

impl<P: AuthenticationMetadataProvider> DefaultAuthenticationHandler<P> {
    /// Create a new default authentication handler.
    ///
    /// # Arguments
    ///
    /// * `metadata_provider` - Provider for retrieving user metadata
    pub fn new(metadata_provider: Arc<P>) -> Self {
        Self {
            authentication_metadata_provider: metadata_provider,
        }
    }

    /// Get user from metadata provider.
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Username is empty/missing
    /// - User lookup fails
    async fn get_user(&self, context: &DefaultAuthenticationContext) -> Result<User, AuthError> {
        let username = context
            .username()
            .ok_or_else(|| AuthError::AuthenticationFailed("username cannot be null".to_string()))?;

        if username.is_empty() {
            return Err(AuthError::AuthenticationFailed("username cannot be empty".to_string()));
        }

        self.authentication_metadata_provider
            .get_user(username.as_str())
            .await
            .map_err(|e| AuthError::AuthenticationFailed(format!("Failed to get user: {}", e)))
    }

    /// Perform authentication logic.
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - User is disabled
    /// - Signature verification fails
    fn do_authenticate(&self, context: &DefaultAuthenticationContext, user: &User) -> Result<(), AuthError> {
        // Check user status
        if let Some(UserStatus::Disable) = user.user_status() {
            return Err(AuthError::AuthenticationFailed(format!(
                "User:{} is disabled",
                user.username()
            )));
        }

        // Get password for signature calculation
        let password = user.password().ok_or_else(|| {
            AuthError::AuthenticationFailed(format!("User:{} has no password configured", user.username()))
        })?;

        // Get content for signing
        let content = context
            .content()
            .ok_or_else(|| AuthError::AuthenticationFailed("Authentication content cannot be null".to_string()))?;

        // Calculate expected signature
        let expected_signature = acl_signer::cal_signature(content, password.as_str())?;

        // Get provided signature
        let provided_signature = context
            .signature()
            .ok_or_else(|| AuthError::AuthenticationFailed("Signature cannot be null".to_string()))?;

        // Constant-time comparison to prevent timing attacks
        if !constant_time_eq(expected_signature.as_bytes(), provided_signature.as_bytes()) {
            return Err(AuthError::AuthenticationFailed("check signature failed".to_string()));
        }

        Ok(())
    }
}

/// Constant-time equality check to prevent timing attacks.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }

    let mut result = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        result |= x ^ y;
    }

    result == 0
}

impl<P: AuthenticationMetadataProvider> AuthenticationHandler for DefaultAuthenticationHandler<P> {
    fn handle<'a>(
        &'a self,
        context: &'a DefaultAuthenticationContext,
    ) -> Pin<Box<dyn Future<Output = Result<(), AuthError>> + Send + 'a>> {
        Box::pin(async move {
            let user = self.get_user(context).await?;
            self.do_authenticate(context, &user)?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;
    use rocketmq_error::RocketMQResult;

    use super::*;
    use crate::authentication::enums::user_type::UserType;

    struct MockMetadataProvider {
        users: Vec<User>,
    }

    #[allow(async_fn_in_trait)]
    impl AuthenticationMetadataProvider for MockMetadataProvider {
        fn initialize<'a>(
            &'a mut self,
            _config: crate::config::AuthConfig,
            _metadata_service: Option<Arc<dyn std::any::Any + Send + Sync>>,
        ) -> Pin<Box<dyn Future<Output = RocketMQResult<()>> + Send + 'a>> {
            Box::pin(async { Ok(()) })
        }

        fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = RocketMQResult<()>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }

        fn get_user<'a>(
            &'a self,
            username: &'a str,
        ) -> Pin<Box<dyn Future<Output = RocketMQResult<User>> + Send + 'a>> {
            let result = self
                .users
                .iter()
                .find(|u| u.username().as_str() == username)
                .cloned()
                .ok_or_else(|| rocketmq_error::RocketMQError::user_not_found(username));

            Box::pin(async move { result })
        }

        fn create_user<'a>(&'a self, _user: User) -> Pin<Box<dyn Future<Output = RocketMQResult<()>> + Send + 'a>> {
            unimplemented!()
        }

        fn delete_user<'a>(
            &'a self,
            _username: &'a str,
        ) -> Pin<Box<dyn Future<Output = RocketMQResult<()>> + Send + 'a>> {
            unimplemented!()
        }

        fn update_user<'a>(&'a self, _user: User) -> Pin<Box<dyn Future<Output = RocketMQResult<()>> + Send + 'a>> {
            unimplemented!()
        }

        fn list_user<'a>(
            &'a self,
            _filter: Option<&'a str>,
        ) -> Pin<Box<dyn Future<Output = RocketMQResult<Vec<User>>> + Send + 'a>> {
            unimplemented!()
        }
    }

    fn create_test_user(username: &str, password: &str, status: UserStatus) -> User {
        let mut user = User::of_with_type(username, password, UserType::Super);
        user.set_user_status(status);
        user
    }

    #[tokio::test]
    async fn test_user_not_found() {
        let provider = Arc::new(MockMetadataProvider { users: vec![] });
        let handler = DefaultAuthenticationHandler::new(provider);

        let mut context = DefaultAuthenticationContext::new();
        context.set_username(CheetahString::from("unknown"));

        let result = handler.handle(&context).await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        // The error message includes "Failed to get user: User not found: unknown"
        assert!(
            err_msg.contains("unknown"),
            "Expected error message to contain 'unknown', got: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn test_user_disabled() {
        let user = create_test_user("test_user", "password", UserStatus::Disable);
        let provider = Arc::new(MockMetadataProvider { users: vec![user] });
        let handler = DefaultAuthenticationHandler::new(provider);

        let mut context = DefaultAuthenticationContext::new();
        context.set_username(CheetahString::from("test_user"));
        context.set_content(vec![1, 2, 3]);
        context.set_signature(CheetahString::from("dummy"));

        let result = handler.handle(&context).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("User:test_user is disabled"));
    }

    #[tokio::test]
    async fn test_signature_mismatch() {
        let user = create_test_user("test_user", "password", UserStatus::Enable);
        let provider = Arc::new(MockMetadataProvider { users: vec![user] });
        let handler = DefaultAuthenticationHandler::new(provider);

        let content = b"test content";
        let mut context = DefaultAuthenticationContext::new();
        context.set_username(CheetahString::from("test_user"));
        context.set_content(content.to_vec());
        context.set_signature(CheetahString::from("wrong_signature"));

        let result = handler.handle(&context).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("check signature failed"));
    }

    #[tokio::test]
    async fn test_successful_authentication() {
        let password = "test_password";
        let user = create_test_user("test_user", password, UserStatus::Enable);
        let provider = Arc::new(MockMetadataProvider { users: vec![user] });
        let handler = DefaultAuthenticationHandler::new(provider);

        let content = b"test content";
        let expected_signature = acl_signer::cal_signature(content, password).unwrap();

        let mut context = DefaultAuthenticationContext::new();
        context.set_username(CheetahString::from("test_user"));
        context.set_content(content.to_vec());
        context.set_signature(CheetahString::from(expected_signature));

        let result = handler.handle(&context).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_missing_username() {
        let provider = Arc::new(MockMetadataProvider { users: vec![] });
        let handler = DefaultAuthenticationHandler::new(provider);

        let context = DefaultAuthenticationContext::new();

        let result = handler.handle(&context).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("username cannot be null"));
    }

    #[tokio::test]
    async fn test_missing_content() {
        let user = create_test_user("test_user", "password", UserStatus::Enable);
        let provider = Arc::new(MockMetadataProvider { users: vec![user] });
        let handler = DefaultAuthenticationHandler::new(provider);

        let mut context = DefaultAuthenticationContext::new();
        context.set_username(CheetahString::from("test_user"));
        context.set_signature(CheetahString::from("sig"));

        let result = handler.handle(&context).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Authentication content cannot be null"));
    }

    #[tokio::test]
    async fn test_missing_signature() {
        let user = create_test_user("test_user", "password", UserStatus::Enable);
        let provider = Arc::new(MockMetadataProvider { users: vec![user] });
        let handler = DefaultAuthenticationHandler::new(provider);

        let mut context = DefaultAuthenticationContext::new();
        context.set_username(CheetahString::from("test_user"));
        context.set_content(vec![1, 2, 3]);

        let result = handler.handle(&context).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Signature cannot be null"));
    }

    #[test]
    fn test_constant_time_eq() {
        assert!(constant_time_eq(b"hello", b"hello"));
        assert!(!constant_time_eq(b"hello", b"world"));
        assert!(!constant_time_eq(b"hello", b"hello2"));
        assert!(!constant_time_eq(b"", b"a"));
        assert!(constant_time_eq(b"", b""));
    }
}
