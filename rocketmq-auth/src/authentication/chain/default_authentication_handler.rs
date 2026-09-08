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

use rocketmq_runtime::common::time_utils::current_millis;

use crate::authentication::chain::acl_signer;
use crate::authentication::chain::acl_signer::SignatureAlgorithm;
use crate::authentication::chain::handler::AuthenticationHandler;
use crate::authentication::context::default_authentication_context::DefaultAuthenticationContext;
use crate::authentication::enums::user_status::UserStatus;
use crate::authentication::model::user::User;
use crate::authentication::provider::AuthenticationMetadataProvider;
use crate::AuthFailureKind;
use crate::AuthMetrics;
use crate::AuthOperation;
use crate::AuthServiceError;
use crate::AuthServiceResult;

/// Default authentication handler.
///
/// This handler implements the core authentication logic:
/// 1. Retrieve user from metadata provider
/// 2. Check user status (enabled/disabled)
/// 3. Calculate signature and verify against provided signature
pub struct DefaultAuthenticationHandler<P: AuthenticationMetadataProvider> {
    authentication_metadata_provider: Arc<P>,
    signature_algorithm: SignatureAlgorithm,
    request_timestamp_expired_millis: u64,
    metrics: AuthMetrics,
}

impl<P: AuthenticationMetadataProvider> DefaultAuthenticationHandler<P> {
    pub fn with_options(
        metadata_provider: Arc<P>,
        signature_algorithm: SignatureAlgorithm,
        request_timestamp_expired_millis: u64,
        metrics: AuthMetrics,
    ) -> Self {
        Self {
            authentication_metadata_provider: metadata_provider,
            signature_algorithm,
            request_timestamp_expired_millis,
            metrics,
        }
    }

    /// Get user from metadata provider.
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Username is empty/missing
    /// - User lookup fails
    async fn get_user(&self, context: &DefaultAuthenticationContext) -> AuthServiceResult<User> {
        let username = context.username().ok_or_else(authentication_failed)?;

        if username.is_empty() {
            return Err(authentication_failed());
        }

        self.authentication_metadata_provider
            .get_user(username.as_str())
            .await
            .map_err(|source| {
                let kind = source.kind();
                AuthServiceError::with_source(AuthOperation::Authenticate, kind, source)
            })
    }

    /// Perform authentication logic.
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - User is disabled
    /// - Signature verification fails
    fn do_authenticate(&self, context: &DefaultAuthenticationContext, user: &User) -> AuthServiceResult<()> {
        self.validate_request_timestamp(context)?;

        // Check user status
        if let Some(UserStatus::Disable) = user.user_status() {
            return Err(authentication_failed());
        }

        // Get password for signature calculation
        let password = user.password().ok_or_else(authentication_failed)?;

        // Get content for signing
        let content = context.content().ok_or_else(authentication_failed)?;

        // Calculate expected signature
        let expected_signature =
            acl_signer::cal_signature_with_algorithm(content, password.as_str(), self.signature_algorithm)?;

        // Get provided signature
        let provided_signature = context.signature().ok_or_else(authentication_failed)?;

        // Constant-time comparison to prevent timing attacks
        let signatures_match = constant_time_eq(expected_signature.as_bytes(), provided_signature.as_bytes());
        self.metrics.record_signature_verification(signatures_match);
        if !signatures_match {
            return Err(authentication_failed());
        }

        Ok(())
    }

    fn validate_request_timestamp(&self, context: &DefaultAuthenticationContext) -> AuthServiceResult<()> {
        if self.request_timestamp_expired_millis == 0 {
            return Ok(());
        }

        let Some(request_timestamp_millis) = context.request_timestamp_millis() else {
            if context.request_timestamp().is_some() {
                return Err(authentication_failed());
            }
            return Ok(());
        };

        if request_timestamp_millis < 0 {
            return Err(authentication_failed());
        }

        let now_millis = current_millis() as i64;
        let skew_millis = now_millis.abs_diff(request_timestamp_millis);
        if skew_millis > self.request_timestamp_expired_millis {
            return Err(AuthServiceError::new(
                AuthOperation::Authenticate,
                AuthFailureKind::Expired,
            ));
        }

        Ok(())
    }
}

fn authentication_failed() -> AuthServiceError {
    AuthServiceError::new(AuthOperation::Authenticate, AuthFailureKind::Unauthenticated)
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
    ) -> Pin<Box<dyn Future<Output = AuthServiceResult<()>> + Send + 'a>> {
        Box::pin(async move {
            let user = self.get_user(context).await?;
            self.do_authenticate(context, &user)?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authentication::enums::user_type::UserType;
    use cheetah_string::CheetahString;

    struct MockMetadataProvider {
        users: Vec<User>,
    }

    #[allow(async_fn_in_trait)]
    impl AuthenticationMetadataProvider for MockMetadataProvider {
        fn initialize<'a>(
            &'a mut self,
            _config: crate::config::AuthConfig,
            _metadata_service: Option<Arc<dyn std::any::Any + Send + Sync>>,
        ) -> Pin<Box<dyn Future<Output = AuthServiceResult<()>> + Send + 'a>> {
            Box::pin(async { Ok(()) })
        }

        fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = AuthServiceResult<()>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }

        fn get_user<'a>(
            &'a self,
            username: &'a str,
        ) -> Pin<Box<dyn Future<Output = AuthServiceResult<User>> + Send + 'a>> {
            let result = self
                .users
                .iter()
                .find(|u| u.username().as_str() == username)
                .cloned()
                .ok_or_else(|| AuthServiceError::new(AuthOperation::ReadMetadata, AuthFailureKind::NotFound));

            Box::pin(async move { result })
        }

        fn create_user<'a>(&'a self, _user: User) -> Pin<Box<dyn Future<Output = AuthServiceResult<()>> + Send + 'a>> {
            Box::pin(async {
                Err(AuthServiceError::new(
                    AuthOperation::ManageMetadata,
                    AuthFailureKind::Unsupported,
                ))
            })
        }

        fn delete_user<'a>(
            &'a self,
            _username: &'a str,
        ) -> Pin<Box<dyn Future<Output = AuthServiceResult<()>> + Send + 'a>> {
            Box::pin(async {
                Err(AuthServiceError::new(
                    AuthOperation::ManageMetadata,
                    AuthFailureKind::Unsupported,
                ))
            })
        }

        fn update_user<'a>(&'a self, _user: User) -> Pin<Box<dyn Future<Output = AuthServiceResult<()>> + Send + 'a>> {
            Box::pin(async {
                Err(AuthServiceError::new(
                    AuthOperation::ManageMetadata,
                    AuthFailureKind::Unsupported,
                ))
            })
        }

        fn list_user<'a>(
            &'a self,
            _filter: Option<&'a str>,
        ) -> Pin<Box<dyn Future<Output = AuthServiceResult<Vec<User>>> + Send + 'a>> {
            let users = self.users.clone();
            Box::pin(async move { Ok(users) })
        }
    }

    fn create_test_user(username: &str, password: &str, status: UserStatus) -> User {
        let mut user = User::of_with_type(username, password, UserType::Super);
        user.set_user_status(status);
        user
    }

    fn create_handler(provider: Arc<MockMetadataProvider>) -> DefaultAuthenticationHandler<MockMetadataProvider> {
        DefaultAuthenticationHandler::with_options(provider, SignatureAlgorithm::default(), 0, AuthMetrics::default())
    }

    #[tokio::test]
    async fn test_user_not_found() {
        let provider = Arc::new(MockMetadataProvider { users: vec![] });
        let handler = create_handler(provider);

        let mut context = DefaultAuthenticationContext::new();
        context.set_username(CheetahString::from("unknown"));

        let error = handler
            .handle(&context)
            .await
            .expect_err("unknown user must be rejected");
        assert_eq!(error.operation(), AuthOperation::Authenticate);
        assert_eq!(error.kind(), AuthFailureKind::NotFound);
        let source = std::error::Error::source(&error)
            .and_then(|source| source.downcast_ref::<AuthServiceError>())
            .expect("metadata-provider error must remain typed");
        assert_eq!(source.operation(), AuthOperation::ReadMetadata);
        assert_eq!(source.kind(), AuthFailureKind::NotFound);
        assert!(
            !error.to_string().contains("unknown"),
            "top-level error must remain redacted"
        );
    }

    #[tokio::test]
    async fn test_user_disabled() {
        let user = create_test_user("test_user", "password", UserStatus::Disable);
        let provider = Arc::new(MockMetadataProvider { users: vec![user] });
        let handler = create_handler(provider);

        let mut context = DefaultAuthenticationContext::new();
        context.set_username(CheetahString::from("test_user"));
        context.set_content(vec![1, 2, 3]);
        context.set_signature(CheetahString::from("dummy"));

        let result = handler.handle(&context).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), AuthFailureKind::Unauthenticated);
    }

    #[tokio::test]
    async fn test_signature_mismatch() {
        let user = create_test_user("test_user", "password", UserStatus::Enable);
        let provider = Arc::new(MockMetadataProvider { users: vec![user] });
        let handler = create_handler(provider);

        let content = b"test content";
        let mut context = DefaultAuthenticationContext::new();
        context.set_username(CheetahString::from("test_user"));
        context.set_content(content.to_vec());
        context.set_signature(CheetahString::from("wrong_signature"));

        let result = handler.handle(&context).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), AuthFailureKind::Unauthenticated);
    }

    #[tokio::test]
    async fn test_successful_authentication() {
        let password = "test_password";
        let user = create_test_user("test_user", password, UserStatus::Enable);
        let provider = Arc::new(MockMetadataProvider { users: vec![user] });
        let handler = create_handler(provider);

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
    async fn test_request_timestamp_expired() {
        let password = "test_password";
        let user = create_test_user("test_user", password, UserStatus::Enable);
        let provider = Arc::new(MockMetadataProvider { users: vec![user] });
        let handler = DefaultAuthenticationHandler::with_options(
            provider,
            SignatureAlgorithm::HmacSha1,
            1,
            AuthMetrics::default(),
        );

        let content = b"test content";
        let expected_signature = acl_signer::cal_signature(content, password).unwrap();

        let mut context = DefaultAuthenticationContext::new();
        context.set_username(CheetahString::from("test_user"));
        context.set_content(content.to_vec());
        context.set_signature(CheetahString::from(expected_signature));
        context.set_request_timestamp(CheetahString::from("1"));
        context.set_request_timestamp_millis(1);

        let result = handler.handle(&context).await;

        assert_eq!(
            result.expect_err("timestamp must expire").kind(),
            AuthFailureKind::Expired
        );
    }

    #[tokio::test]
    async fn test_absent_request_timestamp_is_compatible_when_window_enabled() {
        let password = "test_password";
        let user = create_test_user("test_user", password, UserStatus::Enable);
        let provider = Arc::new(MockMetadataProvider { users: vec![user] });
        let handler = DefaultAuthenticationHandler::with_options(
            provider,
            SignatureAlgorithm::HmacSha1,
            1,
            AuthMetrics::default(),
        );

        let content = b"test content";
        let expected_signature = acl_signer::cal_signature(content, password).unwrap();

        let mut context = DefaultAuthenticationContext::new();
        context.set_username(CheetahString::from("test_user"));
        context.set_content(content.to_vec());
        context.set_signature(CheetahString::from(expected_signature));

        assert!(handler.handle(&context).await.is_ok());
    }

    #[tokio::test]
    async fn test_missing_username() {
        let provider = Arc::new(MockMetadataProvider { users: vec![] });
        let handler = create_handler(provider);

        let context = DefaultAuthenticationContext::new();

        let result = handler.handle(&context).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), AuthFailureKind::Unauthenticated);
    }

    #[tokio::test]
    async fn test_missing_content() {
        let user = create_test_user("test_user", "password", UserStatus::Enable);
        let provider = Arc::new(MockMetadataProvider { users: vec![user] });
        let handler = create_handler(provider);

        let mut context = DefaultAuthenticationContext::new();
        context.set_username(CheetahString::from("test_user"));
        context.set_signature(CheetahString::from("sig"));

        let result = handler.handle(&context).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), AuthFailureKind::Unauthenticated);
    }

    #[tokio::test]
    async fn test_missing_signature() {
        let user = create_test_user("test_user", "password", UserStatus::Enable);
        let provider = Arc::new(MockMetadataProvider { users: vec![user] });
        let handler = create_handler(provider);

        let mut context = DefaultAuthenticationContext::new();
        context.set_username(CheetahString::from("test_user"));
        context.set_content(vec![1, 2, 3]);

        let result = handler.handle(&context).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), AuthFailureKind::Unauthenticated);
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
