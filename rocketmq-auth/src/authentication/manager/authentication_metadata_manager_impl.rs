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

//! Authentication metadata manager implementation.
//!
//! This module provides the default implementation of `AuthenticationMetadataManager`,
//! coordinating authentication and authorization metadata providers.

use std::sync::Arc;

use rocketmq_error::RocketMQError;
use tokio::sync::RwLock;
use tracing::info;
use tracing::warn;

use super::authentication_metadata_manager::AuthenticationMetadataManager;
use super::authentication_metadata_manager::ManagerResult;
use crate::authentication::enums::user_status::UserStatus;
use crate::authentication::enums::user_type::UserType;
use crate::authentication::model::user::User;
use crate::authentication::provider::AuthenticationMetadataProvider;
use crate::authorization::metadata_provider::AuthorizationMetadataProvider;
use crate::config::AuthConfig;

/// Default implementation of `AuthenticationMetadataManager`.
///
/// This implementation coordinates between:
/// - `AuthenticationMetadataProvider` - For user CRUD operations
/// - `AuthorizationMetadataProvider` - For ACL operations (optional)
///
/// # Concurrency
///
/// Thread-safe through `Arc<RwLock<>>` for mutable provider references.
/// Read operations acquire read locks, write operations acquire write locks.
pub struct AuthenticationMetadataManagerImpl<A, B>
where
    A: AuthenticationMetadataProvider + 'static,
    B: AuthorizationMetadataProvider + 'static,
{
    /// Authentication metadata provider for user operations.
    authentication_metadata_provider: Arc<RwLock<Option<A>>>,

    /// Authorization metadata provider for ACL operations (optional).
    authorization_metadata_provider: Arc<RwLock<Option<B>>>,
}

impl<A, B> AuthenticationMetadataManagerImpl<A, B>
where
    A: AuthenticationMetadataProvider,
    B: AuthorizationMetadataProvider,
{
    /// Create a new authentication metadata manager.
    ///
    /// # Arguments
    ///
    /// * `auth_provider` - Authentication metadata provider
    /// * `authz_provider` - Authorization metadata provider (optional)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let auth_provider = LocalAuthenticationMetadataProvider::new();
    /// let authz_provider = Some(LocalAuthorizationMetadataProvider::new());
    /// let manager = AuthenticationMetadataManagerImpl::new(auth_provider, authz_provider);
    /// ```
    pub fn new(auth_provider: Option<A>, authz_provider: Option<B>) -> Self {
        Self {
            authentication_metadata_provider: Arc::new(RwLock::new(auth_provider)),
            authorization_metadata_provider: Arc::new(RwLock::new(authz_provider)),
        }
    }

    /// Set the authentication metadata provider.
    pub async fn set_authentication_provider(&self, provider: A) {
        let mut guard = self.authentication_metadata_provider.write().await;
        *guard = Some(provider);
    }

    /// Set the authorization metadata provider.
    pub async fn set_authorization_provider(&self, provider: B) {
        let mut guard = self.authorization_metadata_provider.write().await;
        *guard = Some(provider);
    }

    /// Validate user for creation.
    ///
    /// # Validation Rules
    ///
    /// - Username must not be blank
    /// - Password must not be blank (for create operation)
    /// - User type must be set (for create operation)
    fn validate_user(&self, user: &User, is_create: bool) -> ManagerResult<()> {
        // Validate username
        if user.username().is_empty() {
            return Err(RocketMQError::authentication_failed("username can not be blank"));
        }

        // Validate password (required for creation)
        if is_create && user.password().is_none() {
            return Err(RocketMQError::authentication_failed("password can not be blank"));
        }

        // Validate user type (required for creation, optional for update)
        if is_create && user.user_type().is_none() {
            return Err(RocketMQError::authentication_failed("userType can not be blank"));
        }

        Ok(())
    }

    /// Handle exceptions from provider operations.
    ///
    /// Converts provider exceptions into consistent RocketMQError types.
    fn handle_exception(&self, error: RocketMQError) -> RocketMQError {
        // Log the error
        warn!("Authentication metadata manager operation failed: {}", error);

        // For now, just pass through the error
        // In production, you might want to map specific error types
        error
    }

    /// Initialize user from string (supports both JSON and "username:password" format)
    async fn init_user_from_string(&self, init_user_str: &str, provider: &A) -> ManagerResult<()> {
        // Try to parse as JSON first
        if init_user_str.trim().starts_with('{') {
            // JSON format - try to parse as User
            if let Ok(mut user) = serde_json::from_str::<User>(init_user_str) {
                user.set_user_type(UserType::Super);
                self.create_user_if_not_exists(user, provider).await?;
                return Ok(());
            }
            warn!("Failed to parse init_authentication_user as JSON, trying simple format");
        }

        // Fall back to "username:password" format
        let parts: Vec<&str> = init_user_str.splitn(2, ':').collect();
        if parts.len() != 2 {
            warn!(
                "Invalid init_authentication_user format: '{}'. Expected 'username:password' or JSON",
                init_user_str
            );
            return Ok(()); // Don't fail initialization
        }

        let username = parts[0].trim();
        let password = parts[1].trim();

        if !username.is_empty() && !password.is_empty() {
            let mut user = User::of_with_type(username, password, UserType::Super);
            user.set_user_status(UserStatus::Enable);
            self.create_user_if_not_exists(user, provider).await?;
        }

        Ok(())
    }

    /// Initialize inner client user from SessionCredentials format
    async fn init_inner_client_user(&self, credentials_str: &str, provider: &A) -> ManagerResult<()> {
        // Try to parse as JSON with accessKey and secretKey fields
        #[derive(serde::Deserialize)]
        struct SessionCredentials {
            #[serde(rename = "accessKey")]
            access_key: String,
            #[serde(rename = "secretKey")]
            secret_key: String,
        }

        match serde_json::from_str::<SessionCredentials>(credentials_str) {
            Ok(credentials) => {
                let mut user = User::of_with_type(credentials.access_key, credentials.secret_key, UserType::Super);
                user.set_user_status(UserStatus::Enable);
                self.create_user_if_not_exists(user, provider).await?;
            }
            Err(e) => {
                warn!("Failed to parse inner_client_authentication_credentials: {}", e);
            }
        }

        Ok(())
    }

    /// Create user if it doesn't exist
    async fn create_user_if_not_exists(&self, user: User, provider: &A) -> ManagerResult<()> {
        match provider.get_user(user.username().as_str()).await {
            Ok(_existing) => {
                info!("User '{}' already exists, skipping creation", user.username());
                Ok(())
            }
            Err(_) => {
                provider.create_user(user.clone()).await?;
                info!("Created super user '{}'", user.username());
                Ok(())
            }
        }
    }
}

#[allow(async_fn_in_trait)]
impl<A, B> AuthenticationMetadataManager for AuthenticationMetadataManagerImpl<A, B>
where
    A: AuthenticationMetadataProvider,
    B: AuthorizationMetadataProvider,
{
    async fn shutdown(&mut self) -> ManagerResult<()> {
        info!("Shutting down authentication metadata manager");

        // Shutdown authentication provider
        let mut auth_guard = self.authentication_metadata_provider.write().await;
        if let Some(mut provider) = auth_guard.take() {
            let _ = provider.shutdown().await; // Ignore shutdown errors in cleanup
        }

        // Shutdown authorization provider
        let mut authz_guard = self.authorization_metadata_provider.write().await;
        if let Some(mut provider) = authz_guard.take() {
            provider.shutdown(); // AuthorizationMetadataProvider::shutdown is synchronous
        }

        info!("Authentication metadata manager shutdown complete");
        Ok(())
    }

    async fn init_user(&self, auth_config: &AuthConfig) -> ManagerResult<()> {
        info!("Initializing default super user from configuration");

        let guard = self.authentication_metadata_provider.read().await;
        let provider = match &*guard {
            Some(p) => p,
            None => {
                return Err(RocketMQError::not_initialized(
                    "Authentication metadata provider is not configured",
                ));
            }
        };

        // 1. Handle init_authentication_user (JSON format or simple "username:password")
        let init_user_str = auth_config.init_authentication_user.as_str();
        if !init_user_str.is_empty() {
            self.init_user_from_string(init_user_str, provider).await?;
        }

        // 2. Handle inner_client_authentication_credentials (SessionCredentials format)
        let inner_credentials = auth_config.inner_client_authentication_credentials.as_str();
        if !inner_credentials.is_empty() {
            self.init_inner_client_user(inner_credentials, provider).await?;
        }

        Ok(())
    }

    async fn create_user(&self, user: User) -> ManagerResult<()> {
        // Validate user input
        self.validate_user(&user, true)?;

        let guard = self.authentication_metadata_provider.read().await;
        let provider = match &*guard {
            Some(p) => p,
            None => {
                return Err(RocketMQError::not_initialized(
                    "Authentication metadata provider is not configured",
                ));
            }
        };

        // Check if user already exists
        if let Ok(_existing) = provider.get_user(user.username().as_str()).await {
            return Err(RocketMQError::authentication_failed("The user already exists"));
        }

        // Set default values if not provided
        let mut user_to_create = user.clone();
        if user_to_create.user_type().is_none() {
            user_to_create.set_user_type(UserType::Normal);
        }
        if user_to_create.user_status().is_none() {
            user_to_create.set_user_status(UserStatus::Enable);
        }

        provider
            .create_user(user_to_create)
            .await
            .map_err(|e| self.handle_exception(e))?;

        info!("User '{}' created successfully", user.username());
        Ok(())
    }

    async fn update_user(&self, user: User) -> ManagerResult<()> {
        // Validate user input (password not required for update)
        self.validate_user(&user, false)?;

        let guard = self.authentication_metadata_provider.read().await;
        let provider = match &*guard {
            Some(p) => p,
            None => {
                return Err(RocketMQError::not_initialized(
                    "Authentication metadata provider is not configured",
                ));
            }
        };

        // Get existing user first
        let existing_user = provider
            .get_user(user.username().as_str())
            .await
            .map_err(|_| RocketMQError::authentication_failed("The user does not exist"))?;

        // Merge fields: update only provided fields
        let mut updated_user = existing_user.clone();

        // Update password if provided
        if let Some(new_password) = user.password() {
            updated_user.set_password(new_password.clone());
        }

        // Update user type if provided
        if let Some(new_type) = user.user_type() {
            updated_user.set_user_type(new_type);
        }

        // Update user status if provided
        if let Some(new_status) = user.user_status() {
            updated_user.set_user_status(new_status);
        }

        provider
            .update_user(updated_user)
            .await
            .map_err(|e| self.handle_exception(e))?;

        info!("User '{}' updated successfully", user.username());
        Ok(())
    }

    async fn delete_user(&self, username: &str) -> ManagerResult<()> {
        if username.is_empty() {
            return Err(RocketMQError::authentication_failed("username can not be blank"));
        }

        let auth_guard = self.authentication_metadata_provider.read().await;
        let auth_provider = match &*auth_guard {
            Some(p) => p,
            None => {
                return Err(RocketMQError::not_initialized(
                    "Authentication metadata provider is not configured",
                ));
            }
        };

        // Delete user from authentication provider
        let delete_user_result = auth_provider
            .delete_user(username)
            .await
            .map_err(|e| self.handle_exception(e));

        // Also delete related ACLs from authorization provider
        let authz_guard = self.authorization_metadata_provider.read().await;
        if let Some(authz_provider) = &*authz_guard {
            let user = User::of(username);
            let _ = authz_provider.delete_acl(&user).await; // Ignore ACL deletion errors
        }

        delete_user_result?;
        info!("User '{}' and related ACLs deleted successfully", username);
        Ok(())
    }

    async fn get_user(&self, username: &str) -> ManagerResult<User> {
        if username.is_empty() {
            return Err(RocketMQError::authentication_failed("username can not be blank"));
        }

        let guard = self.authentication_metadata_provider.read().await;
        let provider = match &*guard {
            Some(p) => p,
            None => {
                return Err(RocketMQError::not_initialized(
                    "Authentication metadata provider is not configured",
                ));
            }
        };

        provider.get_user(username).await.map_err(|e| self.handle_exception(e))
    }

    async fn list_user(&self, filter: Option<&str>) -> ManagerResult<Vec<User>> {
        let guard = self.authentication_metadata_provider.read().await;
        let provider = match &*guard {
            Some(p) => p,
            None => {
                return Err(RocketMQError::not_initialized(
                    "Authentication metadata provider is not configured",
                ));
            }
        };

        provider.list_user(filter).await.map_err(|e| self.handle_exception(e))
    }

    async fn is_super_user(&self, username: &str) -> ManagerResult<bool> {
        if username.is_empty() {
            return Ok(false);
        }

        match self.get_user(username).await {
            Ok(user) => Ok(matches!(user.user_type(), Some(UserType::Super))),
            Err(_) => Ok(false), // User not found = not super user
        }
    }
}

impl<A, B> Default for AuthenticationMetadataManagerImpl<A, B>
where
    A: AuthenticationMetadataProvider,
    B: AuthorizationMetadataProvider,
{
    fn default() -> Self {
        Self::new(None, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authentication::provider::local_authentication_metadata_provider::LocalAuthenticationMetadataProvider;
    use crate::authorization::metadata_provider::local::LocalAuthorizationMetadataProvider;

    #[tokio::test]
    async fn test_manager_creation() {
        let manager = AuthenticationMetadataManagerImpl::<
            LocalAuthenticationMetadataProvider,
            LocalAuthorizationMetadataProvider,
        >::default();

        // Manager should be created successfully
        assert!(manager.authentication_metadata_provider.read().await.is_none());
    }

    #[tokio::test]
    async fn test_validate_user_creation() {
        let manager = AuthenticationMetadataManagerImpl::<
            LocalAuthenticationMetadataProvider,
            LocalAuthorizationMetadataProvider,
        >::default();

        // Valid user
        let user = User::of_with_type("test_user", "password", UserType::Normal);
        assert!(manager.validate_user(&user, true).is_ok());

        // Invalid: empty username
        let user = User::of_with_type("", "password", UserType::Normal);
        assert!(manager.validate_user(&user, true).is_err());

        // Invalid: missing password for creation
        let mut user = User::of("test_user");
        user.set_user_type(UserType::Normal);
        assert!(manager.validate_user(&user, true).is_err());
    }

    #[tokio::test]
    async fn test_is_super_user_with_empty_username() {
        let manager = AuthenticationMetadataManagerImpl::<
            LocalAuthenticationMetadataProvider,
            LocalAuthorizationMetadataProvider,
        >::default();

        let result = manager.is_super_user("").await.unwrap();
        assert!(!result);
    }
}
