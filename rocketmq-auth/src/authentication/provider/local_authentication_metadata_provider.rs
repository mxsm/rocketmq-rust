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

//! Local Authentication Metadata Provider Implementation (Rust 2021 Standard)

use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;

use crate::authentication::model::user::User;
use crate::config::AuthConfig;

use super::authentication_metadata_provider::AuthenticationMetadataProvider;

/// Column family name for authentication metadata in RocksDB.
const AUTH_METADATA_COLUMN_FAMILY: &str = "default";

/// Empty user marker (sentinel value for cache).
const EMPTY_USER_MARKER: &str = "__EMPTY_USER__";

/// Local authentication metadata provider using in-memory storage.
///
/// Note: This is a simplified implementation using HashMap instead of RocksDB.
/// For production use, consider implementing RocksDB storage.
pub struct LocalAuthenticationMetadataProvider {
    /// In-memory user storage (simplified implementation).
    storage: Arc<tokio::sync::RwLock<HashMap<String, User>>>,

    /// Storage path (for future RocksDB implementation).
    storage_path: Option<PathBuf>,
}

impl LocalAuthenticationMetadataProvider {
    /// Create a new local authentication metadata provider.
    pub fn new() -> Self {
        Self {
            storage: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            storage_path: None,
        }
    }

    /// Get user from storage.
    async fn get_user_from_storage(&self, username: &str) -> RocketMQResult<Option<User>> {
        let storage = self.storage.read().await;
        Ok(storage.get(username).cloned())
    }
}

impl Default for LocalAuthenticationMetadataProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl AuthenticationMetadataProvider for LocalAuthenticationMetadataProvider {
    /// Initialize the provider with storage.
    fn initialize<'a>(
        &'a mut self,
        config: AuthConfig,
        _metadata_service: Option<Arc<dyn Any + Send + Sync>>,
    ) -> Pin<Box<dyn Future<Output = RocketMQResult<()>> + Send + 'a>> {
        Box::pin(async move {
            // Build storage path
            let mut storage_path = PathBuf::from(config.auth_config_path.to_string());
            storage_path.push("users");
            self.storage_path = Some(storage_path.clone());

            tracing::info!(
                "LocalAuthenticationMetadataProvider initialized (in-memory mode) at {:?}",
                storage_path
            );
            Ok(())
        })
    }

    /// Shutdown the provider and release resources.
    fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = RocketMQResult<()>> + Send + '_>> {
        Box::pin(async move {
            let mut storage = self.storage.write().await;
            storage.clear();
            tracing::info!("LocalAuthenticationMetadataProvider shutdown successfully");
            Ok(())
        })
    }

    /// Create a new user.
    fn create_user<'a>(&'a self, user: User) -> Pin<Box<dyn Future<Output = RocketMQResult<()>> + Send + 'a>> {
        Box::pin(async move {
            let username = user.username().to_string();
            let mut storage = self.storage.write().await;
            storage.insert(username.clone(), user);

            tracing::info!("User '{}' created successfully", username);
            Ok(())
        })
    }

    /// Delete a user.
    fn delete_user<'a>(&'a self, username: &'a str) -> Pin<Box<dyn Future<Output = RocketMQResult<()>> + Send + 'a>> {
        Box::pin(async move {
            let mut storage = self.storage.write().await;
            storage.remove(username);

            tracing::info!("User '{}' deleted successfully", username);
            Ok(())
        })
    }

    /// Update a user.
    fn update_user<'a>(&'a self, user: User) -> Pin<Box<dyn Future<Output = RocketMQResult<()>> + Send + 'a>> {
        Box::pin(async move {
            let username = user.username().to_string();
            let mut storage = self.storage.write().await;
            storage.insert(username.clone(), user);

            tracing::info!("User '{}' updated successfully", username);
            Ok(())
        })
    }

    /// Get a user by username.
    fn get_user<'a>(&'a self, username: &'a str) -> Pin<Box<dyn Future<Output = RocketMQResult<User>> + Send + 'a>> {
        Box::pin(async move {
            let storage = self.storage.read().await;

            storage
                .get(username)
                .cloned()
                .ok_or_else(|| RocketMQError::user_not_found(username))
        })
    }

    /// List all users, optionally filtered by username pattern.
    fn list_user<'a>(
        &'a self,
        filter: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = RocketMQResult<Vec<User>>> + Send + 'a>> {
        Box::pin(async move {
            let storage = self.storage.read().await;
            let mut result = Vec::new();

            for (username, user) in storage.iter() {
                // Apply filter if provided
                if let Some(filter_str) = filter {
                    if !filter_str.is_empty() && !username.contains(filter_str) {
                        continue;
                    }
                }

                result.push(user.clone());
            }

            tracing::debug!("Listed {} users (filter: {:?})", result.len(), filter);
            Ok(result)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_local_provider_initialization() {
        let config = AuthConfig::default();
        let mut provider = LocalAuthenticationMetadataProvider::new();

        assert!(provider.initialize(config, None).await.is_ok());
    }

    #[tokio::test]
    async fn test_create_and_get_user() {
        let mut provider = LocalAuthenticationMetadataProvider::new();
        provider.initialize(AuthConfig::default(), None).await.unwrap();

        let user = User::of("testuser");
        provider.create_user(user).await.unwrap();

        let retrieved = provider.get_user("testuser").await.unwrap();
        assert_eq!(retrieved.username().to_string(), "testuser");
    }

    #[tokio::test]
    async fn test_update_user() {
        let mut provider = LocalAuthenticationMetadataProvider::new();
        provider.initialize(AuthConfig::default(), None).await.unwrap();

        let user = User::of_with_password("testuser", "password");
        provider.create_user(user).await.unwrap();

        let updated_user = User::of_with_password("testuser", "newpassword");
        provider.update_user(updated_user).await.unwrap();

        let retrieved = provider.get_user("testuser").await.unwrap();
        assert_eq!(retrieved.username().to_string(), "testuser");
    }

    #[tokio::test]
    async fn test_delete_user() {
        let mut provider = LocalAuthenticationMetadataProvider::new();
        provider.initialize(AuthConfig::default(), None).await.unwrap();

        let user = User::of("testuser");
        provider.create_user(user).await.unwrap();

        provider.delete_user("testuser").await.unwrap();

        assert!(provider.get_user("testuser").await.is_err());
    }

    #[tokio::test]
    async fn test_list_users() {
        let mut provider = LocalAuthenticationMetadataProvider::new();
        provider.initialize(AuthConfig::default(), None).await.unwrap();

        provider.create_user(User::of("user1")).await.unwrap();
        provider.create_user(User::of("user2")).await.unwrap();
        provider.create_user(User::of("admin1")).await.unwrap();

        let all_users = provider.list_user(None).await.unwrap();
        assert_eq!(all_users.len(), 3);

        let filtered_users = provider.list_user(Some("user")).await.unwrap();
        assert_eq!(filtered_users.len(), 2);
    }
}
