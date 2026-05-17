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
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_error::SerializationError;
use tokio::fs;

use crate::authentication::model::user::User;
use crate::config::AuthConfig;

use super::authentication_metadata_provider::AuthenticationMetadataProvider;

/// Local authentication metadata provider backed by an in-memory snapshot and an
/// optional JSON snapshot file.
pub struct LocalAuthenticationMetadataProvider {
    storage: Arc<tokio::sync::RwLock<HashMap<String, User>>>,
    storage_path: Option<PathBuf>,
    write_lock: Arc<tokio::sync::Mutex<()>>,
}

impl LocalAuthenticationMetadataProvider {
    /// Create a new local authentication metadata provider.
    pub fn new() -> Self {
        Self {
            storage: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            storage_path: None,
            write_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    pub fn with_config(config: &AuthConfig) -> RocketMQResult<Self> {
        let storage_path = auth_metadata_snapshot_path(config, "users.json");
        let users = match &storage_path {
            Some(path) => read_users_snapshot_blocking(path)?,
            None => HashMap::new(),
        };
        Ok(Self {
            storage: Arc::new(tokio::sync::RwLock::new(users)),
            storage_path,
            write_lock: Arc::new(tokio::sync::Mutex::new(())),
        })
    }

    async fn persist_users(&self, users: &HashMap<String, User>) -> RocketMQResult<()> {
        let Some(path) = &self.storage_path else {
            return Ok(());
        };
        write_users_snapshot(path, users).await
    }

    async fn mutate_users<F>(&self, mutation: F) -> RocketMQResult<()>
    where
        F: FnOnce(&mut HashMap<String, User>) -> RocketMQResult<()>,
    {
        let _write_guard = self.write_lock.lock().await;
        let mut snapshot = {
            let storage = self.storage.read().await;
            storage.clone()
        };
        mutation(&mut snapshot)?;
        self.persist_users(&snapshot).await?;
        let mut storage = self.storage.write().await;
        *storage = snapshot;
        Ok(())
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
            let storage_path = auth_metadata_snapshot_path(&config, "users.json");
            let users = if let Some(path) = storage_path.clone() {
                let users = read_users_snapshot(&path).await?;
                tracing::info!("LocalAuthenticationMetadataProvider initialized at {:?}", path);
                users
            } else {
                tracing::info!("LocalAuthenticationMetadataProvider initialized in memory-only mode");
                HashMap::new()
            };
            let mut storage = self.storage.write().await;
            *storage = users;
            self.storage_path = storage_path;
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
            self.mutate_users(|storage| {
                if storage.contains_key(&username) {
                    return Err(RocketMQError::authentication_failed(format!(
                        "The user already exists: {username}"
                    )));
                }
                storage.insert(username.clone(), user);
                Ok(())
            })
            .await?;

            tracing::info!("User '{}' created successfully", username);
            Ok(())
        })
    }

    /// Delete a user.
    fn delete_user<'a>(&'a self, username: &'a str) -> Pin<Box<dyn Future<Output = RocketMQResult<()>> + Send + 'a>> {
        Box::pin(async move {
            self.mutate_users(|storage| {
                storage.remove(username);
                Ok(())
            })
            .await?;

            tracing::info!("User '{}' deleted successfully", username);
            Ok(())
        })
    }

    /// Update a user.
    fn update_user<'a>(&'a self, user: User) -> Pin<Box<dyn Future<Output = RocketMQResult<()>> + Send + 'a>> {
        Box::pin(async move {
            let username = user.username().to_string();
            self.mutate_users(|storage| {
                storage.insert(username.clone(), user);
                Ok(())
            })
            .await?;

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

fn auth_metadata_snapshot_path(config: &AuthConfig, file_name: &str) -> Option<PathBuf> {
    let raw_path = config.auth_config_path.as_str().trim();
    if raw_path.is_empty() {
        return None;
    }
    let path = PathBuf::from(raw_path);
    let root = if path.extension().is_some() {
        path.with_extension("")
    } else {
        path
    };
    Some(root.join(file_name))
}

async fn read_users_snapshot(path: &Path) -> RocketMQResult<HashMap<String, User>> {
    let bytes = match fs::read(path).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(HashMap::new()),
        Err(error) => {
            return Err(RocketMQError::storage_read_failed(
                path.display().to_string(),
                error.to_string(),
            ))
        }
    };
    decode_users_snapshot(path, &bytes)
}

fn read_users_snapshot_blocking(path: &Path) -> RocketMQResult<HashMap<String, User>> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(HashMap::new()),
        Err(error) => {
            return Err(RocketMQError::storage_read_failed(
                path.display().to_string(),
                error.to_string(),
            ))
        }
    };
    decode_users_snapshot(path, &bytes)
}

fn decode_users_snapshot(path: &Path, bytes: &[u8]) -> RocketMQResult<HashMap<String, User>> {
    if bytes.iter().all(u8::is_ascii_whitespace) {
        return Ok(HashMap::new());
    }
    serde_json::from_slice(bytes)
        .map_err(|error| RocketMQError::deserialization_failed("JSON", format!("{}: {error}", path.display())))
}

async fn write_users_snapshot(path: &Path, users: &HashMap<String, User>) -> RocketMQResult<()> {
    let content = serde_json::to_vec_pretty(users)
        .map_err(|error| RocketMQError::Serialization(SerializationError::encode_failed("JSON", error.to_string())))?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .await
            .map_err(|error| RocketMQError::storage_write_failed(parent.display().to_string(), error.to_string()))?;
    }

    let temp_file = temp_snapshot_path(path);
    fs::write(&temp_file, content)
        .await
        .map_err(|error| RocketMQError::storage_write_failed(temp_file.display().to_string(), error.to_string()))?;

    match fs::rename(&temp_file, path).await {
        Ok(()) => Ok(()),
        Err(rename_error) => {
            fs::copy(&temp_file, path).await.map_err(|error| {
                RocketMQError::storage_write_failed(
                    path.display().to_string(),
                    format!("{error}; rename failed first: {rename_error}"),
                )
            })?;
            let _ = fs::remove_file(&temp_file).await;
            Ok(())
        }
    }
}

fn temp_snapshot_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("users.json");
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    path.with_file_name(format!(".{file_name}.{nanos}.tmp"))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use cheetah_string::CheetahString;
    use tempfile::TempDir;

    use super::*;
    use crate::authentication::enums::user_status::UserStatus;

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
    async fn create_user_rejects_duplicate_without_overwriting_existing_secret() {
        let mut provider = LocalAuthenticationMetadataProvider::new();
        provider.initialize(AuthConfig::default(), None).await.unwrap();

        provider
            .create_user(User::of_with_password("duplicate", "first"))
            .await
            .unwrap();

        let error = provider
            .create_user(User::of_with_password("duplicate", "second"))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("already exists"));

        let user = provider.get_user("duplicate").await.unwrap();
        assert_eq!(user.password().map(|value| value.as_str()), Some("first"));
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

    #[tokio::test]
    async fn local_provider_persists_users_across_reinitialization() {
        let temp = TempDir::new().unwrap();
        let config = AuthConfig {
            auth_config_path: CheetahString::from_string(temp.path().join("auth.json").to_string_lossy().into_owned()),
            ..AuthConfig::default()
        };

        let mut provider = LocalAuthenticationMetadataProvider::new();
        provider.initialize(config.clone(), None).await.unwrap();
        let mut user = User::of_with_password("persisted", "secret");
        user.set_user_status(UserStatus::Enable);
        provider.create_user(user).await.unwrap();

        let mut restarted = LocalAuthenticationMetadataProvider::new();
        restarted.initialize(config, None).await.unwrap();
        let restored = restarted.get_user("persisted").await.unwrap();

        assert_eq!(restored.username().as_str(), "persisted");
        assert_eq!(restored.password().map(|value| value.as_str()), Some("secret"));
        assert_eq!(restored.user_status(), Some(UserStatus::Enable));
    }

    #[tokio::test]
    async fn local_provider_persists_user_update_and_delete_across_reinitialization() {
        let temp = TempDir::new().unwrap();
        let config = AuthConfig {
            auth_config_path: CheetahString::from_string(temp.path().join("auth.json").to_string_lossy().into_owned()),
            ..AuthConfig::default()
        };

        let mut provider = LocalAuthenticationMetadataProvider::new();
        provider.initialize(config.clone(), None).await.unwrap();
        provider
            .create_user(User::of_with_password("persisted", "first"))
            .await
            .unwrap();
        provider
            .update_user(User::of_with_password("persisted", "second"))
            .await
            .unwrap();

        let mut restarted = LocalAuthenticationMetadataProvider::new();
        restarted.initialize(config.clone(), None).await.unwrap();
        let restored = restarted.get_user("persisted").await.unwrap();
        assert_eq!(restored.password().map(|value| value.as_str()), Some("second"));

        restarted.delete_user("persisted").await.unwrap();

        let mut deleted_restart = LocalAuthenticationMetadataProvider::new();
        deleted_restart.initialize(config, None).await.unwrap();
        assert!(deleted_restart.get_user("persisted").await.is_err());
    }

    #[tokio::test]
    async fn local_provider_rejects_corrupted_user_snapshot() {
        let temp = TempDir::new().unwrap();
        let config = AuthConfig {
            auth_config_path: CheetahString::from_string(temp.path().join("auth.json").to_string_lossy().into_owned()),
            ..AuthConfig::default()
        };
        let snapshot = temp.path().join("auth").join("users.json");
        fs::create_dir_all(snapshot.parent().unwrap()).unwrap();
        fs::write(&snapshot, b"{not valid json").unwrap();

        let mut provider = LocalAuthenticationMetadataProvider::new();
        let error = provider.initialize(config, None).await.unwrap_err();

        assert!(error.to_string().contains("users.json"));
    }

    #[tokio::test]
    async fn initialize_without_storage_path_clears_previous_snapshot() {
        let temp = TempDir::new().unwrap();
        let config = AuthConfig {
            auth_config_path: CheetahString::from_string(temp.path().join("auth.json").to_string_lossy().into_owned()),
            ..AuthConfig::default()
        };

        let mut provider = LocalAuthenticationMetadataProvider::new();
        provider.initialize(config, None).await.unwrap();
        provider
            .create_user(User::of_with_password("stale", "secret"))
            .await
            .unwrap();

        provider.initialize(AuthConfig::default(), None).await.unwrap();

        assert!(provider.get_user("stale").await.is_err());
    }
}
