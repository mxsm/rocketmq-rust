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

//! Authentication metadata manager trait.
//!
//! This module defines the high-level interface for managing authentication metadata,
//! including user creation, modification, deletion, and query operations.

use rocketmq_error::RocketMQResult;

use crate::authentication::model::user::User;
use crate::config::AuthConfig;

/// Type alias for manager operation results.
///
/// Following the same pattern as `AuthorizationMetadataManager::ManagerResult`.
pub type ManagerResult<T> = RocketMQResult<T>;

/// Authentication metadata manager trait.
///
/// This trait provides high-level operations for managing authentication metadata,
/// wrapping the underlying `AuthenticationMetadataProvider` and adding validation,
/// initialization, and error handling logic.
///
/// # Responsibilities
///
/// - User lifecycle management (create, update, delete)
/// - User query and listing
/// - Super user privilege checking
/// - Provider initialization and shutdown
/// - Input validation and error handling
///
/// # Concurrency
///
/// All methods are async and designed for concurrent access. Implementations should
/// ensure thread-safety through appropriate synchronization mechanisms.
///
/// # Example
///
/// ```rust,ignore
/// use rocketmq_auth::authentication::manager::AuthenticationMetadataManager;
/// use rocketmq_auth::config::AuthConfig;
///
/// async fn example(manager: &dyn AuthenticationMetadataManager, config: AuthConfig) {
///     // Initialize default users
///     manager.init_user(config).await?;
///     
///     // Create a user
///     let user = User::of_with_type("alice", "password123", UserType::Normal);
///     manager.create_user(user).await?;
///     
///     // Check if user is super user
///     let is_super = manager.is_super_user("alice").await?;
/// }
/// ```
#[allow(async_fn_in_trait)]
pub trait AuthenticationMetadataManager: Send + Sync {
    /// Shutdown the metadata manager and release resources.
    ///
    /// This method should cleanup all resources including:
    /// - Closing database connections
    /// - Shutting down thread pools
    /// - Flushing pending operations
    async fn shutdown(&mut self) -> ManagerResult<()>;

    /// Initialize default super user from configuration.
    ///
    /// This method reads the default user credentials from `AuthConfig` and
    /// creates the super user if it doesn't exist. This is typically called
    /// during system initialization.
    ///
    /// # Arguments
    ///
    /// * `auth_config` - Authentication configuration containing default user settings
    async fn init_user(&self, auth_config: &AuthConfig) -> ManagerResult<()>;

    /// Create a new user.
    ///
    /// # Arguments
    ///
    /// * `user` - The user to create
    ///
    /// # Returns
    ///
    /// * `Ok(())` - User created successfully
    /// * `Err(RocketMQError)` - Validation failed or creation error
    ///
    /// # Errors
    ///
    /// - `AuthenticationFailed` - Username is blank or invalid
    /// - `InvalidCredential` - Password is missing or invalid
    /// - Storage errors from underlying provider
    async fn create_user(&self, user: User) -> ManagerResult<()>;

    /// Update an existing user.
    ///
    /// # Arguments
    ///
    /// * `user` - The user with updated information
    ///
    /// # Returns
    ///
    /// * `Ok(())` - User updated successfully
    /// * `Err(RocketMQError)` - Validation failed or update error
    async fn update_user(&self, user: User) -> ManagerResult<()>;

    /// Delete a user by username.
    ///
    /// # Arguments
    ///
    /// * `username` - The username of the user to delete
    ///
    /// # Returns
    ///
    /// * `Ok(())` - User deleted successfully
    /// * `Err(RocketMQError)` - Username is blank or deletion error
    async fn delete_user(&self, username: &str) -> ManagerResult<()>;

    /// Get a user by username.
    ///
    /// # Arguments
    ///
    /// * `username` - The username to query
    ///
    /// # Returns
    ///
    /// * `Ok(User)` - User found
    /// * `Err(RocketMQError)` - Username is blank or user not found
    async fn get_user(&self, username: &str) -> ManagerResult<User>;

    /// List users with optional filtering.
    ///
    /// # Arguments
    ///
    /// * `filter` - Optional username filter (substring match)
    ///
    /// # Returns
    ///
    /// * `Ok(Vec<User>)` - List of matching users
    /// * `Err(RocketMQError)` - Query error
    async fn list_user(&self, filter: Option<&str>) -> ManagerResult<Vec<User>>;

    /// Check if a user is a super user.
    ///
    /// # Arguments
    ///
    /// * `username` - The username to check
    ///
    /// # Returns
    ///
    /// * `Ok(true)` - User exists and is a super user
    /// * `Ok(false)` - User doesn't exist or is not a super user
    /// * `Err(RocketMQError)` - Query error
    async fn is_super_user(&self, username: &str) -> ManagerResult<bool>;
}
