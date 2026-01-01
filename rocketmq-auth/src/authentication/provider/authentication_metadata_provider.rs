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

//! Authentication Metadata Provider Trait (Rust 2021 Standard)

use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use rocketmq_error::RocketMQResult;

use crate::authentication::model::user::User;
use crate::config::AuthConfig;

/// Authentication metadata provider for user management.
pub trait AuthenticationMetadataProvider: Send + Sync {
    /// Initialize the provider.
    fn initialize<'a>(
        &'a mut self,
        config: AuthConfig,
        metadata_service: Option<Arc<dyn Any + Send + Sync>>,
    ) -> Pin<Box<dyn Future<Output = RocketMQResult<()>> + Send + 'a>>;

    /// Shutdown the provider.
    fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = RocketMQResult<()>> + Send + '_>>;

    /// Create a user.
    fn create_user<'a>(&'a self, user: User) -> Pin<Box<dyn Future<Output = RocketMQResult<()>> + Send + 'a>>;

    /// Delete a user.
    fn delete_user<'a>(&'a self, username: &'a str) -> Pin<Box<dyn Future<Output = RocketMQResult<()>> + Send + 'a>>;

    /// Update a user.
    fn update_user<'a>(&'a self, user: User) -> Pin<Box<dyn Future<Output = RocketMQResult<()>> + Send + 'a>>;

    /// Get a user by username.
    fn get_user<'a>(&'a self, username: &'a str) -> Pin<Box<dyn Future<Output = RocketMQResult<User>> + Send + 'a>>;

    /// List users with optional filter.
    fn list_user<'a>(
        &'a self,
        filter: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = RocketMQResult<Vec<User>>> + Send + 'a>>;
}
