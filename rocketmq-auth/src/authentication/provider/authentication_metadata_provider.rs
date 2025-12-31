//  Licensed to the Apache Software Foundation (ASF) under one
//  or more contributor license agreements.  See the NOTICE file
//  distributed with this work for additional information
//  regarding copyright ownership.  The ASF licenses this file
//  to you under the Apache License, Version 2.0 (the
//  "License"); you may not use this file except in compliance
//  with the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an
//  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  KIND, either express or implied.  See the License for the
//  specific language governing permissions and limitations
//  under the License.

//! Authentication Metadata Provider Trait (Rust 2021 Standard)

use std::any::Any;
use std::sync::Arc;

use rocketmq_error::RocketMQResult;

use crate::authentication::model::user::User;
use crate::config::AuthConfig;

/// Authentication metadata provider for user management.
///
/// Maps to Java interface:
/// ```java
/// public interface AuthenticationMetadataProvider
/// ```
#[allow(async_fn_in_trait)]
pub trait AuthenticationMetadataProvider: Send + Sync {
    /// Initialize the provider.
    async fn initialize(
        &mut self,
        config: AuthConfig,
        metadata_service: Option<Arc<dyn Any + Send + Sync>>,
    ) -> RocketMQResult<()>;

    /// Shutdown the provider.
    async fn shutdown(&mut self) -> RocketMQResult<()>;

    /// Create a user.
    async fn create_user(&self, user: User) -> RocketMQResult<()>;

    /// Delete a user.
    async fn delete_user(&self, username: &str) -> RocketMQResult<()>;

    /// Update a user.
    async fn update_user(&self, user: User) -> RocketMQResult<()>;

    /// Get a user by username.
    async fn get_user(&self, username: &str) -> RocketMQResult<User>;

    /// List users with optional filter.
    async fn list_user(&self, filter: Option<&str>) -> RocketMQResult<Vec<User>>;
}
