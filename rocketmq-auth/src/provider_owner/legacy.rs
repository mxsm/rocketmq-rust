// Copyright 2026 The RocketMQ Rust Authors
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

use std::any::Any;

use super::*;
use crate::authentication::provider::AuthenticationMetadataProvider;
use crate::authorization::metadata_provider::AuthorizationMetadataProvider;
use crate::ProviderRegistry;

// Compatibility adapters borrow runtime data handles. Legacy shutdown releases
// only the adapter; provider finalization remains with the shared runtime owner.
pub(crate) struct RegistryUserMetadata(Option<Arc<UserMetadataHandle>>);
pub(crate) struct RegistryAclMetadata(Option<Arc<AclMetadataHandle>>);

impl RegistryUserMetadata {
    pub(crate) fn new(registry: &ProviderRegistry) -> Self {
        Self(Some(registry.authentication_metadata_provider()))
    }

    fn handle(&self) -> AuthServiceResult<&UserMetadataHandle> {
        self.0.as_deref().ok_or_else(unavailable)
    }
}

impl RegistryAclMetadata {
    pub(crate) fn new(registry: &ProviderRegistry) -> Self {
        Self(Some(registry.authorization_metadata_provider()))
    }

    fn handle(&self) -> AuthServiceResult<&AclMetadataHandle> {
        self.0.as_deref().ok_or_else(unavailable)
    }
}

fn unavailable() -> AuthServiceError {
    AuthServiceError::new(AuthOperation::ManageMetadata, AuthFailureKind::Unavailable)
}

impl AuthenticationMetadataProvider for RegistryUserMetadata {
    fn initialize<'a>(&'a mut self, _: AuthConfig, _: Option<Arc<dyn Any + Send + Sync>>) -> ProviderFuture<'a, ()> {
        Box::pin(async {
            Err(AuthServiceError::new(
                AuthOperation::InitializeProvider,
                AuthFailureKind::Unsupported,
            ))
        })
    }

    fn shutdown(&mut self) -> ProviderFuture<'_, ()> {
        Box::pin(async {
            self.0 = None;
            Ok(())
        })
    }

    fn create_user<'a>(&'a self, user: User) -> ProviderFuture<'a, ()> {
        Box::pin(async move { self.handle()?.create_user(user).await })
    }

    fn update_user<'a>(&'a self, user: User) -> ProviderFuture<'a, ()> {
        Box::pin(async move { self.handle()?.update_user(user).await })
    }

    fn get_user<'a>(&'a self, username: &'a str) -> ProviderFuture<'a, User> {
        Box::pin(async move { self.handle()?.get_user(username).await })
    }

    fn delete_user<'a>(&'a self, username: &'a str) -> ProviderFuture<'a, ()> {
        Box::pin(async move { self.handle()?.delete_user(username).await })
    }

    fn list_user<'a>(&'a self, filter: Option<&'a str>) -> ProviderFuture<'a, Vec<User>> {
        Box::pin(async move { self.handle()?.list_user(filter).await })
    }
}

impl AuthorizationMetadataProvider for RegistryAclMetadata {
    fn initialize(&mut self, _: AuthConfig, _: Option<Box<dyn Any + Send + Sync>>) -> AuthServiceResult<()> {
        Err(AuthServiceError::new(
            AuthOperation::InitializeProvider,
            AuthFailureKind::Unsupported,
        ))
    }

    fn shutdown(&mut self) {
        self.0 = None;
    }

    async fn create_acl(&self, acl: Acl) -> AuthServiceResult<()> {
        self.handle()?.create_acl(acl).await
    }

    async fn update_acl(&self, acl: Acl) -> AuthServiceResult<()> {
        self.handle()?.update_acl(acl).await
    }

    async fn get_acl<S: Subject + Send + Sync>(&self, subject: &S) -> AuthServiceResult<Option<Acl>> {
        self.handle()?.get_acl(subject).await
    }

    async fn delete_acl<S: Subject + Send + Sync>(&self, subject: &S) -> AuthServiceResult<()> {
        self.handle()?.delete_acl(subject).await
    }

    async fn list_acl(
        &self,
        subject_filter: Option<&str>,
        resource_filter: Option<&str>,
    ) -> AuthServiceResult<Vec<Acl>> {
        self.handle()?.list_acl(subject_filter, resource_filter).await
    }
}
