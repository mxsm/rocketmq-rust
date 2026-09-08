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

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::authentication::enums::subject_type::SubjectType;
use crate::authentication::model::subject::Subject;
use crate::authentication::model::user::User;
use crate::authentication::provider::AuthenticationMetadataProvider;
use crate::authorization::metadata_provider::AuthorizationMetadataProvider;
use crate::authorization::metadata_provider::LocalAuthorizationMetadataProvider;
use crate::authorization::model::acl::Acl;
use crate::AuthServiceResult;

pub type ProviderFuture<'a, T> = Pin<Box<dyn Future<Output = AuthServiceResult<T>> + Send + 'a>>;

/// Owned subject identity at the dynamic metadata boundary.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct SubjectKey {
    subject_type: SubjectType,
    key: String,
}

impl SubjectKey {
    pub fn new(subject_type: SubjectType, key: impl Into<String>) -> Self {
        Self {
            subject_type,
            key: key.into(),
        }
    }

    pub fn from_subject(subject: &(impl Subject + ?Sized)) -> Self {
        Self::new(subject.subject_type(), subject.subject_key())
    }
}

impl Subject for SubjectKey {
    fn subject_key(&self) -> &str {
        &self.key
    }
    fn subject_type(&self) -> SubjectType {
        self.subject_type
    }
}

/// User lookup capability without mutation or provider lifecycle access.
pub trait UserMetadataRead: Send + Sync {
    fn lookup_user<'a>(&'a self, username: &'a str) -> ProviderFuture<'a, User>;
}

impl<T: AuthenticationMetadataProvider + ?Sized> UserMetadataRead for T {
    fn lookup_user<'a>(&'a self, username: &'a str) -> ProviderFuture<'a, User> {
        AuthenticationMetadataProvider::get_user(self, username)
    }
}

pub(crate) struct LegacyUserReader(pub(crate) Arc<dyn AuthenticationMetadataProvider>);

impl UserMetadataRead for LegacyUserReader {
    fn lookup_user<'a>(&'a self, username: &'a str) -> ProviderFuture<'a, User> {
        self.0.get_user(username)
    }
}

/// Concurrent user metadata operations owned by an Auth runtime.
pub trait UserMetadataPort: UserMetadataRead {
    fn create_user(&self, user: User) -> ProviderFuture<'_, ()>;
    fn update_user(&self, user: User) -> ProviderFuture<'_, ()>;
    fn delete_user<'a>(&'a self, username: &'a str) -> ProviderFuture<'a, ()>;
    fn list_user<'a>(&'a self, filter: Option<&'a str>) -> ProviderFuture<'a, Vec<User>>;
}

impl<T: AuthenticationMetadataProvider + ?Sized> UserMetadataPort for T {
    fn create_user(&self, user: User) -> ProviderFuture<'_, ()> {
        AuthenticationMetadataProvider::create_user(self, user)
    }

    fn update_user(&self, user: User) -> ProviderFuture<'_, ()> {
        AuthenticationMetadataProvider::update_user(self, user)
    }

    fn delete_user<'a>(&'a self, username: &'a str) -> ProviderFuture<'a, ()> {
        AuthenticationMetadataProvider::delete_user(self, username)
    }

    fn list_user<'a>(&'a self, filter: Option<&'a str>) -> ProviderFuture<'a, Vec<User>> {
        AuthenticationMetadataProvider::list_user(self, filter)
    }
}

/// ACL lookup capability without mutation or provider lifecycle access.
pub trait AclMetadataRead: Send + Sync {
    fn lookup_acl<'a>(&'a self, subject: &'a SubjectKey) -> ProviderFuture<'a, Option<Acl>>;
}

impl<T: AuthorizationMetadataProvider + ?Sized> AclMetadataRead for T {
    fn lookup_acl<'a>(&'a self, subject: &'a SubjectKey) -> ProviderFuture<'a, Option<Acl>> {
        Box::pin(AuthorizationMetadataProvider::get_acl(self, subject))
    }
}

/// Concurrent ACL metadata operations, independent of lifecycle control.
pub trait AclMetadataPort: AclMetadataRead {
    fn create_acl(&self, acl: Acl) -> ProviderFuture<'_, ()>;
    fn update_acl(&self, acl: Acl) -> ProviderFuture<'_, ()>;
    fn delete_acl<'a>(&'a self, subject: &'a SubjectKey) -> ProviderFuture<'a, ()>;
    fn list_acl<'a>(
        &'a self,
        subject_filter: Option<&'a str>,
        resource_filter: Option<&'a str>,
    ) -> ProviderFuture<'a, Vec<Acl>>;
}

impl AclMetadataPort for LocalAuthorizationMetadataProvider {
    fn create_acl(&self, acl: Acl) -> ProviderFuture<'_, ()> {
        Box::pin(AuthorizationMetadataProvider::create_acl(self, acl))
    }

    fn update_acl(&self, acl: Acl) -> ProviderFuture<'_, ()> {
        Box::pin(AuthorizationMetadataProvider::update_acl(self, acl))
    }

    fn delete_acl<'a>(&'a self, subject: &'a SubjectKey) -> ProviderFuture<'a, ()> {
        Box::pin(AuthorizationMetadataProvider::delete_acl(self, subject))
    }

    fn list_acl<'a>(
        &'a self,
        subject_filter: Option<&'a str>,
        resource_filter: Option<&'a str>,
    ) -> ProviderFuture<'a, Vec<Acl>> {
        Box::pin(AuthorizationMetadataProvider::list_acl(
            self,
            subject_filter,
            resource_filter,
        ))
    }
}
