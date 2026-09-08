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

use super::*;
use crate::authentication::provider::AuthenticationMetadataProvider;
use crate::authentication::provider::LocalAuthenticationMetadataProvider;
use crate::authorization::metadata_provider::AuthorizationMetadataProvider;
use crate::authorization::metadata_provider::LocalAuthorizationMetadataProvider;

struct InitializedLocalProviders {
    users: Arc<LocalAuthenticationMetadataProvider>,
    acls: Arc<LocalAuthorizationMetadataProvider>,
}

struct LocalUserControl(Arc<LocalAuthenticationMetadataProvider>);
struct LocalAclControl(Arc<LocalAuthorizationMetadataProvider>);

pub(crate) fn local_bundle(
    users: Arc<LocalAuthenticationMetadataProvider>,
    acls: Arc<LocalAuthorizationMetadataProvider>,
) -> ProviderBundle {
    let importer = Arc::new(InitializedLocalProviders {
        users: users.clone(),
        acls: acls.clone(),
    });
    let controls: Vec<Arc<dyn ProviderControl>> = vec![
        Arc::new(LocalUserControl(users.clone())),
        Arc::new(LocalAclControl(acls.clone())),
    ];
    ProviderBundle::new(users, acls, controls).with_snapshot_import(importer)
}

impl ProviderControl for LocalUserControl {
    fn initialize<'a>(&'a self, _: &'a AuthConfig, _: ChildServiceContext) -> ProviderFuture<'a, ()> {
        // Snapshot loading already completed on the caller's bounded I/O lane.
        Box::pin(async { Ok(()) })
    }

    fn flush(&self) -> ProviderFuture<'_, ()> {
        Box::pin(self.0.flush_shared())
    }

    fn close(&self) -> ProviderFuture<'_, ()> {
        Box::pin(self.0.close_shared())
    }
}

impl ProviderControl for LocalAclControl {
    fn initialize<'a>(&'a self, _: &'a AuthConfig, _: ChildServiceContext) -> ProviderFuture<'a, ()> {
        // Snapshot loading already completed on the caller's bounded I/O lane.
        Box::pin(async { Ok(()) })
    }

    fn flush(&self) -> ProviderFuture<'_, ()> {
        Box::pin(self.0.flush_shared())
    }

    fn close(&self) -> ProviderFuture<'_, ()> {
        Box::pin(self.0.close_shared())
    }
}

impl AclSnapshotImport for InitializedLocalProviders {
    fn import<'a>(&'a self, accounts: &'a [AclImportAccount], removed: &'a [SubjectKey]) -> ProviderFuture<'a, ()> {
        Box::pin(async move {
            for account in accounts {
                match AuthenticationMetadataProvider::get_user(self.users.as_ref(), account.user.username()).await {
                    Ok(_) => {
                        AuthenticationMetadataProvider::update_user(self.users.as_ref(), account.user.clone()).await?
                    }
                    Err(error) if error.kind() == AuthFailureKind::NotFound => {
                        AuthenticationMetadataProvider::create_user(self.users.as_ref(), account.user.clone()).await?
                    }
                    Err(error) => return Err(error),
                }
                match &account.acl {
                    Some(acl) => match AuthorizationMetadataProvider::get_acl(self.acls.as_ref(), &account.user).await?
                    {
                        Some(_) => AuthorizationMetadataProvider::update_acl(self.acls.as_ref(), acl.clone()).await?,
                        None => AuthorizationMetadataProvider::create_acl(self.acls.as_ref(), acl.clone()).await?,
                    },
                    None => AuthorizationMetadataProvider::delete_acl(self.acls.as_ref(), &account.user).await?,
                }
            }
            for subject in removed {
                AuthorizationMetadataProvider::delete_acl(self.acls.as_ref(), subject).await?;
                AuthenticationMetadataProvider::delete_user(
                    self.users.as_ref(),
                    User::username_from_subject_key(subject.subject_key()),
                )
                .await?;
            }
            Ok(())
        })
    }
}
