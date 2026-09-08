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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::ShutdownDeadline;
use tokio::sync::Mutex;
use tokio::sync::Notify;

use crate::authentication::model::subject::Subject;
use crate::authentication::model::user::User;
use crate::authorization::model::acl::Acl;
use crate::AclMetadataPort;
use crate::AclMetadataRead;
use crate::AuthConfig;
use crate::AuthFailureKind;
use crate::AuthOperation;
use crate::AuthServiceError;
use crate::AuthServiceResult;
use crate::ProviderFuture;
use crate::SubjectKey;
use crate::UserMetadataPort;
use crate::UserMetadataRead;

pub(crate) mod legacy;
mod local;
pub(crate) use local::local_bundle;

/// Lifecycle control owned once by the runtime, separate from concurrent data ports.
pub trait ProviderControl: Send + Sync {
    fn initialize<'a>(&'a self, config: &'a AuthConfig, context: ChildServiceContext) -> ProviderFuture<'a, ()>;
    fn flush(&self) -> ProviderFuture<'_, ()>;
    /// Closes resources idempotently, including a partially initialized provider.
    fn close(&self) -> ProviderFuture<'_, ()>;
}

/// One prepared account in an authoritative ACL file import.
pub struct AclImportAccount {
    pub user: User,
    pub acl: Option<Acl>,
}

/// Optional capability for importing user and ACL metadata as one provider operation.
pub trait AclSnapshotImport: Send + Sync {
    fn import<'a>(&'a self, accounts: &'a [AclImportAccount], removed: &'a [SubjectKey]) -> ProviderFuture<'a, ()>;
}

/// Candidate provider bindings. Their control handles follow initialization order.
pub struct ProviderBundle {
    pub(crate) users: Arc<dyn UserMetadataPort>,
    pub(crate) acls: Arc<dyn AclMetadataPort>,
    pub(crate) controls: Vec<Arc<dyn ProviderControl>>,
    pub(crate) importer: Option<Arc<dyn AclSnapshotImport>>,
}

impl ProviderBundle {
    pub fn new(
        users: Arc<dyn UserMetadataPort>,
        acls: Arc<dyn AclMetadataPort>,
        controls: Vec<Arc<dyn ProviderControl>>,
    ) -> Self {
        Self {
            users,
            acls,
            controls,
            importer: None,
        }
    }

    pub fn with_snapshot_import(mut self, importer: Arc<dyn AclSnapshotImport>) -> Self {
        self.importer = Some(importer);
        self
    }
}

struct OwnedControl {
    port: Arc<dyn ProviderControl>,
    attempted: AtomicBool,
    initialized: AtomicBool,
    flushed: AtomicBool,
    closed: AtomicBool,
}

#[derive(Default)]
struct ProviderLifecycle {
    initialized: bool,
    stopping: bool,
    stopped: bool,
    finalizer: Option<ProviderFuture<'static, ()>>,
}

pub(crate) struct ProviderOwner {
    pub(crate) admission: Arc<ProviderAdmission>,
    pub(crate) users: Arc<UserMetadataHandle>,
    pub(crate) acls: Arc<AclMetadataHandle>,
    pub(crate) importer: Option<Arc<dyn AclSnapshotImport>>,
    controls: Vec<Arc<OwnedControl>>,
    lifecycle: Mutex<ProviderLifecycle>,
}

impl ProviderOwner {
    pub(crate) fn new(bundle: ProviderBundle, initialized: bool) -> Arc<Self> {
        let admission = Arc::new(ProviderAdmission::default());
        if !initialized {
            admission.state.store(2, Ordering::Release);
        }
        Arc::new(Self {
            users: UserMetadataHandle::new(bundle.users, admission.clone()),
            acls: AclMetadataHandle::new(bundle.acls, admission.clone()),
            admission,
            importer: bundle.importer,
            controls: bundle
                .controls
                .into_iter()
                .map(|port| {
                    Arc::new(OwnedControl {
                        port,
                        attempted: AtomicBool::new(initialized),
                        initialized: AtomicBool::new(initialized),
                        flushed: AtomicBool::new(false),
                        closed: AtomicBool::new(false),
                    })
                })
                .collect(),
            lifecycle: Mutex::new(ProviderLifecycle {
                initialized,
                ..Default::default()
            }),
        })
    }

    pub(crate) async fn initialize(&self, config: &AuthConfig, context: &ChildServiceContext) -> AuthServiceResult<()> {
        let mut lifecycle = self.lifecycle.lock().await;
        if lifecycle.stopping || lifecycle.stopped || self.admission.state.load(Ordering::Acquire) & 1 != 0 {
            return Err(AuthServiceError::new(
                AuthOperation::InitializeProvider,
                AuthFailureKind::Unavailable,
            ));
        }
        if lifecycle.initialized {
            return Ok(());
        }
        if self.controls.is_empty() {
            return Err(AuthServiceError::new(
                AuthOperation::InitializeProvider,
                AuthFailureKind::InvalidConfiguration,
            ));
        }
        for control in &self.controls {
            if control.initialized.load(Ordering::Acquire) {
                continue;
            }
            control.attempted.store(true, Ordering::Release);
            control.port.initialize(config, context.clone()).await?;
            control.initialized.store(true, Ordering::Release);
        }
        lifecycle.initialized = true;
        self.admission
            .state
            .compare_exchange(2, 0, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| AuthServiceError::new(AuthOperation::InitializeProvider, AuthFailureKind::Unavailable))?;
        Ok(())
    }

    pub(crate) async fn shutdown_until(&self, deadline: ShutdownDeadline, flush: bool) -> AuthServiceResult<()> {
        self.admission.close();
        let at = tokio::time::Instant::from_std(deadline.instant());
        let mut lifecycle = tokio::time::timeout_at(at, self.lifecycle.lock())
            .await
            .map_err(|_| stop_timeout())?;
        if lifecycle.stopped {
            return Ok(());
        }
        lifecycle.stopping = true;
        if lifecycle.finalizer.is_none() {
            let admission = self.admission.clone();
            let controls = self.controls.clone();
            lifecycle.finalizer = Some(Box::pin(async move {
                admission.wait_drained().await;
                // Finish every attempted control in reverse order. Retained flags
                // prevent successful flush/close operations from running twice on retry.
                let mut failures = Vec::new();
                for control in controls.iter().rev() {
                    if !control.attempted.load(Ordering::Acquire) || control.closed.load(Ordering::Acquire) {
                        continue;
                    }
                    if flush && control.initialized.load(Ordering::Acquire) && !control.flushed.load(Ordering::Acquire)
                    {
                        if let Err(error) = control.port.flush().await {
                            failures.push(error);
                            continue;
                        }
                        control.flushed.store(true, Ordering::Release);
                    }
                    match control.port.close().await {
                        Ok(()) => control.closed.store(true, Ordering::Release),
                        Err(error) => failures.push(error),
                    }
                }
                if failures.is_empty() {
                    Ok(())
                } else {
                    Err(AuthServiceError::with_source(
                        AuthOperation::MaintainService,
                        AuthFailureKind::Unavailable,
                        ProviderCleanupErrors(failures),
                    ))
                }
            }));
        }
        let finalizer = lifecycle
            .finalizer
            .as_mut()
            .ok_or_else(|| AuthServiceError::new(AuthOperation::MaintainService, AuthFailureKind::Internal))?;
        let result = tokio::time::timeout_at(at, finalizer)
            .await
            .map_err(|_| stop_timeout())?;
        lifecycle.finalizer = None;
        result?;
        lifecycle.stopped = true;
        Ok(())
    }
}

fn stop_timeout() -> AuthServiceError {
    AuthServiceError::new(AuthOperation::MaintainService, AuthFailureKind::Timeout)
}

/// All provider cleanup failures, retained for trusted diagnostics.
#[derive(Debug)]
pub struct ProviderCleanupErrors(Vec<AuthServiceError>);

impl ProviderCleanupErrors {
    pub fn failures(&self) -> &[AuthServiceError] {
        &self.0
    }
}

impl std::fmt::Display for ProviderCleanupErrors {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("provider cleanup failed")
    }
}

impl std::error::Error for ProviderCleanupErrors {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.first().map(|error| error as &(dyn std::error::Error + 'static))
    }
}

#[derive(Default)]
pub(crate) struct ProviderAdmission {
    // Low bits mean stopping (1) or initializing (2); remaining bits count
    // operations. Initialization cannot reopen admission after a concurrent stop.
    state: AtomicUsize,
    drained: Notify,
}

impl ProviderAdmission {
    pub(crate) fn enter(self: &Arc<Self>) -> AuthServiceResult<ProviderOperation> {
        self.state
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |state| {
                if state & 3 == 0 {
                    state.checked_add(4)
                } else {
                    None
                }
            })
            .map_err(|_| AuthServiceError::new(AuthOperation::MaintainService, AuthFailureKind::Unavailable))?;
        Ok(ProviderOperation {
            admission: self.clone(),
        })
    }

    pub(crate) fn close(&self) {
        self.state.fetch_or(1, Ordering::AcqRel);
    }

    pub(crate) async fn wait_drained(&self) {
        loop {
            let notified = self.drained.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.state.load(Ordering::Acquire) >> 2 == 0 {
                return;
            }
            notified.await;
        }
    }
}

/// Keeps an admitted authentication request in the runtime's drain set.
/// Dropping the guard releases the request; it does not close providers.
pub struct ProviderOperation {
    admission: Arc<ProviderAdmission>,
}

impl Drop for ProviderOperation {
    fn drop(&mut self) {
        if self.admission.state.fetch_sub(4, Ordering::AcqRel) >> 2 == 1 {
            self.admission.drained.notify_waiters();
        }
    }
}

/// Concurrent user operations sharing the runtime's admission and drain owner.
pub struct UserMetadataHandle {
    port: Arc<dyn UserMetadataPort>,
    admission: Arc<ProviderAdmission>,
}

impl UserMetadataHandle {
    pub(crate) fn legacy(port: Arc<dyn UserMetadataPort>) -> Arc<Self> {
        Self::new(port, Arc::default())
    }

    pub(crate) fn new(port: Arc<dyn UserMetadataPort>, admission: Arc<ProviderAdmission>) -> Arc<Self> {
        Arc::new(Self { port, admission })
    }

    pub async fn get_user(&self, username: &str) -> AuthServiceResult<User> {
        let _operation = self.admission.enter()?;
        self.port.lookup_user(username).await
    }

    pub async fn create_user(&self, user: User) -> AuthServiceResult<()> {
        let _operation = self.admission.enter()?;
        self.port.create_user(user).await
    }

    pub async fn update_user(&self, user: User) -> AuthServiceResult<()> {
        let _operation = self.admission.enter()?;
        self.port.update_user(user).await
    }

    pub async fn delete_user(&self, username: &str) -> AuthServiceResult<()> {
        let _operation = self.admission.enter()?;
        self.port.delete_user(username).await
    }

    pub async fn list_user(&self, filter: Option<&str>) -> AuthServiceResult<Vec<User>> {
        let _operation = self.admission.enter()?;
        self.port.list_user(filter).await
    }
}

impl UserMetadataRead for UserMetadataHandle {
    fn lookup_user<'a>(&'a self, username: &'a str) -> ProviderFuture<'a, User> {
        Box::pin(self.get_user(username))
    }
}

/// Concurrent ACL operations sharing the runtime's admission and drain owner.
pub struct AclMetadataHandle {
    port: Arc<dyn AclMetadataPort>,
    admission: Arc<ProviderAdmission>,
}

impl AclMetadataHandle {
    pub(crate) fn legacy(port: Arc<dyn AclMetadataPort>) -> Arc<Self> {
        Self::new(port, Arc::default())
    }

    pub(crate) fn new(port: Arc<dyn AclMetadataPort>, admission: Arc<ProviderAdmission>) -> Arc<Self> {
        Arc::new(Self { port, admission })
    }

    pub async fn get_acl(&self, subject: &(impl Subject + Send + Sync + ?Sized)) -> AuthServiceResult<Option<Acl>> {
        let _operation = self.admission.enter()?;
        self.port.lookup_acl(&SubjectKey::from_subject(subject)).await
    }

    pub async fn create_acl(&self, acl: Acl) -> AuthServiceResult<()> {
        let _operation = self.admission.enter()?;
        self.port.create_acl(acl).await
    }

    pub async fn update_acl(&self, acl: Acl) -> AuthServiceResult<()> {
        let _operation = self.admission.enter()?;
        self.port.update_acl(acl).await
    }

    pub async fn delete_acl(&self, subject: &(impl Subject + Send + Sync + ?Sized)) -> AuthServiceResult<()> {
        let _operation = self.admission.enter()?;
        self.port.delete_acl(&SubjectKey::from_subject(subject)).await
    }

    pub async fn list_acl(
        &self,
        subject_filter: Option<&str>,
        resource_filter: Option<&str>,
    ) -> AuthServiceResult<Vec<Acl>> {
        let _operation = self.admission.enter()?;
        self.port.list_acl(subject_filter, resource_filter).await
    }
}

impl AclMetadataRead for AclMetadataHandle {
    fn lookup_acl<'a>(&'a self, subject: &'a SubjectKey) -> ProviderFuture<'a, Option<Acl>> {
        Box::pin(self.get_acl(subject))
    }
}
