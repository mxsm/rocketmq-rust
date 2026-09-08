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

//! Revision-aware Admin provider with concurrent queries and serialized CAS writes.

use std::{error::Error as StdError, fmt, future::Future, sync::Arc, time::SystemTime};

use rocketmq_admin_core::{
    client_adapter::ClientRuntime,
    core::{AdminError, AdminFailure},
};
use rocketmq_dashboard_common::{
    AdminSessionStatus, AdminSessionSummary, ConnectionScope, ConnectionSnapshot, EndpointAvailability, EndpointHealth,
};
use rocketmq_runtime::{ChildServiceContext, TaskKind};
use tokio_util::sync::CancellationToken;

use super::{
    admin_session::{
        DashboardMutationSession, DashboardQuerySession, DashboardSessionFactory, RealDashboardSessionFactory,
    },
    auth_state::{AuthRejection, DesktopAuthState},
};

#[path = "admin_provider/consumers.rs"]
mod consumers;
#[path = "admin_provider/delivery03.rs"]
mod delivery03;
#[path = "admin_provider/topics.rs"]
mod topics;

pub(crate) use delivery03::{
    SafeBrokerInfo, SafeBrokerList, SafeBrokerTarget, SafeConfigPatchOutcome, SafeConfigPatchRequest,
};
pub(crate) use topics::{
    SafeTopicCreateRequest, SafeTopicDeleteBrokerRequest, SafeTopicDeleteRequest, SafeTopicOffsetRequest,
    SafeTopicPatchOutcome, SafeTopicQueuePatchRequest, SafeTopicSendRequest,
};

struct CancelOnDrop(CancellationToken);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

trait HealthClock: Send + Sync {
    fn now_epoch_ms(&self) -> Option<u64>;
}

struct SystemHealthClock;

impl HealthClock for SystemHealthClock {
    fn now_epoch_ms(&self) -> Option<u64> {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .ok()
            .and_then(|duration| u64::try_from(duration.as_millis()).ok())
    }
}

struct ProviderState {
    snapshot: Option<ConnectionSnapshot>,
    summary: AdminSessionSummary,
}

impl ProviderState {
    fn new() -> Self {
        Self {
            snapshot: None,
            summary: AdminSessionSummary {
                revision: 0,
                status: AdminSessionStatus::NotConfigured,
                credential_source: Default::default(),
            },
        }
    }
}

struct RevisionedQuerySession {
    revision: u64,
    session: Box<dyn DashboardQuerySession>,
}

struct RevisionedMutationSession {
    revision: u64,
    session: Box<dyn DashboardMutationSession>,
}

/// Stable safe provider failure categories.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProviderFailureCode {
    /// A NameServer has not been selected.
    NotConfigured,
    /// Environment-backed credentials are absent or invalid.
    Authentication,
    /// A real Admin operation failed.
    Unavailable,
    /// Owner cancellation won the operation race.
    Cancelled,
    /// A newer revision already owns the provider.
    StaleRevision,
    /// The owned runtime refused new work.
    Runtime,
}

/// Provider result that separates deterministic rejections from operational errors.
#[derive(Clone)]
pub(crate) struct ProviderFailure {
    kind: ProviderFailureKind,
}

#[derive(Clone)]
enum ProviderFailureKind {
    Rejected(ProviderRejection),
    Operational(ProviderError),
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ProviderRejection {
    NotConfigured(&'static str),
    Authentication(AuthRejection),
    Cancelled,
    StaleRevision,
    SessionUnavailable(&'static str),
    InvalidRequest(&'static str),
    InvalidData(&'static str),
    NotFound(&'static str),
}

/// Redacted operational provider error. Backend bodies and snapshots stay behind the source chain.
#[derive(Clone)]
pub(crate) struct ProviderError {
    inner: Arc<ProviderErrorInner>,
}

struct ProviderErrorInner {
    code: ProviderFailureCode,
    summary: &'static str,
    retryable: bool,
    source: Arc<dyn StdError + Send + Sync>,
}

impl ProviderFailure {
    /// Returns the stable category.
    pub(crate) fn code(&self) -> ProviderFailureCode {
        match &self.kind {
            ProviderFailureKind::Rejected(ProviderRejection::NotConfigured(_)) => ProviderFailureCode::NotConfigured,
            ProviderFailureKind::Rejected(ProviderRejection::Authentication(_)) => ProviderFailureCode::Authentication,
            ProviderFailureKind::Rejected(ProviderRejection::Cancelled) => ProviderFailureCode::Cancelled,
            ProviderFailureKind::Rejected(ProviderRejection::StaleRevision) => ProviderFailureCode::StaleRevision,
            ProviderFailureKind::Rejected(
                ProviderRejection::InvalidRequest(_)
                | ProviderRejection::InvalidData(_)
                | ProviderRejection::NotFound(_)
                | ProviderRejection::SessionUnavailable(_),
            ) => ProviderFailureCode::Unavailable,
            ProviderFailureKind::Operational(error) => error.inner.code,
        }
    }

    /// Returns whether the user may retry without changing input.
    pub(crate) fn is_retryable(&self) -> bool {
        match &self.kind {
            ProviderFailureKind::Rejected(ProviderRejection::NotConfigured(_) | ProviderRejection::StaleRevision) => {
                false
            }
            ProviderFailureKind::Rejected(ProviderRejection::Authentication(rejection)) => {
                matches!(
                    rejection,
                    AuthRejection::MissingEnvironment { .. } | AuthRejection::InvalidAdminCredential
                )
            }
            ProviderFailureKind::Rejected(ProviderRejection::Cancelled) => true,
            ProviderFailureKind::Rejected(
                ProviderRejection::InvalidRequest(_)
                | ProviderRejection::InvalidData(_)
                | ProviderRejection::NotFound(_),
            ) => false,
            ProviderFailureKind::Rejected(ProviderRejection::SessionUnavailable(_)) => true,
            ProviderFailureKind::Operational(error) => error.inner.retryable,
        }
    }

    fn caused_by(
        code: ProviderFailureCode,
        summary: &'static str,
        retryable: bool,
        source: impl StdError + Send + Sync + 'static,
    ) -> Self {
        Self {
            kind: ProviderFailureKind::Operational(ProviderError {
                inner: Arc::new(ProviderErrorInner {
                    code,
                    summary,
                    retryable,
                    source: Arc::new(source),
                }),
            }),
        }
    }

    pub(crate) fn summary(&self) -> &'static str {
        match &self.kind {
            ProviderFailureKind::Rejected(ProviderRejection::NotConfigured(summary)) => summary,
            ProviderFailureKind::Rejected(ProviderRejection::Authentication(_)) => {
                "The environment-backed Admin credential is unavailable."
            }
            ProviderFailureKind::Rejected(ProviderRejection::Cancelled) => "The Admin operation was cancelled.",
            ProviderFailureKind::Rejected(ProviderRejection::StaleRevision) => {
                "A newer connection revision is already active."
            }
            ProviderFailureKind::Rejected(
                ProviderRejection::InvalidRequest(summary)
                | ProviderRejection::InvalidData(summary)
                | ProviderRejection::NotFound(summary)
                | ProviderRejection::SessionUnavailable(summary),
            ) => summary,
            ProviderFailureKind::Operational(error) => error.inner.summary,
        }
    }

    pub(crate) fn into_operational(self) -> Option<ProviderError> {
        match self.kind {
            ProviderFailureKind::Operational(error) => Some(error),
            ProviderFailureKind::Rejected(_) => None,
        }
    }

    fn cancelled() -> Self {
        Self {
            kind: ProviderFailureKind::Rejected(ProviderRejection::Cancelled),
        }
    }

    fn runtime(source: impl StdError + Send + Sync + 'static) -> Self {
        Self::caused_by(
            ProviderFailureCode::Runtime,
            "The dashboard runtime is shutting down.",
            false,
            source,
        )
    }

    fn stale() -> Self {
        Self {
            kind: ProviderFailureKind::Rejected(ProviderRejection::StaleRevision),
        }
    }

    pub(super) fn not_configured(summary: &'static str) -> Self {
        Self {
            kind: ProviderFailureKind::Rejected(ProviderRejection::NotConfigured(summary)),
        }
    }

    pub(super) fn invalid_request(summary: &'static str) -> Self {
        Self {
            kind: ProviderFailureKind::Rejected(ProviderRejection::InvalidRequest(summary)),
        }
    }

    pub(super) fn invalid_data(summary: &'static str) -> Self {
        Self {
            kind: ProviderFailureKind::Rejected(ProviderRejection::InvalidData(summary)),
        }
    }

    pub(super) fn not_found(summary: &'static str) -> Self {
        Self {
            kind: ProviderFailureKind::Rejected(ProviderRejection::NotFound(summary)),
        }
    }

    fn session_unavailable(summary: &'static str) -> Self {
        Self {
            kind: ProviderFailureKind::Rejected(ProviderRejection::SessionUnavailable(summary)),
        }
    }

    #[cfg(test)]
    pub(crate) fn unavailable_for_test() -> Self {
        Self::caused_by(
            ProviderFailureCode::Unavailable,
            "The injected Admin connection is unavailable.",
            true,
            std::io::Error::other("injected Admin connection failure"),
        )
    }
}

impl PartialEq for ProviderFailure {
    fn eq(&self, other: &Self) -> bool {
        self.code() == other.code() && self.summary() == other.summary() && self.is_retryable() == other.is_retryable()
    }
}

impl Eq for ProviderFailure {}

impl fmt::Debug for ProviderFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderFailure")
            .field("code", &self.code())
            .field("retryable", &self.is_retryable())
            .field(
                "operational",
                &matches!(&self.kind, ProviderFailureKind::Operational(_)),
            )
            .finish()
    }
}

impl fmt::Debug for ProviderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderError")
            .field("code", &self.inner.code)
            .field("retryable", &self.inner.retryable)
            .finish()
    }
}

impl fmt::Display for ProviderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.inner.summary)
    }
}

impl StdError for ProviderError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(self.inner.source.as_ref())
    }
}

/// GPUI-specific Admin provider backed by application-owned query and mutation sessions.
pub struct GpuiAdminProvider {
    context: ChildServiceContext,
    factory: Arc<dyn DashboardSessionFactory>,
    auth: Arc<DesktopAuthState>,
    clock: Arc<dyn HealthClock>,
    state: parking_lot::RwLock<ProviderState>,
    switch_gate: tokio::sync::Mutex<()>,
    query_session: tokio::sync::RwLock<Option<RevisionedQuerySession>>,
    mutation_session: tokio::sync::Mutex<Option<RevisionedMutationSession>>,
}

impl GpuiAdminProvider {
    /// Creates the production provider.
    pub fn new(
        context: ChildServiceContext,
        client_runtime: Arc<ClientRuntime>,
        auth: Arc<DesktopAuthState>,
    ) -> Arc<Self> {
        Self::with_factory(
            context,
            Arc::new(RealDashboardSessionFactory::new(client_runtime)),
            auth,
            Arc::new(SystemHealthClock),
        )
    }

    /// Returns the currently published connection revision, if configured.
    pub fn revision(&self) -> Option<u64> {
        self.state.read().snapshot.as_ref().map(|snapshot| snapshot.revision)
    }

    fn with_factory(
        context: ChildServiceContext,
        factory: Arc<dyn DashboardSessionFactory>,
        auth: Arc<DesktopAuthState>,
        clock: Arc<dyn HealthClock>,
    ) -> Arc<Self> {
        Arc::new(Self {
            context,
            factory,
            auth,
            clock,
            state: parking_lot::RwLock::new(ProviderState::new()),
            switch_gate: tokio::sync::Mutex::new(()),
            query_session: tokio::sync::RwLock::new(None),
            mutation_session: tokio::sync::Mutex::new(None),
        })
    }

    /// Replaces both session scopes for a persisted connection revision.
    pub async fn switch(
        self: &Arc<Self>,
        snapshot: ConnectionSnapshot,
    ) -> Result<AdminSessionSummary, ProviderFailure> {
        let this = Arc::clone(self);
        self.run_owned("gpui-provider-switch", move |cancellation| async move {
            this.switch_inner(snapshot, cancellation).await
        })
        .await
    }

    /// Checks the current NameServer through the concurrent query session.
    pub async fn check_health(self: &Arc<Self>) -> Result<EndpointHealth, ProviderFailure> {
        let revision = self.current_snapshot()?.revision;
        let this = Arc::clone(self);
        self.run_owned("gpui-provider-health", move |cancellation| async move {
            this.health_inner(revision, cancellation).await
        })
        .await
    }

    #[cfg(test)]
    async fn check_health_with_cancellation(
        self: &Arc<Self>,
        cancellation: CancellationToken,
    ) -> Result<EndpointHealth, ProviderFailure> {
        let revision = self.current_snapshot()?.revision;
        let this = Arc::clone(self);
        self.run_owned("gpui-provider-health", move |owned_cancellation| async move {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => Err(ProviderFailure::cancelled()),
                result = this.health_inner(revision, owned_cancellation) => result,
            }
        })
        .await
    }

    /// Checks configured NameServers without changing the active provider revision.
    pub async fn check_endpoints(
        self: &Arc<Self>,
        snapshots: Vec<ConnectionSnapshot>,
    ) -> Result<Vec<EndpointHealth>, ProviderFailure> {
        let this = Arc::clone(self);
        self.run_owned("gpui-provider-check-all", move |cancellation| async move {
            let mut results = Vec::with_capacity(snapshots.len());
            for snapshot in snapshots {
                if cancellation.is_cancelled() {
                    return Err(ProviderFailure::cancelled());
                }
                results.push(this.check_snapshot_inner(snapshot, cancellation.clone()).await?);
            }
            Ok(results)
        })
        .await
    }

    /// Explicitly closes query and mutation sessions before the client runtime stops.
    pub async fn shutdown(&self) {
        let _switch = self.switch_gate.lock().await;
        let mut query = self.query_session.write().await;
        let mut mutation = self.mutation_session.lock().await;
        shutdown_sessions(query.take(), mutation.take()).await;
        self.state.write().summary.status = AdminSessionStatus::Closed;
    }

    async fn switch_inner(
        &self,
        snapshot: ConnectionSnapshot,
        cancellation: CancellationToken,
    ) -> Result<AdminSessionSummary, ProviderFailure> {
        let _switch = self.switch_gate.lock().await;
        {
            let mut state = self.state.write();
            if let Some(current) = state.snapshot.as_ref() {
                if current == &snapshot {
                    return Ok(state.summary.clone());
                }
                if snapshot.revision <= current.revision {
                    return Err(ProviderFailure::stale());
                }
            }
            state.snapshot = Some(snapshot.clone());
            state.summary = AdminSessionSummary {
                revision: snapshot.revision,
                status: AdminSessionStatus::Connecting,
                credential_source: snapshot.credential_source,
            };
        }

        // Publishing the revision first makes new callers reject the old slot. The write lock then
        // waits for every in-flight query; the mutation lock serializes and drains CAS work.
        let mut query = self.query_session.write().await;
        let mut mutation = self.mutation_session.lock().await;
        shutdown_sessions(query.take(), mutation.take()).await;

        let Some(_) = snapshot.nameserver.as_ref() else {
            self.state.write().summary.status = AdminSessionStatus::NotConfigured;
            return Ok(self.state.read().summary.clone());
        };
        let credentials = match self.auth.resolve_admin_credentials(snapshot.credential_source) {
            Ok(credentials) => credentials,
            Err(error) => {
                self.state.write().summary.status = AdminSessionStatus::Failed;
                return Err(map_auth_error(error));
            }
        };
        let created = tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(ProviderFailure::cancelled()),
            created = self.factory.create_query(snapshot.clone(), credentials) => created.map_err(map_admin_error),
        };
        match created {
            Ok(session) => {
                *query = Some(RevisionedQuerySession {
                    revision: snapshot.revision,
                    session,
                });
                self.state.write().summary.status = AdminSessionStatus::Connected;
                Ok(self.state.read().summary.clone())
            }
            Err(error) => {
                self.state.write().summary.status = AdminSessionStatus::Failed;
                Err(error)
            }
        }
    }

    async fn health_inner(
        &self,
        revision: u64,
        cancellation: CancellationToken,
    ) -> Result<EndpointHealth, ProviderFailure> {
        let snapshot = self.snapshot_for_revision(revision)?;
        if snapshot.scope == ConnectionScope::Proxy {
            let endpoint = snapshot.proxy.clone().ok_or_else(not_configured)?;
            return Ok(EndpointHealth {
                endpoint,
                revision,
                availability: EndpointAvailability::Unknown,
                checked_at_epoch_ms: self.clock.now_epoch_ms(),
                failure_summary: None,
            });
        }
        let endpoint = snapshot.nameserver.clone().ok_or_else(not_configured)?;
        let guard = self.query_session.read().await;
        let session = query_for_revision(&guard, revision)?;
        match select_admin(cancellation, session.health()).await {
            Ok(()) => Ok(EndpointHealth {
                endpoint,
                revision,
                availability: EndpointAvailability::Available,
                checked_at_epoch_ms: self.clock.now_epoch_ms(),
                failure_summary: None,
            }),
            Err(error) if error.code() == ProviderFailureCode::Cancelled => Err(error),
            Err(error) => Ok(EndpointHealth {
                endpoint,
                revision,
                availability: EndpointAvailability::Unavailable,
                checked_at_epoch_ms: self.clock.now_epoch_ms(),
                failure_summary: Some(error.summary().to_owned()),
            }),
        }
    }

    async fn check_snapshot_inner(
        &self,
        snapshot: ConnectionSnapshot,
        cancellation: CancellationToken,
    ) -> Result<EndpointHealth, ProviderFailure> {
        if snapshot.scope == ConnectionScope::Proxy {
            return Ok(EndpointHealth {
                endpoint: snapshot.proxy.clone().unwrap_or_default(),
                revision: snapshot.revision,
                availability: EndpointAvailability::Unknown,
                checked_at_epoch_ms: self.clock.now_epoch_ms(),
                failure_summary: None,
            });
        }
        let endpoint = snapshot.nameserver.clone().unwrap_or_default();
        let credentials = match self.auth.resolve_admin_credentials(snapshot.credential_source) {
            Ok(credentials) => credentials,
            Err(error) => {
                return Ok(unavailable_health(
                    &snapshot,
                    endpoint,
                    map_auth_error(error),
                    self.clock.as_ref(),
                ));
            }
        };
        let created = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(ProviderFailure::cancelled()),
            created = self.factory.create_query(snapshot.clone(), credentials) => created,
        };
        let session = match created {
            Ok(session) => session,
            Err(error) => {
                return Ok(unavailable_health(
                    &snapshot,
                    endpoint,
                    map_admin_error(error),
                    self.clock.as_ref(),
                ));
            }
        };
        let result = select_admin(cancellation, session.health()).await;
        session.shutdown().await;
        match result {
            Ok(()) => Ok(EndpointHealth {
                endpoint,
                revision: snapshot.revision,
                availability: EndpointAvailability::Available,
                checked_at_epoch_ms: self.clock.now_epoch_ms(),
                failure_summary: None,
            }),
            Err(error) if error.code() == ProviderFailureCode::Cancelled => Err(error),
            Err(error) => Ok(unavailable_health(&snapshot, endpoint, error, self.clock.as_ref())),
        }
    }

    async fn ensure_mutation(
        &self,
        slot: &mut Option<RevisionedMutationSession>,
        revision: u64,
        cancellation: CancellationToken,
    ) -> Result<(), ProviderFailure> {
        if slot.as_ref().is_some_and(|session| session.revision == revision) {
            return Ok(());
        }
        if let Some(session) = slot.take() {
            session.session.shutdown().await;
        }
        let snapshot = self.snapshot_for_revision(revision)?;
        let credentials = self
            .auth
            .resolve_admin_credentials(snapshot.credential_source)
            .map_err(map_auth_error)?;
        let session = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(ProviderFailure::cancelled()),
            created = self.factory.create_mutation(snapshot, credentials) => created.map_err(map_admin_error)?,
        };
        *slot = Some(RevisionedMutationSession { revision, session });
        Ok(())
    }

    fn current_snapshot(&self) -> Result<ConnectionSnapshot, ProviderFailure> {
        self.state.read().snapshot.clone().ok_or_else(not_configured)
    }

    fn snapshot_for_revision(&self, revision: u64) -> Result<ConnectionSnapshot, ProviderFailure> {
        let snapshot = self.current_snapshot()?;
        if snapshot.revision == revision {
            Ok(snapshot)
        } else {
            Err(ProviderFailure::stale())
        }
    }

    async fn run_owned<T, Build, OwnedFuture>(&self, name: &'static str, build: Build) -> Result<T, ProviderFailure>
    where
        T: Send + 'static,
        Build: FnOnce(CancellationToken) -> OwnedFuture,
        OwnedFuture: Future<Output = Result<T, ProviderFailure>> + Send + 'static,
    {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let owner_cancellation = self.context.task_spawner().cancellation_token();
        let request_cancellation = CancellationToken::new();
        let _cancel_on_drop = CancelOnDrop(request_cancellation.clone());
        let future = build(request_cancellation.clone());
        self.context
            .spawn(name, TaskKind::Other, async move {
                tokio::pin!(future);
                let result = tokio::select! {
                    biased;
                    result = &mut future => result,
                    _ = owner_cancellation.cancelled() => {
                        request_cancellation.cancel();
                        future.await
                    },
                    _ = request_cancellation.cancelled() => future.await,
                };
                let _ = sender.send(result);
            })
            .map_err(ProviderFailure::runtime)?;
        receiver.await.map_err(ProviderFailure::runtime)?
    }
}

fn query_for_revision(
    slot: &Option<RevisionedQuerySession>,
    revision: u64,
) -> Result<&dyn DashboardQuerySession, ProviderFailure> {
    match slot {
        Some(session) if session.revision == revision => Ok(session.session.as_ref()),
        Some(_) => Err(ProviderFailure::stale()),
        None => Err(ProviderFailure::session_unavailable(
            "The Admin query session is unavailable.",
        )),
    }
}

fn mutation_for_revision(
    slot: &mut Option<RevisionedMutationSession>,
    revision: u64,
) -> Result<&mut dyn DashboardMutationSession, ProviderFailure> {
    match slot {
        Some(session) if session.revision == revision => Ok(session.session.as_mut()),
        Some(_) => Err(ProviderFailure::stale()),
        None => Err(ProviderFailure::session_unavailable(
            "The Admin mutation session is unavailable.",
        )),
    }
}

async fn select_admin<T>(
    cancellation: CancellationToken,
    future: impl Future<Output = Result<T, AdminError>>,
) -> Result<T, ProviderFailure> {
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => Err(ProviderFailure::cancelled()),
        result = future => result.map_err(map_admin_error),
    }
}

async fn shutdown_sessions(query: Option<RevisionedQuerySession>, mutation: Option<RevisionedMutationSession>) {
    if let Some(query) = query {
        query.session.shutdown().await;
    }
    if let Some(mutation) = mutation {
        mutation.session.shutdown().await;
    }
}

fn unavailable_health(
    snapshot: &ConnectionSnapshot,
    endpoint: String,
    error: ProviderFailure,
    clock: &dyn HealthClock,
) -> EndpointHealth {
    EndpointHealth {
        endpoint,
        revision: snapshot.revision,
        availability: EndpointAvailability::Unavailable,
        checked_at_epoch_ms: clock.now_epoch_ms(),
        failure_summary: Some(error.summary().to_owned()),
    }
}

fn not_configured() -> ProviderFailure {
    ProviderFailure::not_configured("No NameServer is configured.")
}

fn map_auth_error(rejection: AuthRejection) -> ProviderFailure {
    ProviderFailure {
        kind: ProviderFailureKind::Rejected(ProviderRejection::Authentication(rejection)),
    }
}

fn map_admin_error(error: AdminError) -> ProviderFailure {
    let retryable = error.is_retryable() || error.is_session_closed();
    match error.failure() {
        AdminFailure::InvalidArgument => ProviderFailure::caused_by(
            ProviderFailureCode::Unavailable,
            "The Admin operation configuration is invalid.",
            false,
            error,
        ),
        AdminFailure::NotFound | AdminFailure::Backend | AdminFailure::SessionClosed => ProviderFailure::caused_by(
            ProviderFailureCode::Unavailable,
            "The RocketMQ Admin operation failed.",
            retryable,
            error,
        ),
    }
}

#[cfg(test)]
#[path = "admin_provider/tests.rs"]
mod tests;
