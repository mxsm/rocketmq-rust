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

//! Read-only Admin provider with one serialized session and revision-aware results.

use std::{future::Future, pin::Pin, sync::Arc, time::SystemTime};

use rocketmq_admin_core::{
    core::{
        AdminError, AdminResult,
        security::AdminCredentials,
        topic::{ListTopicsRequest, TopicQueryAdmin},
    },
    read_client_adapter::{ClientRuntime, ReadAdminBuilder, ReadAdminSession},
};
use rocketmq_dashboard_common::{
    AdminSessionStatus, AdminSessionSummary, ConnectionScope, ConnectionSnapshot, EndpointAvailability, EndpointHealth,
};
use rocketmq_runtime::{ChildServiceContext, TaskKind};
use tokio_util::sync::CancellationToken;

use super::auth_state::{AuthStateError, DesktopAuthState};

type SessionFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

struct CancelOnDrop(CancellationToken);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

trait AdminHealthSession: Send {
    fn health(&mut self) -> SessionFuture<'_, AdminResult<()>>;
    fn shutdown(&mut self) -> SessionFuture<'_, ()>;
}

struct ReadHealthSession {
    inner: ReadAdminSession,
}

impl AdminHealthSession for ReadHealthSession {
    fn health(&mut self) -> SessionFuture<'_, AdminResult<()>> {
        Box::pin(async {
            TopicQueryAdmin::list_topics(&mut self.inner, &ListTopicsRequest::default())
                .await
                .map(|_| ())
        })
    }

    fn shutdown(&mut self) -> SessionFuture<'_, ()> {
        Box::pin(self.inner.shutdown())
    }
}

trait AdminSessionFactory: Send + Sync {
    fn create(
        &self,
        snapshot: &ConnectionSnapshot,
        credentials: Option<AdminCredentials>,
    ) -> SessionFuture<'_, AdminResult<Box<dyn AdminHealthSession>>>;
}

struct ReadSessionFactory {
    client_runtime: Arc<ClientRuntime>,
}

impl AdminSessionFactory for ReadSessionFactory {
    fn create(
        &self,
        snapshot: &ConnectionSnapshot,
        credentials: Option<AdminCredentials>,
    ) -> SessionFuture<'_, AdminResult<Box<dyn AdminHealthSession>>> {
        let mut builder = ReadAdminBuilder::new(Arc::clone(&self.client_runtime))
            .vip_channel_enabled(snapshot.transport.use_vip_channel)
            .use_tls(snapshot.transport.use_tls)
            .timeout_millis(5_000)
            .instance_name(format!("gpui-read-{}", snapshot.revision));
        if let Some(nameserver) = snapshot.nameserver.as_deref() {
            builder = builder.namesrv_addr(nameserver);
        }
        if let Some(credentials) = credentials {
            builder = builder.credentials(credentials);
        }
        Box::pin(async move {
            builder
                .build_and_start()
                .await
                .map(|inner| Box::new(ReadHealthSession { inner }) as Box<dyn AdminHealthSession>)
        })
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
    session: Option<Box<dyn AdminHealthSession>>,
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
            session: None,
        }
    }
}

/// Stable safe provider failure categories.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProviderErrorCode {
    /// A NameServer has not been selected.
    NotConfigured,
    /// Environment-backed credentials are absent or invalid.
    Authentication,
    /// The real Admin session or health call failed.
    Unavailable,
    /// Owner cancellation won the operation race.
    Cancelled,
    /// A newer revision already owns the provider.
    StaleRevision,
    /// The owned runtime refused new work.
    Runtime,
}

/// Redacted provider error. Source bodies and connection snapshots are never retained.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{summary}")]
pub struct ProviderError {
    code: ProviderErrorCode,
    summary: &'static str,
    retryable: bool,
}

impl ProviderError {
    /// Returns the stable category.
    pub fn code(&self) -> ProviderErrorCode {
        self.code
    }

    /// Returns whether the user may retry without changing input.
    pub fn is_retryable(&self) -> bool {
        self.retryable
    }

    fn new(code: ProviderErrorCode, summary: &'static str, retryable: bool) -> Self {
        Self {
            code,
            summary,
            retryable,
        }
    }

    fn cancelled() -> Self {
        Self::new(
            ProviderErrorCode::Cancelled,
            "The connection check was cancelled.",
            true,
        )
    }

    fn runtime() -> Self {
        Self::new(
            ProviderErrorCode::Runtime,
            "The dashboard runtime is shutting down.",
            false,
        )
    }

    #[cfg(test)]
    pub(crate) fn unavailable_for_test() -> Self {
        Self::new(
            ProviderErrorCode::Unavailable,
            "The injected Admin connection is unavailable.",
            true,
        )
    }
}

/// GPUI-specific Admin provider backed by the read-only Admin adapter.
pub struct GpuiAdminProvider {
    context: ChildServiceContext,
    factory: Arc<dyn AdminSessionFactory>,
    auth: Arc<DesktopAuthState>,
    clock: Arc<dyn HealthClock>,
    state: tokio::sync::Mutex<ProviderState>,
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
            Arc::new(ReadSessionFactory { client_runtime }),
            auth,
            Arc::new(SystemHealthClock),
        )
    }

    fn with_factory(
        context: ChildServiceContext,
        factory: Arc<dyn AdminSessionFactory>,
        auth: Arc<DesktopAuthState>,
        clock: Arc<dyn HealthClock>,
    ) -> Arc<Self> {
        Arc::new(Self {
            context,
            factory,
            auth,
            clock,
            state: tokio::sync::Mutex::new(ProviderState::new()),
        })
    }

    /// Replaces the serialized Admin session for a persisted snapshot.
    pub async fn switch(self: &Arc<Self>, snapshot: ConnectionSnapshot) -> Result<AdminSessionSummary, ProviderError> {
        let this = Arc::clone(self);
        self.run_owned("gpui-provider-switch", move |cancellation| async move {
            this.switch_inner(snapshot, cancellation).await
        })
        .await
    }

    /// Checks the current NameServer with exactly one read-only `list_topics(Default)` call.
    pub async fn check_health(self: &Arc<Self>) -> Result<EndpointHealth, ProviderError> {
        let this = Arc::clone(self);
        self.run_owned("gpui-provider-health", move |cancellation| async move {
            this.health_inner(cancellation).await
        })
        .await
    }

    #[cfg(test)]
    async fn check_health_with_cancellation(
        self: &Arc<Self>,
        cancellation: CancellationToken,
    ) -> Result<EndpointHealth, ProviderError> {
        let this = Arc::clone(self);
        self.run_owned("gpui-provider-health", move |owned_cancellation| async move {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => Err(ProviderError::cancelled()),
                result = this.health_inner(owned_cancellation) => result,
            }
        })
        .await
    }

    /// Checks configured NameServers without changing the active provider snapshot.
    pub async fn check_endpoints(
        self: &Arc<Self>,
        snapshots: Vec<ConnectionSnapshot>,
    ) -> Result<Vec<EndpointHealth>, ProviderError> {
        let this = Arc::clone(self);
        self.run_owned("gpui-provider-check-all", move |cancellation| async move {
            let mut results = Vec::with_capacity(snapshots.len());
            for snapshot in snapshots {
                if cancellation.is_cancelled() {
                    return Err(ProviderError::cancelled());
                }
                let _session_guard = this.state.lock().await;
                results.push(this.check_snapshot_inner(snapshot, cancellation.clone()).await?);
            }
            Ok(results)
        })
        .await
    }

    /// Explicitly closes the old session before the client runtime is stopped.
    pub async fn shutdown(&self) {
        let mut state = self.state.lock().await;
        if let Some(mut session) = state.session.take() {
            session.shutdown().await;
        }
        state.summary.status = AdminSessionStatus::Closed;
    }

    async fn switch_inner(
        &self,
        snapshot: ConnectionSnapshot,
        cancellation: CancellationToken,
    ) -> Result<AdminSessionSummary, ProviderError> {
        let mut state = self.state.lock().await;
        if let Some(current) = state.snapshot.as_ref() {
            if current == &snapshot {
                return Ok(state.summary.clone());
            }
            if snapshot.revision <= current.revision {
                return Err(ProviderError::new(
                    ProviderErrorCode::StaleRevision,
                    "A newer connection revision is already active.",
                    false,
                ));
            }
        }
        let credentials = self
            .auth
            .resolve_admin_credentials(snapshot.credential_source)
            .map_err(map_auth_error)?;
        state.summary = AdminSessionSummary {
            revision: snapshot.revision,
            status: AdminSessionStatus::Connecting,
            credential_source: snapshot.credential_source,
        };
        if let Some(mut old_session) = state.session.take() {
            old_session.shutdown().await;
        }
        if snapshot.nameserver.is_none() {
            state.snapshot = Some(snapshot);
            state.summary.status = AdminSessionStatus::NotConfigured;
            return Ok(state.summary.clone());
        }
        let created = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(ProviderError::cancelled()),
            created = self.factory.create(&snapshot, credentials) => created,
        };
        match created {
            Ok(session) => {
                state.session = Some(session);
                state.snapshot = Some(snapshot);
                state.summary.status = AdminSessionStatus::Connected;
                Ok(state.summary.clone())
            }
            Err(error) => {
                let safe = map_admin_error(&error);
                state.snapshot = Some(snapshot);
                state.summary.status = AdminSessionStatus::Failed;
                Err(safe)
            }
        }
    }

    async fn health_inner(&self, cancellation: CancellationToken) -> Result<EndpointHealth, ProviderError> {
        let mut state = self.state.lock().await;
        let snapshot = state.snapshot.clone().ok_or_else(|| {
            ProviderError::new(ProviderErrorCode::NotConfigured, "No NameServer is configured.", false)
        })?;
        if snapshot.scope == ConnectionScope::Proxy {
            let endpoint = snapshot.proxy.clone().ok_or_else(|| {
                ProviderError::new(ProviderErrorCode::NotConfigured, "No Proxy is configured.", false)
            })?;
            return Ok(EndpointHealth {
                endpoint,
                revision: snapshot.revision,
                availability: EndpointAvailability::Unknown,
                checked_at_epoch_ms: self.clock.now_epoch_ms(),
                failure_summary: None,
            });
        }
        let endpoint = snapshot.nameserver.clone().ok_or_else(|| {
            ProviderError::new(ProviderErrorCode::NotConfigured, "No NameServer is configured.", false)
        })?;
        let session = state.session.as_mut().ok_or_else(|| {
            ProviderError::new(
                ProviderErrorCode::Unavailable,
                "The Admin session is unavailable.",
                true,
            )
        })?;
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(ProviderError::cancelled()),
            result = session.health() => result,
        };
        match result {
            Ok(()) => Ok(EndpointHealth {
                endpoint,
                revision: snapshot.revision,
                availability: EndpointAvailability::Available,
                checked_at_epoch_ms: self.clock.now_epoch_ms(),
                failure_summary: None,
            }),
            Err(error) => {
                let safe = map_admin_error(&error);
                Ok(EndpointHealth {
                    endpoint,
                    revision: snapshot.revision,
                    availability: EndpointAvailability::Unavailable,
                    checked_at_epoch_ms: self.clock.now_epoch_ms(),
                    failure_summary: Some(safe.to_string()),
                })
            }
        }
    }

    async fn check_snapshot_inner(
        &self,
        snapshot: ConnectionSnapshot,
        cancellation: CancellationToken,
    ) -> Result<EndpointHealth, ProviderError> {
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
            _ = cancellation.cancelled() => return Err(ProviderError::cancelled()),
            created = self.factory.create(&snapshot, credentials) => created,
        };
        let mut session = match created {
            Ok(session) => session,
            Err(error) => {
                return Ok(unavailable_health(
                    &snapshot,
                    endpoint,
                    map_admin_error(&error),
                    self.clock.as_ref(),
                ));
            }
        };
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(ProviderError::cancelled()),
            result = session.health() => result.map_err(|error| map_admin_error(&error)),
        };
        session.shutdown().await;
        match result {
            Ok(()) => Ok(EndpointHealth {
                endpoint,
                revision: snapshot.revision,
                availability: EndpointAvailability::Available,
                checked_at_epoch_ms: self.clock.now_epoch_ms(),
                failure_summary: None,
            }),
            Err(error) if error.code() == ProviderErrorCode::Cancelled => Err(error),
            Err(error) => Ok(unavailable_health(&snapshot, endpoint, error, self.clock.as_ref())),
        }
    }

    async fn run_owned<T, Build, OwnedFuture>(&self, name: &'static str, build: Build) -> Result<T, ProviderError>
    where
        T: Send + 'static,
        Build: FnOnce(CancellationToken) -> OwnedFuture,
        OwnedFuture: Future<Output = Result<T, ProviderError>> + Send + 'static,
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
            .map_err(|_| ProviderError::runtime())?;
        receiver.await.map_err(|_| ProviderError::runtime())?
    }
}

fn unavailable_health(
    snapshot: &ConnectionSnapshot,
    endpoint: String,
    error: ProviderError,
    clock: &dyn HealthClock,
) -> EndpointHealth {
    EndpointHealth {
        endpoint,
        revision: snapshot.revision,
        availability: EndpointAvailability::Unavailable,
        checked_at_epoch_ms: clock.now_epoch_ms(),
        failure_summary: Some(error.to_string()),
    }
}

fn map_auth_error(_error: AuthStateError) -> ProviderError {
    ProviderError::new(
        ProviderErrorCode::Authentication,
        "The environment-backed Admin credential is unavailable.",
        true,
    )
}

fn map_admin_error(error: &AdminError) -> ProviderError {
    match error {
        AdminError::InvalidArgument { .. } => ProviderError::new(
            ProviderErrorCode::Unavailable,
            "The Admin connection configuration is invalid.",
            false,
        ),
        AdminError::NotFound { .. } | AdminError::Backend { .. } | AdminError::SessionClosed => ProviderError::new(
            ProviderErrorCode::Unavailable,
            "The RocketMQ Admin health check failed.",
            error.is_retryable() || matches!(error, AdminError::SessionClosed),
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use rocketmq_dashboard_common::{CredentialSourceKind, TransportSettings};
    use rocketmq_runtime::{ProcessMemoryLimit, RuntimeConfig, RuntimeOwner};

    use super::*;
    use crate::infrastructure::auth_state::MapEnvironment;

    struct FixedClock;

    impl HealthClock for FixedClock {
        fn now_epoch_ms(&self) -> Option<u64> {
            Some(42)
        }
    }

    struct FakeFactory {
        health: Result<(), AdminError>,
        shutdowns: Arc<AtomicUsize>,
    }

    impl AdminSessionFactory for FakeFactory {
        fn create(
            &self,
            _snapshot: &ConnectionSnapshot,
            _credentials: Option<AdminCredentials>,
        ) -> SessionFuture<'_, AdminResult<Box<dyn AdminHealthSession>>> {
            let health = self.health.clone();
            let shutdowns = Arc::clone(&self.shutdowns);
            Box::pin(async move { Ok(Box::new(FakeSession { health, shutdowns }) as Box<dyn AdminHealthSession>) })
        }
    }

    struct FakeSession {
        health: Result<(), AdminError>,
        shutdowns: Arc<AtomicUsize>,
    }

    struct BlockingFactory {
        entered: std::sync::mpsc::Sender<()>,
        shutdown: std::sync::mpsc::Sender<()>,
    }

    impl AdminSessionFactory for BlockingFactory {
        fn create(
            &self,
            _snapshot: &ConnectionSnapshot,
            _credentials: Option<AdminCredentials>,
        ) -> SessionFuture<'_, AdminResult<Box<dyn AdminHealthSession>>> {
            let entered = self.entered.clone();
            let shutdown = self.shutdown.clone();
            Box::pin(async move {
                let _ = entered.send(());
                Ok(Box::new(BlockingSession { shutdown }) as Box<dyn AdminHealthSession>)
            })
        }
    }

    struct BlockingSession {
        shutdown: std::sync::mpsc::Sender<()>,
    }

    impl AdminHealthSession for BlockingSession {
        fn health(&mut self) -> SessionFuture<'_, AdminResult<()>> {
            Box::pin(std::future::pending())
        }

        fn shutdown(&mut self) -> SessionFuture<'_, ()> {
            let shutdown = self.shutdown.clone();
            Box::pin(async move {
                let _ = shutdown.send(());
            })
        }
    }

    impl AdminHealthSession for FakeSession {
        fn health(&mut self) -> SessionFuture<'_, AdminResult<()>> {
            Box::pin(std::future::ready(self.health.clone()))
        }

        fn shutdown(&mut self) -> SessionFuture<'_, ()> {
            self.shutdowns.fetch_add(1, Ordering::SeqCst);
            Box::pin(std::future::ready(()))
        }
    }

    fn runtime() -> RuntimeOwner {
        RuntimeOwner::new_with_memory_limit(
            RuntimeConfig::for_parallelism("gpui-provider-test", 1),
            ProcessMemoryLimit::configured(256 * 1024 * 1024).expect("memory limit"),
        )
        .expect("test runtime")
    }

    fn snapshot(revision: u64, scope: ConnectionScope) -> ConnectionSnapshot {
        ConnectionSnapshot {
            revision,
            nameserver: Some("localhost:9876".into()),
            proxy: (scope == ConnectionScope::Proxy).then(|| "localhost:8080".into()),
            scope,
            transport: TransportSettings::default(),
            credential_source: CredentialSourceKind::None,
        }
    }

    fn provider(
        runtime: &RuntimeOwner,
        health: Result<(), AdminError>,
        shutdowns: Arc<AtomicUsize>,
    ) -> Arc<GpuiAdminProvider> {
        GpuiAdminProvider::with_factory(
            runtime.root_context().component("provider"),
            Arc::new(FakeFactory { health, shutdowns }),
            DesktopAuthState::new(Arc::new(MapEnvironment::new([]))),
            Arc::new(FixedClock),
        )
    }

    #[test]
    fn health_uses_session_success_only_and_proxy_health_is_unknown() {
        let runtime = runtime();
        let provider = provider(&runtime, Ok(()), Arc::new(AtomicUsize::new(0)));
        runtime.block_on(async {
            provider
                .switch(snapshot(1, ConnectionScope::NameServer))
                .await
                .expect("switch");
            let health = provider.check_health().await.expect("health");
            assert_eq!(health.availability, EndpointAvailability::Available);
            assert_eq!(health.checked_at_epoch_ms, Some(42));
            let mut proxy = snapshot(2, ConnectionScope::Proxy);
            proxy.nameserver = None;
            provider.switch(proxy).await.expect("switch");
            let health = provider.check_health().await.expect("proxy health");
            assert_eq!(health.availability, EndpointAvailability::Unknown);
        });
        runtime.block_on(provider.shutdown());
        runtime.shutdown_runtime_blocking().expect("shutdown");
    }

    #[test]
    fn backend_error_body_is_not_exposed_and_cancellation_wins_deterministically() {
        let runtime = runtime();
        let provider = provider(
            &runtime,
            Err(AdminError::backend("list_topics", "access-value secret-value")),
            Arc::new(AtomicUsize::new(0)),
        );
        runtime.block_on(async {
            provider
                .switch(snapshot(1, ConnectionScope::NameServer))
                .await
                .expect("switch");
            let health = provider.check_health().await.expect("failed health is a result");
            let summary = health.failure_summary.expect("failure summary");
            assert!(!summary.contains("access-value"));
            assert!(!summary.contains("secret-value"));

            let cancellation = CancellationToken::new();
            cancellation.cancel();
            let error = provider
                .check_health_with_cancellation(cancellation)
                .await
                .expect_err("cancelled");
            assert_eq!(error.code(), ProviderErrorCode::Cancelled);
        });
        runtime.block_on(provider.shutdown());
        runtime.shutdown_runtime_blocking().expect("shutdown");
    }

    #[test]
    fn switch_shuts_old_session_and_rejects_stale_revision() {
        let runtime = runtime();
        let shutdowns = Arc::new(AtomicUsize::new(0));
        let provider = provider(&runtime, Ok(()), Arc::clone(&shutdowns));
        runtime.block_on(async {
            provider
                .switch(snapshot(2, ConnectionScope::NameServer))
                .await
                .expect("first");
            provider
                .switch(snapshot(3, ConnectionScope::NameServer))
                .await
                .expect("second");
            assert_eq!(shutdowns.load(Ordering::SeqCst), 1);
            provider
                .switch(snapshot(3, ConnectionScope::NameServer))
                .await
                .expect("identical revision is idempotent");
            assert_eq!(shutdowns.load(Ordering::SeqCst), 1);
            let stale = provider
                .switch(snapshot(2, ConnectionScope::NameServer))
                .await
                .expect_err("stale");
            assert_eq!(stale.code(), ProviderErrorCode::StaleRevision);
        });
        runtime.block_on(provider.shutdown());
        assert_eq!(shutdowns.load(Ordering::SeqCst), 2);
        runtime.shutdown_runtime_blocking().expect("shutdown");
    }

    #[test]
    fn cancelling_the_calling_owner_stops_check_all_before_the_endpoint_timeout() {
        let runtime = runtime();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (shutdown_tx, shutdown_rx) = std::sync::mpsc::channel();
        let provider = GpuiAdminProvider::with_factory(
            runtime.root_context().component("provider"),
            Arc::new(BlockingFactory {
                entered: entered_tx,
                shutdown: shutdown_tx,
            }),
            DesktopAuthState::new(Arc::new(MapEnvironment::new([]))),
            Arc::new(FixedClock),
        );
        let work = runtime.root_context().component("application-work");
        let work_cancellation = work.task_spawner().cancellation_token();
        let request_provider = Arc::clone(&provider);
        work.spawn("check-all-caller", TaskKind::Other, async move {
            tokio::select! {
                biased;
                _ = work_cancellation.cancelled() => {}
                _ = request_provider.check_endpoints(vec![
                    snapshot(1, ConnectionScope::NameServer),
                    snapshot(1, ConnectionScope::NameServer),
                ]) => {}
            }
        })
        .expect("caller task");

        entered_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("first endpoint entered health");
        runtime.block_on(async {
            work.task_group().cancel();
            let report = work.task_group().shutdown(std::time::Duration::from_secs(1)).await;
            assert_eq!(report.timed_out, 0);
            provider.shutdown().await;
        });
        shutdown_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("ephemeral session shutdown completed");
        assert!(entered_rx.try_recv().is_err(), "second endpoint must not start");
        runtime.shutdown_runtime_blocking().expect("shutdown");
    }
}
