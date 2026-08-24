// Copyright 2026 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Application services coordinating persistence, shell state, provider replacement, and health.

#[path = "services/brokers.rs"]
pub mod brokers;
#[path = "services/consumers.rs"]
pub(crate) mod consumers;
#[path = "services/dashboard.rs"]
pub mod dashboard;
#[path = "services/delivery03.rs"]
pub(crate) mod delivery03;
#[path = "services/topics.rs"]
pub(crate) mod topics;

use std::{
    fmt,
    future::Future,
    io,
    path::{Path, PathBuf},
    pin::Pin,
    process::Command,
    sync::Arc,
};

use rocketmq_dashboard_common::{
    AdminSessionStatus, AdminSessionSummary, ConnectionScope, CredentialSourceKind, EndpointHealth, HistoryRetention,
    TransportSettings, add_endpoint, normalize_nameserver_address, normalize_proxy_address, remove_endpoint,
    switch_endpoint,
};
use rocketmq_runtime::{ChildServiceContext, TaskKind};
use tokio_util::sync::CancellationToken;

use crate::{
    infrastructure::{
        admin_provider::{GpuiAdminProvider, ProviderError},
        auth_state::DesktopAuthState,
        config_store::{DesktopConfig, DesktopConfigStore},
        history_collector::{HistoryLifecycle, HistorySampler},
        history_store::HistoryStore,
        monitor_store::{MonitorLifecycle, MonitorStore},
    },
    route::AppRoute,
    state::{UiError, UiErrorCode},
};

type ServiceFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Clone)]
struct RuntimeBridge {
    context: ChildServiceContext,
    #[cfg(test)]
    completion: Option<std::sync::mpsc::Sender<&'static str>>,
}

struct CancelOnDrop(CancellationToken);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

impl RuntimeBridge {
    async fn run<T>(
        &self,
        name: &'static str,
        future: impl Future<Output = Result<T, UiError>> + Send + 'static,
    ) -> Result<T, UiError>
    where
        T: Send + 'static,
    {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let request_cancellation = CancellationToken::new();
        let _cancel_on_drop = CancelOnDrop(request_cancellation.clone());
        let owner_cancellation = self.context.task_spawner().cancellation_token();
        #[cfg(test)]
        let completion = self.completion.clone();
        self.context
            .spawn(name, TaskKind::Other, async move {
                let result = tokio::select! {
                    biased;
                    _ = owner_cancellation.cancelled() => Err(runtime_cancelled()),
                    _ = request_cancellation.cancelled() => Err(runtime_cancelled()),
                    result = future => result,
                };
                let _ = sender.send(result);
                #[cfg(test)]
                if let Some(completion) = completion {
                    // Test observers are released only after the GPUI-facing result is ready.
                    let _ = completion.send(name);
                }
            })
            .map_err(|_| runtime_unavailable())?;
        receiver.await.map_err(|_| runtime_unavailable())?
    }
}

fn runtime_cancelled() -> UiError {
    UiError::new("The dashboard operation was cancelled.", UiErrorCode::Connection, true)
}

fn runtime_unavailable() -> UiError {
    UiError::new(
        "The dashboard runtime is shutting down.",
        UiErrorCode::Connection,
        false,
    )
}

/// Result of reading startup configuration and safe session metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StartupSnapshot {
    /// Revision captured by the completed startup attempt.
    pub configuration_revision: u64,
    /// Whether the user must authenticate before reaching the shell.
    pub login_required: bool,
    /// Whether the in-memory local session is valid.
    pub has_valid_session: bool,
}

impl StartupSnapshot {
    /// Chooses Login or Dashboard without exposing a session value.
    pub const fn destination(&self) -> AppRoute {
        if self.login_required && !self.has_valid_session {
            AppRoute::Login
        } else {
            AppRoute::Dashboard
        }
    }
}

/// A non-secret local session marker and optional display username.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct SessionState {
    username: Option<String>,
}

impl fmt::Debug for SessionState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionState")
            .field("authenticated", &self.is_authenticated())
            .field("username_available", &self.username.is_some())
            .finish()
    }
}

impl SessionState {
    /// Creates a compatibility marker for injected D1 tests.
    pub fn authenticated() -> Self {
        Self {
            username: Some("Authenticated".into()),
        }
    }

    /// Creates a session for a validated environment-backed username.
    pub fn for_username(username: String) -> Self {
        Self {
            username: Some(username),
        }
    }

    /// Creates a signed-out marker.
    pub const fn signed_out() -> Self {
        Self { username: None }
    }

    /// Returns whether protected navigation is admitted.
    pub fn is_authenticated(&self) -> bool {
        self.username.is_some()
    }

    /// Clears the complete marker.
    pub fn clear(&mut self) {
        self.username = None;
    }
}

/// Safe shell snapshot updated before provider replacement begins.
#[derive(Clone, PartialEq, Eq)]
pub struct GlobalConnectionState {
    /// Last persisted non-sensitive configuration.
    pub config: DesktopConfig,
    /// Current Admin session lifecycle status.
    pub session: AdminSessionSummary,
    /// Latest accepted health result.
    pub health: Option<EndpointHealth>,
    /// Monotonic invalidation count for all feature request epochs.
    pub request_invalidation: u64,
    /// Monotonic sensitive-state clear generation.
    pub sensitive_clear_generation: u64,
}

impl fmt::Debug for GlobalConnectionState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GlobalConnectionState")
            .field("config", &self.config)
            .field("session", &self.session)
            .field("health_available", &self.health.is_some())
            .field("request_invalidation", &self.request_invalidation)
            .field("sensitive_clear_generation", &self.sensitive_clear_generation)
            .finish()
    }
}

impl Default for GlobalConnectionState {
    fn default() -> Self {
        Self {
            config: DesktopConfig::default(),
            session: AdminSessionSummary {
                revision: 0,
                status: AdminSessionStatus::NotConfigured,
                credential_source: CredentialSourceKind::None,
            },
            health: None,
            request_invalidation: 0,
            sensitive_clear_generation: 0,
        }
    }
}

impl GlobalConnectionState {
    fn persisted(&mut self, config: DesktopConfig) {
        self.config = config;
        self.health = None;
        self.session = AdminSessionSummary {
            revision: self.config.revision,
            status: if self.config.current_nameserver.is_some() || self.config.current_proxy.is_some() {
                AdminSessionStatus::Connecting
            } else {
                AdminSessionStatus::NotConfigured
            },
            credential_source: self.config.auth.credential_source,
        };
    }

    fn provider_switched(&mut self, session: AdminSessionSummary) {
        if session.revision == self.config.revision {
            self.session = session;
        }
    }

    fn provider_failed(&mut self) {
        self.session = AdminSessionSummary {
            revision: self.config.revision,
            status: AdminSessionStatus::Failed,
            credential_source: self.config.auth.credential_source,
        };
    }

    fn invalidate_old_scope(&mut self) {
        self.request_invalidation = self.request_invalidation.saturating_add(1);
        self.sensitive_clear_generation = self.sensitive_clear_generation.saturating_add(1);
    }

    fn apply_health(&mut self, health: EndpointHealth) -> bool {
        if health.revision != self.config.revision {
            return false;
        }
        self.health = Some(health);
        true
    }
}

/// Typed configuration mutation. No write operation is replayed after it completes.
#[derive(Clone, PartialEq, Eq)]
pub enum ConfigMutation {
    /// Adds a NameServer, selecting it only when the list was empty.
    AddNameServer(String),
    /// Selects an existing NameServer.
    SwitchNameServer(String),
    /// Removes a NameServer with an optional explicit replacement.
    RemoveNameServer {
        /// Removed address.
        address: String,
        /// Required when removing the active endpoint.
        replacement: Option<String>,
    },
    /// Saves TLS/VIP settings; provider failure triggers a persisted rollback revision.
    SaveTransport(TransportSettings),
    /// Adds a Proxy, selecting it only when the list was empty.
    AddProxy(String),
    /// Selects an existing Proxy and enables Proxy scope.
    SwitchProxy(String),
    /// Removes a Proxy with replacement or explicit NameServer fallback.
    RemoveProxy {
        /// Removed address.
        address: String,
        /// Optional configured replacement.
        replacement: Option<String>,
        /// Explicitly returns the query scope to NameServer.
        fallback_to_nameserver: bool,
    },
    /// Enables or disables local sign-in.
    SetAuthEnabled(bool),
    /// Chooses where RocketMQ Admin credentials are resolved.
    SetCredentialSource(CredentialSourceKind),
    /// Reloads from disk without persisting or replaying a mutation.
    Reload,
}

impl ConfigMutation {
    fn route_transition(&self, before_auth: bool, after_auth: bool) -> ConfigRouteTransition {
        match (before_auth, after_auth) {
            (false, true) => ConfigRouteTransition::AuthenticationEnabled,
            (true, false) => ConfigRouteTransition::AuthenticationDisabled,
            (false, false) | (true, true) => ConfigRouteTransition::None,
        }
    }
}

impl fmt::Debug for ConfigMutation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::AddNameServer(_) => "AddNameServer",
            Self::SwitchNameServer(_) => "SwitchNameServer",
            Self::RemoveNameServer { .. } => "RemoveNameServer",
            Self::SaveTransport(_) => "SaveTransport",
            Self::AddProxy(_) => "AddProxy",
            Self::SwitchProxy(_) => "SwitchProxy",
            Self::RemoveProxy { .. } => "RemoveProxy",
            Self::SetAuthEnabled(_) => "SetAuthEnabled",
            Self::SetCredentialSource(_) => "SetCredentialSource",
            Self::Reload => "Reload",
        };
        formatter
            .debug_struct("ConfigMutation")
            .field("kind", &kind)
            .finish_non_exhaustive()
    }
}

/// Result of the ordered ConfigUpdated pipeline.
#[derive(Clone, PartialEq, Eq)]
pub struct ConfigUpdate {
    /// Persisted configuration after the operation.
    pub config: DesktopConfig,
    /// Safe provider replacement warning, if the configuration remains persisted.
    pub connection_warning: Option<UiError>,
}

impl fmt::Debug for ConfigUpdate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConfigUpdate")
            .field("config", &self.config)
            .field("connection_warning", &self.connection_warning)
            .finish()
    }
}

/// Observable stages of the ordered configuration pipeline.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfigUpdatePhase {
    /// Persistence and revision advancement completed; the shell now shows Connecting.
    Persisted,
    /// Provider replacement completed or failed with a safe session state.
    ProviderSwitched,
    /// Old request epochs and scope-sensitive state were invalidated.
    Invalidated,
    /// The read-only health refresh completed or was skipped after provider failure.
    HealthRefreshed,
    /// The requested configuration is the final persisted state.
    Completed,
    /// A failed transport update was persisted and then rolled back at a second revision.
    RolledBack,
}

/// Explicit shell navigation boundary carried by configuration progress.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ConfigRouteTransition {
    /// The mutation must preserve the current route and navigation history.
    #[default]
    None,
    /// Authentication changed from disabled to enabled and requires Login.
    AuthenticationEnabled,
    /// Authentication changed from enabled to disabled and admits the main shell.
    AuthenticationDisabled,
}

/// Strongly typed progress event sent from owned Tokio work back to GPUI.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConfigUpdated {
    /// Persisted revision visible to the shell and provider.
    pub revision: u64,
    /// The stage whose state is now observable through [`AppServices::connection_state`].
    pub phase: ConfigUpdatePhase,
    /// Explicit route boundary; ordinary connection mutations preserve navigation.
    pub route_transition: ConfigRouteTransition,
}

type ConfigProgressSender = tokio::sync::mpsc::UnboundedSender<ConfigUpdated>;

trait ConnectionProvider: Send + Sync {
    fn switch(
        &self,
        snapshot: rocketmq_dashboard_common::ConnectionSnapshot,
    ) -> ServiceFuture<'_, Result<AdminSessionSummary, ProviderError>>;
    fn check_health(&self) -> ServiceFuture<'_, Result<EndpointHealth, ProviderError>>;
    fn check_endpoints(
        &self,
        snapshots: Vec<rocketmq_dashboard_common::ConnectionSnapshot>,
    ) -> ServiceFuture<'_, Result<Vec<EndpointHealth>, ProviderError>>;
}

struct RealConnectionProvider(Arc<GpuiAdminProvider>);

impl ConnectionProvider for RealConnectionProvider {
    fn switch(
        &self,
        snapshot: rocketmq_dashboard_common::ConnectionSnapshot,
    ) -> ServiceFuture<'_, Result<AdminSessionSummary, ProviderError>> {
        Box::pin(self.0.switch(snapshot))
    }

    fn check_health(&self) -> ServiceFuture<'_, Result<EndpointHealth, ProviderError>> {
        Box::pin(self.0.check_health())
    }

    fn check_endpoints(
        &self,
        snapshots: Vec<rocketmq_dashboard_common::ConnectionSnapshot>,
    ) -> ServiceFuture<'_, Result<Vec<EndpointHealth>, ProviderError>> {
        Box::pin(self.0.check_endpoints(snapshots))
    }
}

#[cfg(test)]
struct ProductFakeConnectionProvider;

#[cfg(test)]
impl ConnectionProvider for ProductFakeConnectionProvider {
    fn switch(
        &self,
        snapshot: rocketmq_dashboard_common::ConnectionSnapshot,
    ) -> ServiceFuture<'_, Result<AdminSessionSummary, ProviderError>> {
        let configured = snapshot.nameserver.is_some() || snapshot.proxy.is_some();
        Box::pin(std::future::ready(Ok(AdminSessionSummary {
            revision: snapshot.revision,
            status: if configured {
                AdminSessionStatus::Connected
            } else {
                AdminSessionStatus::NotConfigured
            },
            credential_source: snapshot.credential_source,
        })))
    }

    fn check_health(&self) -> ServiceFuture<'_, Result<EndpointHealth, ProviderError>> {
        Box::pin(std::future::ready(Ok(EndpointHealth {
            endpoint: String::new(),
            revision: 0,
            availability: Default::default(),
            checked_at_epoch_ms: None,
            failure_summary: None,
        })))
    }

    fn check_endpoints(
        &self,
        snapshots: Vec<rocketmq_dashboard_common::ConnectionSnapshot>,
    ) -> ServiceFuture<'_, Result<Vec<EndpointHealth>, ProviderError>> {
        Box::pin(std::future::ready(Ok(snapshots
            .into_iter()
            .map(|snapshot| EndpointHealth {
                endpoint: snapshot.nameserver.unwrap_or_default(),
                revision: snapshot.revision,
                availability: Default::default(),
                checked_at_epoch_ms: None,
                failure_summary: None,
            })
            .collect())))
    }
}

trait ApplicationBackend: Send + Sync {
    fn bootstrap(&self) -> ServiceFuture<'_, Result<StartupSnapshot, UiError>>;
    fn authenticate<'a>(
        &'a self,
        username: &'a str,
        password: &'a str,
    ) -> ServiceFuture<'a, Result<SessionState, UiError>>;
    fn sign_out(&self) -> ServiceFuture<'_, Result<(), UiError>>;
    fn mutate(
        &self,
        mutation: ConfigMutation,
        progress: ConfigProgressSender,
    ) -> ServiceFuture<'_, Result<ConfigUpdate, UiError>>;
    fn check_all_nameservers(&self) -> ServiceFuture<'_, Result<Vec<EndpointHealth>, UiError>>;
    fn connection_state(&self) -> GlobalConnectionState;
    fn config_path(&self) -> Option<PathBuf>;
    fn open_config_location(&self) -> ServiceFuture<'_, Result<(), UiError>>;
}

/// Cloneable service aggregation shared by the shell and feature entities.
#[derive(Clone)]
pub struct AppServices {
    backend: Arc<dyn ApplicationBackend>,
    runtime_bridge: Option<RuntimeBridge>,
    delivery03: Arc<dyn delivery03::Delivery03Backend>,
    consumers: Arc<dyn consumers::ConsumerBackend>,
    topics: Arc<dyn topics::TopicBackend>,
}

impl AppServices {
    /// Preserves the narrow D1 injection surface for focused shell tests.
    pub fn new(startup: Arc<dyn StartupService>, config: Arc<dyn ConfigService>, auth: Arc<dyn AuthService>) -> Self {
        Self {
            backend: Arc::new(LegacyBackend { startup, config, auth }),
            runtime_bridge: None,
            delivery03: delivery03::RealDelivery03Backend::new(
                dashboard::DashboardService::unavailable(),
                brokers::BrokerService::unavailable(),
            ),
            consumers: consumers::RealConsumerBackend::unavailable(),
            topics: topics::RealTopicBackend::unavailable(),
        }
    }

    /// Creates the full Delivery 02 service pipeline.
    pub fn desktop(
        store: Arc<DesktopConfigStore>,
        provider: Arc<GpuiAdminProvider>,
        auth: Arc<DesktopAuthState>,
        runtime_context: ChildServiceContext,
        history_context: ChildServiceContext,
        monitor_context: ChildServiceContext,
    ) -> Self {
        Self::desktop_inner(
            store,
            Arc::new(RealConnectionProvider(Arc::clone(&provider))),
            Some(provider),
            auth,
            runtime_context,
            history_context,
            monitor_context,
        )
    }

    fn desktop_inner(
        store: Arc<DesktopConfigStore>,
        provider: Arc<dyn ConnectionProvider>,
        admin_provider: Option<Arc<GpuiAdminProvider>>,
        auth: Arc<DesktopAuthState>,
        runtime_context: ChildServiceContext,
        history_context: ChildServiceContext,
        monitor_context: ChildServiceContext,
    ) -> Self {
        let host_context = runtime_context.clone();
        let history_store = HistoryStore::new(store.path().with_file_name("history.json"), history_context.clone());
        let monitor_store = MonitorStore::new(store.path().with_file_name("monitors.json"), monitor_context.clone());
        let dashboard = admin_provider
            .as_ref()
            .map(|provider| dashboard::DashboardService::new(Arc::clone(provider), Arc::clone(&history_store)))
            .unwrap_or_else(dashboard::DashboardService::unavailable);
        let brokers = admin_provider
            .as_ref()
            .map(|provider| brokers::BrokerService::new(Arc::clone(provider)))
            .unwrap_or_else(brokers::BrokerService::unavailable);
        let consumers = admin_provider
            .as_ref()
            .map(|provider| consumers::RealConsumerBackend::new(Arc::clone(provider)))
            .unwrap_or_else(consumers::RealConsumerBackend::unavailable);
        let topics = admin_provider
            .map(topics::RealTopicBackend::new)
            .unwrap_or_else(topics::RealTopicBackend::unavailable);
        let history_sampler: Arc<dyn HistorySampler> = dashboard.clone();
        let delivery03 = delivery03::RealDelivery03Backend::new(dashboard, brokers);
        Self {
            backend: Arc::new(DesktopBackend {
                store,
                provider,
                auth,
                state: parking_lot::RwLock::new(GlobalConnectionState::default()),
                update_gate: tokio::sync::Mutex::new(()),
                history_context,
                monitor_context,
                host_context,
                history_store,
                history_sampler,
                monitor_store,
                history_lifecycle: parking_lot::Mutex::new(None),
                monitor_lifecycle: parking_lot::Mutex::new(None),
            }),
            runtime_bridge: Some(RuntimeBridge {
                context: runtime_context,
                #[cfg(test)]
                completion: None,
            }),
            delivery03,
            consumers,
            topics,
        }
    }

    /// Creates the real store/auth/runtime pipeline with a deterministic no-network provider.
    #[cfg(test)]
    pub fn desktop_with_fake_provider(
        store: Arc<DesktopConfigStore>,
        auth: Arc<DesktopAuthState>,
        runtime_context: ChildServiceContext,
        history_context: ChildServiceContext,
        monitor_context: ChildServiceContext,
    ) -> Self {
        Self::desktop_inner(
            store,
            Arc::new(ProductFakeConnectionProvider),
            None,
            auth,
            runtime_context,
            history_context,
            monitor_context,
        )
    }

    #[cfg(test)]
    pub fn with_runtime_completion(mut self, completion: std::sync::mpsc::Sender<&'static str>) -> Self {
        if let Some(runtime) = self.runtime_bridge.as_mut() {
            runtime.completion = Some(completion);
        }
        self
    }

    /// Performs owned startup work.
    pub async fn bootstrap(&self) -> Result<StartupSnapshot, UiError> {
        let Some(runtime) = self.runtime_bridge.as_ref() else {
            return self.backend.bootstrap().await;
        };
        let backend = Arc::clone(&self.backend);
        runtime
            .run("gpui-bootstrap", async move { backend.bootstrap().await })
            .await
    }

    /// Authenticates without retaining the supplied password.
    pub async fn authenticate(&self, username: &str, password: &str) -> Result<SessionState, UiError> {
        let Some(runtime) = self.runtime_bridge.as_ref() else {
            return self.backend.authenticate(username, password).await;
        };
        let backend = Arc::clone(&self.backend);
        let username = username.to_owned();
        let password = password.to_owned();
        runtime
            .run("gpui-authenticate", async move {
                backend.authenticate(&username, &password).await
            })
            .await
    }

    /// Clears the in-memory local session.
    pub async fn sign_out(&self) -> Result<(), UiError> {
        let Some(runtime) = self.runtime_bridge.as_ref() else {
            return self.backend.sign_out().await;
        };
        let backend = Arc::clone(&self.backend);
        runtime
            .run("gpui-sign-out", async move { backend.sign_out().await })
            .await
    }

    /// Runs one mutation while publishing each observable ordering stage.
    pub async fn mutate_with_progress(
        &self,
        mutation: ConfigMutation,
        progress: tokio::sync::mpsc::UnboundedSender<ConfigUpdated>,
    ) -> Result<ConfigUpdate, UiError> {
        let Some(runtime) = self.runtime_bridge.as_ref() else {
            return self.backend.mutate(mutation, progress).await;
        };
        let backend = Arc::clone(&self.backend);
        runtime
            .run("gpui-config-mutation", async move {
                backend.mutate(mutation, progress).await
            })
            .await
    }

    /// Checks every configured NameServer without changing the selected endpoint.
    pub async fn check_all_nameservers(&self) -> Result<Vec<EndpointHealth>, UiError> {
        let Some(runtime) = self.runtime_bridge.as_ref() else {
            return self.backend.check_all_nameservers().await;
        };
        let backend = Arc::clone(&self.backend);
        runtime
            .run("gpui-check-all-nameservers", async move {
                backend.check_all_nameservers().await
            })
            .await
    }

    /// Returns the current safe shell/config snapshot.
    pub fn connection_state(&self) -> GlobalConnectionState {
        self.backend.connection_state()
    }

    /// Returns the local configuration path when backed by the desktop store.
    pub fn config_path(&self) -> Option<PathBuf> {
        self.backend.config_path()
    }

    /// Compatibility intent used by the startup error page.
    pub async fn open_config_location(&self) -> Result<(), UiError> {
        let Some(runtime) = self.runtime_bridge.as_ref() else {
            return self.backend.open_config_location().await;
        };
        let backend = Arc::clone(&self.backend);
        runtime
            .run("gpui-open-config-location", async move {
                backend.open_config_location().await
            })
            .await
    }
}

impl Default for AppServices {
    fn default() -> Self {
        Self::new(
            Arc::new(DefaultStartupService),
            Arc::new(CapabilityUnavailableConfigService),
            Arc::new(CapabilityUnavailableAuthService),
        )
    }
}

struct DesktopBackend {
    store: Arc<DesktopConfigStore>,
    provider: Arc<dyn ConnectionProvider>,
    auth: Arc<DesktopAuthState>,
    state: parking_lot::RwLock<GlobalConnectionState>,
    update_gate: tokio::sync::Mutex<()>,
    history_context: ChildServiceContext,
    monitor_context: ChildServiceContext,
    host_context: ChildServiceContext,
    history_store: Arc<HistoryStore>,
    history_sampler: Arc<dyn HistorySampler>,
    monitor_store: Arc<MonitorStore>,
    history_lifecycle: parking_lot::Mutex<Option<HistoryLifecycle>>,
    monitor_lifecycle: parking_lot::Mutex<Option<MonitorLifecycle>>,
}

impl DesktopBackend {
    async fn reconcile_foundations(
        &self,
        foundations: crate::infrastructure::config_store::FoundationFlags,
    ) -> Result<(), UiError> {
        let history_retention = HistoryRetention {
            max_points_per_series: foundations.history_max_points_per_series,
            max_series: foundations.history_max_series,
            max_total_points: foundations.history_max_total_points,
        };
        if foundations.history_enabled && foundations.history_interval_seconds > 0 {
            let existing = {
                let mut lifecycle = self.history_lifecycle.lock();
                if lifecycle.as_ref().is_some_and(|lifecycle| {
                    lifecycle.matches_settings(foundations.history_interval_seconds, history_retention)
                }) {
                    None
                } else {
                    lifecycle.take()
                }
            };
            if let Some(mut lifecycle) = existing
                && !lifecycle.stop().await
            {
                return Err(UiError::new(
                    "The History lifecycle did not stop cleanly.",
                    UiErrorCode::Configuration,
                    true,
                ));
            }
            if self.history_lifecycle.lock().is_none() {
                self.history_store.points().await.map_err(config_ui_error)?;
                let lifecycle = HistoryLifecycle::start(
                    &self.history_context,
                    foundations.history_interval_seconds,
                    history_retention,
                    Arc::clone(&self.history_store),
                    Arc::clone(&self.history_sampler),
                )
                .map_err(config_ui_error)?;
                if !lifecycle.is_started() {
                    return Err(UiError::new(
                        "The History lifecycle did not start.",
                        UiErrorCode::Configuration,
                        true,
                    ));
                }
                *self.history_lifecycle.lock() = Some(lifecycle);
            }
        } else {
            let lifecycle = self.history_lifecycle.lock().take();
            if let Some(mut lifecycle) = lifecycle
                && !lifecycle.stop().await
            {
                return Err(UiError::new(
                    "The History lifecycle did not stop cleanly.",
                    UiErrorCode::Configuration,
                    true,
                ));
            }
        }

        if foundations.monitor_enabled {
            let needs_start = self.monitor_lifecycle.lock().is_none();
            if needs_start {
                self.monitor_store.list().await.map_err(config_ui_error)?;
                let lifecycle = MonitorLifecycle::start(&self.monitor_context, true).map_err(config_ui_error)?;
                if !lifecycle.is_started() {
                    return Err(UiError::new(
                        "The Monitor lifecycle did not start.",
                        UiErrorCode::Configuration,
                        true,
                    ));
                }
                *self.monitor_lifecycle.lock() = Some(lifecycle);
            }
        } else {
            let lifecycle = self.monitor_lifecycle.lock().take();
            if let Some(mut lifecycle) = lifecycle
                && !lifecycle.stop().await
            {
                return Err(UiError::new(
                    "The Monitor lifecycle did not stop before its deadline.",
                    UiErrorCode::Configuration,
                    true,
                ));
            }
        }
        Ok(())
    }

    async fn install_persisted(
        &self,
        config: DesktopConfig,
        progress: Option<&ConfigProgressSender>,
        route_transition: ConfigRouteTransition,
    ) -> Option<UiError> {
        // ConfigUpdated ordering: persisted shell summary, provider, invalidation/clear, read-only health.
        self.state.write().persisted(config.clone());
        send_progress(
            progress,
            config.revision,
            ConfigUpdatePhase::Persisted,
            route_transition,
        );
        let session = self.provider.switch(config.connection_snapshot()).await;
        let warning = match session {
            Ok(session) => {
                self.state.write().provider_switched(session);
                None
            }
            Err(error) => {
                self.state.write().provider_failed();
                Some(provider_ui_error(error))
            }
        };
        send_progress(
            progress,
            config.revision,
            ConfigUpdatePhase::ProviderSwitched,
            route_transition,
        );
        self.state.write().invalidate_old_scope();
        send_progress(
            progress,
            config.revision,
            ConfigUpdatePhase::Invalidated,
            route_transition,
        );
        if warning.is_none()
            && let Ok(health) = self.provider.check_health().await
        {
            self.state.write().apply_health(health);
        }
        send_progress(
            progress,
            config.revision,
            ConfigUpdatePhase::HealthRefreshed,
            route_transition,
        );
        warning
    }

    async fn rollback_transport(
        &self,
        old: DesktopConfig,
        failed: UiError,
        progress: &ConfigProgressSender,
    ) -> Result<ConfigUpdate, UiError> {
        let current_revision = self.state.read().config.revision;
        let mut rollback = old;
        rollback.revision = current_revision;
        let rollback = self.store.save_next(rollback).await.map_err(config_ui_error)?;
        let warning = self
            .install_persisted(rollback.clone(), Some(progress), ConfigRouteTransition::None)
            .await;
        if warning.is_some() {
            return Err(UiError::new(
                "Transport update failed and the previous session could not be restored.",
                UiErrorCode::Connection,
                true,
            ));
        }
        send_progress(
            Some(progress),
            rollback.revision,
            ConfigUpdatePhase::RolledBack,
            ConfigRouteTransition::None,
        );
        Err(failed)
    }
}

impl ApplicationBackend for DesktopBackend {
    fn bootstrap(&self) -> ServiceFuture<'_, Result<StartupSnapshot, UiError>> {
        Box::pin(async move {
            let config = self.store.load().await.map_err(config_ui_error)?;
            self.auth
                .validate_startup(&config.auth)
                .map_err(|error| UiError::new(error.to_string(), UiErrorCode::Authentication, true))?;
            if self
                .install_persisted(config.clone(), None, ConfigRouteTransition::None)
                .await
                .is_some()
            {
                self.state.write().session.status = AdminSessionStatus::Failed;
                tracing::warn!(
                    error_code = "connection_unavailable",
                    "initial Admin session is unavailable"
                );
            }
            self.reconcile_foundations(config.foundations).await?;
            Ok(StartupSnapshot {
                configuration_revision: config.revision,
                login_required: config.auth.enabled,
                has_valid_session: self.auth.session().is_authenticated(),
            })
        })
    }

    fn authenticate<'a>(
        &'a self,
        username: &'a str,
        password: &'a str,
    ) -> ServiceFuture<'a, Result<SessionState, UiError>> {
        Box::pin(async move {
            self.auth.authenticate(username, password).map_or_else(
                |error| {
                    let retryable = matches!(
                        error,
                        crate::infrastructure::auth_state::AuthStateError::MissingEnvironment { .. }
                    );
                    Err(UiError::new(error.to_string(), UiErrorCode::Authentication, retryable))
                },
                |session| {
                    Ok(SessionState::for_username(
                        session.username().unwrap_or_default().to_owned(),
                    ))
                },
            )
        })
    }

    fn sign_out(&self) -> ServiceFuture<'_, Result<(), UiError>> {
        Box::pin(async move {
            self.auth.sign_out();
            Ok(())
        })
    }

    fn mutate(
        &self,
        mutation: ConfigMutation,
        progress: ConfigProgressSender,
    ) -> ServiceFuture<'_, Result<ConfigUpdate, UiError>> {
        Box::pin(async move {
            let _update = self.update_gate.lock().await;
            if mutation == ConfigMutation::Reload {
                let before_auth = self.state.read().config.auth.enabled;
                let config = self.store.load().await.map_err(config_ui_error)?;
                let route_transition = mutation.route_transition(before_auth, config.auth.enabled);
                let warning = self
                    .install_persisted(config.clone(), Some(&progress), route_transition)
                    .await;
                self.reconcile_foundations(config.foundations).await?;
                send_progress(
                    Some(&progress),
                    config.revision,
                    ConfigUpdatePhase::Completed,
                    route_transition,
                );
                return Ok(ConfigUpdate {
                    config,
                    connection_warning: warning,
                });
            }
            let old = self.state.read().config.clone();
            let mut next = old.clone();
            apply_mutation(&mut next, &mutation)?;
            let route_transition = mutation.route_transition(old.auth.enabled, next.auth.enabled);
            if matches!(
                mutation,
                ConfigMutation::SetAuthEnabled(true) | ConfigMutation::SetCredentialSource(_)
            ) {
                self.auth
                    .validate_startup(&next.auth)
                    .map_err(|error| UiError::new(error.to_string(), UiErrorCode::Authentication, true))?;
            }
            let saved = self.store.save_next(next).await.map_err(config_ui_error)?;
            let warning = self
                .install_persisted(saved.clone(), Some(&progress), route_transition)
                .await;
            self.reconcile_foundations(saved.foundations).await?;
            if matches!(mutation, ConfigMutation::SaveTransport(_))
                && let Some(error) = warning
            {
                return self.rollback_transport(old, error, &progress).await;
            }
            if mutation == ConfigMutation::SetAuthEnabled(false) {
                self.auth.sign_out();
            }
            send_progress(
                Some(&progress),
                saved.revision,
                ConfigUpdatePhase::Completed,
                route_transition,
            );
            Ok(ConfigUpdate {
                config: saved,
                connection_warning: warning,
            })
        })
    }

    fn check_all_nameservers(&self) -> ServiceFuture<'_, Result<Vec<EndpointHealth>, UiError>> {
        Box::pin(async move {
            let config = self.state.read().config.clone();
            let snapshots = config
                .nameservers
                .iter()
                .map(|endpoint| {
                    let mut snapshot = config.connection_snapshot();
                    snapshot.nameserver = Some(endpoint.clone());
                    snapshot.scope = ConnectionScope::NameServer;
                    snapshot.proxy = None;
                    snapshot
                })
                .collect();
            self.provider
                .check_endpoints(snapshots)
                .await
                .map_err(provider_ui_error)
        })
    }

    fn connection_state(&self) -> GlobalConnectionState {
        self.state.read().clone()
    }

    fn config_path(&self) -> Option<PathBuf> {
        Some(self.store.path().to_path_buf())
    }

    fn open_config_location(&self) -> ServiceFuture<'_, Result<(), UiError>> {
        Box::pin(async move {
            let path = self.store.path().to_path_buf();
            self.host_context
                .storage_io()
                .spawn_io("gpui-open-config-location", move || open_platform_location(&path))
                .await
                .map_err(|error| UiError::new(error.to_string(), UiErrorCode::Configuration, true))?
                .map_err(|error| {
                    UiError::new(
                        format!("Unable to open the configuration location: {error}"),
                        UiErrorCode::Configuration,
                        true,
                    )
                })
        })
    }
}

fn send_progress(
    progress: Option<&ConfigProgressSender>,
    revision: u64,
    phase: ConfigUpdatePhase,
    route_transition: ConfigRouteTransition,
) {
    if let Some(progress) = progress {
        let _ = progress.send(ConfigUpdated {
            revision,
            phase,
            route_transition,
        });
    }
}

fn open_platform_location(path: &Path) -> io::Result<()> {
    let existing = if path.exists() {
        path
    } else {
        path.parent().unwrap_or(path)
    };
    #[cfg(target_os = "windows")]
    let mut command = {
        let mut command = Command::new("explorer.exe");
        if path.exists() {
            command.arg(format!("/select,{}", existing.display()));
        } else {
            command.arg(existing);
        }
        command
    };
    #[cfg(target_os = "macos")]
    let mut command = {
        let mut command = Command::new("open");
        if path.exists() {
            command.arg("-R");
        }
        command.arg(existing);
        command
    };
    #[cfg(all(unix, not(target_os = "macos")))]
    let mut command = {
        let mut command = Command::new("xdg-open");
        command.arg(existing);
        command
    };
    let status = command.status()?;
    if status.success() {
        Ok(())
    } else {
        Err(io::Error::other("platform file browser returned a failure status"))
    }
}

fn apply_mutation(config: &mut DesktopConfig, mutation: &ConfigMutation) -> Result<(), UiError> {
    let result = match mutation {
        ConfigMutation::AddNameServer(address) => add_endpoint(
            &mut config.nameservers,
            &mut config.current_nameserver,
            address,
            normalize_nameserver_address,
        ),
        ConfigMutation::SwitchNameServer(address) => switch_endpoint(
            &config.nameservers,
            &mut config.current_nameserver,
            address,
            normalize_nameserver_address,
        ),
        ConfigMutation::RemoveNameServer { address, replacement } => remove_endpoint(
            &mut config.nameservers,
            &mut config.current_nameserver,
            address,
            replacement.as_deref(),
            false,
            normalize_nameserver_address,
        ),
        ConfigMutation::SaveTransport(transport) => {
            config.transport = *transport;
            Ok(())
        }
        ConfigMutation::AddProxy(address) => add_endpoint(
            &mut config.proxies,
            &mut config.current_proxy,
            address,
            normalize_proxy_address,
        ),
        ConfigMutation::SwitchProxy(address) => {
            let result = switch_endpoint(
                &config.proxies,
                &mut config.current_proxy,
                address,
                normalize_proxy_address,
            );
            if result.is_ok() {
                config.scope = ConnectionScope::Proxy;
            }
            result
        }
        ConfigMutation::RemoveProxy {
            address,
            replacement,
            fallback_to_nameserver,
        } => {
            if *fallback_to_nameserver && config.current_nameserver.is_none() {
                return Err(UiError::new(
                    "NameServer fallback requires a selected NameServer endpoint.",
                    UiErrorCode::Validation,
                    false,
                ));
            }
            let result = remove_endpoint(
                &mut config.proxies,
                &mut config.current_proxy,
                address,
                replacement.as_deref(),
                *fallback_to_nameserver || config.scope != ConnectionScope::Proxy,
                normalize_proxy_address,
            );
            if result.is_ok() && *fallback_to_nameserver {
                config.scope = ConnectionScope::NameServer;
            }
            result
        }
        ConfigMutation::SetAuthEnabled(enabled) => {
            config.auth.enabled = *enabled;
            Ok(())
        }
        ConfigMutation::SetCredentialSource(source) => {
            config.auth.credential_source = *source;
            Ok(())
        }
        ConfigMutation::Reload => Ok(()),
    };
    result.map_err(|error| UiError::new(error.to_string(), UiErrorCode::Validation, false))
}

fn config_ui_error(error: impl std::fmt::Display) -> UiError {
    UiError::new(error.to_string(), UiErrorCode::Configuration, true)
}

fn provider_ui_error(error: ProviderError) -> UiError {
    let retryable = error.is_retryable();
    let code = match error.code() {
        crate::infrastructure::admin_provider::ProviderErrorCode::Authentication => UiErrorCode::Authentication,
        crate::infrastructure::admin_provider::ProviderErrorCode::NotConfigured => UiErrorCode::Configuration,
        crate::infrastructure::admin_provider::ProviderErrorCode::Unavailable
        | crate::infrastructure::admin_provider::ProviderErrorCode::Cancelled
        | crate::infrastructure::admin_provider::ProviderErrorCode::StaleRevision
        | crate::infrastructure::admin_provider::ProviderErrorCode::Runtime => UiErrorCode::Connection,
    };
    UiError::new(error.to_string(), code, retryable)
}

/// D1 startup injection seam retained for focused shell tests.
pub trait StartupService: Send + Sync {
    /// Returns the safe startup decision.
    fn bootstrap(&self) -> Result<StartupSnapshot, UiError>;
}

/// D1 configuration intent seam retained for compatibility.
pub trait ConfigService: Send + Sync {
    /// Opens the configuration location if supported.
    fn open_config_location(&self) -> Result<(), UiError>;
}

/// D1 authentication seam retained for compatibility.
pub trait AuthService: Send + Sync {
    /// Authenticates without retaining supplied values.
    fn authenticate(&self, username: &str, password: &str) -> Result<SessionState, UiError>;
    /// Clears the local session.
    fn sign_out(&self) -> Result<(), UiError>;
}

struct LegacyBackend {
    startup: Arc<dyn StartupService>,
    config: Arc<dyn ConfigService>,
    auth: Arc<dyn AuthService>,
}

impl ApplicationBackend for LegacyBackend {
    fn bootstrap(&self) -> ServiceFuture<'_, Result<StartupSnapshot, UiError>> {
        Box::pin(std::future::ready(self.startup.bootstrap()))
    }

    fn authenticate<'a>(
        &'a self,
        username: &'a str,
        password: &'a str,
    ) -> ServiceFuture<'a, Result<SessionState, UiError>> {
        Box::pin(std::future::ready(self.auth.authenticate(username, password)))
    }

    fn sign_out(&self) -> ServiceFuture<'_, Result<(), UiError>> {
        Box::pin(std::future::ready(self.auth.sign_out()))
    }

    fn mutate(
        &self,
        _mutation: ConfigMutation,
        _progress: ConfigProgressSender,
    ) -> ServiceFuture<'_, Result<ConfigUpdate, UiError>> {
        Box::pin(std::future::ready(Err(capability_unavailable(
            "Configuration mutation",
        ))))
    }

    fn check_all_nameservers(&self) -> ServiceFuture<'_, Result<Vec<EndpointHealth>, UiError>> {
        Box::pin(std::future::ready(Ok(Vec::new())))
    }

    fn connection_state(&self) -> GlobalConnectionState {
        GlobalConnectionState::default()
    }

    fn config_path(&self) -> Option<PathBuf> {
        None
    }

    fn open_config_location(&self) -> ServiceFuture<'_, Result<(), UiError>> {
        Box::pin(std::future::ready(self.config.open_config_location()))
    }
}

struct DefaultStartupService;

impl StartupService for DefaultStartupService {
    fn bootstrap(&self) -> Result<StartupSnapshot, UiError> {
        Ok(StartupSnapshot {
            configuration_revision: 0,
            login_required: false,
            has_valid_session: false,
        })
    }
}

/// Deterministic startup fake.
#[cfg(test)]
#[derive(Clone)]
pub struct FakeStartupService {
    result: Result<StartupSnapshot, UiError>,
}

#[cfg(test)]
impl FakeStartupService {
    /// Creates a successful fake.
    pub fn ready(snapshot: StartupSnapshot) -> Self {
        Self { result: Ok(snapshot) }
    }
}

#[cfg(test)]
impl StartupService for FakeStartupService {
    fn bootstrap(&self) -> Result<StartupSnapshot, UiError> {
        self.result.clone()
    }
}

/// Deterministic authentication fake.
#[cfg(test)]
#[derive(Clone)]
pub struct FakeAuthService {
    result: Result<SessionState, UiError>,
}

#[cfg(test)]
impl FakeAuthService {
    /// Creates a successful fake.
    pub fn authenticated() -> Self {
        Self {
            result: Ok(SessionState::authenticated()),
        }
    }

    /// Creates a failing fake.
    pub fn failed(error: UiError) -> Self {
        Self { result: Err(error) }
    }
}

#[cfg(test)]
impl AuthService for FakeAuthService {
    fn authenticate(&self, _username: &str, _password: &str) -> Result<SessionState, UiError> {
        self.result.clone()
    }

    fn sign_out(&self) -> Result<(), UiError> {
        Ok(())
    }
}

/// Compatibility service with no host integration.
pub struct CapabilityUnavailableConfigService;

impl ConfigService for CapabilityUnavailableConfigService {
    fn open_config_location(&self) -> Result<(), UiError> {
        Err(capability_unavailable("Opening the configuration location"))
    }
}

/// Compatibility auth service with no backing source.
pub struct CapabilityUnavailableAuthService;

impl AuthService for CapabilityUnavailableAuthService {
    fn authenticate(&self, _username: &str, _password: &str) -> Result<SessionState, UiError> {
        Err(capability_unavailable("Authentication"))
    }

    fn sign_out(&self) -> Result<(), UiError> {
        Err(capability_unavailable("Sign out"))
    }
}

/// Creates a safe explicit signal for a missing capability.
pub fn capability_unavailable(capability: &str) -> UiError {
    UiError::new(
        format!("{capability} is not available in this delivery."),
        UiErrorCode::CapabilityUnavailable,
        false,
    )
}

#[cfg(test)]
#[path = "services/tests.rs"]
mod tests;
