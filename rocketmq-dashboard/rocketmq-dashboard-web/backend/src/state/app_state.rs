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
use crate::admin::DashboardAdminClient;
use crate::config::AppConfig;
use crate::error::DashboardError;
use crate::model::DashboardConfigView;
use crate::model::DashboardEnvironment;
use crate::model::PublishedEnvironment;
use crate::persistence::DashboardPersistence;
use crate::persistence::StorageHealth;
use crate::persistence::StorageStatus;
use crate::persistence::error::PersistenceError;
use crate::service::AuthState;
use crate::service::DashboardHistoryRuntime;
use crate::service::HistoryCollectorConfig;
use crate::service::SessionAuditCleanupRuntime;
use crate::service::start_dashboard_history_collector;
use crate::service::start_session_audit_cleanup;
use rocketmq_admin_core::client_adapter::ClientRuntime;
use rocketmq_dashboard_common::DashboardAdminFacade;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::sync::oneshot;
use tokio::time::MissedTickBehavior;

const ENVIRONMENT_CONVERGENCE_INTERVAL: Duration = Duration::from_secs(2);

pub type WebAdminFacade = DashboardAdminFacade<DashboardAdminClient>;

#[cfg(test)]
struct TestConfigPublishHook {
    persisted: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
    published: oneshot::Sender<()>,
}

#[cfg(test)]
struct TestPersistedMutationCompletionHook {
    reached: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
    completed: oneshot::Sender<()>,
}

#[derive(Clone)]
pub struct AppState {
    pub persistence: Arc<DashboardPersistence>,
    pub(crate) config_mutation_lock: Arc<Mutex<()>>,
    pub(crate) environment_refresh_lock: Arc<Mutex<()>>,
    persisted_mutation_context: rocketmq_runtime::ChildServiceContext,
    pub auth_state: Arc<AuthState>,
    pub history_runtime: DashboardHistoryRuntime,
    pub session_audit_cleanup_runtime: SessionAuditCleanupRuntime,
    pub published_environment: Arc<std::sync::RwLock<PublishedEnvironment>>,
    pub admin_client: DashboardAdminClient,
    #[cfg(test)]
    config_publish_hook: Arc<Mutex<Option<TestConfigPublishHook>>>,
    #[cfg(test)]
    persisted_mutation_completion_hook: Arc<Mutex<Option<TestPersistedMutationCompletionHook>>>,
}

impl AppState {
    pub async fn try_new(config: AppConfig, client_runtime: Arc<ClientRuntime>) -> Result<Self, DashboardError> {
        Self::try_new_inner(config, client_runtime, true).await
    }

    #[cfg(test)]
    pub(crate) async fn try_new_without_environment_convergence(
        config: AppConfig,
        client_runtime: Arc<ClientRuntime>,
    ) -> Result<Self, DashboardError> {
        Self::try_new_inner(config, client_runtime, false).await
    }

    async fn try_new_inner(
        config: AppConfig,
        client_runtime: Arc<ClientRuntime>,
        start_environment_convergence: bool,
    ) -> Result<Self, DashboardError> {
        let persistence = Arc::new(
            DashboardPersistence::initialize(&config.storage, client_runtime.component("dashboard-persistence"))
                .await?,
        );
        ensure_persistence_ready(persistence.storage_health().await)?;
        let session_audit_config = config.auth.clone();
        let auth_state = Arc::new(AuthState::new(config.auth)?);
        let environment = load_or_create_default_environment(&persistence, &config.initial_config).await?;
        let history_environment_id = environment.environment_id.clone();
        let published_environment = Arc::new(std::sync::RwLock::new(PublishedEnvironment::from_environment(
            environment,
            persistence.storage_backend(),
        )));
        let convergence_context = client_runtime.component("dashboard-environment-convergence");
        let persisted_mutation_context = client_runtime.component("dashboard-persisted-mutation-owner");
        let admin_client = DashboardAdminClient::new(
            published_environment.clone(),
            client_runtime.clone(),
            config.admin_credentials,
        );
        let history_runtime = DashboardHistoryRuntime::new(persistence.storage_backend());
        if config.dashboard_history_interval_secs > 0 {
            start_dashboard_history_collector(
                client_runtime.component("dashboard-history-collector"),
                persistence.clone(),
                DashboardAdminFacade::new(admin_client.clone()),
                history_environment_id,
                HistoryCollectorConfig {
                    interval_secs: config.dashboard_history_interval_secs,
                    retention_days: config.dashboard_history_retention_days,
                    retention_batch_size: config.dashboard_history_retention_batch_size,
                    lease_ttl_secs: config.dashboard_history_lease_ttl_secs,
                },
                history_runtime.clone(),
            )?;
        }
        let session_audit_cleanup_runtime = SessionAuditCleanupRuntime::new(&persistence);
        start_session_audit_cleanup(
            client_runtime.component("dashboard-session-audit-cleanup"),
            persistence.clone(),
            session_audit_config,
            session_audit_cleanup_runtime.clone(),
        )?;

        let state = Self {
            persistence,
            config_mutation_lock: Arc::new(Mutex::new(())),
            environment_refresh_lock: Arc::new(Mutex::new(())),
            persisted_mutation_context,
            auth_state,
            history_runtime,
            session_audit_cleanup_runtime,
            published_environment,
            admin_client,
            #[cfg(test)]
            config_publish_hook: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            persisted_mutation_completion_hook: Arc::new(Mutex::new(None)),
        };
        if start_environment_convergence {
            state.start_environment_convergence(convergence_context)?;
        }
        Ok(state)
    }

    pub fn admin_facade(&self) -> WebAdminFacade {
        DashboardAdminFacade::new(self.admin_client.clone())
    }

    pub fn published(&self) -> PublishedEnvironment {
        match self.published_environment.read() {
            Ok(published) => published.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }

    /// Publishes a durable aggregate only when it is not older than the
    /// currently visible revision. A delayed multi-node refresh must never
    /// roll admin consumers back after a local successful CAS.
    pub fn publish_environment(&self, environment: DashboardEnvironment) -> bool {
        let published = PublishedEnvironment::from_environment(environment, self.persistence.storage_backend());
        match self.published_environment.write() {
            Ok(mut current) => publish_if_current_or_newer(&mut current, published),
            Err(poisoned) => publish_if_current_or_newer(&mut poisoned.into_inner(), published),
        }
    }

    pub async fn refresh_default_environment(&self) -> Result<DashboardEnvironment, DashboardError> {
        refresh_published_default_environment(
            &self.persistence,
            &self.published_environment,
            &self.environment_refresh_lock,
        )
        .await
    }

    /// Runs an admitted candidate-to-persistence mutation under the dashboard
    /// service task group. Its future owns all post-admission work even when
    /// the HTTP handler drops its receiver during request cancellation.
    pub(crate) async fn run_persisted_mutation<T, F, Fut>(
        &self,
        name: &'static str,
        operation: F,
    ) -> Result<T, DashboardError>
    where
        T: Send + 'static,
        F: FnOnce(AppState) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, DashboardError>> + Send + 'static,
    {
        let (sender, receiver) = oneshot::channel();
        let state = self.clone();
        self.persisted_mutation_context
            .spawn_service(name, async move {
                let result = operation(state.clone()).await;
                #[cfg(test)]
                state.wait_after_persisted_mutation_for_tests().await;
                let _ = sender.send(result);
            })
            .map_err(|error| DashboardError::internal_source("Could not admit persisted mutation", error))?;
        receiver
            .await
            .unwrap_or(Err(PersistenceError::ConnectionUnavailable.into()))
    }

    #[cfg(test)]
    pub(crate) async fn install_test_config_publish_hook(
        &self,
    ) -> (oneshot::Receiver<()>, oneshot::Sender<()>, oneshot::Receiver<()>) {
        let (persisted_sender, persisted_receiver) = oneshot::channel();
        let (release_sender, release_receiver) = oneshot::channel();
        let (published_sender, published_receiver) = oneshot::channel();
        *self.config_publish_hook.lock().await = Some(TestConfigPublishHook {
            persisted: persisted_sender,
            release: release_receiver,
            published: published_sender,
        });
        (persisted_receiver, release_sender, published_receiver)
    }

    #[cfg(test)]
    pub(crate) async fn wait_before_config_publish_for_tests(&self) -> Option<oneshot::Sender<()>> {
        let hook = self.config_publish_hook.lock().await.take();
        let hook = hook?;
        let _ = hook.persisted.send(());
        let _ = hook.release.await;
        Some(hook.published)
    }

    #[cfg(test)]
    pub(crate) async fn install_test_persisted_mutation_completion_hook(
        &self,
    ) -> (oneshot::Receiver<()>, oneshot::Sender<()>, oneshot::Receiver<()>) {
        let (reached_sender, reached_receiver) = oneshot::channel();
        let (release_sender, release_receiver) = oneshot::channel();
        let (completed_sender, completed_receiver) = oneshot::channel();
        *self.persisted_mutation_completion_hook.lock().await = Some(TestPersistedMutationCompletionHook {
            reached: reached_sender,
            release: release_receiver,
            completed: completed_sender,
        });
        (reached_receiver, release_sender, completed_receiver)
    }

    #[cfg(test)]
    async fn wait_after_persisted_mutation_for_tests(&self) {
        let hook = self.persisted_mutation_completion_hook.lock().await.take();
        let Some(hook) = hook else {
            return;
        };
        let _ = hook.reached.send(());
        let _ = hook.release.await;
        let _ = hook.completed.send(());
    }

    fn start_environment_convergence(
        &self,
        service_context: rocketmq_runtime::ChildServiceContext,
    ) -> Result<(), DashboardError> {
        let persistence = self.persistence.clone();
        let published_environment = self.published_environment.clone();
        let refresh_lock = self.environment_refresh_lock.clone();
        let cancellation = service_context.task_group().cancellation_token();
        service_context
            .spawn_service("environment-cache-convergence", async move {
                let mut interval = tokio::time::interval(ENVIRONMENT_CONVERGENCE_INTERVAL);
                interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        _ = cancellation.cancelled() => return,
                        _ = interval.tick() => {
                            // HTTP reads expose storage failures; a periodic reconciliation
                            // leaves the last coherent aggregate in place until that happens.
                            let _ = refresh_published_default_environment(
                                &persistence,
                                &published_environment,
                                &refresh_lock,
                            ).await;
                        }
                    }
                }
            })
            .map_err(|error| DashboardError::internal_source("Could not start environment convergence", error))?;
        Ok(())
    }
}

async fn refresh_published_default_environment(
    persistence: &DashboardPersistence,
    published_environment: &std::sync::RwLock<PublishedEnvironment>,
    refresh_lock: &Mutex<()>,
) -> Result<DashboardEnvironment, DashboardError> {
    let _refresh = refresh_lock.lock().await;
    let environment = persistence
        .load_default_environment()
        .await?
        .ok_or(PersistenceError::NotFound)?;
    let published = PublishedEnvironment::from_environment(environment.clone(), persistence.storage_backend());
    match published_environment.write() {
        Ok(mut current) => {
            publish_if_current_or_newer(&mut current, published);
        }
        Err(poisoned) => {
            publish_if_current_or_newer(&mut poisoned.into_inner(), published);
        }
    }
    Ok(environment)
}

fn publish_if_current_or_newer(current: &mut PublishedEnvironment, candidate: PublishedEnvironment) -> bool {
    if current.environment.environment_id != candidate.environment.environment_id
        || candidate.environment.revision < current.environment.revision
    {
        return false;
    }
    *current = candidate;
    true
}

fn ensure_persistence_ready(storage_health: StorageHealth) -> Result<(), DashboardError> {
    match storage_health.status {
        StorageStatus::Available => Ok(()),
        StorageStatus::Degraded => Err(PersistenceError::Capacity.into()),
        StorageStatus::Unavailable => Err(PersistenceError::ConnectionUnavailable.into()),
    }
}

async fn load_or_create_default_environment(
    persistence: &DashboardPersistence,
    initial_config: &DashboardConfigView,
) -> Result<DashboardEnvironment, DashboardError> {
    let environments = persistence.list_environments().await?;
    match environments.as_slice() {
        [environment] if environment.name == "default" => return Ok(environment.clone()),
        [] => {}
        _ => {
            return Err(PersistenceError::InvalidConfig(
                "dashboard configuration requires exactly one default environment".to_string(),
            )
            .into());
        }
    }
    let candidate = DashboardEnvironment::bootstrap(initial_config, chrono::Utc::now().timestamp_millis());
    match persistence.create_environment(candidate).await {
        Ok(environment) => Ok(environment),
        Err(PersistenceError::Conflict) => persistence
            .load_default_environment()
            .await?
            .ok_or(PersistenceError::Conflict.into()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::AppState;
    use super::ensure_persistence_ready;
    use super::publish_if_current_or_newer;
    use crate::config::AppConfig;
    use crate::config::AuthConfig;
    use crate::config::ServerConfig;
    use crate::config::SqlPoolConfig;
    use crate::config::StorageConfig;
    use crate::error::DashboardError;
    use crate::model::AddressRequest;
    use crate::model::ConsumerMonitorUpsertRequest;
    use crate::model::DashboardConfigView;
    use crate::model::DashboardEnvironment;
    use crate::model::PublishedEnvironment;
    use crate::model::StorageBackend;
    use crate::persistence::Revision;
    use crate::persistence::StorageHealth;
    use crate::persistence::StorageMode;
    use crate::persistence::StorageStatus;
    use crate::persistence::error::PersistenceError;
    use crate::service::add_proxy;
    use crate::service::create_or_update_consumer_monitor;
    use rocketmq_admin_core::client_adapter::ClientRuntime;
    use rocketmq_admin_core::client_adapter::ClientRuntimeConfig;
    use rocketmq_runtime::RuntimeConfig;
    use rocketmq_runtime::RuntimeOwner;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    fn file_app_config(root: PathBuf) -> AppConfig {
        AppConfig {
            server: ServerConfig {
                host: "127.0.0.1".to_string(),
                port: 0,
            },
            storage: StorageConfig {
                backend: StorageBackend::File,
                data_path: root,
                database_url: None,
                pool: SqlPoolConfig::default(),
            },
            auth: AuthConfig {
                login_required: false,
                username: "admin".to_string(),
                password: "test-password".to_string(),
                ..AuthConfig::default()
            },
            dashboard_history_interval_secs: 0,
            dashboard_history_retention_days: 30,
            dashboard_history_retention_batch_size: 500,
            dashboard_history_lease_ttl_secs: 30,
            initial_config: DashboardConfigView::default(),
            admin_credentials: None,
        }
    }

    async fn test_state(owner: &RuntimeOwner, root: PathBuf) -> (AppState, Arc<ClientRuntime>) {
        let client_runtime = ClientRuntime::try_new(
            owner.root_context().component("state-owner-test-admin-client"),
            ClientRuntimeConfig::default(),
            rocketmq_observability::TelemetryHandle::noop(),
        )
        .expect("client runtime");
        let state = AppState::try_new_without_environment_convergence(file_app_config(root), client_runtime.clone())
            .await
            .expect("app state");
        (state, client_runtime)
    }

    #[test]
    fn startup_rejects_unavailable_persistence_before_serving_http() {
        let result = ensure_persistence_ready(StorageHealth {
            backend: StorageBackend::Sqlite,
            mode: StorageMode::SingleNode,
            status: StorageStatus::Unavailable,
            schema_version: None,
            last_successful_write_at: None,
            available_bytes: None,
            pool_size: Some(0),
            idle_connections: Some(0),
        });

        assert!(matches!(
            result,
            Err(DashboardError::Storage(PersistenceError::ConnectionUnavailable))
        ));
    }

    #[test]
    fn delayed_refresh_cannot_publish_an_older_environment_revision() {
        let mut current_environment = DashboardEnvironment::bootstrap(&DashboardConfigView::default(), 2);
        current_environment.revision = Revision(2);
        let mut current = PublishedEnvironment::from_environment(current_environment, StorageBackend::MySql);
        let older = PublishedEnvironment::from_environment(
            DashboardEnvironment::bootstrap(&DashboardConfigView::default(), 1),
            StorageBackend::MySql,
        );

        assert!(!publish_if_current_or_newer(&mut current, older));
        assert_eq!(current.environment.revision, Revision(2));
    }

    #[test]
    fn aborted_config_service_request_commits_and_publishes_the_same_revision() {
        let directory = tempfile::tempdir().expect("temp dir");
        let owner = RuntimeOwner::new(RuntimeConfig::default()).expect("runtime owner");
        owner.block_on(async {
            let (state, client_runtime) = test_state(&owner, directory.path().join("dashboard")).await;
            let initial = state.published().environment;
            let (persisted, release_publish, published) = state.install_test_config_publish_hook().await;

            let request_state = state.clone();
            let request = tokio::spawn(async move {
                add_proxy(
                    &request_state,
                    AddressRequest {
                        address: "127.0.0.2:8080".to_string(),
                        expected_revision: initial.revision,
                    },
                    None,
                )
                .await
            });
            tokio::time::timeout(Duration::from_millis(250), persisted)
                .await
                .expect("candidate must persist before publish")
                .expect("publish hook must signal persisted aggregate");
            let durable = state
                .persistence
                .load_default_environment()
                .await
                .expect("load durable environment")
                .expect("default environment");
            assert_eq!(durable.revision, Revision(initial.revision.0 + 1));
            assert_eq!(
                state.published().environment.revision,
                initial.revision,
                "disabled convergence cannot publish before the service owner reaches its publish step"
            );
            request.abort();
            assert!(request.await.expect_err("cancelled request").is_cancelled());
            release_publish.send(()).expect("release service-owned publish");
            tokio::time::timeout(Duration::from_millis(250), published)
                .await
                .expect("service owner must publish without periodic convergence")
                .expect("publish hook must signal the exact publish step");
            let published_environment = state.published().environment;
            assert_eq!(published_environment.revision, durable.revision);
            assert_eq!(published_environment.endpoints, durable.endpoints);
            assert!(
                durable
                    .endpoints
                    .iter()
                    .any(|endpoint| endpoint.address == "127.0.0.2:8080")
            );

            drop(state);
            client_runtime.shutdown().await.log_if_unhealthy();
        });
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }

    #[test]
    fn aborted_monitor_service_request_still_finishes_its_owned_persistence() {
        let directory = tempfile::tempdir().expect("temp dir");
        let owner = RuntimeOwner::new(RuntimeConfig::default()).expect("runtime owner");
        owner.block_on(async {
            let (state, client_runtime) = test_state(&owner, directory.path().join("dashboard")).await;
            let environment_id = state.published().environment.environment_id;
            let (persisted, release_owner, owner_completed) =
                state.install_test_persisted_mutation_completion_hook().await;

            let request_state = state.clone();
            let request_environment_id = environment_id.clone();
            let request = tokio::spawn(async move {
                create_or_update_consumer_monitor(
                    &request_state,
                    ConsumerMonitorUpsertRequest {
                        environment_id: request_environment_id,
                        consumer_group: "cancelled-owner-group".to_string(),
                        min_count: 1,
                        max_diff_total: 0,
                        expected_revision: Revision(0),
                    },
                    None,
                )
                .await
            });
            tokio::time::timeout(Duration::from_millis(250), persisted)
                .await
                .expect("monitor persistence must complete before its owner responds")
                .expect("monitor owner hook must signal persisted rule");
            let rules = state
                .persistence
                .list_monitor_rules(&environment_id)
                .await
                .expect("load durable monitor rules");
            assert!(
                rules
                    .iter()
                    .any(|rule| rule.consumer_group == "cancelled-owner-group" && rule.revision == Revision(1))
            );
            request.abort();
            assert!(request.await.expect_err("cancelled request").is_cancelled());
            release_owner.send(()).expect("release monitor owner");
            tokio::time::timeout(Duration::from_millis(250), owner_completed)
                .await
                .expect("monitor owner must complete after handler cancellation")
                .expect("monitor owner completion hook must fire");

            drop(state);
            client_runtime.shutdown().await.log_if_unhealthy();
        });
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }
}
