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
use crate::config::ConfigStore;
use crate::error::DashboardError;
use crate::model::DashboardConfigView;
use crate::persistence::DashboardPersistence;
use crate::persistence::StorageHealth;
use crate::persistence::StorageStatus;
use crate::persistence::error::PersistenceError;
use crate::service::AuthState;
use crate::service::DashboardHistoryStore;
use crate::service::DashboardTaskManager;
use crate::service::MonitorStore;
use crate::service::spawn_dashboard_history_collector;
use rocketmq_admin_core::client_adapter::ClientRuntime;
use rocketmq_dashboard_common::DashboardAdminFacade;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::RwLock;

pub type WebAdminFacade = DashboardAdminFacade<DashboardAdminClient>;

#[derive(Clone)]
pub struct AppState {
    pub persistence: Arc<DashboardPersistence>,
    pub config_store: Arc<ConfigStore>,
    pub(crate) config_mutation_lock: Arc<Mutex<()>>,
    pub auth_state: Arc<AuthState>,
    pub monitor_store: Arc<MonitorStore>,
    pub history_store: DashboardHistoryStore,
    pub dashboard_tasks: DashboardTaskManager,
    pub dashboard_config: Arc<RwLock<DashboardConfigView>>,
    pub admin_client: DashboardAdminClient,
}

impl AppState {
    pub async fn try_new(config: AppConfig, client_runtime: Arc<ClientRuntime>) -> Result<Self, DashboardError> {
        let persistence = Arc::new(
            DashboardPersistence::initialize(&config.storage, client_runtime.component("dashboard-persistence"))
                .await?,
        );
        ensure_persistence_ready(persistence.storage_health().await)?;
        let compatibility_context = client_runtime.component("dashboard-compatibility-config");
        let config_store = Arc::new(ConfigStore::new(
            config.interim_config_path,
            compatibility_context.storage_io().clone(),
        ));
        let auth_state = Arc::new(AuthState::new(config.auth));
        let monitor_store = Arc::new(MonitorStore::new(config.monitor_store_path));
        let history_store = DashboardHistoryStore::default();
        let dashboard_tasks = DashboardTaskManager::default();
        let dashboard_config = align_config_storage_backend(
            config_store.load_or_init(&config.initial_config).await?,
            persistence.storage_backend(),
        );
        let dashboard_config = Arc::new(RwLock::new(dashboard_config));
        let admin_client =
            DashboardAdminClient::new(dashboard_config.clone(), client_runtime, config.admin_credentials);
        if config.dashboard_history_interval_secs > 0 {
            spawn_dashboard_history_collector(
                &dashboard_tasks,
                DashboardAdminFacade::new(admin_client.clone()),
                history_store.clone(),
                config.dashboard_history_interval_secs,
            )?;
        }

        Ok(Self {
            persistence,
            config_store,
            config_mutation_lock: Arc::new(Mutex::new(())),
            auth_state,
            monitor_store,
            history_store,
            dashboard_tasks,
            dashboard_config,
            admin_client,
        })
    }

    pub fn admin_facade(&self) -> WebAdminFacade {
        DashboardAdminFacade::new(self.admin_client.clone())
    }
}

fn ensure_persistence_ready(storage_health: StorageHealth) -> Result<(), DashboardError> {
    match storage_health.status {
        StorageStatus::Available => Ok(()),
        StorageStatus::Degraded => Err(PersistenceError::Capacity.into()),
        StorageStatus::Unavailable => Err(PersistenceError::ConnectionUnavailable.into()),
    }
}

fn align_config_storage_backend(
    mut dashboard_config: DashboardConfigView,
    storage_backend: crate::model::StorageBackend,
) -> DashboardConfigView {
    dashboard_config.storage_backend = storage_backend;
    dashboard_config
}

#[cfg(test)]
mod tests {
    use super::align_config_storage_backend;
    use super::ensure_persistence_ready;
    use crate::error::DashboardError;
    use crate::model::DashboardConfigView;
    use crate::model::StorageBackend;
    use crate::persistence::StorageHealth;
    use crate::persistence::StorageMode;
    use crate::persistence::StorageStatus;
    use crate::persistence::error::PersistenceError;

    #[test]
    fn selected_persistence_backend_overrides_a_stale_compatibility_value() {
        let stored = DashboardConfigView {
            storage_backend: StorageBackend::File,
            ..DashboardConfigView::default()
        };

        let resolved = align_config_storage_backend(stored, StorageBackend::Postgres);

        assert_eq!(resolved.storage_backend, StorageBackend::Postgres);
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
}
