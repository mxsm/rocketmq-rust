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
use crate::model::DashboardConfigView;
use crate::service::AuthState;
use crate::service::DashboardHistoryStore;
use crate::service::MonitorStore;
use crate::service::spawn_dashboard_history_collector;
use rocketmq_dashboard_common::DashboardAdminFacade;
use std::sync::Arc;
use tokio::sync::RwLock;

pub type WebAdminFacade = DashboardAdminFacade<DashboardAdminClient>;

#[derive(Clone)]
pub struct AppState {
    pub config_store: Arc<ConfigStore>,
    pub auth_state: Arc<AuthState>,
    pub monitor_store: Arc<MonitorStore>,
    pub history_store: DashboardHistoryStore,
    pub dashboard_config: Arc<RwLock<DashboardConfigView>>,
    pub admin_client: DashboardAdminClient,
}

impl AppState {
    pub async fn try_new(config: AppConfig) -> anyhow::Result<Self> {
        let config_store = Arc::new(ConfigStore::new(&config.storage)?);
        let auth_state = Arc::new(AuthState::new(config.auth));
        let monitor_store = Arc::new(MonitorStore::new(config.monitor_store_path));
        let history_store = DashboardHistoryStore::default();
        let dashboard_config = config_store.load_or_init(&config.initial_config)?;
        let dashboard_config = Arc::new(RwLock::new(dashboard_config));
        let admin_client = DashboardAdminClient::new(dashboard_config.clone());
        if config.dashboard_history_interval_secs > 0 {
            spawn_dashboard_history_collector(
                DashboardAdminFacade::new(admin_client.clone()),
                history_store.clone(),
                config.dashboard_history_interval_secs,
            )?;
        }

        Ok(Self {
            config_store,
            auth_state,
            monitor_store,
            history_store,
            dashboard_config,
            admin_client,
        })
    }

    pub fn admin_facade(&self) -> WebAdminFacade {
        DashboardAdminFacade::new(self.admin_client.clone())
    }
}
