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

use crate::nameserver::db::NameServerDb;
use crate::nameserver::db::SqliteNameServerStore;
use crate::nameserver::runtime::NameServerRuntimeState;
use crate::nameserver::types::NameServerHomePageView;
use crate::nameserver::types::NameServerStatusItem;
use anyhow::Result;
use cheetah_string::CheetahString;
use rocketmq_admin_core::admin::default_mq_admin_ext::DefaultMQAdminExt;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_dashboard_common::NameServerConfigSnapshot;
use rocketmq_dashboard_common::NameServerMutationResult;
use rocketmq_dashboard_common::NameServerService;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

const NAMESERVER_PROBE_TIMEOUT_MILLIS: u64 = 1_500;

pub(crate) trait NameServerProbe: Send + Sync {
    fn probe<'a>(
        &'a self,
        snapshot: &'a NameServerConfigSnapshot,
        address: &'a str,
    ) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>>;
}

struct DefaultNameServerProbe;

impl NameServerProbe for DefaultNameServerProbe {
    fn probe<'a>(
        &'a self,
        snapshot: &'a NameServerConfigSnapshot,
        address: &'a str,
    ) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>> {
        Box::pin(async move {
            let mut admin = DefaultMQAdminExt::with_admin_ext_group_and_timeout(
                format!("dashboard-nameserver-probe-{}", Uuid::new_v4()),
                Duration::from_millis(NAMESERVER_PROBE_TIMEOUT_MILLIS),
            );
            admin.client_config_mut().set_namesrv_addr(CheetahString::from(address));
            admin.client_config_mut().set_vip_channel_enabled(snapshot.use_vip_channel);
            admin.client_config_mut().set_use_tls(snapshot.use_tls);

            let started = match admin.start().await {
                Ok(_) => true,
                Err(error) => {
                    log::warn!("NameServer probe start failed for `{}`: {}", address, error);
                    false
                }
            };

            if !started {
                return false;
            }

            let probe_result = admin
                .get_name_server_config(vec![CheetahString::from(address)])
                .await
                .map(|_| true)
                .unwrap_or_else(|error| {
                    log::warn!("NameServer probe failed for `{}`: {}", address, error);
                    false
                });

            admin.shutdown().await;
            probe_result
        })
    }
}

#[derive(Clone)]
pub(crate) struct NameServerManager {
    service: Arc<NameServerService<SqliteNameServerStore, NameServerRuntimeState>>,
    #[cfg_attr(not(test), allow(dead_code))]
    runtime: Arc<NameServerRuntimeState>,
    probe: Arc<dyn NameServerProbe>,
}

impl NameServerManager {
    pub(crate) fn new(db: NameServerDb, runtime: Arc<NameServerRuntimeState>) -> Result<Self> {
        Self::with_probe(db, runtime, Arc::new(DefaultNameServerProbe))
    }

    pub(crate) fn with_probe(
        db: NameServerDb,
        runtime: Arc<NameServerRuntimeState>,
        probe: Arc<dyn NameServerProbe>,
    ) -> Result<Self> {
        let store = Arc::new(SqliteNameServerStore::new(db));
        let service = Arc::new(NameServerService::new(store, runtime.clone()));

        Ok(Self {
            service,
            runtime,
            probe,
        })
    }

    pub(crate) async fn home_page_info(&self) -> Result<NameServerHomePageView> {
        let home_page = self.service.home_page_info()?;
        let snapshot = NameServerConfigSnapshot {
            current_namesrv: home_page.current_namesrv.clone(),
            namesrv_addr_list: home_page.namesrv_addr_list.clone(),
            use_vip_channel: home_page.use_vip_channel,
            use_tls: home_page.use_tls,
        };

        let mut servers = Vec::with_capacity(home_page.namesrv_addr_list.len());
        for address in &home_page.namesrv_addr_list {
            let is_alive = self.probe.probe(&snapshot, address).await;
            servers.push(NameServerStatusItem {
                address: address.clone(),
                is_current: home_page.current_namesrv.as_deref() == Some(address.as_str()),
                is_alive,
            });
        }

        Ok(NameServerHomePageView {
            current_namesrv: home_page.current_namesrv,
            namesrv_addr_list: home_page.namesrv_addr_list,
            use_vip_channel: home_page.use_vip_channel,
            use_tls: home_page.use_tls,
            servers,
        })
    }

    pub(crate) fn add_name_server(&self, address: &str) -> Result<NameServerMutationResult> {
        self.service.add_nameserver(address)
    }

    pub(crate) fn switch_name_server(&self, address: &str) -> Result<NameServerMutationResult> {
        self.service.update_current_nameserver(address)
    }

    pub(crate) fn delete_name_server(&self, address: &str) -> Result<NameServerMutationResult> {
        self.service.delete_nameserver(address)
    }

    pub(crate) fn update_vip_channel(&self, enabled: bool) -> Result<NameServerMutationResult> {
        self.service.update_use_vip_channel(enabled)
    }

    pub(crate) fn update_use_tls(&self, enabled: bool) -> Result<NameServerMutationResult> {
        self.service.update_use_tls(enabled)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn runtime_snapshot(&self) -> NameServerConfigSnapshot {
        self.runtime.snapshot()
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn runtime_generation(&self) -> u64 {
        self.runtime.generation()
    }
}

#[cfg(test)]
mod tests {
    use super::NameServerManager;
    use super::NameServerProbe;
    use crate::nameserver::db::NameServerDb;
    use crate::nameserver::db::SqliteNameServerStore;
    use crate::nameserver::runtime::NameServerRuntimeState;
    use crate::nameserver::types::NameServerHomePageView;
    use rocketmq_dashboard_common::NameServerConfigStore;
    use rocketmq_dashboard_common::NameServerConfigSnapshot;
    use std::collections::HashMap;
    use std::env;
    use std::future::Future;
    use std::fs;
    use std::pin::Pin;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::Mutex;
    use tokio::runtime::Builder;
    use uuid::Uuid;

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new() -> Self {
            let path = env::temp_dir().join(format!(
                "rocketmq-dashboard-tauri-nameserver-manager-tests-{}",
                Uuid::new_v4()
            ));
            fs::create_dir_all(&path).expect("failed to create test directory");
            Self { path }
        }

        fn db_path(&self) -> PathBuf {
            self.path.join("dashboard.db")
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    struct TestContext {
        _test_dir: TestDir,
        manager: NameServerManager,
    }

    fn setup_manager() -> TestContext {
        let test_dir = TestDir::new();
        let db = NameServerDb::from_path(test_dir.db_path());
        db.init().expect("database initialization should succeed");
        let store = SqliteNameServerStore::new(db.clone());
        TestContext {
            _test_dir: test_dir,
            manager: NameServerManager::new(
                db,
                Arc::new(NameServerRuntimeState::new(
                    store.load_snapshot().expect("snapshot should load"),
                )),
            )
            .expect("manager should initialize"),
        }
    }

    #[derive(Default)]
    struct ProbeSpy {
        statuses: Mutex<HashMap<String, bool>>,
    }

    impl ProbeSpy {
        fn with_statuses(statuses: &[(&str, bool)]) -> Self {
            Self {
                statuses: Mutex::new(
                    statuses
                        .iter()
                        .map(|(address, is_alive)| (address.to_string(), *is_alive))
                        .collect(),
                ),
            }
        }
    }

    impl NameServerProbe for ProbeSpy {
        fn probe<'a>(
            &'a self,
            _snapshot: &'a NameServerConfigSnapshot,
            address: &'a str,
        ) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>> {
            Box::pin(async move {
                self.statuses
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .get(address)
                    .copied()
                    .unwrap_or(false)
            })
        }
    }

    fn setup_manager_with_probe(statuses: &[(&str, bool)]) -> TestContext {
        let test_dir = TestDir::new();
        let db = NameServerDb::from_path(test_dir.db_path());
        db.init().expect("database initialization should succeed");
        let store = SqliteNameServerStore::new(db.clone());
        TestContext {
            _test_dir: test_dir,
            manager: NameServerManager::with_probe(
                db,
                Arc::new(NameServerRuntimeState::new(
                    store.load_snapshot().expect("snapshot should load"),
                )),
                Arc::new(ProbeSpy::with_statuses(statuses)),
            )
            .expect("manager should initialize"),
        }
    }

    fn load_home_page(manager: &NameServerManager) -> NameServerHomePageView {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime should build")
            .block_on(manager.home_page_info())
            .expect("home page should load")
    }

    #[test]
    fn mutations_refresh_runtime_state_immediately() {
        let context = setup_manager();
        let manager = &context.manager;

        manager.add_name_server("127.0.0.2:9876").expect("add should succeed");
        manager
            .switch_name_server("127.0.0.2:9876")
            .expect("switch should succeed");
        manager.update_vip_channel(false).expect("vip update should succeed");
        manager.update_use_tls(true).expect("tls update should succeed");

        let runtime_snapshot = manager.runtime_snapshot();
        assert_eq!(runtime_snapshot.current_namesrv.as_deref(), Some("127.0.0.2:9876"));
        assert_eq!(
            runtime_snapshot.namesrv_addr_list,
            vec!["127.0.0.1:9876".to_string(), "127.0.0.2:9876".to_string()]
        );
        assert!(!runtime_snapshot.use_vip_channel);
        assert!(runtime_snapshot.use_tls);
        assert_eq!(manager.runtime_generation(), 4);
    }

    #[test]
    fn home_page_info_reports_alive_status_per_server_and_preserves_order() {
        let context = setup_manager_with_probe(&[("127.0.0.1:9876", true), ("127.0.0.2:9876", false)]);
        let manager = &context.manager;

        manager.add_name_server("127.0.0.2:9876").expect("add should succeed");

        let home_page = load_home_page(manager);

        assert_eq!(
            home_page
                .servers
                .iter()
                .map(|server| (server.address.as_str(), server.is_current, server.is_alive))
                .collect::<Vec<_>>(),
            vec![
                ("127.0.0.1:9876", true, true),
                ("127.0.0.2:9876", false, false),
            ]
        );
    }

    #[test]
    fn home_page_info_marks_current_server_offline_without_failing_the_page() {
        let context = setup_manager_with_probe(&[("127.0.0.1:9876", false), ("127.0.0.2:9876", true)]);
        let manager = &context.manager;

        manager.add_name_server("127.0.0.2:9876").expect("add should succeed");

        let home_page = load_home_page(manager);
        let current_server = home_page
            .servers
            .iter()
            .find(|server| server.is_current)
            .expect("current server should exist");

        assert_eq!(current_server.address, "127.0.0.1:9876");
        assert!(!current_server.is_alive);
        assert_eq!(home_page.servers.len(), 2);
    }

    #[test]
    fn home_page_info_keeps_non_current_servers_when_current_is_offline() {
        let context = setup_manager_with_probe(&[
            ("127.0.0.1:9876", false),
            ("127.0.0.2:9876", true),
            ("127.0.0.3:9876", false),
        ]);
        let manager = &context.manager;

        manager.add_name_server("127.0.0.2:9876").expect("add should succeed");
        manager.add_name_server("127.0.0.3:9876").expect("add should succeed");

        let home_page = load_home_page(manager);

        assert_eq!(home_page.servers.len(), 3);
        assert_eq!(
            home_page
                .servers
                .iter()
                .map(|server| server.address.as_str())
                .collect::<Vec<_>>(),
            vec!["127.0.0.1:9876", "127.0.0.2:9876", "127.0.0.3:9876"]
        );
        assert!(home_page.servers[1].is_alive);
    }
}
