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
use anyhow::Result;
use rocketmq_dashboard_common::NameServerConfigSnapshot;
use rocketmq_dashboard_common::NameServerHomePageInfo;
use rocketmq_dashboard_common::NameServerMutationResult;
use rocketmq_dashboard_common::NameServerService;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct NameServerManager {
    service: Arc<NameServerService<SqliteNameServerStore, NameServerRuntimeState>>,
    #[cfg_attr(not(test), allow(dead_code))]
    runtime: Arc<NameServerRuntimeState>,
}

impl NameServerManager {
    pub(crate) fn new(db: NameServerDb, runtime: Arc<NameServerRuntimeState>) -> Result<Self> {
        let store = Arc::new(SqliteNameServerStore::new(db));
        let service = Arc::new(NameServerService::new(store, runtime.clone()));

        Ok(Self { service, runtime })
    }

    pub(crate) fn home_page_info(&self) -> Result<NameServerHomePageInfo> {
        self.service.home_page_info()
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
    use crate::nameserver::db::NameServerDb;
    use crate::nameserver::db::SqliteNameServerStore;
    use crate::nameserver::runtime::NameServerRuntimeState;
    use rocketmq_dashboard_common::NameServerConfigStore;
    use std::env;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
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
}
