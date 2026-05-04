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

use crate::proxy::db::ProxyDb;
use anyhow::Result;
use anyhow::bail;
use rocketmq_dashboard_common::ProxyConfigSnapshot;
use rocketmq_dashboard_common::ProxyMutationResult;
use rocketmq_dashboard_common::normalize_proxy_address;

#[derive(Clone)]
pub(crate) struct ProxyManager {
    db: ProxyDb,
}

impl ProxyManager {
    pub(crate) fn new(db: ProxyDb) -> Result<Self> {
        Ok(Self { db })
    }

    pub(crate) fn home_page_info(&self) -> Result<ProxyConfigSnapshot> {
        self.db.load_snapshot()
    }

    pub(crate) fn add_proxy_addr(&self, address: &str) -> Result<ProxyMutationResult> {
        let address = normalize_proxy_address(address)?;
        let snapshot = self.db.update_snapshot(|snapshot| {
            if snapshot.proxy_addr_list.iter().any(|existing| existing == &address) {
                bail!("Proxy address already exists");
            }

            snapshot.proxy_addr_list.push(address.clone());
            if snapshot.current_proxy_addr.is_none() {
                snapshot.current_proxy_addr = Some(address.clone());
            }

            Ok(())
        })?;

        Ok(ProxyMutationResult {
            message: format!("Proxy address `{address}` added"),
            snapshot,
        })
    }

    pub(crate) fn switch_proxy_addr(&self, address: &str) -> Result<ProxyMutationResult> {
        let address = normalize_proxy_address(address)?;
        let snapshot = self.db.update_snapshot(|snapshot| {
            if !snapshot.proxy_addr_list.iter().any(|existing| existing == &address) {
                bail!("Proxy address does not exist");
            }

            snapshot.current_proxy_addr = Some(address.clone());
            Ok(())
        })?;

        Ok(ProxyMutationResult {
            message: format!("Current Proxy switched to `{address}`"),
            snapshot,
        })
    }

    pub(crate) fn delete_proxy_addr(&self, address: &str) -> Result<ProxyMutationResult> {
        let address = normalize_proxy_address(address)?;
        let snapshot = self.db.update_snapshot(|snapshot| {
            let previous_len = snapshot.proxy_addr_list.len();
            snapshot.proxy_addr_list.retain(|existing| existing != &address);

            if snapshot.proxy_addr_list.len() == previous_len {
                bail!("Proxy address does not exist");
            }

            if snapshot.current_proxy_addr.as_deref() == Some(address.as_str())
                || snapshot
                    .current_proxy_addr
                    .as_ref()
                    .is_some_and(|current| !snapshot.proxy_addr_list.iter().any(|existing| existing == current))
            {
                snapshot.current_proxy_addr = snapshot.proxy_addr_list.first().cloned();
            }

            Ok(())
        })?;

        Ok(ProxyMutationResult {
            message: format!("Proxy address `{address}` deleted"),
            snapshot,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::ProxyManager;
    use crate::proxy::db::ProxyDb;
    use std::env;
    use std::fs;
    use std::path::PathBuf;
    use uuid::Uuid;

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new() -> Self {
            let path = env::temp_dir().join(format!(
                "rocketmq-dashboard-tauri-proxy-manager-tests-{}",
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
        manager: ProxyManager,
    }

    fn setup_manager() -> TestContext {
        let test_dir = TestDir::new();
        let db = ProxyDb::from_path(test_dir.db_path());
        db.init().expect("database initialization should succeed");
        TestContext {
            _test_dir: test_dir,
            manager: ProxyManager::new(db).expect("manager should initialize"),
        }
    }

    #[test]
    fn first_added_proxy_becomes_current() {
        let context = setup_manager();

        let result = context
            .manager
            .add_proxy_addr(" LOCALHOST : 8080 ")
            .expect("add should succeed");

        assert_eq!(result.snapshot.proxy_addr_list, vec!["localhost:8080".to_string()]);
        assert_eq!(result.snapshot.current_proxy_addr.as_deref(), Some("localhost:8080"));
    }

    #[test]
    fn switch_requires_existing_proxy() {
        let context = setup_manager();

        let error = context
            .manager
            .switch_proxy_addr("127.0.0.2:8080")
            .expect_err("switch should fail for missing proxy");

        assert!(error.to_string().contains("does not exist"));
    }

    #[test]
    fn deleting_current_proxy_falls_back_to_first_remaining_proxy() {
        let context = setup_manager();

        context
            .manager
            .add_proxy_addr("127.0.0.1:8080")
            .expect("first add should succeed");
        context
            .manager
            .add_proxy_addr("127.0.0.2:8080")
            .expect("second add should succeed");
        context
            .manager
            .switch_proxy_addr("127.0.0.2:8080")
            .expect("switch should succeed");

        let result = context
            .manager
            .delete_proxy_addr("127.0.0.2:8080")
            .expect("delete should succeed");

        assert_eq!(result.snapshot.proxy_addr_list, vec!["127.0.0.1:8080".to_string()]);
        assert_eq!(result.snapshot.current_proxy_addr.as_deref(), Some("127.0.0.1:8080"));
    }
}
