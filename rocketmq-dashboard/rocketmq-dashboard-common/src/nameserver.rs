// Copyright 2025 The RocketMQ Rust Authors
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

//! Shared NameServer configuration domain for RocketMQ dashboard frontends.

use anyhow::anyhow;
use anyhow::bail;
use anyhow::Context;
use anyhow::Result;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashSet;
use std::sync::Arc;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct NameServerConfigSnapshot {
    pub current_namesrv: Option<String>,
    pub namesrv_addr_list: Vec<String>,
    #[serde(rename = "useVIPChannel")]
    pub use_vip_channel: bool,
    #[serde(rename = "useTLS")]
    pub use_tls: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct NameServerHomePageInfo {
    pub current_namesrv: Option<String>,
    pub namesrv_addr_list: Vec<String>,
    #[serde(rename = "useVIPChannel")]
    pub use_vip_channel: bool,
    #[serde(rename = "useTLS")]
    pub use_tls: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct NameServerMutationResult {
    pub message: String,
    pub snapshot: NameServerConfigSnapshot,
}

impl From<NameServerConfigSnapshot> for NameServerHomePageInfo {
    fn from(snapshot: NameServerConfigSnapshot) -> Self {
        Self {
            current_namesrv: snapshot.current_namesrv,
            namesrv_addr_list: snapshot.namesrv_addr_list,
            use_vip_channel: snapshot.use_vip_channel,
            use_tls: snapshot.use_tls,
        }
    }
}

pub trait NameServerConfigTransaction {
    fn load_snapshot(&mut self) -> Result<NameServerConfigSnapshot>;

    fn save_snapshot(&mut self, snapshot: &NameServerConfigSnapshot) -> Result<()>;
}

pub trait NameServerConfigStore: Send + Sync {
    fn load_snapshot(&self) -> Result<NameServerConfigSnapshot>;

    fn run_in_transaction<T, F>(&self, operation: F) -> Result<T>
    where
        F: FnOnce(&mut dyn NameServerConfigTransaction) -> Result<T>;
}

pub trait NameServerRuntimeAdapter: Send + Sync {
    fn apply_snapshot(&self, snapshot: &NameServerConfigSnapshot) -> Result<()>;
}

pub struct NameServerService<S, R> {
    store: Arc<S>,
    runtime: Arc<R>,
}

impl<S, R> NameServerService<S, R>
where
    S: NameServerConfigStore,
    R: NameServerRuntimeAdapter,
{
    pub fn new(store: Arc<S>, runtime: Arc<R>) -> Self {
        Self { store, runtime }
    }

    pub fn home_page_info(&self) -> Result<NameServerHomePageInfo> {
        Ok(self.store.load_snapshot()?.into())
    }

    pub fn update_current_nameserver(&self, address: &str) -> Result<NameServerMutationResult> {
        let normalized_address = normalize_nameserver_address(address)?;
        self.mutate_snapshot("NameServer updated", move |snapshot| {
            if !snapshot
                .namesrv_addr_list
                .iter()
                .any(|item| item == &normalized_address)
            {
                bail!("NameServer address not found");
            }

            snapshot.current_namesrv = Some(normalized_address.clone());
            Ok(())
        })
    }

    pub fn add_nameserver(&self, address: &str) -> Result<NameServerMutationResult> {
        let normalized_address = normalize_nameserver_address(address)?;
        self.mutate_snapshot("NameServer added", move |snapshot| {
            if snapshot
                .namesrv_addr_list
                .iter()
                .any(|item| item == &normalized_address)
            {
                bail!("NameServer address already exists");
            }

            snapshot.namesrv_addr_list.push(normalized_address.clone());
            Ok(())
        })
    }

    pub fn delete_nameserver(&self, address: &str) -> Result<NameServerMutationResult> {
        let normalized_address = normalize_nameserver_address(address)?;
        self.mutate_snapshot("NameServer removed", move |snapshot| {
            if snapshot.current_namesrv.as_deref() == Some(normalized_address.as_str()) {
                bail!("Cannot delete the active NameServer");
            }

            let original_len = snapshot.namesrv_addr_list.len();
            snapshot.namesrv_addr_list.retain(|item| item != &normalized_address);

            if snapshot.namesrv_addr_list.len() == original_len {
                bail!("NameServer address not found");
            }

            Ok(())
        })
    }

    pub fn update_use_vip_channel(&self, enabled: bool) -> Result<NameServerMutationResult> {
        self.mutate_snapshot("VIP channel updated", move |snapshot| {
            snapshot.use_vip_channel = enabled;
            Ok(())
        })
    }

    pub fn update_use_tls(&self, enabled: bool) -> Result<NameServerMutationResult> {
        self.mutate_snapshot("TLS setting updated", move |snapshot| {
            snapshot.use_tls = enabled;
            Ok(())
        })
    }

    fn mutate_snapshot<F>(&self, message: &'static str, operation: F) -> Result<NameServerMutationResult>
    where
        F: FnOnce(&mut NameServerConfigSnapshot) -> Result<()>,
    {
        self.store.run_in_transaction(|transaction| {
            let current_snapshot = transaction.load_snapshot()?;
            let mut next_snapshot = canonicalize_snapshot(&current_snapshot)?;

            operation(&mut next_snapshot)?;

            let next_snapshot = canonicalize_snapshot(&next_snapshot)?;
            transaction.save_snapshot(&next_snapshot)?;
            self.runtime.apply_snapshot(&next_snapshot)?;

            Ok(NameServerMutationResult {
                message: message.to_string(),
                snapshot: next_snapshot,
            })
        })
    }
}

pub fn normalize_nameserver_address(address: &str) -> Result<String> {
    let trimmed = address.trim();
    let (host_part, port_part) = trimmed
        .rsplit_once(':')
        .ok_or_else(|| anyhow!("NameServer address must be in host:port format"))?;

    let host = host_part.trim().to_ascii_lowercase();
    if host.is_empty() {
        bail!("NameServer host cannot be empty");
    }

    let port = port_part.trim();
    let port_number: u16 = port
        .parse()
        .with_context(|| format!("Invalid NameServer port `{port}`"))?;

    Ok(format!("{host}:{port_number}"))
}

pub fn canonicalize_snapshot(snapshot: &NameServerConfigSnapshot) -> Result<NameServerConfigSnapshot> {
    let mut seen = HashSet::new();
    let mut addresses = Vec::with_capacity(snapshot.namesrv_addr_list.len());

    for address in &snapshot.namesrv_addr_list {
        let normalized = normalize_nameserver_address(address)?;
        if !seen.insert(normalized.clone()) {
            bail!("NameServer address already exists");
        }
        addresses.push(normalized);
    }

    let current_namesrv = snapshot
        .current_namesrv
        .as_deref()
        .map(normalize_nameserver_address)
        .transpose()?;

    if let Some(current) = &current_namesrv {
        if !addresses.iter().any(|address| address == current) {
            bail!("Current NameServer must exist in the address list");
        }
    }

    Ok(NameServerConfigSnapshot {
        current_namesrv,
        namesrv_addr_list: addresses,
        use_vip_channel: snapshot.use_vip_channel,
        use_tls: snapshot.use_tls,
    })
}

#[cfg(test)]
mod tests {
    use super::NameServerConfigSnapshot;
    use super::NameServerConfigStore;
    use super::NameServerConfigTransaction;
    use super::NameServerRuntimeAdapter;
    use super::NameServerService;
    use std::sync::Arc;
    use std::sync::Mutex;

    #[derive(Debug)]
    struct MemoryStore {
        snapshot: Mutex<NameServerConfigSnapshot>,
    }

    impl MemoryStore {
        fn new(snapshot: NameServerConfigSnapshot) -> Self {
            Self {
                snapshot: Mutex::new(snapshot),
            }
        }
    }

    struct MemoryTransaction {
        snapshot: NameServerConfigSnapshot,
    }

    impl NameServerConfigTransaction for MemoryTransaction {
        fn load_snapshot(&mut self) -> anyhow::Result<NameServerConfigSnapshot> {
            Ok(self.snapshot.clone())
        }

        fn save_snapshot(&mut self, snapshot: &NameServerConfigSnapshot) -> anyhow::Result<()> {
            self.snapshot = snapshot.clone();
            Ok(())
        }
    }

    impl NameServerConfigStore for MemoryStore {
        fn load_snapshot(&self) -> anyhow::Result<NameServerConfigSnapshot> {
            Ok(self
                .snapshot
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone())
        }

        fn run_in_transaction<T, F>(&self, operation: F) -> anyhow::Result<T>
        where
            F: FnOnce(&mut dyn NameServerConfigTransaction) -> anyhow::Result<T>,
        {
            let mut transaction = MemoryTransaction {
                snapshot: self.load_snapshot()?,
            };

            match operation(&mut transaction) {
                Ok(value) => {
                    *self.snapshot.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = transaction.snapshot;
                    Ok(value)
                }
                Err(error) => Err(error),
            }
        }
    }

    #[derive(Debug, Default)]
    struct RuntimeSpy {
        applied_snapshots: Mutex<Vec<NameServerConfigSnapshot>>,
        fail_apply: Mutex<bool>,
    }

    impl RuntimeSpy {
        fn set_fail_apply(&self, value: bool) {
            *self.fail_apply.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = value;
        }

        fn apply_count(&self) -> usize {
            self.applied_snapshots
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .len()
        }
    }

    impl NameServerRuntimeAdapter for RuntimeSpy {
        fn apply_snapshot(&self, snapshot: &NameServerConfigSnapshot) -> anyhow::Result<()> {
            if *self.fail_apply.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) {
                anyhow::bail!("runtime apply failed");
            }

            self.applied_snapshots
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(snapshot.clone());
            Ok(())
        }
    }

    fn baseline_snapshot() -> NameServerConfigSnapshot {
        NameServerConfigSnapshot {
            current_namesrv: Some("127.0.0.1:9876".to_string()),
            namesrv_addr_list: vec!["127.0.0.1:9876".to_string(), "127.0.0.2:9876".to_string()],
            use_vip_channel: true,
            use_tls: false,
        }
    }

    fn service_with_snapshot(
        snapshot: NameServerConfigSnapshot,
    ) -> (
        Arc<MemoryStore>,
        Arc<RuntimeSpy>,
        NameServerService<MemoryStore, RuntimeSpy>,
    ) {
        let store = Arc::new(MemoryStore::new(snapshot));
        let runtime = Arc::new(RuntimeSpy::default());
        let service = NameServerService::new(store.clone(), runtime.clone());
        (store, runtime, service)
    }

    #[test]
    fn home_page_info_returns_complete_snapshot() {
        let (_store, _runtime, service) = service_with_snapshot(baseline_snapshot());

        let home_page = service.home_page_info().expect("home page info should load");

        assert_eq!(home_page.current_namesrv.as_deref(), Some("127.0.0.1:9876"));
        assert_eq!(home_page.namesrv_addr_list.len(), 2);
        assert!(home_page.use_vip_channel);
        assert!(!home_page.use_tls);
    }

    #[test]
    fn switch_current_nameserver_matches_java_update_semantics() {
        let (store, runtime, service) = service_with_snapshot(baseline_snapshot());

        let result = service
            .update_current_nameserver("127.0.0.2:9876")
            .expect("switch should succeed");

        assert_eq!(result.snapshot.current_namesrv.as_deref(), Some("127.0.0.2:9876"));
        assert_eq!(
            store
                .load_snapshot()
                .expect("snapshot should load")
                .current_namesrv
                .as_deref(),
            Some("127.0.0.2:9876")
        );
        assert_eq!(runtime.apply_count(), 1);
    }

    #[test]
    fn duplicate_nameserver_is_rejected_after_normalization() {
        let (_store, _runtime, service) = service_with_snapshot(NameServerConfigSnapshot {
            current_namesrv: Some("localhost:9876".to_string()),
            namesrv_addr_list: vec!["localhost:9876".to_string()],
            use_vip_channel: true,
            use_tls: false,
        });

        let error = service
            .add_nameserver(" LOCALHOST : 9876 ")
            .expect_err("duplicate address should be rejected");

        assert!(error.to_string().contains("already exists"));
    }

    #[test]
    fn deleting_active_nameserver_is_rejected() {
        let (_store, _runtime, service) = service_with_snapshot(baseline_snapshot());

        let error = service
            .delete_nameserver("127.0.0.1:9876")
            .expect_err("active nameserver should not be deleted");

        assert!(error.to_string().contains("Cannot delete the active"));
    }

    #[test]
    fn toggles_apply_runtime_immediately() {
        let (_store, runtime, service) = service_with_snapshot(baseline_snapshot());

        let vip_result = service
            .update_use_vip_channel(false)
            .expect("vip update should succeed");
        let tls_result = service.update_use_tls(true).expect("tls update should succeed");

        assert!(!vip_result.snapshot.use_vip_channel);
        assert!(tls_result.snapshot.use_tls);
        assert_eq!(runtime.apply_count(), 2);
    }

    #[test]
    fn runtime_apply_failure_rolls_back_store_changes() {
        let (store, runtime, service) = service_with_snapshot(baseline_snapshot());
        runtime.set_fail_apply(true);

        let error = service.update_use_tls(true).expect_err("runtime apply should fail");

        assert!(error.to_string().contains("runtime apply failed"));
        assert!(!store.load_snapshot().expect("snapshot should load").use_tls);
        assert_eq!(runtime.apply_count(), 0);
    }

    #[test]
    fn serde_uses_dashboard_field_names_for_vip_and_tls() {
        let snapshot = NameServerConfigSnapshot {
            current_namesrv: Some("127.0.0.1:9876".to_string()),
            namesrv_addr_list: vec!["127.0.0.1:9876".to_string()],
            use_vip_channel: true,
            use_tls: false,
        };

        let json = serde_json::to_value(&snapshot).expect("snapshot should serialize");

        assert_eq!(json.get("useVIPChannel").and_then(|value| value.as_bool()), Some(true));
        assert_eq!(json.get("useTLS").and_then(|value| value.as_bool()), Some(false));
        assert!(json.get("useVipChannel").is_none());
        assert!(json.get("useTls").is_none());
    }
}
