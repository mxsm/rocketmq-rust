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

use rocketmq_admin_core::client_adapter::ClientRuntime;
use rocketmq_dashboard_common::DashboardCommonResult;
use rocketmq_dashboard_common::NameServerConfigSnapshot;
use rocketmq_dashboard_common::NameServerRuntimeAdapter;
use std::sync::Arc;
use std::sync::Mutex;

pub(crate) trait ClientResetHook: Send + Sync {
    fn reset_clients(&self);
}

#[derive(Default)]
pub(crate) struct NoopClientResetHook;

impl ClientResetHook for NoopClientResetHook {
    fn reset_clients(&self) {}
}

#[derive(Debug, Clone)]
struct RuntimeState {
    snapshot: NameServerConfigSnapshot,
    generation: u64,
}

pub(crate) struct NameServerRuntimeState {
    state: Mutex<RuntimeState>,
    client_runtime: Arc<ClientRuntime>,
    reset_hook: Arc<dyn ClientResetHook>,
}

impl NameServerRuntimeState {
    pub(crate) fn new(snapshot: NameServerConfigSnapshot, client_runtime: Arc<ClientRuntime>) -> Self {
        Self::with_reset_hook(snapshot, client_runtime, Arc::new(NoopClientResetHook))
    }

    pub(crate) fn with_reset_hook(
        snapshot: NameServerConfigSnapshot,
        client_runtime: Arc<ClientRuntime>,
        reset_hook: Arc<dyn ClientResetHook>,
    ) -> Self {
        Self {
            state: Mutex::new(RuntimeState {
                snapshot,
                generation: 0,
            }),
            client_runtime,
            reset_hook,
        }
    }

    pub(crate) fn client_runtime(&self) -> Arc<ClientRuntime> {
        Arc::clone(&self.client_runtime)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn snapshot(&self) -> NameServerConfigSnapshot {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .snapshot
            .clone()
    }

    pub(crate) fn snapshot_and_generation(&self) -> (NameServerConfigSnapshot, u64) {
        let state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        (state.snapshot.clone(), state.generation)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn generation(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .generation
    }
}

impl NameServerRuntimeAdapter for NameServerRuntimeState {
    fn apply_snapshot(&self, snapshot: &NameServerConfigSnapshot) -> DashboardCommonResult<()> {
        {
            let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            state.snapshot = snapshot.clone();
            state.generation += 1;
        }

        self.reset_hook.reset_clients();
        Ok(())
    }
}

#[cfg(test)]
pub(crate) fn test_client_runtime() -> Arc<ClientRuntime> {
    use std::sync::LazyLock;

    use rocketmq_admin_core::client_adapter::ClientRuntimeConfig;
    use rocketmq_admin_core::client_adapter::TelemetryHandle;
    use rocketmq_runtime::RuntimeConfig;
    use rocketmq_runtime::RuntimeOwner;

    static OWNER: LazyLock<RuntimeOwner> = LazyLock::new(|| {
        RuntimeOwner::new(RuntimeConfig::server_default("rocketmq-dashboard-tauri-test"))
            .expect("dashboard Tauri test runtime should start")
    });

    ClientRuntime::new(
        OWNER.root_context().child("client"),
        ClientRuntimeConfig::default(),
        TelemetryHandle::noop(),
    )
}

#[cfg(test)]
mod tests {
    use super::ClientResetHook;
    use super::NameServerRuntimeState;
    use rocketmq_dashboard_common::NameServerConfigSnapshot;
    use rocketmq_dashboard_common::NameServerRuntimeAdapter;
    use std::sync::Arc;
    use std::sync::Mutex;

    use crate::nameserver::runtime::test_client_runtime;

    #[derive(Default)]
    struct ResetSpy {
        count: Mutex<u64>,
    }

    impl ResetSpy {
        fn count(&self) -> u64 {
            *self.count.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
        }
    }

    impl ClientResetHook for ResetSpy {
        fn reset_clients(&self) {
            let mut count = self.count.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            *count += 1;
        }
    }

    #[test]
    fn apply_snapshot_updates_runtime_state_and_resets_clients() {
        let reset_spy = Arc::new(ResetSpy::default());
        let runtime = NameServerRuntimeState::with_reset_hook(
            NameServerConfigSnapshot {
                current_namesrv: Some("127.0.0.1:9876".to_string()),
                namesrv_addr_list: vec!["127.0.0.1:9876".to_string()],
                use_vip_channel: true,
                use_tls: false,
            },
            test_client_runtime(),
            reset_spy.clone(),
        );

        runtime
            .apply_snapshot(&NameServerConfigSnapshot {
                current_namesrv: Some("127.0.0.2:9876".to_string()),
                namesrv_addr_list: vec!["127.0.0.1:9876".to_string(), "127.0.0.2:9876".to_string()],
                use_vip_channel: false,
                use_tls: true,
            })
            .expect("runtime apply should succeed");

        assert_eq!(runtime.snapshot().current_namesrv.as_deref(), Some("127.0.0.2:9876"));
        assert_eq!(runtime.generation(), 1);
        assert_eq!(reset_spy.count(), 1);
    }
}
