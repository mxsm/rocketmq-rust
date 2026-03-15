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

use anyhow::Result;
use rocketmq_common::common::mix_all;
use rocketmq_dashboard_common::NameServerConfigSnapshot;
use rocketmq_dashboard_common::NameServerRuntimeAdapter;
use std::env;
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
    reset_hook: Arc<dyn ClientResetHook>,
}

impl NameServerRuntimeState {
    pub(crate) fn new(snapshot: NameServerConfigSnapshot) -> Self {
        Self::with_reset_hook(snapshot, Arc::new(NoopClientResetHook))
    }

    pub(crate) fn with_reset_hook(snapshot: NameServerConfigSnapshot, reset_hook: Arc<dyn ClientResetHook>) -> Self {
        Self {
            state: Mutex::new(RuntimeState {
                snapshot,
                generation: 0,
            }),
            reset_hook,
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn snapshot(&self) -> NameServerConfigSnapshot {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .snapshot
            .clone()
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
    fn apply_snapshot(&self, snapshot: &NameServerConfigSnapshot) -> Result<()> {
        {
            let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            state.snapshot = snapshot.clone();
            state.generation += 1;
        }

        sync_nameserver_process_env(snapshot);
        self.reset_hook.reset_clients();
        Ok(())
    }
}

fn sync_nameserver_process_env(snapshot: &NameServerConfigSnapshot) {
    let Some(current_namesrv) = snapshot.current_namesrv.as_deref() else {
        return;
    };

    // The Rust RocketMQ client resolves NameServer from process environment, so we keep both
    // keys in sync with the current runtime selection.
    unsafe {
        env::set_var(mix_all::NAMESRV_ADDR_ENV, current_namesrv);
        env::set_var(mix_all::NAMESRV_ADDR_PROPERTY, current_namesrv);
    }
}

#[cfg(test)]
mod tests {
    use super::ClientResetHook;
    use super::NameServerRuntimeState;
    use rocketmq_common::common::mix_all;
    use rocketmq_dashboard_common::NameServerConfigSnapshot;
    use rocketmq_dashboard_common::NameServerRuntimeAdapter;
    use std::env;
    use std::sync::Arc;
    use std::sync::LazyLock;
    use std::sync::Mutex;

    static ENV_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

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
        let _env_guard = ENV_LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let previous_env = env::var(mix_all::NAMESRV_ADDR_ENV).ok();
        let previous_property = env::var(mix_all::NAMESRV_ADDR_PROPERTY).ok();
        let reset_spy = Arc::new(ResetSpy::default());
        let runtime = NameServerRuntimeState::with_reset_hook(
            NameServerConfigSnapshot {
                current_namesrv: Some("127.0.0.1:9876".to_string()),
                namesrv_addr_list: vec!["127.0.0.1:9876".to_string()],
                use_vip_channel: true,
                use_tls: false,
            },
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
        assert_eq!(
            env::var(mix_all::NAMESRV_ADDR_ENV).ok().as_deref(),
            Some("127.0.0.2:9876")
        );
        assert_eq!(
            env::var(mix_all::NAMESRV_ADDR_PROPERTY).ok().as_deref(),
            Some("127.0.0.2:9876")
        );

        unsafe {
            match previous_env {
                Some(value) => env::set_var(mix_all::NAMESRV_ADDR_ENV, value),
                None => env::remove_var(mix_all::NAMESRV_ADDR_ENV),
            }
            match previous_property {
                Some(value) => env::set_var(mix_all::NAMESRV_ADDR_PROPERTY, value),
                None => env::remove_var(mix_all::NAMESRV_ADDR_PROPERTY),
            }
        }
    }
}
