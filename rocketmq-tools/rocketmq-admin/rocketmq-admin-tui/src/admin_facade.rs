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

use rocketmq_admin_core::client_adapter::services::message::MessagePullEvent;
use std::sync::Arc;

use rocketmq_admin_core::client_adapter::AdminBuilder;
use rocketmq_admin_core::client_adapter::ClientRuntime;

#[derive(Clone)]
pub struct TuiAdminFacade {
    client_runtime: Arc<ClientRuntime>,
    namesrv_addr: Option<String>,
}

impl std::fmt::Debug for TuiAdminFacade {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TuiAdminFacade")
            .field("namesrv_addr", &self.namesrv_addr)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone)]
pub struct MessagePullCapture {
    pub events: Vec<MessagePullEvent>,
    pub event_limit: usize,
    pub truncated: bool,
}

impl TuiAdminFacade {
    pub fn new(client_runtime: Arc<ClientRuntime>) -> Self {
        Self {
            client_runtime,
            namesrv_addr: None,
        }
    }

    #[allow(dead_code)]
    pub fn with_namesrv_addr(client_runtime: Arc<ClientRuntime>, addr: impl Into<String>) -> Self {
        Self {
            client_runtime,
            namesrv_addr: Some(addr.into()),
        }
    }

    pub fn set_namesrv_addr(&mut self, addr: Option<String>) {
        self.namesrv_addr = addr.map(|addr| addr.trim().to_string()).filter(|addr| !addr.is_empty());
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    #[allow(dead_code)]
    pub fn admin_builder(&self) -> AdminBuilder {
        let builder = AdminBuilder::new(Arc::clone(&self.client_runtime));
        match &self.namesrv_addr {
            Some(addr) => builder.namesrv_addr(addr),
            None => builder,
        }
    }

    pub(crate) fn client_runtime(&self) -> Arc<ClientRuntime> {
        Arc::clone(&self.client_runtime)
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
        RuntimeOwner::plan(RuntimeConfig::server_default("rocketmq-admin-tui-test"))
            .expect("runtime configuration is valid")
            .build()
            .expect("admin TUI test runtime should start")
    });

    ClientRuntime::try_new(
        OWNER.root_context().component("client"),
        ClientRuntimeConfig::default(),
        TelemetryHandle::noop(),
    )
    .expect("admin TUI test client runtime should be valid")
}

mod operations;

#[cfg(test)]
mod tests;
