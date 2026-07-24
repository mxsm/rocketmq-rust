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

//! Internal session builder for Client-backed command services.

use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_client_rust::admin_adapter_compat::remoting::runtime::RPCHook;
use rocketmq_client_rust::DefaultMQAdminExt;
use rocketmq_common::TimeUtils::current_millis;

use crate::client_adapter::lifecycle::AdminSession;
use crate::client_adapter::security::rpc_hook_from_credentials;
use crate::client_adapter::services::RocketMQResult;
use crate::core::clock::SystemClock;
use crate::core::security::AdminCredentials;

#[derive(Clone, Default)]
pub(crate) struct AdminBuilder {
    namesrv_addr: Option<String>,
    timeout_millis: Option<u64>,
    rpc_hook: Option<Arc<dyn RPCHook>>,
}

impl std::fmt::Debug for AdminBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AdminBuilder")
            .field("namesrv_addr", &self.namesrv_addr)
            .field("timeout_millis", &self.timeout_millis)
            .field("rpc_hook", &self.rpc_hook.is_some())
            .finish()
    }
}

impl AdminBuilder {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn namesrv_addr(mut self, addr: impl Into<String>) -> Self {
        self.namesrv_addr = Some(addr.into());
        self
    }

    pub(crate) fn timeout_millis(mut self, timeout: u64) -> Self {
        self.timeout_millis = Some(timeout);
        self
    }

    pub(crate) fn rpc_hook(mut self, hook: Arc<dyn RPCHook>) -> Self {
        self.rpc_hook = Some(hook);
        self
    }

    pub(crate) fn credentials(self, credentials: AdminCredentials) -> Self {
        self.rpc_hook(rpc_hook_from_credentials(&credentials))
    }

    pub(crate) async fn build_and_start(self) -> RocketMQResult<ServiceAdminSession> {
        let timeout = self.timeout_millis.map(Duration::from_millis);
        let mut admin = match (self.rpc_hook, timeout) {
            (Some(hook), Some(timeout)) => DefaultMQAdminExt::with_rpc_hook_and_timeout(hook, timeout),
            (Some(hook), None) => DefaultMQAdminExt::with_rpc_hook(hook),
            (None, Some(timeout)) => DefaultMQAdminExt::with_timeout(timeout),
            (None, None) => DefaultMQAdminExt::new(),
        };

        if let Some(addr) = self.namesrv_addr {
            admin.set_namesrv_addr(addr);
        }

        let instance_name = format!("tools-{}", current_millis());
        let client_config = admin.client_config_mut();
        client_config.set_instance_name(instance_name.into());

        admin.start().await?;
        Ok(ServiceAdminSession {
            session: AdminSession::from_started(admin, Arc::new(SystemClock)),
        })
    }
}

/// Private bridge for command services that still call SDK operations.
///
/// The wrapper keeps the SDK handle inaccessible to external consumers while
/// routing all lifecycle ownership through the canonical [`AdminSession`].
#[must_use = "a started admin session must be explicitly shut down"]
pub(crate) struct ServiceAdminSession {
    session: AdminSession,
}

impl ServiceAdminSession {
    pub(crate) fn from_started(admin: DefaultMQAdminExt) -> Self {
        Self {
            session: AdminSession::from_started(admin, Arc::new(SystemClock)),
        }
    }

    pub(crate) async fn shutdown(&mut self) {
        self.session.shutdown().await;
    }
}

impl Deref for ServiceAdminSession {
    type Target = DefaultMQAdminExt;

    fn deref(&self) -> &Self::Target {
        self.session.client()
    }
}

impl DerefMut for ServiceAdminSession {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.session.client_mut()
    }
}

#[cfg(test)]
mod tests {
    use super::AdminBuilder;

    #[test]
    fn builder_owns_command_session_configuration() {
        let builder = AdminBuilder::new().namesrv_addr("127.0.0.1:9876").timeout_millis(5_000);

        assert_eq!(builder.namesrv_addr.as_deref(), Some("127.0.0.1:9876"));
        assert_eq!(builder.timeout_millis, Some(5_000));
    }
}
