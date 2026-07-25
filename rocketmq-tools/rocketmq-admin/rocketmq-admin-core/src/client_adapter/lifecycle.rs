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

//! Client lifecycle adapter for an opaque admin session.

use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_client_rust::ClientRuntime;
use rocketmq_client_rust::DefaultMQAdminExt;
use rocketmq_error::RocketMQError;

use crate::core::clock::Clock;
use crate::core::AdminError;
use crate::core::AdminResult;

/// Builds an SDK-backed admin session within an explicitly owned client runtime.
#[derive(Clone)]
pub struct AdminBuilder {
    client_runtime: Arc<ClientRuntime>,
    config: crate::core::admin::AdminBuilder,
}

impl std::fmt::Debug for AdminBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AdminBuilder")
            .field("client_runtime", &"explicit")
            .field("config", &self.config)
            .finish()
    }
}

impl AdminBuilder {
    pub fn new(client_runtime: Arc<ClientRuntime>) -> Self {
        Self {
            client_runtime,
            config: crate::core::admin::AdminBuilder::new(),
        }
    }

    pub fn namesrv_addr(mut self, addr: impl Into<String>) -> Self {
        self.config = self.config.namesrv_addr(addr);
        self
    }

    pub fn admin_group(mut self, group: impl Into<String>) -> Self {
        self.config = self.config.admin_group(group);
        self
    }

    pub fn instance_name(mut self, name: impl Into<String>) -> Self {
        self.config = self.config.instance_name(name);
        self
    }

    pub fn timeout_millis(mut self, timeout_millis: u64) -> Self {
        self.config = self.config.timeout_millis(timeout_millis);
        self
    }

    pub fn unit_name(mut self, name: impl Into<String>) -> Self {
        self.config = self.config.unit_name(name);
        self
    }

    pub fn vip_channel_enabled(mut self, enabled: bool) -> Self {
        self.config = self.config.vip_channel_enabled(enabled);
        self
    }

    pub fn use_tls(mut self, use_tls: bool) -> Self {
        self.config = self.config.use_tls(use_tls);
        self
    }

    pub fn clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.config = self.config.clock(clock);
        self
    }
}

#[must_use = "a started admin session must be explicitly shut down"]
pub struct AdminSession {
    pub(crate) inner: DefaultMQAdminExt,
    client_runtime: Arc<ClientRuntime>,
    pub(crate) clock: Arc<dyn Clock>,
    closed: bool,
}

impl AdminSession {
    pub(crate) fn from_started(inner: DefaultMQAdminExt, clock: Arc<dyn Clock>) -> Self {
        let client_runtime = inner.client_runtime();
        Self {
            inner,
            client_runtime,
            clock,
            closed: false,
        }
    }

    pub(crate) fn client(&self) -> &DefaultMQAdminExt {
        &self.inner
    }

    pub(crate) fn client_mut(&mut self) -> &mut DefaultMQAdminExt {
        &mut self.inner
    }

    pub async fn shutdown(&mut self) {
        if !self.closed {
            self.inner.shutdown().await;
            self.closed = true;
        }
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }

    pub fn client_runtime(&self) -> Arc<ClientRuntime> {
        Arc::clone(&self.client_runtime)
    }

    pub async fn probe_name_server(&self, name_server: &str) -> AdminResult<()> {
        self.ensure_open()?;
        use rocketmq_client_rust::MQAdminExt;
        self.inner
            .probe_name_server(name_server.into())
            .await
            .map_err(|error| backend_error("probe_name_server", error))
    }

    pub(crate) fn ensure_open(&self) -> AdminResult<()> {
        if self.closed {
            Err(AdminError::SessionClosed)
        } else {
            Ok(())
        }
    }
}

impl Drop for AdminSession {
    fn drop(&mut self) {
        if !self.closed {
            tracing::warn!("admin session dropped before explicit shutdown");
        }
    }
}

#[must_use = "the guard owns a live admin session; call shutdown when the workflow completes"]
pub struct AdminGuard {
    session: Option<AdminSession>,
}

impl AdminGuard {
    pub(crate) fn new(session: AdminSession) -> Self {
        Self { session: Some(session) }
    }

    pub async fn shutdown(mut self) {
        if let Some(mut session) = self.session.take() {
            session.shutdown().await;
        }
    }

    /// Returns the live admin session.
    ///
    /// # Panics
    ///
    /// Panics only if the internal session has already been consumed. Public
    /// APIs consume the guard together with the session, so this state cannot
    /// be observed through safe code.
    pub fn inner(&self) -> &AdminSession {
        self.session.as_ref().expect("AdminGuard already consumed")
    }

    /// Returns the live admin session mutably.
    ///
    /// # Panics
    ///
    /// Panics only if the internal session has already been consumed. Public
    /// APIs consume the guard together with the session, so this state cannot
    /// be observed through safe code.
    pub fn inner_mut(&mut self) -> &mut AdminSession {
        self.session.as_mut().expect("AdminGuard already consumed")
    }
}

impl Deref for AdminGuard {
    type Target = AdminSession;

    fn deref(&self) -> &Self::Target {
        self.inner()
    }
}

impl DerefMut for AdminGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner_mut()
    }
}

impl AdminBuilder {
    pub async fn build_and_start(self) -> AdminResult<AdminSession> {
        let client_runtime = self.client_runtime;
        let config = self.config;
        let clock = config.configured_clock();
        let now_millis = clock.now_millis();
        let admin_group = config
            .configured_admin_group()
            .map(str::to_owned)
            .unwrap_or_else(|| format!("tools-admin-{now_millis}"));
        let mut admin = DefaultMQAdminExt::with_admin_ext_group_and_timeout(
            client_runtime.clone(),
            admin_group,
            Duration::from_millis(config.configured_timeout_millis()),
        );
        if let Some(namesrv_addr) = config.configured_namesrv_addr() {
            admin.set_namesrv_addr(namesrv_addr);
        }

        let instance_name = config
            .configured_instance_name()
            .map(str::to_owned)
            .unwrap_or_else(|| format!("tools-{now_millis}"));
        let client_config = admin.client_config_mut();
        client_config.set_instance_name(instance_name.into());
        client_config.set_vip_channel_enabled(config.configured_vip_channel_enabled());
        if let Some(unit_name) = config.configured_unit_name() {
            client_config.set_unit_name(unit_name.into());
        }
        admin.set_use_tls(config.configured_use_tls());
        admin
            .start()
            .await
            .map_err(|error| backend_error("start_admin_session", error))?;

        Ok(AdminSession {
            inner: admin,
            client_runtime,
            clock,
            closed: false,
        })
    }

    pub async fn build_with_guard(self) -> AdminResult<AdminGuard> {
        self.build_and_start().await.map(AdminGuard::new)
    }
}

fn backend_error(operation: &'static str, error: RocketMQError) -> AdminError {
    let view = error.boundary_view();
    let context = (!view.context().is_empty()).then(|| view.context().to_string());
    AdminError::backend_view(
        operation,
        view.code().as_str(),
        view.message(),
        context,
        view.http().status.as_u16(),
        view.is_retryable(),
    )
}
