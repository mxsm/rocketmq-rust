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

use rocketmq_client_rust::admin_adapter_compat::error::RocketMQError;
use rocketmq_client_rust::DefaultMQAdminExt;

use crate::core::clock::Clock;
use crate::core::clock::SystemClock;
use crate::core::AdminError;
use crate::core::AdminResult;

#[derive(Clone)]
pub struct ClientAdminBuilder {
    namesrv_addr: Option<String>,
    admin_group: Option<String>,
    instance_name: Option<String>,
    timeout_millis: u64,
    unit_name: Option<String>,
    vip_channel_enabled: bool,
    use_tls: bool,
    clock: Arc<dyn Clock>,
}

impl Default for ClientAdminBuilder {
    fn default() -> Self {
        Self {
            namesrv_addr: None,
            admin_group: None,
            instance_name: None,
            timeout_millis: 5_000,
            unit_name: None,
            vip_channel_enabled: false,
            use_tls: false,
            clock: Arc::new(SystemClock),
        }
    }
}

impl std::fmt::Debug for ClientAdminBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClientAdminBuilder")
            .field("namesrv_addr", &self.namesrv_addr)
            .field("admin_group", &self.admin_group)
            .field("instance_name", &self.instance_name)
            .field("timeout_millis", &self.timeout_millis)
            .field("unit_name", &self.unit_name)
            .field("vip_channel_enabled", &self.vip_channel_enabled)
            .field("use_tls", &self.use_tls)
            .field("clock", &"dyn Clock")
            .finish()
    }
}

impl ClientAdminBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn namesrv_addr(mut self, addr: impl Into<String>) -> Self {
        self.namesrv_addr = Some(addr.into());
        self
    }

    pub fn admin_group(mut self, group: impl Into<String>) -> Self {
        self.admin_group = Some(group.into());
        self
    }

    pub fn instance_name(mut self, name: impl Into<String>) -> Self {
        self.instance_name = Some(name.into());
        self
    }

    pub fn timeout_millis(mut self, timeout_millis: u64) -> Self {
        self.timeout_millis = timeout_millis;
        self
    }

    pub fn unit_name(mut self, name: impl Into<String>) -> Self {
        self.unit_name = Some(name.into());
        self
    }

    pub fn vip_channel_enabled(mut self, enabled: bool) -> Self {
        self.vip_channel_enabled = enabled;
        self
    }

    pub fn use_tls(mut self, use_tls: bool) -> Self {
        self.use_tls = use_tls;
        self
    }

    pub fn clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    pub async fn build_and_start(self) -> AdminResult<AdminSession> {
        let admin_group = self
            .admin_group
            .unwrap_or_else(|| format!("tools-admin-{}", self.clock.now_millis()));
        let mut admin = DefaultMQAdminExt::with_admin_ext_group_and_timeout(
            admin_group,
            Duration::from_millis(self.timeout_millis),
        );
        if let Some(namesrv_addr) = self.namesrv_addr {
            admin.set_namesrv_addr(namesrv_addr);
        }
        let instance_name = self
            .instance_name
            .unwrap_or_else(|| format!("tools-{}", self.clock.now_millis()));
        let client_config = admin.client_config_mut();
        client_config.set_instance_name(instance_name.into());
        client_config.set_vip_channel_enabled(self.vip_channel_enabled);
        if let Some(unit_name) = self.unit_name {
            client_config.set_unit_name(unit_name.into());
        }
        admin.set_use_tls(self.use_tls);
        admin
            .start()
            .await
            .map_err(|error| backend_error("start_admin_session", error))?;

        Ok(AdminSession {
            inner: admin,
            clock: self.clock,
            closed: false,
        })
    }

    pub async fn build_with_guard(self) -> AdminResult<AdminGuard> {
        self.build_and_start().await.map(AdminGuard::new)
    }
}

pub struct AdminSession {
    pub(crate) inner: DefaultMQAdminExt,
    pub(crate) clock: Arc<dyn Clock>,
    closed: bool,
}

impl AdminSession {
    pub async fn shutdown(&mut self) {
        if !self.closed {
            self.inner.shutdown().await;
            self.closed = true;
        }
    }

    pub fn is_closed(&self) -> bool {
        self.closed
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

#[cfg(not(feature = "legacy-common-compat"))]
impl crate::core::admin::AdminBuilder {
    pub async fn build_and_start(self) -> AdminResult<AdminSession> {
        let mut builder = ClientAdminBuilder::new()
            .timeout_millis(self.configured_timeout_millis())
            .clock(self.configured_clock());
        if let Some(namesrv_addr) = self.configured_namesrv_addr() {
            builder = builder.namesrv_addr(namesrv_addr);
        }
        if let Some(instance_name) = self.configured_instance_name() {
            builder = builder.instance_name(instance_name);
        }
        builder.build_and_start().await
    }

    pub async fn build_with_guard(self) -> AdminResult<AdminGuard> {
        self.build_and_start().await.map(AdminGuard::new)
    }
}

pub async fn create_admin(namesrv_addr: impl Into<String>) -> AdminResult<AdminSession> {
    ClientAdminBuilder::new()
        .namesrv_addr(namesrv_addr)
        .build_and_start()
        .await
}

pub async fn create_admin_with_guard(namesrv_addr: impl Into<String>) -> AdminResult<AdminGuard> {
    ClientAdminBuilder::new()
        .namesrv_addr(namesrv_addr)
        .build_with_guard()
        .await
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
