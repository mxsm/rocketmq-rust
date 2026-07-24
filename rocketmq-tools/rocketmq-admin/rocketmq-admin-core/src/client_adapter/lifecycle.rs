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
use crate::core::AdminError;
use crate::core::AdminResult;

#[must_use = "a started admin session must be explicitly shut down"]
pub struct AdminSession {
    pub(crate) inner: DefaultMQAdminExt,
    pub(crate) clock: Arc<dyn Clock>,
    closed: bool,
}

impl AdminSession {
    pub(crate) fn from_started(inner: DefaultMQAdminExt, clock: Arc<dyn Clock>) -> Self {
        Self {
            inner,
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

impl crate::core::admin::AdminBuilder {
    pub async fn build_and_start(self) -> AdminResult<AdminSession> {
        let clock = self.configured_clock();
        let now_millis = clock.now_millis();
        let admin_group = self
            .configured_admin_group()
            .map(str::to_owned)
            .unwrap_or_else(|| format!("tools-admin-{now_millis}"));
        let mut admin = DefaultMQAdminExt::with_admin_ext_group_and_timeout(
            admin_group,
            Duration::from_millis(self.configured_timeout_millis()),
        );
        if let Some(namesrv_addr) = self.configured_namesrv_addr() {
            admin.set_namesrv_addr(namesrv_addr);
        }

        let instance_name = self
            .configured_instance_name()
            .map(str::to_owned)
            .unwrap_or_else(|| format!("tools-{now_millis}"));
        let client_config = admin.client_config_mut();
        client_config.set_instance_name(instance_name.into());
        client_config.set_vip_channel_enabled(self.configured_vip_channel_enabled());
        if let Some(unit_name) = self.configured_unit_name() {
            client_config.set_unit_name(unit_name.into());
        }
        admin.set_use_tls(self.configured_use_tls());
        admin
            .start()
            .await
            .map_err(|error| backend_error("start_admin_session", error))?;

        Ok(AdminSession {
            inner: admin,
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
