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

//! Unique application-owned runtime and RocketMQ client lifecycle.

use std::{sync::Arc, time::Duration};

use rocketmq_admin_core::read_client_adapter::{ClientRuntime, ClientRuntimeConfig, TelemetryHandle};
use rocketmq_runtime::{ChildServiceContext, RuntimeConfig, RuntimeOwner, ShutdownReport};

use super::admin_provider::GpuiAdminProvider;

/// Runtime bootstrap or shutdown failure at the process boundary.
#[derive(Debug, thiserror::Error)]
pub enum DesktopRuntimeError {
    /// Unified runtime bootstrap or shutdown failed.
    #[error("dashboard runtime failed: {0}")]
    Runtime(String),
    /// RocketMQ client runtime bootstrap failed.
    #[error("RocketMQ client runtime failed: {0}")]
    Client(String),
    /// Shutdown was requested more than once.
    #[error("dashboard runtime is already shut down")]
    AlreadyShutdown,
}

/// The only runtime owner created by the desktop process.
pub struct DesktopClientRuntime {
    owner: Option<RuntimeOwner>,
    application_context: ChildServiceContext,
    work_context: ChildServiceContext,
    client_runtime: Arc<ClientRuntime>,
}

impl DesktopClientRuntime {
    /// Creates one RuntimeOwner, one application child, and one ClientRuntime.
    pub fn new(telemetry: TelemetryHandle) -> Result<Self, DesktopRuntimeError> {
        let owner = RuntimeOwner::new(RuntimeConfig::for_parallelism(
            "rocketmq-dashboard-gpui",
            std::thread::available_parallelism().map_or(1, |parallelism| parallelism.get()),
        ))
        .map_err(|error| DesktopRuntimeError::Runtime(error.to_string()))?;
        let application_context = owner.root_context().component("dashboard-gpui");
        let work_context = application_context.component("application-work");
        let client_runtime = ClientRuntime::try_new(
            application_context.component("client-runtime"),
            ClientRuntimeConfig::default(),
            telemetry,
        )
        .map_err(|error| DesktopRuntimeError::Client(error.to_string()))?;
        Ok(Self {
            owner: Some(owner),
            application_context,
            work_context,
            client_runtime,
        })
    }

    /// Returns a named application-owned child scope.
    pub fn component(&self, name: &'static str) -> ChildServiceContext {
        self.work_context.component(name)
    }

    /// Returns a provider scope that stays alive while application work is cancelled and drained.
    pub fn provider_component(&self, name: &'static str) -> ChildServiceContext {
        self.application_context.component(name)
    }

    /// Returns the single RocketMQ client runtime shared by every Admin session.
    pub fn client_runtime(&self) -> Arc<ClientRuntime> {
        Arc::clone(&self.client_runtime)
    }

    /// Closes provider sessions, then the ClientRuntime, then all remaining owned tasks.
    /// Telemetry remains the caller's owner and must be shut down after this returns.
    pub fn shutdown(mut self, provider: Arc<GpuiAdminProvider>) -> Result<ShutdownReport, DesktopRuntimeError> {
        let owner = self.owner.take().ok_or(DesktopRuntimeError::AlreadyShutdown)?;
        owner.block_on(async {
            self.work_context.task_group().cancel();
            let work_report = self.work_context.task_group().shutdown(Duration::from_secs(5)).await;
            work_report.log_if_unhealthy();
            provider.shutdown().await;
            self.client_runtime.shutdown().await;
        });
        drop(provider);
        drop(self.client_runtime);
        owner
            .shutdown_runtime_blocking()
            .map_err(|error| DesktopRuntimeError::Runtime(error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infrastructure::{admin_provider::GpuiAdminProvider, auth_state::DesktopAuthState};

    #[test]
    fn application_shutdown_closes_client_and_awaits_owned_tasks() {
        let runtime = DesktopClientRuntime::new(TelemetryHandle::noop()).expect("runtime");
        let provider = GpuiAdminProvider::new(
            runtime.provider_component("provider"),
            runtime.client_runtime(),
            DesktopAuthState::from_process_environment(),
        );
        let report = runtime.shutdown(provider).expect("shutdown");

        assert_eq!(report.leaked, 0);
        assert_eq!(report.timed_out, 0);
    }
}
