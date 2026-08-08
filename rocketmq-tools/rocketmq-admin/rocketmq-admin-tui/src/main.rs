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

// The facade intentionally awaits the complete admin -> client -> transport call graph. Keep a
// bounded compiler query budget above rustc's default as those typed async layers evolve.
#![recursion_limit = "256"]

mod action;
mod admin_facade;
mod commands;
mod event;
mod rocketmq_tui_app;
mod state;
mod ui;
mod view_model;

use crate::rocketmq_tui_app::RocketmqTuiApp;

use anyhow::Context;
use rocketmq_admin_core::client_adapter::ClientRuntime;
use rocketmq_admin_core::client_adapter::ClientRuntimeConfig;
use rocketmq_admin_core::client_adapter::TelemetryHandle;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;

fn main() -> anyhow::Result<()> {
    let owner = RuntimeOwner::new(admin_tui_runtime_config()).context("failed to build rocketmq-admin-tui runtime")?;
    let client_runtime = ClientRuntime::try_new(
        owner.root_context().component("rocketmq-admin-client"),
        ClientRuntimeConfig::default(),
        TelemetryHandle::noop(),
    )
    .context("failed to initialize rocketmq-admin-tui client runtime")?;
    let run_result = owner.block_on(async {
        let run_result = run(client_runtime.clone()).await;
        let report = client_runtime.shutdown().await;
        if !report.is_healthy() {
            tracing::warn!(
                report = %report.to_json(),
                "rocketmq-admin-tui client runtime shutdown report is unhealthy"
            );
        }
        run_result
    });
    let shutdown_result = owner
        .shutdown_runtime_blocking()
        .context("failed to shutdown rocketmq-admin-tui runtime");

    match (run_result, shutdown_result) {
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(report)) => {
            if !report.is_healthy() {
                tracing::warn!(
                    report = %report.to_json(),
                    "rocketmq-admin-tui runtime shutdown report is unhealthy"
                );
            }
            Ok(())
        }
    }
}

fn admin_tui_runtime_config() -> RuntimeConfig {
    RuntimeConfig::server_default("rocketmq-admin-tui")
}

async fn run(client_runtime: std::sync::Arc<ClientRuntime>) -> anyhow::Result<()> {
    let terminal = ratatui::try_init()?;
    let local = tokio::task::LocalSet::new();
    let result = local.run_until(RocketmqTuiApp::new(client_runtime).run(terminal)).await;
    ratatui::try_restore()?;
    result
}

#[cfg(test)]
mod tests {
    use rocketmq_admin_core::client_adapter::AdminBuilder;

    use crate::admin_facade::test_client_runtime;
    use crate::admin_facade::TuiAdminFacade;

    #[test]
    fn admin_facade_builds_core_admin_builder() {
        let facade = TuiAdminFacade::with_namesrv_addr(test_client_runtime(), "127.0.0.1:9876");

        assert_eq!(facade.namesrv_addr(), Some("127.0.0.1:9876"));

        let _builder: AdminBuilder = facade.admin_builder();
    }
}
