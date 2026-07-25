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
use rocketmq_admin_core::client_adapter::ClientRuntimeConfig;
use rocketmq_dashboard_web_backend::config::AppConfig;
use rocketmq_dashboard_web_backend::run;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;

fn main() -> anyhow::Result<()> {
    let owner = RuntimeOwner::new(RuntimeConfig::server_default("rocketmq-dashboard-web-backend"))?;
    let client_runtime = ClientRuntime::new(
        owner.root_context().child("rocketmq-admin-client"),
        ClientRuntimeConfig::default(),
    );
    let config = AppConfig::load()?;
    let environment_filter = rocketmq_observability::read_rust_log()?;
    let resolved_filter =
        rocketmq_observability::LogFilterResolver::resolve(rocketmq_observability::LogFilterInputs {
            environment: environment_filter.as_deref(),
            ..rocketmq_observability::LogFilterInputs::default()
        })?;
    let mut bootstrap = rocketmq_observability::TelemetryBootstrapConfig::default();
    bootstrap.observability.service_name = "rocketmq-dashboard-web-backend".to_string();
    bootstrap.observability.service_namespace = "rocketmq".to_string();
    bootstrap.observability.node_type = "dashboard".to_string();
    bootstrap.observability.node_id = "web-backend".to_string();
    bootstrap.observability.subscriber_install_policy = rocketmq_observability::SubscriberInstallPolicy::Required;
    let telemetry_guard = rocketmq_observability::install_global_with_filter(&bootstrap, resolved_filter.clone())?;
    tracing::info!(
        service = "rocketmq-dashboard-web-backend",
        effective_filter = resolved_filter.filter(),
        filter_source = %resolved_filter.source(),
        subscriber_installed = telemetry_guard.subscriber_install_status().installed,
        reload_enabled = bootstrap.logging.reload.enabled,
        "Dashboard Web telemetry bootstrap initialized"
    );

    let run_result = owner.block_on(async {
        let run_result = run(config, client_runtime.clone()).await;
        let report = client_runtime.shutdown().await;
        report.log_if_unhealthy();
        run_result
    });
    let telemetry_shutdown_result = telemetry_guard.shutdown().into_result();
    let runtime_shutdown_result = owner.shutdown_runtime_blocking();

    run_result?;
    telemetry_shutdown_result?;
    runtime_shutdown_result?;
    Ok(())
}
