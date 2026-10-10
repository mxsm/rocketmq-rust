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

use std::sync::Arc;
use std::time::Duration;

use rocketmq_mcp_control::audit::AuditTrail;
use rocketmq_mcp_control::audit::JsonlAuditSink;
use rocketmq_mcp_control::config::ControlConfig;
use rocketmq_mcp_control::error::ControlError;
use rocketmq_mcp_control::telemetry::ControlSignals;
use rocketmq_mcp_control::telemetry::Telemetry;
use rocketmq_mcp_control::transport;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;

const CONFIG_PATH_ENV: &str = "ROCKETMQ_MCP_CONTROL_CONFIG";
const RUNTIME_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(8);
/// Covers a final metrics export that runs into the exporter timeout more than once. A flush
/// that outlives this budget is still running when the runtime shuts down and makes that
/// shutdown unhealthy.
const TELEMETRY_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

fn main() -> Result<(), ControlError> {
    // The runtime and the log subscriber start before the configuration is read, so that a
    // rejected configuration can be explained.
    let mut runtime_config = RuntimeConfig::server_default("rocketmq-mcp-control");
    runtime_config.shutdown_timeout = RUNTIME_SHUTDOWN_TIMEOUT;
    let owner = RuntimeOwner::plan(runtime_config)
        .expect("runtime configuration is valid")
        .build()
        .map_err(|_| ControlError::execution_failed())?;
    let service_context = owner.root_context().component("rocketmq-mcp-control");
    let result = owner.block_on(run(service_context));
    let shutdown = owner
        .shutdown_runtime_blocking_with_timeout(RUNTIME_SHUTDOWN_TIMEOUT)
        .map_err(|_| ControlError::shutdown_failed())?;
    if !shutdown.is_healthy() {
        return Err(ControlError::shutdown_failed());
    }
    result
}

async fn run(service_context: ChildServiceContext) -> Result<(), ControlError> {
    let telemetry_context = service_context.component("telemetry");
    let telemetry = Telemetry::install(&telemetry_context).await?;
    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        write_tools_compiled = cfg!(feature = "write-tools"),
        "rocketmq-mcp-control is starting"
    );
    let result = serve(service_context, telemetry.signals()).await;
    match &result {
        Ok(()) => tracing::info!("rocketmq-mcp-control stopped"),
        Err(error) => tracing::error!(
            code = error.code().as_str(),
            "rocketmq-mcp-control stopped with an error"
        ),
    }
    telemetry.shutdown(&telemetry_context, TELEMETRY_SHUTDOWN_TIMEOUT).await;
    result
}

async fn serve(service_context: ChildServiceContext, signals: ControlSignals) -> Result<(), ControlError> {
    let config = load_config()?;
    let sink = JsonlAuditSink::open(&config.audit.path, config.audit.capacity, config.audit.max_record_bytes)
        .await
        .map_err(audit_startup_failed)?;
    let audit = AuditTrail::resume(Arc::new(sink)).await.map_err(audit_startup_failed)?;
    transport::serve_with_signals(config, service_context, audit, signals, async {
        if rocketmq_runtime::wait_for_signal_result().await.is_err() {
            tracing::warn!("control termination signal observation failed");
        }
    })
    .await
}

fn load_config() -> Result<ControlConfig, ControlError> {
    let Ok(config_path) = std::env::var(CONFIG_PATH_ENV) else {
        tracing::error!(
            stage = "locate",
            variable = CONFIG_PATH_ENV,
            "control configuration was rejected"
        );
        return Err(ControlError::invalid_config());
    };
    ControlConfig::load_detailed(config_path).map_err(|error| {
        tracing::error!(
            stage = error.stage(),
            reason = %error,
            "control configuration was rejected"
        );
        error.into()
    })
}

fn audit_startup_failed(error: ControlError) -> ControlError {
    tracing::error!(stage = "audit", code = error.code().as_str(), "control server failed");
    error
}
