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
use rocketmq_mcp_control::transport;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;

const CONFIG_PATH_ENV: &str = "ROCKETMQ_MCP_CONTROL_CONFIG";
const RUNTIME_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(8);

fn main() -> Result<(), ControlError> {
    let config_path = std::env::var(CONFIG_PATH_ENV).map_err(|_| ControlError::invalid_config())?;
    let config = ControlConfig::load(config_path)?;
    let mut runtime_config = RuntimeConfig::server_default("rocketmq-mcp-control");
    runtime_config.shutdown_timeout = RUNTIME_SHUTDOWN_TIMEOUT;
    let owner = RuntimeOwner::plan(runtime_config)
        .expect("runtime configuration is valid")
        .build()
        .map_err(|_| ControlError::execution_failed())?;
    let service_context = owner.root_context().component("rocketmq-mcp-control");
    let result = owner.block_on(run(config, service_context));
    let shutdown = owner
        .shutdown_runtime_blocking_with_timeout(RUNTIME_SHUTDOWN_TIMEOUT)
        .map_err(|_| ControlError::shutdown_failed())?;
    if !shutdown.is_healthy() {
        return Err(ControlError::shutdown_failed());
    }
    result
}

async fn run(
    config: ControlConfig,
    service_context: rocketmq_runtime::ChildServiceContext,
) -> Result<(), ControlError> {
    let sink = JsonlAuditSink::open(&config.audit.path, config.audit.capacity, config.audit.max_record_bytes).await?;
    let audit = AuditTrail::resume(Arc::new(sink)).await?;
    transport::serve(config, service_context, audit, async {
        if rocketmq_runtime::wait_for_signal_result().await.is_err() {
            tracing::warn!("control termination signal observation failed");
        }
    })
    .await
}
