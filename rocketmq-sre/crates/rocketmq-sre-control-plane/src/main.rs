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

use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_sre_control_plane::ControlPlaneConfig;
use tracing_subscriber::EnvFilter;

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("rocketmq_sre_control_plane=info")),
        )
        .json()
        .try_init()?;

    let config = ControlPlaneConfig::from_env()?;
    let mut runtime_config = RuntimeConfig::server_default("rocketmq-sre-control-plane");
    runtime_config.shutdown_timeout = config.shutdown_timeout();
    let runtime_owner = RuntimeOwner::new(runtime_config)?;
    let service_context = runtime_owner.root_context().child("rocketmq-sre-control-plane.http");

    let service_result = runtime_owner.block_on(rocketmq_sre_control_plane::run(config, service_context));
    let shutdown_result = runtime_owner.shutdown_runtime_blocking();
    service_result?;
    shutdown_result?;
    Ok(())
}
