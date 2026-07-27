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

use std::future::Future;
use std::sync::Arc;

use rocketmq_client_rust::ClientRuntime;
use rocketmq_client_rust::ClientRuntimeConfig;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;

pub fn run<F, Fut>(operation: F) -> RocketMQResult<()>
where
    F: FnOnce(Arc<ClientRuntime>) -> Fut,
    Fut: Future<Output = RocketMQResult<()>>,
{
    let owner = RuntimeOwner::new(RuntimeConfig::server_default("rocketmq-example"))
        .map_err(|source| RocketMQError::internal("create example runtime", source))?;
    let telemetry_guard =
        rocketmq_observability::install_global(&rocketmq_observability::TelemetryBootstrapConfig::default())
            .map_err(|source| RocketMQError::internal("initialize example telemetry", source))?;
    let client_runtime = ClientRuntime::new(
        owner.root_context().child("client"),
        ClientRuntimeConfig::default(),
        telemetry_guard.handle(),
    );

    let operation_result = owner.block_on(async {
        let result = operation(Arc::clone(&client_runtime)).await;
        let report = client_runtime.shutdown().await;
        report.log_if_unhealthy();
        result
    });
    let shutdown_result = owner
        .shutdown_runtime_blocking()
        .map_err(|source| RocketMQError::internal("shut down example runtime", source));
    let telemetry_result = telemetry_guard
        .shutdown()
        .into_result()
        .map_err(|source| RocketMQError::internal("shut down example telemetry", source));

    operation_result
        .and(shutdown_result.map(|_| ()))
        .and(telemetry_result.map(|_| ()))
}
