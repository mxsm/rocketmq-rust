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

use std::sync::Arc;

use rocketmq_client_rust::ClientRuntime;
use rocketmq_client_rust::ClientRuntimeConfig;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;

pub struct ExampleClientRuntime {
    owner: RuntimeOwner,
    client_runtime: Arc<ClientRuntime>,
    telemetry_guard: rocketmq_observability::TelemetryRuntimeGuard,
}

impl ExampleClientRuntime {
    pub fn try_new(scope: &str) -> RocketMQResult<Self> {
        let owner = RuntimeOwner::new(RuntimeConfig {
            thread_name: format!("rocketmq-client-example-{scope}"),
            ..Default::default()
        })
        .map_err(|source| RocketMQError::internal("create client example runtime", source))?;
        let telemetry_guard =
            rocketmq_observability::install_global(&rocketmq_observability::TelemetryBootstrapConfig::default())
                .map_err(|source| RocketMQError::internal("initialize client example telemetry", source))?;
        let client_runtime = ClientRuntime::try_new(
            owner.root_context().child("client"),
            ClientRuntimeConfig::default(),
            telemetry_guard.handle(),
        )?;
        Ok(Self {
            owner,
            client_runtime,
            telemetry_guard,
        })
    }

    pub fn client_runtime(&self) -> Arc<ClientRuntime> {
        Arc::clone(&self.client_runtime)
    }

    pub async fn shutdown(self) {
        let client_report = self.client_runtime.shutdown().await;
        assert!(client_report.is_healthy(), "{}", client_report.to_json());

        let owner_report = self.owner.shutdown_tasks().await;
        assert!(owner_report.is_healthy(), "{}", owner_report.to_json());

        let background_report = self.owner.shutdown_background();
        assert!(background_report.is_healthy(), "{}", background_report.to_json());

        let telemetry_report = self.telemetry_guard.shutdown();
        assert!(telemetry_report.is_healthy(), "{}", telemetry_report.to_json());
    }
}
