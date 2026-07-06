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

use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_observability::TelemetryRuntimeGuard;
use rocketmq_runtime::ServiceContext;
use rocketmq_rust::wait_for_signal;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use tracing::error;
use tracing::info;

use crate::broker_runtime::BrokerRuntime;

pub struct BrokerBootstrap {
    broker_runtime: BrokerRuntime,
}

impl BrokerBootstrap {
    pub async fn boot(mut self) {
        if !self.initialize().await {
            error!("Broker initialization failed");
            return;
        }

        // Start broker services (non-blocking)
        self.start().await;

        // Wait for shutdown signal (Ctrl+C or SIGTERM)
        wait_for_signal().await;
        info!("Broker received shutdown signal");

        // Graceful shutdown
        self.shutdown().await;
        info!("Broker shutdown completed");
    }

    #[inline]
    async fn initialize(&mut self) -> bool {
        self.broker_runtime.initialize().await
    }

    #[inline]
    async fn start(&mut self) {
        self.broker_runtime.start().await;
    }

    #[inline]
    async fn shutdown(&mut self) {
        self.broker_runtime.shutdown().await;
    }
}

pub struct Builder {
    broker_config: BrokerConfig,
    message_store_config: MessageStoreConfig,
    service_context: Option<ServiceContext>,
    telemetry_runtime_guard: Option<TelemetryRuntimeGuard>,
}

impl Builder {
    #[inline]
    pub fn new() -> Self {
        Builder {
            broker_config: Default::default(),
            message_store_config: MessageStoreConfig::default(),
            service_context: None,
            telemetry_runtime_guard: None,
        }
    }
    #[inline]
    pub fn set_broker_config(mut self, broker_config: BrokerConfig) -> Self {
        self.broker_config = broker_config;
        self
    }
    #[inline]
    pub fn set_message_store_config(mut self, message_store_config: MessageStoreConfig) -> Self {
        self.message_store_config = message_store_config;
        self
    }
    #[inline]
    pub fn set_service_context(mut self, service_context: ServiceContext) -> Self {
        self.service_context = Some(service_context);
        self
    }
    #[inline]
    pub fn set_telemetry_runtime_guard(mut self, telemetry_runtime_guard: TelemetryRuntimeGuard) -> Self {
        self.telemetry_runtime_guard = Some(telemetry_runtime_guard);
        self
    }
    #[inline]
    pub fn build(self) -> BrokerBootstrap {
        let broker_config = Arc::new(self.broker_config);
        let message_store_config = Arc::new(self.message_store_config);
        let mut broker_runtime = match self.service_context {
            Some(service_context) => {
                BrokerRuntime::new_with_service_context(broker_config, message_store_config, service_context)
            }
            None => BrokerRuntime::new(broker_config, message_store_config),
        };
        if let Some(telemetry_runtime_guard) = self.telemetry_runtime_guard {
            broker_runtime.set_telemetry_runtime_guard(telemetry_runtime_guard);
        }

        BrokerBootstrap { broker_runtime }
    }
}

impl Default for Builder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(all(test, feature = "local_file_store"))]
mod tests {
    use rocketmq_runtime::RuntimeContext;

    use super::*;

    #[tokio::test]
    async fn builder_passes_service_context_to_broker_runtime() {
        let context = RuntimeContext::from_current("broker-bootstrap-context-test");
        let service_context = context.service_context("broker-bootstrap-service");

        let mut bootstrap = Builder::new().set_service_context(service_context.clone()).build();

        let broker_task_group = bootstrap
            .broker_runtime
            .inner_for_test()
            .broker_service_task_group()
            .expect("broker service task group should come from service context");

        assert_eq!(broker_task_group.id(), service_context.task_group().id());
    }
}
