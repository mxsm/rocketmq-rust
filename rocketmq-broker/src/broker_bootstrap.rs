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
}

impl Builder {
    #[inline]
    pub fn new() -> Self {
        Builder {
            broker_config: Default::default(),
            message_store_config: MessageStoreConfig::default(),
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
    pub fn build(self) -> BrokerBootstrap {
        BrokerBootstrap {
            broker_runtime: BrokerRuntime::new(Arc::new(self.broker_config), Arc::new(self.message_store_config)),
        }
    }
}

impl Default for Builder {
    fn default() -> Self {
        Self::new()
    }
}
