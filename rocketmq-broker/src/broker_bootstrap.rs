/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use rocketmq_remoting::server::config::ServerConfig;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use tokio::select;
use tracing::error;

use crate::{broker_config::BrokerConfig, broker_runtime::BrokerRuntime};

pub struct BrokerBootstrap {
    broker_runtime: BrokerRuntime,
}

impl BrokerBootstrap {
    pub async fn boot(mut self) {
        if !self.initialize().await {
            error!("initialize fail");
            return;
        }

        select! {
            _ = self.start() =>{

            }
        }
    }

    async fn initialize(&mut self) -> bool {
        self.broker_runtime.initialize().await
    }

    async fn start(&mut self) {
        self.broker_runtime.start().await;
    }
}

pub struct Builder {
    broker_config: BrokerConfig,
    message_store_config: MessageStoreConfig,
    server_config: ServerConfig,
}

impl Builder {
    pub fn new() -> Self {
        Builder {
            broker_config: Default::default(),
            message_store_config: MessageStoreConfig::default(),
            server_config: Default::default(),
        }
    }

    pub fn set_broker_config(mut self, broker_config: BrokerConfig) -> Self {
        self.broker_config = broker_config;
        self
    }
    pub fn set_message_store_config(mut self, message_store_config: MessageStoreConfig) -> Self {
        self.message_store_config = message_store_config;
        self
    }

    pub fn set_server_config(mut self, server_config: ServerConfig) -> Self {
        self.server_config = server_config;
        self
    }

    pub fn build(self) -> BrokerBootstrap {
        BrokerBootstrap {
            broker_runtime: BrokerRuntime::new(
                self.broker_config,
                self.message_store_config,
                self.server_config,
            ),
        }
    }
}

impl Default for Builder {
    fn default() -> Self {
        Self::new()
    }
}
