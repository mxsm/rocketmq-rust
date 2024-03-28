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

use std::sync::Arc;

use rocketmq_store::config::message_store_config::MessageStoreConfig;

use crate::broker_config::BrokerConfig;

pub(crate) struct BrokerRuntime {
    broker_config:  Arc<BrokerConfig>,
    message_store_config: Arc<MessageStoreConfig>,
}

impl BrokerRuntime {
    pub(crate) fn new(
        broker_config: BrokerConfig,
        message_store_config: MessageStoreConfig,
    ) -> Self {
        Self {
            broker_config: Arc::new(broker_config),
            message_store_config: Arc::new(message_store_config),
        }
    }

    pub(crate) fn broker_config(&self) -> &BrokerConfig {
        &self.broker_config
    }

    pub(crate) fn message_store_config(&self) -> &MessageStoreConfig {
        &self.message_store_config
    }
}

impl BrokerRuntime {
    pub(crate) fn initialize(&mut self) -> bool {
        self.initialize_metadata()
            && self.initialize_message_store()
            && self.recover_initialize_service()
    }

    fn initialize_metadata(&mut self) -> bool {
        true
    }

    fn initialize_message_store(&mut self) -> bool {
        true
    }

    fn recover_initialize_service(&mut self) -> bool {
        true
    }

    pub async fn start(&self) {
        unimplemented!()
    }
}
