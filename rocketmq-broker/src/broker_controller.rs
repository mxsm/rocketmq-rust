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

use std::error::Error;
use std::fmt;

use cheetah_string::CheetahString;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_store::config::message_store_config::MessageStoreConfig;

#[derive(Debug)]
pub enum BrokerControllerError {
    MinBrokerIdNotExists,
}

impl fmt::Display for BrokerControllerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BrokerControllerError::MinBrokerIdNotExists => write!(f, "min broker id not exists"),
        }
    }
}

impl Error for BrokerControllerError {}

type BrokerControllerResult<T> = Result<T, BrokerControllerError>;

#[derive(Default, Clone)]
pub struct BrokerController {
    broker_config: Option<BrokerConfig>,

    message_store_config: Option<MessageStoreConfig>,
}

impl BrokerController {
    #[inline]
    pub fn new(broker_config: BrokerConfig, message_store_config: MessageStoreConfig) -> Self {
        Self {
            broker_config: Some(broker_config),
            message_store_config: Some(message_store_config),
        }
    }

    #[inline]
    pub fn get_min_broker_in_group(&self) -> BrokerControllerResult<u64> {
        if let Some(id) = &self.broker_config {
            Ok(id.broker_identity.broker_id)
        } else {
            Err(BrokerControllerError::MinBrokerIdNotExists)
        }
    }

    pub fn update_min_broker(
        &self,
        _min_broker_id: &Option<u64>,
        _min_broket_addr: &Option<CheetahString>,
        _offline_broker_addr: &Option<CheetahString>,
        _master_ha_addr: &Option<CheetahString>,
    ) {
        // Missing update min broker implementation
        todo!("");
    }
}
