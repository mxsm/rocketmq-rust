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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;

use rocketmq_common::common::broker::broker_role::BrokerRole;

use crate::config::message_store_config::MessageStoreConfig;

const ASYNC_MASTER: u8 = 0;
const SYNC_MASTER: u8 = 1;
const SLAVE: u8 = 2;

/// Narrow state for Store settings that may change while shared readers are active.
pub(crate) struct StoreRuntimeState {
    broker_role: AtomicU8,
    data_read_ahead_enable: AtomicBool,
}

impl StoreRuntimeState {
    pub(crate) fn new(config: &MessageStoreConfig) -> Self {
        Self {
            broker_role: AtomicU8::new(Self::encode_role(config.broker_role)),
            data_read_ahead_enable: AtomicBool::new(config.data_read_ahead_enable),
        }
    }

    #[inline]
    pub(crate) fn broker_role(&self) -> BrokerRole {
        match self.broker_role.load(Ordering::Acquire) {
            ASYNC_MASTER => BrokerRole::AsyncMaster,
            SYNC_MASTER => BrokerRole::SyncMaster,
            SLAVE => BrokerRole::Slave,
            _ => unreachable!("Store broker role is written only through the validated encoder"),
        }
    }

    #[inline]
    pub(crate) fn set_broker_role(&self, broker_role: BrokerRole) {
        self.broker_role
            .store(Self::encode_role(broker_role), Ordering::Release);
    }

    #[inline]
    pub(crate) fn data_read_ahead_enable(&self) -> bool {
        self.data_read_ahead_enable.load(Ordering::Acquire)
    }

    #[inline]
    pub(crate) fn set_data_read_ahead_enable(&self, enabled: bool) {
        self.data_read_ahead_enable.store(enabled, Ordering::Release);
    }

    const fn encode_role(broker_role: BrokerRole) -> u8 {
        match broker_role {
            BrokerRole::AsyncMaster => ASYNC_MASTER,
            BrokerRole::SyncMaster => SYNC_MASTER,
            BrokerRole::Slave => SLAVE,
        }
    }
}
