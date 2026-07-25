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

use cheetah_string::CheetahString;
use rocketmq_model::common::broker::broker_identity::BrokerIdentity;

/// Backend-neutral broker settings required by the storage subsystem.
///
/// Keeping this narrow snapshot in the Store owner prevents the storage layer
/// from depending on the Broker service configuration or creating a Cargo
/// dependency cycle.
#[derive(Debug, Clone)]
pub struct StoreRuntimeConfig {
    pub broker_identity: BrokerIdentity,
    pub broker_ip1: CheetahString,
    pub enable_controller_mode: bool,
    pub duplication_enable: bool,
    pub enable_slave_acting_master: bool,
    pub auto_delete_unused_stats: bool,
    pub recover_concurrently: bool,
    pub long_polling_enable: bool,
    pub enable_detail_stat: bool,
}

impl Default for StoreRuntimeConfig {
    fn default() -> Self {
        Self {
            broker_identity: BrokerIdentity::default(),
            broker_ip1: "127.0.0.1".into(),
            enable_controller_mode: false,
            duplication_enable: false,
            enable_slave_acting_master: false,
            auto_delete_unused_stats: false,
            recover_concurrently: false,
            long_polling_enable: true,
            enable_detail_stat: true,
        }
    }
}
