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

use std::collections::HashSet;
use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_remoting::protocol::DataVersion;
use rocketmq_store::base::message_store::MessageStore;

use crate::broker_runtime::BrokerRuntimeInner;

/// Request-scoped compatibility boundary for the legacy admin dispatcher.
///
/// The admin processor is serialized by its owning Tokio mutex. Keeping the legacy pointer behind
/// this boundary prevents leaf handlers from importing `ArcMut` or manufacturing mutable
/// references from shared borrows while R01 migrates the remaining broker runtime aggregate.
pub(crate) struct BrokerAdminRuntimeHandle<MS: MessageStore>(pub(crate) rocketmq_rust::ArcMut<BrokerRuntimeInner<MS>>);

impl<MS: MessageStore> Clone for BrokerAdminRuntimeHandle<MS> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<MS: MessageStore> Deref for BrokerAdminRuntimeHandle<MS> {
    type Target = BrokerRuntimeInner<MS>;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl<MS: MessageStore> DerefMut for BrokerAdminRuntimeHandle<MS> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.as_mut()
    }
}

impl<MS: MessageStore> BrokerAdminRuntimeHandle<MS> {
    pub(crate) fn runtime_mut(&mut self) -> &mut BrokerRuntimeInner<MS> {
        self.0.as_mut()
    }

    pub(crate) async fn register_increment_broker_data(
        self,
        topic_config_list: Vec<Arc<TopicConfig>>,
        data_version: DataVersion,
    ) {
        BrokerRuntimeInner::register_increment_broker_data(self.0, topic_config_list, data_version).await;
    }

    pub(crate) async fn apply_controller_role_change(
        self,
        controller_leader_address: Option<CheetahString>,
        new_master_broker_id: Option<u64>,
        new_master_address: Option<CheetahString>,
        new_master_epoch: Option<i32>,
        sync_state_set_epoch: Option<i32>,
        sync_state_set: HashSet<i64>,
    ) -> rocketmq_error::RocketMQResult<()> {
        BrokerRuntimeInner::apply_controller_role_change(
            self.0,
            controller_leader_address,
            new_master_broker_id,
            new_master_address,
            new_master_epoch,
            sync_state_set_epoch,
            sync_state_set,
        )
        .await
    }
}
