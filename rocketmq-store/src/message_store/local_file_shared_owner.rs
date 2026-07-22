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

use std::ops::DerefMut;
use std::sync::Arc;

use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::config::TopicConfig;

use crate::config::message_store_config::MessageStoreConfig;
use crate::message_store::local_file_message_store::LocalFileMessageStore;
use crate::stats::broker_stats_manager::BrokerStatsManager;

/// Builds the legacy shared owner and completes the Store self-wiring in one step.
///
/// The concrete compatibility pointer stays private to this owner module so callers
/// cannot accidentally construct a partially wired `LocalFileMessageStore`.
pub(crate) fn new_legacy_shared_owner(
    message_store_config: Arc<MessageStoreConfig>,
    broker_config: Arc<BrokerConfig>,
    topic_config_table: Arc<DashMap<CheetahString, Arc<TopicConfig>>>,
    broker_stats_manager: Option<Arc<BrokerStatsManager>>,
    notify_message_arrive_in_batch: bool,
) -> impl DerefMut<Target = LocalFileMessageStore> + Clone {
    let mut store = rocketmq_rust::ArcMut::new(LocalFileMessageStore::new(
        message_store_config,
        broker_config,
        topic_config_table,
        broker_stats_manager,
        notify_message_arrive_in_batch,
    ));
    let store_clone = store.clone();
    store.set_message_store_arc(store_clone);
    store
}
