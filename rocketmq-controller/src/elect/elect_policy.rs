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

pub trait ElectPolicy: Send + Sync {
    /// Elect a new master broker.
    ///
    /// # Arguments
    /// * `cluster_name` - cluster name
    /// * `broker_name` - broker group name
    /// * `sync_state_brokers` - replicas in SyncStateSet
    /// * `all_replica_brokers` - all replicas
    /// * `old_master` - previous master broker id
    /// * `broker_id` - preferred or assigned broker id
    ///
    /// # Returns
    /// New master's broker id, or None if election fails
    fn elect(
        &self,
        cluster_name: &str,
        broker_name: &str,
        sync_state_brokers: &HashSet<i64>,
        all_replica_brokers: &HashSet<i64>,
        old_master: Option<i64>,
        broker_id: Option<i64>,
    ) -> Option<i64>;
}
