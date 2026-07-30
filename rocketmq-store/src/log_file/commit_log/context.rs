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

use std::collections::BTreeMap;
use std::sync::atomic::AtomicI32;
use std::sync::Arc;

use crate::base::store_stats_service::StoreStatsService;
use crate::ha::general_ha_service::GeneralHAService;
use crate::store::running_flags::RunningFlags;
use arc_swap::ArcSwapOption;

/// Immutable and atomically published LocalStore capabilities consumed by the commit log.
///
/// The context deliberately excludes the `LocalFileMessageStore` root. Static state is shared
/// through `Arc`, counters remain atomic, and the HA service is published only after local-store
/// initialization. Commit-log hot paths therefore never require a mutable store back-reference.
#[derive(Clone)]
pub(crate) struct CommitLogStoreContext {
    pub(super) running_flags: Arc<RunningFlags>,
    pub(super) alive_replica_num_in_group: Arc<AtomicI32>,
    pub(super) store_stats_service: Arc<StoreStatsService>,
    pub(super) ha_service: Arc<ArcSwapOption<GeneralHAService>>,
    pub(super) max_delay_level: i32,
    pub(super) delay_level_table: Arc<BTreeMap<i32, i64>>,
}

impl CommitLogStoreContext {
    pub(crate) fn new(
        running_flags: Arc<RunningFlags>,
        alive_replica_num_in_group: Arc<AtomicI32>,
        store_stats_service: Arc<StoreStatsService>,
        max_delay_level: i32,
        delay_level_table: Arc<BTreeMap<i32, i64>>,
    ) -> Self {
        Self {
            running_flags,
            alive_replica_num_in_group,
            store_stats_service,
            ha_service: Arc::new(ArcSwapOption::empty()),
            max_delay_level,
            delay_level_table,
        }
    }

    pub(super) fn publish_ha_service(&self, ha_service: GeneralHAService) {
        self.ha_service.store(Some(Arc::new(ha_service)));
    }

    pub(super) fn ha_service(&self) -> Option<Arc<GeneralHAService>> {
        self.ha_service.load_full()
    }
}
