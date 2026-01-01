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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use parking_lot::RwLock;
use tokio::task;
use tokio::time::interval;

use crate::common::statistics::statistics_item::StatisticsItem;
use crate::common::statistics::statistics_item_state_getter::StatisticsItemStateGetter;
use crate::common::statistics::statistics_kind_meta::StatisticsKindMeta;
use crate::TimeUtils::get_current_millis;

type StatsTable = Arc<RwLock<HashMap<String, HashMap<String, Arc<StatisticsItem>>>>>;

pub struct StatisticsManager {
    kind_meta_map: Arc<RwLock<HashMap<String, Arc<StatisticsKindMeta>>>>,
    brief_metas: Option<Vec<(String, Vec<Vec<i64>>)>>,
    stats_table: StatsTable,
    statistics_item_state_getter: Option<Arc<dyn StatisticsItemStateGetter + Send + Sync>>,
}

impl Default for StatisticsManager {
    fn default() -> Self {
        Self::new()
    }
}

impl StatisticsManager {
    const MAX_IDLE_TIME: u64 = 10 * 60 * 1000;

    pub fn new() -> Self {
        let manager = Self {
            kind_meta_map: Arc::new(RwLock::new(HashMap::new())),
            brief_metas: None,
            stats_table: Arc::new(RwLock::new(HashMap::new())),
            statistics_item_state_getter: None,
        };
        manager.start();
        manager
    }

    pub fn with_kind_meta(kind_meta: HashMap<String, Arc<StatisticsKindMeta>>) -> Self {
        let manager = Self {
            kind_meta_map: Arc::new(RwLock::new(kind_meta)),
            brief_metas: None,
            stats_table: Arc::new(RwLock::new(HashMap::new())),
            statistics_item_state_getter: None,
        };
        manager.start();
        manager
    }

    pub fn add_statistics_kind_meta(&self, kind_meta: Arc<StatisticsKindMeta>) {
        let mut kind_meta_map = self.kind_meta_map.write();
        kind_meta_map.insert(kind_meta.get_name().to_string(), kind_meta.clone());
        let mut stats_table = self.stats_table.write();
        stats_table.entry(kind_meta.get_name().to_string()).or_default();
    }

    pub fn set_brief_meta(&mut self, brief_metas: Vec<(String, Vec<Vec<i64>>)>) {
        self.brief_metas = Some(brief_metas);
    }

    fn start(&self) {
        let stats_table = self.stats_table.clone();
        let kind_meta_map = self.kind_meta_map.clone();
        let statistics_item_state_getter = self.statistics_item_state_getter.clone();

        task::spawn(async move {
            let mut interval = interval(Duration::from_millis(Self::MAX_IDLE_TIME / 3));
            let stats_table_clone = stats_table.clone();
            loop {
                interval.tick().await;

                let stats_table = stats_table.read();
                for (_kind, item_map) in stats_table.iter() {
                    let tmp_item_map: HashMap<_, _> = item_map.clone().into_iter().collect();

                    for item in tmp_item_map.values() {
                        let last_time_stamp = item.last_timestamp();
                        if get_current_millis() - last_time_stamp > Self::MAX_IDLE_TIME
                            && (statistics_item_state_getter.is_none()
                                || !statistics_item_state_getter.as_ref().unwrap().online(item))
                        {
                            // Remove expired item
                            remove(item, &stats_table_clone, &kind_meta_map);
                        }
                    }
                }
            }
        });
    }

    pub async fn inc(&self, kind: &str, key: &str, item_accumulates: Vec<i64>) -> bool {
        if let Some(item_map) = self.stats_table.write().get_mut(kind) {
            if let Some(item) = item_map.get(key) {
                item.inc_items(item_accumulates);
                return true;
            } else {
                let kind_meta_map = self.kind_meta_map.read();
                if let Some(kind_meta) = kind_meta_map.get(kind) {
                    let new_item = Arc::new(StatisticsItem::new(
                        kind,
                        key,
                        kind_meta.get_item_names().iter().map(|item| item.as_str()).collect(),
                    ));
                    item_map.insert(key.to_string(), new_item.clone());
                    new_item.inc_items(item_accumulates);
                    self.schedule_statistics_item(new_item);
                    return true;
                }
            }
        }
        false
    }

    fn schedule_statistics_item(&self, item: Arc<StatisticsItem>) {
        let kind_meta_map = self.kind_meta_map.read();
        if let Some(kind_meta) = kind_meta_map.get(item.stat_kind()) {
            kind_meta.get_scheduled_printer().schedule(item.as_ref());
        }
    }

    pub fn set_statistics_item_state_getter(&mut self, getter: Arc<dyn StatisticsItemStateGetter + Send + Sync>) {
        self.statistics_item_state_getter = Some(getter);
    }
}

pub fn remove(
    item: &StatisticsItem,
    stats_table: &StatsTable,
    kind_meta_map: &Arc<RwLock<HashMap<String, Arc<StatisticsKindMeta>>>>,
) {
    let stat_kind = item.stat_kind();
    let stat_object = item.stat_object();
    if let Some(item_map) = stats_table.write().get_mut(stat_kind) {
        item_map.remove(stat_object);
    }

    // Remove from scheduled printer
    let kind_meta_map = kind_meta_map.write();
    if let Some(kind_meta) = kind_meta_map.get(stat_kind) {
        kind_meta.get_scheduled_printer().remove(item);
    }
}
