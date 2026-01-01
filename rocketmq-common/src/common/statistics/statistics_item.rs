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

use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use crate::common::statistics::interceptor::Interceptor;
use crate::TimeUtils::get_current_millis;

pub struct StatisticsItem {
    stat_kind: String,
    stat_object: String,
    item_names: Vec<String>,
    item_accumulates: Vec<AtomicI64>,
    invoke_times: AtomicI64,
    last_timestamp: AtomicU64,
    interceptor: Option<Arc<dyn Interceptor + Send + Sync>>,
}

impl StatisticsItem {
    pub fn new(stat_kind: &str, stat_object: &str, item_names: Vec<&str>) -> Self {
        if item_names.is_empty() {
            panic!("StatisticsItem \"itemNames\" is empty");
        }

        let item_accumulates = item_names.iter().map(|_| AtomicI64::new(0)).collect();
        let invoke_times = AtomicI64::new(0);
        let last_timestamp = AtomicU64::new(get_current_millis());

        Self {
            stat_kind: stat_kind.to_string(),
            stat_object: stat_object.to_string(),
            item_names: item_names.into_iter().map(String::from).collect(),
            item_accumulates,
            invoke_times,
            last_timestamp,
            interceptor: None,
        }
    }

    pub fn inc_items(&self, item_incs: Vec<i64>) {
        let len = std::cmp::min(item_incs.len(), self.item_accumulates.len());
        for (i, _item) in item_incs.iter().enumerate().take(len) {
            self.item_accumulates[i].fetch_add(item_incs[i], Ordering::SeqCst);
        }

        self.invoke_times.fetch_add(1, Ordering::SeqCst);
        self.last_timestamp.store(get_current_millis(), Ordering::SeqCst);

        if let Some(ref interceptor) = self.interceptor {
            interceptor.inc(item_incs);
        }
    }

    pub fn all_zeros(&self) -> bool {
        if self.invoke_times.load(Ordering::SeqCst) == 0 {
            return true;
        }

        for acc in &self.item_accumulates {
            if acc.load(Ordering::SeqCst) != 0 {
                return false;
            }
        }
        true
    }

    // Getters
    pub fn stat_kind(&self) -> &str {
        &self.stat_kind
    }

    pub fn stat_object(&self) -> &str {
        &self.stat_object
    }

    pub fn item_names(&self) -> &Vec<String> {
        &self.item_names
    }

    pub fn item_accumulates(&self) -> &Vec<AtomicI64> {
        &self.item_accumulates
    }

    pub fn invoke_times(&self) -> &AtomicI64 {
        &self.invoke_times
    }

    pub fn last_timestamp(&self) -> u64 {
        self.last_timestamp.load(Ordering::Relaxed)
    }

    pub fn item_accumulate(&self, item_name: &str) -> Option<&AtomicI64> {
        self.item_names
            .iter()
            .position(|name| name == item_name)
            .map(|index| &self.item_accumulates[index])
    }

    pub fn snapshot(&self) -> Self {
        let item_accumulates = self
            .item_accumulates
            .iter()
            .map(|acc| AtomicI64::new(acc.load(Ordering::SeqCst)))
            .collect();

        Self {
            stat_kind: self.stat_kind.clone(),
            stat_object: self.stat_object.clone(),
            item_names: self.item_names.clone(),
            item_accumulates,
            invoke_times: AtomicI64::new(self.invoke_times.load(Ordering::SeqCst)),
            last_timestamp: AtomicU64::new(self.last_timestamp.load(Ordering::SeqCst)),
            interceptor: self.interceptor.clone(),
        }
    }

    pub fn subtract(&self, item: &StatisticsItem) -> Self {
        if self.stat_kind != item.stat_kind
            || self.stat_object != item.stat_object
            || self.item_names != item.item_names
        {
            panic!("StatisticsItem's kind, key and itemNames must be exactly the same");
        }

        let item_accumulates = self
            .item_accumulates
            .iter()
            .zip(&item.item_accumulates)
            .map(|(a, b)| AtomicI64::new(a.load(Ordering::SeqCst) - b.load(Ordering::SeqCst)))
            .collect();

        Self {
            stat_kind: self.stat_kind.clone(),
            stat_object: self.stat_object.clone(),
            item_names: self.item_names.clone(),
            item_accumulates,
            invoke_times: AtomicI64::new(
                self.invoke_times.load(Ordering::SeqCst) - item.invoke_times.load(Ordering::SeqCst),
            ),
            last_timestamp: AtomicU64::new(self.last_timestamp.load(Ordering::SeqCst)),
            interceptor: self.interceptor.clone(),
        }
    }

    pub fn get_interceptor(&self) -> Option<Arc<dyn Interceptor + Send + Sync>> {
        self.interceptor.clone()
    }

    pub fn set_interceptor(&mut self, interceptor: Arc<dyn Interceptor + Send + Sync>) {
        self.interceptor = Some(interceptor);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    struct TestInterceptor;

    impl Interceptor for TestInterceptor {
        fn inc(&self, _deltas: Vec<i64>) {}

        fn reset(&self) {}
    }

    #[test]
    fn new_statistics_item_initializes_correctly() {
        let item = StatisticsItem::new("kind", "object", vec!["item1", "item2"]);
        assert_eq!(item.stat_kind(), "kind");
        assert_eq!(item.stat_object(), "object");
        assert_eq!(item.item_names(), &vec!["item1", "item2"]);
    }

    #[test]
    fn inc_items_updates_values_correctly() {
        let item = StatisticsItem::new("kind", "object", vec!["item1", "item2"]);
        item.inc_items(vec![1, 2]);
        assert_eq!(item.item_accumulate("item1").unwrap().load(Ordering::SeqCst), 1);
        assert_eq!(item.item_accumulate("item2").unwrap().load(Ordering::SeqCst), 2);
    }

    #[test]
    fn all_zeros_returns_true_when_all_zeros() {
        let item = StatisticsItem::new("kind", "object", vec!["item1", "item2"]);
        assert!(item.all_zeros());
    }

    #[test]
    fn all_zeros_returns_false_when_not_all_zeros() {
        let item = StatisticsItem::new("kind", "object", vec!["item1", "item2"]);
        item.inc_items(vec![1, 0]);
        assert!(!item.all_zeros());
    }

    #[test]
    fn snapshot_creates_correct_snapshot() {
        let item = StatisticsItem::new("kind", "object", vec!["item1", "item2"]);
        item.inc_items(vec![1, 2]);
        let snapshot = item.snapshot();
        assert_eq!(snapshot.item_accumulate("item1").unwrap().load(Ordering::SeqCst), 1);
        assert_eq!(snapshot.item_accumulate("item2").unwrap().load(Ordering::SeqCst), 2);
    }

    #[test]
    fn subtract_creates_correct_subtraction() {
        let item1 = StatisticsItem::new("kind", "object", vec!["item1", "item2"]);
        item1.inc_items(vec![3, 4]);
        let item2 = StatisticsItem::new("kind", "object", vec!["item1", "item2"]);
        item2.inc_items(vec![1, 2]);
        let result = item1.subtract(&item2);
        assert_eq!(result.item_accumulate("item1").unwrap().load(Ordering::SeqCst), 2);
        assert_eq!(result.item_accumulate("item2").unwrap().load(Ordering::SeqCst), 2);
    }

    #[test]
    fn set_interceptor_sets_interceptor_correctly() {
        let mut item = StatisticsItem::new("kind", "object", vec!["item1", "item2"]);
        item.set_interceptor(Arc::new(TestInterceptor));
        assert!(item.get_interceptor().is_some());
    }
}
