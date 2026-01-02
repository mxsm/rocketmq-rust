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
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use tokio::task;
use tokio::time;
use tracing::info;

use crate::TimeUtils::get_current_millis;
use crate::UtilAll::compute_next_minutes_time_millis;

#[derive(Clone)]
pub struct MomentStatsItem {
    value: Arc<AtomicI64>,
    stats_name: String,
    stats_key: String,
}

impl MomentStatsItem {
    pub fn new(stats_name: String, stats_key: String) -> Self {
        MomentStatsItem {
            value: Arc::new(AtomicI64::new(0)),
            stats_name,
            stats_key,
        }
    }

    pub fn init(self: Arc<Self>) {
        let self_clone = self;
        tokio::spawn(async move {
            let initial_delay = Duration::from_millis(
                (compute_next_minutes_time_millis() as i64 - get_current_millis() as i64).unsigned_abs(),
            );
            time::sleep(initial_delay).await;
            let interval = time::interval(Duration::from_secs(300));
            tokio::pin!(interval);
            loop {
                interval.as_mut().tick().await;
                self_clone.print_at_minutes();
                self_clone.value.store(0, Ordering::Relaxed);
            }
        });
    }

    pub fn print_at_minutes(&self) {
        info!(
            "[{}] [{}] Stats Every 5 Minutes, Value: {}",
            self.stats_name,
            self.stats_key,
            self.value.load(Ordering::Relaxed)
        );
    }

    pub fn get_value(&self) -> Arc<AtomicI64> {
        self.value.clone()
    }

    pub fn get_stats_key(&self) -> &str {
        &self.stats_key
    }

    pub fn get_stats_name(&self) -> &str {
        &self.stats_name
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use super::*;

    #[tokio::test]
    async fn moment_stats_item_initializes_with_zero_value() {
        let stats_item = MomentStatsItem::new("TestName".to_string(), "TestKey".to_string());
        assert_eq!(stats_item.get_value().load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn moment_stats_item_returns_correct_stats_name() {
        let stats_item = MomentStatsItem::new("TestName".to_string(), "TestKey".to_string());
        assert_eq!(stats_item.get_stats_name(), "TestName");
    }

    #[tokio::test]
    async fn moment_stats_item_returns_correct_stats_key() {
        let stats_item = MomentStatsItem::new("TestName".to_string(), "TestKey".to_string());
        assert_eq!(stats_item.get_stats_key(), "TestKey");
    }
}
