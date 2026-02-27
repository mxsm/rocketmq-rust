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

use rocketmq_common::common::stats::stats_item_set::StatsItemSet;
use rocketmq_remoting::protocol::body::consume_status::ConsumeStatus;

const TOPIC_AND_GROUP_CONSUME_OK_TPS: &str = "CONSUME_OK_TPS";
const TOPIC_AND_GROUP_CONSUME_FAILED_TPS: &str = "CONSUME_FAILED_TPS";
const TOPIC_AND_GROUP_CONSUME_RT: &str = "CONSUME_RT";
const TOPIC_AND_GROUP_PULL_TPS: &str = "PULL_TPS";
const TOPIC_AND_GROUP_PULL_RT: &str = "PULL_RT";

/// Tracks consumer-side statistics for each topic/group pair.
///
/// Maintains five time-windowed metric sets: consume OK TPS, consume failed
/// TPS, consume RT, pull TPS, and pull RT. All `inc_*` methods are
/// thread-safe and intended to be called on the hot consumption path.
pub struct ConsumerStatsManager {
    topic_and_group_consume_ok_tps: StatsItemSet,
    topic_and_group_consume_rt: StatsItemSet,
    topic_and_group_consume_failed_tps: StatsItemSet,
    topic_and_group_pull_tps: StatsItemSet,
    topic_and_group_pull_rt: StatsItemSet,
}

impl Default for ConsumerStatsManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ConsumerStatsManager {
    /// Creates a new `ConsumerStatsManager` with all metric sets initialised.
    pub fn new() -> Self {
        Self {
            topic_and_group_consume_ok_tps: StatsItemSet::new(TOPIC_AND_GROUP_CONSUME_OK_TPS.to_string()),
            topic_and_group_consume_rt: StatsItemSet::new(TOPIC_AND_GROUP_CONSUME_RT.to_string()),
            topic_and_group_consume_failed_tps: StatsItemSet::new(TOPIC_AND_GROUP_CONSUME_FAILED_TPS.to_string()),
            topic_and_group_pull_tps: StatsItemSet::new(TOPIC_AND_GROUP_PULL_TPS.to_string()),
            topic_and_group_pull_rt: StatsItemSet::new(TOPIC_AND_GROUP_PULL_RT.to_string()),
        }
    }

    /// Starts background sampling tasks.
    ///
    /// Spawns three Tokio tasks that advance the sliding-window snapshots used
    /// by [`consume_status`](Self::consume_status):
    ///
    /// - every 10 s  → `cs_list_minute` (drives per-minute stats)
    /// - every 10 min → `cs_list_hour`   (drives per-hour stats)
    /// - every 1 h   → `cs_list_day`    (drives per-day stats)
    pub fn start(&self) {
        // 10-second tick — drives cs_list_minute on each StatsItem.
        let sets_sec = [
            self.topic_and_group_consume_ok_tps.clone(),
            self.topic_and_group_consume_rt.clone(),
            self.topic_and_group_consume_failed_tps.clone(),
            self.topic_and_group_pull_tps.clone(),
            self.topic_and_group_pull_rt.clone(),
        ];
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
            loop {
                interval.tick().await;
                for set in &sets_sec {
                    set.sampling_in_seconds();
                }
            }
        });

        // 10-minute tick — drives cs_list_hour on each StatsItem.
        let sets_min = [
            self.topic_and_group_consume_ok_tps.clone(),
            self.topic_and_group_consume_rt.clone(),
            self.topic_and_group_consume_failed_tps.clone(),
            self.topic_and_group_pull_tps.clone(),
            self.topic_and_group_pull_rt.clone(),
        ];
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(10 * 60));
            loop {
                interval.tick().await;
                for set in &sets_min {
                    set.sampling_in_minutes();
                }
            }
        });

        // 1-hour tick — drives cs_list_day on each StatsItem.
        let sets_hour = [
            self.topic_and_group_consume_ok_tps.clone(),
            self.topic_and_group_consume_rt.clone(),
            self.topic_and_group_consume_failed_tps.clone(),
            self.topic_and_group_pull_tps.clone(),
            self.topic_and_group_pull_rt.clone(),
        ];
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(3600));
            loop {
                interval.tick().await;
                for set in &sets_hour {
                    set.sampling_in_hours();
                }
            }
        });
    }

    /// Shuts down the stats manager.
    pub fn shutdown(&self) {}

    /// Records a single pull response-time observation in milliseconds.
    pub fn inc_pull_rt(&self, group: &str, topic: &str, rt: u64) {
        self.topic_and_group_pull_rt
            .add_rt_value(&stats_key(topic, group), rt as i64, 1);
    }

    /// Records `msgs` messages successfully pulled in one batch.
    pub fn inc_pull_tps(&self, group: &str, topic: &str, msgs: u64) {
        self.topic_and_group_pull_tps
            .add_value(&stats_key(topic, group), msgs as i64, 1);
    }

    /// Records a single consume response-time observation in milliseconds.
    pub fn inc_consume_rt(&self, group: &str, topic: &str, rt: u64) {
        self.topic_and_group_consume_rt
            .add_rt_value(&stats_key(topic, group), rt as i64, 1);
    }

    /// Records `msgs` messages consumed successfully in one batch.
    pub fn inc_consume_ok_tps(&self, group: &str, topic: &str, msgs: u64) {
        self.topic_and_group_consume_ok_tps
            .add_value(&stats_key(topic, group), msgs as i64, 1);
    }

    /// Records `msgs` messages that failed consumption in one batch.
    pub fn inc_consume_failed_tps(&self, group: &str, topic: &str, msgs: u64) {
        self.topic_and_group_consume_failed_tps
            .add_value(&stats_key(topic, group), msgs as i64, 1);
    }

    /// Returns a point-in-time [`ConsumeStatus`] snapshot for the given
    /// consumer group and topic.
    ///
    /// - Pull / consume RT uses the per-minute average; consume RT falls back to the per-hour
    ///   average when the per-minute window is empty.
    /// - `consume_failed_msgs` accumulates the per-hour sum of failed messages.
    pub fn consume_status(&self, group: &str, topic: &str) -> ConsumeStatus {
        let key = stats_key(topic, group);

        let pull_rt = self.topic_and_group_pull_rt.get_stats_data_in_minute(&key).get_avgpt();

        let pull_tps = self.topic_and_group_pull_tps.get_stats_data_in_minute(&key).get_tps();

        let consume_rt = {
            let minute = self.topic_and_group_consume_rt.get_stats_data_in_minute(&key);
            if minute.get_sum() == 0 {
                self.topic_and_group_consume_rt.get_stats_data_in_hour(&key).get_avgpt()
            } else {
                minute.get_avgpt()
            }
        };

        let consume_ok_tps = self
            .topic_and_group_consume_ok_tps
            .get_stats_data_in_minute(&key)
            .get_tps();

        let consume_failed_tps = self
            .topic_and_group_consume_failed_tps
            .get_stats_data_in_minute(&key)
            .get_tps();

        let consume_failed_msgs = self
            .topic_and_group_consume_failed_tps
            .get_stats_data_in_hour(&key)
            .get_sum() as i64;

        ConsumeStatus {
            pull_rt,
            pull_tps,
            consume_rt,
            consume_ok_tps,
            consume_failed_tps,
            consume_failed_msgs,
        }
    }
}

/// Builds the canonical stats key `"topic@group"` used by all metric sets.
#[inline]
fn stats_key(topic: &str, group: &str) -> String {
    format!("{topic}@{group}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_manager() -> ConsumerStatsManager {
        ConsumerStatsManager::new()
    }

    #[test]
    fn stats_key_format() {
        assert_eq!(stats_key("TopicA", "GroupA"), "TopicA@GroupA");
    }

    #[test]
    fn smoke_inc_consume_ok_tps() {
        let mgr = make_manager();
        mgr.inc_consume_ok_tps("GroupA", "TopicA", 5);
    }

    #[test]
    fn smoke_inc_consume_failed_tps() {
        let mgr = make_manager();
        mgr.inc_consume_failed_tps("GroupA", "TopicA", 3);
    }

    #[test]
    fn smoke_inc_consume_rt() {
        let mgr = make_manager();
        mgr.inc_consume_rt("GroupA", "TopicA", 42);
    }

    #[test]
    fn smoke_inc_pull_rt() {
        let mgr = make_manager();
        mgr.inc_pull_rt("GroupA", "TopicA", 10);
    }

    #[test]
    fn smoke_inc_pull_tps() {
        let mgr = make_manager();
        mgr.inc_pull_tps("GroupA", "TopicA", 100);
    }

    #[test]
    fn consume_status_returns_zero_for_empty_stats() {
        let mgr = make_manager();
        let status = mgr.consume_status("GroupA", "TopicA");
        assert_eq!(status.pull_rt, 0.0);
        assert_eq!(status.pull_tps, 0.0);
        assert_eq!(status.consume_rt, 0.0);
        assert_eq!(status.consume_ok_tps, 0.0);
        assert_eq!(status.consume_failed_tps, 0.0);
        assert_eq!(status.consume_failed_msgs, 0);
    }

    #[tokio::test]
    async fn start_launches_background_tasks() {
        let mgr = make_manager();
        // Verifies start() does not panic in a Tokio runtime context.
        mgr.start();
        mgr.shutdown();
    }

    #[test]
    fn default_creates_valid_manager() {
        let mgr = ConsumerStatsManager::default();
        // Should not panic when querying uninitialised key.
        let _ = mgr.consume_status("G", "T");
    }
}
