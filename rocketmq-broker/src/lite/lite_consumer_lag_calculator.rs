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

use cheetah_string::CheetahString;
use rocketmq_model::common::lite::get_lite_topic;
use rocketmq_model::common::lite::get_parent_topic;
use rocketmq_model::common::lite::is_lite_topic_queue;
use rocketmq_protocol::protocol::body::lite_lag_info::LiteLagInfo;

use crate::offset::manager::consumer_offset_manager::TOPIC_GROUP_SEPARATOR;

pub(crate) type LiteOffsetTable = HashMap<CheetahString, HashMap<i32, i64>>;

pub(crate) trait LiteConsumerLagDataSource {
    fn offset_table_snapshot(&self) -> LiteOffsetTable;

    fn max_offset(&self, lmq_name: &CheetahString) -> i64;

    fn message_store_timestamp(&self, lmq_name: &CheetahString, offset: i64) -> i64;
}

pub(crate) struct LiteConsumerLagCalculator;

impl LiteConsumerLagCalculator {
    const INIT_CONSUME_TIMESTAMP: i64 = -1;

    pub(crate) fn get_lag_count_top_k(
        data_source: &impl LiteConsumerLagDataSource,
        group: &CheetahString,
        top_k: i32,
    ) -> (Vec<LiteLagInfo>, i64) {
        let mut total_lag_count = 0;
        let offset_table = data_source.offset_table_snapshot();
        let mut lag_infos = Self::collect_group_offsets(&offset_table, Some(group))
            .into_iter()
            .filter_map(|(lmq_name, consumer_offset)| {
                let lag_count = Self::offset_diff(data_source, &lmq_name, consumer_offset);
                if lag_count <= 0 {
                    return None;
                }

                total_lag_count += lag_count;
                let mut lag_info = LiteLagInfo::new();
                lag_info
                    .with_lite_topic(Self::decode_lite_topic(&lmq_name))
                    .with_lag_count(lag_count)
                    .with_earliest_unconsumed_timestamp(Self::message_store_timestamp(
                        data_source,
                        &lmq_name,
                        consumer_offset,
                    ));
                Some(lag_info)
            })
            .collect::<Vec<_>>();

        lag_infos.sort_by(|left, right| {
            right
                .lag_count()
                .cmp(&left.lag_count())
                .then_with(|| left.lite_topic().as_str().cmp(right.lite_topic().as_str()))
        });
        Self::truncate_top_k(&mut lag_infos, top_k);

        (lag_infos, total_lag_count)
    }

    pub(crate) fn get_lag_timestamp_top_k(
        data_source: &impl LiteConsumerLagDataSource,
        group: &CheetahString,
        parent_topic: &CheetahString,
        top_k: i32,
    ) -> (Vec<LiteLagInfo>, i64) {
        let offset_table = data_source.offset_table_snapshot();
        let mut lag_infos = Self::collect_group_offsets(&offset_table, Some(group))
            .into_iter()
            .filter(|(lmq_name, _)| get_parent_topic(lmq_name.as_str()).as_deref() == Some(parent_topic.as_str()))
            .filter_map(|(lmq_name, consumer_offset)| {
                let lag_count = Self::offset_diff(data_source, &lmq_name, consumer_offset);
                if lag_count <= 0 {
                    return None;
                }

                let timestamp = Self::message_store_timestamp(data_source, &lmq_name, consumer_offset);
                let mut lag_info = LiteLagInfo::new();
                lag_info
                    .with_lite_topic(Self::decode_lite_topic(&lmq_name))
                    .with_lag_count(lag_count)
                    .with_earliest_unconsumed_timestamp(timestamp);
                Some(lag_info)
            })
            .collect::<Vec<_>>();

        lag_infos.sort_by(|left, right| {
            left.earliest_unconsumed_timestamp()
                .cmp(&right.earliest_unconsumed_timestamp())
                .then_with(|| right.lag_count().cmp(&left.lag_count()))
                .then_with(|| left.lite_topic().as_str().cmp(right.lite_topic().as_str()))
        });
        Self::truncate_top_k(&mut lag_infos, top_k);

        let earliest_unconsumed_timestamp = lag_infos
            .first()
            .map(|lag_info| lag_info.earliest_unconsumed_timestamp())
            .unwrap_or(Self::INIT_CONSUME_TIMESTAMP);

        (lag_infos, earliest_unconsumed_timestamp)
    }

    fn collect_group_offsets(
        offset_table: &LiteOffsetTable,
        target_group: Option<&CheetahString>,
    ) -> Vec<(CheetahString, i64)> {
        let mut offsets = Vec::new();

        for (topic_at_group, queue_offsets) in offset_table {
            let Some((topic, group)) = topic_at_group.as_str().split_once(TOPIC_GROUP_SEPARATOR) else {
                continue;
            };
            if !is_lite_topic_queue(topic) {
                continue;
            }
            if let Some(target_group) = target_group {
                if group != target_group.as_str() {
                    continue;
                }
            }
            let Some(consumer_offset) = queue_offsets.get(&0).copied() else {
                continue;
            };
            offsets.push((CheetahString::from_slice(topic), consumer_offset));
        }

        offsets
    }

    fn offset_diff(
        data_source: &impl LiteConsumerLagDataSource,
        lmq_name: &CheetahString,
        consumer_offset: i64,
    ) -> i64 {
        if consumer_offset < 0 {
            return 0;
        }

        let broker_offset = data_source.max_offset(lmq_name);
        (broker_offset - consumer_offset).max(0)
    }

    fn message_store_timestamp(
        data_source: &impl LiteConsumerLagDataSource,
        lmq_name: &CheetahString,
        offset: i64,
    ) -> i64 {
        if offset < 0 {
            return Self::INIT_CONSUME_TIMESTAMP;
        }

        data_source.message_store_timestamp(lmq_name, offset).max(0)
    }

    fn decode_lite_topic(lmq_name: &CheetahString) -> CheetahString {
        get_lite_topic(lmq_name.as_str())
            .map(CheetahString::from_string)
            .unwrap_or_else(|| lmq_name.clone())
    }

    fn truncate_top_k(lag_infos: &mut Vec<LiteLagInfo>, top_k: i32) {
        if top_k > 0 {
            lag_infos.truncate(top_k as usize);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use cheetah_string::CheetahString;
    use rocketmq_model::common::lite::to_lmq_name;

    use super::*;

    const GROUP: &str = "group_a";
    const OTHER_GROUP: &str = "group_b";
    const PARENT_TOPIC: &str = "parent_topic";

    struct MockLagDataSource {
        offset_table: LiteOffsetTable,
        max_offsets: HashMap<CheetahString, i64>,
        store_timestamps: HashMap<CheetahString, i64>,
    }

    impl MockLagDataSource {
        fn new() -> Self {
            Self {
                offset_table: HashMap::new(),
                max_offsets: HashMap::new(),
                store_timestamps: HashMap::new(),
            }
        }

        fn with_queue(mut self, parent_topic: &str, lite_topic: &str, group: &str, consumer_offset: i64) -> Self {
            let lmq_name = lmq_name(parent_topic, lite_topic);
            let key = offset_table_key(&lmq_name, group);
            self.offset_table.entry(key).or_default().insert(0, consumer_offset);
            self
        }

        fn with_max_offset(mut self, parent_topic: &str, lite_topic: &str, max_offset: i64) -> Self {
            self.max_offsets.insert(lmq_name(parent_topic, lite_topic), max_offset);
            self
        }

        fn with_store_timestamp(mut self, parent_topic: &str, lite_topic: &str, timestamp: i64) -> Self {
            self.store_timestamps
                .insert(lmq_name(parent_topic, lite_topic), timestamp);
            self
        }
    }

    impl LiteConsumerLagDataSource for MockLagDataSource {
        fn offset_table_snapshot(&self) -> LiteOffsetTable {
            self.offset_table.clone()
        }

        fn max_offset(&self, lmq_name: &CheetahString) -> i64 {
            self.max_offsets.get(lmq_name).copied().unwrap_or(0)
        }

        fn message_store_timestamp(&self, lmq_name: &CheetahString, _offset: i64) -> i64 {
            self.store_timestamps.get(lmq_name).copied().unwrap_or(0)
        }
    }

    fn lmq_name(parent_topic: &str, lite_topic: &str) -> CheetahString {
        CheetahString::from_string(to_lmq_name(parent_topic, lite_topic).expect("valid lite topic name"))
    }

    fn offset_table_key(lmq_name: &CheetahString, group: &str) -> CheetahString {
        CheetahString::from_string(format!("{}{}{}", lmq_name, TOPIC_GROUP_SEPARATOR, group))
    }

    fn group(group_name: &str) -> CheetahString {
        CheetahString::from_slice(group_name)
    }

    /// Collect (lite topic, lag count) pairs sorted by lite topic so results can be
    /// asserted without depending on `HashMap` iteration order.
    fn sorted_lag_pairs(lag_infos: &[LiteLagInfo]) -> Vec<(&str, i64)> {
        let mut pairs: Vec<(&str, i64)> = lag_infos
            .iter()
            .map(|lag_info| (lag_info.lite_topic().as_str(), lag_info.lag_count()))
            .collect();
        pairs.sort_unstable();
        pairs
    }

    #[test]
    fn lag_count_single_queue_behind_max_offset() {
        let data_source = MockLagDataSource::new()
            .with_queue(PARENT_TOPIC, "lite_a", GROUP, 100)
            .with_max_offset(PARENT_TOPIC, "lite_a", 150);

        let (lag_infos, total_lag_count) =
            LiteConsumerLagCalculator::get_lag_count_top_k(&data_source, &group(GROUP), 10);

        assert_eq!(lag_infos.len(), 1);
        assert_eq!(lag_infos[0].lite_topic(), "lite_a");
        assert_eq!(lag_infos[0].lag_count(), 50);
        assert_eq!(total_lag_count, 50);
    }

    #[test]
    fn lag_count_zero_when_consumer_offset_equals_max_offset() {
        let data_source = MockLagDataSource::new()
            .with_queue(PARENT_TOPIC, "lite_a", GROUP, 200)
            .with_max_offset(PARENT_TOPIC, "lite_a", 200);

        let (lag_infos, total_lag_count) =
            LiteConsumerLagCalculator::get_lag_count_top_k(&data_source, &group(GROUP), 10);

        assert!(lag_infos.is_empty());
        assert_eq!(total_lag_count, 0);
    }

    #[test]
    fn lag_count_totals_across_several_queues_in_group() {
        let data_source = MockLagDataSource::new()
            .with_queue(PARENT_TOPIC, "lite_a", GROUP, 10)
            .with_max_offset(PARENT_TOPIC, "lite_a", 20)
            .with_queue(PARENT_TOPIC, "lite_b", GROUP, 5)
            .with_max_offset(PARENT_TOPIC, "lite_b", 15)
            .with_queue(PARENT_TOPIC, "lite_c", GROUP, 0)
            .with_max_offset(PARENT_TOPIC, "lite_c", 7);

        let (lag_infos, total_lag_count) =
            LiteConsumerLagCalculator::get_lag_count_top_k(&data_source, &group(GROUP), 10);

        assert_eq!(
            sorted_lag_pairs(&lag_infos),
            vec![("lite_a", 10), ("lite_b", 10), ("lite_c", 7)]
        );
        assert_eq!(total_lag_count, 27);
    }

    #[test]
    fn lag_count_ignores_other_groups() {
        let data_source = MockLagDataSource::new()
            .with_queue(PARENT_TOPIC, "lite_a", GROUP, 10)
            .with_max_offset(PARENT_TOPIC, "lite_a", 40)
            .with_queue(PARENT_TOPIC, "lite_b", OTHER_GROUP, 0)
            .with_max_offset(PARENT_TOPIC, "lite_b", 90);

        let (lag_infos, total_lag_count) =
            LiteConsumerLagCalculator::get_lag_count_top_k(&data_source, &group(GROUP), 10);

        assert_eq!(lag_infos.len(), 1);
        assert_eq!(lag_infos[0].lite_topic(), "lite_a");
        assert_eq!(lag_infos[0].lag_count(), 30);
        assert_eq!(total_lag_count, 30);
    }

    #[test]
    fn lag_count_empty_group_returns_empty_result() {
        let data_source = MockLagDataSource::new()
            .with_queue(PARENT_TOPIC, "lite_a", OTHER_GROUP, 0)
            .with_max_offset(PARENT_TOPIC, "lite_a", 10);

        let (lag_infos, total_lag_count) =
            LiteConsumerLagCalculator::get_lag_count_top_k(&data_source, &group(GROUP), 10);

        assert!(lag_infos.is_empty());
        assert_eq!(total_lag_count, 0);
    }

    #[test]
    fn lag_count_top_k_truncates_sorted_results() {
        let data_source = MockLagDataSource::new()
            .with_queue(PARENT_TOPIC, "lite_a", GROUP, 0)
            .with_max_offset(PARENT_TOPIC, "lite_a", 5)
            .with_queue(PARENT_TOPIC, "lite_b", GROUP, 0)
            .with_max_offset(PARENT_TOPIC, "lite_b", 50)
            .with_queue(PARENT_TOPIC, "lite_c", GROUP, 0)
            .with_max_offset(PARENT_TOPIC, "lite_c", 30);

        let (lag_infos, total_lag_count) =
            LiteConsumerLagCalculator::get_lag_count_top_k(&data_source, &group(GROUP), 2);

        assert_eq!(lag_infos.len(), 2);
        assert_eq!(lag_infos[0].lite_topic(), "lite_b");
        assert_eq!(lag_infos[0].lag_count(), 50);
        assert_eq!(lag_infos[1].lite_topic(), "lite_c");
        assert_eq!(lag_infos[1].lag_count(), 30);
        // The total is accumulated before truncation.
        assert_eq!(total_lag_count, 85);
    }

    #[test]
    fn lag_count_ties_are_ordered_by_lite_topic() {
        let data_source = MockLagDataSource::new()
            .with_queue(PARENT_TOPIC, "zeta", GROUP, 0)
            .with_max_offset(PARENT_TOPIC, "zeta", 10)
            .with_queue(PARENT_TOPIC, "alpha", GROUP, 0)
            .with_max_offset(PARENT_TOPIC, "alpha", 10)
            .with_queue(PARENT_TOPIC, "mid", GROUP, 0)
            .with_max_offset(PARENT_TOPIC, "mid", 10);

        let (lag_infos, _) = LiteConsumerLagCalculator::get_lag_count_top_k(&data_source, &group(GROUP), 10);

        let topics: Vec<&str> = lag_infos
            .iter()
            .map(|lag_info| lag_info.lite_topic().as_str())
            .collect();
        assert_eq!(topics, vec!["alpha", "mid", "zeta"]);
    }

    #[test]
    fn lag_count_top_k_zero_and_negative_keep_all_entries() {
        let data_source = MockLagDataSource::new()
            .with_queue(PARENT_TOPIC, "lite_a", GROUP, 0)
            .with_max_offset(PARENT_TOPIC, "lite_a", 1)
            .with_queue(PARENT_TOPIC, "lite_b", GROUP, 0)
            .with_max_offset(PARENT_TOPIC, "lite_b", 2);

        // truncate_top_k only truncates for top_k > 0; zero and negative values
        // keep the full result set.
        let (zero_k_infos, _) = LiteConsumerLagCalculator::get_lag_count_top_k(&data_source, &group(GROUP), 0);
        assert_eq!(zero_k_infos.len(), 2);

        let (negative_k_infos, _) = LiteConsumerLagCalculator::get_lag_count_top_k(&data_source, &group(GROUP), -3);
        assert_eq!(negative_k_infos.len(), 2);
    }

    #[test]
    fn lag_timestamp_filters_by_parent_topic() {
        let data_source = MockLagDataSource::new()
            .with_queue(PARENT_TOPIC, "lite_a", GROUP, 0)
            .with_max_offset(PARENT_TOPIC, "lite_a", 3)
            .with_store_timestamp(PARENT_TOPIC, "lite_a", 100)
            .with_queue("other_parent", "lite_b", GROUP, 0)
            .with_max_offset("other_parent", "lite_b", 4)
            .with_store_timestamp("other_parent", "lite_b", 200);

        let (lag_infos, _) =
            LiteConsumerLagCalculator::get_lag_timestamp_top_k(&data_source, &group(GROUP), &group(PARENT_TOPIC), 10);

        assert_eq!(lag_infos.len(), 1);
        assert_eq!(lag_infos[0].lite_topic(), "lite_a");
        assert_eq!(lag_infos[0].lag_count(), 3);
        assert_eq!(lag_infos[0].earliest_unconsumed_timestamp(), 100);
    }

    #[test]
    fn lag_timestamp_reports_oldest_timestamp() {
        let data_source = MockLagDataSource::new()
            .with_queue(PARENT_TOPIC, "lite_a", GROUP, 0)
            .with_max_offset(PARENT_TOPIC, "lite_a", 1)
            .with_store_timestamp(PARENT_TOPIC, "lite_a", 300)
            .with_queue(PARENT_TOPIC, "lite_b", GROUP, 0)
            .with_max_offset(PARENT_TOPIC, "lite_b", 1)
            .with_store_timestamp(PARENT_TOPIC, "lite_b", 100)
            .with_queue(PARENT_TOPIC, "lite_c", GROUP, 0)
            .with_max_offset(PARENT_TOPIC, "lite_c", 1)
            .with_store_timestamp(PARENT_TOPIC, "lite_c", 200);

        let (lag_infos, earliest_unconsumed_timestamp) =
            LiteConsumerLagCalculator::get_lag_timestamp_top_k(&data_source, &group(GROUP), &group(PARENT_TOPIC), 10);

        assert_eq!(lag_infos.len(), 3);
        assert_eq!(lag_infos[0].lite_topic(), "lite_b");
        assert_eq!(earliest_unconsumed_timestamp, 100);
    }

    #[test]
    fn decode_lite_topic_maps_lmq_name_and_keeps_plain_topic() {
        let lite = lmq_name(PARENT_TOPIC, "lite_a");
        assert_eq!(
            LiteConsumerLagCalculator::decode_lite_topic(&lite),
            CheetahString::from_static_str("lite_a")
        );

        let plain = CheetahString::from_static_str("regular_topic");
        assert_eq!(LiteConsumerLagCalculator::decode_lite_topic(&plain), plain);
    }

    #[test]
    fn offset_diff_handles_consumer_offset_beyond_max_offset() {
        let data_source = MockLagDataSource::new().with_max_offset(PARENT_TOPIC, "lite_a", 100);

        let lmq = lmq_name(PARENT_TOPIC, "lite_a");
        assert_eq!(LiteConsumerLagCalculator::offset_diff(&data_source, &lmq, 110), 0);
        // i64 boundary: max_offset at i64::MAX stays exact, and a consumer offset
        // of i64::MAX clamps to zero without wrapping.
        let boundary_source = MockLagDataSource::new().with_max_offset(PARENT_TOPIC, "lite_a", i64::MAX);
        assert_eq!(
            LiteConsumerLagCalculator::offset_diff(&boundary_source, &lmq, i64::MAX - 5),
            5
        );
        assert_eq!(
            LiteConsumerLagCalculator::offset_diff(&boundary_source, &lmq, i64::MAX),
            0
        );
    }
}
