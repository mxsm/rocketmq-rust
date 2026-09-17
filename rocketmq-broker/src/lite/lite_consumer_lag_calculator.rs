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

    use super::LiteConsumerLagCalculator;
    use super::LiteConsumerLagDataSource;
    use super::LiteOffsetTable;

    #[derive(Default)]
    struct TestDataSource {
        offsets: LiteOffsetTable,
        max_offsets: HashMap<CheetahString, i64>,
        timestamps: HashMap<(CheetahString, i64), i64>,
    }

    impl TestDataSource {
        fn insert(&mut self, parent: &str, lite: &str, group: &str, consumer: i64, max: i64, timestamp: i64) {
            let queue = lite_queue(parent, lite);
            self.offsets.insert(
                CheetahString::from_string(format!("{queue}@{group}")),
                HashMap::from([(0, consumer)]),
            );
            self.max_offsets.insert(queue.clone(), max);
            self.timestamps.insert((queue, consumer), timestamp);
        }
    }

    impl LiteConsumerLagDataSource for TestDataSource {
        fn offset_table_snapshot(&self) -> LiteOffsetTable {
            self.offsets.clone()
        }

        fn max_offset(&self, lmq_name: &CheetahString) -> i64 {
            self.max_offsets.get(lmq_name).copied().unwrap_or_default()
        }

        fn message_store_timestamp(&self, lmq_name: &CheetahString, offset: i64) -> i64 {
            self.timestamps
                .get(&(lmq_name.clone(), offset))
                .copied()
                .unwrap_or_default()
        }
    }

    fn lite_queue(parent: &str, lite: &str) -> CheetahString {
        CheetahString::from_string(to_lmq_name(parent, lite).expect("non-empty lite queue components"))
    }

    #[test]
    fn lite_consumer_lag_count_filters_groups_truncates_results_and_preserves_total() {
        let mut source = TestDataSource::default();
        source.insert("orders", "priority", "group-a", 3, 10, 300);
        source.insert("orders", "standard", "group-a", 7, 10, 700);
        source.insert("orders", "express", "group-a", 3, 10, 300);
        source.insert("orders", "other-group", "group-b", 1, 20, 100);

        let (lag_infos, total) = LiteConsumerLagCalculator::get_lag_count_top_k(&source, &"group-a".into(), 1);

        assert_eq!(total, 17);
        assert_eq!(lag_infos.len(), 1);
        assert_eq!(lag_infos[0].lite_topic(), "express");
        assert_eq!(lag_infos[0].lag_count(), 7);

        let (lag_infos, total) = LiteConsumerLagCalculator::get_lag_count_top_k(&source, &"missing-group".into(), 10);
        assert!(lag_infos.is_empty());
        assert_eq!(total, 0);
    }

    #[test]
    fn lite_consumer_lag_count_omits_non_positive_lag_and_keeps_all_for_non_positive_top_k() {
        let mut source = TestDataSource::default();
        source.insert("orders", "behind", "group-a", 3, 5, 300);
        source.insert("orders", "further-behind", "group-a", 1, 4, 100);
        source.insert("orders", "equal", "group-a", 5, 5, 500);
        source.insert("orders", "ahead", "group-a", 7, 5, 700);

        for top_k in [0, -1] {
            let (lag_infos, total) = LiteConsumerLagCalculator::get_lag_count_top_k(&source, &"group-a".into(), top_k);
            assert_eq!(total, 5);
            assert_eq!(lag_infos.len(), 2);
            assert_eq!(lag_infos[0].lite_topic(), "further-behind");
            assert_eq!(lag_infos[1].lite_topic(), "behind");
        }
        assert_eq!(
            LiteConsumerLagCalculator::offset_diff(&source, &lite_queue("orders", "ahead"), 7),
            0
        );

        source.insert("orders", "boundary", "group-a", i64::MAX - 5, i64::MAX, 300);
        for (consumer_offset, expected) in [(i64::MAX - 5, 5), (i64::MAX, 0)] {
            assert_eq!(
                LiteConsumerLagCalculator::offset_diff(&source, &lite_queue("orders", "boundary"), consumer_offset),
                expected
            );
        }
    }

    #[test]
    fn lite_consumer_lag_timestamp_filters_parent_and_reports_oldest_timestamp() {
        let mut source = TestDataSource::default();
        source.insert("orders", "newer", "group-a", 1, 3, 400);
        source.insert("orders", "older", "group-a", 2, 6, 100);
        source.insert("payments", "ignored", "group-a", 1, 9, 50);

        let (lag_infos, oldest) =
            LiteConsumerLagCalculator::get_lag_timestamp_top_k(&source, &"group-a".into(), &"orders".into(), 10);

        assert_eq!(oldest, 100);
        assert_eq!(lag_infos.len(), 2);
        assert_eq!(lag_infos[0].lite_topic(), "older");
        assert_eq!(lag_infos[1].lite_topic(), "newer");
    }

    #[test]
    fn lite_consumer_lag_decodes_lite_and_plain_topic_names() {
        assert_eq!(
            LiteConsumerLagCalculator::decode_lite_topic(&lite_queue("orders", "priority")),
            "priority"
        );
        assert_eq!(
            LiteConsumerLagCalculator::decode_lite_topic(&CheetahString::from_static_str("plain-topic")),
            "plain-topic"
        );
    }
}
