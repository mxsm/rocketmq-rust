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

use cheetah_string::CheetahString;
use rocketmq_common::common::lite::get_lite_topic;
use rocketmq_common::common::lite::get_parent_topic;
use rocketmq_common::common::lite::is_lite_topic_queue;
use rocketmq_remoting::protocol::body::lite_lag_info::LiteLagInfo;
use rocketmq_store::base::message_store::MessageStore;

use crate::broker_runtime::BrokerRuntimeInner;
use crate::offset::manager::consumer_offset_manager::TOPIC_GROUP_SEPARATOR;

pub(crate) struct LiteConsumerLagCalculator;

impl LiteConsumerLagCalculator {
    const INIT_CONSUME_TIMESTAMP: i64 = -1;

    pub(crate) fn get_lag_count_top_k<MS: MessageStore>(
        broker_runtime_inner: &BrokerRuntimeInner<MS>,
        group: &CheetahString,
        top_k: i32,
    ) -> (Vec<LiteLagInfo>, i64) {
        let mut total_lag_count = 0;
        let mut lag_infos = Self::collect_group_offsets(broker_runtime_inner, Some(group))
            .into_iter()
            .filter_map(|(lmq_name, consumer_offset)| {
                let lag_count = Self::offset_diff(broker_runtime_inner, &lmq_name, consumer_offset);
                if lag_count <= 0 {
                    return None;
                }

                total_lag_count += lag_count;
                let mut lag_info = LiteLagInfo::new();
                lag_info
                    .with_lite_topic(Self::decode_lite_topic(&lmq_name))
                    .with_lag_count(lag_count)
                    .with_earliest_unconsumed_timestamp(Self::message_store_timestamp(
                        broker_runtime_inner,
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

    pub(crate) fn get_lag_timestamp_top_k<MS: MessageStore>(
        broker_runtime_inner: &BrokerRuntimeInner<MS>,
        group: &CheetahString,
        parent_topic: &CheetahString,
        top_k: i32,
    ) -> (Vec<LiteLagInfo>, i64) {
        let mut lag_infos = Self::collect_group_offsets(broker_runtime_inner, Some(group))
            .into_iter()
            .filter(|(lmq_name, _)| get_parent_topic(lmq_name.as_str()).as_deref() == Some(parent_topic.as_str()))
            .filter_map(|(lmq_name, consumer_offset)| {
                let lag_count = Self::offset_diff(broker_runtime_inner, &lmq_name, consumer_offset);
                if lag_count <= 0 {
                    return None;
                }

                let timestamp = Self::message_store_timestamp(broker_runtime_inner, &lmq_name, consumer_offset);
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

    fn collect_group_offsets<MS: MessageStore>(
        broker_runtime_inner: &BrokerRuntimeInner<MS>,
        target_group: Option<&CheetahString>,
    ) -> Vec<(CheetahString, i64)> {
        let offset_table = broker_runtime_inner.consumer_offset_manager().offset_table();
        let read_guard = offset_table.read();
        let mut offsets = Vec::new();

        for (topic_at_group, queue_offsets) in read_guard.iter() {
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
            offsets.push((CheetahString::from_string(topic.to_string()), consumer_offset));
        }

        offsets
    }

    fn offset_diff<MS: MessageStore>(
        broker_runtime_inner: &BrokerRuntimeInner<MS>,
        lmq_name: &CheetahString,
        consumer_offset: i64,
    ) -> i64 {
        if consumer_offset < 0 {
            return 0;
        }

        let broker_offset = broker_runtime_inner
            .lite_lifecycle_manager()
            .get_max_offset_in_queue(broker_runtime_inner.message_store(), lmq_name);
        (broker_offset - consumer_offset).max(0)
    }

    fn message_store_timestamp<MS: MessageStore>(
        broker_runtime_inner: &BrokerRuntimeInner<MS>,
        lmq_name: &CheetahString,
        offset: i64,
    ) -> i64 {
        if offset < 0 {
            return Self::INIT_CONSUME_TIMESTAMP;
        }

        broker_runtime_inner
            .message_store()
            .map(|message_store| message_store.get_message_store_timestamp(lmq_name, 0, offset).max(0))
            .unwrap_or(0)
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
