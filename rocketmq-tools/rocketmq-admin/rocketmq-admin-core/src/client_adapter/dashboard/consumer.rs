// Copyright 2026 The RocketMQ Rust Authors
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

use super::*;

pub(super) fn map_consumer_progress(group: &str, stats: &ConsumeStats) -> dashboard::DashboardConsumerProgress {
    let mut queues = stats
        .get_offset_table()
        .iter()
        .map(|(queue, offset)| dashboard::DashboardConsumerQueueProgress {
            topic: queue.topic().to_string(),
            broker_name: queue.broker_name().to_string(),
            queue_id: queue.queue_id(),
            broker_offset: offset.get_broker_offset(),
            consumer_offset: offset.get_consumer_offset(),
            diff: offset.get_broker_offset().saturating_sub(offset.get_consumer_offset()),
        })
        .collect::<Vec<_>>();
    queues.sort_by(|left, right| {
        left.topic
            .cmp(&right.topic)
            .then(left.broker_name.cmp(&right.broker_name))
            .then(left.queue_id.cmp(&right.queue_id))
    });
    let topic_count = queues
        .iter()
        .map(|queue| queue.topic.as_str())
        .collect::<HashSet<_>>()
        .len();
    let diff_total = queues.iter().map(|queue| queue.diff).sum();
    dashboard::DashboardConsumerProgress {
        group: group.to_string(),
        topic_count,
        diff_total,
        queues,
    }
}
