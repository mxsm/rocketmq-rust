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

use cheetah_string::CheetahString;
use rocketmq_model::allocation::AllocateMessageQueueAveragely;
use rocketmq_model::allocation::AllocateMessageQueueAveragelyByCircle;
use rocketmq_model::allocation::AllocateMessageQueueByConfig;
use rocketmq_model::allocation::AllocateMessageQueueByMachineRoom;
use rocketmq_model::allocation::AllocateMessageQueueByMachineRoomNearby;
use rocketmq_model::allocation::AllocateMessageQueueConsistentHash;
use rocketmq_model::allocation::AllocateMessageQueueStrategy;
use rocketmq_model::allocation::MachineRoomResolver;
use rocketmq_model::message::MessageQueue;

fn queues(count: i32) -> Vec<MessageQueue> {
    (0..count)
        .map(|id| MessageQueue::from_parts("TopicA", "room-a@broker-a", id))
        .collect()
}

fn consumers() -> Vec<CheetahString> {
    vec![CheetahString::from("room-a@cid-0"), CheetahString::from("room-a@cid-1")]
}

fn allocated_ids(strategy: &dyn AllocateMessageQueueStrategy, cid: &CheetahString) -> Vec<i32> {
    strategy
        .allocate(&CheetahString::from("group"), cid, &queues(6), &consumers())
        .expect("valid allocation should succeed")
        .into_iter()
        .map(|queue| queue.queue_id())
        .collect()
}

#[test]
fn average_circle_config_and_machine_room_strategies_match_frozen_corpus() {
    assert_eq!(
        allocated_ids(&AllocateMessageQueueAveragely, &consumers()[0]),
        vec![0, 1, 2]
    );
    assert_eq!(
        allocated_ids(&AllocateMessageQueueAveragelyByCircle, &consumers()[1]),
        vec![1, 3, 5]
    );

    let configured = AllocateMessageQueueByConfig::new(vec![queues(6)[4].clone()]);
    assert_eq!(allocated_ids(&configured, &consumers()[0]), vec![4]);

    let machine_room = AllocateMessageQueueByMachineRoom::new(HashSet::from([CheetahString::from("room-a")]));
    assert_eq!(allocated_ids(&machine_room, &consumers()[1]), vec![3, 4, 5]);
}

struct AtSignResolver;

impl MachineRoomResolver for AtSignResolver {
    fn broker_deploy_in(&self, queue: &MessageQueue) -> Option<CheetahString> {
        queue.broker_name().split_once('@').map(|(room, _)| room.into())
    }

    fn consumer_deploy_in(&self, client_id: &CheetahString) -> Option<CheetahString> {
        client_id.split_once('@').map(|(room, _)| room.into())
    }
}

#[test]
fn nearby_and_consistent_hash_strategies_are_deterministic() {
    let nearby =
        AllocateMessageQueueByMachineRoomNearby::new(Box::new(AllocateMessageQueueAveragely), Box::new(AtSignResolver));
    assert_eq!(allocated_ids(&nearby, &consumers()[0]), vec![0, 1, 2]);

    let consistent = AllocateMessageQueueConsistentHash::new(10);
    let first = allocated_ids(&consistent, &consumers()[0]);
    let second = allocated_ids(&consistent, &consumers()[0]);
    assert_eq!(first, second);

    let mut all = first;
    all.extend(allocated_ids(&consistent, &consumers()[1]));
    all.sort_unstable();
    assert_eq!(all, vec![0, 1, 2, 3, 4, 5]);
}

#[test]
fn allocation_rejects_empty_or_unknown_consumer_input() {
    let error = AllocateMessageQueueAveragely
        .allocate(
            &CheetahString::from("group"),
            &CheetahString::new(),
            &queues(1),
            &consumers(),
        )
        .expect_err("empty consumer id must fail");
    assert!(error.to_string().contains("currentCID is empty"));

    assert!(AllocateMessageQueueAveragely
        .allocate(
            &CheetahString::from("group"),
            &CheetahString::from("unknown"),
            &queues(1),
            &consumers(),
        )
        .expect("unknown consumer should not be a protocol error")
        .is_empty());
}
