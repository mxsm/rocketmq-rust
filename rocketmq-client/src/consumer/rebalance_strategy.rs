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

pub mod allocate_message_queue_averagely;
pub mod allocate_message_queue_averagely_by_circle;
pub mod allocate_message_queue_by_config;
pub mod allocate_message_queue_by_machine_room;
pub mod allocate_message_queue_by_machine_room_nearby;

use std::collections::HashSet;

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_queue::MessageQueue;
use tracing::info;

pub fn check(
    consumer_group: &CheetahString,
    current_cid: &CheetahString,
    mq_all: &[MessageQueue],
    cid_all: &[CheetahString],
) -> rocketmq_error::RocketMQResult<bool> {
    if current_cid.is_empty() {
        return Err(mq_client_err!("currentCID is empty".to_string()));
    }
    if mq_all.is_empty() {
        return Err(mq_client_err!("mqAll is null or mqAll empty".to_string()));
    }
    if cid_all.is_empty() {
        return Err(mq_client_err!("cidAll is null or cidAll empty".to_string()));
    }

    let cid_set: HashSet<_> = cid_all.iter().collect();
    if !cid_set.contains(current_cid) {
        info!(
            "[BUG] ConsumerGroup: {} The consumerId: {} not in cidAll: {:?}",
            consumer_group, current_cid, cid_all
        );
        return Ok(false);
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;
    use rocketmq_common::common::message::message_queue::MessageQueue;

    use super::*;

    #[test]
    fn check_with_valid_inputs() {
        let consumer_group = CheetahString::from("test_group");
        let current_cid = CheetahString::from("cid_1");
        let mq_all = vec![MessageQueue::from_parts("test_topic", "broker_a", 0)];
        let cid_all = vec![CheetahString::from("cid_1"), CheetahString::from("cid_2")];
        let result = check(&consumer_group, &current_cid, &mq_all, &cid_all);
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[test]
    fn check_with_empty_current_cid() {
        let consumer_group = CheetahString::from("test_group");
        let current_cid = CheetahString::new();
        let mq_all = vec![MessageQueue::from_parts("test_topic", "broker_a", 0)];
        let cid_all = vec![CheetahString::from("cid_1"), CheetahString::from("cid_2")];
        let result = check(&consumer_group, &current_cid, &mq_all, &cid_all);
        assert!(result.is_err());
    }

    #[test]
    fn check_with_empty_mq_all() {
        let consumer_group = CheetahString::from("test_group");
        let current_cid = CheetahString::from("cid_1");
        let mq_all: Vec<MessageQueue> = vec![];
        let cid_all = vec![CheetahString::from("cid_1"), CheetahString::from("cid_2")];
        let result = check(&consumer_group, &current_cid, &mq_all, &cid_all);
        assert!(result.is_err());
    }

    #[test]
    fn check_with_empty_cid_all() {
        let consumer_group = CheetahString::from("test_group");
        let current_cid = CheetahString::from("cid_1");
        let mq_all = vec![MessageQueue::from_parts("test_topic", "broker_a", 0)];
        let cid_all: Vec<CheetahString> = vec![];
        let result = check(&consumer_group, &current_cid, &mq_all, &cid_all);
        assert!(result.is_err());
    }

    #[test]
    fn check_with_current_cid_not_in_cid_all() {
        let consumer_group = CheetahString::from("test_group");
        let current_cid = CheetahString::from("cid_3");
        let mq_all = vec![MessageQueue::from_parts("test_topic", "broker_a", 0)];
        let cid_all = vec![CheetahString::from("cid_1"), CheetahString::from("cid_2")];
        let result = check(&consumer_group, &current_cid, &mq_all, &cid_all);
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }
}
