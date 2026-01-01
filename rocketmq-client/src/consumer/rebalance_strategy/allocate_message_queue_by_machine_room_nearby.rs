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

use std::collections::BTreeMap;

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_queue::MessageQueue;

use crate::consumer::allocate_message_queue_strategy::AllocateMessageQueueStrategy;
use crate::consumer::rebalance_strategy::check;

pub trait MachineRoomResolver: Send + Sync {
    fn broker_deploy_in(&self, message_queue: &MessageQueue) -> Option<CheetahString>;
    fn consumer_deploy_in(&self, client_id: &CheetahString) -> Option<CheetahString>;
}

pub struct AllocateMessageQueueByMachineRoomNearby {
    strategy: Box<dyn AllocateMessageQueueStrategy>,
    resolver: Box<dyn MachineRoomResolver>,
}

impl AllocateMessageQueueByMachineRoomNearby {
    pub fn new(strategy: Box<dyn AllocateMessageQueueStrategy>, resolver: Box<dyn MachineRoomResolver>) -> Self {
        Self { strategy, resolver }
    }
}

impl AllocateMessageQueueStrategy for AllocateMessageQueueByMachineRoomNearby {
    fn allocate(
        &self,
        consumer_group: &CheetahString,
        current_cid: &CheetahString,
        mq_all: &[MessageQueue],
        cid_all: &[CheetahString],
    ) -> rocketmq_error::RocketMQResult<Vec<MessageQueue>> {
        let mut result = Vec::new();

        if !check(consumer_group, current_cid, mq_all, cid_all)? {
            return Ok(result);
        }

        // Group MQs by machine room
        let mut mr2mq: BTreeMap<CheetahString, Vec<MessageQueue>> = BTreeMap::new();
        for mq in mq_all {
            if let Some(broker_machine_room) = self.resolver.broker_deploy_in(mq) {
                mr2mq.entry(broker_machine_room).or_default().push(mq.clone());
            } else {
                return Err(mq_client_err!(format!("Machine room is null for mq {mq}")));
            }
        }

        // Group consumers by machine room
        let mut mr2c: BTreeMap<CheetahString, Vec<CheetahString>> = BTreeMap::new();
        for cid in cid_all {
            if let Some(consumer_machine_room) = self.resolver.consumer_deploy_in(cid) {
                mr2c.entry(consumer_machine_room).or_default().push(cid.clone());
            } else {
                return Err(mq_client_err!(format!("Machine room is null for consumer id {cid}")));
            }
        }

        // 1. Allocate MQs in the same machine room as current consumer
        if let Some(current_machine_room) = self.resolver.consumer_deploy_in(current_cid) {
            if let Some(mq_in_room) = mr2mq.remove(&current_machine_room) {
                if let Some(consumers_in_room) = mr2c.get(&current_machine_room) {
                    result.extend(self.strategy.allocate(
                        consumer_group,
                        current_cid,
                        &mq_in_room,
                        consumers_in_room,
                    )?);
                }
            }

            // 2. Allocate remaining MQs from machine rooms with no consumers
            for (machine_room, mqs) in mr2mq {
                if !mr2c.contains_key(&machine_room) {
                    result.extend(self.strategy.allocate(consumer_group, current_cid, &mqs, cid_all)?);
                }
            }
        }

        Ok(result)
    }

    fn get_name(&self) -> &'static str {
        // TODO: find a better way to do this
        format!("MACHINE_ROOM_NEARBY-{}", self.strategy.get_name()).leak()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;

    use rand::Rng;
    use rand::SeedableRng;

    use super::*;
    use crate::consumer::rebalance_strategy::allocate_message_queue_averagely_by_circle::AllocateMessageQueueAveragelyByCircle;

    const CID_PREFIX: &str = "CID-";
    const TOPIC: &str = "topic_test";

    #[derive(Clone)]
    struct TestMachineRoomResolver;

    impl MachineRoomResolver for TestMachineRoomResolver {
        fn broker_deploy_in(&self, message_queue: &MessageQueue) -> Option<CheetahString> {
            let parts: Vec<&str> = message_queue.get_broker_name().split('-').collect();
            Some(CheetahString::from(parts[0]))
        }

        fn consumer_deploy_in(&self, client_id: &CheetahString) -> Option<CheetahString> {
            let parts: Vec<&str> = client_id.as_str().split('-').collect();
            Some(CheetahString::from(parts[0]))
        }
    }

    fn create_consumer_id_list(machine_room: &str, size: usize) -> Vec<CheetahString> {
        (0..size)
            .map(|i| CheetahString::from(format!("{}-{}{}", machine_room, CID_PREFIX, i)))
            .collect()
    }

    fn create_message_queue_list(machine_room: &str, size: usize) -> Vec<MessageQueue> {
        (0..size)
            .map(|i| MessageQueue::from_parts(TOPIC, format!("{}-brokerName", machine_room), i as i32))
            .collect()
    }

    fn prepare_mq(broker_idc_size: usize, queue_size: usize) -> Vec<MessageQueue> {
        let mut mq_all = Vec::new();
        for i in 1..=broker_idc_size {
            mq_all.extend(create_message_queue_list(&format!("IDC{}", i), queue_size));
        }
        mq_all
    }

    fn prepare_consumer(idc_size: usize, consumer_size: usize) -> Vec<CheetahString> {
        let mut cid_all = Vec::new();
        for i in 1..=idc_size {
            cid_all.extend(create_consumer_id_list(&format!("IDC{}", i), consumer_size));
        }
        cid_all
    }

    fn has_allocate_all_q(
        cid_all: &[CheetahString],
        mq_all: &[MessageQueue],
        allocated_res_all: &[MessageQueue],
    ) -> bool {
        if cid_all.is_empty() {
            return allocated_res_all.is_empty();
        }
        let mq_set: BTreeSet<_> = mq_all.iter().collect();
        let res_set: BTreeSet<_> = allocated_res_all.iter().collect();
        mq_set.is_superset(&res_set) && res_set.is_superset(&mq_set) && mq_all.len() == allocated_res_all.len()
    }

    fn test_when_idc_size_equals(idc_size: usize, queue_size: usize, consumer_size: usize) {
        let strategy = Box::new(AllocateMessageQueueAveragelyByCircle);
        let resolver = Box::new(TestMachineRoomResolver);
        let allocator = AllocateMessageQueueByMachineRoomNearby::new(strategy, resolver);

        let cid_all = prepare_consumer(idc_size, consumer_size);
        let mq_all = prepare_mq(idc_size, queue_size);
        let mut res_all = Vec::new();

        for current_id in &cid_all {
            let res = allocator
                .allocate(&CheetahString::from("Test-C-G"), current_id, &mq_all, &cid_all)
                .unwrap();

            for mq in &res {
                assert_eq!(
                    allocator.resolver.broker_deploy_in(mq),
                    allocator.resolver.consumer_deploy_in(current_id)
                );
            }
            res_all.extend(res);
        }
        assert!(has_allocate_all_q(&cid_all, &mq_all, &res_all));
    }

    fn test_when_consumer_idc_is_more(
        broker_idc_size: usize,
        consumer_more: usize,
        queue_size: usize,
        consumer_size: usize,
        _print: bool,
    ) {
        let strategy = Box::new(AllocateMessageQueueAveragelyByCircle);
        let resolver = Box::new(TestMachineRoomResolver);
        let allocator = AllocateMessageQueueByMachineRoomNearby::new(strategy, resolver);

        let mut broker_idc_with_consumer = BTreeSet::new();
        let cid_all = prepare_consumer(broker_idc_size + consumer_more, consumer_size);
        let mq_all = prepare_mq(broker_idc_size, queue_size);

        for mq in &mq_all {
            if let Some(idc) = allocator.resolver.broker_deploy_in(mq) {
                broker_idc_with_consumer.insert(idc);
            }
        }

        let mut res_all = Vec::new();
        for current_id in &cid_all {
            let res = allocator
                .allocate(&CheetahString::from("Test-C-G"), current_id, &mq_all, &cid_all)
                .unwrap();

            for mq in &res {
                if let Some(broker_idc) = allocator.resolver.broker_deploy_in(mq) {
                    if broker_idc_with_consumer.contains(&broker_idc) {
                        assert_eq!(Some(broker_idc), allocator.resolver.consumer_deploy_in(current_id));
                    }
                }
            }
            res_all.extend(res);
        }

        assert!(has_allocate_all_q(&cid_all, &mq_all, &res_all));
    }

    fn test_when_consumer_idc_is_less(
        broker_idc_size: usize,
        consumer_idc_less: usize,
        queue_size: usize,
        consumer_size: usize,
        _print: bool,
    ) {
        let strategy = Box::new(AllocateMessageQueueAveragelyByCircle);
        let resolver = Box::new(TestMachineRoomResolver);
        let allocator = AllocateMessageQueueByMachineRoomNearby::new(strategy, resolver);

        let mut healthy_idc = BTreeSet::new();
        let cid_all = prepare_consumer(broker_idc_size - consumer_idc_less, consumer_size);
        let mq_all = prepare_mq(broker_idc_size, queue_size);

        for cid in &cid_all {
            if let Some(idc) = allocator.resolver.consumer_deploy_in(cid) {
                healthy_idc.insert(idc);
            }
        }

        let mut res_all = Vec::new();
        let mut idc2res: BTreeMap<CheetahString, Vec<MessageQueue>> = BTreeMap::new();

        for current_id in &cid_all {
            if let Some(current_idc) = allocator.resolver.consumer_deploy_in(current_id) {
                let res = allocator
                    .allocate(&CheetahString::from("Test-C-G"), current_id, &mq_all, &cid_all)
                    .unwrap();

                idc2res.entry(current_idc.clone()).or_default().extend(res.clone());
                res_all.extend(res);
            }
        }

        for consumer_idc in healthy_idc {
            if let Some(res_in_one_idc) = idc2res.get(&consumer_idc) {
                let mq_in_this_idc = create_message_queue_list(&consumer_idc, queue_size);
                assert!(mq_in_this_idc.iter().all(|mq| res_in_one_idc.contains(mq)));
            }
        }

        assert!(has_allocate_all_q(&cid_all, &mq_all, &res_all));
    }

    #[test]
    fn test1() {
        test_when_idc_size_equals(5, 20, 10);
        test_when_idc_size_equals(5, 20, 20);
        test_when_idc_size_equals(5, 20, 30);
        test_when_idc_size_equals(5, 20, 0);
    }

    #[test]
    fn test2() {
        test_when_consumer_idc_is_more(5, 1, 10, 10, false);
        test_when_consumer_idc_is_more(5, 1, 10, 5, false);
        test_when_consumer_idc_is_more(5, 1, 10, 20, false);
        test_when_consumer_idc_is_more(5, 1, 10, 0, false);
    }

    #[test]
    fn test3() {
        test_when_consumer_idc_is_less(5, 2, 10, 10, false);
        test_when_consumer_idc_is_less(5, 2, 10, 5, false);
        test_when_consumer_idc_is_less(5, 2, 10, 20, false);
        test_when_consumer_idc_is_less(5, 2, 10, 0, false);
    }

    #[test]
    fn test_run_10_random_case() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        for _ in 0..10 {
            let consumer_size = rng.random_range(1..=200);
            let queue_size = rng.random_range(1..=100);
            let broker_idc_size = rng.random_range(1..=10);
            let consumer_idc_size = rng.random_range(1..=10);

            if broker_idc_size == consumer_idc_size {
                test_when_idc_size_equals(broker_idc_size, queue_size, consumer_size);
            } else if broker_idc_size > consumer_idc_size {
                test_when_consumer_idc_is_less(
                    broker_idc_size,
                    broker_idc_size - consumer_idc_size,
                    queue_size,
                    consumer_size,
                    false,
                );
            } else {
                test_when_consumer_idc_is_more(
                    broker_idc_size,
                    consumer_idc_size - broker_idc_size,
                    queue_size,
                    consumer_size,
                    false,
                );
            }
        }
    }
}
