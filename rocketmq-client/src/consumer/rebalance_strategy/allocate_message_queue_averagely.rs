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
use rocketmq_common::common::message::message_queue::MessageQueue;

use crate::consumer::allocate_message_queue_strategy::AllocateMessageQueueStrategy;
use crate::consumer::rebalance_strategy::check;

pub struct AllocateMessageQueueAveragely;

impl AllocateMessageQueueStrategy for AllocateMessageQueueAveragely {
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

        let index = cid_all.iter().position(|cid| cid == current_cid).unwrap_or(0);
        let mod_val = mq_all.len() % cid_all.len();
        let average_size = if mq_all.len() <= cid_all.len() {
            1
        } else if mod_val > 0 && index < mod_val {
            mq_all.len() / cid_all.len() + 1
        } else {
            mq_all.len() / cid_all.len()
        };
        let start_index = if mod_val > 0 && index < mod_val {
            index * average_size
        } else {
            index * average_size + mod_val
        };
        //let range = average_size.min(mq_all.len() - start_index);
        //fix the bug " subtract with overflow" caused by (mq_all.len() - start_index )
        let mut range: usize = 0;
        if mq_all.len() > start_index {
            range = average_size.min(mq_all.len() - start_index);
        }
        //in case of  mq_all.len() < start_index ,  means the customers is much more than queue
        // so let range ==0 ,the for loop not work, and then no queue alloced to this customerID
        for i in 0..range {
            result.push(mq_all[start_index + i].clone());
        }
        Ok(result)
    }
    fn get_name(&self) -> &'static str {
        "AVG"
    }
}
