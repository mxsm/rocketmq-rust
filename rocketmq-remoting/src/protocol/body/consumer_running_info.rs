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
use std::collections::BTreeSet;
use std::fmt::Display;

use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;

use crate::protocol::body::consume_status::ConsumeStatus;
use crate::protocol::body::pop_process_queue_info::PopProcessQueueInfo;
use crate::protocol::body::process_queue_info::ProcessQueueInfo;
use crate::protocol::heartbeat::consume_type::ConsumeType;
use crate::protocol::heartbeat::subscription_data::SubscriptionData;

#[derive(Clone, Default, Debug)]
pub struct ConsumerRunningInfo {
    pub subscription_set: BTreeSet<SubscriptionData>,
    pub mq_table: BTreeMap<MessageQueue, ProcessQueueInfo>,
    pub mq_pop_table: BTreeMap<MessageQueue, PopProcessQueueInfo>,
    pub status_table: BTreeMap<String, ConsumeStatus>,
    pub user_consumer_info: BTreeMap<String, String>,
    pub consume_type: ConsumeType,
    pub consume_orderly: bool,
    pub prop_consumer_start_timestamp: u64,
}

impl ConsumerRunningInfo {
    pub fn new() -> Self {
        ConsumerRunningInfo {
            subscription_set: BTreeSet::new(),
            mq_table: BTreeMap::new(),
            mq_pop_table: BTreeMap::new(),
            status_table: BTreeMap::new(),
            user_consumer_info: BTreeMap::new(),
            consume_type: ConsumeType::ConsumePassively,
            consume_orderly: false,
            prop_consumer_start_timestamp: 0,
        }
    }
}

impl Display for ConsumerRunningInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut sb = String::new();
        sb.push_str("\n\n#Consumer Subscription#\n");
        let mut i = 0;
        for subscription in &self.subscription_set {
            i += 1;
            let item = format!(
                "{} Topic: {} ClassFilter: {} SubExpression: {}\n",
                i, subscription.topic, subscription.class_filter_mode, subscription.sub_string
            );

            sb.push_str(&item);
        }
        sb.push_str("\n\n#Consumer Offset#\n");
        sb.push_str("#Topic #Broker Name #QID #Consumer Offset\n");

        for (k, v) in &self.mq_table {
            let item = format!(
                "{}  {}  {}  {}\n",
                k.get_topic(),
                k.get_broker_name(),
                k.get_queue_id(),
                v.commit_offset
            );

            sb.push_str(&item);
        }

        sb.push_str("\n\n#Consumer MQ Detail#\n");
        sb.push_str("#Topic #Broker Name #QID #ProcessQueueInfo\n");

        for (k, v) in &self.mq_table {
            let item = format!(
                "{}  {}  {}  {}\n",
                k.get_topic(),
                k.get_broker_name(),
                k.get_queue_id(),
                v
            );

            sb.push_str(&item);
        }

        sb.push_str("\n\n#Consumer Pop Detail#\n");
        sb.push_str("#Topic #Broker Name #QID #ProcessQueueInfo\n");
        for (k, v) in &self.mq_pop_table {
            let item = format!(
                "{}  {}  {}  {}\n",
                k.get_topic(),
                k.get_broker_name(),
                k.get_queue_id(),
                v
            );

            sb.push_str(&item);
        }
        sb.push_str("\n\n#Consumer RT&TPS#\n");
        sb.push_str(
            "#Topic #Pull RT #Pull TPS #Consume RT #ConsumeOK TPS #ConsumeFailed TPS #ConsumeFailedMsgsInHour\n",
        );

        for (k, v) in &self.status_table {
            let item = format!(
                "{} {} {} {} {} {} {}\n",
                k, v.pull_rt, v.pull_tps, v.consume_rt, v.consume_ok_tps, v.consume_failed_tps, v.consume_failed_msgs
            );

            sb.push_str(&item);
        }

        sb.push_str("\n\n#User Consume Info#\n");
        for (k, v) in &self.user_consumer_info {
            let item = format!("{}: {}\n", k, v);
            sb.push_str(&item);
        }
        f.write_str(&sb)
    }
}

impl ConsumerRunningInfo {
    pub async fn analyze_subscription(
        cri_table: BTreeMap<String /* clientId */, ConsumerRunningInfo>,
    ) -> RocketMQResult<()> {
        let first = cri_table.first_key_value().ok_or(RocketMQError::Internal(
            "analyze_subscription err :cri_table is empty".to_string(),
        ))?;
        let prev = first.1;

        let push = matches!(prev.consume_type, ConsumeType::ConsumePassively);

        let start_for_a_while = (get_current_millis() - prev.prop_consumer_start_timestamp) > (1000 * 60 * 2);

        if push && start_for_a_while {
            let mut prev = prev.clone();
            for v in cri_table.values() {
                if v.subscription_set != prev.subscription_set {
                    // Different subscription in the same group of consumer
                    return Err(RocketMQError::Internal(
                        "Different subscription in the same group of consumer".to_string(),
                    ));
                }

                prev = v.clone();
            }
        }
        Ok(())
    }

    pub async fn analyze_process_queue(client_id: String, info: ConsumerRunningInfo) -> RocketMQResult<String> {
        let mut sb = String::new();
        let push = matches!(info.consume_type, ConsumeType::ConsumePassively);

        let order_msg = info.consume_orderly;

        if push {
            for (k, v) in &info.mq_table {
                if order_msg {
                    if !v.locked {
                        sb.push_str(&format!(
                            "{} {} can't lock for a while, {}ms\n",
                            client_id,
                            k,
                            get_current_millis() - v.last_lock_timestamp
                        ));
                    } else if v.droped && v.try_unlock_times > 0 {
                        sb.push_str(&format!(
                            "{} {} unlock {} times, still failed\n",
                            client_id, k, v.try_unlock_times
                        ));
                    }
                } else {
                    let diff = get_current_millis() - v.last_consume_timestamp;

                    if diff > (1000 * 60) && v.cached_msg_count > 0 {
                        sb.push_str(&format!(
                            "{} {} can't consume for a while, maybe blocked, {}ms\n",
                            client_id, k, diff
                        ));
                    }
                }
            }
        }

        Ok(sb)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consumer_running_info_default() {
        let info = ConsumerRunningInfo::new();
        assert!(info.subscription_set.is_empty());
        assert!(info.mq_table.is_empty());
        assert!(info.mq_pop_table.is_empty());
        assert!(info.status_table.is_empty());
        assert!(info.user_consumer_info.is_empty());
        assert_eq!(info.consume_type, ConsumeType::ConsumePassively);
        assert!(!info.consume_orderly);
        assert_eq!(info.prop_consumer_start_timestamp, 0);
    }

    #[test]
    fn consumer_running_info_display() {
        let mut info = ConsumerRunningInfo::new();
        let subscription_data = SubscriptionData {
            topic: "topic".into(),
            sub_string: "*".into(),
            ..Default::default()
        };
        info.subscription_set.insert(subscription_data);

        let mq = MessageQueue::from_parts("topic", "broker", 1);
        let process_queue_info = ProcessQueueInfo {
            commit_offset: 100,
            cached_msg_min_offset: 0,
            cached_msg_max_offset: 100,
            cached_msg_count: 100,
            cached_msg_size_in_mib: 0,
            transaction_msg_min_offset: 0,
            transaction_msg_max_offset: 0,
            transaction_msg_count: 0,
            locked: false,
            try_unlock_times: 0,
            last_lock_timestamp: 0,
            droped: false,
            last_pull_timestamp: 0,
            last_consume_timestamp: 0,
        };
        info.mq_table.insert(mq.clone(), process_queue_info);

        let pop_process_queue_info = PopProcessQueueInfo::new(1, true, 2);
        info.mq_pop_table.insert(mq.clone(), pop_process_queue_info);

        let status = ConsumeStatus {
            pull_rt: 1.0,
            pull_tps: 2.0,
            consume_rt: 3.0,
            consume_ok_tps: 4.0,
            consume_failed_tps: 5.0,
            consume_failed_msgs: 6,
        };

        info.status_table.insert("clientId".to_string(), status);
        info.user_consumer_info
            .insert("userKey".to_string(), "userValue".to_string());

        let display = format!("{}", info);
        assert!(display.contains("#Consumer Subscription#"));
        assert!(display.contains("Topic: topic"));
        assert!(display.contains("#Consumer Offset#"));
        assert!(display.contains("broker"));
        assert!(display.contains("100"));
        assert!(display.contains("#Consumer MQ Detail#"));
        assert!(display.contains("#Consumer Pop Detail#"));
        assert!(display.contains("#Consumer RT&TPS#"));
        assert!(display.contains("clientId"));
        assert!(display.contains("#User Consume Info#"));
        assert!(display.contains("userKey: userValue"));
    }
}
