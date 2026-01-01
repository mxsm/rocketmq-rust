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
use std::vec::Vec;

use rocketmq_common::common::key_builder;
use rocketmq_common::common::key_builder::KeyBuilder;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::mix_all;
use rocketmq_error::RocketMQResult;
use rocketmq_error::RocketmqError::IllegalArgument;
use rocketmq_error::RocketmqError::IllegalArgumentError;

pub struct ExtraInfoUtil;

const NORMAL_TOPIC: &str = "0";
const RETRY_TOPIC: &str = "1";
const RETRY_TOPIC_V2: &str = "2";
const QUEUE_OFFSET: &str = "qo";

impl ExtraInfoUtil {
    /// Split the extra info string using the key separator
    pub fn split(extra_info: &str) -> Vec<String> {
        extra_info
            .split(MessageConst::KEY_SEPARATOR)
            .map(|s| s.to_string())
            .collect()
    }

    /// Get the checkpoint queue offset from the extra info
    pub fn get_ck_queue_offset(extra_info_strs: &[String]) -> RocketMQResult<i64> {
        if extra_info_strs.is_empty() {
            return Err(IllegalArgument(format!(
                "getCkQueueOffset fail, extraInfoStrs length {}",
                extra_info_strs.len()
            ))
            .into());
        }
        extra_info_strs[0]
            .parse::<i64>()
            .map_err(|_| IllegalArgument("parse ck_queue_offset error".to_string()).into())
    }

    /// Get the pop time from the extra info
    pub fn get_pop_time(extra_info_strs: &[String]) -> RocketMQResult<i64> {
        if extra_info_strs.len() < 2 {
            return Err(IllegalArgument(format!(
                "getPopTime fail, extraInfoStrs length {}",
                extra_info_strs.len()
            ))
            .into());
        }
        extra_info_strs[1]
            .parse::<i64>()
            .map_err(|_| IllegalArgument("parse pop_time error".to_string()).into())
    }

    /// Get the invisible time from the extra info
    pub fn get_invisible_time(extra_info_strs: &[String]) -> RocketMQResult<i64> {
        if extra_info_strs.len() < 3 {
            return Err(IllegalArgument(format!(
                "getInvisibleTime fail, extraInfoStrs length {}",
                extra_info_strs.len()
            ))
            .into());
        }
        extra_info_strs[2]
            .parse::<i64>()
            .map_err(|_| IllegalArgument("parse invisible_time error".to_string()).into())
    }

    /// Get the revive queue ID from the extra info
    pub fn get_revive_qid(extra_info_strs: &[String]) -> RocketMQResult<i32> {
        if extra_info_strs.len() < 4 {
            return Err(IllegalArgument(format!(
                "getReviveQid fail, extraInfoStrs length {}",
                extra_info_strs.len()
            ))
            .into());
        }
        extra_info_strs[3]
            .parse::<i32>()
            .map_err(|_| IllegalArgument("parse revive_qid error".to_string()).into())
    }

    /// Get the real topic name based on the retry flag
    pub fn get_real_topic(extra_info_strs: &[String], topic: &str, cid: &str) -> RocketMQResult<String> {
        if extra_info_strs.len() < 5 {
            return Err(IllegalArgument(format!(
                "getRealTopic fail, extraInfoStrs length {}",
                extra_info_strs.len()
            ))
            .into());
        }

        if RETRY_TOPIC == extra_info_strs[4] {
            Ok(KeyBuilder::build_pop_retry_topic_v1(topic, cid))
        } else if RETRY_TOPIC_V2 == extra_info_strs[4] {
            Ok(KeyBuilder::build_pop_retry_topic_v2(topic, cid))
        } else {
            Ok(topic.to_string())
        }
    }

    /// Get the real topic name based on the retry flag
    pub fn get_real_topic_with_retry(topic: &str, cid: &str, retry: &str) -> RocketMQResult<String> {
        if retry == NORMAL_TOPIC {
            Ok(topic.to_string())
        } else if retry == RETRY_TOPIC {
            Ok(KeyBuilder::build_pop_retry_topic_v1(topic, cid))
        } else if retry == RETRY_TOPIC_V2 {
            Ok(KeyBuilder::build_pop_retry_topic_v2(topic, cid))
        } else {
            Err(IllegalArgument("getRetry fail, format is wrong".to_string()).into())
        }
    }

    /// Get the retry flag from the extra info
    pub fn get_retry(extra_info_strs: &[String]) -> RocketMQResult<String> {
        if extra_info_strs.len() < 5 {
            return Err(
                IllegalArgument(format!("getRetry fail, extraInfoStrs length {}", extra_info_strs.len())).into(),
            );
        }
        Ok(extra_info_strs[4].clone())
    }

    /// Get the broker name from the extra info
    pub fn get_broker_name(extra_info_strs: &[String]) -> RocketMQResult<String> {
        if extra_info_strs.len() < 6 {
            return Err(IllegalArgument(format!(
                "getBrokerName fail, extraInfoStrs length {}",
                extra_info_strs.len()
            ))
            .into());
        }
        Ok(extra_info_strs[5].clone())
    }

    /// Get the queue ID from the extra info
    pub fn get_queue_id(extra_info_strs: &[String]) -> rocketmq_error::RocketMQResult<i32> {
        if extra_info_strs.len() < 7 {
            return Err(IllegalArgument(format!(
                "getQueueId fail, extraInfoStrs length {}",
                extra_info_strs.len()
            ))
            .into());
        }
        extra_info_strs[6]
            .parse::<i32>()
            .map_err(|_| IllegalArgument("parse queue_id error".to_string()).into())
    }

    /// Get the queue offset from the extra info
    pub fn get_queue_offset(extra_info_strs: &[String]) -> rocketmq_error::RocketMQResult<i64> {
        if extra_info_strs.len() < 8 {
            return Err(IllegalArgument(format!(
                "getQueueOffset fail, extraInfoStrs length {}",
                extra_info_strs.len()
            ))
            .into());
        }
        extra_info_strs[7]
            .parse::<i64>()
            .map_err(|_| IllegalArgument("parse queue_offset error".to_string()).into())
    }

    /// Build extra info string
    pub fn build_extra_info(
        ck_queue_offset: i64,
        pop_time: i64,
        invisible_time: i64,
        revive_qid: i32,
        topic: &str,
        broker_name: &str,
        queue_id: i32,
    ) -> String {
        let t = Self::get_retry_from_topic(topic);
        format!(
            "{}{sep}{}{sep}{}{sep}{}{sep}{}{sep}{}{sep}{}",
            ck_queue_offset,
            pop_time,
            invisible_time,
            revive_qid,
            t,
            broker_name,
            queue_id,
            sep = MessageConst::KEY_SEPARATOR
        )
    }

    /// Build extra info string with queue offset
    pub fn build_extra_info_with_offset(
        ck_queue_offset: i64,
        pop_time: i64,
        invisible_time: i64,
        revive_qid: i32,
        topic: &str,
        broker_name: &str,
        queue_id: i32,
        msg_queue_offset: i64,
    ) -> String {
        let t = Self::get_retry_from_topic(topic);
        format!(
            "{}{sep}{}{sep}{}{sep}{}{sep}{}{sep}{}{sep}{}{sep}{}",
            ck_queue_offset,
            pop_time,
            invisible_time,
            revive_qid,
            t,
            broker_name,
            queue_id,
            msg_queue_offset,
            sep = MessageConst::KEY_SEPARATOR
        )
    }

    /// Build start offset info
    pub fn build_start_offset_info(string_builder: &mut String, topic: &str, queue_id: i32, start_offset: i64) {
        if !string_builder.is_empty() {
            string_builder.push(';');
        }

        string_builder.push_str(&Self::get_retry_from_topic(topic));
        string_builder.push_str(MessageConst::KEY_SEPARATOR);
        string_builder.push_str(&queue_id.to_string());
        string_builder.push_str(MessageConst::KEY_SEPARATOR);
        string_builder.push_str(&start_offset.to_string());
    }

    /// Build queue ID order count info
    pub fn build_queue_id_order_count_info(string_builder: &mut String, topic: &str, queue_id: i32, order_count: i32) {
        if !string_builder.is_empty() {
            string_builder.push(';');
        }

        string_builder.push_str(&Self::get_retry_from_topic(topic));
        string_builder.push_str(MessageConst::KEY_SEPARATOR);
        string_builder.push_str(&queue_id.to_string());
        string_builder.push_str(MessageConst::KEY_SEPARATOR);
        string_builder.push_str(&order_count.to_string());
    }

    /// Build queue offset order count info
    pub fn build_queue_offset_order_count_info(
        string_builder: &mut String,
        topic: &str,
        queue_id: i64,
        queue_offset: i64,
        order_count: i32,
    ) {
        if !string_builder.is_empty() {
            string_builder.push(';');
        }

        string_builder.push_str(&Self::get_retry_from_topic(topic));
        string_builder.push_str(MessageConst::KEY_SEPARATOR);
        string_builder.push_str(&Self::get_queue_offset_key_value_key(queue_id, queue_offset));
        string_builder.push_str(MessageConst::KEY_SEPARATOR);
        string_builder.push_str(&order_count.to_string());
    }

    /// Build message offset info
    pub fn build_msg_offset_info(string_builder: &mut String, topic: &str, queue_id: i32, msg_offsets: &[u64]) {
        if !string_builder.is_empty() {
            string_builder.push(';');
        }

        string_builder.push_str(&Self::get_retry_from_topic(topic));
        string_builder.push_str(MessageConst::KEY_SEPARATOR);
        string_builder.push_str(&queue_id.to_string());
        string_builder.push_str(MessageConst::KEY_SEPARATOR);

        for (i, offset) in msg_offsets.iter().enumerate() {
            string_builder.push_str(&offset.to_string());
            if i < msg_offsets.len() - 1 {
                string_builder.push(';');
            }
        }
    }

    /// Parse message offset info into a HashMap
    pub fn parse_msg_offset_info(msg_offset_info: &str) -> RocketMQResult<HashMap<String, Vec<i64>>> {
        let mut msg_offset_map: HashMap<String, Vec<i64>> = HashMap::with_capacity(4);
        if msg_offset_info.is_empty() {
            return Ok(msg_offset_map);
        }
        let array = if !msg_offset_info.contains(";") {
            vec![msg_offset_info]
        } else {
            msg_offset_info.split(";").collect::<Vec<&str>>()
        };

        for one in array {
            let split: Vec<&str> = one.split(MessageConst::KEY_SEPARATOR).collect();
            if split.len() != 3 {
                return Err(IllegalArgument("parse msgOffsetInfo error".to_string()).into());
            }

            let key = format!("{}@{}", split[0], split[1]);
            if msg_offset_map.contains_key(&key) {
                return Err(IllegalArgument("parse msgOffsetMap error, duplicate".to_string()).into());
            }

            msg_offset_map.insert(key.clone(), Vec::with_capacity(8));
            let msg_offsets: Vec<&str> = split[2].split(",").collect();

            for msg_offset in msg_offsets {
                let offset = msg_offset
                    .parse::<i64>()
                    .map_err(|_| IllegalArgumentError("parse msgOffset error".to_string()))?;
                msg_offset_map.get_mut(&key).unwrap().push(offset);
            }
        }

        Ok(msg_offset_map)
    }

    /// Parse start offset info into a HashMap
    pub fn parse_start_offset_info(start_offset_info: &str) -> RocketMQResult<HashMap<String, i64>> {
        let mut start_offset_map: HashMap<String, i64> = HashMap::with_capacity(4);
        if start_offset_info.is_empty() {
            return Ok(start_offset_map);
        }
        let array = if !start_offset_info.contains(";") {
            vec![start_offset_info]
        } else {
            start_offset_info.split(";").collect::<Vec<&str>>()
        };

        for one in array {
            let split: Vec<&str> = one.split(MessageConst::KEY_SEPARATOR).collect();
            if split.len() != 3 {
                return Err(IllegalArgument(format!("parse startOffsetInfo error, {start_offset_info}")).into());
            }

            let key = format!("{}@{}", split[0], split[1]);
            if start_offset_map.contains_key(&key) {
                return Err(IllegalArgumentError("parse startOffsetMap error, duplicate".to_string()).into());
            }

            start_offset_map.insert(
                key,
                split[2]
                    .parse::<i64>()
                    .map_err(|_| IllegalArgumentError("parse startOffset error".to_string()))?,
            );
        }

        Ok(start_offset_map)
    }

    /// Parse order count info into a HashMap
    pub fn parse_order_count_info(order_count_info: &str) -> RocketMQResult<HashMap<String, i32>> {
        let mut order_count_map: HashMap<String, i32> = HashMap::with_capacity(4);
        if order_count_info.is_empty() {
            return Ok(order_count_map);
        }
        let array = if !order_count_info.contains(";") {
            vec![order_count_info]
        } else {
            order_count_info.split(";").collect::<Vec<&str>>()
        };

        for one in array {
            let split: Vec<&str> = one.split(MessageConst::KEY_SEPARATOR).collect();
            if split.len() != 3 {
                return Err(IllegalArgument(format!("parse orderCountInfo error {order_count_info}")).into());
            }

            let key = format!("{}@{}", split[0], split[1]);
            if order_count_map.contains_key(&key) {
                return Err(
                    IllegalArgument(format!("parse orderCountInfo error, duplicate, {order_count_info}")).into(),
                );
            }

            order_count_map.insert(
                key,
                split[2]
                    .parse::<i32>()
                    .map_err(|_| IllegalArgumentError("parse orderCountInfo error".to_string()))?,
            );
        }

        Ok(order_count_map)
    }

    /// Get start offset info map key
    pub fn get_start_offset_info_map_key(topic: &str, key: i64) -> String {
        format!("{}@{}", Self::get_retry_from_topic(topic), key)
    }

    /// Get start offset info map key with pop check
    pub fn get_start_offset_info_map_key_with_pop_ck(
        topic: &str,
        pop_ck: Option<&str>,
        key: i64,
    ) -> RocketMQResult<String> {
        Ok(format!("{}@{}", Self::get_retry_from_topic_pop_ck(topic, pop_ck)?, key))
    }

    /// Get queue offset key value key
    pub fn get_queue_offset_key_value_key(queue_id: i64, queue_offset: i64) -> String {
        format!("{}{}{}{}", QUEUE_OFFSET, queue_id, "%", queue_offset)
    }

    /// Get queue offset map key
    pub fn get_queue_offset_map_key(topic: &str, queue_id: i64, queue_offset: i64) -> String {
        format!(
            "{}@{}",
            Self::get_retry_from_topic(topic),
            Self::get_queue_offset_key_value_key(queue_id, queue_offset)
        )
    }

    /// Check if the extra info represents an order
    pub fn is_order(extra_info: &[String]) -> bool {
        Self::get_revive_qid(extra_info).unwrap_or_default() == key_builder::POP_ORDER_REVIVE_QUEUE
    }

    /// Get retry flag from topic name
    fn get_retry_from_topic(topic: &str) -> String {
        if KeyBuilder::is_pop_retry_topic_v2(topic) {
            RETRY_TOPIC_V2.to_string()
        } else if topic.starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX) {
            RETRY_TOPIC.to_string()
        } else {
            NORMAL_TOPIC.to_string()
        }
    }

    /// Get retry flag with pop check
    fn get_retry_from_topic_pop_ck(topic: &str, pop_ck: Option<&str>) -> RocketMQResult<String> {
        match pop_ck {
            Some(ck) => Self::get_retry(&Self::split(ck)),
            None => Ok(Self::get_retry_from_topic(topic)),
        }
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_common::common::key_builder::KeyBuilder;

    use super::*;

    #[test]
    fn split_with_valid_string() {
        let result = ExtraInfoUtil::split("a b c");
        assert_eq!(result, vec!["a", "b", "c"]);
    }

    #[test]
    fn get_ck_queue_offset_with_valid_string() {
        let result = ExtraInfoUtil::get_ck_queue_offset(&["123".to_string()]).unwrap();
        assert_eq!(result, 123);
    }

    #[test]
    fn get_ck_queue_offset_with_invalid_string() {
        let result = ExtraInfoUtil::get_ck_queue_offset(&["abc".to_string()]).unwrap_err();
        assert_eq!(result.to_string(), "Illegal argument: parse ck_queue_offset error");
    }

    #[test]
    fn get_pop_time_with_valid_string() {
        let result = ExtraInfoUtil::get_pop_time(&["123".to_string(), "456".to_string()]).unwrap();
        assert_eq!(result, 456);
    }

    #[test]
    fn get_pop_time_with_insufficient_length() {
        let result = ExtraInfoUtil::get_pop_time(&["123".to_string()]).unwrap_err();
        assert_eq!(
            result.to_string(),
            "Illegal argument: getPopTime fail, extraInfoStrs length 1"
        );
    }

    #[test]
    fn get_invisible_time_with_valid_string() {
        let result =
            ExtraInfoUtil::get_invisible_time(&["123".to_string(), "456".to_string(), "789".to_string()]).unwrap();
        assert_eq!(result, 789);
    }

    #[test]
    fn get_invisible_time_with_insufficient_length() {
        let result = ExtraInfoUtil::get_invisible_time(&["123".to_string(), "456".to_string()]).unwrap_err();
        assert_eq!(
            result.to_string(),
            "Illegal argument: getInvisibleTime fail, extraInfoStrs length 2"
        );
    }

    #[test]
    fn get_revive_qid_with_valid_string() {
        let result = ExtraInfoUtil::get_revive_qid(&[
            "123".to_string(),
            "456".to_string(),
            "789".to_string(),
            "10".to_string(),
        ])
        .unwrap();
        assert_eq!(result, 10);
    }

    #[test]
    fn get_revive_qid_with_insufficient_length() {
        let result =
            ExtraInfoUtil::get_revive_qid(&["123".to_string(), "456".to_string(), "789".to_string()]).unwrap_err();
        assert_eq!(
            result.to_string(),
            "Illegal argument: getReviveQid fail, extraInfoStrs length 3"
        );
    }

    #[test]
    fn get_real_topic_with_retry_v1() {
        let result = ExtraInfoUtil::get_real_topic(
            &[
                "123".to_string(),
                "456".to_string(),
                "789".to_string(),
                "10".to_string(),
                "1".to_string(),
            ],
            "topic",
            "cid",
        )
        .unwrap();
        assert_eq!(result, KeyBuilder::build_pop_retry_topic_v1("topic", "cid"));
    }

    #[test]
    fn get_real_topic_with_retry_v2() {
        let result = ExtraInfoUtil::get_real_topic(
            &[
                "123".to_string(),
                "456".to_string(),
                "789".to_string(),
                "10".to_string(),
                "2".to_string(),
            ],
            "topic",
            "cid",
        )
        .unwrap();
        assert_eq!(result, KeyBuilder::build_pop_retry_topic_v2("topic", "cid"));
    }

    #[test]
    fn get_real_topic_with_normal_topic() {
        let result = ExtraInfoUtil::get_real_topic(
            &[
                "123".to_string(),
                "456".to_string(),
                "789".to_string(),
                "10".to_string(),
                "0".to_string(),
            ],
            "topic",
            "cid",
        )
        .unwrap();
        assert_eq!(result, "topic".to_string());
    }

    #[test]
    fn get_real_topic_with_insufficient_length() {
        let result = ExtraInfoUtil::get_real_topic(
            &[
                "123".to_string(),
                "456".to_string(),
                "789".to_string(),
                "10".to_string(),
            ],
            "topic",
            "cid",
        )
        .unwrap_err();
        assert_eq!(
            result.to_string(),
            "Illegal argument: getRealTopic fail, extraInfoStrs length 4"
        );
    }

    #[test]
    fn get_real_topic_with_retry_invalid() {
        let result = ExtraInfoUtil::get_real_topic_with_retry("topic", "cid", "3").unwrap_err();
        assert_eq!(result.to_string(), "Illegal argument: getRetry fail, format is wrong");
    }

    #[test]
    fn get_retry_slice_with_valid_string() {
        let result = ExtraInfoUtil::get_retry(&[
            "123".to_string(),
            "456".to_string(),
            "789".to_string(),
            "10".to_string(),
            "1".to_string(),
        ])
        .unwrap();
        assert_eq!(result, "1".to_string());
    }

    #[test]
    fn get_retry_slice_with_insufficient_length() {
        let result = ExtraInfoUtil::get_retry(&[
            "123".to_string(),
            "456".to_string(),
            "789".to_string(),
            "10".to_string(),
        ])
        .unwrap_err();
        assert_eq!(
            result.to_string(),
            "Illegal argument: getRetry fail, extraInfoStrs length 4"
        );
    }

    #[test]
    fn get_broker_name_with_valid_string() {
        let result = ExtraInfoUtil::get_broker_name(&[
            "123".to_string(),
            "456".to_string(),
            "789".to_string(),
            "10".to_string(),
            "1".to_string(),
            "broker".to_string(),
        ])
        .unwrap();
        assert_eq!(result, "broker".to_string());
    }

    #[test]
    fn get_broker_name_with_insufficient_length() {
        let result = ExtraInfoUtil::get_broker_name(&[
            "123".to_string(),
            "456".to_string(),
            "789".to_string(),
            "10".to_string(),
            "1".to_string(),
        ])
        .unwrap_err();
        assert_eq!(
            result.to_string(),
            "Illegal argument: getBrokerName fail, extraInfoStrs length 5"
        );
    }

    #[test]
    fn get_queue_id_with_valid_string() {
        let result = ExtraInfoUtil::get_queue_id(&[
            "123".to_string(),
            "456".to_string(),
            "789".to_string(),
            "10".to_string(),
            "1".to_string(),
            "broker".to_string(),
            "7".to_string(),
        ])
        .unwrap();
        assert_eq!(result, 7);
    }

    #[test]
    fn get_queue_id_with_insufficient_length() {
        let result = ExtraInfoUtil::get_queue_id(&[
            "123".to_string(),
            "456".to_string(),
            "789".to_string(),
            "10".to_string(),
            "1".to_string(),
            "broker".to_string(),
        ])
        .unwrap_err();
        assert_eq!(
            result.to_string(),
            "Illegal argument: getQueueId fail, extraInfoStrs length 6"
        );
    }

    #[test]
    fn get_queue_offset_with_valid_string() {
        let result = ExtraInfoUtil::get_queue_offset(&[
            "123".to_string(),
            "456".to_string(),
            "789".to_string(),
            "10".to_string(),
            "1".to_string(),
            "broker".to_string(),
            "7".to_string(),
            "100".to_string(),
        ])
        .unwrap();
        assert_eq!(result, 100);
    }

    #[test]
    fn get_queue_offset_with_insufficient_length() {
        let result = ExtraInfoUtil::get_queue_offset(&[
            "123".to_string(),
            "456".to_string(),
            "789".to_string(),
            "10".to_string(),
            "1".to_string(),
            "broker".to_string(),
            "7".to_string(),
        ])
        .unwrap_err();
        assert_eq!(
            result.to_string(),
            "Illegal argument: getQueueOffset fail, extraInfoStrs length 7"
        );
    }

    #[test]
    fn build_extra_info_creates_correct_string() {
        let result = ExtraInfoUtil::build_extra_info(123, 456, 789, 10, "topic", "broker", 7);
        assert_eq!(result, "123 456 789 10 0 broker 7");
    }

    #[test]
    fn build_extra_info_with_msg_queue_offset_creates_correct_string() {
        let result = ExtraInfoUtil::build_extra_info_with_offset(123, 456, 789, 10, "topic", "broker", 7, 100);
        assert_eq!(result, "123 456 789 10 0 broker 7 100");
    }

    #[test]
    fn build_start_offset_info_creates_correct_string() {
        let mut string_builder = String::new();
        ExtraInfoUtil::build_start_offset_info(&mut string_builder, "topic", 7, 100);
        assert_eq!(string_builder, "0 7 100");
    }

    #[test]
    fn build_queue_id_order_count_info_creates_correct_string() {
        let mut string_builder = String::new();
        ExtraInfoUtil::build_queue_id_order_count_info(&mut string_builder, "topic", 7, 100);
        assert_eq!(string_builder, "0 7 100");
    }

    #[test]
    fn build_queue_offset_order_count_info_creates_correct_string() {
        let mut string_builder = String::new();
        ExtraInfoUtil::build_queue_offset_order_count_info(&mut string_builder, "topic", 7, 100, 100);
        assert_eq!(string_builder, "0 qo7%100 100");
    }

    #[test]
    fn build_msg_offset_info_creates_correct_string() {
        let mut string_builder = String::new();
        ExtraInfoUtil::build_msg_offset_info(&mut string_builder, "topic", 7, &[100, 200, 300]);
        assert_eq!(string_builder, "0 7 100;200;300");
    }

    #[test]
    fn parse_msg_offset_info_with_valid_string() {
        let result = ExtraInfoUtil::parse_msg_offset_info("0 7 100,200,300").unwrap();
        let mut expected = HashMap::new();
        expected.insert("0@7".to_string(), vec![100, 200, 300]);
        assert_eq!(result, expected);
    }

    #[test]
    fn parse_start_offset_info_with_valid_string() {
        let result = ExtraInfoUtil::parse_start_offset_info("0 7 100").unwrap();
        let mut expected = HashMap::new();
        expected.insert("0@7".to_string(), 100);
        assert_eq!(result, expected);
    }

    #[test]
    fn parse_order_count_info_with_valid_string() {
        let result = ExtraInfoUtil::parse_order_count_info("0 7 100").unwrap();
        let mut expected = HashMap::new();
        expected.insert("0@7".to_string(), 100);
        assert_eq!(result, expected);
    }

    #[test]
    fn get_retry_for_topic_with_retry_topic() {
        let result = ExtraInfoUtil::get_retry_from_topic("RETRY_topic");
        assert_eq!(result, "0");
    }

    #[test]
    fn get_retry_for_topic_with_normal_topic() {
        let result = ExtraInfoUtil::get_retry_from_topic("topic");
        assert_eq!(result, "0");
    }

    #[test]
    fn get_start_offset_info_map_key_creates_correct_string() {
        let result = ExtraInfoUtil::get_start_offset_info_map_key("topic", 7);
        assert_eq!(result, "0@7");
    }

    #[test]
    fn get_start_offset_info_map_key_with_pop_ck_creates_correct_string() {
        let result = ExtraInfoUtil::get_start_offset_info_map_key_with_pop_ck("topic", None, 7).unwrap();
        assert_eq!(result, "0@7");
    }

    #[test]
    fn get_queue_offset_key_value_key_creates_correct_string() {
        let result = ExtraInfoUtil::get_queue_offset_key_value_key(7, 100);
        assert_eq!(result, "qo7%100");
    }

    #[test]
    fn get_queue_offset_map_key_creates_correct_string() {
        let result = ExtraInfoUtil::get_queue_offset_map_key("topic", 7, 100);
        assert_eq!(result, "0@qo7%100");
    }

    #[test]
    fn is_order_with_order_queue() {
        let result = ExtraInfoUtil::is_order(&[
            "123".to_string(),
            "456".to_string(),
            "789".to_string(),
            "999".to_string(),
            "1".to_string(),
            "broker".to_string(),
            "7".to_string(),
            "999".to_string(),
            "0".to_string(),
        ]);
        assert!(result);
    }

    #[test]
    fn is_order_with_non_order_queue() {
        let result = ExtraInfoUtil::is_order(&[
            "123".to_string(),
            "456".to_string(),
            "789".to_string(),
            "10".to_string(),
            "1".to_string(),
            "broker".to_string(),
            "7".to_string(),
            "100".to_string(),
            "1".to_string(),
        ]);
        assert!(!result);
    }
}
