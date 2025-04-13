/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::collections::HashMap;
use std::vec::Vec;

use rocketmq_common::common::key_builder::KeyBuilder;
use rocketmq_common::common::key_builder::POP_ORDER_REVIVE_QUEUE;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::mix_all;
use rocketmq_error::RocketmqError::IllegalArgument;

pub struct ExtraInfoUtil;

const NORMAL_TOPIC: &str = "0";
const RETRY_TOPIC: &str = "1";
const RETRY_TOPIC_V2: &str = "2";
const QUEUE_OFFSET: &str = "qo";

impl ExtraInfoUtil {
    pub fn split(extra_info: &str) -> rocketmq_error::RocketMQResult<Vec<String>> {
        if extra_info.is_empty() {
            return Err(IllegalArgument("split extraInfo is empty".to_string()));
        }
        Ok(extra_info.split('|').map(String::from).collect())
    }

    pub fn get_ck_queue_offset(extra_info_strs: &[String]) -> rocketmq_error::RocketMQResult<i64> {
        if extra_info_strs.is_empty() {
            return Err(IllegalArgument(format!(
                "getCkQueueOffset fail, extraInfoStrs length {}",
                extra_info_strs.len()
            )));
        }
        extra_info_strs[0]
            .parse::<i64>()
            .map_err(|_| IllegalArgument("parse ck_queue_offset error".to_string()))
    }

    pub fn get_pop_time(extra_info_strs: &[String]) -> rocketmq_error::RocketMQResult<i64> {
        if extra_info_strs.len() < 2 {
            return Err(IllegalArgument(format!(
                "getPopTime fail, extraInfoStrs length {}",
                extra_info_strs.len()
            )));
        }
        extra_info_strs[1]
            .parse::<i64>()
            .map_err(|_| IllegalArgument("parse pop_time error".to_string()))
    }

    pub fn get_invisible_time(extra_info_strs: &[String]) -> rocketmq_error::RocketMQResult<i64> {
        if extra_info_strs.len() < 3 {
            return Err(IllegalArgument(format!(
                "getInvisibleTime fail, extraInfoStrs length {}",
                extra_info_strs.len()
            )));
        }
        extra_info_strs[2]
            .parse::<i64>()
            .map_err(|_| IllegalArgument("parse invisible_time error".to_string()))
    }

    pub fn get_revive_qid(extra_info_strs: &[String]) -> rocketmq_error::RocketMQResult<i32> {
        if extra_info_strs.len() < 4 {
            return Err(IllegalArgument(format!(
                "getReviveQid fail, extraInfoStrs length {}",
                extra_info_strs.len()
            )));
        }
        extra_info_strs[3]
            .parse::<i32>()
            .map_err(|_| IllegalArgument("parse revive_qid error".to_string()))
    }

    pub fn get_real_topic(
        extra_info_strs: &[String],
        topic: &str,
        cid: &str,
    ) -> rocketmq_error::RocketMQResult<String> {
        if extra_info_strs.len() < 5 {
            return Err(IllegalArgument(format!(
                "getRealTopic fail, extraInfoStrs length {}",
                extra_info_strs.len()
            )));
        }
        match extra_info_strs[4].as_str() {
            RETRY_TOPIC => Ok(KeyBuilder::build_pop_retry_topic_v1(topic, cid)),
            RETRY_TOPIC_V2 => Ok(KeyBuilder::build_pop_retry_topic_v2(topic, cid)),
            _ => Ok(topic.to_string()),
        }
    }

    pub fn get_real_topic_with_retry(
        topic: &str,
        cid: &str,
        retry: &str,
    ) -> rocketmq_error::RocketMQResult<String> {
        match retry {
            NORMAL_TOPIC => Ok(topic.to_string()),
            RETRY_TOPIC => Ok(KeyBuilder::build_pop_retry_topic_v1(topic, cid)),
            RETRY_TOPIC_V2 => Ok(KeyBuilder::build_pop_retry_topic_v2(topic, cid)),
            _ => Err(IllegalArgument(
                "getRetry fail, format is wrong".to_string(),
            )),
        }
    }

    pub fn get_retry_slice(extra_info_strs: &[String]) -> rocketmq_error::RocketMQResult<String> {
        if extra_info_strs.len() < 5 {
            return Err(IllegalArgument(format!(
                "getRetry fail, extraInfoStrs length {}",
                extra_info_strs.len()
            )));
        }
        Ok(extra_info_strs[4].clone())
    }

    pub fn get_broker_name(extra_info_strs: &[String]) -> rocketmq_error::RocketMQResult<String> {
        if extra_info_strs.len() < 6 {
            return Err(IllegalArgument(format!(
                "getBrokerName fail, extraInfoStrs length {}",
                extra_info_strs.len()
            )));
        }
        Ok(extra_info_strs[5].clone())
    }

    pub fn get_queue_id(extra_info_strs: &[String]) -> rocketmq_error::RocketMQResult<i32> {
        if extra_info_strs.len() < 7 {
            return Err(IllegalArgument(format!(
                "getQueueId fail, extraInfoStrs length {}",
                extra_info_strs.len()
            )));
        }
        extra_info_strs[6]
            .parse::<i32>()
            .map_err(|_| IllegalArgument("parse queue_id error".to_string()))
    }

    pub fn get_queue_offset(extra_info_strs: &[String]) -> rocketmq_error::RocketMQResult<i64> {
        if extra_info_strs.len() < 8 {
            return Err(IllegalArgument(format!(
                "getQueueOffset fail, extraInfoStrs length {}",
                extra_info_strs.len()
            )));
        }
        extra_info_strs[7]
            .parse::<i64>()
            .map_err(|_| IllegalArgument("parse queue_offset error".to_string()))
    }

    pub fn build_extra_info(
        ck_queue_offset: i64,
        pop_time: i64,
        invisible_time: i64,
        revive_qid: i32,
        topic: &str,
        broker_name: &str,
        queue_id: i32,
    ) -> String {
        let t = ExtraInfoUtil::get_retry(topic);
        format!(
            "{}{}{}{}{}{}{}{}{}{}{}{}{}",
            ck_queue_offset,
            MessageConst::KEY_SEPARATOR,
            pop_time,
            MessageConst::KEY_SEPARATOR,
            invisible_time,
            MessageConst::KEY_SEPARATOR,
            revive_qid,
            MessageConst::KEY_SEPARATOR,
            t,
            MessageConst::KEY_SEPARATOR,
            broker_name,
            MessageConst::KEY_SEPARATOR,
            queue_id
        )
    }

    pub fn build_extra_info_with_msg_queue_offset(
        ck_queue_offset: i64,
        pop_time: i64,
        invisible_time: i64,
        revive_qid: i32,
        topic: &str,
        broker_name: &str,
        queue_id: i32,
        msg_queue_offset: i64,
    ) -> String {
        let t = ExtraInfoUtil::get_retry(topic);
        format!(
            "{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}",
            ck_queue_offset,
            MessageConst::KEY_SEPARATOR,
            pop_time,
            MessageConst::KEY_SEPARATOR,
            invisible_time,
            MessageConst::KEY_SEPARATOR,
            revive_qid,
            MessageConst::KEY_SEPARATOR,
            t,
            MessageConst::KEY_SEPARATOR,
            broker_name,
            MessageConst::KEY_SEPARATOR,
            queue_id,
            MessageConst::KEY_SEPARATOR,
            msg_queue_offset
        )
    }

    pub fn build_start_offset_info(
        string_builder: &mut String,
        topic: &str,
        queue_id: i32,
        start_offset: i64,
    ) {
        let retry = ExtraInfoUtil::get_retry(topic);
        if !string_builder.is_empty() {
            string_builder.push(';');
        }
        string_builder.push_str(&format!(
            "{}{}{}{}{}",
            retry,
            MessageConst::KEY_SEPARATOR,
            queue_id,
            MessageConst::KEY_SEPARATOR,
            start_offset
        ));
    }

    pub fn build_queue_id_order_count_info(
        string_builder: &mut String,
        topic: &str,
        queue_id: i32,
        order_count: i32,
    ) {
        let retry = ExtraInfoUtil::get_retry(topic);
        if !string_builder.is_empty() {
            string_builder.push(';');
        }
        string_builder.push_str(&format!(
            "{}{}{}{}{}",
            retry,
            MessageConst::KEY_SEPARATOR,
            queue_id,
            MessageConst::KEY_SEPARATOR,
            order_count
        ));
    }

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
        string_builder.push_str(&format!(
            "{}{}{}{}{}",
            ExtraInfoUtil::get_retry(topic),
            MessageConst::KEY_SEPARATOR,
            ExtraInfoUtil::get_queue_offset_key_value_key(queue_id, queue_offset),
            MessageConst::KEY_SEPARATOR,
            order_count
        ));
    }

    pub fn build_msg_offset_info(
        string_builder: &mut String,
        topic: &str,
        queue_id: i32,
        msg_offsets: Vec<u64>,
    ) {
        let retry = ExtraInfoUtil::get_retry(topic);
        if !string_builder.is_empty() {
            string_builder.push(';');
        }
        string_builder.push_str(&format!(
            "{}{}{}{}",
            retry,
            MessageConst::KEY_SEPARATOR,
            queue_id,
            MessageConst::KEY_SEPARATOR,
        ));
        for (i, msg_offset) in msg_offsets.iter().enumerate() {
            string_builder.push_str(&msg_offset.to_string());
            if i < msg_offsets.len() - 1 {
                string_builder.push(',');
            }
        }
    }

    pub fn parse_msg_offset_info(
        msg_offset_info: &str,
    ) -> rocketmq_error::RocketMQResult<HashMap<String, Vec<i64>>> {
        let mut msg_offset_map = HashMap::new();
        let entries: Vec<&str> = msg_offset_info.split(';').collect();

        for entry in entries {
            let parts: Vec<&str> = entry.split(MessageConst::KEY_SEPARATOR).collect();
            if parts.len() != 3 {
                return Err(IllegalArgument("parse msgOffsetInfo error".to_string()));
            }

            let key = format!("{}@{}", parts[0], parts[1]);
            let offsets: Vec<i64> = parts[2]
                .split(',')
                .map(|s| s.parse::<i64>().unwrap())
                .collect();

            msg_offset_map.insert(key, offsets);
        }

        Ok(msg_offset_map)
    }

    pub fn parse_start_offset_info(
        start_offset_info: &str,
    ) -> rocketmq_error::RocketMQResult<HashMap<String, i64>> {
        let mut start_offset_map = HashMap::new();
        let entries: Vec<&str> = start_offset_info.split(';').collect();

        for entry in entries {
            let parts: Vec<&str> = entry.split(MessageConst::KEY_SEPARATOR).collect();
            if parts.len() != 3 {
                return Err(IllegalArgument("parse startOffsetInfo error".to_string()));
            }

            let key = format!("{}@{}", parts[0], parts[1]);
            let offset = parts[2]
                .parse::<i64>()
                .map_err(|_| IllegalArgument("Invalid start offset value".to_string()))?;
            start_offset_map.insert(key, offset);
        }

        Ok(start_offset_map)
    }

    pub fn parse_order_count_info(
        order_count_info: &str,
    ) -> rocketmq_error::RocketMQResult<HashMap<String, i32>> {
        let mut start_offset_map = HashMap::new();
        if order_count_info.is_empty() {
            return Ok(start_offset_map); // return None if the input is empty
        }

        // Split the input string by semicolon
        let array: Vec<&str> = order_count_info.split(';').collect();

        for one in array {
            // Split each element by the separator
            let split: Vec<&str> = one.split(MessageConst::KEY_SEPARATOR).collect();

            if split.len() != 3 {
                return Err(IllegalArgument(format!(
                    "parse orderCountInfo error {}",
                    order_count_info
                ))); // Handle invalid split
            }

            // Construct the key as `split[0]@split[1]`
            let key = format!("{}@{}", split[0], split[1]);

            // Check for duplicate keys
            if start_offset_map.contains_key(&key) {
                return Err(IllegalArgument(format!(
                    "parse orderCountInfo error, duplicate key, {}",
                    order_count_info
                )));
            }

            // Insert the key-value pair into the map
            if let Ok(value) = split[2].parse::<i32>() {
                start_offset_map.insert(key, value);
            } else {
                return Err(IllegalArgument(format!(
                    "parse orderCountInfo error, duplicate key, {}",
                    order_count_info
                )));
            }
        }

        Ok(start_offset_map) // Return the populated HashMap
    }

    pub fn get_retry_for_topic(topic: &str) -> String {
        if topic.starts_with("RETRY_") {
            RETRY_TOPIC.to_string()
        } else {
            NORMAL_TOPIC.to_string()
        }
    }

    pub fn get_start_offset_info_map_key(topic: &str, key: i64) -> String {
        format!("{}@{}", ExtraInfoUtil::get_retry(topic), key)
    }

    pub fn get_start_offset_info_map_key_with_pop_ck(
        topic: &str,
        pop_ck: &str,
        key: i64,
    ) -> String {
        format!(
            "{}@{}",
            ExtraInfoUtil::get_retry_with_pop_ck(topic, pop_ck),
            key
        )
    }

    pub fn get_queue_offset_key_value_key(queue_id: i64, queue_offset: i64) -> String {
        format!("{}{}%{}", QUEUE_OFFSET, queue_id, queue_offset)
    }

    pub fn get_queue_offset_map_key(topic: &str, queue_id: i64, queue_offset: i64) -> String {
        format!(
            "{}@{}",
            ExtraInfoUtil::get_retry(topic),
            ExtraInfoUtil::get_queue_offset_key_value_key(queue_id, queue_offset)
        )
    }

    pub fn is_order(extra_info: &[String]) -> bool {
        ExtraInfoUtil::get_revive_qid(extra_info).unwrap_or_default() == POP_ORDER_REVIVE_QUEUE
    }

    fn get_retry(topic: &str) -> String {
        if KeyBuilder::is_pop_retry_topic_v2(topic) {
            RETRY_TOPIC_V2.to_string()
        } else if topic.starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX) {
            RETRY_TOPIC.to_string()
        } else {
            NORMAL_TOPIC.to_string()
        }
    }

    fn get_retry_with_pop_ck(topic: &str, pop_ck: &str) -> String {
        if let Ok(split_info) = ExtraInfoUtil::split(pop_ck) {
            return ExtraInfoUtil::get_retry_from_split(&split_info);
        }
        ExtraInfoUtil::get_retry(topic)
    }

    fn get_retry_from_split(split_info: &[String]) -> String {
        if split_info.is_empty() {
            return NORMAL_TOPIC.to_string();
        }
        ExtraInfoUtil::get_retry(split_info[0].as_str())
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_common::common::key_builder::KeyBuilder;
    use rocketmq_error::RocketmqError::IllegalArgument;

    use super::*;

    #[test]
    fn split_with_valid_string() {
        let result = ExtraInfoUtil::split("a|b|c").unwrap();
        assert_eq!(result, vec!["a", "b", "c"]);
    }

    #[test]
    fn split_with_empty_string() {
        let result = ExtraInfoUtil::split("").unwrap_err();
        assert_eq!(
            result.to_string(),
            IllegalArgument("split extraInfo is empty".to_string()).to_string()
        );
    }

    #[test]
    fn get_ck_queue_offset_with_valid_string() {
        let result = ExtraInfoUtil::get_ck_queue_offset(&vec!["123".to_string()]).unwrap();
        assert_eq!(result, 123);
    }

    #[test]
    fn get_ck_queue_offset_with_invalid_string() {
        let result = ExtraInfoUtil::get_ck_queue_offset(&vec!["abc".to_string()]).unwrap_err();
        assert_eq!(
            result.to_string(),
            IllegalArgument("parse ck_queue_offset error".to_string()).to_string()
        );
    }

    #[test]
    fn get_pop_time_with_valid_string() {
        let result =
            ExtraInfoUtil::get_pop_time(&vec!["123".to_string(), "456".to_string()]).unwrap();
        assert_eq!(result, 456);
    }

    #[test]
    fn get_pop_time_with_insufficient_length() {
        let result = ExtraInfoUtil::get_pop_time(&vec!["123".to_string()]).unwrap_err();
        assert_eq!(
            result.to_string(),
            IllegalArgument("getPopTime fail, extraInfoStrs length 1".to_string()).to_string()
        );
    }

    #[test]
    fn get_invisible_time_with_valid_string() {
        let result = ExtraInfoUtil::get_invisible_time(&vec![
            "123".to_string(),
            "456".to_string(),
            "789".to_string(),
        ])
        .unwrap();
        assert_eq!(result, 789);
    }

    #[test]
    fn get_invisible_time_with_insufficient_length() {
        let result = ExtraInfoUtil::get_invisible_time(&vec!["123".to_string(), "456".to_string()])
            .unwrap_err();
        assert_eq!(
            result.to_string(),
            IllegalArgument("getInvisibleTime fail, extraInfoStrs length 2".to_string())
                .to_string()
        );
    }

    #[test]
    fn get_revive_qid_with_valid_string() {
        let result = ExtraInfoUtil::get_revive_qid(&vec![
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
        let result = ExtraInfoUtil::get_revive_qid(&vec![
            "123".to_string(),
            "456".to_string(),
            "789".to_string(),
        ])
        .unwrap_err();
        assert_eq!(
            result.to_string(),
            IllegalArgument("getReviveQid fail, extraInfoStrs length 3".to_string()).to_string()
        );
    }

    #[test]
    fn get_real_topic_with_retry_v1() {
        let result = ExtraInfoUtil::get_real_topic(
            &vec![
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
            &vec![
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
            &vec![
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
            &vec![
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
            IllegalArgument("getRealTopic fail, extraInfoStrs length 4".to_string()).to_string()
        );
    }

    #[test]
    fn get_real_topic_with_retry_invalid() {
        let result = ExtraInfoUtil::get_real_topic_with_retry("topic", "cid", "3").unwrap_err();
        assert_eq!(
            result.to_string(),
            IllegalArgument("getRetry fail, format is wrong".to_string()).to_string()
        );
    }

    #[test]
    fn get_retry_slice_with_valid_string() {
        let result = ExtraInfoUtil::get_retry_slice(&vec![
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
        let result = ExtraInfoUtil::get_retry_slice(&vec![
            "123".to_string(),
            "456".to_string(),
            "789".to_string(),
            "10".to_string(),
        ])
        .unwrap_err();
        assert_eq!(
            result.to_string(),
            IllegalArgument("getRetry fail, extraInfoStrs length 4".to_string()).to_string()
        );
    }

    #[test]
    fn get_broker_name_with_valid_string() {
        let result = ExtraInfoUtil::get_broker_name(&vec![
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
        let result = ExtraInfoUtil::get_broker_name(&vec![
            "123".to_string(),
            "456".to_string(),
            "789".to_string(),
            "10".to_string(),
            "1".to_string(),
        ])
        .unwrap_err();
        assert_eq!(
            result.to_string(),
            IllegalArgument("getBrokerName fail, extraInfoStrs length 5".to_string()).to_string()
        );
    }

    #[test]
    fn get_queue_id_with_valid_string() {
        let result = ExtraInfoUtil::get_queue_id(&vec![
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
        let result = ExtraInfoUtil::get_queue_id(&vec![
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
            IllegalArgument("getQueueId fail, extraInfoStrs length 6".to_string()).to_string()
        );
    }

    #[test]
    fn get_queue_offset_with_valid_string() {
        let result = ExtraInfoUtil::get_queue_offset(&vec![
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
        let result = ExtraInfoUtil::get_queue_offset(&vec![
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
            IllegalArgument("getQueueOffset fail, extraInfoStrs length 7".to_string()).to_string()
        );
    }

    #[test]
    fn build_extra_info_creates_correct_string() {
        let result = ExtraInfoUtil::build_extra_info(123, 456, 789, 10, "topic", "broker", 7);
        assert_eq!(result, "123 456 789 10 0 broker 7");
    }

    #[test]
    fn build_extra_info_with_msg_queue_offset_creates_correct_string() {
        let result = ExtraInfoUtil::build_extra_info_with_msg_queue_offset(
            123, 456, 789, 10, "topic", "broker", 7, 100,
        );
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
        ExtraInfoUtil::build_queue_offset_order_count_info(
            &mut string_builder,
            "topic",
            7,
            100,
            100,
        );
        assert_eq!(string_builder, "0 qo7%100 100");
    }

    #[test]
    fn build_msg_offset_info_creates_correct_string() {
        let mut string_builder = String::new();
        ExtraInfoUtil::build_msg_offset_info(&mut string_builder, "topic", 7, vec![100, 200, 300]);
        assert_eq!(string_builder, "0 7 100,200,300");
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
        let result = ExtraInfoUtil::get_retry_for_topic("RETRY_topic");
        assert_eq!(result, "1");
    }

    #[test]
    fn get_retry_for_topic_with_normal_topic() {
        let result = ExtraInfoUtil::get_retry_for_topic("topic");
        assert_eq!(result, "0");
    }

    #[test]
    fn get_start_offset_info_map_key_creates_correct_string() {
        let result = ExtraInfoUtil::get_start_offset_info_map_key("topic", 7);
        assert_eq!(result, "0@7");
    }

    #[test]
    fn get_start_offset_info_map_key_with_pop_ck_creates_correct_string() {
        let result = ExtraInfoUtil::get_start_offset_info_map_key_with_pop_ck("topic", "pop_ck", 7);
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
        let result = ExtraInfoUtil::is_order(&vec![
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
        let result = ExtraInfoUtil::is_order(&vec![
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
