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
use std::vec::Vec;

use crate::common::wire_constants::KEY_SEPARATOR;
use crate::common::wire_constants::RETRY_GROUP_TOPIC_PREFIX;
use rocketmq_error::Error;
use rocketmq_error::Result;
use rocketmq_model::common::key_builder;
use rocketmq_model::common::key_builder::KeyBuilder;
use rocketmq_model::common::pop_retry_policy::PopRetryPolicy;

pub struct ExtraInfoUtil;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AckExtraInfo<'a> {
    pub ck_queue_offset: i64,
    pub pop_time: i64,
    pub invisible_time: i64,
    pub revive_queue_id: i32,
    pub retry: &'a str,
    pub broker_name: &'a str,
    pub queue_id: i32,
    pub queue_offset: i64,
    pub retry_generation: Option<u64>,
}

const NORMAL_TOPIC: &str = "0";
const RETRY_TOPIC: &str = "1";
const RETRY_TOPIC_V2: &str = "2";
const QUEUE_OFFSET: &str = "qo";

#[inline]
fn illegal_argument(message: impl Into<String>) -> Error {
    crate::error::invalid_argument(message)
}

impl ExtraInfoUtil {
    /// Parses the fixed POP receipt-handle prefix without allocating its fields.
    pub fn parse_ack_extra_info(extra_info: &str) -> Result<AckExtraInfo<'_>> {
        let mut parts = extra_info.split(KEY_SEPARATOR);
        let mut next = |name: &str| {
            parts
                .next()
                .ok_or_else(|| illegal_argument(format!("parse POP receipt handle: missing {name}")))
        };

        let ck_queue_offset = next("checkpoint queue offset")?
            .parse::<i64>()
            .map_err(|_| illegal_argument("parse ck_queue_offset error"))?;
        let pop_time = next("pop time")?
            .parse::<i64>()
            .map_err(|_| illegal_argument("parse pop_time error"))?;
        let invisible_time = next("invisible time")?
            .parse::<i64>()
            .map_err(|_| illegal_argument("parse invisible_time error"))?;
        let revive_queue_id = next("revive queue id")?
            .parse::<i32>()
            .map_err(|_| illegal_argument("parse revive_qid error"))?;
        let retry = next("retry marker")?;
        let broker_name = next("broker name")?;
        let queue_id = next("queue id")?
            .parse::<i32>()
            .map_err(|_| illegal_argument("parse queue_id error"))?;
        let queue_offset = next("queue offset")?
            .parse::<i64>()
            .map_err(|_| illegal_argument("parse queue_offset error"))?;
        let retry_generation = parts
            .next()
            .map(|value| {
                value
                    .parse::<u64>()
                    .map_err(|_| illegal_argument("parse retry generation error"))
            })
            .transpose()?;

        if broker_name.trim().is_empty() {
            return Err(illegal_argument("parse POP receipt handle: broker name is blank"));
        }
        if !matches!(retry, NORMAL_TOPIC | RETRY_TOPIC | RETRY_TOPIC_V2) {
            return Err(illegal_argument("parse POP receipt handle: invalid retry marker"));
        }

        Ok(AckExtraInfo {
            ck_queue_offset,
            pop_time,
            invisible_time,
            revive_queue_id,
            retry,
            broker_name,
            queue_id,
            queue_offset,
            retry_generation,
        })
    }

    /// Split the extra info string using the key separator
    pub fn split(extra_info: &str) -> Vec<String> {
        extra_info.split(KEY_SEPARATOR).map(|s| s.to_string()).collect()
    }

    /// Get the checkpoint queue offset from the extra info
    pub fn get_ck_queue_offset(extra_info_strs: &[String]) -> Result<i64> {
        if extra_info_strs.is_empty() {
            return Err(illegal_argument(format!(
                "getCkQueueOffset fail, extraInfoStrs length {}",
                extra_info_strs.len()
            )));
        }
        extra_info_strs[0]
            .parse::<i64>()
            .map_err(|_| illegal_argument("parse ck_queue_offset error"))
    }

    /// Get the pop time from the extra info
    pub fn get_pop_time(extra_info_strs: &[String]) -> Result<i64> {
        if extra_info_strs.len() < 2 {
            return Err(illegal_argument(format!(
                "getPopTime fail, extraInfoStrs length {}",
                extra_info_strs.len()
            )));
        }
        extra_info_strs[1]
            .parse::<i64>()
            .map_err(|_| illegal_argument("parse pop_time error"))
    }

    /// Get the invisible time from the extra info
    pub fn get_invisible_time(extra_info_strs: &[String]) -> Result<i64> {
        if extra_info_strs.len() < 3 {
            return Err(illegal_argument(format!(
                "getInvisibleTime fail, extraInfoStrs length {}",
                extra_info_strs.len()
            )));
        }
        extra_info_strs[2]
            .parse::<i64>()
            .map_err(|_| illegal_argument("parse invisible_time error"))
    }

    /// Get the revive queue ID from the extra info
    pub fn get_revive_qid(extra_info_strs: &[String]) -> Result<i32> {
        if extra_info_strs.len() < 4 {
            return Err(illegal_argument(format!(
                "getReviveQid fail, extraInfoStrs length {}",
                extra_info_strs.len()
            )));
        }
        extra_info_strs[3]
            .parse::<i32>()
            .map_err(|_| illegal_argument("parse revive_qid error"))
    }

    /// Get the real topic name based on the retry flag
    pub fn get_real_topic(extra_info_strs: &[String], topic: &str, cid: &str) -> Result<String> {
        if extra_info_strs.len() < 5 {
            return Err(illegal_argument(format!(
                "getRealTopic fail, extraInfoStrs length {}",
                extra_info_strs.len()
            )));
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
    pub fn get_real_topic_with_retry(topic: &str, cid: &str, retry: &str) -> Result<String> {
        if retry == NORMAL_TOPIC {
            Ok(topic.to_string())
        } else if retry == RETRY_TOPIC {
            Ok(KeyBuilder::build_pop_retry_topic_v1(topic, cid))
        } else if retry == RETRY_TOPIC_V2 {
            Ok(KeyBuilder::build_pop_retry_topic_v2(topic, cid))
        } else {
            Err(illegal_argument("getRetry fail, format is wrong"))
        }
    }

    /// Get the retry flag from the extra info
    pub fn get_retry(extra_info_strs: &[String]) -> Result<String> {
        if extra_info_strs.len() < 5 {
            return Err(illegal_argument(format!(
                "getRetry fail, extraInfoStrs length {}",
                extra_info_strs.len()
            )));
        }
        Ok(extra_info_strs[4].clone())
    }

    /// Get the broker name from the extra info
    pub fn get_broker_name(extra_info_strs: &[String]) -> Result<String> {
        if extra_info_strs.len() < 6 {
            return Err(illegal_argument(format!(
                "getBrokerName fail, extraInfoStrs length {}",
                extra_info_strs.len()
            )));
        }
        Ok(extra_info_strs[5].clone())
    }

    /// Get the queue ID from the extra info
    pub fn get_queue_id(extra_info_strs: &[String]) -> rocketmq_error::Result<i32> {
        if extra_info_strs.len() < 7 {
            return Err(illegal_argument(format!(
                "getQueueId fail, extraInfoStrs length {}",
                extra_info_strs.len()
            )));
        }
        extra_info_strs[6]
            .parse::<i32>()
            .map_err(|_| illegal_argument("parse queue_id error"))
    }

    /// Get the queue offset from the extra info
    pub fn get_queue_offset(extra_info_strs: &[String]) -> rocketmq_error::Result<i64> {
        if extra_info_strs.len() < 8 {
            return Err(illegal_argument(format!(
                "getQueueOffset fail, extraInfoStrs length {}",
                extra_info_strs.len()
            )));
        }
        extra_info_strs[7]
            .parse::<i64>()
            .map_err(|_| illegal_argument("parse queue_offset error"))
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
            sep = KEY_SEPARATOR
        )
    }

    /// Builds the same text as [`Self::build_extra_info`] straight into one shared allocation.
    pub fn build_extra_info_cheetah(
        ck_queue_offset: i64,
        pop_time: i64,
        invisible_time: i64,
        revive_qid: i32,
        topic: &str,
        broker_name: &str,
        queue_id: i32,
    ) -> CheetahString {
        let mut numbers = [itoa::Buffer::new(); 5];
        let [ck_queue_offset_text, pop_time_text, invisible_time_text, revive_qid_text, queue_id_text] = &mut numbers;
        CheetahString::concat(&[
            ck_queue_offset_text.format(ck_queue_offset),
            KEY_SEPARATOR,
            pop_time_text.format(pop_time),
            KEY_SEPARATOR,
            invisible_time_text.format(invisible_time),
            KEY_SEPARATOR,
            revive_qid_text.format(revive_qid),
            KEY_SEPARATOR,
            Self::get_retry_from_topic(topic),
            KEY_SEPARATOR,
            broker_name,
            KEY_SEPARATOR,
            queue_id_text.format(queue_id),
        ])
    }

    /// Builds the same text as [`Self::build_extra_info_with_offset`] straight into one shared
    /// allocation.
    #[allow(clippy::too_many_arguments, reason = "mirrors build_extra_info_with_offset")]
    pub fn build_extra_info_with_offset_cheetah(
        ck_queue_offset: i64,
        pop_time: i64,
        invisible_time: i64,
        revive_qid: i32,
        topic: &str,
        broker_name: &str,
        queue_id: i32,
        msg_queue_offset: i64,
    ) -> CheetahString {
        let mut numbers = [itoa::Buffer::new(); 6];
        let [ck_queue_offset_text, pop_time_text, invisible_time_text, revive_qid_text, queue_id_text, msg_queue_offset_text] =
            &mut numbers;
        CheetahString::concat(&[
            ck_queue_offset_text.format(ck_queue_offset),
            KEY_SEPARATOR,
            pop_time_text.format(pop_time),
            KEY_SEPARATOR,
            invisible_time_text.format(invisible_time),
            KEY_SEPARATOR,
            revive_qid_text.format(revive_qid),
            KEY_SEPARATOR,
            Self::get_retry_from_topic(topic),
            KEY_SEPARATOR,
            broker_name,
            KEY_SEPARATOR,
            queue_id_text.format(queue_id),
            KEY_SEPARATOR,
            msg_queue_offset_text.format(msg_queue_offset),
        ])
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
            sep = KEY_SEPARATOR
        )
    }

    /// Builds a receipt handle that carries the persisted POP retry policy generation.
    ///
    /// Existing eight-field handles remain valid. The optional ninth field is emitted only
    /// for retry topics, while the existing retry marker continues to identify v1 or v2.
    #[allow(clippy::too_many_arguments, reason = "preserves the established POP receipt fields")]
    pub fn build_extra_info_with_offset_and_retry_policy(
        ck_queue_offset: i64,
        pop_time: i64,
        invisible_time: i64,
        revive_qid: i32,
        topic: &str,
        broker_name: &str,
        queue_id: i32,
        msg_queue_offset: i64,
        retry_policy: &PopRetryPolicy,
    ) -> String {
        let base = Self::build_extra_info_with_offset(
            ck_queue_offset,
            pop_time,
            invisible_time,
            revive_qid,
            topic,
            broker_name,
            queue_id,
            msg_queue_offset,
        );
        if KeyBuilder::classify_pop_retry_topic(topic).is_some() {
            format!("{base}{KEY_SEPARATOR}{}", retry_policy.generation)
        } else {
            base
        }
    }

    /// Build start offset info
    pub fn build_start_offset_info(string_builder: &mut String, topic: &str, queue_id: i32, start_offset: i64) {
        if !string_builder.is_empty() {
            string_builder.push(';');
        }

        string_builder.push_str(Self::get_retry_from_topic(topic));
        string_builder.push_str(KEY_SEPARATOR);
        string_builder.push_str(&queue_id.to_string());
        string_builder.push_str(KEY_SEPARATOR);
        string_builder.push_str(&start_offset.to_string());
    }

    /// Build queue ID order count info
    pub fn build_queue_id_order_count_info(string_builder: &mut String, topic: &str, queue_id: i32, order_count: i32) {
        if !string_builder.is_empty() {
            string_builder.push(';');
        }

        string_builder.push_str(Self::get_retry_from_topic(topic));
        string_builder.push_str(KEY_SEPARATOR);
        string_builder.push_str(&queue_id.to_string());
        string_builder.push_str(KEY_SEPARATOR);
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

        string_builder.push_str(Self::get_retry_from_topic(topic));
        string_builder.push_str(KEY_SEPARATOR);
        string_builder.push_str(&Self::get_queue_offset_key_value_key(queue_id, queue_offset));
        string_builder.push_str(KEY_SEPARATOR);
        string_builder.push_str(&order_count.to_string());
    }

    /// Build message offset info
    pub fn build_msg_offset_info(string_builder: &mut String, topic: &str, queue_id: i32, msg_offsets: &[u64]) {
        if !string_builder.is_empty() {
            string_builder.push(';');
        }

        string_builder.push_str(Self::get_retry_from_topic(topic));
        string_builder.push_str(KEY_SEPARATOR);
        string_builder.push_str(&queue_id.to_string());
        string_builder.push_str(KEY_SEPARATOR);

        for (i, offset) in msg_offsets.iter().enumerate() {
            string_builder.push_str(&offset.to_string());
            if i < msg_offsets.len() - 1 {
                string_builder.push(',');
            }
        }
    }

    /// Parse message offset info into a HashMap
    pub fn parse_msg_offset_info(msg_offset_info: &str) -> Result<HashMap<String, Vec<i64>>> {
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
            let split: Vec<&str> = one.split(KEY_SEPARATOR).collect();
            if split.len() != 3 {
                return Err(illegal_argument("parse msgOffsetInfo error"));
            }

            let key = format!("{}@{}", split[0], split[1]);
            let offsets = match msg_offset_map.entry(key) {
                std::collections::hash_map::Entry::Occupied(_) => {
                    return Err(illegal_argument("parse msgOffsetMap error, duplicate"));
                }
                std::collections::hash_map::Entry::Vacant(entry) => entry.insert(Vec::with_capacity(8)),
            };
            let msg_offsets: Vec<&str> = split[2].split(",").collect();

            for msg_offset in msg_offsets {
                let offset = msg_offset
                    .parse::<i64>()
                    .map_err(|_| illegal_argument("parse msgOffset error"))?;
                offsets.push(offset);
            }
        }

        Ok(msg_offset_map)
    }

    /// Parse start offset info into a HashMap
    pub fn parse_start_offset_info(start_offset_info: &str) -> Result<HashMap<String, i64>> {
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
            let split: Vec<&str> = one.split(KEY_SEPARATOR).collect();
            if split.len() != 3 {
                return Err(illegal_argument(format!(
                    "parse startOffsetInfo error, {start_offset_info}"
                )));
            }

            let key = format!("{}@{}", split[0], split[1]);
            if start_offset_map.contains_key(&key) {
                return Err(illegal_argument("parse startOffsetMap error, duplicate"));
            }

            start_offset_map.insert(
                key,
                split[2]
                    .parse::<i64>()
                    .map_err(|_| illegal_argument("parse startOffset error"))?,
            );
        }

        Ok(start_offset_map)
    }

    /// Parse order count info into a HashMap
    pub fn parse_order_count_info(order_count_info: &str) -> Result<HashMap<String, i32>> {
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
            let split: Vec<&str> = one.split(KEY_SEPARATOR).collect();
            if split.len() != 3 {
                return Err(illegal_argument(format!(
                    "parse orderCountInfo error {order_count_info}"
                )));
            }

            let key = format!("{}@{}", split[0], split[1]);
            if order_count_map.contains_key(&key) {
                return Err(illegal_argument(format!(
                    "parse orderCountInfo error, duplicate, {order_count_info}"
                )));
            }

            order_count_map.insert(
                key,
                split[2]
                    .parse::<i32>()
                    .map_err(|_| illegal_argument("parse orderCountInfo error"))?,
            );
        }

        Ok(order_count_map)
    }

    /// Get start offset info map key
    pub fn get_start_offset_info_map_key(topic: &str, key: i64) -> String {
        let mut map_key = String::with_capacity(24);
        Self::write_start_offset_info_map_key(&mut map_key, topic, key);
        map_key
    }

    /// Writes the key [`Self::get_start_offset_info_map_key`] returns into `map_key`, replacing
    /// its contents, so that a caller can look up many messages with one buffer.
    pub fn write_start_offset_info_map_key(map_key: &mut String, topic: &str, key: i64) {
        map_key.clear();
        map_key.push_str(Self::get_retry_from_topic(topic));
        map_key.push('@');
        map_key.push_str(itoa::Buffer::new().format(key));
    }

    /// Get start offset info map key with pop check
    pub fn get_start_offset_info_map_key_with_pop_ck(topic: &str, pop_ck: Option<&str>, key: i64) -> Result<String> {
        Ok(format!("{}@{}", Self::get_retry_from_topic_pop_ck(topic, pop_ck)?, key))
    }

    /// Get queue offset key value key
    pub fn get_queue_offset_key_value_key(queue_id: i64, queue_offset: i64) -> String {
        format!("{}{}{}{}", QUEUE_OFFSET, queue_id, "%", queue_offset)
    }

    /// Get queue offset map key
    pub fn get_queue_offset_map_key(topic: &str, queue_id: i64, queue_offset: i64) -> String {
        let mut map_key = String::with_capacity(48);
        Self::write_queue_offset_map_key(&mut map_key, topic, queue_id, queue_offset);
        map_key
    }

    /// Writes the key [`Self::get_queue_offset_map_key`] returns into `map_key`, replacing its
    /// contents, so that a caller can look up many messages with one buffer.
    pub fn write_queue_offset_map_key(map_key: &mut String, topic: &str, queue_id: i64, queue_offset: i64) {
        map_key.clear();
        map_key.push_str(Self::get_retry_from_topic(topic));
        map_key.push('@');
        map_key.push_str(QUEUE_OFFSET);
        map_key.push_str(itoa::Buffer::new().format(queue_id));
        map_key.push('%');
        map_key.push_str(itoa::Buffer::new().format(queue_offset));
    }

    /// Check if the extra info represents an order
    pub fn is_order(extra_info: &[String]) -> bool {
        Self::get_revive_qid(extra_info).unwrap_or_default() == key_builder::POP_ORDER_REVIVE_QUEUE
    }

    /// Get retry flag from topic name
    fn get_retry_from_topic(topic: &str) -> &'static str {
        if KeyBuilder::is_pop_retry_topic_v2(topic) {
            RETRY_TOPIC_V2
        } else if topic.starts_with(RETRY_GROUP_TOPIC_PREFIX) {
            RETRY_TOPIC
        } else {
            NORMAL_TOPIC
        }
    }

    /// Get retry flag with pop check
    fn get_retry_from_topic_pop_ck(topic: &str, pop_ck: Option<&str>) -> Result<String> {
        match pop_ck {
            Some(ck) => Self::get_retry(&Self::split(ck)),
            None => Ok(Self::get_retry_from_topic(topic).to_owned()),
        }
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_model::common::key_builder::KeyBuilder;

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
        assert_eq!(result.descriptor(), &rocketmq_error::CORE_ARGUMENT_INVALID);
    }

    #[test]
    fn get_pop_time_with_valid_string() {
        let result = ExtraInfoUtil::get_pop_time(&["123".to_string(), "456".to_string()]).unwrap();
        assert_eq!(result, 456);
    }

    #[test]
    fn get_pop_time_with_insufficient_length() {
        let result = ExtraInfoUtil::get_pop_time(&["123".to_string()]).unwrap_err();
        assert_eq!(result.descriptor(), &rocketmq_error::CORE_ARGUMENT_INVALID);
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
        assert_eq!(result.descriptor(), &rocketmq_error::CORE_ARGUMENT_INVALID);
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
        assert_eq!(result.descriptor(), &rocketmq_error::CORE_ARGUMENT_INVALID);
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
        assert_eq!(result.descriptor(), &rocketmq_error::CORE_ARGUMENT_INVALID);
    }

    #[test]
    fn get_real_topic_with_retry_invalid() {
        let result = ExtraInfoUtil::get_real_topic_with_retry("topic", "cid", "3").unwrap_err();
        assert_eq!(result.descriptor(), &rocketmq_error::CORE_ARGUMENT_INVALID);
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
        assert_eq!(result.descriptor(), &rocketmq_error::CORE_ARGUMENT_INVALID);
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
        assert_eq!(result.descriptor(), &rocketmq_error::CORE_ARGUMENT_INVALID);
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
        assert_eq!(result.descriptor(), &rocketmq_error::CORE_ARGUMENT_INVALID);
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
        assert_eq!(result.descriptor(), &rocketmq_error::CORE_ARGUMENT_INVALID);
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
    fn retry_receipt_round_trips_actual_version_and_policy_generation() {
        let policy = PopRetryPolicy::dual_read_v2_write(17);
        let retry_topic = policy.write_topic("topic", "consumer-a");
        let handle = ExtraInfoUtil::build_extra_info_with_offset_and_retry_policy(
            123,
            456,
            789,
            10,
            &retry_topic,
            "broker",
            7,
            100,
            &policy,
        );

        let parsed = ExtraInfoUtil::parse_ack_extra_info(&handle).expect("generated receipt should parse");
        assert_eq!(parsed.retry, RETRY_TOPIC_V2);
        assert_eq!(parsed.retry_generation, Some(17));
        assert_eq!(
            ExtraInfoUtil::get_real_topic_with_retry("topic", "consumer-a", parsed.retry)
                .expect("retry marker should resolve"),
            retry_topic
        );
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
    fn msg_offset_info_round_trips_multiple_offsets_and_queues() {
        let mut string_builder = String::new();
        ExtraInfoUtil::build_msg_offset_info(&mut string_builder, "topic", 7, &[100, 200, 300]);
        ExtraInfoUtil::build_msg_offset_info(&mut string_builder, "topic", 8, &[42]);
        assert_eq!(string_builder, "0 7 100,200,300;0 8 42");
        assert_eq!(
            ExtraInfoUtil::parse_msg_offset_info(&string_builder).unwrap(),
            HashMap::from([("0@7".to_string(), vec![100, 200, 300]), ("0@8".to_string(), vec![42])])
        );
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
    fn buffered_keys_and_cheetah_extra_info_match_the_formatted_text() {
        let retry_v1 = KeyBuilder::build_pop_retry_topic("orders", "group-a", false);
        let retry_v2 = KeyBuilder::build_pop_retry_topic_v2("orders", "group-a");
        let mut map_key = String::from("stale");
        for topic in ["orders", "订单", retry_v1.as_str(), retry_v2.as_str()] {
            let flag = ExtraInfoUtil::get_retry_from_topic(topic);
            for number in [i64::MIN, -1, 0, 7, i64::MAX] {
                let queue_id = number as i32;
                ExtraInfoUtil::write_start_offset_info_map_key(&mut map_key, topic, number);
                assert_eq!(map_key, format!("{flag}@{number}"));
                assert_eq!(ExtraInfoUtil::get_start_offset_info_map_key(topic, number), map_key);
                let offset = number.wrapping_neg();
                ExtraInfoUtil::write_queue_offset_map_key(&mut map_key, topic, number, offset);
                assert_eq!(map_key, format!("{flag}@qo{number}%{offset}"));
                assert_eq!(ExtraInfoUtil::get_queue_offset_map_key(topic, number, offset), map_key);

                let extra_info = format!("{number} {number} {number} {queue_id} {flag} broker-a {queue_id}");
                assert_eq!(
                    ExtraInfoUtil::build_extra_info(number, number, number, queue_id, topic, "broker-a", queue_id),
                    extra_info
                );
                assert_eq!(
                    ExtraInfoUtil::build_extra_info_cheetah(
                        number, number, number, queue_id, topic, "broker-a", queue_id
                    ),
                    extra_info
                );
                let with_offset = format!("{extra_info} {number}");
                assert_eq!(
                    ExtraInfoUtil::build_extra_info_with_offset(
                        number, number, number, queue_id, topic, "broker-a", queue_id, number
                    ),
                    with_offset
                );
                assert_eq!(
                    ExtraInfoUtil::build_extra_info_with_offset_cheetah(
                        number, number, number, queue_id, topic, "broker-a", queue_id, number
                    ),
                    with_offset
                );
            }
        }
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
