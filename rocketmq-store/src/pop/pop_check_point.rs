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
use std::cmp::Ordering;
use std::fmt::Display;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct PopCheckPoint {
    #[serde(rename = "so")]
    pub start_offset: i64,
    #[serde(rename = "pt")]
    pub pop_time: i64,
    #[serde(rename = "it")]
    pub invisible_time: i64,
    #[serde(rename = "bm")]
    pub bit_map: i32,
    #[serde(rename = "n")]
    pub num: u8,
    #[serde(rename = "q")]
    pub queue_id: i32,
    #[serde(rename = "t")]
    pub topic: CheetahString,
    #[serde(rename = "c")]
    pub cid: CheetahString,
    #[serde(rename = "ro")]
    pub revive_offset: i64,
    #[serde(rename = "d")]
    pub queue_offset_diff: Vec<i32>,
    #[serde(rename = "bn")]
    pub broker_name: Option<CheetahString>,
    #[serde(rename = "rp")]
    pub re_put_times: Option<CheetahString>,
}

impl PopCheckPoint {
    pub fn add_diff(&mut self, diff: i32) {
        if self.queue_offset_diff.is_empty() {
            self.queue_offset_diff = Vec::with_capacity(8);
        }

        self.queue_offset_diff.push(diff);
    }

    pub fn index_of_ack(&self, ack_offset: i64) -> i32 {
        if ack_offset < self.start_offset {
            return -1;
        }

        // old version of checkpoint
        if self.queue_offset_diff.is_empty() {
            if ack_offset - self.start_offset < self.num as i64 {
                return (ack_offset - self.start_offset) as i32;
            }
            return -1;
        }

        // new version of checkpoint
        self.queue_offset_diff[(ack_offset - self.start_offset) as usize]
    }

    pub fn ack_offset_by_index(&self, index: u8) -> i64 {
        // old version of checkpoint
        if self.queue_offset_diff.is_empty() {
            return self.start_offset + index as i64;
        }

        self.start_offset + self.queue_offset_diff[index as usize] as i64
    }

    pub fn parse_re_put_times(&self) -> i32 {
        if self.re_put_times.is_none() {
            return 0;
        }
        if let Some(ref re_put_times) = self.re_put_times {
            if let Ok(parsed) = re_put_times.parse::<i32>() {
                return parsed;
            }
        }
        i32::MAX
    }
}

impl Ord for PopCheckPoint {
    fn cmp(&self, other: &Self) -> Ordering {
        self.start_offset.cmp(&other.start_offset)
    }
}

impl PartialOrd for PopCheckPoint {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Display for PopCheckPoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PopCheckPoint [start_offset={}, pop_time={}, invisible_time={}, bit_map={}, num={}, \
             queue_id={}, topic={}, cid={}, revive_offset={}, queue_offset_diff={:?}, \
             broker_name={}, re_put_times={}]",
            self.start_offset,
            self.pop_time,
            self.invisible_time,
            self.bit_map,
            self.num,
            self.queue_id,
            self.topic,
            self.cid,
            self.revive_offset,
            self.queue_offset_diff,
            self.broker_name.as_deref().unwrap_or("None"),
            self.re_put_times.as_deref().unwrap_or("None")
        )
    }
}

#[cfg(test)]
mod tests {
    use serde_json;

    use super::*;

    #[test]
    fn add_diff_appends_correctly() {
        let mut checkpoint = PopCheckPoint {
            start_offset: 0,
            pop_time: 0,
            invisible_time: 0,
            bit_map: 0,
            num: 0,
            queue_id: 0,
            topic: CheetahString::from(""),
            cid: CheetahString::from(""),
            revive_offset: 0,
            queue_offset_diff: vec![],
            broker_name: None,
            re_put_times: None,
        };
        checkpoint.add_diff(5);
        assert_eq!(checkpoint.queue_offset_diff, vec![5]);
    }

    #[test]
    fn index_of_ack_returns_correct_index() {
        let checkpoint = PopCheckPoint {
            start_offset: 10,
            pop_time: 0,
            invisible_time: 0,
            bit_map: 0,
            num: 5,
            queue_id: 0,
            topic: CheetahString::from(""),
            cid: CheetahString::from(""),
            revive_offset: 0,
            queue_offset_diff: vec![0, 1, 2, 3, 4],
            broker_name: None,
            re_put_times: None,
        };
        assert_eq!(checkpoint.index_of_ack(12), 2);
    }

    #[test]
    fn index_of_ack_returns_negative_for_invalid_offset() {
        let checkpoint = PopCheckPoint {
            start_offset: 10,
            pop_time: 0,
            invisible_time: 0,
            bit_map: 0,
            num: 5,
            queue_id: 0,
            topic: CheetahString::from(""),
            cid: CheetahString::from(""),
            revive_offset: 0,
            queue_offset_diff: vec![0, 1, 2, 3, 4],
            broker_name: None,
            re_put_times: None,
        };
        assert_eq!(checkpoint.index_of_ack(5), -1);
    }

    #[test]
    fn ack_offset_by_index_returns_correct_offset() {
        let checkpoint = PopCheckPoint {
            start_offset: 10,
            pop_time: 0,
            invisible_time: 0,
            bit_map: 0,
            num: 5,
            queue_id: 0,
            topic: CheetahString::from(""),
            cid: CheetahString::from(""),
            revive_offset: 0,
            queue_offset_diff: vec![0, 1, 2, 3, 4],
            broker_name: None,
            re_put_times: None,
        };
        assert_eq!(checkpoint.ack_offset_by_index(2), 12);
    }

    #[test]
    fn parse_re_put_times_parses_correctly() {
        let checkpoint = PopCheckPoint {
            start_offset: 0,
            pop_time: 0,
            invisible_time: 0,
            bit_map: 0,
            num: 0,
            queue_id: 0,
            topic: CheetahString::from(""),
            cid: CheetahString::from(""),
            revive_offset: 0,
            queue_offset_diff: vec![],
            broker_name: None,
            re_put_times: Some(CheetahString::from("5")),
        };
        assert_eq!(checkpoint.parse_re_put_times(), 5);
    }

    #[test]
    fn parse_re_put_times_returns_max_for_invalid_string() {
        let checkpoint = PopCheckPoint {
            start_offset: 0,
            pop_time: 0,
            invisible_time: 0,
            bit_map: 0,
            num: 0,
            queue_id: 0,
            topic: CheetahString::from(""),
            cid: CheetahString::from(""),
            revive_offset: 0,
            queue_offset_diff: vec![],
            broker_name: None,
            re_put_times: Some(CheetahString::from("invalid")),
        };
        assert_eq!(checkpoint.parse_re_put_times(), i32::MAX);
    }

    #[test]
    fn parse_re_put_times_returns_zero_for_none() {
        let checkpoint = PopCheckPoint {
            start_offset: 0,
            pop_time: 0,
            invisible_time: 0,
            bit_map: 0,
            num: 0,
            queue_id: 0,
            topic: CheetahString::from(""),
            cid: CheetahString::from(""),
            revive_offset: 0,
            queue_offset_diff: vec![],
            broker_name: None,
            re_put_times: None,
        };
        assert_eq!(checkpoint.parse_re_put_times(), 0);
    }

    #[test]
    fn pop_check_point_ord_works_correctly() {
        let p1 = PopCheckPoint {
            start_offset: 10,
            pop_time: 0,
            invisible_time: 0,
            bit_map: 0,
            num: 0,
            queue_id: 0,
            topic: CheetahString::from(""),
            cid: CheetahString::from(""),
            revive_offset: 0,
            queue_offset_diff: vec![],
            broker_name: None,
            re_put_times: None,
        };
        let p2 = PopCheckPoint {
            start_offset: 20,
            ..p1.clone()
        };
        assert!(p1 < p2);
    }

    #[test]
    fn pop_check_point_partial_ord_works_correctly() {
        let p1 = PopCheckPoint {
            start_offset: 10,
            pop_time: 0,
            invisible_time: 0,
            bit_map: 0,
            num: 0,
            queue_id: 0,
            topic: CheetahString::from(""),
            cid: CheetahString::from(""),
            revive_offset: 0,
            queue_offset_diff: vec![],
            broker_name: None,
            re_put_times: None,
        };
        let p2 = PopCheckPoint {
            start_offset: 20,
            ..p1.clone()
        };
        assert!(p1.partial_cmp(&p2).unwrap() == Ordering::Less);
    }

    #[test]
    fn pop_check_point_equality_works_correctly() {
        let p1 = PopCheckPoint {
            start_offset: 10,
            pop_time: 0,
            invisible_time: 0,
            bit_map: 0,
            num: 0,
            queue_id: 0,
            topic: CheetahString::from(""),
            cid: CheetahString::from(""),
            revive_offset: 0,
            queue_offset_diff: vec![],
            broker_name: None,
            re_put_times: None,
        };
        let p2 = PopCheckPoint {
            start_offset: 10,
            ..p1.clone()
        };
        assert_eq!(p1, p2);
    }

    #[test]
    fn pop_check_point_serialization_works_correctly() {
        let p = PopCheckPoint {
            start_offset: 10,
            pop_time: 20,
            invisible_time: 30,
            bit_map: 40,
            num: 50,
            queue_id: 60,
            topic: CheetahString::from("test_topic"),
            cid: CheetahString::from("test_cid"),
            revive_offset: 70,
            queue_offset_diff: vec![1, 2, 3],
            broker_name: Some(CheetahString::from("test_broker")),
            re_put_times: Some(CheetahString::from("test_reput")),
        };
        let serialized = serde_json::to_string(&p).unwrap();
        let deserialized: PopCheckPoint = serde_json::from_str(&serialized).unwrap();
        assert_eq!(p, deserialized);
    }

    #[test]
    fn pop_check_point_deserialization_handles_missing_optional_fields() {
        let data = r#"{
             "so": 10,
             "pt": 20,
             "it": 30,
             "bm": 40,
             "n": 50,
             "q": 60,
             "t": "test_topic",
             "c": "test_cid",
             "ro": 70,
             "d": [1, 2, 3]
         }"#;
        let deserialized: PopCheckPoint = serde_json::from_str(data).unwrap();
        assert_eq!(deserialized.broker_name, None);
        assert_eq!(deserialized.re_put_times, None);
    }

    #[test]
    fn pop_check_point_display_formats_correctly() {
        let p = PopCheckPoint {
            start_offset: 10,
            pop_time: 20,
            invisible_time: 30,
            bit_map: 40,
            num: 50,
            queue_id: 60,
            topic: CheetahString::from("test_topic"),
            cid: CheetahString::from("test_cid"),
            revive_offset: 70,
            queue_offset_diff: vec![1, 2, 3],
            broker_name: Some(CheetahString::from("test_broker")),
            re_put_times: Some(CheetahString::from("test_reput")),
        };
        let display = format!("{}", p);
        let expected = "PopCheckPoint [start_offset=10, pop_time=20, invisible_time=30, \
                        bit_map=40, num=50, queue_id=60, topic=test_topic, cid=test_cid, \
                        revive_offset=70, queue_offset_diff=[1, 2, 3], broker_name=test_broker, \
                        re_put_times=test_reput]";
        assert_eq!(display, expected);
    }
}
