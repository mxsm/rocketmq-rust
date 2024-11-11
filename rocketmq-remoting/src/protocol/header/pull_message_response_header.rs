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

use bytes::BytesMut;
use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::command_custom_header::CommandCustomHeader;
use crate::protocol::command_custom_header::FromMap;

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PullMessageResponseHeader {
    pub suggest_which_broker_id: Option<u64>,
    pub next_begin_offset: Option<i64>,
    pub min_offset: Option<i64>,
    pub max_offset: Option<i64>,
    pub offset_delta: Option<i64>,
    pub topic_sys_flag: Option<i32>,
    pub group_sys_flag: Option<i32>,
    pub forbidden_type: Option<i32>,
}

impl PullMessageResponseHeader {
    pub const SUGGEST_WHICH_BROKER_ID: &'static str = "suggestWhichBrokerId";
    pub const NEXT_BEGIN_OFFSET: &'static str = "nextBeginOffset";
    pub const MIN_OFFSET: &'static str = "minOffset";
    pub const MAX_OFFSET: &'static str = "maxOffset";
    pub const OFFSET_DELTA: &'static str = "offsetDelta";
    pub const TOPIC_SYS_FLAG: &'static str = "topicSysFlag";
    pub const GROUP_SYS_FLAG: &'static str = "groupSysFlag";
    pub const FORBIDDEN_TYPE: &'static str = "forbiddenType";
}

impl CommandCustomHeader for PullMessageResponseHeader {
    fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
        let mut map = HashMap::new();
        if let Some(value) = self.suggest_which_broker_id {
            map.insert(
                CheetahString::from_static_str(Self::SUGGEST_WHICH_BROKER_ID),
                CheetahString::from_string(value.to_string()),
            );
        }
        if let Some(value) = self.next_begin_offset {
            map.insert(
                CheetahString::from_static_str(Self::NEXT_BEGIN_OFFSET),
                CheetahString::from_string(value.to_string()),
            );
        }
        if let Some(value) = self.min_offset {
            map.insert(
                CheetahString::from_static_str(Self::MIN_OFFSET),
                CheetahString::from_string(value.to_string()),
            );
        }
        if let Some(value) = self.max_offset {
            map.insert(
                CheetahString::from_static_str(Self::MAX_OFFSET),
                CheetahString::from_string(value.to_string()),
            );
        }
        if let Some(value) = self.offset_delta {
            map.insert(
                CheetahString::from_static_str(Self::OFFSET_DELTA),
                CheetahString::from_string(value.to_string()),
            );
        }
        if let Some(value) = self.topic_sys_flag {
            map.insert(
                CheetahString::from_static_str(Self::TOPIC_SYS_FLAG),
                CheetahString::from_string(value.to_string()),
            );
        }
        if let Some(value) = self.group_sys_flag {
            map.insert(
                CheetahString::from_static_str(Self::GROUP_SYS_FLAG),
                CheetahString::from_string(value.to_string()),
            );
        }
        if let Some(value) = self.forbidden_type {
            map.insert(
                CheetahString::from_static_str(Self::FORBIDDEN_TYPE),
                CheetahString::from_string(value.to_string()),
            );
        }
        Some(map)
    }

    fn encode_fast(&mut self, out: &mut BytesMut) {
        if let Some(value) = self.suggest_which_broker_id {
            self.write_if_not_null(
                out,
                Self::SUGGEST_WHICH_BROKER_ID,
                value.to_string().as_str(),
            );
        }
        if let Some(value) = self.next_begin_offset {
            self.write_if_not_null(out, Self::NEXT_BEGIN_OFFSET, value.to_string().as_str());
        }
        if let Some(value) = self.min_offset {
            self.write_if_not_null(out, Self::MIN_OFFSET, value.to_string().as_str());
        }
        if let Some(value) = self.max_offset {
            self.write_if_not_null(out, Self::MAX_OFFSET, value.to_string().as_str());
        }
        if let Some(value) = self.offset_delta {
            self.write_if_not_null(out, Self::OFFSET_DELTA, value.to_string().as_str());
        }
        if let Some(value) = self.topic_sys_flag {
            self.write_if_not_null(out, Self::TOPIC_SYS_FLAG, value.to_string().as_str());
        }
        if let Some(value) = self.group_sys_flag {
            self.write_if_not_null(out, Self::GROUP_SYS_FLAG, value.to_string().as_str());
        }
        if let Some(value) = self.forbidden_type {
            self.write_if_not_null(out, Self::FORBIDDEN_TYPE, value.to_string().as_str());
        }
    }

    fn decode_fast(&mut self, fields: &HashMap<String, String>) {
        if let Some(offset_delta) = fields.get(Self::SUGGEST_WHICH_BROKER_ID) {
            self.suggest_which_broker_id = Some(offset_delta.parse().unwrap());
        }
        if let Some(offset_delta) = fields.get(Self::NEXT_BEGIN_OFFSET) {
            self.next_begin_offset = Some(offset_delta.parse().unwrap());
        }
        if let Some(offset_delta) = fields.get(Self::MIN_OFFSET) {
            self.min_offset = Some(offset_delta.parse().unwrap());
        }
        if let Some(offset_delta) = fields.get(Self::MAX_OFFSET) {
            self.max_offset = Some(offset_delta.parse().unwrap());
        }
        if let Some(offset_delta) = fields.get(Self::OFFSET_DELTA) {
            self.offset_delta = Some(offset_delta.parse().unwrap());
        }
        if let Some(offset_delta) = fields.get(Self::TOPIC_SYS_FLAG) {
            self.topic_sys_flag = Some(offset_delta.parse().unwrap());
        }
        if let Some(offset_delta) = fields.get(Self::GROUP_SYS_FLAG) {
            self.group_sys_flag = Some(offset_delta.parse().unwrap());
        }
        if let Some(offset_delta) = fields.get(Self::FORBIDDEN_TYPE) {
            self.forbidden_type = Some(offset_delta.parse().unwrap());
        }
    }

    fn support_fast_codec(&self) -> bool {
        true
    }
}

impl FromMap for PullMessageResponseHeader {
    type Target = Self;

    fn from(map: &HashMap<CheetahString, CheetahString>) -> Option<Self::Target> {
        let suggest_which_broker_id = map.get(&CheetahString::from_static_str(
            PullMessageResponseHeader::SUGGEST_WHICH_BROKER_ID,
        ));
        let next_begin_offset = map.get(&CheetahString::from_static_str(
            PullMessageResponseHeader::NEXT_BEGIN_OFFSET,
        ));
        let min_offset = map.get(&CheetahString::from_static_str(
            PullMessageResponseHeader::MIN_OFFSET,
        ));
        let max_offset = map.get(&CheetahString::from_static_str(
            PullMessageResponseHeader::MAX_OFFSET,
        ));
        let offset_delta = map.get(&CheetahString::from_static_str(
            PullMessageResponseHeader::OFFSET_DELTA,
        ));
        let topic_sys_flag = map.get(&CheetahString::from_static_str(
            PullMessageResponseHeader::TOPIC_SYS_FLAG,
        ));
        let group_sys_flag = map.get(&CheetahString::from_static_str(
            PullMessageResponseHeader::GROUP_SYS_FLAG,
        ));
        let forbidden_type = map.get(&CheetahString::from_static_str(
            PullMessageResponseHeader::FORBIDDEN_TYPE,
        ));

        Some(PullMessageResponseHeader {
            suggest_which_broker_id: suggest_which_broker_id.map(|v| v.parse().unwrap()),
            next_begin_offset: next_begin_offset.map(|v| v.parse().unwrap()),
            min_offset: min_offset.map(|v| v.parse().unwrap()),
            max_offset: max_offset.map(|v| v.parse().unwrap()),
            offset_delta: offset_delta.map(|v| v.parse().unwrap()),
            topic_sys_flag: topic_sys_flag.map(|v| v.parse().unwrap()),
            group_sys_flag: group_sys_flag.map(|v| v.parse().unwrap()),
            forbidden_type: forbidden_type.map(|v| v.parse().unwrap()),
        })
    }
}

#[cfg(test)]
mod pull_message_response_header_tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn from_map_creates_correct_instance() {
        let mut map = HashMap::new();
        map.insert("suggestWhichBrokerId".to_string(), "1".to_string());
        map.insert("nextBeginOffset".to_string(), "100".to_string());
        map.insert("minOffset".to_string(), "50".to_string());
        map.insert("maxOffset".to_string(), "150".to_string());
        map.insert("offsetDelta".to_string(), "5".to_string());
        map.insert("topicSysFlag".to_string(), "2".to_string());
        map.insert("groupSysFlag".to_string(), "3".to_string());
        map.insert("forbiddenType".to_string(), "4".to_string());

        let header = <PullMessageResponseHeader as FromMap>::from(&map).unwrap();

        assert_eq!(header.suggest_which_broker_id, Some(1));
        assert_eq!(header.next_begin_offset, Some(100));
        assert_eq!(header.min_offset, Some(50));
        assert_eq!(header.max_offset, Some(150));
        assert_eq!(header.offset_delta, Some(5));
        assert_eq!(header.topic_sys_flag, Some(2));
        assert_eq!(header.group_sys_flag, Some(3));
        assert_eq!(header.forbidden_type, Some(4));
    }

    #[test]
    fn to_map_returns_correct_map() {
        let header = PullMessageResponseHeader {
            suggest_which_broker_id: Some(1),
            next_begin_offset: Some(100),
            min_offset: Some(50),
            max_offset: Some(150),
            offset_delta: Some(5),
            topic_sys_flag: Some(2),
            group_sys_flag: Some(3),
            forbidden_type: Some(4),
        };

        let map = header.to_map().unwrap();

        assert_eq!(map.get("suggestWhichBrokerId").unwrap(), "1");
        assert_eq!(map.get("nextBeginOffset").unwrap(), "100");
        assert_eq!(map.get("minOffset").unwrap(), "50");
        assert_eq!(map.get("maxOffset").unwrap(), "150");
        assert_eq!(map.get("offsetDelta").unwrap(), "5");
        assert_eq!(map.get("topicSysFlag").unwrap(), "2");
        assert_eq!(map.get("groupSysFlag").unwrap(), "3");
        assert_eq!(map.get("forbiddenType").unwrap(), "4");
    }

    #[test]
    fn encode_fast_writes_all_fields_correctly() {
        let mut header = PullMessageResponseHeader {
            suggest_which_broker_id: Some(1),
            next_begin_offset: Some(100),
            min_offset: Some(50),
            max_offset: Some(150),
            offset_delta: Some(5),
            topic_sys_flag: Some(2),
            group_sys_flag: Some(3),
            forbidden_type: Some(4),
        };
        let mut out = BytesMut::new();

        header.encode_fast(&mut out);
        let result = String::from_utf8(out.to_vec()).unwrap();
        assert!(result.contains("suggestWhichBrokerId"));
        assert!(result.contains("nextBeginOffset"));
        assert!(result.contains("minOffset"));
        assert!(result.contains("maxOffset"));
        assert!(result.contains("offsetDelta"));
        assert!(result.contains("topicSysFlag"));
        assert!(result.contains("groupSysFlag"));
        assert!(result.contains("forbiddenType"));
    }

    #[test]
    fn decode_fast_populates_all_fields_correctly() {
        let mut header = PullMessageResponseHeader::default();
        let mut fields = HashMap::new();
        fields.insert("suggestWhichBrokerId".to_string(), "1".to_string());
        fields.insert("nextBeginOffset".to_string(), "100".to_string());
        fields.insert("minOffset".to_string(), "50".to_string());
        fields.insert("maxOffset".to_string(), "150".to_string());
        fields.insert("offsetDelta".to_string(), "5".to_string());
        fields.insert("topicSysFlag".to_string(), "2".to_string());
        fields.insert("groupSysFlag".to_string(), "3".to_string());
        fields.insert("forbiddenType".to_string(), "4".to_string());

        header.decode_fast(&fields);

        assert_eq!(header.suggest_which_broker_id, Some(1));
        assert_eq!(header.next_begin_offset, Some(100));
        assert_eq!(header.min_offset, Some(50));
        assert_eq!(header.max_offset, Some(150));
        assert_eq!(header.offset_delta, Some(5));
        assert_eq!(header.topic_sys_flag, Some(2));
        assert_eq!(header.group_sys_flag, Some(3));
        assert_eq!(header.forbidden_type, Some(4));
    }
}
