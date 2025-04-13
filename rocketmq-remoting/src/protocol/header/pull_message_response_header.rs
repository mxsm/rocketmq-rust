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
use rocketmq_error::RocketmqError;
use rocketmq_error::RocketmqError::DeserializeHeaderError;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::command_custom_header::CommandCustomHeader;
use crate::protocol::command_custom_header::FromMap;

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PullMessageResponseHeader {
    pub suggest_which_broker_id: u64,
    pub next_begin_offset: i64,
    pub min_offset: i64,
    pub max_offset: i64,
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

        map.insert(
            CheetahString::from_static_str(Self::SUGGEST_WHICH_BROKER_ID),
            CheetahString::from_string(self.suggest_which_broker_id.to_string()),
        );

        map.insert(
            CheetahString::from_static_str(Self::NEXT_BEGIN_OFFSET),
            CheetahString::from_string(self.next_begin_offset.to_string()),
        );

        map.insert(
            CheetahString::from_static_str(Self::MIN_OFFSET),
            CheetahString::from_string(self.min_offset.to_string()),
        );

        map.insert(
            CheetahString::from_static_str(Self::MAX_OFFSET),
            CheetahString::from_string(self.max_offset.to_string()),
        );

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
        self.write_if_not_null(
            out,
            Self::SUGGEST_WHICH_BROKER_ID,
            self.suggest_which_broker_id.to_string().as_str(),
        );

        self.write_if_not_null(
            out,
            Self::NEXT_BEGIN_OFFSET,
            self.next_begin_offset.to_string().as_str(),
        );

        self.write_if_not_null(out, Self::MIN_OFFSET, self.min_offset.to_string().as_str());

        self.write_if_not_null(out, Self::MAX_OFFSET, self.max_offset.to_string().as_str());

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

    fn decode_fast(
        &mut self,
        fields: &HashMap<CheetahString, CheetahString>,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.suggest_which_broker_id = self
            .get_and_check_not_none(
                fields,
                &CheetahString::from_static_str(Self::SUGGEST_WHICH_BROKER_ID),
            )?
            .parse()
            .map_err(|_| {
                RocketmqError::DeserializeHeaderError(
                    "Parse field suggestWhichBrokerId error".to_string(),
                )
            })?;
        self.next_begin_offset = self
            .get_and_check_not_none(
                fields,
                &CheetahString::from_static_str(Self::NEXT_BEGIN_OFFSET),
            )?
            .parse()
            .map_err(|_| {
                RocketmqError::DeserializeHeaderError(
                    "Parse field nextBeginOffset error".to_string(),
                )
            })?;
        self.min_offset = self
            .get_and_check_not_none(fields, &CheetahString::from_static_str(Self::MIN_OFFSET))?
            .parse()
            .map_err(|_| {
                RocketmqError::DeserializeHeaderError("Parse field minOffset error".to_string())
            })?;
        self.max_offset = self
            .get_and_check_not_none(fields, &CheetahString::from_static_str(Self::MAX_OFFSET))?
            .parse()
            .map_err(|_| {
                RocketmqError::DeserializeHeaderError("Parse field maxOffset error".to_string())
            })?;

        self.offset_delta = fields
            .get(&CheetahString::from_static_str(Self::OFFSET_DELTA))
            .and_then(|v| v.parse().ok());

        self.topic_sys_flag = fields
            .get(&CheetahString::from_static_str(Self::TOPIC_SYS_FLAG))
            .and_then(|v| v.parse().ok());

        self.group_sys_flag = fields
            .get(&CheetahString::from_static_str(Self::GROUP_SYS_FLAG))
            .and_then(|v| v.parse().ok());

        self.forbidden_type = fields
            .get(&CheetahString::from_static_str(Self::FORBIDDEN_TYPE))
            .and_then(|v| v.parse().ok());

        Ok(())
    }

    fn support_fast_codec(&self) -> bool {
        true
    }
}

impl FromMap for PullMessageResponseHeader {
    type Error = rocketmq_error::RocketmqError;

    type Target = Self;

    fn from(map: &HashMap<CheetahString, CheetahString>) -> Result<Self::Target, Self::Error> {
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

        Ok(PullMessageResponseHeader {
            suggest_which_broker_id: suggest_which_broker_id.and_then(|v| v.parse().ok()).ok_or(
                DeserializeHeaderError("Parse field suggestWhichBrokerId error".to_string()),
            )?,
            next_begin_offset: next_begin_offset.and_then(|v| v.parse().ok()).ok_or(
                DeserializeHeaderError("Parse field nextBeginOffset error".to_string()),
            )?,
            min_offset: min_offset
                .and_then(|v| v.parse().ok())
                .ok_or(DeserializeHeaderError(
                    "Parse field minOffset error".to_string(),
                ))?,
            max_offset: max_offset
                .and_then(|v| v.parse().ok())
                .ok_or(DeserializeHeaderError(
                    "Parse field maxOffset error".to_string(),
                ))?,
            offset_delta: offset_delta.and_then(|v| v.parse().ok()),
            topic_sys_flag: topic_sys_flag.and_then(|v| v.parse().ok()),
            group_sys_flag: group_sys_flag.and_then(|v| v.parse().ok()),
            forbidden_type: forbidden_type.and_then(|v| v.parse().ok()),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn pull_message_response_header_serializes_correctly() {
        let header = PullMessageResponseHeader {
            suggest_which_broker_id: 123,
            next_begin_offset: 456,
            min_offset: 789,
            max_offset: 101112,
            offset_delta: Some(131415),
            topic_sys_flag: Some(161718),
            group_sys_flag: Some(192021),
            forbidden_type: Some(222324),
        };
        let map = header.to_map().unwrap();
        assert_eq!(
            map.get(&CheetahString::from_static_str("suggestWhichBrokerId"))
                .unwrap(),
            "123"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("nextBeginOffset"))
                .unwrap(),
            "456"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("minOffset"))
                .unwrap(),
            "789"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("maxOffset"))
                .unwrap(),
            "101112"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("offsetDelta"))
                .unwrap(),
            "131415"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("topicSysFlag"))
                .unwrap(),
            "161718"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("groupSysFlag"))
                .unwrap(),
            "192021"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("forbiddenType"))
                .unwrap(),
            "222324"
        );
    }

    #[test]
    fn pull_message_response_header_deserializes_correctly() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str("suggestWhichBrokerId"),
            CheetahString::from_static_str("123"),
        );
        map.insert(
            CheetahString::from_static_str("nextBeginOffset"),
            CheetahString::from_static_str("456"),
        );
        map.insert(
            CheetahString::from_static_str("minOffset"),
            CheetahString::from_static_str("789"),
        );
        map.insert(
            CheetahString::from_static_str("maxOffset"),
            CheetahString::from_static_str("101112"),
        );
        map.insert(
            CheetahString::from_static_str("offsetDelta"),
            CheetahString::from_static_str("131415"),
        );
        map.insert(
            CheetahString::from_static_str("topicSysFlag"),
            CheetahString::from_static_str("161718"),
        );
        map.insert(
            CheetahString::from_static_str("groupSysFlag"),
            CheetahString::from_static_str("192021"),
        );
        map.insert(
            CheetahString::from_static_str("forbiddenType"),
            CheetahString::from_static_str("222324"),
        );

        let header = <PullMessageResponseHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.suggest_which_broker_id, 123);
        assert_eq!(header.next_begin_offset, 456);
        assert_eq!(header.min_offset, 789);
        assert_eq!(header.max_offset, 101112);
        assert_eq!(header.offset_delta.unwrap(), 131415);
        assert_eq!(header.topic_sys_flag.unwrap(), 161718);
        assert_eq!(header.group_sys_flag.unwrap(), 192021);
        assert_eq!(header.forbidden_type.unwrap(), 222324);
    }

    #[test]
    fn pull_message_response_header_handles_missing_optional_fields() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str("suggestWhichBrokerId"),
            CheetahString::from_static_str("123"),
        );
        map.insert(
            CheetahString::from_static_str("nextBeginOffset"),
            CheetahString::from_static_str("456"),
        );
        map.insert(
            CheetahString::from_static_str("minOffset"),
            CheetahString::from_static_str("789"),
        );
        map.insert(
            CheetahString::from_static_str("maxOffset"),
            CheetahString::from_static_str("101112"),
        );

        let header = <PullMessageResponseHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.suggest_which_broker_id, 123);
        assert_eq!(header.next_begin_offset, 456);
        assert_eq!(header.min_offset, 789);
        assert_eq!(header.max_offset, 101112);
        assert!(header.offset_delta.is_none());
        assert!(header.topic_sys_flag.is_none());
        assert!(header.group_sys_flag.is_none());
        assert!(header.forbidden_type.is_none());
    }

    #[test]
    fn pull_message_response_header_handles_invalid_data() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str("suggestWhichBrokerId"),
            CheetahString::from_static_str("invalid"),
        );
        map.insert(
            CheetahString::from_static_str("nextBeginOffset"),
            CheetahString::from_static_str("invalid"),
        );
        map.insert(
            CheetahString::from_static_str("minOffset"),
            CheetahString::from_static_str("invalid"),
        );
        map.insert(
            CheetahString::from_static_str("maxOffset"),
            CheetahString::from_static_str("invalid"),
        );

        let result = <PullMessageResponseHeader as FromMap>::from(&map);
        assert!(result.is_err());
    }
}
