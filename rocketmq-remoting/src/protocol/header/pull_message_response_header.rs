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
use rocketmq_macros::RemotingSerializable;
use rocketmq_macros::RequestHeaderCodec;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::FastCodesHeader;

#[derive(Serialize, Deserialize, Debug, Default, RemotingSerializable, RequestHeaderCodec)]
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

/*impl PullMessageResponseHeader {
    const FORBIDDEN_TYPE: &'static str = "forbiddenType";
    const GROUP_SYS_FLAG: &'static str = "groupSysFlag";
    const MAX_OFFSET: &'static str = "maxOffset";
    const MIN_OFFSET: &'static str = "minOffset";
    const NEXT_BEGIN_OFFSET: &'static str = "nextBeginOffset";
    const OFFSET_DELTA: &'static str = "offsetDelta";
    const SUGGEST_WHICH_BROKER_ID: &'static str = "suggestWhichBrokerId";
    const TOPIC_SYS_FLAG: &'static str = "topicSysFlag";
}*/

impl FastCodesHeader for PullMessageResponseHeader {
    fn encode_fast(&mut self, out: &mut BytesMut) {
        if let Some(value) = self.suggest_which_broker_id {
            Self::write_if_not_null(
                out,
                Self::SUGGEST_WHICH_BROKER_ID,
                value.to_string().as_str(),
            );
        }
        if let Some(value) = self.next_begin_offset {
            Self::write_if_not_null(out, Self::NEXT_BEGIN_OFFSET, value.to_string().as_str());
        }
        if let Some(value) = self.min_offset {
            Self::write_if_not_null(out, Self::MIN_OFFSET, value.to_string().as_str());
        }
        if let Some(value) = self.max_offset {
            Self::write_if_not_null(out, Self::MAX_OFFSET, value.to_string().as_str());
        }
        if let Some(value) = self.offset_delta {
            Self::write_if_not_null(out, Self::OFFSET_DELTA, value.to_string().as_str());
        }
        if let Some(value) = self.topic_sys_flag {
            Self::write_if_not_null(out, Self::TOPIC_SYS_FLAG, value.to_string().as_str());
        }
        if let Some(value) = self.group_sys_flag {
            Self::write_if_not_null(out, Self::GROUP_SYS_FLAG, value.to_string().as_str());
        }
        if let Some(value) = self.forbidden_type {
            Self::write_if_not_null(out, Self::FORBIDDEN_TYPE, value.to_string().as_str());
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
}
