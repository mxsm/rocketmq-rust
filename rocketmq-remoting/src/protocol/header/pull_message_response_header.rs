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
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::FastCodesHeader;

#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub struct PullMessageResponseHeader {
    suggest_which_broker_id: Option<i64>,
    next_begin_offset: Option<i64>,
    min_offset: Option<i64>,
    max_offset: Option<i64>,
    offset_delta: Option<i64>,
    topic_sys_flag: Option<i32>,
    group_sys_flag: Option<i32>,
    forbidden_type: Option<i32>,
}

impl PullMessageResponseHeader {
    const FORBIDDEN_TYPE: &'static str = "forbiddenType";
    const GROUP_SYS_FLAG: &'static str = "groupSysFlag";
    const MAX_OFFSET: &'static str = "maxOffset";
    const MIN_OFFSET: &'static str = "minOffset";
    const NEXT_BEGIN_OFFSET: &'static str = "nextBeginOffset";
    const OFFSET_DELTA: &'static str = "offsetDelta";
    const SUGGEST_WHICH_BROKER_ID: &'static str = "suggestWhichBrokerId";
    const TOPIC_SYS_FLAG: &'static str = "topicSysFlag";
}

impl FastCodesHeader for PullMessageResponseHeader {
    fn decode_fast(&mut self, fields: &HashMap<String, String>) {
        if let Some(offset_delta) = fields.get(Self::SUGGEST_WHICH_BROKER_ID) {
            self.suggest_which_broker_id = offset_delta.parse().ok();
        }
        if let Some(offset_delta) = fields.get(Self::NEXT_BEGIN_OFFSET) {
            self.next_begin_offset = offset_delta.parse().ok();
        }
        if let Some(offset_delta) = fields.get(Self::MIN_OFFSET) {
            self.min_offset = offset_delta.parse().ok();
        }
        if let Some(offset_delta) = fields.get(Self::MAX_OFFSET) {
            self.max_offset = offset_delta.parse().ok();
        }
        if let Some(offset_delta) = fields.get(Self::OFFSET_DELTA) {
            self.offset_delta = offset_delta.parse().ok();
        }
        if let Some(offset_delta) = fields.get(Self::TOPIC_SYS_FLAG) {
            self.topic_sys_flag = offset_delta.parse().ok();
        }
        if let Some(offset_delta) = fields.get(Self::GROUP_SYS_FLAG) {
            self.group_sys_flag = offset_delta.parse().ok();
        }
        if let Some(offset_delta) = fields.get(Self::FORBIDDEN_TYPE) {
            self.forbidden_type = offset_delta.parse().ok();
        }
    }
}
