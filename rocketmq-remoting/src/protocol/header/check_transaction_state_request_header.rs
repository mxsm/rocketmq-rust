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
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::command_custom_header::CommandCustomHeader;
use crate::protocol::command_custom_header::FromMap;
use crate::rpc::rpc_request_header::RpcRequestHeader;

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct CheckTransactionStateRequestHeader {
    pub topic: Option<String>,
    pub tran_state_table_offset: i64,
    pub commit_log_offset: i64,
    pub msg_id: Option<String>,
    pub transaction_id: Option<String>,
    pub offset_msg_id: Option<String>,
    #[serde(flatten)]
    pub rpc_request_header: Option<RpcRequestHeader>,
}

impl CheckTransactionStateRequestHeader {
    pub const TOPIC: &'static str = "topic";
    pub const TRAN_STATE_TABLE_OFFSET: &'static str = "tranStateTableOffset";
    pub const COMMIT_LOG_OFFSET: &'static str = "commitLogOffset";
    pub const MSG_ID: &'static str = "msgId";
    pub const TRANSACTION_ID: &'static str = "transactionId";
    pub const OFFSET_MSG_ID: &'static str = "offsetMsgId";
}

impl CommandCustomHeader for CheckTransactionStateRequestHeader {
    fn to_map(&self) -> Option<std::collections::HashMap<String, String>> {
        let mut map = std::collections::HashMap::new();
        if let Some(value) = self.topic.as_ref() {
            map.insert(Self::TOPIC.to_string(), value.clone());
        }
        map.insert(
            Self::TRAN_STATE_TABLE_OFFSET.to_string(),
            self.tran_state_table_offset.to_string(),
        );
        map.insert(
            Self::COMMIT_LOG_OFFSET.to_string(),
            self.commit_log_offset.to_string(),
        );
        if let Some(value) = self.msg_id.as_ref() {
            map.insert(Self::MSG_ID.to_string(), value.clone());
        }
        if let Some(value) = self.transaction_id.as_ref() {
            map.insert(Self::TRANSACTION_ID.to_string(), value.clone());
        }
        if let Some(value) = self.offset_msg_id.as_ref() {
            map.insert(Self::OFFSET_MSG_ID.to_string(), value.clone());
        }
        if let Some(value) = self.rpc_request_header.as_ref() {
            if let Some(value) = value.to_map() {
                map.extend(value);
            }
        }
        Some(map)
    }
}

impl FromMap for CheckTransactionStateRequestHeader {
    type Target = Self;

    fn from(map: &std::collections::HashMap<String, String>) -> Option<Self::Target> {
        Some(CheckTransactionStateRequestHeader {
            topic: map.get(Self::TOPIC).cloned(),
            tran_state_table_offset: map
                .get(Self::TRAN_STATE_TABLE_OFFSET)
                .and_then(|v| v.parse().ok())
                .unwrap_or_default(),
            commit_log_offset: map
                .get(Self::COMMIT_LOG_OFFSET)
                .and_then(|v| v.parse().ok())
                .unwrap_or_default(),
            msg_id: map.get(Self::MSG_ID).cloned(),
            transaction_id: map.get(Self::TRANSACTION_ID).cloned(),
            offset_msg_id: map.get(Self::OFFSET_MSG_ID).cloned(),
            rpc_request_header: <RpcRequestHeader as FromMap>::from(map),
        })
    }
}
