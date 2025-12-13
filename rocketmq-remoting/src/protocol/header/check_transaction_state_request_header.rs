//  Licensed to the Apache Software Foundation (ASF) under one
//  or more contributor license agreements.  See the NOTICE file
//  distributed with this work for additional information
//  regarding copyright ownership.  The ASF licenses this file
//  to you under the Apache License, Version 2.0 (the
//  "License"); you may not use this file except in compliance
//  with the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an
//  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  KIND, either express or implied.  See the License for the
//  specific language governing permissions and limitations
//  under the License.

use cheetah_string::CheetahString;
use rocketmq_macros::RequestHeaderCodecV2;
use serde::Deserialize;
use serde::Serialize;

use crate::rpc::rpc_request_header::RpcRequestHeader;

#[derive(Serialize, Deserialize, Debug, Default, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct CheckTransactionStateRequestHeader {
    pub topic: Option<CheetahString>,
    #[required]
    pub tran_state_table_offset: i64,
    #[required]
    pub commit_log_offset: i64,
    pub msg_id: Option<CheetahString>,
    pub transaction_id: Option<CheetahString>,
    pub offset_msg_id: Option<CheetahString>,
    #[serde(flatten)]
    pub rpc_request_header: Option<RpcRequestHeader>,
}

/*impl CheckTransactionStateRequestHeader {
    pub const TOPIC: &'static str = "topic";
    pub const TRAN_STATE_TABLE_OFFSET: &'static str = "tranStateTableOffset";
    pub const COMMIT_LOG_OFFSET: &'static str = "commitLogOffset";
    pub const MSG_ID: &'static str = "msgId";
    pub const TRANSACTION_ID: &'static str = "transactionId";
    pub const OFFSET_MSG_ID: &'static str = "offsetMsgId";
}

impl CommandCustomHeader for CheckTransactionStateRequestHeader {
    fn to_map(&self) -> Option<std::collections::HashMap<CheetahString, CheetahString>> {
        let mut map = std::collections::HashMap::new();
        if let Some(value) = self.topic.as_ref() {
            map.insert(CheetahString::from_static_str(Self::TOPIC), value.clone());
        }
        map.insert(
            CheetahString::from_static_str(Self::TRAN_STATE_TABLE_OFFSET),
            CheetahString::from_string(self.tran_state_table_offset.to_string()),
        );

        map.insert(
            CheetahString::from_static_str(Self::COMMIT_LOG_OFFSET),
            CheetahString::from_string(self.commit_log_offset.to_string()),
        );
        if let Some(value) = self.msg_id.as_ref() {
            map.insert(CheetahString::from_static_str(Self::MSG_ID), value.clone());
        }
        if let Some(value) = self.transaction_id.as_ref() {
            map.insert(
                CheetahString::from_static_str(Self::TRANSACTION_ID),
                value.clone(),
            );
        }
        if let Some(value) = self.offset_msg_id.as_ref() {
            map.insert(
                CheetahString::from_static_str(Self::OFFSET_MSG_ID),
                value.clone(),
            );
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
    type Error = rocketmq_error::RocketMQError;

    type Target = Self;

    fn from(
        map: &std::collections::HashMap<CheetahString, CheetahString>,
    ) -> Result<Self::Target, Self::Error> {
        Ok(CheckTransactionStateRequestHeader {
            topic: map
                .get(&CheetahString::from_static_str(Self::TOPIC))
                .cloned(),
            tran_state_table_offset: map
                .get(&CheetahString::from_static_str(
                    Self::TRAN_STATE_TABLE_OFFSET,
                ))
                .and_then(|v| v.parse().ok())
                .unwrap_or_default(),
            commit_log_offset: map
                .get(&CheetahString::from_static_str(Self::COMMIT_LOG_OFFSET))
                .and_then(|v| v.parse().ok())
                .unwrap_or_default(),
            msg_id: map
                .get(&CheetahString::from_static_str(Self::MSG_ID))
                .cloned(),
            transaction_id: map
                .get(&CheetahString::from_static_str(Self::TRANSACTION_ID))
                .cloned(),
            offset_msg_id: map
                .get(&CheetahString::from_static_str(Self::OFFSET_MSG_ID))
                .cloned(),
            rpc_request_header: Some(<RpcRequestHeader as FromMap>::from(map)?),
        })
    }
}
*/

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use cheetah_string::CheetahString;

    use super::*;
    use crate::protocol::command_custom_header::CommandCustomHeader;
    use crate::protocol::command_custom_header::FromMap;

    #[test]
    fn check_transaction_state_request_header_serializes_correctly() {
        let header = CheckTransactionStateRequestHeader {
            topic: Some(CheetahString::from_static_str("test_topic")),
            tran_state_table_offset: 123,
            commit_log_offset: 456,
            msg_id: Some(CheetahString::from_static_str("test_msg_id")),
            transaction_id: Some(CheetahString::from_static_str("test_transaction_id")),
            offset_msg_id: Some(CheetahString::from_static_str("test_offset_msg_id")),
            rpc_request_header: None,
        };
        let map = header.to_map().unwrap();
        assert_eq!(
            map.get(&CheetahString::from_static_str("topic")).unwrap(),
            "test_topic"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("tranStateTableOffset"))
                .unwrap(),
            "123"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("commitLogOffset"))
                .unwrap(),
            "456"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("msgId")).unwrap(),
            "test_msg_id"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("transactionId"))
                .unwrap(),
            "test_transaction_id"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("offsetMsgId"))
                .unwrap(),
            "test_offset_msg_id"
        );
    }

    #[test]
    fn check_transaction_state_request_header_deserializes_correctly() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str("topic"),
            CheetahString::from_static_str("test_topic"),
        );
        map.insert(
            CheetahString::from_static_str("tranStateTableOffset"),
            CheetahString::from_static_str("123"),
        );
        map.insert(
            CheetahString::from_static_str("commitLogOffset"),
            CheetahString::from_static_str("456"),
        );
        map.insert(
            CheetahString::from_static_str("msgId"),
            CheetahString::from_static_str("test_msg_id"),
        );
        map.insert(
            CheetahString::from_static_str("transactionId"),
            CheetahString::from_static_str("test_transaction_id"),
        );
        map.insert(
            CheetahString::from_static_str("offsetMsgId"),
            CheetahString::from_static_str("test_offset_msg_id"),
        );

        let header = <CheckTransactionStateRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.topic.unwrap(), "test_topic");
        assert_eq!(header.tran_state_table_offset, 123);
        assert_eq!(header.commit_log_offset, 456);
        assert_eq!(header.msg_id.unwrap(), "test_msg_id");
        assert_eq!(header.transaction_id.unwrap(), "test_transaction_id");
        assert_eq!(header.offset_msg_id.unwrap(), "test_offset_msg_id");
    }

    #[test]
    fn check_transaction_state_request_header_handles_missing_optional_fields() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str("tranStateTableOffset"),
            CheetahString::from_static_str("123"),
        );
        map.insert(
            CheetahString::from_static_str("commitLogOffset"),
            CheetahString::from_static_str("456"),
        );

        let header = <CheckTransactionStateRequestHeader as FromMap>::from(&map).unwrap();
        assert!(header.topic.is_none());
        assert_eq!(header.tran_state_table_offset, 123);
        assert_eq!(header.commit_log_offset, 456);
        assert!(header.msg_id.is_none());
        assert!(header.transaction_id.is_none());
        assert!(header.offset_msg_id.is_none());
    }

    #[test]
    fn check_transaction_state_request_header_handles_invalid_data() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str("tranStateTableOffset"),
            CheetahString::from_static_str("invalid"),
        );
        map.insert(
            CheetahString::from_static_str("commitLogOffset"),
            CheetahString::from_static_str("invalid"),
        );

        let result = <CheckTransactionStateRequestHeader as FromMap>::from(&map);
        assert!(result.is_err());
    }
}
