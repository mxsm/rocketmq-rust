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
use anyhow::anyhow;
use anyhow::Error;
use cheetah_string::CheetahString;
use rocketmq_common::common::sys_flag::message_sys_flag::MessageSysFlag;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::command_custom_header::CommandCustomHeader;
use crate::protocol::command_custom_header::FromMap;
use crate::rpc::rpc_request_header::RpcRequestHeader;

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct EndTransactionRequestHeader {
    pub topic: CheetahString,

    pub producer_group: CheetahString,

    //ConsumeQueue Offset
    pub tran_state_table_offset: u64,

    // Offset of the message in the CommitLog
    pub commit_log_offset: u64,
    //TRANSACTION_COMMIT_TYPE,TRANSACTION_ROLLBACK_TYPE,TRANSACTION_NOT_TYPE
    pub commit_or_rollback: i32,

    //Whether the check-back is initiated by the Broker
    pub from_transaction_check: bool,

    pub msg_id: CheetahString,
    pub transaction_id: Option<CheetahString>,
    #[serde(flatten)]
    pub rpc_request_header: RpcRequestHeader,
}

impl EndTransactionRequestHeader {
    pub const TOPIC: &'static str = "topic";
    pub const PRODUCER_GROUP: &'static str = "producerGroup";
    pub const TRAN_STATE_TABLE_OFFSET: &'static str = "tranStateTableOffset";
    pub const COMMIT_LOG_OFFSET: &'static str = "commitLogOffset";
    pub const COMMIT_OR_ROLLBACK: &'static str = "commitOrRollback";
    pub const FROM_TRANSACTION_CHECK: &'static str = "fromTransactionCheck";
    pub const MSG_ID: &'static str = "msgId";
    pub const TRANSACTION_ID: &'static str = "transactionId";
}

impl CommandCustomHeader for EndTransactionRequestHeader {
    fn to_map(&self) -> Option<std::collections::HashMap<CheetahString, CheetahString>> {
        let mut map = std::collections::HashMap::new();
        map.insert(
            CheetahString::from_static_str(Self::TOPIC),
            self.topic.clone(),
        );
        map.insert(
            CheetahString::from_static_str(Self::PRODUCER_GROUP),
            self.producer_group.clone(),
        );
        map.insert(
            CheetahString::from_static_str(Self::TRAN_STATE_TABLE_OFFSET),
            CheetahString::from_string(self.tran_state_table_offset.to_string()),
        );

        map.insert(
            CheetahString::from_static_str(Self::COMMIT_LOG_OFFSET),
            CheetahString::from_string(self.commit_log_offset.to_string()),
        );
        map.insert(
            CheetahString::from_static_str(Self::COMMIT_OR_ROLLBACK),
            CheetahString::from_string(self.commit_or_rollback.to_string()),
        );
        map.insert(
            CheetahString::from_static_str(Self::FROM_TRANSACTION_CHECK),
            CheetahString::from_string(self.from_transaction_check.to_string()),
        );

        map.insert(
            CheetahString::from_static_str(Self::MSG_ID),
            self.msg_id.clone(),
        );
        if let Some(value) = self.transaction_id.as_ref() {
            map.insert(
                CheetahString::from_static_str(Self::TRANSACTION_ID),
                value.clone(),
            );
        }
        if let Some(value) = self.rpc_request_header.to_map() {
            map.extend(value);
        }
        Some(map)
    }

    fn check_fields(&self) -> anyhow::Result<(), Error> {
        if MessageSysFlag::TRANSACTION_NOT_TYPE == self.commit_or_rollback {
            return Ok(());
        }
        if MessageSysFlag::TRANSACTION_COMMIT_TYPE == self.commit_or_rollback {
            return Ok(());
        }
        if MessageSysFlag::TRANSACTION_ROLLBACK_TYPE == self.commit_or_rollback {
            return Ok(());
        }
        Err(anyhow!("commitOrRollback field wrong"))
    }
}

impl FromMap for EndTransactionRequestHeader {
    type Error = rocketmq_error::RocketmqError;

    type Target = Self;

    fn from(
        map: &std::collections::HashMap<CheetahString, CheetahString>,
    ) -> Result<Self::Target, Self::Error> {
        Ok(EndTransactionRequestHeader {
            topic: map
                .get(&CheetahString::from_static_str(
                    EndTransactionRequestHeader::TOPIC,
                ))
                .cloned()
                .unwrap_or_default(),
            producer_group: map
                .get(&CheetahString::from_static_str(
                    EndTransactionRequestHeader::PRODUCER_GROUP,
                ))
                .cloned()
                .unwrap_or_default(),
            tran_state_table_offset: map
                .get(&CheetahString::from_static_str(
                    EndTransactionRequestHeader::TRAN_STATE_TABLE_OFFSET,
                ))
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or_default(),
            commit_log_offset: map
                .get(&CheetahString::from_static_str(
                    EndTransactionRequestHeader::COMMIT_LOG_OFFSET,
                ))
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or_default(),
            commit_or_rollback: map
                .get(&CheetahString::from_static_str(
                    EndTransactionRequestHeader::COMMIT_OR_ROLLBACK,
                ))
                .and_then(|s| s.parse::<i32>().ok())
                .unwrap_or_default(),
            from_transaction_check: map
                .get(&CheetahString::from_static_str(
                    EndTransactionRequestHeader::FROM_TRANSACTION_CHECK,
                ))
                .and_then(|s| s.parse::<bool>().ok())
                .unwrap_or_default(),
            msg_id: map
                .get(&CheetahString::from_static_str(
                    EndTransactionRequestHeader::MSG_ID,
                ))
                .cloned()
                .unwrap_or_default(),
            transaction_id: map
                .get(&CheetahString::from_static_str(
                    EndTransactionRequestHeader::TRANSACTION_ID,
                ))
                .cloned(),
            rpc_request_header: <RpcRequestHeader as FromMap>::from(map).unwrap_or_default(),
        })
    }
}
