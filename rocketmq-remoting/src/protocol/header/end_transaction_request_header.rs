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

use cheetah_string::CheetahString;
use rocketmq_macros::RequestHeaderCodecV2;
use serde::Deserialize;
use serde::Serialize;

use crate::rpc::rpc_request_header::RpcRequestHeader;

#[derive(Clone, Debug, Serialize, Deserialize, Default, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct EndTransactionRequestHeader {
    #[required]
    pub topic: CheetahString,

    #[required]
    pub producer_group: CheetahString,

    //ConsumeQueue Offset
    #[required]
    pub tran_state_table_offset: u64,

    // Offset of the message in the CommitLog
    #[required]
    pub commit_log_offset: u64,

    //TRANSACTION_COMMIT_TYPE,TRANSACTION_ROLLBACK_TYPE,TRANSACTION_NOT_TYPE
    #[required]
    pub commit_or_rollback: i32,

    //Whether the check-back is initiated by the Broker
    #[required]
    pub from_transaction_check: bool,

    #[required]
    pub msg_id: CheetahString,

    pub transaction_id: Option<CheetahString>,

    #[serde(flatten)]
    pub rpc_request_header: RpcRequestHeader,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn end_transaction_request_header_default() {
        let header = EndTransactionRequestHeader::default();
        assert_eq!(header.topic, "");
        assert_eq!(header.producer_group, "");
        assert_eq!(header.tran_state_table_offset, 0);
        assert_eq!(header.commit_log_offset, 0);
        assert_eq!(header.commit_or_rollback, 0);
        assert_eq!(header.from_transaction_check, false);
        assert_eq!(header.msg_id, "");
        assert!(header.transaction_id.is_none());
    }

    #[test]
    fn end_transaction_request_header_clone() {
        let header = EndTransactionRequestHeader {
            topic: CheetahString::from("topic1"),
            producer_group: CheetahString::from("group1"),
            tran_state_table_offset: 123,
            commit_log_offset: 456,
            commit_or_rollback: 1,
            from_transaction_check: true,
            msg_id: CheetahString::from("msg1"),
            transaction_id: Some(CheetahString::from("tran1")),
            rpc_request_header: RpcRequestHeader::default(),
        };
        let cloned_header = header.clone();
        assert_eq!(cloned_header.topic, "topic1");
        assert_eq!(cloned_header.producer_group, "group1");
        assert_eq!(cloned_header.tran_state_table_offset, 123);
        assert_eq!(cloned_header.commit_log_offset, 456);
        assert_eq!(cloned_header.commit_or_rollback, 1);
        assert_eq!(cloned_header.from_transaction_check, true);
        assert_eq!(cloned_header.msg_id, "msg1");
        assert_eq!(cloned_header.transaction_id.as_ref().unwrap(), "tran1");
    }

    #[test]
    fn end_transaction_request_header_serialization() {
        let header = EndTransactionRequestHeader {
            topic: CheetahString::from("topic1"),
            producer_group: CheetahString::from("group1"),
            tran_state_table_offset: 123,
            commit_log_offset: 456,
            commit_or_rollback: 1,
            from_transaction_check: true,
            msg_id: CheetahString::from("msg1"),
            transaction_id: Some(CheetahString::from("tran1")),
            rpc_request_header: RpcRequestHeader {
                broker_name: Some(CheetahString::from("broker1")),
                ..Default::default()
            },
        };
        let json = serde_json::to_string(&header).unwrap();
        assert!(json.contains("\"topic\":\"topic1\""));
        assert!(json.contains("\"producerGroup\":\"group1\""));
        assert!(json.contains("\"tranStateTableOffset\":123"));
        assert!(json.contains("\"commitLogOffset\":456"));
        assert!(json.contains("\"commitOrRollback\":1"));
        assert!(json.contains("\"fromTransactionCheck\":true"));
        assert!(json.contains("\"msgId\":\"msg1\""));
        assert!(json.contains("\"transactionId\":\"tran1\""));
        assert!(json.contains("\"brokerName\":\"broker1\""));
    }

    #[test]
    fn end_transaction_request_header_deserialization() {
        let json = r#"{"topic":"topic1","producerGroup":"group1","tranStateTableOffset":123,"commitLogOffset":456,"commitOrRollback":1,"fromTransactionCheck":true,"msgId":"msg1","transactionId":"tran1","brokerName":"broker1"}"#;
        let header: EndTransactionRequestHeader = serde_json::from_str(json).unwrap();
        assert_eq!(header.topic, "topic1");
        assert_eq!(header.producer_group, "group1");
        assert_eq!(header.tran_state_table_offset, 123);
        assert_eq!(header.commit_log_offset, 456);
        assert_eq!(header.commit_or_rollback, 1);
        assert_eq!(header.from_transaction_check, true);
        assert_eq!(header.msg_id, "msg1");
        assert_eq!(header.transaction_id.as_ref().unwrap(), "tran1");
        assert_eq!(header.rpc_request_header.broker_name.as_ref().unwrap(), "broker1");
    }
    #[test]
    fn end_transaction_request_header_required_fields() {
        let json = r#"{"topic":"topic1","producerGroup":"group1","tranStateTableOffset":123,"commitLogOffset":456,"commitOrRollback":1,"fromTransactionCheck":true,"msgId":"msg1"}"#;
        let header: EndTransactionRequestHeader = serde_json::from_str(json).unwrap();
        assert_eq!(header.topic, "topic1");
        assert_eq!(header.producer_group, "group1");
        assert_eq!(header.tran_state_table_offset, 123);
        assert_eq!(header.commit_log_offset, 456);
        assert_eq!(header.commit_or_rollback, 1);
        assert_eq!(header.from_transaction_check, true);
        assert_eq!(header.msg_id, "msg1");
        assert!(header.transaction_id.is_none());
    }

    #[test]
    fn end_transaction_request_header_rpc_request_header() {
        let header = EndTransactionRequestHeader {
            topic: CheetahString::from("topic1"),
            producer_group: CheetahString::from("group1"),
            tran_state_table_offset: 123,
            commit_log_offset: 456,
            commit_or_rollback: 1,
            from_transaction_check: true,
            msg_id: CheetahString::from("msg1"),
            transaction_id: Some(CheetahString::from("tran1")),
            rpc_request_header: RpcRequestHeader {
                namespace: Some(CheetahString::from("namespace1")),
                namespaced: Some(true),
                broker_name: Some(CheetahString::from("broker1")),
                oneway: Some(false),
            },
        };
        assert_eq!(header.rpc_request_header.namespace.as_ref().unwrap(), "namespace1");
        assert_eq!(header.rpc_request_header.namespaced.unwrap(), true);
        assert_eq!(header.rpc_request_header.broker_name.as_ref().unwrap(), "broker1");
        assert_eq!(header.rpc_request_header.oneway.unwrap(), false);
    }
}
