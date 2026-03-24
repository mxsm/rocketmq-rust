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
        assert!(!header.from_transaction_check);
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
        assert!(cloned_header.from_transaction_check);
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
        assert!(header.from_transaction_check);
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
        assert!(header.from_transaction_check);
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
        assert!(header.rpc_request_header.namespaced.unwrap());
        assert_eq!(header.rpc_request_header.broker_name.as_ref().unwrap(), "broker1");
        assert!(!header.rpc_request_header.oneway.unwrap());
    }

    #[test]
    fn end_transaction_request_header_to_map() {
        use crate::protocol::command_custom_header::CommandCustomHeader;

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

        let map = header.to_map().unwrap();
        assert_eq!(
            map.get(&CheetahString::from_static_str(EndTransactionRequestHeader::TOPIC))
                .unwrap(),
            "topic1"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str(
                EndTransactionRequestHeader::PRODUCER_GROUP
            ))
            .unwrap(),
            "group1"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str(
                EndTransactionRequestHeader::TRAN_STATE_TABLE_OFFSET
            ))
            .unwrap(),
            "123"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str(
                EndTransactionRequestHeader::COMMIT_LOG_OFFSET
            ))
            .unwrap(),
            "456"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str(
                EndTransactionRequestHeader::COMMIT_OR_ROLLBACK
            ))
            .unwrap(),
            "1"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str(
                EndTransactionRequestHeader::FROM_TRANSACTION_CHECK
            ))
            .unwrap(),
            "true"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str(EndTransactionRequestHeader::MSG_ID))
                .unwrap(),
            "msg1"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str(
                EndTransactionRequestHeader::TRANSACTION_ID
            ))
            .unwrap(),
            "tran1"
        );
    }

    #[test]
    fn end_transaction_request_header_to_map_without_transaction_id() {
        use crate::protocol::command_custom_header::CommandCustomHeader;

        let header = EndTransactionRequestHeader {
            topic: CheetahString::from("topic1"),
            producer_group: CheetahString::from("group1"),
            tran_state_table_offset: 123,
            commit_log_offset: 456,
            commit_or_rollback: 1,
            from_transaction_check: true,
            msg_id: CheetahString::from("msg1"),
            transaction_id: None,
            rpc_request_header: RpcRequestHeader::default(),
        };

        let map = header.to_map().unwrap();
        assert_eq!(
            map.get(&CheetahString::from_static_str(EndTransactionRequestHeader::TOPIC))
                .unwrap(),
            "topic1"
        );
        assert!(!map.contains_key(&CheetahString::from_static_str(
            EndTransactionRequestHeader::TRANSACTION_ID
        )));
    }

    #[test]
    fn end_transaction_request_header_from_map() {
        use crate::protocol::command_custom_header::FromMap;
        use std::collections::HashMap;

        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str(EndTransactionRequestHeader::TOPIC),
            CheetahString::from_static_str("topic1"),
        );
        map.insert(
            CheetahString::from_static_str(EndTransactionRequestHeader::PRODUCER_GROUP),
            CheetahString::from_static_str("group1"),
        );
        map.insert(
            CheetahString::from_static_str(EndTransactionRequestHeader::TRAN_STATE_TABLE_OFFSET),
            CheetahString::from_static_str("123"),
        );
        map.insert(
            CheetahString::from_static_str(EndTransactionRequestHeader::COMMIT_LOG_OFFSET),
            CheetahString::from_static_str("456"),
        );
        map.insert(
            CheetahString::from_static_str(EndTransactionRequestHeader::COMMIT_OR_ROLLBACK),
            CheetahString::from_static_str("1"),
        );
        map.insert(
            CheetahString::from_static_str(EndTransactionRequestHeader::FROM_TRANSACTION_CHECK),
            CheetahString::from_static_str("true"),
        );
        map.insert(
            CheetahString::from_static_str(EndTransactionRequestHeader::MSG_ID),
            CheetahString::from_static_str("msg1"),
        );
        map.insert(
            CheetahString::from_static_str(EndTransactionRequestHeader::TRANSACTION_ID),
            CheetahString::from_static_str("tran1"),
        );

        let header = <EndTransactionRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.topic, "topic1");
        assert_eq!(header.producer_group, "group1");
        assert_eq!(header.tran_state_table_offset, 123);
        assert_eq!(header.commit_log_offset, 456);
        assert_eq!(header.commit_or_rollback, 1);
        assert!(header.from_transaction_check);
        assert_eq!(header.msg_id, "msg1");
        assert_eq!(header.transaction_id.unwrap(), "tran1");
    }

    #[test]
    fn end_transaction_request_header_from_map_without_transaction_id() {
        use crate::protocol::command_custom_header::FromMap;
        use std::collections::HashMap;

        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str(EndTransactionRequestHeader::TOPIC),
            CheetahString::from_static_str("topic1"),
        );
        map.insert(
            CheetahString::from_static_str(EndTransactionRequestHeader::PRODUCER_GROUP),
            CheetahString::from_static_str("group1"),
        );
        map.insert(
            CheetahString::from_static_str(EndTransactionRequestHeader::TRAN_STATE_TABLE_OFFSET),
            CheetahString::from_static_str("123"),
        );
        map.insert(
            CheetahString::from_static_str(EndTransactionRequestHeader::COMMIT_LOG_OFFSET),
            CheetahString::from_static_str("456"),
        );
        map.insert(
            CheetahString::from_static_str(EndTransactionRequestHeader::COMMIT_OR_ROLLBACK),
            CheetahString::from_static_str("1"),
        );
        map.insert(
            CheetahString::from_static_str(EndTransactionRequestHeader::FROM_TRANSACTION_CHECK),
            CheetahString::from_static_str("true"),
        );
        map.insert(
            CheetahString::from_static_str(EndTransactionRequestHeader::MSG_ID),
            CheetahString::from_static_str("msg1"),
        );

        let header = <EndTransactionRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.topic, "topic1");
        assert_eq!(header.producer_group, "group1");
        assert_eq!(header.tran_state_table_offset, 123);
        assert_eq!(header.commit_log_offset, 456);
        assert_eq!(header.commit_or_rollback, 1);
        assert!(header.from_transaction_check);
        assert_eq!(header.msg_id, "msg1");
        assert!(header.transaction_id.is_none());
    }

    #[test]
    fn end_transaction_request_header_from_map_missing_required_field() {
        use crate::protocol::command_custom_header::FromMap;
        use std::collections::HashMap;

        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        enum RequiredField {
            Topic,
            ProducerGroup,
            TranStateTableOffset,
            CommitLogOffset,
            CommitOrRollback,
            FromTransactionCheck,
            MsgId,
        }

        impl RequiredField {
            fn as_str(&self) -> &'static str {
                match self {
                    RequiredField::Topic => EndTransactionRequestHeader::TOPIC,
                    RequiredField::ProducerGroup => EndTransactionRequestHeader::PRODUCER_GROUP,
                    RequiredField::TranStateTableOffset => EndTransactionRequestHeader::TRAN_STATE_TABLE_OFFSET,
                    RequiredField::CommitLogOffset => EndTransactionRequestHeader::COMMIT_LOG_OFFSET,
                    RequiredField::CommitOrRollback => EndTransactionRequestHeader::COMMIT_OR_ROLLBACK,
                    RequiredField::FromTransactionCheck => EndTransactionRequestHeader::FROM_TRANSACTION_CHECK,
                    RequiredField::MsgId => EndTransactionRequestHeader::MSG_ID,
                }
            }

            fn test_value(&self) -> &'static str {
                match self {
                    RequiredField::Topic => "topic1",
                    RequiredField::ProducerGroup => "group1",
                    RequiredField::TranStateTableOffset => "123",
                    RequiredField::CommitLogOffset => "456",
                    RequiredField::CommitOrRollback => "1",
                    RequiredField::FromTransactionCheck => "true",
                    RequiredField::MsgId => "msg1",
                }
            }
        }

        let all_required_fields: &[RequiredField] = &[
            RequiredField::Topic,
            RequiredField::ProducerGroup,
            RequiredField::TranStateTableOffset,
            RequiredField::CommitLogOffset,
            RequiredField::CommitOrRollback,
            RequiredField::FromTransactionCheck,
            RequiredField::MsgId,
        ];

        for &missing_field in all_required_fields {
            let mut map = HashMap::new();
            for &field in all_required_fields {
                if field != missing_field {
                    map.insert(
                        CheetahString::from_static_str(field.as_str()),
                        CheetahString::from_static_str(field.test_value()),
                    );
                }
            }

            let result = <EndTransactionRequestHeader as FromMap>::from(&map);
            assert!(
                result.is_err(),
                "Expected failure when missing required field: {:?}",
                missing_field
            );
        }
    }

    #[test]
    fn end_transaction_request_header_from_map_invalid_numeric_field() {
        use crate::protocol::command_custom_header::FromMap;
        use std::collections::HashMap;

        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str(EndTransactionRequestHeader::TOPIC),
            CheetahString::from_static_str("topic1"),
        );
        map.insert(
            CheetahString::from_static_str(EndTransactionRequestHeader::PRODUCER_GROUP),
            CheetahString::from_static_str("group1"),
        );
        map.insert(
            CheetahString::from_static_str(EndTransactionRequestHeader::TRAN_STATE_TABLE_OFFSET),
            CheetahString::from_static_str("invalid"),
        );
        map.insert(
            CheetahString::from_static_str(EndTransactionRequestHeader::COMMIT_LOG_OFFSET),
            CheetahString::from_static_str("456"),
        );
        map.insert(
            CheetahString::from_static_str(EndTransactionRequestHeader::COMMIT_OR_ROLLBACK),
            CheetahString::from_static_str("1"),
        );
        map.insert(
            CheetahString::from_static_str(EndTransactionRequestHeader::FROM_TRANSACTION_CHECK),
            CheetahString::from_static_str("true"),
        );
        map.insert(
            CheetahString::from_static_str(EndTransactionRequestHeader::MSG_ID),
            CheetahString::from_static_str("msg1"),
        );

        let result = <EndTransactionRequestHeader as FromMap>::from(&map);
        assert!(result.is_err());
    }

    #[test]
    fn end_transaction_request_header_from_map_invalid_bool_field() {
        use crate::protocol::command_custom_header::FromMap;
        use std::collections::HashMap;

        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str(EndTransactionRequestHeader::TOPIC),
            CheetahString::from_static_str("topic1"),
        );
        map.insert(
            CheetahString::from_static_str(EndTransactionRequestHeader::PRODUCER_GROUP),
            CheetahString::from_static_str("group1"),
        );
        map.insert(
            CheetahString::from_static_str(EndTransactionRequestHeader::TRAN_STATE_TABLE_OFFSET),
            CheetahString::from_static_str("123"),
        );
        map.insert(
            CheetahString::from_static_str(EndTransactionRequestHeader::COMMIT_LOG_OFFSET),
            CheetahString::from_static_str("456"),
        );
        map.insert(
            CheetahString::from_static_str(EndTransactionRequestHeader::COMMIT_OR_ROLLBACK),
            CheetahString::from_static_str("1"),
        );
        map.insert(
            CheetahString::from_static_str(EndTransactionRequestHeader::FROM_TRANSACTION_CHECK),
            CheetahString::from_static_str("invalid"),
        );
        map.insert(
            CheetahString::from_static_str(EndTransactionRequestHeader::MSG_ID),
            CheetahString::from_static_str("msg1"),
        );

        let result = <EndTransactionRequestHeader as FromMap>::from(&map);
        assert!(result.is_err());
    }

    #[test]
    fn end_transaction_request_header_from_map_transaction_state_values() {
        use crate::protocol::command_custom_header::FromMap;
        use rocketmq_common::common::sys_flag::message_sys_flag::MessageSysFlag;
        use std::collections::HashMap;

        struct TestCase {
            value: i32,
        }

        let test_cases = &[
            TestCase {
                value: MessageSysFlag::TRANSACTION_NOT_TYPE,
            },
            TestCase {
                value: MessageSysFlag::TRANSACTION_COMMIT_TYPE,
            },
            TestCase {
                value: MessageSysFlag::TRANSACTION_ROLLBACK_TYPE,
            },
        ];

        for case in test_cases {
            let mut map = HashMap::new();
            map.insert(
                CheetahString::from_static_str(EndTransactionRequestHeader::TOPIC),
                CheetahString::from_static_str("topic1"),
            );
            map.insert(
                CheetahString::from_static_str(EndTransactionRequestHeader::PRODUCER_GROUP),
                CheetahString::from_static_str("group1"),
            );
            map.insert(
                CheetahString::from_static_str(EndTransactionRequestHeader::TRAN_STATE_TABLE_OFFSET),
                CheetahString::from_static_str("123"),
            );
            map.insert(
                CheetahString::from_static_str(EndTransactionRequestHeader::COMMIT_LOG_OFFSET),
                CheetahString::from_static_str("456"),
            );
            map.insert(
                CheetahString::from_static_str(EndTransactionRequestHeader::COMMIT_OR_ROLLBACK),
                CheetahString::from(case.value.to_string()),
            );
            map.insert(
                CheetahString::from_static_str(EndTransactionRequestHeader::FROM_TRANSACTION_CHECK),
                CheetahString::from_static_str("true"),
            );
            map.insert(
                CheetahString::from_static_str(EndTransactionRequestHeader::MSG_ID),
                CheetahString::from_static_str("msg1"),
            );

            let result = <EndTransactionRequestHeader as FromMap>::from(&map);
            assert!(
                result.is_ok(),
                "commit_or_rollback={} should parse successfully but got {:?}",
                case.value,
                result
            );
            let header = result.unwrap();
            assert_eq!(
                header.commit_or_rollback, case.value,
                "commit_or_rollback should be {}",
                case.value
            );
        }
    }

    #[test]
    fn end_transaction_request_header_roundtrip() {
        use crate::protocol::command_custom_header::CommandCustomHeader;
        use crate::protocol::command_custom_header::FromMap;
        use rocketmq_common::common::sys_flag::message_sys_flag::MessageSysFlag;

        let original = EndTransactionRequestHeader {
            topic: CheetahString::from("topic1"),
            producer_group: CheetahString::from("group1"),
            tran_state_table_offset: 123,
            commit_log_offset: 456,
            commit_or_rollback: MessageSysFlag::TRANSACTION_COMMIT_TYPE,
            from_transaction_check: true,
            msg_id: CheetahString::from("msg1"),
            transaction_id: Some(CheetahString::from("tran1")),
            rpc_request_header: RpcRequestHeader::default(),
        };

        let map = original.to_map().unwrap();
        let restored = <EndTransactionRequestHeader as FromMap>::from(&map).unwrap();

        assert_eq!(original.topic, restored.topic);
        assert_eq!(original.producer_group, restored.producer_group);
        assert_eq!(original.tran_state_table_offset, restored.tran_state_table_offset);
        assert_eq!(original.commit_log_offset, restored.commit_log_offset);
        assert_eq!(original.commit_or_rollback, restored.commit_or_rollback);
        assert_eq!(original.from_transaction_check, restored.from_transaction_check);
        assert_eq!(original.msg_id, restored.msg_id);
        assert_eq!(original.transaction_id, restored.transaction_id);
    }

    #[test]
    fn end_transaction_request_header_via_remoting_command() {
        use crate::code::request_code::RequestCode;
        use crate::protocol::remoting_command::RemotingCommand;
        use bytes::BytesMut;
        use rocketmq_common::common::sys_flag::message_sys_flag::MessageSysFlag;

        let original = EndTransactionRequestHeader {
            topic: CheetahString::from("topic1"),
            producer_group: CheetahString::from("group1"),
            tran_state_table_offset: 123,
            commit_log_offset: 456,
            commit_or_rollback: MessageSysFlag::TRANSACTION_COMMIT_TYPE,
            from_transaction_check: true,
            msg_id: CheetahString::from("msg1"),
            transaction_id: Some(CheetahString::from("tran1")),
            rpc_request_header: RpcRequestHeader::default(),
        };

        let mut command = RemotingCommand::create_request_command(RequestCode::EndTransaction, original);
        let header_bytes = command.encode_header().expect("Failed to encode header");

        let mut buf = BytesMut::from(header_bytes);
        let decoded_command = RemotingCommand::decode(&mut buf).expect("Failed to decode command");
        let decoded_command = decoded_command.expect("Decoded command is None");

        assert_eq!(decoded_command.code(), RequestCode::EndTransaction as i32);

        let decoded_header = decoded_command
            .decode_command_custom_header::<EndTransactionRequestHeader>()
            .expect("Failed to decode header");

        assert_eq!(decoded_header.topic, "topic1");
        assert_eq!(decoded_header.producer_group, "group1");
        assert_eq!(decoded_header.tran_state_table_offset, 123);
        assert_eq!(decoded_header.commit_log_offset, 456);
        assert_eq!(
            decoded_header.commit_or_rollback,
            MessageSysFlag::TRANSACTION_COMMIT_TYPE
        );
        assert!(decoded_header.from_transaction_check);
        assert_eq!(decoded_header.msg_id, "msg1");
        assert_eq!(decoded_header.transaction_id.unwrap(), "tran1");
    }
}
