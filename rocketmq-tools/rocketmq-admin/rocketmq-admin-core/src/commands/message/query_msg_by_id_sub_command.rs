// Copyright 2026 The RocketMQ Rust Authors
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

use std::sync::Arc;
use std::time::Instant;

use cheetah_string::CheetahString;
use chrono::Local;
use chrono::TimeZone;
use clap::Parser;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::MessageDecoder::validate_message_id;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_common::UtilAll::YYYY_MM_DD_HH_MM_SS_SSS;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::commands::CommandExecute;
use crate::commands::CommonArgs;

#[derive(Debug, Clone, Parser)]
pub struct QueryMsgByIdSubCommand {
    #[arg(
        short = 'i',
        long = "messageId",
        required = true,
        help = "Message ID to query (32-char hex for IPv4, 40-char for IPv6)"
    )]
    message_id: String,

    #[arg(
        short = 't',
        long = "topic",
        help = "Topic name (optional, used as hint for broker lookup)"
    )]
    topic: Option<String>,

    #[arg(
        short = 'p',
        long = "printBody",
        default_value = "true",
        help = "Whether to print message body"
    )]
    print_body: bool,

    #[arg(
        short = 'c',
        long = "charset",
        default_value = "UTF-8",
        help = "Character set for body decoding"
    )]
    charset: String,

    #[command(flatten)]
    common_args: CommonArgs,
}

impl QueryMsgByIdSubCommand {
    fn format_timestamp(timestamp: i64) -> String {
        if timestamp <= 0 {
            return "N/A".to_string();
        }
        let dt = Local.timestamp_millis_opt(timestamp);
        match dt {
            chrono::LocalResult::Single(dt) => dt.format(YYYY_MM_DD_HH_MM_SS_SSS).to_string(),
            _ => "N/A".to_string(),
        }
    }

    fn is_system_property(key: &str) -> bool {
        key.starts_with("MIN_OFFSET")
            || key.starts_with("MAX_OFFSET")
            || key.starts_with("UNIQ_KEY")
            || key.starts_with("WAIT")
            || key.starts_with("CLUSTER")
            || key.starts_with("CONSUME_START_TIME")
            || key.starts_with("TRANSACTION_PREPARED")
            || key.starts_with("TRANSACTION_STATE")
            || key.starts_with("DELAY")
    }

    fn print_basic_info(msg: &MessageExt, broker_addr: &str) {
        println!("Basic Information:");
        println!("  Topic: {}", msg.topic());
        println!("  Broker Name: {}", msg.broker_name);
        println!("  Broker Address: {}", broker_addr);
        println!("  Queue ID: {}", msg.queue_id());
        println!("  Queue Offset: {}", msg.queue_offset());
        println!("  Physical Offset: {}", msg.commit_log_offset());
        println!("  Message Size: {} bytes", msg.store_size());
        if let Some(body) = msg.body() {
            println!("  Body Size: {} bytes", body.len());
        }
    }

    fn print_timestamps(msg: &MessageExt) {
        println!("Timestamps:");
        println!("  Born Time: {}", Self::format_timestamp(msg.born_timestamp()));
        println!("  Store Time: {}", Self::format_timestamp(msg.store_timestamp()));
        let time_diff = msg.store_timestamp() - msg.born_timestamp();
        println!("  Time Difference: {} ms", time_diff);
    }

    fn print_hosts(msg: &MessageExt) {
        println!("Hosts:");
        println!("  Born Host: {} (Producer)", msg.born_host());
        println!("  Store Host: {} (Broker)", msg.store_host());
    }

    fn print_metadata(msg: &MessageExt) {
        println!("Message Metadata:");
        let uniq_key_key = CheetahString::from_static_str(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
        if let Some(uniq_key) = msg.get_property(&uniq_key_key) {
            println!("  Unique Key: {}", uniq_key);
        }
        if let Some(tags) = msg.get_tags() {
            println!("  Tags: {}", tags);
        }
        if let Some(keys) = msg.message_inner().keys() {
            println!("  Keys: {}", keys.join(", "));
        }
        println!("  System Flag: {}", msg.sys_flag());
        println!("  Body CRC: {}", msg.body_crc());
        println!("  Reconsume Times: {}", msg.reconsume_times());

        let prepared_tx_offset = msg.prepared_transaction_offset();
        if prepared_tx_offset > 0 {
            println!("  Prepared Transaction: true (offset: {})", prepared_tx_offset);
        } else {
            println!("  Prepared Transaction: false");
        }

        let delay_level_key = CheetahString::from_static_str(MessageConst::PROPERTY_DELAY_TIME_LEVEL);
        if let Some(delay_level) = msg.get_property(&delay_level_key) {
            println!("  Delay Level: {}", delay_level);
        }

        let tx_state_key = CheetahString::from_static_str("TRANSACTION_STATE");
        if let Some(tx_state) = msg.get_property(&tx_state_key) {
            println!("  Transaction State: {}", tx_state);
        }
    }

    fn print_properties(msg: &MessageExt) {
        let properties = msg.properties();
        if properties.is_empty() {
            return;
        }

        let mut user_props: Vec<(&CheetahString, &CheetahString)> = Vec::new();
        let mut system_props: Vec<(&CheetahString, &CheetahString)> = Vec::new();

        for (key, value) in properties {
            if Self::is_system_property(key.as_str()) {
                system_props.push((key, value));
            } else {
                user_props.push((key, value));
            }
        }

        if !user_props.is_empty() {
            println!("Properties (User):");
            for (key, value) in user_props {
                println!("  {}: {}", key, value);
            }
            println!();
        }

        if !system_props.is_empty() {
            println!("Properties (System):");
            for (key, value) in system_props {
                println!("  {}: {}", key, value);
            }
            println!();
        }
    }

    fn print_body(msg: &MessageExt, charset: &str, print_body: bool) {
        if !print_body {
            println!("Message Body: <Not printed>");
            return;
        }

        if let Some(body) = msg.body() {
            println!("Message Body ({}):", charset);
            println!("{}", "-".repeat(50));

            let charset_upper = charset.to_uppercase();
            if charset_upper == "UTF-8" || charset_upper == "UTF8" {
                match std::str::from_utf8(&body[..]) {
                    Ok(body_str) => println!("{}", body_str),
                    Err(_) => Self::print_hex_dump(&body),
                }
            } else {
                if std::str::from_utf8(&body[..]).is_ok() {
                    if let Ok(body_str) = std::str::from_utf8(&body[..]) {
                        println!("{}", body_str);
                    }
                } else {
                    Self::print_hex_dump(&body);
                }
            }
            println!("{}", "-".repeat(50));
        } else {
            println!("Message Body: <Empty>");
        }
    }

    fn print_hex_dump(data: &[u8]) {
        const BYTES_PER_LINE: usize = 16;
        println!("<Binary data: {} bytes>", data.len());
        println!("Hex dump:");
        for (i, chunk) in data.chunks(BYTES_PER_LINE).enumerate() {
            let offset = i * BYTES_PER_LINE;
            let hex: String = chunk.iter().map(|b| format!("{:02X} ", b)).collect();
            let ascii: String = chunk
                .iter()
                .map(|b| {
                    if b.is_ascii_graphic() || *b == b' ' {
                        *b as char
                    } else {
                        '.'
                    }
                })
                .collect();
            println!("{:08X}: {:<48} {}", offset, hex, ascii);
        }
    }

    fn print_message_info(msg: &MessageExt, broker_addr: &str, charset: &str, print_body: bool, query_time_ms: u64) {
        println!("Message Query Result");
        println!("====================");
        println!();
        println!("Message ID: {}", msg.msg_id());
        println!();
        Self::print_basic_info(msg, broker_addr);
        println!();
        Self::print_timestamps(msg);
        println!();
        Self::print_hosts(msg);
        println!();
        Self::print_metadata(msg);
        println!();
        Self::print_properties(msg);
        Self::print_body(msg, charset, print_body);
        println!();
        println!("Query Time: {} ms", query_time_ms);
        println!("Message Found: true");
    }
}

impl CommandExecute for QueryMsgByIdSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        validate_message_id(&self.message_id).map_err(RocketMQError::IllegalArgument)?;

        let start_time = Instant::now();

        let mut default_mqadmin_ext = if let Some(rpc_hook) = rpc_hook {
            DefaultMQAdminExt::with_rpc_hook(rpc_hook)
        } else {
            DefaultMQAdminExt::new()
        };
        default_mqadmin_ext
            .client_config_mut()
            .set_instance_name(current_millis().to_string().into());

        if let Some(ref namesrv_addr) = self.common_args.namesrv_addr {
            default_mqadmin_ext.set_namesrv_addr(namesrv_addr);
        }

        let operation_result = async {
            MQAdminExt::start(&mut default_mqadmin_ext)
                .await
                .map_err(|e| RocketMQError::Internal(format!("Failed to start MQAdminExt: {}", e)))?;

            let msg_id = self.message_id.trim();
            let topic = self
                .topic
                .as_ref()
                .map(|t| CheetahString::from(t.trim()))
                .unwrap_or_else(|| CheetahString::from_static_str(""));

            let cluster_name = CheetahString::from_static_str("DefaultCluster");

            let msg = default_mqadmin_ext
                .query_message(cluster_name, topic, CheetahString::from(msg_id))
                .await
                .map_err(|e| RocketMQError::Internal(format!("Failed to query message by ID '{}': {}", msg_id, e)))?;

            let broker_addr = format!("{}:{}", msg.store_host().ip(), msg.store_host().port());
            let query_time_ms = start_time.elapsed().as_millis() as u64;
            Self::print_message_info(&msg, &broker_addr, &self.charset, self.print_body, query_time_ms);

            Ok(())
        }
        .await;

        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;
        operation_result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_common::common::message::message_single::Message;

    fn create_test_command() -> QueryMsgByIdSubCommand {
        QueryMsgByIdSubCommand {
            message_id: "AC11000100002A9F0000000000000001".to_string(),
            topic: Some("test_topic".to_string()),
            print_body: true,
            charset: "UTF-8".to_string(),
            common_args: CommonArgs {
                namesrv_addr: Some("127.0.0.1:9876".to_string()),
                skip_confirm: false,
            },
        }
    }

    #[test]
    fn test_validate_message_id_valid_ipv4() {
        let result = validate_message_id("AC11000100002A9F0000000000000001");
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_message_id_valid_ipv6() {
        let result = validate_message_id("20010db800000000000000000000000100000001");
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_message_id_empty() {
        let result = validate_message_id("");
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.contains("empty"));
        }
    }

    #[test]
    fn test_validate_message_id_invalid_length() {
        let result = validate_message_id("AC11000100002A9F");
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.contains("Invalid message ID length"));
        }
    }

    #[test]
    fn test_validate_message_id_invalid_chars() {
        let result = validate_message_id("GH11000100002A9F0000000000000001");
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.contains("hexadecimal") || e.contains("Invalid"));
        }
    }

    #[test]
    fn test_validate_message_id_with_whitespace() {
        let result = validate_message_id("  AC11000100002A9F0000000000000001  ");
        assert!(result.is_ok());
    }

    #[test]
    fn test_format_timestamp_valid() {
        let result = QueryMsgByIdSubCommand::format_timestamp(1708337445123);
        assert!(!result.is_empty());
        assert!(!result.contains("N/A"));
    }

    #[test]
    fn test_format_timestamp_zero() {
        let result = QueryMsgByIdSubCommand::format_timestamp(0);
        assert_eq!(result, "N/A");
    }

    #[test]
    fn test_format_timestamp_negative() {
        let result = QueryMsgByIdSubCommand::format_timestamp(-1);
        assert_eq!(result, "N/A");
    }

    #[test]
    fn test_is_system_property() {
        assert!(QueryMsgByIdSubCommand::is_system_property("MIN_OFFSET"));
        assert!(QueryMsgByIdSubCommand::is_system_property("MAX_OFFSET"));
        assert!(QueryMsgByIdSubCommand::is_system_property("UNIQ_KEY"));
        assert!(QueryMsgByIdSubCommand::is_system_property("WAIT"));
        assert!(QueryMsgByIdSubCommand::is_system_property("CLUSTER"));
        assert!(QueryMsgByIdSubCommand::is_system_property("CONSUME_START_TIME"));
        assert!(QueryMsgByIdSubCommand::is_system_property("TRANSACTION_PREPARED"));
        assert!(QueryMsgByIdSubCommand::is_system_property("TRANSACTION_STATE"));
        assert!(QueryMsgByIdSubCommand::is_system_property("DELAY"));
        assert!(!QueryMsgByIdSubCommand::is_system_property("custom_key"));
        assert!(!QueryMsgByIdSubCommand::is_system_property("orderType"));
    }

    fn create_test_message_ext() -> MessageExt {
        let mut message = Message::default();
        message.set_topic(CheetahString::from_static_str("test_topic"));
        message.set_body(Some(bytes::Bytes::from("test message body")));

        MessageExt {
            message,
            broker_name: CheetahString::from_static_str("test_broker"),
            queue_id: 0,
            store_size: 128,
            queue_offset: 12345,
            sys_flag: 0,
            born_timestamp: 1708337445123,
            born_host: "192.168.1.100:54321".parse().unwrap(),
            store_timestamp: 1708337445125,
            store_host: "192.168.1.10:10911".parse().unwrap(),
            msg_id: CheetahString::from_static_str("AC11000100002A9F0000000000000001"),
            commit_log_offset: 123456789,
            body_crc: 12345,
            reconsume_times: 0,
            prepared_transaction_offset: 0,
        }
    }

    #[test]
    fn test_print_message_info() {
        let msg = create_test_message_ext();
        let broker_addr = "192.168.1.10:10911";
        QueryMsgByIdSubCommand::print_message_info(&msg, broker_addr, "UTF-8", true, 12);
    }

    #[test]
    fn test_print_message_info_without_body() {
        let msg = create_test_message_ext();
        let broker_addr = "192.168.1.10:10911";
        QueryMsgByIdSubCommand::print_message_info(&msg, broker_addr, "UTF-8", false, 15);
    }

    #[test]
    fn test_print_basic_info() {
        let msg = create_test_message_ext();
        QueryMsgByIdSubCommand::print_basic_info(&msg, "192.168.1.10:10911");
    }

    #[test]
    fn test_print_timestamps() {
        let msg = create_test_message_ext();
        QueryMsgByIdSubCommand::print_timestamps(&msg);
    }

    #[test]
    fn test_print_hosts() {
        let msg = create_test_message_ext();
        QueryMsgByIdSubCommand::print_hosts(&msg);
    }

    #[test]
    fn test_print_metadata() {
        let msg = create_test_message_ext();
        QueryMsgByIdSubCommand::print_metadata(&msg);
    }

    #[test]
    fn test_print_body_with_valid_utf8() {
        let msg = create_test_message_ext();
        QueryMsgByIdSubCommand::print_body(&msg, "UTF-8", true);
    }

    #[test]
    fn test_print_body_binary() {
        let mut message = Message::default();
        message.set_topic(CheetahString::from_static_str("test_topic"));
        message.set_body(Some(bytes::Bytes::from(vec![0x00, 0xFF, 0xAB, 0xCD])));

        let msg = MessageExt {
            message,
            broker_name: CheetahString::from_static_str("test_broker"),
            queue_id: 0,
            store_size: 128,
            queue_offset: 12345,
            sys_flag: 0,
            born_timestamp: 1708337445123,
            born_host: "192.168.1.100:54321".parse().unwrap(),
            store_timestamp: 1708337445125,
            store_host: "192.168.1.10:10911".parse().unwrap(),
            msg_id: CheetahString::from_static_str("AC11000100002A9F0000000000000001"),
            commit_log_offset: 123456789,
            body_crc: 12345,
            reconsume_times: 0,
            prepared_transaction_offset: 0,
        };
        QueryMsgByIdSubCommand::print_body(&msg, "UTF-8", true);
    }

    #[test]
    fn test_print_body_disabled() {
        let msg = create_test_message_ext();
        QueryMsgByIdSubCommand::print_body(&msg, "UTF-8", false);
    }
}
