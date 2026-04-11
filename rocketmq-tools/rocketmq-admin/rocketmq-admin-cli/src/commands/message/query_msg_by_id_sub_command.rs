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

use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use cheetah_string::CheetahString;
use chrono::Local;
use chrono::TimeZone;
use clap::Parser;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::MessageDecoder::validate_message_id;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_common::UtilAll::YYYY_MM_DD_HH_MM_SS_SSS;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use crate::commands::CommonArgs;
use rocketmq_admin_core::admin::default_mq_admin_ext::DefaultMQAdminExt;

const DEFAULT_TIMEOUT_MS: u64 = 3000;

#[derive(Debug, Clone, Parser)]
pub struct QueryMsgByIdSubCommand {
    #[arg(
        short = 'i',
        long = "messageId",
        required = true,
        action = clap::ArgAction::Append,
        help = "Message ID to query (32-char hex for IPv4, 40-char for IPv6). Can be specified multiple times."
    )]
    message_ids: Vec<String>,

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
        help = "Character set for body decoding (supported: UTF-8, ASCII, ISO-8859-1)"
    )]
    charset: String,

    #[arg(
        short = 'T',
        long = "timeout",
        default_value_t = DEFAULT_TIMEOUT_MS,
        help = "Query timeout in milliseconds"
    )]
    timeout: u64,

    #[command(flatten)]
    common_args: CommonArgs,
}

struct MessageOutputConfig<'a> {
    charset: &'a str,
    print_body: bool,
    broker_addr: &'a str,
    query_time_ms: u64,
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

        let (user_props, system_props): (Vec<_>, Vec<_>) = properties
            .iter()
            .partition(|(key, _)| !Self::is_system_property(key.as_str()));

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

    fn is_utf8_charset(charset: &str) -> bool {
        charset.eq_ignore_ascii_case("utf-8") || charset.eq_ignore_ascii_case("utf8")
    }

    fn is_supported_charset(charset: &str) -> bool {
        Self::is_utf8_charset(charset)
            || charset.eq_ignore_ascii_case("ascii")
            || charset.eq_ignore_ascii_case("iso-8859-1")
            || charset.eq_ignore_ascii_case("latin1")
            || charset.eq_ignore_ascii_case("latin-1")
    }

    fn decode_body_with_charset(body: &[u8], charset: &str) -> Result<String, String> {
        if Self::is_utf8_charset(charset) {
            return std::str::from_utf8(body)
                .map(|s| s.to_string())
                .map_err(|_| "Invalid UTF-8 sequence".to_string());
        }

        if charset.eq_ignore_ascii_case("iso-8859-1")
            || charset.eq_ignore_ascii_case("latin1")
            || charset.eq_ignore_ascii_case("latin-1")
        {
            return Ok(body.iter().map(|&b| b as char).collect());
        }

        if charset.eq_ignore_ascii_case("ascii") {
            return body
                .iter()
                .map(|&b| {
                    if b.is_ascii() {
                        Ok(b as char)
                    } else {
                        Err(format!("Non-ASCII byte: {:02X}", b))
                    }
                })
                .collect::<Result<String, String>>();
        }

        std::str::from_utf8(body).map(|s| s.to_string()).map_err(|_| {
            format!(
                "Unsupported charset: {}. Only UTF-8, ASCII, and ISO-8859-1 are supported.",
                charset
            )
        })
    }

    fn print_body(msg: &MessageExt, charset: &str, print_body: bool) {
        if !print_body {
            println!("Message Body: <Not printed>");
            return;
        }

        if let Some(body) = msg.body() {
            println!("Message Body ({}):", charset);
            println!("{}", "-".repeat(50));

            match Self::decode_body_with_charset(&body, charset) {
                Ok(body_str) => {
                    if body_str.is_empty() {
                        println!("<Empty body>");
                    } else {
                        println!("{}", body_str);
                    }
                }
                Err(_) => Self::print_hex_dump(&body),
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

    fn print_message_not_found(msg_id: &str, query_time_ms: u64, reason: &str) {
        println!("Message Query Result");
        println!("====================");
        println!();
        println!("Message ID: {}", msg_id);
        println!();
        println!("Message Found: false");
        println!("Reason: {}", reason);
        println!("Query Time: {} ms", query_time_ms);
        println!();
        println!("Possible causes:");
        println!("  - Message has been deleted or compacted");
        println!("  - Message ID is incorrect");
        println!("  - Broker is unreachable");
    }

    fn print_message_info(msg: &MessageExt, config: &MessageOutputConfig<'_>) {
        println!("Message Query Result");
        println!("====================");
        println!();
        println!("Message ID: {}", msg.msg_id());
        println!();
        Self::print_basic_info(msg, config.broker_addr);
        println!();
        Self::print_timestamps(msg);
        println!();
        Self::print_hosts(msg);
        println!();
        Self::print_metadata(msg);
        println!();
        Self::print_properties(msg);
        Self::print_body(msg, config.charset, config.print_body);
        println!();
        println!("Query Time: {} ms", config.query_time_ms);
        println!("Message Found: true");
    }

    async fn query_single_message(
        admin: &mut DefaultMQAdminExt,
        msg_id: &str,
        topic: &CheetahString,
        cluster_name: &CheetahString,
        charset: &str,
        print_body: bool,
    ) -> RocketMQResult<()> {
        let start_time = Instant::now();
        let msg_id = msg_id.trim().to_string();

        let query_result = admin
            .query_message(cluster_name.clone(), topic.clone(), CheetahString::from(&msg_id))
            .await;

        let query_time_ms = start_time.elapsed().as_millis() as u64;

        match query_result {
            Ok(msg) => {
                let broker_addr = format!("{}", msg.store_host());
                let config = MessageOutputConfig {
                    charset,
                    print_body,
                    broker_addr: &broker_addr,
                    query_time_ms,
                };
                Self::print_message_info(&msg, &config);
                Ok(())
            }
            Err(RocketMQError::BrokerOperationFailed { code: _, message, .. })
                if message.contains("not found")
                    || message.contains("does not exist")
                    || message.contains("No message") =>
            {
                Self::print_message_not_found(&msg_id, query_time_ms, &message);
                Ok(())
            }
            Err(RocketMQError::Rpc(rpc_error)) => {
                let err_msg = format!("{}", rpc_error);
                if err_msg.contains("not found") || err_msg.contains("does not exist") || err_msg.contains("No message")
                {
                    Self::print_message_not_found(&msg_id, query_time_ms, &err_msg);
                    Ok(())
                } else {
                    Err(RocketMQError::Internal(format!(
                        "Failed to query message by ID '{}': {}",
                        msg_id, err_msg
                    )))
                }
            }
            Err(e) => Err(RocketMQError::Internal(format!(
                "Failed to query message by ID '{}': {}",
                msg_id, e
            ))),
        }
    }
}

impl CommandExecute for QueryMsgByIdSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        if self.message_ids.is_empty() {
            return Err(RocketMQError::IllegalArgument(
                "At least one message ID is required".to_string(),
            ));
        }

        for msg_id in &self.message_ids {
            validate_message_id(msg_id).map_err(RocketMQError::IllegalArgument)?;
        }

        if !Self::is_supported_charset(&self.charset) {
            return Err(RocketMQError::IllegalArgument(format!(
                "Unsupported charset: '{}'. Supported charsets are: UTF-8, ASCII, ISO-8859-1 (also: latin1, latin-1)",
                self.charset
            )));
        }

        let timeout_duration = Duration::from_millis(self.timeout);
        let mut default_mqadmin_ext = if let Some(rpc_hook) = rpc_hook {
            DefaultMQAdminExt::with_rpc_hook_and_timeout(rpc_hook, timeout_duration)
        } else {
            DefaultMQAdminExt::with_timeout(timeout_duration)
        };
        default_mqadmin_ext
            .client_config_mut()
            .set_instance_name(current_millis().to_string().into());

        if let Some(ref namesrv_addr) = self.common_args.namesrv_addr {
            default_mqadmin_ext.set_namesrv_addr(namesrv_addr);
        }

        let timeout_ms = self.timeout;
        let operation_result = async {
            MQAdminExt::start(&mut default_mqadmin_ext)
                .await
                .map_err(|e| RocketMQError::Internal(format!("Failed to start MQAdminExt: {}", e)))?;

            let topic = self
                .topic
                .as_ref()
                .map(|t| CheetahString::from(t.trim()))
                .unwrap_or_else(|| CheetahString::from_static_str(""));

            let cluster_name = CheetahString::from_static_str("DefaultCluster");

            let total_count = self.message_ids.len();
            let mut success_count = 0;
            let mut fail_count = 0;

            for (index, msg_id) in self.message_ids.iter().enumerate() {
                if total_count > 1 {
                    println!();
                    println!(
                        "{}Message {} of {}{}",
                        "=".repeat(20),
                        index + 1,
                        total_count,
                        "=".repeat(20)
                    );
                    println!();
                }

                let query_future = Self::query_single_message(
                    &mut default_mqadmin_ext,
                    msg_id,
                    &topic,
                    &cluster_name,
                    &self.charset,
                    self.print_body,
                );

                match tokio::time::timeout(Duration::from_millis(timeout_ms), query_future).await {
                    Ok(Ok(())) => success_count += 1,
                    Ok(Err(_)) => fail_count += 1,
                    Err(_) => {
                        eprintln!("Query timed out for message ID: {}", msg_id);
                        fail_count += 1;
                    }
                }
            }

            if total_count > 1 {
                println!();
                println!("{}Summary{}", "=".repeat(20), "=".repeat(20));
                println!("Total: {} messages", total_count);
                println!("Success: {}", success_count);
                println!("Failed: {}", fail_count);
            }

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

    #[allow(dead_code)]
    fn create_test_command() -> QueryMsgByIdSubCommand {
        QueryMsgByIdSubCommand {
            message_ids: vec!["AC11000100002A9F0000000000000001".to_string()],
            topic: Some("test_topic".to_string()),
            print_body: true,
            charset: "UTF-8".to_string(),
            timeout: DEFAULT_TIMEOUT_MS,
            common_args: CommonArgs {
                namesrv_addr: Some("127.0.0.1:9876".to_string()),
                skip_confirm: false,
            },
        }
    }

    #[allow(dead_code)]
    fn create_test_command_multiple_ids() -> QueryMsgByIdSubCommand {
        QueryMsgByIdSubCommand {
            message_ids: vec![
                "AC11000100002A9F0000000000000001".to_string(),
                "AC11000100002A9F0000000000000002".to_string(),
            ],
            topic: Some("test_topic".to_string()),
            print_body: true,
            charset: "UTF-8".to_string(),
            timeout: DEFAULT_TIMEOUT_MS,
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
        let result = validate_message_id("20010db800000000000000000000000100002A9F0000000000000001");
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
        let config = MessageOutputConfig {
            charset: "UTF-8",
            print_body: true,
            broker_addr,
            query_time_ms: 12,
        };
        QueryMsgByIdSubCommand::print_message_info(&msg, &config);
    }

    #[test]
    fn test_print_message_info_without_body() {
        let msg = create_test_message_ext();
        let broker_addr = "192.168.1.10:10911";
        let config = MessageOutputConfig {
            charset: "UTF-8",
            print_body: false,
            broker_addr,
            query_time_ms: 15,
        };
        QueryMsgByIdSubCommand::print_message_info(&msg, &config);
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

    #[test]
    fn test_is_utf8_charset() {
        assert!(QueryMsgByIdSubCommand::is_utf8_charset("UTF-8"));
        assert!(QueryMsgByIdSubCommand::is_utf8_charset("utf-8"));
        assert!(QueryMsgByIdSubCommand::is_utf8_charset("UTF8"));
        assert!(QueryMsgByIdSubCommand::is_utf8_charset("utf8"));
        assert!(!QueryMsgByIdSubCommand::is_utf8_charset("GBK"));
        assert!(!QueryMsgByIdSubCommand::is_utf8_charset("ISO-8859-1"));
    }

    #[test]
    fn test_decode_body_with_charset_utf8() {
        let body = b"Hello, World!";
        let result = QueryMsgByIdSubCommand::decode_body_with_charset(body, "UTF-8");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Hello, World!");
    }

    #[test]
    fn test_decode_body_with_charset_utf8_case_insensitive() {
        let body = b"Hello, World!";
        let result = QueryMsgByIdSubCommand::decode_body_with_charset(body, "utf-8");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Hello, World!");

        let result = QueryMsgByIdSubCommand::decode_body_with_charset(body, "utf8");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Hello, World!");
    }

    #[test]
    fn test_decode_body_with_charset_iso_8859_1() {
        let body = b"Hello, World!";
        let result = QueryMsgByIdSubCommand::decode_body_with_charset(body, "ISO-8859-1");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Hello, World!");
    }

    #[test]
    fn test_decode_body_with_charset_latin1() {
        let body = b"Hello, World!";
        let result = QueryMsgByIdSubCommand::decode_body_with_charset(body, "latin1");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Hello, World!");
    }

    #[test]
    fn test_decode_body_with_charset_ascii() {
        let body = b"Hello, World!";
        let result = QueryMsgByIdSubCommand::decode_body_with_charset(body, "ASCII");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Hello, World!");
    }

    #[test]
    fn test_decode_body_with_charset_unsupported() {
        let body = b"Hello, World!";
        let result = QueryMsgByIdSubCommand::decode_body_with_charset(body, "invalid-charset");
        assert!(result.is_ok());
    }

    #[test]
    fn test_decode_body_with_empty_body() {
        let body: &[u8] = b"";
        let result = QueryMsgByIdSubCommand::decode_body_with_charset(body, "UTF-8");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "");
    }

    #[test]
    fn test_print_message_not_found() {
        QueryMsgByIdSubCommand::print_message_not_found(
            "AC11000100002A9F0000000000000001",
            15,
            "Message not found in broker",
        );
    }

    #[test]
    fn test_print_body_with_iso_8859_1_charset() {
        let mut message = Message::default();
        message.set_topic(CheetahString::from_static_str("test_topic"));
        let iso_bytes = vec![0x48, 0x65, 0x6C, 0x6C, 0x6F]; // "Hello" in ISO-8859-1
        message.set_body(Some(bytes::Bytes::from(iso_bytes)));

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
        QueryMsgByIdSubCommand::print_body(&msg, "ISO-8859-1", true);
    }

    #[test]
    fn test_print_body_with_empty_body() {
        let mut message = Message::default();
        message.set_topic(CheetahString::from_static_str("test_topic"));
        message.set_body(None);

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
}
