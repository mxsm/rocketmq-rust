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

use clap::Parser;
use rocketmq_common::common::mq_version::RocketMqVersion;
use rocketmq_common::utils::util_all;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;

use crate::commands::CommandExecute;
use rocketmq_admin_core::core::consumer::ConsumerProgressRequest;
use rocketmq_admin_core::core::consumer::ConsumerProgressResult;
use rocketmq_admin_core::core::consumer::ConsumerService;
use rocketmq_admin_core::core::consumer::GroupConsumeInfo;

#[derive(Debug, Clone, Parser)]
pub struct ConsumerProgressSubCommand {
    #[arg(short = 'g', long = "groupName", required = false, help = "consumer group name")]
    consumer_group: Option<String>,

    #[arg(
        short = 't',
        long = "topicName",
        required = false,
        help = "topic name",
        requires = "consumer_group"
    )]
    topic_name: Option<String>,

    #[arg(
        short = 's',
        long = "showClientIP",
        required = false,
        help = "Show Client IP per Queue",
        default_value_t = false,
        requires = "consumer_group"
    )]
    show_client_ip: bool,

    #[arg(
        short = 'c',
        long = "cluster",
        required = false,
        help = "Cluster name or lmq parent topic, lmq is used to find the route.",
        requires = "consumer_group"
    )]
    cluster: Option<String>,

    #[arg(
        short = 'n',
        long = "namesrvAddr",
        required = false,
        help = "input name server address"
    )]
    namesrv_addr: Option<String>,
}

impl ConsumerProgressSubCommand {
    fn request(&self) -> RocketMQResult<ConsumerProgressRequest> {
        ConsumerProgressRequest::try_new(
            self.consumer_group.clone(),
            self.topic_name.clone(),
            self.show_client_ip,
            self.cluster.clone(),
            self.namesrv_addr.clone(),
        )
    }

    fn print_result(result: ConsumerProgressResult) {
        match result {
            ConsumerProgressResult::Group(group) => {
                if group.show_client_ip {
                    println!(
                        "{:<64}  {:<32}  {:<4}  {:<20}  {:<20}  {:<20} {:<20} {:<20} #LastTime",
                        "#Topic",
                        "#Broker Name",
                        "#QID",
                        "#Broker Offset",
                        "#Consumer Offset",
                        "#Client IP",
                        "#Diff",
                        "#Inflight"
                    );
                } else {
                    println!(
                        "{:<64}  {:<32}  {:<4}  {:<20}  {:<20}  {:<20} {:<20} #LastTime",
                        "#Topic", "#Broker Name", "#QID", "#Broker Offset", "#Consumer Offset", "#Diff", "#Inflight"
                    );
                }

                for row in group.rows {
                    let last_time = if row.last_timestamp == 0 {
                        "N/A".to_string()
                    } else {
                        util_all::time_millis_to_human_string2(row.last_timestamp)
                    };
                    if group.show_client_ip {
                        println!(
                            "{:<64}  {:<32}  {:<4}  {:<20}  {:<20}  {:<20} {:<20} {:<20} {}",
                            row.topic,
                            row.broker_name,
                            row.queue_id,
                            row.broker_offset,
                            row.consumer_offset,
                            row.client_ip.as_deref().unwrap_or("N/A"),
                            row.diff,
                            row.inflight,
                            last_time
                        );
                    } else {
                        println!(
                            "{:<64}  {:<32}  {:<4}  {:<20}  {:<20}  {:<20} {:<20} {}",
                            row.topic,
                            row.broker_name,
                            row.queue_id,
                            row.broker_offset,
                            row.consumer_offset,
                            row.diff,
                            row.inflight,
                            last_time
                        );
                    }
                }

                println!("\nConsume TPS: {:.2}", group.consume_tps);
                println!("Consume Diff Total: {}", group.diff_total);
                println!("Consume Inflight Total: {}", group.inflight_total);
            }
            ConsumerProgressResult::All(groups) => {
                println!(
                    "{:<64}  {:<6}  {:<24} {:<5}  {:<14}  {:<7}  #Diff Total",
                    "#Group", "#Count", "#Version", "#Type", "#Model", "#TPS"
                );
                for group in groups {
                    println!(
                        "{:<64}  {:<6}  {:<24} {:<5}  {:<14}  {:<7.2}  {}",
                        group.group,
                        group.count,
                        version_desc(&group),
                        consume_type_desc(&group),
                        message_model_desc(&group),
                        group.consume_tps,
                        group.diff_total
                    );
                }
            }
        }
    }
}

impl CommandExecute for ConsumerProgressSubCommand {
    async fn execute(
        &self,
        rpc_hook: Option<std::sync::Arc<dyn rocketmq_remoting::runtime::RPCHook>>,
    ) -> RocketMQResult<()> {
        let result =
            ConsumerService::query_consumer_progress_by_request_with_rpc_hook(self.request()?, rpc_hook).await?;
        Self::print_result(result);
        Ok(())
    }
}

fn consume_type_desc(info: &GroupConsumeInfo) -> &str {
    if info.count != 0 {
        match info.consume_type {
            ConsumeType::ConsumeActively => "PULL",
            ConsumeType::ConsumePassively => "PUSH",
            ConsumeType::ConsumePop => "POP",
        }
    } else {
        ""
    }
}

fn message_model_desc(info: &GroupConsumeInfo) -> String {
    if info.count != 0 && info.consume_type == ConsumeType::ConsumePassively {
        info.message_model.to_string()
    } else {
        String::new()
    }
}

fn version_desc(info: &GroupConsumeInfo) -> String {
    if info.count != 0 {
        RocketMqVersion::from_ordinal(info.version as u32).name().to_string()
    } else {
        String::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;

    #[test]
    fn test_consume_type_desc() {
        let mut info = GroupConsumeInfo {
            group: String::new(),
            version: 0,
            count: 1,
            consume_type: ConsumeType::ConsumeActively,
            message_model: MessageModel::Clustering,
            consume_tps: 0.0,
            diff_total: 0,
        };
        assert_eq!(consume_type_desc(&info), "PULL");

        info.consume_type = ConsumeType::ConsumePassively;
        assert_eq!(consume_type_desc(&info), "PUSH");

        info.consume_type = ConsumeType::ConsumePop;
        assert_eq!(consume_type_desc(&info), "POP");

        info.count = 0;
        assert_eq!(consume_type_desc(&info), "");
    }

    #[test]
    fn test_message_model_desc() {
        let mut info = GroupConsumeInfo {
            group: String::new(),
            version: 0,
            count: 1,
            consume_type: ConsumeType::ConsumePassively,
            message_model: MessageModel::Clustering,
            consume_tps: 0.0,
            diff_total: 0,
        };
        assert_eq!(message_model_desc(&info), "CLUSTERING");

        info.message_model = MessageModel::Broadcasting;
        assert_eq!(message_model_desc(&info), "BROADCASTING");

        info.count = 0;
        assert_eq!(message_model_desc(&info), "");

        info.count = 1;
        info.consume_type = ConsumeType::ConsumeActively;
        assert_eq!(message_model_desc(&info), "");
    }

    #[test]
    fn test_version_desc() {
        let mut info = GroupConsumeInfo {
            group: String::new(),
            version: RocketMqVersion::V4_9_4 as i32,
            count: 1,
            consume_type: ConsumeType::ConsumePassively,
            message_model: MessageModel::Clustering,
            consume_tps: 0.0,
            diff_total: 0,
        };
        assert_eq!(version_desc(&info), "V4_9_4");

        info.count = 0;
        assert_eq!(version_desc(&info), "");
    }

    #[test]
    fn test_command_parsing() {
        let cmd = ConsumerProgressSubCommand::try_parse_from(vec![
            "consumerProgress",
            "-g",
            "test-group",
            "-t",
            "test-topic",
            "-s",
            "--namesrvAddr",
            "127.0.0.1:9876",
        ])
        .unwrap();
        assert_eq!(cmd.consumer_group, Some("test-group".to_string()));
        assert_eq!(cmd.topic_name, Some("test-topic".to_string()));
        assert!(cmd.show_client_ip);
        assert_eq!(cmd.namesrv_addr, Some("127.0.0.1:9876".to_string()));
    }

    #[test]
    fn test_invalid_command_parsing() {
        let result = ConsumerProgressSubCommand::try_parse_from(vec!["consumerProgress", "-t", "test-topic"]);
        assert!(result.is_err());
    }
}
