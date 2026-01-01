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

use clap::Parser;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::commands::command_util::CommandUtil;
use crate::commands::CommandExecute;
use crate::commands::CommonArgs;

#[derive(Debug, Parser, Clone)]
pub struct UpdateTopicPermSubCommand {
    #[command(flatten)]
    common_args: CommonArgs,

    /// Broker address
    #[arg(
        short = 'b',
        long = "brokerAddr",
        required = false,
        conflicts_with = "cluster_name",
        help = "create topic to which broker"
    )]
    broker_addr: Option<String>,

    /// Cluster name
    #[arg(
        short = 'c',
        long = "clusterName",
        required = false,
        help = "create topic to which cluster"
    )]
    cluster_name: Option<String>,

    /// Topic name
    #[arg(short = 't', long = "topic", required = true, help = "Topic name")]
    topic: String,

    /// Perm
    #[arg(
        short = 'p',
        long = "perm",
        required = true,
        help = "set topic's permission(2|4|6), intro[2:W; 4:R; 6:RW]"
    )]
    perm: String,
}

impl CommandExecute for UpdateTopicPermSubCommand {
    async fn execute(&self, _rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        if self.broker_addr.is_none() && self.cluster_name.is_none() {
            return Err(RocketMQError::IllegalArgument(
                "UpdateTopicPermSubCommand: Either brokerAddr (-b) or clusterName (-c) must be provided".into(),
            ));
        }

        let validation_result = TopicValidator::validate_topic(&self.topic);
        if !validation_result.valid() {
            return Err(RocketMQError::IllegalArgument(format!(
                "UpdateTopicPermSubCommand: Invalid topic name: {}",
                validation_result.remark().as_str()
            )));
        }

        let mut default_mqadmin_ext = DefaultMQAdminExt::new();
        default_mqadmin_ext
            .client_config_mut()
            .set_instance_name(get_current_millis().to_string().into());
        let operation_result = async {
            MQAdminExt::start(&mut default_mqadmin_ext).await.map_err(|e| {
                RocketMQError::Internal(format!("UpdateTopicPermSubCommand: Failed to start MQAdminExt: {}", e))
            })?;
            let topic = self.topic.trim();
            let topic_route_data = default_mqadmin_ext
                .examine_topic_route_info(topic.into())
                .await?
                .ok_or_else(|| {
                    RocketMQError::IllegalArgument(format!(
                        "UpdateTopicPermSubCommand: Topic route not found for topic: {}",
                        topic
                    ))
                })?;
            let queue_datas = &topic_route_data.queue_datas;
            if queue_datas.is_empty() {
                return Err(RocketMQError::IllegalArgument(format!(
                    "UpdateTopicPermSubCommand: No queue data found for topic: {}",
                    topic
                )));
            }

            let perm = self.perm.parse::<u32>().map_err(|e| {
                RocketMQError::IllegalArgument(format!(
                    "UpdateTopicPermSubCommand: Invalid perm value '{}': {}",
                    self.perm, e
                ))
            })?;
            if perm != 2 && perm != 4 && perm != 6 {
                return Err(RocketMQError::IllegalArgument(format!(
                    "UpdateTopicPermSubCommand: Invalid perm value '{}': perm must be 2, 4, or 6",
                    perm
                )));
            }
            let topic_config = TopicConfig {
                topic_name: Some(self.topic.clone().into()),
                read_queue_nums: queue_datas[0].read_queue_nums(),
                write_queue_nums: queue_datas[0].write_queue_nums(),
                topic_sys_flag: queue_datas[0].topic_sys_flag(),
                perm,
                ..Default::default()
            };
            if let Some(broker_addr) = &self.broker_addr {
                let broker_addr = broker_addr.trim();
                let broker_datas = &topic_route_data.broker_datas;
                let mut broker_name: Option<String> = None;
                for data in broker_datas {
                    let broker_addrs = data.broker_addrs();
                    for (&key, value) in broker_addrs.iter() {
                        if key == mix_all::MASTER_ID && value == broker_addr {
                            broker_name = Some(data.broker_name().to_string());
                            break;
                        }
                    }
                    if broker_name.is_some() {
                        break;
                    }
                }
                if let Some(ref broker_name_ref) = broker_name {
                    let queue_data_list = &topic_route_data.queue_datas;
                    let mut old_perm = 0;
                    for data in queue_data_list {
                        if broker_name_ref == data.broker_name() {
                            old_perm = data.perm();
                            if perm == old_perm {
                                println!("new perm equals to the old one!");
                                return Ok(());
                            }
                            break;
                        }
                    }
                    default_mqadmin_ext
                        .create_and_update_topic_config(broker_addr.into(), topic_config.clone())
                        .await
                        .map_err(|_e| {
                            RocketMQError::Internal(
                                "UpdateTopicPermSubCommand error broker not exit or broker is not master!.".to_string(),
                            )
                        })?;
                    println!(
                        "update topic perm from {} to {} in {} success.",
                        old_perm, perm, broker_addr
                    );
                    println!("{:?}.", topic_config);
                } else {
                    println!("updateTopicPerm error broker not exit or broker is not master!.");
                    return Ok(());
                }
            } else if let Some(cluster_name) = &self.cluster_name {
                let cluster_name = cluster_name.trim();
                let cluster_info = default_mqadmin_ext.examine_broker_cluster_info().await?;
                let master_set =
                    CommandUtil::fetch_master_addr_by_cluster_name(&cluster_info, cluster_name).map_err(|_e| {
                        RocketMQError::IllegalArgument(format!(
                            "UpdateTopicPermSubCommand: Cluster '{}' not found",
                            cluster_name
                        ))
                    })?;
                for addr in master_set {
                    default_mqadmin_ext
                        .create_and_update_topic_config(addr.clone(), topic_config.clone())
                        .await
                        .map_err(|_e| {
                            RocketMQError::Internal(
                                "UpdateTopicPermSubCommand error broker not exit or broker is not master!.".to_string(),
                            )
                        })?;
                    println!(
                        "update topic perm from {} to {} in {:?} success.",
                        queue_datas[0].perm(),
                        perm,
                        addr
                    )
                }
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
    use crate::commands::CommonArgs;

    #[test]
    fn test_update_topic_perm_sub_command_parse() {
        // Test parsing with broker address
        let args =
            UpdateTopicPermSubCommand::try_parse_from(["test", "-b", "127.0.0.1:10911", "-t", "TestTopic", "-p", "6"])
                .unwrap();

        assert_eq!(args.broker_addr, Some("127.0.0.1:10911".to_string()));
        assert_eq!(args.cluster_name, None);
        assert_eq!(args.topic, "TestTopic");
        assert_eq!(args.perm, "6");
    }

    #[test]
    fn test_update_topic_perm_sub_command_parse_with_cluster() {
        // Test parsing with cluster name
        let args =
            UpdateTopicPermSubCommand::try_parse_from(["test", "-c", "DefaultCluster", "-t", "TestTopic", "-p", "4"])
                .unwrap();

        assert_eq!(args.broker_addr, None);
        assert_eq!(args.cluster_name, Some("DefaultCluster".to_string()));
        assert_eq!(args.topic, "TestTopic");
        assert_eq!(args.perm, "4");
    }

    #[tokio::test]
    async fn test_execute_fails_without_broker_or_cluster() {
        let command = UpdateTopicPermSubCommand {
            common_args: CommonArgs {
                namesrv_addr: None,
                skip_confirm: false,
            },
            broker_addr: None,
            cluster_name: None,
            topic: "TestTopic".to_string(),
            perm: "6".to_string(),
        };

        let result = command.execute(None).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Either brokerAddr (-b) or clusterName (-c) must be provided"));
    }

    #[tokio::test]
    async fn test_execute_fails_with_invalid_topic() {
        let command = UpdateTopicPermSubCommand {
            common_args: CommonArgs {
                namesrv_addr: None,
                skip_confirm: false,
            },
            broker_addr: Some("127.0.0.1:10911".to_string()),
            cluster_name: None,
            topic: "".to_string(), // Invalid topic name
            perm: "6".to_string(),
        };

        let result = command.execute(None).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid topic name"));
    }

    #[test]
    fn test_command_structure() {
        // Verify the command structure
        let command = UpdateTopicPermSubCommand {
            common_args: CommonArgs {
                namesrv_addr: Some("127.0.0.1:9876".to_string()),
                skip_confirm: false,
            },
            broker_addr: Some("127.0.0.1:10911".to_string()),
            cluster_name: None,
            topic: "TestTopic".to_string(),
            perm: "6".to_string(),
        };

        assert_eq!(command.topic, "TestTopic");
        assert_eq!(command.perm, "6");
        assert_eq!(command.broker_addr, Some("127.0.0.1:10911".to_string()));
        assert_eq!(command.cluster_name, None);
    }
}
