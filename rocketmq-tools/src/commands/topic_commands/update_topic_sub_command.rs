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
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_common::common::TopicSysFlag;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::commands::command_util::CommandUtil;
use crate::commands::CommandExecute;
use crate::commands::CommonArgs;

#[derive(Debug, Clone, Parser)]
pub struct UpdateTopicSubCommand {
    /// Common arguments
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
        conflicts_with = "broker_addr",
        help = "create topic to which cluster"
    )]
    cluster_name: Option<String>,

    /// Topic name
    #[arg(short = 't', long = "topic", required = true, help = "topic name")]
    topic: String,

    /// Number of read queues
    #[arg(
        short = 'r',
        long = "readQueueNums",
        required = false,
        default_value = "8",
        value_parser = clap::value_parser!(u32).range(1..=65535),
        help = "set read queue nums"
    )]
    read_queue_nums: u32,

    /// Number of write queues
    #[arg(
        short = 'w',
        long = "writeQueueNums",
        required = false,
        default_value = "8",
        value_parser = clap::value_parser!(u32).range(1..=65535),
        help = "set write queue nums"
    )]
    write_queue_nums: u32,

    /// Topic permission
    /// 2: Write only, 4: Read only, 6: Read and Write
    #[arg(
        short = 'p',
        long = "perm",
        required = false,
        value_parser = clap::builder::PossibleValuesParser::new(["2", "4", "6"]),
        help = "set topic's permission(2|4|6), intro[2:W 4:R; 6:RW]"
    )]
    perm: Option<String>,

    /// Whether it's an ordered message topic
    #[arg(
        short = 'o',
        long = "order",
        required = false,
        help = "set topic's order(true|false)"
    )]
    order: Option<bool>,

    /// Whether it's a unit topic
    #[arg(short = 'u', long = "unit", required = false, help = "is unit topic (true|false)")]
    unit: Option<bool>,

    /// Whether there's a unit subscription group
    #[arg(
        short = 's',
        long = "hasUnitSub",
        required = false,
        help = "has unit sub (true|false)"
    )]
    has_unit_sub: Option<bool>,
}

impl CommandExecute for UpdateTopicSubCommand {
    async fn execute(&self, _rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        if self.broker_addr.is_none() && self.cluster_name.is_none() {
            return Err(RocketMQError::IllegalArgument(
                "UpdateTopicSubCommand: Either brokerAddr (-b) or clusterName (-c) must be provided".into(),
            ));
        }

        let validation_result = TopicValidator::validate_topic(&self.topic);
        if !validation_result.valid() {
            return Err(RocketMQError::IllegalArgument(format!(
                "UpdateTopicSubCommand: Invalid topic name: {}",
                validation_result.remark().as_str()
            )));
        }

        let mut default_mqadmin_ext = DefaultMQAdminExt::new();
        default_mqadmin_ext
            .client_config_mut()
            .set_instance_name(get_current_millis().to_string().into());

        let operation_result = async {
            if let Some(addr) = &self.common_args.namesrv_addr {
                default_mqadmin_ext.set_namesrv_addr(addr.trim());
            }

            MQAdminExt::start(&mut default_mqadmin_ext).await.map_err(|e| {
                RocketMQError::Internal(format!("UpdateTopicSubCommand: Failed to start MQAdminExt: {}", e))
            })?;

            let mut topic_config = TopicConfig {
                topic_name: Some(self.topic.clone().into()),
                read_queue_nums: self.read_queue_nums,
                write_queue_nums: self.write_queue_nums,
                ..Default::default()
            };

            if let Some(perm) = &self.perm {
                topic_config.perm = perm.parse::<u32>().expect("clap validated perm to be 2, 4, or 6");
            } else {
                topic_config.perm = 6;
            }

            let is_order = self.order.unwrap_or(false);
            topic_config.order = is_order;

            let is_unit = self.unit.unwrap_or(false);
            let is_center_sync = self.has_unit_sub.unwrap_or(false);
            let topic_sys_flag = TopicSysFlag::build_sys_flag(is_unit, is_center_sync);
            topic_config.topic_sys_flag = topic_sys_flag;

            if let Some(broker_addr) = &self.broker_addr {
                println!("Creating/updating topic '{}' on broker: {}", self.topic, broker_addr);

                default_mqadmin_ext
                    .create_and_update_topic_config(broker_addr.clone().into(), topic_config.clone())
                    .await
                    .map_err(|e| {
                        RocketMQError::Internal(format!("UpdateTopicSubCommand: Failed to create/update topic: {}", e))
                    })?;

                if is_order {
                    // Note: Order message configuration is not yet fully implemented in
                    // rocketmq-rust. The create_or_update_order_conf method is
                    // still unimplemented. For now, we only create the topic
                    // with order=true flag. Full order configuration will be
                    // added when the underlying API is implemented.

                    println!(
                        "Warning: Order message topic created, but order configuration (queue allocation) is not yet \
                         implemented."
                    );
                    println!(
                        "The topic will be marked as ordered (order=true), but you may need to manually configure \
                         queue allocation."
                    );

                    // TODO: Uncomment when create_or_update_order_conf is implemented:
                    /*
                    let cluster_info = default_mqadmin_ext.examine_broker_cluster_info().await?;
                    let broker_name = CommandUtil::fetch_broker_name_by_addr(&cluster_info, broker_addr)?;
                    let order_conf = format!("{}:{}", broker_name, topic_config.write_queue_nums);
                    default_mqadmin_ext.create_or_update_order_conf(
                        self.topic.clone().into(),
                        order_conf.clone().into(),
                        false,
                    ).await?;
                    */
                }

                println!("create topic to {} success.", broker_addr);
                println!("{:?}", topic_config);
            } else if let Some(cluster_name) = &self.cluster_name {
                println!("Creating/updating topic '{}' on cluster: {}", self.topic, cluster_name);

                let cluster_info = default_mqadmin_ext.examine_broker_cluster_info().await.map_err(|e| {
                    RocketMQError::Internal(format!("UpdateTopicSubCommand: Failed to get cluster info: {}", e))
                })?;

                let master_addrs = CommandUtil::fetch_master_addr_by_cluster_name(&cluster_info, cluster_name)?;

                if master_addrs.is_empty() {
                    return Err(RocketMQError::Internal(format!(
                        "UpdateTopicSubCommand: No master brokers found in cluster: {}",
                        cluster_name
                    )));
                }

                for addr in &master_addrs {
                    default_mqadmin_ext
                        .create_and_update_topic_config(addr.clone(), topic_config.clone())
                        .await
                        .map_err(|e| {
                            RocketMQError::Internal(format!(
                                "UpdateTopicSubCommand: Failed to create/update topic on {}: {}",
                                addr, e
                            ))
                        })?;
                    println!("create topic to {} success.", addr);
                }

                if is_order {
                    // Note: Order message configuration is not yet fully implemented in
                    // rocketmq-rust. The create_or_update_order_conf method is
                    // still unimplemented. For now, we only create the topic
                    // with order=true flag on all brokers. Full order
                    // configuration will be added when the underlying API is implemented.

                    println!(
                        "Warning: Order message topic created on cluster, but order configuration (queue allocation) \
                         is not yet implemented."
                    );
                    println!(
                        "The topic will be marked as ordered (order=true) on all brokers, but you may need to \
                         manually configure queue allocation."
                    );

                    // TODO: Uncomment when create_or_update_order_conf is implemented:
                    /*
                    let broker_names = CommandUtil::fetch_broker_name_by_cluster_name(&cluster_info, cluster_name)?;
                    let order_conf = broker_names
                        .iter()
                        .map(|name| format!("{}:{}", name, topic_config.write_queue_nums))
                        .collect::<Vec<_>>()
                        .join(";");
                    default_mqadmin_ext.create_or_update_order_conf(
                        self.topic.clone().into(),
                        order_conf.clone().into(),
                        true,
                    ).await?;
                    */
                }

                println!("{:?}", topic_config);
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

    #[test]
    fn update_topic_sub_command_parse_broker_mode() {
        let args = vec![
            "update-topic",
            "-t",
            "test-topic",
            "-b",
            "127.0.0.1:10911",
            "-r",
            "8",
            "-w",
            "8",
            "-p",
            "6",
        ];

        let cmd = UpdateTopicSubCommand::try_parse_from(args);
        assert!(cmd.is_ok());

        let cmd = cmd.unwrap();
        assert_eq!(cmd.topic, "test-topic");
        assert_eq!(cmd.broker_addr, Some("127.0.0.1:10911".to_string()));
        assert_eq!(cmd.read_queue_nums, 8);
        assert_eq!(cmd.write_queue_nums, 8);
        assert_eq!(cmd.perm, Some("6".to_string()));
    }

    #[test]
    fn cluster_mode() {
        let args = vec!["update-topic", "-t", "test-topic", "-c", "DefaultCluster"];

        let cmd = UpdateTopicSubCommand::try_parse_from(args);
        assert!(cmd.is_ok());

        let cmd = cmd.unwrap();
        assert_eq!(cmd.cluster_name, Some("DefaultCluster".to_string()));
        assert!(cmd.broker_addr.is_none());
    }

    #[test]
    fn conflicting_args() {
        let args = vec![
            "update-topic",
            "-t",
            "test-topic",
            "-b",
            "127.0.0.1:10911",
            "-c",
            "DefaultCluster",
        ];

        let cmd = UpdateTopicSubCommand::try_parse_from(args);
        assert!(cmd.is_err());
    }
}
