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

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use cheetah_string::CheetahString;
use clap::Parser;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use rocketmq_admin_core::admin::default_mq_admin_ext::DefaultMQAdminExt;

#[derive(Debug, Clone, Parser)]
pub struct GetConsumerConfigSubCommand {
    #[arg(short = 'g', long = "groupName", required = true, help = "subscription group name")]
    group_name: String,
}

struct ConsumerConfigInfo {
    cluster_name: String,
    broker_name: String,
    subscription_group_config: SubscriptionGroupConfig,
}

impl CommandExecute for GetConsumerConfigSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let mut default_mqadmin_ext = if let Some(rpc_hook) = rpc_hook {
            DefaultMQAdminExt::with_rpc_hook(rpc_hook)
        } else {
            DefaultMQAdminExt::new()
        };

        default_mqadmin_ext
            .client_config_mut()
            .set_instance_name(current_millis().to_string().into());

        let group_name = self.group_name.trim().to_string();

        let operation_result = async {
            MQAdminExt::start(&mut default_mqadmin_ext).await.map_err(|e| {
                RocketMQError::Internal(format!(
                    "GetConsumerConfigSubCommand: Failed to start MQAdminExt: {}",
                    e
                ))
            })?;

            let mut consumer_config_info_list: Vec<ConsumerConfigInfo> = Vec::new();

            let cluster_info = default_mqadmin_ext.examine_broker_cluster_info().await.map_err(|e| {
                RocketMQError::Internal(format!(
                    "GetConsumerConfigSubCommand: Failed to examine broker cluster info: {}",
                    e
                ))
            })?;

            let broker_addr_table = cluster_info.broker_addr_table.unwrap_or_default();
            let cluster_addr_table = cluster_info.cluster_addr_table.unwrap_or_default();

            for (broker_name, broker_data) in &broker_addr_table {
                let cluster_name = get_cluster_name(broker_name, &cluster_addr_table);
                let broker_address = match broker_data.select_broker_addr() {
                    Some(addr) => addr,
                    None => continue,
                };

                match default_mqadmin_ext
                    .examine_subscription_group_config(broker_address, CheetahString::from(group_name.as_str()))
                    .await
                {
                    Ok(config) => {
                        consumer_config_info_list.push(ConsumerConfigInfo {
                            cluster_name: cluster_name.unwrap_or_default(),
                            broker_name: broker_name.to_string(),
                            subscription_group_config: config,
                        });
                    }
                    Err(_) => {
                        continue;
                    }
                }
            }

            if consumer_config_info_list.is_empty() {
                return Ok(());
            }

            for info in &consumer_config_info_list {
                println!(
                    "============================={}:{}=============================",
                    info.cluster_name, info.broker_name
                );
                let config = &info.subscription_group_config;
                print_field("groupName", config.group_name());
                print_field("consumeEnable", &config.consume_enable());
                print_field("consumeFromMinEnable", &config.consume_from_min_enable());
                print_field("consumeMessageOrderly", &config.consume_message_orderly());
                print_field("consumeBroadcastEnable", &config.consume_broadcast_enable());
                print_field("retryQueueNums", &config.retry_queue_nums());
                print_field("retryMaxTimes", &config.retry_max_times());
                print_field("brokerId", &config.broker_id());
                print_field(
                    "whichBrokerWhenConsumeSlowly",
                    &config.which_broker_when_consume_slowly(),
                );
                print_field(
                    "notifyConsumerIdsChangedEnable",
                    &config.notify_consumer_ids_changed_enable(),
                );
                print_field("groupSysFlag", &config.group_sys_flag());
                print_field("consumeTimeoutMinute", &config.consume_timeout_minute());
                println!();
            }

            Ok(())
        }
        .await;

        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;
        operation_result
    }
}

fn get_cluster_name(
    broker_name: &CheetahString,
    cluster_addr_table: &HashMap<CheetahString, HashSet<CheetahString>>,
) -> Option<String> {
    for (cluster_name, broker_name_set) in cluster_addr_table {
        if broker_name_set.contains(broker_name) {
            return Some(cluster_name.to_string());
        }
    }
    None
}

fn print_field(name: &str, value: &dyn std::fmt::Display) {
    println!("  {:<40}=  {}", name, value);
}
