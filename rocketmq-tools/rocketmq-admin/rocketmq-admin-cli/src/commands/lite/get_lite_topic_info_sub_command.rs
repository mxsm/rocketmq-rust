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

use cheetah_string::CheetahString;
use clap::Parser;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_common::UtilAll::time_millis_to_human_string2;
use rocketmq_common::common::lite;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use rocketmq_admin_core::admin::default_mq_admin_ext::DefaultMQAdminExt;

#[derive(Debug, Clone, Parser)]
pub struct GetLiteTopicInfoSubCommand {
    #[arg(short = 'p', long = "parentTopic", required = true, help = "Parent topic name")]
    parent_topic: String,

    #[arg(short = 'l', long = "liteTopic", required = true, help = "Lite topic name")]
    lite_topic: String,

    #[arg(short = 's', long = "showClientId", help = "Show all clientId")]
    show_client_id: bool,
}

impl CommandExecute for GetLiteTopicInfoSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let mut default_mqadmin_ext = if let Some(rpc_hook) = rpc_hook {
            DefaultMQAdminExt::with_rpc_hook(rpc_hook)
        } else {
            DefaultMQAdminExt::new()
        };

        default_mqadmin_ext
            .client_config_mut()
            .set_instance_name(current_millis().to_string().into());

        let operation_result = async {
            MQAdminExt::start(&mut default_mqadmin_ext).await.map_err(|e| {
                RocketMQError::Internal(format!("GetLiteTopicInfoSubCommand: Failed to start MQAdminExt: {}", e))
            })?;

            let parent_topic = self.parent_topic.trim();
            let lite_topic = self.lite_topic.trim();
            let show_client_id = self.show_client_id;

            let lmq_name = lite::to_lmq_name(parent_topic, lite_topic).unwrap_or_default();

            let topic_route_data = default_mqadmin_ext
                .examine_topic_route_info(CheetahString::from(parent_topic))
                .await
                .map_err(|e| {
                    RocketMQError::Internal(format!(
                        "GetLiteTopicInfoSubCommand: Failed to examine topic route info: {}",
                        e
                    ))
                })?
                .ok_or_else(|| {
                    RocketMQError::Internal(format!(
                        "GetLiteTopicInfoSubCommand: Topic route not found for: {}",
                        parent_topic
                    ))
                })?;

            println!("Lite Topic Info: [{}] [{}] [{}]", parent_topic, lite_topic, lmq_name);
            println!(
                "{:<50} {:<14} {:<14} {:<30} {:<12} {:<18}",
                "#Broker Name", "#MinOffset", "#MaxOffset", "#LastUpdate", "#Sharding", "#SubClientCount"
            );

            for broker_data in &topic_route_data.broker_datas {
                let broker_addr = match broker_data.select_broker_addr() {
                    Some(addr) => addr,
                    None => continue,
                };

                match default_mqadmin_ext
                    .get_lite_topic_info(
                        broker_addr.clone(),
                        CheetahString::from(parent_topic),
                        CheetahString::from(lite_topic),
                    )
                    .await
                {
                    Ok(body) => {
                        let (min_offset, max_offset, last_update) = if let Some(topic_offset) = body.topic_offset() {
                            (
                                topic_offset.get_min_offset(),
                                topic_offset.get_max_offset(),
                                topic_offset.get_last_update_timestamp(),
                            )
                        } else {
                            (0, 0, 0)
                        };

                        let last_update_str = if last_update > 0 {
                            time_millis_to_human_string2(last_update)
                        } else {
                            "-".to_string()
                        };

                        let broker_name = broker_data.broker_name();
                        let display_name: String = if broker_name.chars().count() > 40 {
                            broker_name.chars().take(40).collect()
                        } else {
                            broker_name.to_string()
                        };

                        println!(
                            "{:<50} {:<14} {:<14} {:<30} {:<12} {:<18}",
                            display_name,
                            min_offset,
                            max_offset,
                            last_update_str,
                            body.sharding_to_broker(),
                            body.subscriber().len()
                        );

                        if show_client_id {
                            let display_list: Vec<String> = body
                                .subscriber()
                                .iter()
                                .map(|client_group| format!("{}@{}", client_group.client_id, client_group.group))
                                .collect();
                            println!("{:?}", display_list);
                        }
                    }
                    Err(e) => {
                        println!("[{}] error: {}", broker_data.broker_name(), e);
                    }
                }
            }

            Ok(())
        }
        .await;

        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;
        operation_result
    }
}
