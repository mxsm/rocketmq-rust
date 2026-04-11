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
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_common::UtilAll::time_millis_to_human_string2;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::body::get_lite_client_info_response_body::GetLiteClientInfoResponseBody;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use rocketmq_admin_core::admin::default_mq_admin_ext::DefaultMQAdminExt;

#[derive(Debug, Clone, Parser)]
pub struct GetLiteClientInfoSubCommand {
    #[arg(short = 'p', long = "parentTopic", required = true, help = "Parent topic name")]
    parent_topic: String,

    #[arg(short = 'g', long = "group", required = true, help = "Consumer group")]
    group: String,

    #[arg(short = 'c', long = "clientId", required = true, help = "Client id")]
    client_id: String,

    #[arg(short = 's', long = "showDetail", help = "Show details")]
    show_detail: bool,
}

impl CommandExecute for GetLiteClientInfoSubCommand {
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
                RocketMQError::Internal(format!(
                    "GetLiteClientInfoSubCommand: Failed to start MQAdminExt: {}",
                    e
                ))
            })?;

            let parent_topic = self.parent_topic.trim();
            let group = self.group.trim();
            let client_id = self.client_id.trim();
            let show_detail = self.show_detail;

            let topic_route_data = default_mqadmin_ext
                .examine_topic_route_info(parent_topic.into())
                .await
                .map_err(|e| {
                    RocketMQError::Internal(format!(
                        "GetLiteClientInfoSubCommand: Failed to examine topic route info: {}",
                        e
                    ))
                })?;

            println!("Lite Client Info: [{}] [{}] [{}]", parent_topic, group, client_id);
            Self::print_header();

            if let Some(route_data) = topic_route_data {
                for broker_data in &route_data.broker_datas {
                    let broker_addr = broker_data.select_broker_addr();
                    let broker_name = broker_data.broker_name();

                    if let Some(addr) = broker_addr {
                        match default_mqadmin_ext
                            .get_lite_client_info(addr, parent_topic.into(), group.into(), client_id.into())
                            .await
                        {
                            Ok(body) => {
                                Self::print_row(&body, broker_name.as_str(), show_detail);
                            }
                            Err(_) => {
                                println!("[{}] error.", broker_name);
                            }
                        }
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

impl GetLiteClientInfoSubCommand {
    fn print_header() {
        println!(
            "{:<30} {:<20} {:<30} {:<30}",
            "#Broker", "#LiteTopicCount", "#LastAccessTime", "#LastConsumeTime"
        );
    }

    fn print_row(response_body: &GetLiteClientInfoResponseBody, broker_name: &str, show_detail: bool) {
        let lite_topic_count = if response_body.lite_topic_count() > 0 {
            response_body.lite_topic_count().to_string()
        } else {
            "N/A".to_string()
        };

        let last_access_time = if response_body.last_access_time() > 0 {
            time_millis_to_human_string2(response_body.last_access_time() as i64)
        } else {
            "N/A".to_string()
        };

        let last_consume_time = if response_body.last_consume_time() > 0 {
            time_millis_to_human_string2(response_body.last_consume_time() as i64)
        } else {
            "N/A".to_string()
        };

        println!(
            "{:<30} {:<20} {:<30} {:<30}",
            broker_name, lite_topic_count, last_access_time, last_consume_time
        );

        if show_detail && !response_body.lite_topic_set().is_empty() {
            println!("Lite Topics: {:?}\n", response_body.lite_topic_set());
        }
    }
}
