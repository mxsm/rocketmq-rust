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
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::body::get_parent_topic_info_response_body::GetParentTopicInfoResponseBody;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use rocketmq_admin_core::admin::default_mq_admin_ext::DefaultMQAdminExt;

#[derive(Debug, Clone, Parser)]
pub struct GetParentTopicInfoSubCommand {
    #[arg(short = 'p', long = "parentTopic", required = true, help = "Parent topic name")]
    parent_topic: String,
}

impl CommandExecute for GetParentTopicInfoSubCommand {
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
                    "GetParentTopicInfoSubCommand: Failed to start MQAdminExt: {}",
                    e
                ))
            })?;

            let parent_topic = self.parent_topic.trim();

            let topic_route_data = default_mqadmin_ext
                .examine_topic_route_info(CheetahString::from(parent_topic))
                .await
                .map_err(|e| {
                    RocketMQError::Internal(format!(
                        "GetParentTopicInfoSubCommand: Failed to examine topic route info: {}",
                        e
                    ))
                })?;

            let topic_route_data = topic_route_data.ok_or_else(|| {
                RocketMQError::Internal(format!(
                    "GetParentTopicInfoSubCommand: Topic route not found for: {}",
                    parent_topic
                ))
            })?;

            println!("Parent Topic Info: [{}]", parent_topic);
            Self::print_header();

            for broker_data in &topic_route_data.broker_datas {
                let broker_addr = match broker_data.select_broker_addr() {
                    Some(addr) => addr,
                    None => continue,
                };

                match default_mqadmin_ext
                    .get_parent_topic_info(broker_addr.clone(), CheetahString::from(parent_topic))
                    .await
                {
                    Ok(body) => {
                        Self::print_row(broker_data.broker_name(), &body);
                    }
                    Err(_) => {
                        println!("[{}] error.", broker_data.broker_name());
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

impl GetParentTopicInfoSubCommand {
    fn print_header() {
        println!(
            "{:<50} {:<8} {:<14} {:<14} {:<100}",
            "#Broker Name", "#TTL", "#Lite Count", "#LMQ NUM", "#GROUPS"
        );
    }

    fn print_row(broker_name: &CheetahString, body: &GetParentTopicInfoResponseBody) {
        let display_name = Self::front_string_at_least(broker_name.as_str(), 40);
        let groups_display = format!("{:?}", body.get_groups());
        println!(
            "{:<50} {:<8} {:<14} {:<14} {:<100}",
            display_name,
            body.get_ttl(),
            body.get_lite_topic_count(),
            body.get_lmq_num(),
            groups_display
        );
    }

    fn front_string_at_least(s: &str, size: usize) -> String {
        s.chars().take(size).collect()
    }
}
