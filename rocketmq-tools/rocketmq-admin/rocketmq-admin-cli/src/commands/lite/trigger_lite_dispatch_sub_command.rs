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
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use rocketmq_admin_core::admin::default_mq_admin_ext::DefaultMQAdminExt;

#[derive(Debug, Clone, Parser)]
pub struct TriggerLiteDispatchSubCommand {
    #[arg(short = 'p', long = "parentTopic", required = true, help = "Parent topic name")]
    parent_topic: String,

    #[arg(short = 'g', long = "group", required = true, help = "Consumer group")]
    group: String,

    #[arg(short = 'c', long = "clientId", required = false, help = "clientId (optional)")]
    client_id: Option<String>,

    #[arg(short = 'b', long = "brokerName", required = false, help = "brokerName (optional)")]
    broker_name: Option<String>,
}

impl CommandExecute for TriggerLiteDispatchSubCommand {
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
                    "TriggerLiteDispatchSubCommand: Failed to start MQAdminExt: {}",
                    e
                ))
            })?;

            let parent_topic = self.parent_topic.trim();
            let group = self.group.trim();
            let client_id = self.client_id.as_deref().map(|s| s.trim());
            let broker_name = self.broker_name.as_deref().map(|s| s.trim());

            let topic_route_data = default_mqadmin_ext
                .examine_topic_route_info(CheetahString::from(parent_topic))
                .await?;

            println!("Group And Topic Info: [{}] [{}]\n", group, parent_topic);

            if let Some(topic_route_data) = topic_route_data {
                for broker_data in &topic_route_data.broker_datas {
                    let broker_addr = match broker_data.select_broker_addr() {
                        Some(addr) => addr,
                        None => continue,
                    };

                    if let Some(bn) = broker_name {
                        if bn != broker_data.broker_name().as_str() {
                            continue;
                        }
                    }

                    let client_id_param = client_id.map(CheetahString::from).unwrap_or_default();

                    let success = default_mqadmin_ext
                        .trigger_lite_dispatch(broker_addr, CheetahString::from(group), client_id_param)
                        .await
                        .is_ok();

                    println!(
                        "{:<30} {:<12}",
                        broker_data.broker_name(),
                        if success { "dispatched" } else { "error" }
                    );
                }
            }

            Ok(())
        }
        .await;

        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;
        operation_result
    }
}
