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
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use rocketmq_admin_core::admin::default_mq_admin_ext::DefaultMQAdminExt;

#[derive(Debug, Clone, Parser)]
pub struct CloneGroupOffsetSubCommand {
    #[arg(short = 's', long = "srcGroup", required = true, help = "set source consumer group")]
    src_group: String,

    #[arg(
        short = 'd',
        long = "destGroup",
        required = true,
        help = "set destination consumer group"
    )]
    dest_group: String,

    #[arg(short = 't', long = "topic", required = true, help = "set the topic")]
    topic: String,

    #[arg(
        short = 'o',
        long = "offline",
        required = false,
        help = "the group or the topic is offline"
    )]
    offline: bool,
}

impl CommandExecute for CloneGroupOffsetSubCommand {
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
                RocketMQError::Internal(format!("CloneGroupOffsetSubCommand: Failed to start MQAdminExt: {}", e))
            })?;

            let src_group = self.src_group.trim();
            let dest_group = self.dest_group.trim();
            let topic = self.topic.trim();

            let consume_stats = default_mqadmin_ext
                .examine_consume_stats(src_group.into(), None, None, None, None)
                .await?;

            let mqs = consume_stats.offset_table.keys().cloned().collect::<Vec<_>>();
            if !mqs.is_empty() {
                let topic_route = default_mqadmin_ext
                    .examine_topic_route_info(topic.into())
                    .await?
                    .ok_or_else(|| {
                        RocketMQError::Internal(format!(
                            "CloneGroupOffsetSubCommand: Topic route not found for: {}",
                            topic
                        ))
                    })?;

                for mq in &mqs {
                    let mut addr = None;
                    for broker_data in &topic_route.broker_datas {
                        if broker_data.broker_name() == mq.broker_name() {
                            addr = broker_data.select_broker_addr();
                            break;
                        }
                    }

                    let offset = consume_stats.offset_table[mq].get_consumer_offset();
                    if offset >= 0 {
                        if let Some(broker_addr) = addr {
                            default_mqadmin_ext
                                .update_consume_offset(broker_addr, dest_group.into(), mq.clone(), offset as u64)
                                .await?;
                        }
                    }
                }
            }

            println!(
                "clone group offset success. srcGroup[{}], destGroup=[{}], topic[{}]",
                src_group, dest_group, topic
            );
            Ok(())
        }
        .await;

        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;
        operation_result
    }
}
