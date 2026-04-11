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
pub struct GetConsumerStatusSubCommand {
    #[arg(short = 'g', long = "group", required = true, help = "set the consumer group")]
    group: String,

    #[arg(short = 't', long = "topic", required = true, help = "set the topic")]
    topic: String,

    #[arg(
        short = 'i',
        long = "originClientId",
        required = false,
        help = "set the consumer clientId"
    )]
    origin_client_id: Option<String>,
}

impl CommandExecute for GetConsumerStatusSubCommand {
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
                    "GetConsumerStatusSubCommand: Failed to start MQAdminExt: {}",
                    e
                ))
            })?;

            let group = self.group.trim();
            let topic = self.topic.trim();
            let origin_client_id = self.origin_client_id.as_deref().map(|s| s.trim()).unwrap_or("");

            let consumer_status_table = default_mqadmin_ext
                .get_consume_status(topic.into(), group.into(), origin_client_id.into())
                .await?;

            println!(
                "get consumer status from client. group={}, topic={}, originClientId={}",
                group, topic, origin_client_id
            );

            println!(
                "{:<50}  {:<15}  {:<15}  {:<20}",
                "#clientId", "#brokerName", "#queueId", "#offset"
            );

            for (client_id, mq_table) in &consumer_status_table {
                let mut entries: Vec<_> = mq_table.iter().collect();
                entries.sort_by(|(a, _), (b, _)| {
                    a.broker_name()
                        .cmp(b.broker_name())
                        .then_with(|| a.queue_id().cmp(&b.queue_id()))
                });

                for (mq, offset) in entries {
                    println!(
                        "{:<50}  {:<15}  {:<15}  {:<20}",
                        client_id,
                        mq.broker_name(),
                        mq.queue_id(),
                        offset,
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
