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
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use crate::commands::CommonArgs;
use rocketmq_admin_core::admin::default_mq_admin_ext::DefaultMQAdminExt;

const SKIP_TO_LATEST_TIMESTAMP: u64 = u64::MAX;

#[derive(Debug, Clone, Parser)]
pub struct SkipAccumulatedMessageSubCommand {
    #[command(flatten)]
    common_args: CommonArgs,

    #[arg(short = 'g', long = "group", required = true, help = "set the consumer group")]
    group: String,

    #[arg(short = 't', long = "topic", required = true, help = "set the topic")]
    topic: String,

    #[arg(
        short = 'f',
        long = "force",
        required = false,
        help = "set the force rollback by timestamp switch[true|false]"
    )]
    force: Option<bool>,

    #[arg(
        short = 'c',
        long = "cluster",
        required = false,
        help = "Cluster name or lmq parent topic, lmq is used to find the route."
    )]
    cluster: Option<String>,
}

impl CommandExecute for SkipAccumulatedMessageSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        if self.group.trim().is_empty() {
            return Err(RocketMQError::IllegalArgument(
                "Consumer group name (--group / -g) cannot be empty".into(),
            ));
        }

        if self.topic.trim().is_empty() {
            return Err(RocketMQError::IllegalArgument(
                "Topic name (--topic / -t) cannot be empty".into(),
            ));
        }

        let mut admin = if let Some(rpc_hook) = rpc_hook {
            DefaultMQAdminExt::with_rpc_hook(rpc_hook)
        } else {
            DefaultMQAdminExt::new()
        };
        admin
            .client_config_mut()
            .set_instance_name(current_millis().to_string().into());

        if let Some(addr) = &self.common_args.namesrv_addr {
            admin.set_namesrv_addr(addr.trim());
        }

        let operation_result = async {
            let group = self.group.trim();
            let topic = self.topic.trim();
            let cluster = self.cluster.as_deref().map(str::trim).filter(|s| !s.is_empty());
            let force = self.force.unwrap_or(true);

            admin.start().await.map_err(|e| {
                RocketMQError::Internal(format!(
                    "SkipAccumulatedMessageSubCommand: Failed to start MQAdminExt: {e}"
                ))
            })?;

            let offset_table = match admin
                .reset_offset_by_timestamp(
                    cluster.map(|name| name.into()),
                    topic.into(),
                    group.into(),
                    SKIP_TO_LATEST_TIMESTAMP,
                    force,
                )
                .await
            {
                Ok(offset_table) => offset_table,
                Err(err) => {
                    if matches!(
                        err,
                        RocketMQError::BrokerOperationFailed { code, .. }
                            if ResponseCode::from(code) == ResponseCode::ConsumerNotOnline
                    ) {
                        let rollback_stats = admin
                            .reset_offset_by_timestamp_old(
                                cluster.map(|name| name.into()),
                                group.into(),
                                topic.into(),
                                SKIP_TO_LATEST_TIMESTAMP,
                                force,
                            )
                            .await?;

                        println!(
                            "{:<20}  {:<20}  {:<20}  {:<20}  {:<20}  {:<20}",
                            "#brokerName",
                            "#queueId",
                            "#brokerOffset",
                            "#consumerOffset",
                            "#timestampOffset",
                            "#rollbackOffset"
                        );

                        for stat in rollback_stats {
                            println!(
                                "{:<20}  {:<20}  {:<20}  {:<20}  {:<20}  {:<20}",
                                stat.broker_name,
                                stat.queue_id,
                                stat.broker_offset,
                                stat.consumer_offset,
                                stat.timestamp_offset,
                                stat.rollback_offset
                            );
                        }
                        return Ok(());
                    }
                    return Err(err);
                }
            };

            println!("{:<40}  {:<40}  {:<40}", "#brokerName", "#queueId", "#offset");
            for (mq, offset) in offset_table {
                println!("{:<40}  {:<40}  {:<40}", mq.broker_name(), mq.queue_id(), offset);
            }

            Ok(())
        }
        .await;

        admin.shutdown().await;
        operation_result
    }
}
