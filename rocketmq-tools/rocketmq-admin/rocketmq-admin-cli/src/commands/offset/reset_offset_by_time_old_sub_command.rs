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
use rocketmq_common::UtilAll::YYYY_MM_DD_HH_MM_SS_SSS;
use rocketmq_common::UtilAll::parse_date;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use crate::commands::CommonArgs;
use rocketmq_admin_core::admin::default_mq_admin_ext::DefaultMQAdminExt;

#[derive(Debug, Clone, Parser)]
pub struct ResetOffsetByTimeOldSubCommand {
    #[command(flatten)]
    common_args: CommonArgs,

    #[arg(short = 'g', long = "group", required = true, help = "set the consumer group")]
    group: String,

    #[arg(short = 't', long = "topic", required = true, help = "set the topic")]
    topic: String,

    #[arg(
        short = 's',
        long = "timestamp",
        required = true,
        help = "set the timestamp[currentTimeMillis|yyyy-MM-dd#HH:mm:ss:SSS]"
    )]
    timestamp: String,

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

impl ResetOffsetByTimeOldSubCommand {
    async fn reset_offset(
        admin: &DefaultMQAdminExt,
        cluster_name: Option<&str>,
        consumer_group: &str,
        topic: &str,
        timestamp: u64,
        force: bool,
        timestamp_str: &str,
    ) -> RocketMQResult<()> {
        let rollback_stats_list = admin
            .reset_offset_by_timestamp_old(
                cluster_name.map(|value| value.into()),
                consumer_group.into(),
                topic.into(),
                timestamp,
                force,
            )
            .await?;

        println!(
            "reset consumer offset by specified consumerGroup[{}], topic[{}], force[{}], timestamp(string)[{}], \
             timestamp(long)[{}]",
            consumer_group, topic, force, timestamp_str, timestamp
        );
        println!(
            "{:<20}  {:<20}  {:<20}  {:<20}  {:<20}  {:<20}",
            "#brokerName", "#queueId", "#brokerOffset", "#consumerOffset", "#timestampOffset", "#resetOffset"
        );

        for rollback_stats in rollback_stats_list {
            let broker_name = rollback_stats.broker_name.to_string();
            let broker_name = if broker_name.chars().count() > 32 {
                broker_name.chars().take(32).collect::<String>()
            } else {
                broker_name
            };

            println!(
                "{:<20}  {:<20}  {:<20}  {:<20}  {:<20}  {:<20}",
                broker_name,
                rollback_stats.queue_id,
                rollback_stats.broker_offset,
                rollback_stats.consumer_offset,
                rollback_stats.timestamp_offset,
                rollback_stats.rollback_offset
            );
        }

        Ok(())
    }
}

impl CommandExecute for ResetOffsetByTimeOldSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let consumer_group = self.group.trim();
        let topic = self.topic.trim();
        let timestamp_str = self.timestamp.trim();
        let cluster_name = self.cluster.as_deref().map(str::trim).filter(|value| !value.is_empty());
        let force = self.force.unwrap_or(true);

        let timestamp = match timestamp_str.parse::<u64>() {
            Ok(timestamp) => timestamp,
            Err(_) => {
                if let Some(date) = parse_date(timestamp_str, YYYY_MM_DD_HH_MM_SS_SSS) {
                    let millis = date.and_utc().timestamp_millis();
                    if millis < 0 {
                        println!("specified timestamp invalid.");
                        return Ok(());
                    }
                    millis as u64
                } else {
                    println!("specified timestamp invalid.");
                    return Ok(());
                }
            }
        };

        let mut default_mqadmin_ext = if let Some(rpc_hook) = rpc_hook {
            DefaultMQAdminExt::with_rpc_hook(rpc_hook)
        } else {
            DefaultMQAdminExt::new()
        };
        default_mqadmin_ext
            .client_config_mut()
            .set_instance_name(current_millis().to_string().into());

        if let Some(namesrv_addr) = &self.common_args.namesrv_addr {
            default_mqadmin_ext.set_namesrv_addr(namesrv_addr.trim());
        }

        let operation_result = async {
            MQAdminExt::start(&mut default_mqadmin_ext).await.map_err(|e| {
                RocketMQError::Internal(format!(
                    "ResetOffsetByTimeOldSubCommand: Failed to start MQAdminExt: {}",
                    e
                ))
            })?;

            Self::reset_offset(
                &default_mqadmin_ext,
                cluster_name,
                consumer_group,
                topic,
                timestamp,
                force,
                timestamp_str,
            )
            .await
        }
        .await;

        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;
        operation_result
    }
}
