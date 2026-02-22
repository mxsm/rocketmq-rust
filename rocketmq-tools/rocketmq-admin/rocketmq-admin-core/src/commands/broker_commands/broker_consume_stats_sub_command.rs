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
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::commands::CommandExecute;

#[derive(Debug, Clone, Parser)]
pub struct BrokerConsumeStatsSubCommand {
    #[arg(short = 'b', long = "brokerAddr", required = true, help = "Broker address")]
    broker_addr: String,

    #[arg(
        short = 't',
        long = "timeoutMillis",
        required = false,
        default_value = "50000",
        help = "request timeout Millis"
    )]
    timeout_millis: u64,

    #[arg(
        short = 'l',
        long = "level",
        required = false,
        default_value = "0",
        help = "threshold of print diff"
    )]
    diff_level: i64,

    #[arg(
        short = 'o',
        long = "order",
        required = false,
        default_value = "false",
        help = "order topic"
    )]
    is_order: String,
}

fn format_timestamp(timestamp: i64) -> String {
    if timestamp <= 0 {
        return "-".to_string();
    }
    let secs = timestamp / 1000;
    let nanos = ((timestamp % 1000) * 1_000_000) as u32;
    match chrono::DateTime::from_timestamp(secs, nanos) {
        Some(dt) => {
            use chrono::Local;
            let local_dt = dt.with_timezone(&Local);
            local_dt.format("%Y-%m-%d %H:%M:%S").to_string()
        }
        None => "-".to_string(),
    }
}

impl CommandExecute for BrokerConsumeStatsSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let mut default_mqadmin_ext = if let Some(rpc_hook) = rpc_hook {
            DefaultMQAdminExt::with_rpc_hook(rpc_hook)
        } else {
            DefaultMQAdminExt::new()
        };

        default_mqadmin_ext
            .client_config_mut()
            .set_instance_name(get_current_millis().to_string().into());

        let broker_addr = self.broker_addr.trim().to_string();
        let is_order = self.is_order.trim().parse::<bool>().unwrap_or(false);
        let timeout_millis = self.timeout_millis;
        let diff_level = self.diff_level;

        let operation_result = async {
            MQAdminExt::start(&mut default_mqadmin_ext).await.map_err(|e| {
                RocketMQError::Internal(format!(
                    "BrokerConsumeStatsSubCommand: Failed to start MQAdminExt: {}",
                    e
                ))
            })?;

            let consume_stats_list = default_mqadmin_ext
                .fetch_consume_stats_in_broker(CheetahString::from(broker_addr.as_str()), is_order, timeout_millis)
                .await
                .map_err(|e| {
                    RocketMQError::Internal(format!(
                        "BrokerConsumeStatsSubCommand: Failed to fetch consume stats: {}",
                        e
                    ))
                })?;

            println!(
                "{:<64}  {:<64}  {:<32}  {:<4}  {:<20}  {:<20}  {:<20}  #LastTime",
                "#Topic", "#Group", "#Broker Name", "#QID", "#Broker Offset", "#Consumer Offset", "#Diff"
            );

            for map in &consume_stats_list.consume_stats_list {
                for (group, consume_stats_array) in map {
                    for consume_stats in consume_stats_array {
                        let mut mq_list: Vec<_> = consume_stats.offset_table.keys().collect();
                        mq_list.sort();

                        for mq in &mq_list {
                            let offset_wrapper = &consume_stats.offset_table[*mq];
                            let diff = offset_wrapper.get_broker_offset() - offset_wrapper.get_consumer_offset();

                            if diff < diff_level {
                                continue;
                            }

                            if offset_wrapper.get_last_timestamp() > 0 {
                                let last_time = format_timestamp(offset_wrapper.get_last_timestamp());
                                println!(
                                    "{:<64}  {:<64}  {:<32}  {:<4}  {:<20}  {:<20}  {:<20}  {}",
                                    mq.get_topic(),
                                    group,
                                    mq.get_broker_name(),
                                    mq.get_queue_id(),
                                    offset_wrapper.get_broker_offset(),
                                    offset_wrapper.get_consumer_offset(),
                                    diff,
                                    last_time
                                );
                            }
                        }
                    }
                }
            }

            println!("\nDiff Total: {}", consume_stats_list.total_diff);

            Ok(())
        }
        .await;

        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;
        operation_result
    }
}
