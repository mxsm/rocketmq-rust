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
use rocketmq_common::utils::util_all::time_millis_to_human_string2;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::admin::offset_wrapper::OffsetWrapper;
use rocketmq_remoting::protocol::body::lite_lag_info::LiteLagInfo;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use rocketmq_admin_core::admin::default_mq_admin_ext::DefaultMQAdminExt;

#[derive(Debug, Clone, Parser)]
pub struct GetLiteGroupInfoSubCommand {
    #[arg(short = 'p', long = "parentTopic", required = true, help = "Parent topic name")]
    parent_topic: String,

    #[arg(short = 'g', long = "group", required = true, help = "Consumer group")]
    group: String,

    #[arg(short = 'l', long = "liteTopic", required = false, help = "Query lite topic detail")]
    lite_topic: Option<String>,

    #[arg(short = 'k', long = "topK", required = false, help = "TopK value of each broker")]
    top_k: Option<i32>,
}

impl CommandExecute for GetLiteGroupInfoSubCommand {
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
                RocketMQError::Internal(format!("GetLiteGroupInfoSubCommand: Failed to start MQAdminExt: {}", e))
            })?;

            let parent_topic = self.parent_topic.trim();
            let group = self.group.trim();
            let top_k = self.top_k.unwrap_or(20);
            let lite_topic = self.lite_topic.as_deref().map(|s| s.trim().to_string());
            let query_by_lite_topic = lite_topic.as_ref().is_some_and(|s| !s.is_empty());

            let topic_route_data = default_mqadmin_ext
                .examine_topic_route_info(CheetahString::from(parent_topic))
                .await
                .map_err(|e| {
                    RocketMQError::Internal(format!(
                        "GetLiteGroupInfoSubCommand: Failed to examine topic route info: {}",
                        e
                    ))
                })?;

            let topic_route_data = topic_route_data.ok_or_else(|| {
                RocketMQError::Internal(format!(
                    "GetLiteGroupInfoSubCommand: Topic route not found for: {}",
                    parent_topic
                ))
            })?;

            println!("Lite Group Info: [{}] [{}]", group, parent_topic);

            let mut total_lag_count: i64 = 0;
            let mut earliest_unconsumed_timestamp: i64 = current_millis() as i64;
            let mut lag_count_top_k: Vec<LiteLagInfo> = Vec::new();
            let mut lag_timestamp_top_k: Vec<LiteLagInfo> = Vec::new();

            if query_by_lite_topic {
                println!(
                    "{:<50} {:<16} {:<16} {:<16} {:<30}",
                    "#Broker Name", "#BrokerOffset", "#ConsumeOffset", "#LagCount", "#LastUpdate"
                );
            }

            for broker_data in &topic_route_data.broker_datas {
                let broker_addr = match broker_data.select_broker_addr() {
                    Some(addr) => addr,
                    None => continue,
                };

                let lite_topic_str = lite_topic.as_deref().map(CheetahString::from).unwrap_or_default();

                match default_mqadmin_ext
                    .get_lite_group_info(broker_addr.clone(), CheetahString::from(group), lite_topic_str, top_k)
                    .await
                {
                    Ok(body) => {
                        if body.total_lag_count() > 0 {
                            total_lag_count += body.total_lag_count();
                        }
                        if body.earliest_unconsumed_timestamp() > 0 {
                            earliest_unconsumed_timestamp =
                                earliest_unconsumed_timestamp.min(body.earliest_unconsumed_timestamp());
                        }

                        Self::print_offset_wrapper(
                            query_by_lite_topic,
                            broker_data.broker_name(),
                            body.lite_topic_offset_wrapper(),
                        );

                        lag_count_top_k.extend_from_slice(body.lag_count_top_k());
                        lag_timestamp_top_k.extend_from_slice(body.lag_timestamp_top_k());
                    }
                    Err(_) => {
                        println!("[{}] error.", broker_data.broker_name());
                    }
                }
            }

            println!("Total Lag Count: {}", total_lag_count);
            let lag_time = current_millis() as i64 - earliest_unconsumed_timestamp;
            println!(
                "Min Unconsumed Timestamp: {} ({} s ago)\n",
                earliest_unconsumed_timestamp,
                lag_time / 1000
            );

            if query_by_lite_topic {
                return Ok(());
            }

            // Sort and print topK by lag count
            lag_count_top_k.sort_by_key(|b| std::cmp::Reverse(b.lag_count()));
            println!("------TopK by lag count-----");
            println!(
                "{:<6} {:<40} {:<12} {:<30}",
                "NO", "Lite Topic", "Lag Count", "UnconsumedTimestamp"
            );
            for (i, info) in lag_count_top_k.iter().enumerate() {
                let timestamp_str = if info.earliest_unconsumed_timestamp() > 0 {
                    time_millis_to_human_string2(info.earliest_unconsumed_timestamp())
                } else {
                    "-".to_string()
                };
                println!(
                    "{:<6} {:<40} {:<12} {:<30}",
                    i + 1,
                    info.lite_topic(),
                    info.lag_count(),
                    timestamp_str
                );
            }

            // Sort and print topK by lag time
            lag_timestamp_top_k.sort_by(|a, b| {
                a.earliest_unconsumed_timestamp()
                    .cmp(&b.earliest_unconsumed_timestamp())
            });
            println!("\n------TopK by lag time------");
            println!(
                "{:<6} {:<40} {:<12} {:<30}",
                "NO", "Lite Topic", "Lag Count", "UnconsumedTimestamp"
            );
            for (i, info) in lag_timestamp_top_k.iter().enumerate() {
                let timestamp_str = if info.earliest_unconsumed_timestamp() > 0 {
                    time_millis_to_human_string2(info.earliest_unconsumed_timestamp())
                } else {
                    "-".to_string()
                };
                println!(
                    "{:<6} {:<40} {:<12} {:<30}",
                    i + 1,
                    info.lite_topic(),
                    info.lag_count(),
                    timestamp_str
                );
            }

            Ok(())
        }
        .await;

        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;
        operation_result
    }
}

impl GetLiteGroupInfoSubCommand {
    fn print_offset_wrapper(
        query_by_lite_topic: bool,
        broker_name: &CheetahString,
        offset_wrapper: Option<&OffsetWrapper>,
    ) {
        if !query_by_lite_topic {
            return;
        }

        match offset_wrapper {
            None => {
                println!("{:<50} {:<16} {:<16} {:<16} {:<30}", broker_name, "-", "-", "-", "-");
            }
            Some(wrapper) => {
                let last_update_str = if wrapper.get_last_timestamp() > 0 {
                    time_millis_to_human_string2(wrapper.get_last_timestamp())
                } else {
                    "-".to_string()
                };
                println!(
                    "{:<50} {:<16} {:<16} {:<16} {:<30}",
                    broker_name,
                    wrapper.get_broker_offset(),
                    wrapper.get_consumer_offset(),
                    wrapper.get_broker_offset() - wrapper.get_consumer_offset(),
                    last_update_str
                );
            }
        }
    }
}
