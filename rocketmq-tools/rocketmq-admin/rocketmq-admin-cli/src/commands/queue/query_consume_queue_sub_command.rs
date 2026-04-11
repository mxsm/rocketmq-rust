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
pub struct QueryCqSubCommand {
    #[arg(short = 't', long = "topic", required = true, help = "topic name")]
    topic: String,

    #[arg(short = 'q', long = "queue", required = true, help = "queue num, ie. 1")]
    queue_id: i32,

    #[arg(short = 'i', long = "index", required = true, help = "start queue index")]
    index: u64,

    #[arg(
        short = 'c',
        long = "count",
        required = false,
        default_value = "10",
        help = "how many"
    )]
    count: i32,

    #[arg(short = 'b', long = "broker", required = false, help = "broker addr")]
    broker: Option<String>,

    #[arg(short = 'g', long = "consumer", required = false, help = "consumer group")]
    consumer_group: Option<String>,
}

impl CommandExecute for QueryCqSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let mut default_mqadmin_ext = if let Some(rpc_hook) = rpc_hook {
            DefaultMQAdminExt::with_rpc_hook(rpc_hook)
        } else {
            DefaultMQAdminExt::new()
        };

        default_mqadmin_ext
            .client_config_mut()
            .set_instance_name(current_millis().to_string().into());

        let topic = self.topic.trim().to_string();
        let queue_id = self.queue_id;
        let index = self.index;
        let count = self.count;
        let consumer_group = self
            .consumer_group
            .as_deref()
            .map(|s| s.trim().to_string())
            .unwrap_or_default();

        let operation_result = async {
            MQAdminExt::start(&mut default_mqadmin_ext).await.map_err(|e| {
                RocketMQError::Internal(format!("QueryCqSubCommand: Failed to start MQAdminExt: {}", e))
            })?;

            let broker_addr = if let Some(ref broker) = self.broker {
                CheetahString::from_string(broker.trim().to_string())
            } else {
                let topic_route_data = default_mqadmin_ext
                    .examine_topic_route_info(CheetahString::from_string(topic.clone()))
                    .await
                    .map_err(|e| {
                        RocketMQError::Internal(format!("QueryCqSubCommand: Failed to examine topic route info: {}", e))
                    })?;

                let topic_route_data =
                    topic_route_data.ok_or_else(|| RocketMQError::Internal("No topic route data!".into()))?;

                if topic_route_data.broker_datas.is_empty() {
                    return Err(RocketMQError::Internal(
                        "No topic route data! broker_datas is empty".into(),
                    ));
                }

                let broker_data = &topic_route_data.broker_datas[0];
                broker_data.broker_addrs().get(&0u64).cloned().ok_or_else(|| {
                    RocketMQError::Internal("No master broker address found in topic route data".into())
                })?
            };

            let response_body = default_mqadmin_ext
                .query_consume_queue(
                    broker_addr,
                    CheetahString::from_string(topic),
                    queue_id,
                    index,
                    count,
                    CheetahString::from_string(consumer_group),
                )
                .await
                .map_err(|e| {
                    RocketMQError::Internal(format!("QueryCqSubCommand: Failed to query consume queue: {}", e))
                })?;

            if let Some(ref subscription_data) = response_body.subscription_data {
                println!("Subscription data:");
                match serde_json::to_string_pretty(subscription_data) {
                    Ok(json) => println!("{}", json),
                    Err(_) => println!("{:?}", subscription_data),
                }
                println!("======================================");
            }

            if let Some(ref filter_data) = response_body.filter_data {
                if !filter_data.is_empty() {
                    println!("Filter data:");
                    println!("{}", filter_data);
                    println!("======================================");
                }
            }

            println!("Queue data:");
            println!(
                "max: {}, min: {}",
                response_body.max_queue_index, response_body.min_queue_index
            );
            println!("======================================");

            if let Some(ref queue_data) = response_body.queue_data {
                for (offset, data) in queue_data.iter().enumerate() {
                    println!("idx: {}", index + offset as u64);
                    match serde_json::to_string_pretty(data) {
                        Ok(json) => println!("{}", json),
                        Err(_) => println!("{}", data),
                    }
                    println!("======================================");
                }
            }

            Ok(())
        }
        .await;

        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;
        operation_result
    }
}
