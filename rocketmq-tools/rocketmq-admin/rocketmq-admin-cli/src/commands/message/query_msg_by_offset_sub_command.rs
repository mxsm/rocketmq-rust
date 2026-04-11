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
use rocketmq_client_rust::consumer::pull_status::PullStatus;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use rocketmq_admin_core::admin::default_mq_admin_ext::DefaultMQAdminExt;

const PULL_TIMEOUT_MILLIS: u64 = 3000;

#[derive(Debug, Clone, Parser)]
pub struct QueryMsgByOffsetSubCommand {
    #[arg(short = 't', long = "topic", required = true, help = "Topic name")]
    topic: String,

    #[arg(short = 'b', long = "brokerName", required = true, help = "Broker name")]
    broker_name: String,

    #[arg(short = 'i', long = "queueId", required = true, help = "Queue Id")]
    queue_id: i32,

    #[arg(short = 'o', long = "offset", required = true, help = "Queue Offset")]
    offset: i64,

    #[arg(
        short = 'f',
        long = "bodyFormat",
        required = false,
        help = "print message body by the specified format"
    )]
    body_format: Option<String>,

    #[arg(
        short = 'r',
        long = "routeTopic",
        required = false,
        help = "the topic which is used to find route info"
    )]
    route_topic: Option<String>,
}

impl CommandExecute for QueryMsgByOffsetSubCommand {
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
            MQAdminExt::start(&mut default_mqadmin_ext)
                .await
                .map_err(|e| RocketMQError::Internal(format!("Failed to start MQAdminExt: {}", e)))?;

            let topic = self.topic.trim();
            let broker_name = self.broker_name.trim();
            let queue_id = self.queue_id;
            let requested_offset = self.offset;
            let route_topic = self.route_topic.as_deref().map(str::trim).unwrap_or(topic);

            let topic_route = default_mqadmin_ext
                .examine_topic_route_info(CheetahString::from(route_topic))
                .await
                .map_err(|e| RocketMQError::Internal(format!("Failed to get topic route info: {}", e)))?
                .ok_or_else(|| RocketMQError::Internal(format!("Topic route not found for: {}", route_topic)))?;

            let broker_data = topic_route
                .broker_datas
                .iter()
                .find(|bd| bd.broker_name().as_str() == broker_name)
                .ok_or_else(|| {
                    RocketMQError::IllegalArgument(format!(
                        "Broker '{}' not found in topic route for '{}'",
                        broker_name, route_topic
                    ))
                })?;

            let broker_addr = broker_data
                .select_broker_addr()
                .ok_or_else(|| RocketMQError::Internal(format!("No available address for broker '{}'", broker_name)))?;

            if route_topic != topic {
                let route_mq = MessageQueue::from_parts(route_topic, broker_name, 0);
                default_mqadmin_ext
                    .pull_message_from_queue(broker_addr.as_str(), &route_mq, "*", 0, 1, PULL_TIMEOUT_MILLIS)
                    .await
                    .map_err(|e| RocketMQError::Internal(format!("Failed to warm up route topic pull: {}", e)))?;
            }

            let mq = MessageQueue::from_parts(topic, broker_name, queue_id);

            let pull_result = default_mqadmin_ext
                .pull_message_from_queue(broker_addr.as_str(), &mq, "*", requested_offset, 1, PULL_TIMEOUT_MILLIS)
                .await
                .map_err(|e| RocketMQError::Internal(format!("Failed to pull message by offset: {}", e)))?;

            match pull_result.pull_status() {
                PullStatus::Found => {
                    if let Some(msg_list) = pull_result.msg_found_list() {
                        if let Some(first_msg) = msg_list.first() {
                            let msg: &MessageExt = first_msg;
                            let body_format = self.body_format.as_deref().map(str::trim).unwrap_or("UTF-8");
                            let body = match msg.body() {
                                Some(bytes) => {
                                    if body_format.eq_ignore_ascii_case("UTF-8")
                                        || body_format.eq_ignore_ascii_case("UTF8")
                                    {
                                        String::from_utf8(bytes.to_vec()).unwrap_or_else(|_| "BINARY".to_string())
                                    } else if body_format.eq_ignore_ascii_case("ISO-8859-1")
                                        || body_format.eq_ignore_ascii_case("LATIN1")
                                        || body_format.eq_ignore_ascii_case("LATIN-1")
                                    {
                                        bytes.iter().map(|&b| b as char).collect()
                                    } else if body_format.eq_ignore_ascii_case("ASCII") {
                                        if bytes.is_ascii() {
                                            String::from_utf8(bytes.to_vec()).unwrap_or_else(|_| "BINARY".to_string())
                                        } else {
                                            return Err(RocketMQError::IllegalArgument(format!(
                                                "Unsupported body bytes for ASCII bodyFormat: {}",
                                                body_format
                                            )));
                                        }
                                    } else {
                                        return Err(RocketMQError::IllegalArgument(format!(
                                            "Unsupported bodyFormat: {}",
                                            body_format
                                        )));
                                    }
                                }
                                None => "EMPTY".to_string(),
                            };
                            println!("MSGID: {} {} BODY: {}", msg.msg_id(), msg, body);
                            return Ok(());
                        }
                    }
                }
                PullStatus::NoMatchedMsg | PullStatus::NoNewMsg | PullStatus::OffsetIllegal => {}
            }

            Ok(())
        }
        .await;

        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;
        operation_result
    }
}
