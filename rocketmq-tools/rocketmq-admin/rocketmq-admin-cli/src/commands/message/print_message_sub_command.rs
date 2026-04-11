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

use std::collections::HashMap;
use std::sync::Arc;

use cheetah_string::CheetahString;
use clap::ArgAction;
use clap::Parser;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_client_rust::consumer::pull_status::PullStatus;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_common::UtilAll::YYYY_MM_DD_HH_MM_SS_SSS;
use rocketmq_common::UtilAll::parse_date;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use rocketmq_admin_core::admin::default_mq_admin_ext::DefaultMQAdminExt;

const PULL_BATCH_SIZE: i32 = 32;
const PULL_TIMEOUT_MILLIS: u64 = 3000;

#[derive(Debug, Clone, Parser)]
pub struct PrintMessageSubCommand {
    #[arg(short = 't', long = "topic", required = true, help = "topic name")]
    topic: String,

    #[arg(
        short = 'c',
        long = "charsetName",
        required = false,
        default_value = "UTF-8",
        help = "CharsetName(eg: UTF-8, GBK)"
    )]
    charset_name: String,

    #[arg(
        short = 's',
        long = "subExpression",
        required = false,
        default_value = "*",
        help = "Subscribe Expression(eg: TagA || TagB)"
    )]
    sub_expression: String,

    #[arg(
        short = 'b',
        long = "beginTimestamp",
        required = false,
        help = "Begin timestamp[currentTimeMillis|yyyy-MM-dd#HH:mm:ss:SSS]"
    )]
    begin_timestamp: Option<String>,

    #[arg(
        short = 'e',
        long = "endTimestamp",
        required = false,
        help = "End timestamp[currentTimeMillis|yyyy-MM-dd#HH:mm:ss:SSS]"
    )]
    end_timestamp: Option<String>,

    #[arg(
        short = 'd',
        long = "printBody",
        required = false,
        default_value_t = true,
        action = ArgAction::Set,
        help = "Print body. eg: true | false(default true)"
    )]
    print_body: bool,

    #[arg(
        short = 'l',
        long = "lmqParentTopic",
        required = false,
        help = "Lmq parent topic, lmq is used to find the route"
    )]
    lmq_parent_topic: Option<String>,
}

fn timestamp_format(value: &str) -> u64 {
    if let Ok(ts) = value.parse::<u64>() {
        return ts;
    }

    let date_str = value.replace('#', " ");
    if let Some(dt) = parse_date(&date_str, YYYY_MM_DD_HH_MM_SS_SSS) {
        return dt.and_utc().timestamp_millis() as u64;
    }

    0
}

fn print_message(msgs: &[rocketmq_rust::ArcMut<MessageExt>], _charset_name: &str, print_body: bool) {
    for msg in msgs {
        let body = if print_body {
            match msg.body() {
                Some(bytes) => String::from_utf8(bytes.to_vec()).unwrap_or_else(|_| "BINARY".to_string()),
                None => "EMPTY".to_string(),
            }
        } else {
            "NOT PRINT BODY".to_string()
        };

        let msg_ref: &MessageExt = msg;
        println!("MSGID: {} {} BODY: {}", msg.msg_id(), msg_ref, body);
    }
}

impl CommandExecute for PrintMessageSubCommand {
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
            let route_topic = self.lmq_parent_topic.as_deref().map(str::trim).unwrap_or(topic);
            let charset_name = self.charset_name.trim();
            let sub_expression = self.sub_expression.trim();
            let print_body = self.print_body;

            let topic_route = default_mqadmin_ext
                .examine_topic_route_info(CheetahString::from(route_topic))
                .await
                .map_err(|e| RocketMQError::Internal(format!("Failed to get topic route info: {}", e)))?
                .ok_or_else(|| RocketMQError::Internal(format!("Topic route not found for: {}", route_topic)))?;

            let mut broker_addr_map: HashMap<String, CheetahString> = HashMap::new();
            for broker_data in &topic_route.broker_datas {
                if let Some(addr) = broker_data.select_broker_addr() {
                    broker_addr_map.insert(broker_data.broker_name().to_string(), addr);
                }
            }

            let mut message_queues: Vec<(MessageQueue, CheetahString)> = Vec::new();
            for queue_data in &topic_route.queue_datas {
                let broker_name = queue_data.broker_name().as_str();

                let Some(broker_addr) = broker_addr_map.get(broker_name).cloned() else {
                    continue;
                };

                let read_queue_nums = queue_data.read_queue_nums() as i32;
                if read_queue_nums <= 0 {
                    continue;
                }

                for queue_id in 0..read_queue_nums {
                    message_queues.push((
                        MessageQueue::from_parts(topic, broker_name, queue_id),
                        broker_addr.clone(),
                    ));
                }
            }

            if message_queues.is_empty() {
                return Err(RocketMQError::Internal("No available message queue found".to_string()));
            }

            for (mq, broker_addr) in message_queues {
                let mut min_offset = default_mqadmin_ext
                    .min_offset(broker_addr.clone(), mq.clone(), PULL_TIMEOUT_MILLIS)
                    .await
                    .map_err(|e| RocketMQError::Internal(format!("Failed to get min offset: {}", e)))?;

                let mut max_offset = default_mqadmin_ext
                    .max_offset(broker_addr.clone(), mq.clone(), PULL_TIMEOUT_MILLIS)
                    .await
                    .map_err(|e| RocketMQError::Internal(format!("Failed to get max offset: {}", e)))?;

                if let Some(begin_ts) = &self.begin_timestamp {
                    let time_value = timestamp_format(begin_ts.trim());
                    min_offset = default_mqadmin_ext
                        .search_offset(
                            broker_addr.clone(),
                            CheetahString::from(topic),
                            mq.queue_id(),
                            time_value,
                            PULL_TIMEOUT_MILLIS,
                        )
                        .await
                        .map_err(|e| RocketMQError::Internal(format!("Failed to search begin offset: {}", e)))?
                        as i64;
                }

                if let Some(end_ts) = &self.end_timestamp {
                    let time_value = timestamp_format(end_ts.trim());
                    max_offset = default_mqadmin_ext
                        .search_offset(
                            broker_addr.clone(),
                            CheetahString::from(topic),
                            mq.queue_id(),
                            time_value,
                            PULL_TIMEOUT_MILLIS,
                        )
                        .await
                        .map_err(|e| RocketMQError::Internal(format!("Failed to search end offset: {}", e)))?
                        as i64;
                }

                println!("minOffset={}, maxOffset={}, {}", min_offset, max_offset, mq);

                let mut offset = min_offset;
                'read_queue: while offset < max_offset {
                    let pull_result = default_mqadmin_ext
                        .pull_message_from_queue(
                            broker_addr.as_str(),
                            &mq,
                            sub_expression,
                            offset,
                            PULL_BATCH_SIZE,
                            PULL_TIMEOUT_MILLIS,
                        )
                        .await;

                    match pull_result {
                        Ok(result) => {
                            offset = result.next_begin_offset() as i64;
                            match result.pull_status() {
                                PullStatus::Found => {
                                    if let Some(msg_list) = result.msg_found_list() {
                                        print_message(msg_list, charset_name, print_body);
                                    }
                                }
                                PullStatus::NoMatchedMsg => {
                                    println!(
                                        "{} no matched msg. status={:?}, offset={}",
                                        mq,
                                        result.pull_status(),
                                        offset
                                    );
                                }
                                PullStatus::NoNewMsg | PullStatus::OffsetIllegal => {
                                    println!(
                                        "{} print msg finished. status={:?}, offset={}",
                                        mq,
                                        result.pull_status(),
                                        offset
                                    );
                                    break 'read_queue;
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("Pull message error: {}", e);
                            break;
                        }
                    }
                }

                println!("--------------------------------------------------------");
            }

            Ok(())
        }
        .await;

        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;
        operation_result
    }
}
