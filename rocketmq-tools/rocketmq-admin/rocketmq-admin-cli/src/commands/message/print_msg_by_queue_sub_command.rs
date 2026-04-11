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
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;

use cheetah_string::CheetahString;
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

#[derive(Debug, Clone, Parser)]
pub struct PrintMsgByQueueSubCommand {
    #[arg(short = 't', long = "topic", required = true, help = "Topic name")]
    topic: String,

    #[arg(short = 'a', long = "brokerName", required = true, help = "Broker name")]
    broker_name: String,

    #[arg(short = 'i', long = "queueId", required = true, help = "Queue id")]
    queue_id: i32,

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
        short = 'p',
        long = "print",
        required = false,
        default_value = "false",
        help = "Print msg. eg: true | false(default)"
    )]
    print_msg: bool,

    #[arg(
        short = 'd',
        long = "printBody",
        required = false,
        default_value = "false",
        help = "Print body. eg: true | false(default)"
    )]
    print_body: bool,

    #[arg(
        short = 'f',
        long = "calculate",
        required = false,
        default_value = "false",
        help = "Calculate by tag. eg: true | false(default)"
    )]
    calculate_by_tag: bool,
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

impl CommandExecute for PrintMsgByQueueSubCommand {
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
            let charset_name = self.charset_name.trim();
            let sub_expression = self.sub_expression.trim();
            let print_msg = self.print_msg;
            let print_body = self.print_body;
            let cal_by_tag = self.calculate_by_tag;

            // Resolve broker address via topic route
            let topic_route = default_mqadmin_ext
                .examine_topic_route_info(CheetahString::from(topic))
                .await
                .map_err(|e| RocketMQError::Internal(format!("Failed to get topic route info: {}", e)))?
                .ok_or_else(|| RocketMQError::Internal(format!("Topic route not found for: {}", topic)))?;

            let broker_data = topic_route
                .broker_datas
                .iter()
                .find(|bd| bd.broker_name().as_str() == broker_name)
                .ok_or_else(|| {
                    RocketMQError::Internal(format!(
                        "Broker '{}' not found in topic route for '{}'",
                        broker_name, topic
                    ))
                })?;

            let broker_addr = broker_data
                .select_broker_addr()
                .ok_or_else(|| RocketMQError::Internal(format!("No available address for broker '{}'", broker_name)))?;

            let mq = MessageQueue::from_parts(topic, broker_name, queue_id);
            let timeout = 3000u64;

            let mut min_offset = default_mqadmin_ext
                .min_offset(broker_addr.clone(), mq.clone(), timeout)
                .await
                .map_err(|e| RocketMQError::Internal(format!("Failed to get min offset: {}", e)))?;

            let mut max_offset = default_mqadmin_ext
                .max_offset(broker_addr.clone(), mq.clone(), timeout)
                .await
                .map_err(|e| RocketMQError::Internal(format!("Failed to get max offset: {}", e)))?;

            if let Some(ref begin_ts) = self.begin_timestamp {
                let time_value = timestamp_format(begin_ts.trim());
                min_offset = default_mqadmin_ext
                    .search_offset(
                        broker_addr.clone(),
                        CheetahString::from(topic),
                        queue_id,
                        time_value,
                        timeout,
                    )
                    .await
                    .map_err(|e| RocketMQError::Internal(format!("Failed to search begin offset: {}", e)))?
                    as i64;
            }

            if let Some(ref end_ts) = self.end_timestamp {
                let time_value = timestamp_format(end_ts.trim());
                max_offset = default_mqadmin_ext
                    .search_offset(
                        broker_addr.clone(),
                        CheetahString::from(topic),
                        queue_id,
                        time_value,
                        timeout,
                    )
                    .await
                    .map_err(|e| RocketMQError::Internal(format!("Failed to search end offset: {}", e)))?
                    as i64;
            }

            let mut tag_cal_map: HashMap<String, AtomicI64> = HashMap::new();
            let mut offset = min_offset;

            'read_queue: while offset < max_offset {
                let pull_result = default_mqadmin_ext
                    .pull_message_from_queue(
                        broker_addr.as_str(),
                        &mq,
                        sub_expression,
                        offset,
                        PULL_BATCH_SIZE,
                        timeout,
                    )
                    .await;

                match pull_result {
                    Ok(result) => {
                        offset = result.next_begin_offset() as i64;
                        match result.pull_status() {
                            PullStatus::Found => {
                                if let Some(msg_list) = result.msg_found_list() {
                                    if cal_by_tag {
                                        calculate_by_tag(msg_list, &mut tag_cal_map);
                                    }
                                    if print_msg {
                                        print_messages(msg_list, charset_name, print_body);
                                    }
                                }
                            }
                            PullStatus::NoMatchedMsg | PullStatus::NoNewMsg | PullStatus::OffsetIllegal => {
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

            if cal_by_tag {
                print_calculate_by_tag(&tag_cal_map);
            }

            Ok(())
        }
        .await;

        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;
        operation_result
    }
}

fn calculate_by_tag(msgs: &[rocketmq_rust::ArcMut<MessageExt>], tag_cal_map: &mut HashMap<String, AtomicI64>) {
    for msg in msgs {
        if let Some(tag) = msg.tags() {
            let tag_str = tag.to_string();
            if !tag_str.is_empty() {
                tag_cal_map
                    .entry(tag_str)
                    .or_insert_with(|| AtomicI64::new(0))
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

fn print_calculate_by_tag(tag_cal_map: &HashMap<String, AtomicI64>) {
    let mut list: Vec<(&String, i64)> = tag_cal_map
        .iter()
        .map(|(tag, count)| (tag, count.load(Ordering::Relaxed)))
        .collect();
    list.sort_by_key(|b| std::cmp::Reverse(b.1));
    for (tag, count) in list {
        println!("Tag: {:<30} Count: {}", tag, count);
    }
}

fn print_messages(msgs: &[rocketmq_rust::ArcMut<MessageExt>], _charset_name: &str, print_body: bool) {
    for msg in msgs {
        let body_str = if print_body {
            if let Some(body) = msg.body() {
                String::from_utf8(body.to_vec()).unwrap_or_else(|_| "BINARY".to_string())
            } else {
                "EMPTY".to_string()
            }
        } else {
            "NOT PRINT BODY".to_string()
        };
        let msg_ref: &MessageExt = msg;
        println!("MSGID: {} {} BODY: {}", msg.msg_id(), msg_ref, body_str);
    }
}
