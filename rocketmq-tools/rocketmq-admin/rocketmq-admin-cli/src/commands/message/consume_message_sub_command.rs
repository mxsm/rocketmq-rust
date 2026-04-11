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
use chrono::NaiveDateTime;
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

const DEFAULT_MESSAGE_COUNT: i64 = 128;
const PULL_TIMEOUT_MILLIS: u64 = 3000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConsumeType {
    Default,
    ByQueue,
    ByOffset,
}

#[derive(Debug, Clone, Parser)]
pub struct ConsumeMessageSubCommand {
    #[arg(short = 't', long = "topic", required = true, help = "Topic name")]
    topic: String,

    #[arg(short = 'b', long = "brokerName", required = false, help = "Broker name")]
    broker_name: Option<String>,

    #[arg(short = 'i', long = "queueId", required = false, help = "Queue Id")]
    queue_id: Option<i32>,

    #[arg(short = 'o', long = "offset", required = false, help = "Queue offset")]
    offset: Option<i64>,

    #[arg(short = 'g', long = "consumerGroup", required = false, help = "Consumer group name")]
    consumer_group: Option<String>,

    #[arg(
        short = 's',
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
        short = 'c',
        long = "MessageNumber",
        required = false,
        default_value_t = DEFAULT_MESSAGE_COUNT,
        help = "Number of messages to be consumed"
    )]
    message_number: i64,
}

impl ConsumeMessageSubCommand {
    fn timestamp_format(value: &str) -> RocketMQResult<i64> {
        if let Ok(ts) = value.trim().parse::<i64>() {
            return Ok(ts);
        }

        let date_str = value.trim().replace('#', " ");
        if let Some(dt) = parse_date(&date_str, YYYY_MM_DD_HH_MM_SS_SSS) {
            return Ok(dt.and_utc().timestamp_millis());
        }

        if let Ok(dt) = NaiveDateTime::parse_from_str(&date_str, "%Y-%m-%d %H:%M:%S") {
            return Ok(dt.and_utc().timestamp_millis());
        }

        Err(RocketMQError::IllegalArgument(format!(
            "Invalid timestamp format: {}",
            value
        )))
    }

    async fn pull_message_by_queue(
        default_mqadmin_ext: &DefaultMQAdminExt,
        broker_addr: &str,
        mq: &MessageQueue,
        min_offset: i64,
        max_offset: i64,
    ) {
        let mut offset = min_offset;
        'readq: while offset <= max_offset {
            let batch_size = (max_offset - offset + 1) as i32;
            let pull_result = default_mqadmin_ext
                .pull_message_from_queue(broker_addr, mq, "*", offset, batch_size, PULL_TIMEOUT_MILLIS)
                .await;

            match pull_result {
                Ok(result) => {
                    offset = result.next_begin_offset() as i64;
                    match result.pull_status() {
                        PullStatus::Found => {
                            println!("Consume ok");
                            if let Some(msgs) = result.msg_found_list() {
                                for msg in msgs {
                                    let body_str = match msg.body() {
                                        Some(body) => {
                                            String::from_utf8(body.to_vec()).unwrap_or_else(|_| "BINARY".to_string())
                                        }
                                        None => "EMPTY".to_string(),
                                    };
                                    let msg_ref: &MessageExt = msg;
                                    println!("MSGID: {} {} BODY: {}", msg.msg_id(), msg_ref, body_str);
                                }
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
                            break 'readq;
                        }
                    }
                }
                Err(error) => {
                    eprintln!("{}", error);
                    break 'readq;
                }
            }
        }
    }

    async fn execute_default(
        &self,
        default_mqadmin_ext: &DefaultMQAdminExt,
        time_value_begin: i64,
        time_value_end: i64,
    ) -> RocketMQResult<()> {
        let topic = self.topic.trim();
        let topic_route = default_mqadmin_ext
            .examine_topic_route_info(CheetahString::from(topic))
            .await
            .map_err(|e| RocketMQError::Internal(format!("Failed to examine topic route info: {}", e)))?
            .ok_or_else(|| RocketMQError::Internal(format!("Topic route not found for: {}", topic)))?;

        let mut broker_addr_map: HashMap<String, CheetahString> = HashMap::new();
        for broker_data in &topic_route.broker_datas {
            if let Some(addr) = broker_data.select_broker_addr() {
                broker_addr_map.insert(broker_data.broker_name().to_string(), addr);
            }
        }

        let mut mqs: Vec<(MessageQueue, CheetahString)> = Vec::new();
        for queue_data in &topic_route.queue_datas {
            let broker_name = queue_data.broker_name().as_str();
            let Some(broker_addr) = broker_addr_map.get(broker_name).cloned() else {
                continue;
            };

            let read_queue_nums = queue_data.read_queue_nums();
            if read_queue_nums == 0 {
                continue;
            }

            for queue_id in 0..read_queue_nums {
                mqs.push((
                    MessageQueue::from_parts(topic, broker_name, queue_id as i32),
                    broker_addr.clone(),
                ));
            }
        }

        let mut count_left = self.message_number;
        for (mq, broker_addr) in mqs {
            if count_left == 0 {
                return Ok(());
            }

            let mut min_offset = default_mqadmin_ext
                .min_offset(broker_addr.clone(), mq.clone(), PULL_TIMEOUT_MILLIS)
                .await
                .map_err(|e| RocketMQError::Internal(format!("Failed to get min offset: {}", e)))?;
            let mut max_offset = default_mqadmin_ext
                .max_offset(broker_addr.clone(), mq.clone(), PULL_TIMEOUT_MILLIS)
                .await
                .map_err(|e| RocketMQError::Internal(format!("Failed to get max offset: {}", e)))?;

            if time_value_begin > 0 {
                min_offset = default_mqadmin_ext
                    .search_offset(
                        broker_addr.clone(),
                        CheetahString::from(topic),
                        mq.queue_id(),
                        time_value_begin as u64,
                        PULL_TIMEOUT_MILLIS,
                    )
                    .await
                    .map_err(|e| RocketMQError::Internal(format!("Failed to search begin offset: {}", e)))?
                    as i64;
            }

            if time_value_end > 0 {
                max_offset = default_mqadmin_ext
                    .search_offset(
                        broker_addr.clone(),
                        CheetahString::from(topic),
                        mq.queue_id(),
                        time_value_end as u64,
                        PULL_TIMEOUT_MILLIS,
                    )
                    .await
                    .map_err(|e| RocketMQError::Internal(format!("Failed to search end offset: {}", e)))?
                    as i64;
            }

            if max_offset - min_offset > count_left {
                println!(
                    "The older {} message of the {} queue will be provided",
                    count_left,
                    mq.queue_id()
                );
                max_offset = min_offset + count_left - 1;
                count_left = 0;
            } else {
                count_left = count_left - (max_offset - min_offset) - 1;
            }

            Self::pull_message_by_queue(default_mqadmin_ext, broker_addr.as_str(), &mq, min_offset, max_offset).await;
        }

        Ok(())
    }

    async fn execute_by_condition(
        &self,
        default_mqadmin_ext: &DefaultMQAdminExt,
        broker_name: &str,
        queue_id: i32,
        offset: i64,
        time_value_begin: i64,
        time_value_end: i64,
    ) -> RocketMQResult<()> {
        let topic = self.topic.trim();

        let topic_route = default_mqadmin_ext
            .examine_topic_route_info(CheetahString::from(topic))
            .await
            .map_err(|e| RocketMQError::Internal(format!("Failed to examine topic route info: {}", e)))?
            .ok_or_else(|| RocketMQError::Internal(format!("Topic route not found for: {}", topic)))?;

        let mut broker_addr_map: HashMap<String, CheetahString> = HashMap::new();
        for broker_data in &topic_route.broker_datas {
            if let Some(addr) = broker_data.select_broker_addr() {
                broker_addr_map.insert(broker_data.broker_name().to_string(), addr);
            }
        }

        let broker_addr = broker_addr_map.get(broker_name).cloned().ok_or_else(|| {
            RocketMQError::IllegalArgument(format!("Broker '{}' not found in topic route", broker_name))
        })?;

        let mq = MessageQueue::from_parts(topic, broker_name, queue_id);

        let mut min_offset = default_mqadmin_ext
            .min_offset(broker_addr.clone(), mq.clone(), PULL_TIMEOUT_MILLIS)
            .await
            .map_err(|e| RocketMQError::Internal(format!("Failed to get min offset: {}", e)))?;
        let mut max_offset = default_mqadmin_ext
            .max_offset(broker_addr.clone(), mq.clone(), PULL_TIMEOUT_MILLIS)
            .await
            .map_err(|e| RocketMQError::Internal(format!("Failed to get max offset: {}", e)))?;

        if time_value_begin > 0 {
            min_offset = default_mqadmin_ext
                .search_offset(
                    broker_addr.clone(),
                    CheetahString::from(topic),
                    queue_id,
                    time_value_begin as u64,
                    PULL_TIMEOUT_MILLIS,
                )
                .await
                .map_err(|e| RocketMQError::Internal(format!("Failed to search begin offset: {}", e)))?
                as i64;
        }

        if time_value_end > 0 {
            max_offset = default_mqadmin_ext
                .search_offset(
                    broker_addr.clone(),
                    CheetahString::from(topic),
                    queue_id,
                    time_value_end as u64,
                    PULL_TIMEOUT_MILLIS,
                )
                .await
                .map_err(|e| RocketMQError::Internal(format!("Failed to search end offset: {}", e)))?
                as i64;
        }

        if offset > max_offset {
            println!("{} no matched msg, offset={}", mq, offset);
            return Ok(());
        }

        min_offset = if min_offset > offset { min_offset } else { offset };

        if max_offset - min_offset > self.message_number {
            println!("The older {} message will be provided", self.message_number);
            max_offset = min_offset + self.message_number - 1;
        }

        Self::pull_message_by_queue(default_mqadmin_ext, broker_addr.as_str(), &mq, min_offset, max_offset).await;
        Ok(())
    }
}

impl CommandExecute for ConsumeMessageSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let consumer_group = self.consumer_group.as_deref().map(str::trim).filter(|s| !s.is_empty());

        let mut default_mqadmin_ext = match (rpc_hook, consumer_group) {
            (Some(rpc_hook), Some(group)) => DefaultMQAdminExt::with_admin_ext_group_and_rpc_hook(group, rpc_hook),
            (Some(rpc_hook), None) => DefaultMQAdminExt::with_rpc_hook(rpc_hook),
            (None, Some(group)) => DefaultMQAdminExt::with_admin_ext_group(group),
            (None, None) => DefaultMQAdminExt::new(),
        };
        default_mqadmin_ext
            .client_config_mut()
            .set_instance_name(current_millis().to_string().into());

        let mut offset = 0;
        let mut time_value_end = 0;
        let mut time_value_begin = 0;
        let mut consume_type = ConsumeType::Default;

        if self.message_number <= 0 {
            return Err(RocketMQError::IllegalArgument(
                "Please input a positive messageNumber!".to_string(),
            ));
        }

        let broker_name = self.broker_name.as_deref().map(str::trim);

        if self.queue_id.is_some() {
            if broker_name.is_none() {
                return Err(RocketMQError::IllegalArgument(
                    "Please set the brokerName before queueId!".to_string(),
                ));
            }
            consume_type = ConsumeType::ByQueue;
        }

        if let Some(user_offset) = self.offset {
            if consume_type != ConsumeType::ByQueue {
                return Err(RocketMQError::IllegalArgument(
                    "Please set queueId before offset!".to_string(),
                ));
            }
            offset = user_offset;
            consume_type = ConsumeType::ByOffset;
        }

        let now = current_millis() as i64;
        if let Some(value) = self.begin_timestamp.as_deref() {
            time_value_begin = Self::timestamp_format(value)?;
            if time_value_begin > now {
                return Err(RocketMQError::IllegalArgument(
                    "Please set the beginTimestamp before now!".to_string(),
                ));
            }
        }

        if let Some(value) = self.end_timestamp.as_deref() {
            time_value_end = Self::timestamp_format(value)?;
            if time_value_end > now {
                return Err(RocketMQError::IllegalArgument(
                    "Please set the endTimestamp before now!".to_string(),
                ));
            }
            if time_value_begin > time_value_end {
                return Err(RocketMQError::IllegalArgument(
                    "Please make sure that the beginTimestamp is less than or equal to the endTimestamp".to_string(),
                ));
            }
        }

        let operation_result = async {
            MQAdminExt::start(&mut default_mqadmin_ext)
                .await
                .map_err(|e| RocketMQError::Internal(format!("Failed to start MQAdminExt: {}", e)))?;

            match consume_type {
                ConsumeType::Default => {
                    self.execute_default(&default_mqadmin_ext, time_value_begin, time_value_end)
                        .await?
                }
                ConsumeType::ByOffset => {
                    self.execute_by_condition(
                        &default_mqadmin_ext,
                        broker_name.unwrap_or_default(),
                        self.queue_id.unwrap_or_default(),
                        offset,
                        time_value_begin,
                        time_value_end,
                    )
                    .await?
                }
                ConsumeType::ByQueue => {
                    self.execute_by_condition(
                        &default_mqadmin_ext,
                        broker_name.unwrap_or_default(),
                        self.queue_id.unwrap_or_default(),
                        0,
                        time_value_begin,
                        time_value_end,
                    )
                    .await?
                }
            }
            Ok(())
        }
        .await;

        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;
        operation_result
    }
}
