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
use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_client_rust::producer::mq_producer::MQProducer;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;

#[derive(Debug, Clone, Parser)]
pub struct SendMessageSubCommand {
    #[arg(short = 't', long = "topic", required = true, help = "Topic name")]
    topic: String,

    #[arg(
        short = 'p',
        long = "body",
        required = true,
        help = "UTF-8 string format of the message body"
    )]
    body: String,

    #[arg(short = 'k', long = "key", required = false, help = "Message keys")]
    keys: Option<String>,

    #[arg(short = 'c', long = "tags", required = false, help = "Message tags")]
    tags: Option<String>,

    #[arg(
        short = 'b',
        long = "broker",
        required = false,
        help = "Send message to target broker"
    )]
    broker_name: Option<String>,

    #[arg(short = 'i', long = "qid", required = false, help = "Send message to target queue")]
    queue_id: Option<i32>,

    #[arg(
        short = 'm',
        long = "msgTraceEnable",
        required = false,
        default_value = "false",
        help = "Message Trace Enable, Default: false"
    )]
    msg_trace_enable: bool,
}

impl CommandExecute for SendMessageSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let topic = self.topic.trim();
        let body = self.body.trim();

        if self.queue_id.is_some() && self.broker_name.is_none() {
            println!("Broker name must be set if the queue is chosen!");
            return Ok(());
        }

        let tag = self.tags.as_deref().map(|t| t.trim());
        let keys = self.keys.as_deref().map(|k| k.trim());

        let msg = Message::builder().topic(topic).body(body.as_bytes().to_vec());
        let msg = if let Some(tag) = tag { msg.tags(tag) } else { msg };
        let msg = if let Some(keys) = keys { msg.key(keys) } else { msg };
        let msg = msg
            .build()
            .map_err(|e| RocketMQError::Internal(format!("SendMessageSubCommand command failed: {}", e)))?;

        let mut builder = DefaultMQProducer::builder().producer_group(current_millis().to_string());
        if let Some(rpc_hook) = rpc_hook {
            builder = builder.rpc_hook(rpc_hook);
        }
        let mut producer = builder.build();

        let operation_result = async {
            producer
                .start()
                .await
                .map_err(|e| RocketMQError::Internal(format!("SendMessageSubCommand command failed: {}", e)))?;

            let result = if let (Some(broker_name), Some(queue_id)) = (&self.broker_name, self.queue_id) {
                let message_queue = MessageQueue::from_parts(topic, broker_name.trim(), queue_id);
                producer.send_to_queue(msg, message_queue).await
            } else {
                producer.send(msg).await
            };

            match result {
                Ok(send_result) => {
                    println!(
                        "{:<32}  {:<4}  {:<20}    #MsgId",
                        "#Broker Name", "#QID", "#Send Result"
                    );
                    if let Some(ref result) = send_result {
                        let broker_name = result
                            .message_queue
                            .as_ref()
                            .map(|mq| mq.broker_name().to_string())
                            .unwrap_or_else(|| "Unknown".to_string());
                        let queue_id = result
                            .message_queue
                            .as_ref()
                            .map(|mq| mq.queue_id().to_string())
                            .unwrap_or_else(|| "Unknown".to_string());
                        let send_status = format!("{:?}", result.send_status);
                        let msg_id = result
                            .msg_id
                            .as_ref()
                            .map(|id| id.to_string())
                            .unwrap_or_else(|| "None".to_string());
                        println!(
                            "{:<32}  {:<4}  {:<20}    {}",
                            broker_name, queue_id, send_status, msg_id
                        );
                    } else {
                        println!("{:<32}  {:<4}  {:<20}    None", "Unknown", "Unknown", "Failed");
                    }
                }
                Err(e) => {
                    return Err(RocketMQError::Internal(format!(
                        "SendMessageSubCommand command failed: {}",
                        e
                    )));
                }
            }

            Ok(())
        }
        .await;

        producer.shutdown().await;
        operation_result
    }
}
