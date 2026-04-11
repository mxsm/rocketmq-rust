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
use std::sync::Mutex;

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

fn get_string_by_size(size: usize) -> Vec<u8> {
    vec![b'a'; size]
}

#[derive(Debug, Clone, Parser)]
pub struct CheckMsgSendRTSubCommand {
    #[arg(short = 't', long = "topic", required = true, help = "topic name")]
    topic: String,

    #[arg(
        short = 'a',
        long = "amount",
        required = false,
        default_value = "100",
        help = "message amount | default 100"
    )]
    amount: u64,

    #[arg(
        short = 's',
        long = "size",
        required = false,
        default_value = "128",
        help = "message size | default 128 Byte"
    )]
    size: usize,
}

impl CommandExecute for CheckMsgSendRTSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let mut builder = DefaultMQProducer::builder().producer_group(current_millis().to_string());
        if let Some(rpc_hook) = rpc_hook {
            builder = builder.rpc_hook(rpc_hook);
        }
        let mut producer = builder.build();

        let operation_result = async {
            producer
                .start()
                .await
                .map_err(|e| RocketMQError::Internal(format!("CheckMsgSendRTSubCommand command failed: {}", e)))?;

            let topic = self.topic.trim();
            let amount = self.amount;
            let msg_size = self.size;

            let msg = Message::builder()
                .topic(topic)
                .body_slice(&get_string_by_size(msg_size))
                .build_unchecked();

            let broker_name_holder: Arc<Mutex<String>> = Arc::new(Mutex::new(String::new()));
            let queue_id_holder: Arc<Mutex<i32>> = Arc::new(Mutex::new(0));

            println!("{:<32}  {:<4}  {:<20}    #RT", "#Broker Name", "#QID", "#Send Result");

            let mut time_elapsed: u64 = 0;

            for i in 0..amount {
                let start = current_millis();
                let send_success;
                let end;

                let bn = broker_name_holder.clone();
                let qi = queue_id_holder.clone();
                let selector = move |mqs: &[MessageQueue], _msg: &Message, arg: &u64| -> Option<MessageQueue> {
                    let queue_index = (*arg as usize) % mqs.len();
                    let queue = &mqs[queue_index];
                    *bn.lock().unwrap() = queue.broker_name().to_string();
                    *qi.lock().unwrap() = queue.queue_id();
                    Some(queue.clone())
                };

                match producer.send_with_selector(msg.clone(), selector, i).await {
                    Ok(_) => {
                        send_success = true;
                        end = current_millis();
                    }
                    Err(_) => {
                        send_success = false;
                        end = current_millis();
                    }
                }

                let broker_name = broker_name_holder.lock().unwrap().clone();
                let queue_id = *queue_id_holder.lock().unwrap();

                if i != 0 {
                    time_elapsed += end - start;
                }

                println!(
                    "{:<32}  {:<4}  {:<20}    {}",
                    broker_name,
                    queue_id,
                    send_success,
                    end - start
                );
            }

            let rt = time_elapsed as f64 / (amount as i64 - 1) as f64;
            println!("Avg RT: {:.2}", rt);

            Ok(())
        }
        .await;

        producer.shutdown().await;
        operation_result
    }
}
