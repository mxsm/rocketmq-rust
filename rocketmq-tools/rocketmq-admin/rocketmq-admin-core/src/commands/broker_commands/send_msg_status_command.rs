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
use rocketmq_client_rust::base::client_config::ClientConfig;
use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_client_rust::producer::mq_producer::MQProducer;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;

const PRODUCER_GROUP: &str = "PID_SMSC";

fn build_message(topic: &str, message_size: usize) -> Message {
    let mut sb = String::new();
    let filler = "hello jodie";
    let mut i = 0;
    while i < message_size {
        sb.push_str(filler);
        i += filler.len();
    }
    Message::builder()
        .topic(topic)
        .body_slice(sb.as_bytes())
        .build_unchecked()
}

#[derive(Debug, Clone, Parser)]
pub struct SendMsgStatusCommand {
    #[arg(
        short = 'b',
        long = "brokerName",
        required = true,
        help = "Broker Name e.g. clusterName_brokerName as DefaultCluster_broker-a"
    )]
    broker_name: String,

    #[arg(
        short = 's',
        long = "messageSize",
        required = false,
        default_value = "128",
        help = "Message Size, Default: 128"
    )]
    message_size: usize,

    #[arg(
        short = 'c',
        long = "count",
        required = false,
        default_value = "50",
        help = "send message count, Default: 50"
    )]
    count: u32,
}

impl CommandExecute for SendMsgStatusCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let instance_name = format!("PID_SMSC_{}", get_current_millis());
        let mut client_config = ClientConfig::default();
        client_config.set_instance_name(instance_name.into());

        let mut builder = DefaultMQProducer::builder()
            .producer_group(PRODUCER_GROUP.to_string())
            .client_config(client_config);
        if let Some(rpc_hook) = rpc_hook {
            builder = builder.rpc_hook(rpc_hook);
        }
        let mut producer = builder.build();

        producer
            .start()
            .await
            .map_err(|e| RocketMQError::Internal(format!("SendMsgStatusCommand: Failed to start producer: {}", e)))?;

        let broker_name = self.broker_name.trim();

        let warmup_result = producer.send(build_message(broker_name, 16)).await;
        if let Err(e) = warmup_result {
            producer.shutdown().await;
            return Err(RocketMQError::Internal(format!(
                "SendMsgStatusCommand command failed: {}",
                e
            )));
        }

        for _i in 0..self.count {
            let begin = get_current_millis();
            match producer.send(build_message(broker_name, self.message_size)).await {
                Ok(result) => {
                    let rt = get_current_millis() - begin;
                    println!(
                        "rt={}ms, SendResult={}",
                        rt,
                        result.map(|r| r.to_string()).unwrap_or_else(|| "None".to_string())
                    );
                }
                Err(e) => {
                    producer.shutdown().await;
                    return Err(RocketMQError::Internal(format!(
                        "SendMsgStatusCommand command failed: {}",
                        e
                    )));
                }
            }
        }

        producer.shutdown().await;
        Ok(())
    }
}
