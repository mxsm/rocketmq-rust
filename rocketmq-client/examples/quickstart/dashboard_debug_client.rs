// Copyright 2026 The RocketMQ Rust Authors
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

#![recursion_limit = "512"]

#[path = "../support/mod.rs"]
mod support;

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use rocketmq_client_rust::ClientConfig;
use rocketmq_client_rust::ClientResult;
use rocketmq_client_rust::ConsumeConcurrentlyContext;
use rocketmq_client_rust::ConsumeConcurrentlyStatus;
use rocketmq_client_rust::DefaultMQProducer;
use rocketmq_client_rust::DefaultMQPushConsumer;
use rocketmq_client_rust::MQPushConsumer;
use rocketmq_client_rust::MessageListenerConcurrently;
use rocketmq_model::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_model::common::message::message_single::Message;

struct RejectFirstDelivery(AtomicBool);

impl MessageListenerConcurrently for RejectFirstDelivery {
    fn consume_message(
        &self,
        messages: &[&MessageExt],
        _context: &ConsumeConcurrentlyContext,
    ) -> ClientResult<ConsumeConcurrentlyStatus> {
        // One message per callback: the first rejection creates a DLQ fixture;
        // subsequent direct-consume requests can exercise successful redelivery.
        if self.0.swap(false, Ordering::AcqRel) {
            println!("REJECTED deliveries={}", messages.len());
            Ok(ConsumeConcurrentlyStatus::ReconsumeLater)
        } else {
            println!("ACCEPTED deliveries={}", messages.len());
            Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() != 4 {
        return Err("usage: dashboard-debug-client <namesrv> <fresh-topic> <fresh-group> <seconds:1..3600>".into());
    }
    let seconds: u64 = args[3].parse()?;
    if !(1..=3600).contains(&seconds) || args[..3].iter().any(|value| value.trim().is_empty()) {
        return Err("provide nonempty addresses/names and a lifetime between 1 and 3600 seconds".into());
    }
    let runtime = support::ExampleClientRuntime::try_new("dashboard-debug")?;
    let config = ClientConfig {
        namesrv_addr: Some(args[0].clone().into()),
        vip_channel_enabled: false,
        enable_trace: true,
        trace_topic: Some(format!("{}_trace", args[1]).into()),
        ..Default::default()
    };
    let mut consumer = DefaultMQPushConsumer::builder(runtime.client_runtime())
        .client_config(config.clone())
        .consumer_group(args[2].clone())
        .consume_from_where(ConsumeFromWhere::ConsumeFromFirstOffset)
        .consume_message_batch_max_size(1)
        .max_reconsume_times(0)
        .build();
    let mut producer = DefaultMQProducer::builder(runtime.client_runtime())
        .client_config(config)
        .producer_group(format!("{}_producer", args[2]))
        .build();
    let outcome: ClientResult<()> = async {
        consumer.subscribe(args[1].as_str(), "*").await?;
        consumer.register_message_listener_concurrently(RejectFirstDelivery(AtomicBool::new(true)));
        consumer.start().await?;
        producer.start().await?;
        let message = Message::builder()
            .topic(args[1].clone())
            .keys(vec!["dashboard-debug-key".into()])
            .tags("dashboard-debug")
            .body_slice(b"dashboard local acceptance fixture")
            .build_unchecked();
        let receipt = producer.send_with_timeout(message, 10_000).await?;
        println!("SENT {receipt:?}");
        println!("READY group={} topic={} lifetime_seconds={seconds}", args[2], args[1]);
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = tokio::time::sleep(Duration::from_secs(seconds)) => {}
        }
        Ok(())
    }
    .await;
    Box::pin(consumer.shutdown()).await;
    Box::pin(producer.shutdown()).await;
    Box::pin(runtime.shutdown()).await;
    outcome?;
    println!("SHUTDOWN complete");
    Ok(())
}
