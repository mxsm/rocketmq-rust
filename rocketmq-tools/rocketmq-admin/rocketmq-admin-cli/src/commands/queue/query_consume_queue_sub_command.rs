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
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::body::query_consume_queue_response_body::QueryConsumeQueueResponseBody;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use rocketmq_admin_core::core::queue::QueryConsumeQueueRequest;
use rocketmq_admin_core::core::queue::QueueService;

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

impl QueryCqSubCommand {
    fn request(&self) -> RocketMQResult<QueryConsumeQueueRequest> {
        QueryConsumeQueueRequest::try_new(
            self.topic.clone(),
            self.queue_id,
            self.index,
            self.count,
            self.broker.clone(),
            self.consumer_group.clone(),
        )
    }
}

impl CommandExecute for QueryCqSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let request = self.request()?;
        let index = request.index();
        let result = QueueService::query_consume_queue_by_request_with_rpc_hook(request, rpc_hook).await?;
        print_query_consume_queue_response(&result.response_body, index);
        Ok(())
    }
}

fn print_query_consume_queue_response(response_body: &QueryConsumeQueueResponseBody, index: u64) {
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
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[test]
    fn parses_query_consume_queue_request() {
        let cmd = QueryCqSubCommand::try_parse_from([
            "queryCq",
            "-t",
            " TestTopic ",
            "-q",
            "1",
            "-i",
            "10",
            "-c",
            "20",
            "-b",
            " 127.0.0.1:10911 ",
            "-g",
            " TestGroup ",
        ])
        .unwrap();
        let request = cmd.request().unwrap();

        assert_eq!(request.topic().as_str(), "TestTopic");
        assert_eq!(request.broker_addr().unwrap().as_str(), "127.0.0.1:10911");
        assert_eq!(request.consumer_group().as_str(), "TestGroup");
        assert_eq!(request.index(), 10);
        assert_eq!(request.count(), 20);
    }
}
