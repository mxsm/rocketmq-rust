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
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use crate::commands::CommonArgs;
use rocketmq_admin_core::core::topic::AllocateMqQueryRequest;
use rocketmq_admin_core::core::topic::AllocatedMqQueryResult;
use rocketmq_admin_core::core::topic::TopicService;

#[derive(Debug, Clone, Parser)]
pub struct AllocateMQSubCommand {
    #[command(flatten)]
    common_args: CommonArgs,

    /// Topic name
    #[arg(short = 't', long = "topic", required = true)]
    topic: String,

    /// Comma-separated list of IP addresses
    #[arg(short = 'i', long = "ipList", required = true)]
    ip_list: String,
}

impl AllocateMQSubCommand {
    fn request(&self) -> RocketMQResult<AllocateMqQueryRequest> {
        Ok(
            AllocateMqQueryRequest::try_new(self.topic.clone(), self.ip_list.clone())?
                .with_optional_namesrv_addr(self.common_args.namesrv_addr.clone()),
        )
    }

    fn print_result(result: AllocatedMqQueryResult) {
        if result.route_found {
            println!("Topic: {}", result.topic);
            println!(
                "IP List: {}",
                result
                    .requested_ips
                    .iter()
                    .map(|ip| ip.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            println!("\nMessage Queue Allocation:");
            println!("Total Queues: {}", result.total_queues);
            println!("\nBrokers:");
            for broker_name in result.broker_names {
                println!("  - {broker_name}");
            }
        } else {
            println!("No route information found for topic: {}", result.topic);
        }
    }
}

impl CommandExecute for AllocateMQSubCommand {
    async fn execute(&self, _rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let result = TopicService::query_allocated_mq_by_request(self.request()?).await?;
        Self::print_result(result);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocate_mq_sub_command_parse() {
        let cmd = AllocateMQSubCommand::try_parse_from([
            "allocateMQ",
            "-t",
            "TestTopic",
            "-i",
            "192.168.1.1,192.168.1.2",
            "-n",
            "127.0.0.1:9876",
        ])
        .unwrap();

        assert_eq!(cmd.topic, "TestTopic");
        assert_eq!(cmd.ip_list, "192.168.1.1,192.168.1.2");
        assert_eq!(cmd.common_args.namesrv_addr, Some("127.0.0.1:9876".to_string()));
    }
}
