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
use rocketmq_common::FileUtils::string_to_file;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_common::common::mq_version::RocketMqVersion;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use rocketmq_admin_core::core::consumer::ConsumerRunningInfoRequest;
use rocketmq_admin_core::core::consumer::ConsumerRunningInfoResult;
use rocketmq_admin_core::core::consumer::ConsumerService;

#[derive(Debug, Clone, Parser)]
pub struct ConsumerSubCommand {
    #[arg(short = 'g', long = "consumerGroup", required = true, help = "consumer group name")]
    consumer_group: String,

    #[arg(
        short = 's',
        long = "jstack",
        required = false,
        default_value = "false",
        help = "Run jstack command in the consumer progress"
    )]
    jstack: bool,

    #[arg(short = 'i', long = "clientId", required = false, help = "The consumer's client id")]
    client_id: Option<String>,

    #[arg(
        short = 'n',
        long = "namesrvAddr",
        required = false,
        help = "Name server address list, eg: '192.168.0.1:9876;192.168.0.2:9876'"
    )]
    namesrv_addr: Option<String>,
}

impl ConsumerSubCommand {
    fn request(&self) -> RocketMQResult<ConsumerRunningInfoRequest> {
        ConsumerRunningInfoRequest::try_new(
            self.consumer_group.clone(),
            self.client_id.clone(),
            None,
            self.jstack,
            self.namesrv_addr.clone(),
        )
    }

    fn print_result(&self, result: ConsumerRunningInfoResult) -> RocketMQResult<()> {
        if self.client_id.is_some() {
            for item in result.items {
                println!("{}", item.running_info);
            }
            return Ok(());
        }

        let now = current_millis();
        for (index, item) in result.items.iter().enumerate() {
            let file_path = format!("{}/{}", now, item.client_id);
            if let Err(e) = string_to_file(&format!("{}", item.running_info), file_path.clone()) {
                eprintln!("Failed to write consumer running info to file: {}", e);
            }
            let version_desc = RocketMqVersion::from_ordinal(item.version as u32).name();
            println!(
                "{:03}  {:<40} {:<20} {}",
                index + 1,
                item.client_id,
                version_desc,
                file_path
            );
        }

        if let Some(subscription_consistent) = result.subscription_consistent {
            if subscription_consistent {
                println!("\n\nSame subscription in the same group of consumer");
                println!("\n\nRebalance OK");
                for analysis in result.process_queue_analysis {
                    println!("{analysis}");
                }
            } else {
                println!("\n\nWARN: Different subscription in the same group of consumer!!!");
            }
        }
        Ok(())
    }
}

impl CommandExecute for ConsumerSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let result =
            ConsumerService::query_consumer_running_info_by_request_with_rpc_hook(self.request()?, rpc_hook).await?;
        self.print_result(result)
    }
}
