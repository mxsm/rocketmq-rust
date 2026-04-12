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
use rocketmq_remoting::protocol::body::producer_table_info::ProducerTableInfo;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use rocketmq_admin_core::core::producer::ProducerInfoQueryRequest;
use rocketmq_admin_core::core::producer::ProducerInfoQueryResult;
use rocketmq_admin_core::core::producer::ProducerService;

#[derive(Debug, Clone, Parser)]
pub struct ProducerSubCommand {
    #[arg(short = 'b', long = "broker", required = true, help = "broker address")]
    broker_addr: String,
}

impl ProducerSubCommand {
    fn request(&self) -> RocketMQResult<ProducerInfoQueryRequest> {
        ProducerInfoQueryRequest::try_new(self.broker_addr.clone())
    }

    fn print_result(request: &ProducerInfoQueryRequest, result: &ProducerInfoQueryResult) {
        Self::print_producer_table(request.broker_addr().as_str(), &result.producer_table_info);
    }

    fn print_producer_table(broker_addr: &str, producer_table_info: &ProducerTableInfo) {
        let data = producer_table_info.data();
        if data.is_empty() {
            println!("No producer groups found on broker {}", broker_addr);
            return;
        }

        for (group, producers) in data {
            if producers.is_empty() {
                println!("producer group ({}) instances are empty", group);
                continue;
            }
            for producer in producers {
                println!("producer group ({}) instance : {}", group, producer);
            }
        }
    }
}

impl CommandExecute for ProducerSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let request = self.request()?;
        let result = ProducerService::query_producer_info_by_request_with_rpc_hook(request.clone(), rpc_hook).await?;
        Self::print_result(&request, &result);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn producer_sub_command_parse_request() {
        let cmd = ProducerSubCommand::try_parse_from(["producer", "-b", " 127.0.0.1:10911 "]).unwrap();

        assert_eq!(cmd.request().unwrap().broker_addr().as_str(), "127.0.0.1:10911");
    }
}
