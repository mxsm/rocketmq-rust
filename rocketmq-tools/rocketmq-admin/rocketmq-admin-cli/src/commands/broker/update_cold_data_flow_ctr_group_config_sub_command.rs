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

use clap::ArgGroup;
use clap::Parser;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use rocketmq_admin_core::core::broker::BrokerOperationResult;
use rocketmq_admin_core::core::broker::BrokerService;
use rocketmq_admin_core::core::broker::ColdDataFlowCtrGroupConfigUpdateRequest;

#[derive(Debug, Clone, Parser)]
#[command(group(ArgGroup::new("target")
    .required(true)
    .args(&["broker_addr", "cluster_name"]))
)]
pub struct UpdateColdDataFlowCtrGroupConfigSubCommand {
    #[arg(short = 'b', long = "brokerAddr", required = false, help = "update which broker")]
    broker_addr: Option<String>,

    #[arg(short = 'c', long = "clusterName", required = false, help = "update which cluster")]
    cluster_name: Option<String>,

    #[arg(
        short = 'g',
        long = "consumerGroup",
        required = true,
        help = "specific consumerGroup"
    )]
    consumer_group: String,

    #[arg(short = 'v', long = "threshold", required = true, help = "cold read threshold value")]
    threshold: String,
}

impl UpdateColdDataFlowCtrGroupConfigSubCommand {
    fn request(&self) -> RocketMQResult<ColdDataFlowCtrGroupConfigUpdateRequest> {
        ColdDataFlowCtrGroupConfigUpdateRequest::try_new(
            self.broker_addr.clone(),
            self.cluster_name.clone(),
            self.consumer_group.clone(),
            self.threshold.clone(),
        )
    }

    fn print_result(result: &BrokerOperationResult) -> RocketMQResult<()> {
        for broker_addr in &result.broker_addrs {
            println!(
                "Update cold data flow control group config was successful for broker {}.",
                broker_addr
            );
        }

        if result.failures.is_empty() {
            Ok(())
        } else {
            Err(RocketMQError::Internal(format!(
                "UpdateColdDataFlowCtrGroupConfigSubCommand: Failed to update for brokers {}",
                result
                    .failures
                    .iter()
                    .map(|failure| failure.broker_addr.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )))
        }
    }
}

impl CommandExecute for UpdateColdDataFlowCtrGroupConfigSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let result =
            BrokerService::update_cold_data_flow_ctr_group_config_by_request_with_rpc_hook(self.request()?, rpc_hook)
                .await?;
        Self::print_result(&result)
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[test]
    fn parses_update_cold_data_flow_ctr_group_config_request() {
        let command = UpdateColdDataFlowCtrGroupConfigSubCommand::try_parse_from([
            "updateColdDataFlowCtrGroupConfig",
            "-b",
            " 127.0.0.1:10911 ",
            "-g",
            " GroupA ",
            "-v",
            " 1024 ",
        ])
        .unwrap();
        let request = command.request().unwrap();

        assert_eq!(request.consumer_group().as_str(), "GroupA");
        assert_eq!(request.threshold().as_str(), "1024");
    }
}
