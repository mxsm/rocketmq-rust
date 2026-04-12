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
use rocketmq_admin_core::core::broker::ColdDataFlowCtrGroupConfigRemoveRequest;

#[derive(Debug, Clone, Parser)]
#[command(group(ArgGroup::new("target")
    .required(true)
    .args(&["broker_addr", "cluster_name"]))
)]
pub struct RemoveColdDataFlowCtrGroupConfigSubCommand {
    #[arg(short = 'b', long = "brokerAddr", required = false, help = "update which broker")]
    broker_addr: Option<String>,

    #[arg(short = 'c', long = "clusterName", required = false, help = "update which cluster")]
    cluster_name: Option<String>,

    #[arg(
        short = 'g',
        long = "consumerGroup",
        required = true,
        help = "the consumer group will remove from the config"
    )]
    consumer_group: String,
}

impl RemoveColdDataFlowCtrGroupConfigSubCommand {
    fn request(&self) -> RocketMQResult<ColdDataFlowCtrGroupConfigRemoveRequest> {
        ColdDataFlowCtrGroupConfigRemoveRequest::try_new(
            self.broker_addr.clone(),
            self.cluster_name.clone(),
            self.consumer_group.clone(),
        )
    }

    fn print_result(result: &BrokerOperationResult) -> RocketMQResult<()> {
        for broker_addr in &result.broker_addrs {
            println!("remove broker cold read threshold success, {}", broker_addr);
        }

        if result.failures.is_empty() {
            Ok(())
        } else {
            Err(RocketMQError::Internal(format!(
                "RemoveColdDataFlowCtrGroupConfigSubCommand: Failed to remove for brokers {}",
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

impl CommandExecute for RemoveColdDataFlowCtrGroupConfigSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let result =
            BrokerService::remove_cold_data_flow_ctr_group_config_by_request_with_rpc_hook(self.request()?, rpc_hook)
                .await?;
        Self::print_result(&result)
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[test]
    fn parses_remove_cold_data_flow_ctr_group_config_request() {
        let command = RemoveColdDataFlowCtrGroupConfigSubCommand::try_parse_from([
            "removeColdDataFlowCtrGroupConfig",
            "-c",
            " DefaultCluster ",
            "-g",
            " GroupA ",
        ])
        .unwrap();
        let request = command.request().unwrap();

        assert_eq!(request.consumer_group().as_str(), "GroupA");
    }
}
