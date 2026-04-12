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
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use crate::commands::CommonArgs;
use rocketmq_admin_core::core::broker::BrokerTarget;
use rocketmq_admin_core::core::consumer::ConsumerOperationResult;
use rocketmq_admin_core::core::consumer::ConsumerService;
use rocketmq_admin_core::core::consumer::DeleteSubscriptionGroupRequest;

#[derive(Debug, Clone, Parser)]
pub struct DeleteSubscriptionGroupSubCommand {
    #[command(flatten)]
    common_args: CommonArgs,

    #[arg(
        short = 'b',
        long = "brokerAddr",
        required = false,
        conflicts_with = "cluster_name",
        help = "delete subscription group from which broker"
    )]
    broker_addr: Option<String>,

    #[arg(
        short = 'c',
        long = "clusterName",
        required = false,
        conflicts_with = "broker_addr",
        help = "delete subscription group from which cluster"
    )]
    cluster_name: Option<String>,

    #[arg(short = 'g', long = "groupName", required = true, help = "subscription group name")]
    group_name: String,

    #[arg(
        short = 'r',
        long = "removeOffset",
        required = false,
        default_value = "false",
        help = "remove offset"
    )]
    remove_offset: bool,
}

impl DeleteSubscriptionGroupSubCommand {
    fn request(&self) -> RocketMQResult<DeleteSubscriptionGroupRequest> {
        DeleteSubscriptionGroupRequest::try_new(
            self.broker_addr.clone(),
            self.cluster_name.clone(),
            self.group_name.clone(),
            self.remove_offset,
        )
        .map(|request| request.with_optional_namesrv_addr(self.common_args.namesrv_addr.clone()))
    }

    fn print_result(request: &DeleteSubscriptionGroupRequest, result: ConsumerOperationResult) -> RocketMQResult<()> {
        match request.target() {
            BrokerTarget::BrokerAddr(_) => {
                for broker_addr in &result.broker_addrs {
                    println!(
                        "delete subscription group [{}] from broker [{}] success.",
                        request.group_name(),
                        broker_addr
                    );
                }
            }
            BrokerTarget::ClusterName(cluster_name) => {
                for broker_addr in &result.broker_addrs {
                    println!(
                        "delete subscription group [{}] from broker [{}] in cluster [{}] success.",
                        request.group_name(),
                        broker_addr,
                        cluster_name
                    );
                }
                for warning in &result.warnings {
                    eprintln!("{warning}");
                }
            }
        }

        if result.failures.is_empty() {
            Ok(())
        } else {
            Err(RocketMQError::Internal(format!(
                "DeleteSubscriptionGroupSubCommand: Failed to delete from brokers {}",
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

impl CommandExecute for DeleteSubscriptionGroupSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let request = self.request()?;
        let result =
            ConsumerService::delete_subscription_group_by_request_with_rpc_hook(request.clone(), rpc_hook).await?;
        Self::print_result(&request, result)
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[test]
    fn parses_delete_subscription_group_request() {
        let command = DeleteSubscriptionGroupSubCommand::try_parse_from([
            "deleteSubGroup",
            "-b",
            " 127.0.0.1:10911 ",
            "-g",
            " GroupA ",
            "-r",
            "-n",
            " 127.0.0.1:9876 ",
        ])
        .unwrap();
        let request = command.request().unwrap();

        assert_eq!(request.group_name().as_str(), "GroupA");
        assert!(request.remove_offset());
    }
}
