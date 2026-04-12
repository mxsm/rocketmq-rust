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

use std::path::PathBuf;
use std::sync::Arc;

use clap::ArgGroup;
use clap::Parser;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use rocketmq_remoting::runtime::RPCHook;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

use crate::commands::CommandExecute;
use rocketmq_admin_core::core::consumer::ConsumerOperationResult;
use rocketmq_admin_core::core::consumer::ConsumerService;
use rocketmq_admin_core::core::consumer::UpdateSubscriptionGroupListRequest;

#[derive(Debug, Clone, Parser)]
#[command(group(ArgGroup::new("target").required(true).args(&["broker_addr", "cluster_name"])))]
pub struct UpdateSubGroupListSubCommand {
    #[arg(
        short = 'b',
        long = "brokerAddr",
        conflicts_with = "cluster_name",
        help = "create groups to which broker"
    )]
    broker_addr: Option<String>,

    #[arg(
        short = 'c',
        long = "clusterName",
        conflicts_with = "broker_addr",
        help = "create groups to which cluster"
    )]
    cluster_name: Option<String>,

    #[arg(
        short = 'f',
        long = "filename",
        alias = "file",
        help = "Path to a file with a list of SubscriptionGroupConfig in json format"
    )]
    file: PathBuf,
}

impl UpdateSubGroupListSubCommand {
    async fn request(&self) -> RocketMQResult<UpdateSubscriptionGroupListRequest> {
        let mut group_config_list_bytes = Vec::new();
        File::open(&self.file)
            .await
            .map_err(|e| RocketMQError::Internal(format!("open file error {}", e)))?
            .read_to_end(&mut group_config_list_bytes)
            .await?;

        let group_configs = serde_json::from_slice::<Vec<SubscriptionGroupConfig>>(&group_config_list_bytes)
            .map_err(|e| RocketMQError::Internal(format!("parse json error {}", e)))?;
        UpdateSubscriptionGroupListRequest::try_new(self.broker_addr.clone(), self.cluster_name.clone(), group_configs)
    }

    fn print_result(result: ConsumerOperationResult) -> RocketMQResult<()> {
        for broker_address in &result.broker_addrs {
            println!(
                "submit batch of subscription group config to {} success, please check the result later",
                broker_address
            );
        }
        for failure in &result.failures {
            eprintln!(
                "UpdateSubGroupListSubCommand: Failed to submit subscription group config to {}: {}",
                failure.broker_addr, failure.error
            );
        }
        if result.failures.is_empty() {
            Ok(())
        } else {
            Err(RocketMQError::Internal(format!(
                "UpdateSubGroupListSubCommand: Failed to update brokers: {}",
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

impl CommandExecute for UpdateSubGroupListSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let request = self.request().await?;
        if request.configs().is_empty() {
            return Ok(());
        }
        let result =
            ConsumerService::update_subscription_group_list_by_request_with_rpc_hook(request, rpc_hook).await?;
        Self::print_result(result)
    }
}

#[cfg(test)]
mod tests {
    use super::UpdateSubGroupListSubCommand;
    use clap::Parser;
    use std::path::PathBuf;

    #[test]
    fn test_arguments() {
        let broker_address = "127.0.0.1:10911";
        let input_file_name = "groups.json";

        let args = vec!["mqadmin", "-b", broker_address, "-f", input_file_name];

        let cmd = UpdateSubGroupListSubCommand::try_parse_from(args).unwrap();
        assert_eq!(Some(broker_address), cmd.broker_addr.as_deref());
        assert!(cmd.cluster_name.is_none());
        assert_eq!(PathBuf::from(input_file_name), cmd.file);
    }
}
