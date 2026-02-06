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
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use rocketmq_remoting::runtime::RPCHook;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::commands::command_util::CommandUtil;
use crate::commands::CommandExecute;

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

impl CommandExecute for UpdateSubGroupListSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let mut group_config_list_bytes = vec![];
        File::open(&self.file)
            .await
            .map_err(|e| RocketMQError::Internal(format!("open file error {}", e)))?
            .read_to_end(&mut group_config_list_bytes)
            .await?;

        let group_configs = serde_json::from_slice::<Vec<SubscriptionGroupConfig>>(&group_config_list_bytes)
            .map_err(|e| RocketMQError::Internal(format!("parse json error {}", e)))?;

        if group_configs.is_empty() {
            return Ok(());
        }

        let mut default_mq_admin_ext = if let Some(rpc_hook) = rpc_hook {
            DefaultMQAdminExt::with_rpc_hook(rpc_hook)
        } else {
            DefaultMQAdminExt::new()
        };
        default_mq_admin_ext
            .client_config_mut()
            .set_instance_name(get_current_millis().to_string().into());

        MQAdminExt::start(&mut default_mq_admin_ext).await.map_err(|e| {
            RocketMQError::Internal(format!(
                "UpdateSubGroupListSubCommand: Failed to start MQAdminExt: {}",
                e
            ))
        })?;

        let operation_result = async {
            if let Some(broker) = &self.broker_addr {
                let broker_address = broker.trim();
                default_mq_admin_ext
                    .create_and_update_subscription_group_config_list(broker_address.into(), group_configs)
                    .await?;
                println!(
                    "submit batch of group config to {} success, please check the result later",
                    broker_address
                );
                Ok(())
            } else if let Some(cluster) = &self.cluster_name {
                let cluster_name = cluster.trim();
                let master_set = CommandUtil::fetch_master_addr_by_cluster_name(
                    &default_mq_admin_ext.examine_broker_cluster_info().await?,
                    cluster_name,
                )?;
                let mut failed_brokers = Vec::new();
                for broker_address in &master_set {
                    match default_mq_admin_ext
                        .create_and_update_subscription_group_config_list(broker_address.into(), group_configs.clone())
                        .await
                    {
                        Ok(_) => {
                            println!(
                                "submit batch of subscription group config to {} success, please check the result \
                                 later",
                                broker_address
                            );
                        }
                        Err(e) => {
                            eprintln!(
                                "UpdateSubGroupListSubCommand: Failed to submit subscription group config to {}: {}",
                                broker_address, e
                            );
                            failed_brokers.push(broker_address.clone());
                        }
                    }
                }
                if failed_brokers.is_empty() {
                    Ok(())
                } else {
                    Err(RocketMQError::Internal(format!(
                        "UpdateSubGroupListSubCommand: Failed to update brokers: {}",
                        failed_brokers.join(", ")
                    )))
                }
            } else {
                Err(RocketMQError::Internal(
                    "UpdateSubGroupListSubCommand: Specify exactly one of --brokerAddr (-b) or --clusterName (-c)"
                        .to_string(),
                ))
            }
        }
        .await;

        MQAdminExt::shutdown(&mut default_mq_admin_ext).await;
        operation_result
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
