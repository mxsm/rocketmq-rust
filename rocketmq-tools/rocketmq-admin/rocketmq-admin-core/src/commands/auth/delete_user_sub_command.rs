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

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::commands::command_util::CommandUtil;
use crate::commands::CommandExecute;
use cheetah_string::CheetahString;
use clap::ArgGroup;
use clap::Parser;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;
use std::sync::Arc;

#[derive(Debug, Clone, Parser)]
#[command(group(ArgGroup::new("target")
    .required(true)
    .args(&["cluster_name", "broker_addr"]))
)]
pub struct DeleteUserSubCommand {
    #[arg(
        short = 'c',
        long = "clusterName",
        required = false,
        help = "delete user from which cluster"
    )]
    cluster_name: Option<String>,

    #[arg(
        short = 'b',
        long = "brokerAddr",
        required = false,
        help = "delete user from which broker"
    )]
    broker_addr: Option<String>,

    #[arg(
        short = 'u',
        long = "username",
        required = true,
        help = "the username of user to delete"
    )]
    username: String,
}

#[derive(Clone)]
struct ParsedDeleteUserSubCommand {
    cluster_name: Option<CheetahString>,
    broker_addr: Option<CheetahString>,
    username: CheetahString,
}

impl ParsedDeleteUserSubCommand {
    fn new(command: &DeleteUserSubCommand) -> Result<Self, RocketMQError> {
        let username = command.username.trim();
        if username.is_empty() {
            return Err(RocketMQError::IllegalArgument(
                "DeleteUserSubCommand: username cannot be empty".into(),
            ));
        }

        Ok(Self {
            cluster_name: command
                .cluster_name
                .as_deref()
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(CheetahString::from),
            broker_addr: command
                .broker_addr
                .as_deref()
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(CheetahString::from),
            username: username.into(),
        })
    }
}

impl CommandExecute for DeleteUserSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let parsed_command = ParsedDeleteUserSubCommand::new(self)?;

        let mut default_mqadmin_ext = if let Some(rpc_hook) = rpc_hook {
            DefaultMQAdminExt::with_rpc_hook(rpc_hook)
        } else {
            DefaultMQAdminExt::new()
        };
        default_mqadmin_ext
            .client_config_mut()
            .set_instance_name(get_current_millis().to_string().into());

        MQAdminExt::start(&mut default_mqadmin_ext)
            .await
            .map_err(|e| RocketMQError::Internal(format!("DeleteUserSubCommand: Failed to start MQAdminExt: {}", e)))?;

        let operation_result = delete_user(&parsed_command, &default_mqadmin_ext).await;

        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;
        operation_result
    }
}

async fn delete_user(
    parsed_command: &ParsedDeleteUserSubCommand,
    default_mqadmin_ext: &DefaultMQAdminExt,
) -> RocketMQResult<()> {
    if let Some(ref broker) = parsed_command.broker_addr {
        default_mqadmin_ext
            .delete_user(broker.clone(), parsed_command.username.clone())
            .await?;
        println!("delete user to {} success.", broker);
    } else if let Some(ref cluster_name) = parsed_command.cluster_name {
        let cluster_info = default_mqadmin_ext.examine_broker_cluster_info().await?;

        let addresses = CommandUtil::fetch_master_and_slave_addr_by_cluster_name(&cluster_info, cluster_name.as_str())?;
        for address in addresses {
            default_mqadmin_ext
                .delete_user(address.clone(), parsed_command.username.clone())
                .await?;
            println!("delete user to {} success.", address);
        }
    }

    Ok(())
}
