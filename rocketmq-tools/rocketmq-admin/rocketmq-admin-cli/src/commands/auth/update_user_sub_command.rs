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

use crate::commands::CommandExecute;
use crate::commands::command_util::CommandUtil;
use cheetah_string::CheetahString;
use clap::ArgGroup;
use clap::Parser;
use rocketmq_admin_core::admin::default_mq_admin_ext::DefaultMQAdminExt;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;
use std::sync::Arc;

#[derive(Debug, Clone, Parser)]
#[command(group(ArgGroup::new("target")
    .required(true)
    .args(&["cluster_name", "broker_addr"])),
    group(ArgGroup::new("update_field")
    .required(true)
    .args(&["password", "user_type","user_status"]))
)]
pub struct UpdateUserSubCommand {
    #[arg(short = 'c', long = "clusterName", required = false)]
    cluster_name: Option<String>,

    #[arg(short = 'b', long = "brokerAddr", required = false)]
    broker_addr: Option<String>,

    #[arg(short = 'u', long = "username", required = true)]
    username: String,

    #[arg(short = 'p', long = "password")]
    password: Option<String>,

    #[arg(short = 't', long = "userType")]
    user_type: Option<String>,

    #[arg(short = 's', long = "userStatus")]
    user_status: Option<String>,
}

#[derive(Clone)]
struct ParsedUpdateUserSubCommand {
    cluster_name: Option<CheetahString>,
    broker_addr: Option<CheetahString>,
    username: CheetahString,
    password: CheetahString,
    user_type: CheetahString,
    user_status: CheetahString,
}

impl ParsedUpdateUserSubCommand {
    fn new(command: &UpdateUserSubCommand) -> Result<Self, RocketMQError> {
        let username = command.username.trim();
        if username.is_empty() {
            return Err(RocketMQError::IllegalArgument(
                "UpdateUserSubCommand: username cannot be empty".into(),
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
            password: command
                .password
                .as_deref()
                .map(|s| CheetahString::from(s.trim()))
                .unwrap_or_default(),
            user_type: command
                .user_type
                .as_deref()
                .map(|s| CheetahString::from(s.trim()))
                .unwrap_or_default(),
            user_status: command
                .user_status
                .as_deref()
                .map(|s| CheetahString::from(s.trim()))
                .unwrap_or_default(),
        })
    }
}

impl CommandExecute for UpdateUserSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let parsed_command = ParsedUpdateUserSubCommand::new(self)?;

        let mut default_mqadmin_ext = if let Some(rpc_hook) = rpc_hook {
            DefaultMQAdminExt::with_rpc_hook(rpc_hook)
        } else {
            DefaultMQAdminExt::new()
        };
        default_mqadmin_ext
            .client_config_mut()
            .set_instance_name(current_millis().to_string().into());

        MQAdminExt::start(&mut default_mqadmin_ext)
            .await
            .map_err(|e| RocketMQError::Internal(format!("UpdateUserSubCommand: Failed to start MQAdminExt: {}", e)))?;

        let operation_result = update_user(&parsed_command, &default_mqadmin_ext).await;

        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;
        operation_result
    }
}

async fn update_user(
    parsed_command: &ParsedUpdateUserSubCommand,
    default_mqadmin_ext: &DefaultMQAdminExt,
) -> RocketMQResult<()> {
    if let Some(ref broker) = parsed_command.broker_addr {
        default_mqadmin_ext
            .update_user(
                broker.clone(),
                parsed_command.username.clone(),
                parsed_command.password.clone(),
                parsed_command.user_type.clone(),
                parsed_command.user_status.clone(),
            )
            .await?;
        println!("update user to {} success.", broker);
    } else if let Some(ref cluster_name) = parsed_command.cluster_name {
        let cluster_info = default_mqadmin_ext.examine_broker_cluster_info().await?;

        let addresses = CommandUtil::fetch_master_addr_by_cluster_name(&cluster_info, cluster_name.as_str())?;
        for address in addresses {
            default_mqadmin_ext
                .update_user(
                    address.clone(),
                    parsed_command.username.clone(),
                    parsed_command.password.clone(),
                    parsed_command.user_type.clone(),
                    parsed_command.user_status.clone(),
                )
                .await?;
            println!("update user to {} success.", address);
        }
    }

    Ok(())
}
