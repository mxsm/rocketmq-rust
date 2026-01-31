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

use cheetah_string::CheetahString;
use clap::Parser;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::body::user_info::UserInfo;
use rocketmq_remoting::runtime::RPCHook;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::commands::CommandExecute;

#[derive(Debug, Clone, Parser)]
pub struct CopyUsersSubCommand {
    #[arg(
        short = 'f',
        long = "fromBroker",
        required = true,
        help = "the source broker that the users copy from"
    )]
    from_broker: String,

    #[arg(
        short = 't',
        long = "toBroker",
        required = true,
        help = "the target broker that the users copy to"
    )]
    to_broker: String,

    #[arg(
        short = 'u',
        long = "usernames",
        required = false,
        help = "the username list of user to copy"
    )]
    usernames: Option<String>,
}

#[derive(Clone)]
struct ParsedCopyUsersSubCommand {
    from_broker: CheetahString,
    to_broker: CheetahString,
    usernames: Option<Vec<CheetahString>>,
}

impl ParsedCopyUsersSubCommand {
    fn new(command: &CopyUsersSubCommand) -> Result<Self, RocketMQError> {
        let from_broker: CheetahString = {
            let from_broker = command.from_broker.trim();
            if from_broker.is_empty() {
                return Err(RocketMQError::IllegalArgument(
                    "CopyUsersSubCommand: fromBroker cannot be empty".into(),
                ));
            }
            from_broker.into()
        };

        let to_broker: CheetahString = {
            let to_broker = command.to_broker.trim();
            if to_broker.is_empty() {
                return Err(RocketMQError::IllegalArgument(
                    "CopyUsersSubCommand: toBroker cannot be empty".into(),
                ));
            }
            to_broker.into()
        };

        let usernames: Option<Vec<CheetahString>> = command
            .usernames
            .as_ref()
            .map(|usernames| {
                usernames
                    .split(',')
                    .map(|username| username.trim())
                    .filter(|username| !username.is_empty())
                    .map(|username| username.into())
                    .collect::<Vec<_>>()
            })
            .filter(|v: &Vec<CheetahString>| !v.is_empty());

        Ok(Self {
            from_broker,
            to_broker,
            usernames,
        })
    }
}

impl CommandExecute for CopyUsersSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let command = ParsedCopyUsersSubCommand::new(self)?;

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
            .map_err(|e| RocketMQError::Internal(format!("CopyUsersSubCommand: Failed to start MQAdminExt: {}", e)))?;

        let operation_result = copy_users(&command, &default_mqadmin_ext).await;

        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;
        operation_result
    }
}

async fn copy_users(
    parsed_command: &ParsedCopyUsersSubCommand,
    default_mqadmin_ext: &DefaultMQAdminExt,
) -> Result<(), RocketMQError> {
    let source_broker = parsed_command.from_broker.as_str();
    let target_broker = parsed_command.to_broker.as_str();

    let user_infos: Vec<UserInfo> = if let Some(ref usernames) = parsed_command.usernames {
        let mut user_list = Vec::new();
        for username in usernames {
            match default_mqadmin_ext
                .get_user(source_broker.into(), username.clone())
                .await
            {
                Ok(user_info) => {
                    user_list.push(user_info);
                }
                Err(e) => {
                    eprintln!("Warning: Failed to get user {} from {}: {}", username, source_broker, e);
                }
            }
        }
        user_list
    } else {
        default_mqadmin_ext
            .list_users(source_broker.into(), CheetahString::default())
            .await
            .map_err(|e| {
                RocketMQError::Internal(format!(
                    "CopyUsersSubCommand: Failed to list users from {}: {}",
                    source_broker, e
                ))
            })?
    };

    if user_infos.is_empty() {
        println!("No users found to copy from {}.", source_broker);
        return Ok(());
    }

    for user_info in user_infos {
        let username = match user_info.username.as_ref() {
            Some(u) => u.clone(),
            None => {
                eprintln!("Warning: User has no username, skipping.");
                continue;
            }
        };

        let target_user_result = default_mqadmin_ext
            .get_user(target_broker.into(), username.clone())
            .await;

        let copy_result = if target_user_result.is_err() {
            default_mqadmin_ext
                .create_user_with_user_info(target_broker.into(), user_info.clone())
                .await
        } else {
            default_mqadmin_ext
                .update_user_with_user_info(target_broker.into(), user_info.clone())
                .await
        };

        match copy_result {
            Ok(_) => {
                println!(
                    "copy user of {} from {} to {} success.",
                    username, source_broker, target_broker
                );
            }
            Err(e) => {
                eprintln!(
                    "copy user of {} from {} to {} failed: {}",
                    username, source_broker, target_broker, e
                );
            }
        }
    }

    Ok(())
}
