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

use cheetah_string::CheetahString;
use rocketmq_admin_core::core::auth::AuthService;
use rocketmq_admin_core::core::auth::GetUserRequest;
use rocketmq_admin_core::core::auth::GetUserResult;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::body::user_info::UserInfo;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;

#[derive(Debug, Clone, Parser)]
#[command(group(ArgGroup::new("target")
    .required(true)
    .args(&["cluster_name", "broker_addr"])))]
pub struct GetUserSubCommand {
    #[arg(short = 'c', long = "clusterName", required = false)]
    cluster_name: Option<String>,

    #[arg(short = 'b', long = "brokerAddr", required = false)]
    broker_addr: Option<String>,

    #[arg(short = 'u', long = "username", required = true)]
    username: String,
}

#[derive(Clone)]
struct ParseGetUserSubCommand {
    username: CheetahString,
}

impl ParseGetUserSubCommand {
    fn new(command: &GetUserSubCommand) -> Result<Self, RocketMQError> {
        let username = command.username.trim();
        if username.is_empty() {
            Err(RocketMQError::IllegalArgument(
                "GetUserSubCommand: username is empty".into(),
            ))
        } else {
            Ok(Self {
                username: username.into(),
            })
        }
    }
}

impl CommandExecute for GetUserSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let command = ParseGetUserSubCommand::new(self)?;
        let request = GetUserRequest::try_new(
            self.broker_addr.clone(),
            self.cluster_name.clone(),
            command.username.clone().to_string(),
        )?;
        let result = AuthService::get_user_by_request_with_rpc_hook(request, rpc_hook).await?;
        render_get_user_result(&command, result, self.broker_addr.is_some())
    }
}

fn print_header() {
    println!(
        "{}",
        format_row(&UserInfo {
            username: Some("#UserName".into()),
            password: Some("#Password".into()),
            user_type: Some("#UserType".into()),
            user_status: Some("#UserStatus".into()),
        })
    );
}

fn print_users(users: &[UserInfo]) {
    users.iter().for_each(|user| println!("{}", format_row(user)));
    println!("Total users: {}", users.len());
}

fn format_row(user: &UserInfo) -> String {
    format!(
        "{:<16}  {:<22}  {:<22}",
        user.username.as_deref().unwrap_or(""),
        user.user_type.as_deref().unwrap_or(""),
        user.user_status.as_deref().unwrap_or(""),
    )
}

fn render_get_user_result(
    command: &ParseGetUserSubCommand,
    result: GetUserResult,
    single_broker_target: bool,
) -> RocketMQResult<()> {
    if single_broker_target && result.users.is_empty() {
        eprintln!("No user with username {} was found", command.username);
        return Ok(());
    }

    print_header();
    print_users(&result.users);
    if result.failed_broker_addrs.is_empty() || !result.users.is_empty() {
        Ok(())
    } else {
        Err(RocketMQError::Internal(format!(
            "GetUserSubCommand: Failed to get user for brokers {}",
            result.failed_broker_addrs.join(", ")
        )))
    }
}

#[cfg(test)]
mod tests {

    use clap::Parser;

    use crate::commands::auth::get_user_sub_command::GetUserSubCommand;

    #[test]
    fn test_get_user_sub_command_with_broker_addr_using_short_commands() {
        let args = [vec![""], vec!["-b", "127.0.0.1:3434"], vec!["-u", "alice"]];

        let args = args.concat();

        let cmd = GetUserSubCommand::try_parse_from(args).unwrap();
        assert_eq!(Some("127.0.0.1:3434"), cmd.broker_addr.as_deref());
        assert_eq!("alice", cmd.username);
    }

    #[test]
    fn test_get_user_sub_command_with_cluster_name_using_short_commands() {
        let args = [vec![""], vec!["-c", "DefaultCluster"], vec!["-u", "alice"]];

        let args = args.concat();

        let cmd = GetUserSubCommand::try_parse_from(args).unwrap();
        assert_eq!(Some("DefaultCluster"), cmd.cluster_name.as_deref());
        assert_eq!("alice", cmd.username);
    }

    #[test]
    fn test_get_user_sub_command_using_conflicting_commands() {
        let args = [
            vec![""],
            vec!["-b", "127.0.0.1:3434"],
            vec!["-c", "DefaultCluster"],
            vec!["-u", "alice"],
        ];

        let args = args.concat();

        let result = GetUserSubCommand::try_parse_from(args);
        assert!(result.is_err());
    }
}
