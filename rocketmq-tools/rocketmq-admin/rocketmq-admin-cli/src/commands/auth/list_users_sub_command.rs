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

use rocketmq_admin_core::core::auth::AuthService;
use rocketmq_admin_core::core::auth::ListUsersRequest;
use rocketmq_admin_core::core::auth::ListUsersResult;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::body::user_info::UserInfo;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;

#[derive(Debug, Clone, Parser)]
#[command(group(ArgGroup::new("target")
    .required(true)
    .args(&["cluster_name", "broker_addr"])))]
pub struct ListUsersSubCommand {
    #[arg(short = 'c', long = "clusterName", required = false)]
    cluster_name: Option<String>,

    #[arg(short = 'b', long = "brokerAddr", required = false)]
    broker_addr: Option<String>,

    #[arg(short = 'f', long = "filter", required = false)]
    filter: Option<String>,
}

impl CommandExecute for ListUsersSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let request =
            ListUsersRequest::try_new(self.broker_addr.clone(), self.cluster_name.clone(), self.filter.clone())?;
        let result = AuthService::list_users_by_request_with_rpc_hook(request, rpc_hook).await?;
        render_list_users_result(result)
    }
}

fn print_users(users: Vec<UserInfo>) {
    println!(
        "{}",
        format_row(&UserInfo {
            username: Some("#UserName".into()),
            password: Some("#Password".into()),
            user_type: Some("#UserType".into()),
            user_status: Some("#UserStatus".into()),
        })
    );
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

fn render_list_users_result(result: ListUsersResult) -> RocketMQResult<()> {
    print_users(result.users);
    if result.failed_broker_addrs.is_empty() {
        Ok(())
    } else {
        Err(RocketMQError::Internal(format!(
            "ListUsersSubCommand: Failed to list users for brokers {}",
            result.failed_broker_addrs.join(", ")
        )))
    }
}

#[cfg(test)]
mod tests {

    use clap::Parser;

    use crate::commands::auth::list_users_sub_command::ListUsersSubCommand;

    #[test]
    fn test_list_users_sub_command_with_broker_addr_using_short_commands() {
        let args = [vec![""], vec!["-b", "127.0.0.1:3434"], vec!["-f", "some_filter"]];

        let args = args.concat();

        let cmd = ListUsersSubCommand::try_parse_from(args).unwrap();
        assert_eq!(Some("127.0.0.1:3434"), cmd.broker_addr.as_deref());
        assert_eq!(Some("some_filter"), cmd.filter.as_deref());
    }

    #[test]
    fn test_list_users_sub_command_with_cluster_name_using_short_commands() {
        let args = [vec![""], vec!["-c", "DefaultCluster"], vec!["-f", "some_filter"]];

        let args = args.concat();

        let cmd = ListUsersSubCommand::try_parse_from(args).unwrap();
        assert_eq!(Some("DefaultCluster"), cmd.cluster_name.as_deref());
        assert_eq!(Some("some_filter"), cmd.filter.as_deref());
    }

    #[test]
    fn test_list_users_sub_command_using_conflicting_commands() {
        let args = [
            vec![""],
            vec!["-b", "127.0.0.1:3434"],
            vec!["-c", "DefaultCluster"],
            vec!["-f", "some_filter"],
        ];

        let args = args.concat();

        let result = ListUsersSubCommand::try_parse_from(args);
        assert!(result.is_err());
    }
}
