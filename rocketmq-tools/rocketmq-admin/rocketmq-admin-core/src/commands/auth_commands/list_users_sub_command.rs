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
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::body::user_info::UserInfo;
use rocketmq_remoting::runtime::RPCHook;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::commands::command_util::CommandUtil;
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

enum Target {
    BrokerAddr(String),
    ClusterName(String),
}

impl Target {
    fn new(cluster_name: &Option<String>, broker_addr: &Option<String>) -> Result<Self, RocketMQError> {
        let cluster_name: Option<String> = cluster_name
            .as_ref()
            .map(|cluster_name| cluster_name.trim())
            .filter(|cluster_name| !cluster_name.is_empty())
            .map(|cluster_name| cluster_name.into());
        let broker_addr: Option<String> = broker_addr
            .as_ref()
            .map(|broker_addr| broker_addr.trim())
            .filter(|broker_addr| !broker_addr.is_empty())
            .map(|broker_addr| broker_addr.into());
        if (cluster_name.is_none() && broker_addr.is_none()) || (cluster_name.is_some() && broker_addr.is_some()) {
            return Err(RocketMQError::IllegalArgument(
                "ListUsersSubCommand: Specify exactly one of --brokerAddr (-b) or --clusterName (-c)".into(),
            ));
        }
        if let Some(cluster_name) = cluster_name {
            Ok(Target::ClusterName(cluster_name))
        } else {
            Ok(Target::BrokerAddr(broker_addr.unwrap()))
        }
    }
}

#[derive(Clone)]
struct ParseListUsersSubCommand {
    filter: Option<CheetahString>,
}

impl ParseListUsersSubCommand {
    fn new(command: &ListUsersSubCommand) -> Result<Self, RocketMQError> {
        let filter: Option<CheetahString> = command
            .filter
            .as_ref()
            .map(|filter| filter.trim())
            .filter(|filter| !filter.is_empty())
            .map(|filter| filter.into());

        Ok(Self { filter })
    }
}

impl CommandExecute for ListUsersSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let command = ParseListUsersSubCommand::new(self)?;
        let target = Target::new(&self.cluster_name, &self.broker_addr)?;

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
            .map_err(|e| RocketMQError::Internal(format!("ListUsersSubCommand: Failed to start MQAdminExt: {}", e)))?;

        let operation_result = async {
            match target {
                Target::BrokerAddr(broker_addr) => {
                    let user_info = get_list_users_broker(&command, &default_mqadmin_ext, &broker_addr).await?;
                    print_users(user_info);
                    Ok(())
                }
                Target::ClusterName(cluster_name) => {
                    let (user_info, failed_broker_addr) =
                        get_list_users_cluster(&command, &default_mqadmin_ext, &cluster_name).await?;
                    print_users(user_info);
                    if failed_broker_addr.is_empty() {
                        Ok(())
                    } else {
                        Err(RocketMQError::Internal(format!(
                            "ListUsersSubCommand: Failed to list users for brokers {}",
                            failed_broker_addr.join(", ")
                        )))
                    }
                }
            }
        }
        .await;
        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;
        operation_result
    }
}

async fn get_list_users_broker(
    parsed_command: &ParseListUsersSubCommand,
    default_mqadmin_ext: &DefaultMQAdminExt,
    broker_addr: &str,
) -> Result<Vec<UserInfo>, RocketMQError> {
    let broker_list_users_result = {
        match default_mqadmin_ext
            .list_users(
                broker_addr.into(),
                parsed_command.filter.clone().unwrap_or_else(|| CheetahString::from("")),
            )
            .await
        {
            Ok(user_info) => {
                println!("List users command was successful for broker {}.", broker_addr);
                Ok(user_info)
            }
            Err(e) => Err(RocketMQError::Internal(format!(
                "ListUsersSubCommand: Failed to list users for broker {}: {}",
                broker_addr, e
            ))),
        }
    };
    broker_list_users_result
}

async fn get_list_users_cluster(
    parsed_command: &ParseListUsersSubCommand,
    default_mqadmin_ext: &DefaultMQAdminExt,
    cluster_name: &str,
) -> Result<(Vec<UserInfo>, Vec<CheetahString>), RocketMQError> {
    let cluster_info = default_mqadmin_ext.examine_broker_cluster_info().await?;

    match CommandUtil::fetch_master_addr_by_cluster_name(&cluster_info, cluster_name) {
        Ok(addresses) => {
            let results: Vec<Result<Vec<UserInfo>, CheetahString>> =
                futures::future::join_all(addresses.into_iter().map(|addr| async {
                    get_list_users_broker(parsed_command, default_mqadmin_ext, addr.as_str())
                        .await
                        .map_err(|_err| addr)
                }))
                .await;

            let (users, errors): (Vec<_>, Vec<_>) = results.into_iter().partition(Result::is_ok);
            let users: Vec<UserInfo> = users.into_iter().flat_map(Result::unwrap).collect();
            let failed_addr: Vec<CheetahString> = errors.into_iter().map(Result::unwrap_err).collect();

            Ok((users, failed_addr))
        }
        Err(e) => Err(RocketMQError::Internal(format!(
            "ListUsersSubCommand: Failed to list users: {}",
            e
        ))),
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

#[cfg(test)]
mod tests {

    use clap::Parser;

    use crate::commands::auth_commands::list_users_sub_command::ListUsersSubCommand;

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
