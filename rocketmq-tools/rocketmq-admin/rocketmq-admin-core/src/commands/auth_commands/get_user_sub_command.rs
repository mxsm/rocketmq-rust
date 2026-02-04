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
use crate::commands::target::Target;
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
        let target = Target::new(&self.cluster_name, &self.broker_addr).map_err(|_| {
            RocketMQError::IllegalArgument(
                "GetUserSubCommand: Specify exactly one of --brokerAddr (-b) or --clusterName (-c)".into(),
            )
        })?;

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
            .map_err(|e| RocketMQError::Internal(format!("GetUserSubCommand: Failed to start MQAdminExt: {}", e)))?;

        let operation_result = async {
            match target {
                Target::BrokerAddr(broker_addr) => {
                    let user_info = get_user_from_broker(&command, &default_mqadmin_ext, &broker_addr).await?;
                    if let Some(user_info) = user_info {
                        print_header();
                        print_user(&user_info);
                    } else {
                        eprintln!("No user with username {} was found", command.username);
                    }
                    Ok(())
                }
                Target::ClusterName(cluster_name) => {
                    let (user_info, failed_broker_addr) =
                        get_user_from_cluster(&command, &default_mqadmin_ext, &cluster_name).await?;
                    print_header();
                    print_users(&user_info);
                    if failed_broker_addr.is_empty() || !user_info.is_empty() {
                        Ok(())
                    } else {
                        Err(RocketMQError::Internal(format!(
                            "GetUserSubCommand: Failed to get user for brokers {}",
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

async fn get_user_from_broker(
    parsed_command: &ParseGetUserSubCommand,
    default_mqadmin_ext: &DefaultMQAdminExt,
    broker_addr: &str,
) -> Result<Option<UserInfo>, RocketMQError> {
    match default_mqadmin_ext
        .get_user(broker_addr.into(), parsed_command.username.clone())
        .await
    {
        Ok(user_info) => {
            println!("Get user command was successful for broker {}.", broker_addr);
            Ok(user_info)
        }
        Err(e) => Err(RocketMQError::Internal(format!(
            "GetUserSubCommand: Failed to get user for broker {}: {}",
            broker_addr, e
        ))),
    }
}

async fn get_user_from_cluster(
    parsed_command: &ParseGetUserSubCommand,
    default_mqadmin_ext: &DefaultMQAdminExt,
    cluster_name: &str,
) -> Result<(Vec<UserInfo>, Vec<CheetahString>), RocketMQError> {
    let cluster_info = default_mqadmin_ext.examine_broker_cluster_info().await?;

    match CommandUtil::fetch_master_addr_by_cluster_name(&cluster_info, cluster_name) {
        Ok(addresses) => {
            let results: Vec<Result<Option<UserInfo>, CheetahString>> =
                futures::future::join_all(addresses.into_iter().map(|addr| async {
                    get_user_from_broker(parsed_command, default_mqadmin_ext, addr.as_str())
                        .await
                        .map_err(|_err| addr)
                }))
                .await;

            let (users, errors): (Vec<_>, Vec<_>) = results.into_iter().partition(Result::is_ok);
            let users: Vec<UserInfo> = users.into_iter().filter_map(Result::unwrap).collect();
            let failed_addr: Vec<CheetahString> = errors.into_iter().map(Result::unwrap_err).collect();

            Ok((users, failed_addr))
        }
        Err(e) => Err(RocketMQError::Internal(format!(
            "GetUserSubCommand: Failed to get user: {}",
            e
        ))),
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

fn print_user(user: &UserInfo) {
    println!("{}", format_row(user));
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

#[cfg(test)]
mod tests {

    use clap::Parser;

    use crate::commands::auth_commands::get_user_sub_command::GetUserSubCommand;

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
