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
use rocketmq_remoting::protocol::body::acl_info::AclInfo;
use rocketmq_remoting::runtime::RPCHook;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::commands::command_util::CommandUtil;
use crate::commands::target::Target;
use crate::commands::CommandExecute;

#[derive(Debug, Clone, Parser)]
#[command(group(ArgGroup::new("target")
    .required(true)
    .args(&["cluster_name", "broker_addr"])))]
pub struct GetAclSubCommand {
    #[arg(short = 'c', long = "clusterName", required = false)]
    cluster_name: Option<String>,

    #[arg(short = 'b', long = "brokerAddr", required = false)]
    broker_addr: Option<String>,

    #[arg(short = 's', long = "subject", required = true)]
    subject: String,
}

#[derive(Clone)]
struct ParsedGetAclSubCommand {
    subject: CheetahString,
}

impl ParsedGetAclSubCommand {
    fn new(command: &GetAclSubCommand) -> Result<Self, RocketMQError> {
        let subject = command.subject.trim();
        if subject.is_empty() {
            Err(RocketMQError::IllegalArgument(
                "GetAclSubCommand: subject cannot be empty".into(),
            ))
        } else {
            Ok(Self {
                subject: subject.into(),
            })
        }
    }
}

impl CommandExecute for GetAclSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let command = ParsedGetAclSubCommand::new(self)?;
        let target = Target::new(&self.cluster_name, &self.broker_addr).map_err(|_| {
            RocketMQError::IllegalArgument(
                "GetAclSubCommand: Specify exactly one of --brokerAddr (-b) or --clusterName (-c)".into(),
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
            .map_err(|e| RocketMQError::Internal(format!("GetAclSubCommand: Failed to start MQAdminExt: {}", e)))?;

        let operation_result = async {
            match target {
                Target::BrokerAddr(broker_addr) => {
                    let acl_info = get_acl_from_broker(&command, &default_mqadmin_ext, &broker_addr).await?;
                    if let Some(acl_info) = acl_info {
                        print_header();
                        print_acl(&acl_info);
                    } else {
                        eprintln!("No ACL with subject {} was found", command.subject);
                    }
                    Ok(())
                }
                Target::ClusterName(cluster_name) => {
                    let (acl_infos, failed_broker_addr) =
                        get_acl_from_cluster(&command, &default_mqadmin_ext, &cluster_name).await?;

                    if acl_infos.is_empty() && failed_broker_addr.is_empty() {
                        eprintln!("No ACL with subject {} was found", command.subject);
                    } else {
                        print_header();
                        print_acls(&acl_infos);
                    }

                    if !failed_broker_addr.is_empty() {
                        eprintln!(
                            "Warning: failed to get ACL from brokers: {}",
                            failed_broker_addr.join(", ")
                        );
                    }

                    if !failed_broker_addr.is_empty() && acl_infos.is_empty() {
                        Err(RocketMQError::Internal(format!(
                            "GetAclSubCommand: Failed to get ACL for brokers {}",
                            failed_broker_addr.join(", ")
                        )))
                    } else {
                        Ok(())
                    }
                }
            }
        }
        .await;
        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;
        operation_result
    }
}

async fn get_acl_from_broker(
    parsed_command: &ParsedGetAclSubCommand,
    default_mqadmin_ext: &DefaultMQAdminExt,
    broker_addr: &str,
) -> Result<Option<AclInfo>, RocketMQError> {
    match default_mqadmin_ext
        .get_acl(broker_addr.into(), parsed_command.subject.clone())
        .await
    {
        Ok(acl_info) => {
            println!("Get ACL command was successful for broker {}.", broker_addr);
            Ok(Some(acl_info))
        }
        Err(e) => Err(RocketMQError::Internal(format!(
            "GetAclSubCommand: Failed to get ACL for broker {}: {}",
            broker_addr, e
        ))),
    }
}

async fn get_acl_from_cluster(
    parsed_command: &ParsedGetAclSubCommand,
    default_mqadmin_ext: &DefaultMQAdminExt,
    cluster_name: &str,
) -> Result<(Vec<AclInfo>, Vec<CheetahString>), RocketMQError> {
    let cluster_info = default_mqadmin_ext.examine_broker_cluster_info().await?;

    match CommandUtil::fetch_master_addr_by_cluster_name(&cluster_info, cluster_name) {
        Ok(addresses) => {
            let results: Vec<Result<Option<AclInfo>, CheetahString>> =
                futures::future::join_all(addresses.into_iter().map(|addr| async {
                    get_acl_from_broker(parsed_command, default_mqadmin_ext, addr.as_str())
                        .await
                        .map_err(|_err| addr)
                }))
                .await;

            let (acls, errors): (Vec<_>, Vec<_>) = results.into_iter().partition(Result::is_ok);
            let acls: Vec<AclInfo> = acls.into_iter().filter_map(Result::unwrap).collect();
            let failed_addr: Vec<CheetahString> = errors.into_iter().map(Result::unwrap_err).collect();

            Ok((acls, failed_addr))
        }
        Err(e) => Err(RocketMQError::Internal(format!(
            "GetAclSubCommand: Failed to get ACL: {}",
            e
        ))),
    }
}

fn print_header() {
    println!(
        "{:<24}  {:<12}  {:<24}  {:<20}  {:<24}  {:<12}",
        "#Subject", "#PolicyType", "#Resource", "#Actions", "#SourceIp", "#Decision"
    );
}

fn print_acl(acl: &AclInfo) {
    let subject = acl.subject.as_ref().map(|s| s.as_str()).unwrap_or("*");
    let policies = acl.policies.as_ref();

    if policies.is_none() || policies.unwrap().is_empty() {
        println!(
            "{:<24}  {:<12}  {:<24}  {:<20}  {:<24}  {:<12}",
            subject, "", "", "", "", ""
        );
        return;
    }

    for policy in policies.unwrap() {
        let policy_type = policy.policy_type.as_ref().map(|p| p.as_str()).unwrap_or("");
        let entries = policy.entries.as_ref();

        if entries.is_none() || entries.unwrap().is_empty() {
            continue;
        }

        for entry in entries.unwrap() {
            let resource = entry.resource.as_ref().map(|r| r.as_str()).unwrap_or("");
            let actions = entry.actions.as_ref().map(|a| a.as_str()).unwrap_or("");
            let source_ips = entry
                .source_ips
                .as_ref()
                .filter(|ips| !ips.is_empty())
                .map(|ips| ips.iter().map(|ip| ip.as_str()).collect::<Vec<_>>().join(","))
                .unwrap_or_else(|| "".to_string());
            let decision = entry.decision.as_ref().map(|d| d.as_str()).unwrap_or("");

            println!(
                "{:<24}  {:<12}  {:<24}  {:<20}  {:<24}  {:<12}",
                subject, policy_type, resource, actions, source_ips, decision
            );
        }
    }
}

fn print_acls(acls: &[AclInfo]) {
    acls.iter().for_each(print_acl);
}

#[cfg(test)]
mod tests {
    use crate::commands::auth_commands::get_acl_sub_command::GetAclSubCommand;
    use clap::Parser;

    #[test]
    fn test_get_acl_sub_command_with_broker_addr_using_short_commands() {
        let args = [vec![""], vec!["-b", "127.0.0.1:3434"], vec!["-s", "user:alice"]];
        let args = args.concat();
        let cmd = GetAclSubCommand::try_parse_from(args).unwrap();
        assert_eq!(Some("127.0.0.1:3434"), cmd.broker_addr.as_deref());
        assert_eq!("user:alice", cmd.subject.as_str());
    }

    #[test]
    fn test_get_acl_sub_command_with_cluster_name_using_short_commands() {
        let args = [vec![""], vec!["-c", "DefaultCluster"], vec!["-s", "user:alice"]];
        let args = args.concat();
        let cmd = GetAclSubCommand::try_parse_from(args).unwrap();
        assert_eq!(Some("DefaultCluster"), cmd.cluster_name.as_deref());
        assert_eq!("user:alice", cmd.subject.as_str());
    }

    #[test]
    fn test_get_acl_sub_command_using_conflicting_commands() {
        let args = [
            vec![""],
            vec!["-b", "127.0.0.1:3434"],
            vec!["-c", "DefaultCluster"],
            vec!["-s", "user:alice"],
        ];
        let args = args.concat();
        let result = GetAclSubCommand::try_parse_from(args);
        assert!(result.is_err());
    }
}
