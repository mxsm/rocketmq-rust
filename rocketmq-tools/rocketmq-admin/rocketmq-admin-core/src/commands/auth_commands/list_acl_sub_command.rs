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

use std::collections::HashSet;
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
use crate::commands::CommonArgs;

#[derive(Debug, Clone, Parser)]
#[command(group(ArgGroup::new("target")
    .required(true)
    .args(&["cluster_name", "broker_addr"])))]
pub struct ListAclSubCommand {
    #[command(flatten)]
    common_args: CommonArgs,

    #[arg(short = 'c', long = "clusterName", required = false)]
    cluster_name: Option<String>,

    #[arg(short = 'b', long = "brokerAddr", required = false)]
    broker_addr: Option<String>,

    #[arg(short = 's', long = "subject", required = false)]
    subject: Option<String>,
}

#[derive(Clone)]
struct ParsedListAclSubCommand {
    subject_filter: CheetahString,
}

impl ParsedListAclSubCommand {
    fn new(command: &ListAclSubCommand) -> Result<Self, RocketMQError> {
        let subject_filter = command
            .subject
            .as_ref()
            .map(|subject| subject.trim())
            .filter(|subject| !subject.is_empty())
            .unwrap_or("")
            .into();

        Ok(Self { subject_filter })
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
struct AclRow {
    subject: String,
    resource: String,
    actions: String,
    source_ips: String,
    decision: String,
    policy_type: String,
}

impl CommandExecute for ListAclSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let command = ParsedListAclSubCommand::new(self)?;
        let target = Target::new(&self.cluster_name, &self.broker_addr).map_err(|_| {
            RocketMQError::IllegalArgument(
                "ListAclSubCommand: Specify exactly one of --brokerAddr (-b) or --clusterName (-c)".into(),
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
        if let Some(addr) = &self.common_args.namesrv_addr {
            default_mqadmin_ext.set_namesrv_addr(addr.trim());
        }

        MQAdminExt::start(&mut default_mqadmin_ext)
            .await
            .map_err(|e| RocketMQError::Internal(format!("ListAclSubCommand: Failed to start MQAdminExt: {}", e)))?;

        let operation_result = async {
            match target {
                Target::BrokerAddr(broker_addr) => {
                    let acl_infos = list_acl_from_broker(&command, &default_mqadmin_ext, &broker_addr).await?;
                    print_acls(acl_infos);
                    Ok(())
                }
                Target::ClusterName(cluster_name) => {
                    let (acl_infos, failed_broker_addr) =
                        list_acl_from_cluster(&command, &default_mqadmin_ext, &cluster_name).await?;
                    print_acls(acl_infos);
                    if failed_broker_addr.is_empty() {
                        Ok(())
                    } else {
                        Err(RocketMQError::Internal(format!(
                            "ListAclSubCommand: Failed to list ACLs for brokers {}",
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

async fn list_acl_from_broker(
    parsed_command: &ParsedListAclSubCommand,
    default_mqadmin_ext: &DefaultMQAdminExt,
    broker_addr: &str,
) -> Result<Vec<AclInfo>, RocketMQError> {
    match default_mqadmin_ext
        .list_acl(
            broker_addr.into(),
            parsed_command.subject_filter.clone(),
            CheetahString::default(),
        )
        .await
    {
        Ok(acl_infos) => {
            println!("List ACL command was successful for broker {}.", broker_addr);
            Ok(acl_infos)
        }
        Err(e) => Err(RocketMQError::Internal(format!(
            "ListAclSubCommand: Failed to list ACL for broker {}: {}",
            broker_addr, e
        ))),
    }
}

async fn list_acl_from_cluster(
    parsed_command: &ParsedListAclSubCommand,
    default_mqadmin_ext: &DefaultMQAdminExt,
    cluster_name: &str,
) -> Result<(Vec<AclInfo>, Vec<CheetahString>), RocketMQError> {
    let cluster_info = default_mqadmin_ext.examine_broker_cluster_info().await?;

    match CommandUtil::fetch_master_and_slave_addr_by_cluster_name(&cluster_info, cluster_name) {
        Ok(addresses) => {
            let results: Vec<Result<Vec<AclInfo>, CheetahString>> =
                futures::future::join_all(addresses.into_iter().map(|addr| async {
                    list_acl_from_broker(parsed_command, default_mqadmin_ext, addr.as_str())
                        .await
                        .map_err(|_err| addr)
                }))
                .await;

            let (acls, errors): (Vec<_>, Vec<_>) = results.into_iter().partition(Result::is_ok);
            let acls: Vec<AclInfo> = acls.into_iter().flat_map(Result::unwrap).collect();
            let failed_addr: Vec<CheetahString> = errors.into_iter().map(Result::unwrap_err).collect();

            Ok((acls, failed_addr))
        }
        Err(e) => Err(RocketMQError::Internal(format!(
            "ListAclSubCommand: Failed to list ACL: {}",
            e
        ))),
    }
}

fn print_acls(acls: Vec<AclInfo>) {
    let rows = deduplicate_rows(expand_acl_rows(acls));
    println!(
        "{:<24}  {:<24}  {:<20}  {:<24}  {:<12}  {:<12}",
        "Subject", "Resources", "Actions", "SourceIps", "Decision", "PolicyType"
    );
    println!("{}", "=".repeat(126));
    rows.iter().for_each(|row| {
        println!(
            "{:<24}  {:<24}  {:<20}  {:<24}  {:<12}  {:<12}",
            row.subject, row.resource, row.actions, row.source_ips, row.decision, row.policy_type
        );
    });
    println!();
    println!("Total ACL entries: {}", rows.len());
}

fn expand_acl_rows(acls: Vec<AclInfo>) -> Vec<AclRow> {
    let mut rows = Vec::new();

    for acl in acls {
        let subject = value_or_default(acl.subject.as_ref().map(|v| v.as_str()));
        let policies = acl.policies.unwrap_or_default();
        if policies.is_empty() {
            rows.push(AclRow {
                subject: subject.clone(),
                resource: "*".to_string(),
                actions: "*".to_string(),
                source_ips: "*".to_string(),
                decision: "*".to_string(),
                policy_type: "*".to_string(),
            });
            continue;
        }

        for policy in policies {
            let policy_type = value_or_default(policy.policy_type.as_ref().map(|v| v.as_str()));
            let entries = policy.entries.unwrap_or_default();
            if entries.is_empty() {
                rows.push(AclRow {
                    subject: subject.clone(),
                    resource: "*".to_string(),
                    actions: "*".to_string(),
                    source_ips: "*".to_string(),
                    decision: "*".to_string(),
                    policy_type: policy_type.clone(),
                });
                continue;
            }

            for entry in entries {
                let source_ips = entry
                    .source_ips
                    .as_ref()
                    .filter(|ips| !ips.is_empty())
                    .map(|ips| ips.iter().map(|ip| ip.as_str()).collect::<Vec<_>>().join(","))
                    .unwrap_or_else(|| "*".to_string());

                rows.push(AclRow {
                    subject: subject.clone(),
                    resource: value_or_default(entry.resource.as_ref().map(|v| v.as_str())),
                    actions: value_or_default(entry.actions.as_ref().map(|v| v.as_str())),
                    source_ips,
                    decision: value_or_default(entry.decision.as_ref().map(|v| v.as_str())),
                    policy_type: policy_type.clone(),
                });
            }
        }
    }

    rows
}

fn deduplicate_rows(rows: Vec<AclRow>) -> Vec<AclRow> {
    let mut unique = HashSet::new();
    let mut deduplicated = Vec::new();
    for row in rows {
        if unique.insert(row.clone()) {
            deduplicated.push(row);
        }
    }
    deduplicated.sort();
    deduplicated
}

fn value_or_default(value: Option<&str>) -> String {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| "*".to_string())
}

#[cfg(test)]
mod tests {

    use clap::Parser;

    use crate::commands::auth_commands::list_acl_sub_command::ListAclSubCommand;

    #[test]
    fn test_list_acl_sub_command_with_broker_addr_using_short_commands() {
        let args = [
            vec![""],
            vec!["-n", "127.0.0.1:9876"],
            vec!["-b", "127.0.0.1:10911"],
            vec!["-s", "user:alice"],
        ];

        let args = args.concat();

        let cmd = ListAclSubCommand::try_parse_from(args).unwrap();
        assert_eq!(Some("127.0.0.1:10911"), cmd.broker_addr.as_deref());
        assert_eq!(Some("user:alice"), cmd.subject.as_deref());
        assert_eq!(Some("127.0.0.1:9876"), cmd.common_args.namesrv_addr.as_deref());
    }

    #[test]
    fn test_list_acl_sub_command_with_cluster_name_using_short_commands() {
        let args = [
            vec![""],
            vec!["-n", "127.0.0.1:9876"],
            vec!["-c", "DefaultCluster"],
            vec!["-s", "user:bob"],
        ];

        let args = args.concat();

        let cmd = ListAclSubCommand::try_parse_from(args).unwrap();
        assert_eq!(Some("DefaultCluster"), cmd.cluster_name.as_deref());
        assert_eq!(Some("user:bob"), cmd.subject.as_deref());
        assert_eq!(Some("127.0.0.1:9876"), cmd.common_args.namesrv_addr.as_deref());
    }

    #[test]
    fn test_list_acl_sub_command_using_conflicting_commands() {
        let args = [
            vec![""],
            vec!["-n", "127.0.0.1:9876"],
            vec!["-b", "127.0.0.1:10911"],
            vec!["-c", "DefaultCluster"],
            vec!["-s", "user:alice"],
        ];

        let args = args.concat();

        let result = ListAclSubCommand::try_parse_from(args);
        assert!(result.is_err());
    }
}
