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

use cheetah_string::CheetahString;
use clap::ArgGroup;
use clap::Parser;
use rocketmq_admin_core::core::auth::AuthService;
use rocketmq_admin_core::core::auth::ListAclRequest;
use rocketmq_admin_core::core::auth::ListAclResult;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::body::acl_info::AclInfo;
use rocketmq_remoting::runtime::RPCHook;

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
        let request = ListAclRequest::try_new(
            self.broker_addr.clone(),
            self.cluster_name.clone(),
            self.subject.clone(),
        )?
        .with_optional_namesrv_addr(self.common_args.namesrv_addr.clone());
        let result = AuthService::list_acl_by_request_with_rpc_hook(request, rpc_hook).await?;
        render_list_acl_result(result)
    }
}

fn render_list_acl_result(result: ListAclResult) -> RocketMQResult<()> {
    print_acls(result.acl_infos);
    if result.failed_broker_addrs.is_empty() {
        Ok(())
    } else {
        Err(RocketMQError::Internal(format!(
            "ListAclSubCommand: Failed to list ACLs for brokers {}",
            result.failed_broker_addrs.join(", ")
        )))
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
                    actions: actions_or_default(entry.actions.as_deref()),
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

fn actions_or_default(actions: Option<&[CheetahString]>) -> String {
    actions
        .filter(|actions| !actions.is_empty())
        .map(|actions| {
            actions
                .iter()
                .map(|action| action.as_str())
                .collect::<Vec<_>>()
                .join(",")
        })
        .filter(|actions| !actions.trim().is_empty())
        .unwrap_or_else(|| "*".to_string())
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use crate::commands::auth::list_acl_sub_command::ListAclSubCommand;

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
