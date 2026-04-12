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
use rocketmq_admin_core::core::auth::GetAclRequest;
use rocketmq_admin_core::core::auth::GetAclResult;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::body::acl_info::AclInfo;
use rocketmq_remoting::runtime::RPCHook;

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

impl CommandExecute for GetAclSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let request = GetAclRequest::try_new(
            self.broker_addr.clone(),
            self.cluster_name.clone(),
            self.subject.clone(),
        )?;
        let result = AuthService::get_acl_by_request_with_rpc_hook(request, rpc_hook).await?;
        render_get_acl_result(result, self.subject.trim(), self.broker_addr.is_some())
    }
}

fn render_get_acl_result(result: GetAclResult, subject: &str, single_broker_target: bool) -> RocketMQResult<()> {
    if single_broker_target && result.acl_infos.is_empty() {
        eprintln!("No ACL with subject {} was found", subject);
        return Ok(());
    }

    if result.acl_infos.is_empty() && result.failed_broker_addrs.is_empty() {
        eprintln!("No ACL with subject {} was found", subject);
    } else {
        print_header();
        print_acls(&result.acl_infos);
    }

    if !result.failed_broker_addrs.is_empty() {
        eprintln!(
            "Warning: failed to get ACL from brokers: {}",
            result.failed_broker_addrs.join(", ")
        );
    }

    if !result.failed_broker_addrs.is_empty() && result.acl_infos.is_empty() {
        Err(RocketMQError::Internal(format!(
            "GetAclSubCommand: Failed to get ACL for brokers {}",
            result.failed_broker_addrs.join(", ")
        )))
    } else {
        Ok(())
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
    use crate::commands::auth::get_acl_sub_command::GetAclSubCommand;
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
