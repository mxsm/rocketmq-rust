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
use rocketmq_admin_core::core::auth::UpdateAclRequest;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;

#[derive(Debug, Clone, Parser)]
#[command(group(ArgGroup::new("target")
    .required(true)
    .args(&["cluster_name", "broker_addr"])))]
pub struct UpdateAclSubCommand {
    #[arg(short = 'c', long = "clusterName", required = false)]
    cluster_name: Option<String>,

    #[arg(short = 'b', long = "brokerAddr", required = false)]
    broker_addr: Option<String>,

    #[arg(short = 's', long = "subject", required = true)]
    subject: String,

    #[arg(short = 'r', long = "resources", required = true)]
    resources: String,

    #[arg(short = 'a', long = "actions", required = true)]
    actions: String,

    #[arg(short = 'd', long = "decision", required = true)]
    decision: String,

    #[arg(short = 'i', long = "sourceIp", required = false)]
    source_ip: Option<String>,
}

impl CommandExecute for UpdateAclSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let request = UpdateAclRequest::try_new(
            self.broker_addr.clone(),
            self.cluster_name.clone(),
            self.subject.clone(),
            self.resources.clone(),
            self.actions.clone(),
            self.decision.clone(),
            self.source_ip.clone(),
        )?;
        let result = AuthService::update_acl_by_request_with_rpc_hook(request, rpc_hook).await?;
        for broker_addr in result.broker_addrs {
            println!("Update access control list (ACL) for {} was successful.", broker_addr);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::auth::update_acl_sub_command::UpdateAclSubCommand;
    use clap::Parser;

    #[test]
    fn test_update_acl_sub_command_with_broker_addr_using_short_commands() {
        let args = [
            vec![""],
            vec!["-b", "127.0.0.1:3434"],
            vec!["-s", "user:alice"],
            vec!["-r", "Topic:order-topic,Topic:user-topic"],
            vec!["-a", "PUB,SUB"],
            vec!["-d", "ALLOW"],
            vec!["-i", "127.0.0.1,127.0.0.2"],
        ];

        let args = args.concat();

        let cmd = UpdateAclSubCommand::try_parse_from(args).unwrap();
        assert_eq!(Some("127.0.0.1:3434"), cmd.broker_addr.as_deref());
        assert_eq!("user:alice", cmd.subject.as_str());
        assert_eq!("Topic:order-topic,Topic:user-topic", cmd.resources.as_str());
        assert_eq!("PUB,SUB", cmd.actions.as_str());
        assert_eq!("ALLOW", cmd.decision.as_str());
        assert_eq!(Some("127.0.0.1,127.0.0.2"), cmd.source_ip.as_deref());
    }

    #[test]
    fn test_update_acl_sub_command_with_cluster_name_using_short_commands() {
        let args = [
            vec![""],
            vec!["-c", "DefaultCluster"],
            vec!["-s", "user:alice"],
            vec!["-r", "Topic:order-topic,Topic:user-topic"],
            vec!["-a", "PUB,SUB"],
            vec!["-d", "ALLOW"],
            vec!["-i", "127.0.0.1,127.0.0.2"],
        ];

        let args = args.concat();

        let cmd = UpdateAclSubCommand::try_parse_from(args).unwrap();
        assert_eq!(Some("DefaultCluster"), cmd.cluster_name.as_deref());
        assert_eq!("user:alice", cmd.subject.as_str());
        assert_eq!("Topic:order-topic,Topic:user-topic", cmd.resources.as_str());
        assert_eq!("PUB,SUB", cmd.actions.as_str());
        assert_eq!("ALLOW", cmd.decision.as_str());
        assert_eq!(Some("127.0.0.1,127.0.0.2"), cmd.source_ip.as_deref());
    }

    #[test]
    fn test_update_acl_sub_command_using_conflicting_commands() {
        let args = [
            vec![""],
            vec!["-b", "127.0.0.1:3434"],
            vec!["-c", "DefaultCluster"],
            vec!["-s", "user:alice"],
            vec!["-r", "Topic:order-topic,Topic:user-topic"],
            vec!["-a", "PUB,SUB"],
            vec!["-d", "ALLOW"],
            vec!["-i", "127.0.0.1,127.0.0.2"],
        ];

        let args = args.concat();

        let result = UpdateAclSubCommand::try_parse_from(args);
        assert!(result.is_err());
    }
}
