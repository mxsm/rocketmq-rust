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
use rocketmq_remoting::runtime::RPCHook;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::commands::command_util::CommandUtil;
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

#[derive(Clone)]
struct ParsedUpdateAclSubCommand {
    cluster_name: Option<String>,
    broker_addr: Option<String>,
    subject: CheetahString,
    resources: Vec<CheetahString>,
    actions: Vec<CheetahString>,
    decision: CheetahString,
    source_ips: Vec<CheetahString>,
}

impl ParsedUpdateAclSubCommand {
    fn new(command: &UpdateAclSubCommand) -> Result<Self, RocketMQError> {
        let cluster_name: Option<String> = command
            .cluster_name
            .as_ref()
            .map(|cluster_name| cluster_name.trim())
            .filter(|cluster_name| !cluster_name.is_empty())
            .map(|cluster_name| cluster_name.into());
        let broker_addr: Option<String> = command
            .broker_addr
            .as_ref()
            .map(|broker_addr| broker_addr.trim())
            .filter(|broker_addr| !broker_addr.is_empty())
            .map(|broker_addr| broker_addr.into());
        if (cluster_name.is_none() && broker_addr.is_none()) || (cluster_name.is_some() && broker_addr.is_some()) {
            return Err(RocketMQError::IllegalArgument(
                "UpdateAclSubCommand: Specify exactly one of --brokerAddr (-b) or --clusterName (-c)".into(),
            ));
        }

        let subject: CheetahString = {
            let subject = command.subject.trim();
            if subject.is_empty() {
                return Err(RocketMQError::IllegalArgument(
                    "UpdateAclSubCommand: subject cannot be empty".into(),
                ));
            } else {
                subject.into()
            }
        };

        let resources: Vec<CheetahString> = command
            .resources
            .split(",")
            .map(|resource| resource.trim())
            .filter(|resource| !resource.is_empty())
            .map(|resource| resource.into())
            .collect();
        if resources.is_empty() {
            return Err(RocketMQError::IllegalArgument(
                "UpdateAclSubCommand: resources cannot be empty".into(),
            ));
        }

        let actions: Vec<CheetahString> = command
            .actions
            .split(",")
            .map(|action| action.trim())
            .filter(|action| !action.is_empty())
            .map(|action| action.into())
            .collect();
        if actions.is_empty() {
            return Err(RocketMQError::IllegalArgument(
                "UpdateAclSubCommand: actions cannot be empty".into(),
            ));
        }

        let source_ips: Vec<CheetahString> = if let Some(ref source_ips) = command.source_ip {
            source_ips
                .split(",")
                .map(|source_ip| source_ip.trim())
                .filter(|source_ip| !source_ip.is_empty())
                .map(|source_ip| source_ip.into())
                .collect()
        } else {
            Vec::new()
        };

        let decision: CheetahString = command.decision.trim().into();
        if decision.is_empty() {
            return Err(RocketMQError::IllegalArgument(
                "UpdateAclSubCommand: decision cannot be empty".into(),
            ));
        }

        Ok(Self {
            cluster_name,
            broker_addr,
            subject,
            resources,
            actions,
            decision,
            source_ips,
        })
    }
}

impl CommandExecute for UpdateAclSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let command = ParsedUpdateAclSubCommand::new(self)?;

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
            .map_err(|e| RocketMQError::Internal(format!("UpdateAclSubCommand: Failed to start MQAdminExt: {}", e)))?;

        let operation_result = async {
            if let Some(broker_addr) = &command.broker_addr {
                update_broker(&command, &default_mqadmin_ext, broker_addr).await
            } else if let Some(cluster_name) = &command.cluster_name {
                update_cluster(&command, &default_mqadmin_ext, cluster_name).await
            } else {
                // This case can not happen.
                Err(RocketMQError::Internal(
                    "UpdateAclSubCommand: broker_address or cluster_name must be set.".into(),
                ))
            }
        }
        .await;
        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;
        operation_result
    }
}

async fn update_broker(
    parsed_command: &ParsedUpdateAclSubCommand,
    default_mqadmin_ext: &DefaultMQAdminExt,
    broker_addr: &str,
) -> Result<(), RocketMQError> {
    let broker_update_result = {
        match default_mqadmin_ext
            .update_acl(
                broker_addr.into(),
                parsed_command.subject.clone(),
                parsed_command.resources.clone(),
                parsed_command.actions.clone(),
                parsed_command.source_ips.clone(),
                parsed_command.decision.clone(),
            )
            .await
        {
            Ok(_) => {
                println!("Update access control list (ACL) for {} was successful.", broker_addr);
                Ok(())
            }
            Err(e) => Err(RocketMQError::Internal(format!(
                "UpdateAclSubCommand: Failed to update access control list (ACL) for broker {}: {}",
                broker_addr, e
            ))),
        }
    };
    broker_update_result
}

async fn update_cluster(
    parsed_command: &ParsedUpdateAclSubCommand,
    default_mqadmin_ext: &DefaultMQAdminExt,
    cluster_name: &str,
) -> Result<(), RocketMQError> {
    let cluster_info = default_mqadmin_ext.examine_broker_cluster_info().await?;

    match CommandUtil::fetch_master_addr_by_cluster_name(&cluster_info, cluster_name) {
        Ok(addresses) => {
            let error_addr: Vec<CheetahString> = futures::future::join_all(addresses.into_iter().map(|addr| async {
                if update_broker(parsed_command, default_mqadmin_ext, addr.as_str())
                    .await
                    .is_err()
                {
                    Some(addr)
                } else {
                    None
                }
            }))
            .await
            .into_iter()
            .flatten()
            .collect();

            if error_addr.is_empty() {
                Ok(())
            } else {
                Err(RocketMQError::Internal(format!(
                    "UpdateAclSubCommand: Failed to update access control list (ACL) for brokers {}",
                    error_addr.join(", ")
                )))
            }
        }
        Err(e) => Err(RocketMQError::Internal(format!(
            "UpdateAclSubCommand: Failed to update access control list (ACL): {}",
            e
        ))),
    }
}

#[cfg(test)]
mod tests {

    use crate::commands::auth_commands::update_acl_sub_command::UpdateAclSubCommand;
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
