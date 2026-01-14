// Copyright 2026 The RocketMQ Rust Authors
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

impl CommandExecute for UpdateAclSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        if (self.cluster_name.is_none() && self.broker_addr.is_none())
            || (self.cluster_name.is_some() && self.broker_addr.is_some())
        {
            return Err(RocketMQError::IllegalArgument(
                "UpdateAclSubCommand: Specify exactly one of --brokerAddr (-b) or --clusterName (-c)".into(),
            ));
        }

        let mut default_mqadmin_ext = if let Some(rpc_hook) = rpc_hook {
            DefaultMQAdminExt::with_rpc_hook(rpc_hook)
        } else {
            DefaultMQAdminExt::new()
        };
        default_mqadmin_ext
            .client_config_mut()
            .set_instance_name(get_current_millis().to_string().into());

        let operation_result = async {
            let subject = self.subject.trim();

            let resources: Vec<CheetahString> = self
                .resources
                .split(",")
                .map(|resource| resource.trim())
                .map(|resource| resource.into())
                .collect();

            let actions: Vec<CheetahString> = self
                .actions
                .split(",")
                .map(|action| action.trim())
                .map(|action| action.into())
                .collect();

            let source_ips: Vec<CheetahString> = if let Some(ref source_ips) = self.source_ip {
                source_ips
                    .split(",")
                    .map(|source_ip| source_ip.trim())
                    .map(|source_ip| source_ip.into())
                    .collect()
            } else {
                Vec::new()
            };

            let decision: CheetahString = self.decision.trim().into();

            if let Some(ref broker_addr) = self.broker_addr {
                MQAdminExt::start(&mut default_mqadmin_ext).await.map_err(|e| {
                    RocketMQError::Internal(format!("UpdateAclSubCommand: Failed to start MQAdminExt: {}", e))
                })?;

                match default_mqadmin_ext
                    .update_acl(
                        broker_addr.as_str().into(),
                        subject.into(),
                        resources,
                        actions,
                        source_ips,
                        decision,
                    )
                    .await
                {
                    Ok(_) => {
                        println!("Update access control list (ACL) for {} was successful.", broker_addr);
                    }
                    Err(e) => {
                        return Err(RocketMQError::Internal(format!(
                            "UpdateAclSubCommand: Failed to update access control list (ACL): {}",
                            e
                        )))
                    }
                }
            } else if let Some(ref cluster_name) = self.cluster_name {
                MQAdminExt::start(&mut default_mqadmin_ext).await.map_err(|e| {
                    RocketMQError::Internal(format!("UpdateAclSubCommand: Failed to start MQAdminExt: {}", e))
                })?;

                let cluster_info = default_mqadmin_ext.examine_broker_cluster_info().await?;
                match CommandUtil::fetch_master_addr_by_cluster_name(&cluster_info, cluster_name.as_str()) {
                    Ok(addresses) => {
                        for addr in addresses {
                            if let Err(e) = default_mqadmin_ext
                                .update_acl(
                                    addr.as_str().into(),
                                    subject.into(),
                                    resources.clone(),
                                    actions.clone(),
                                    source_ips.clone(),
                                    decision.clone(),
                                )
                                .await
                            {
                                eprintln!(
                                    "UpdateAclSubCommand: Failed to update access control list (ACL) for broker with \
                                     address {}: {}",
                                    addr, e
                                )
                            } else {
                                println!(
                                    "Updated access control list (ACL) at broker with address {} successfully.",
                                    addr
                                );
                            }
                        }
                    }
                    Err(e) => {
                        return Err(RocketMQError::Internal(format!(
                            "UpdateAclSubCommand: Failed to update access control list (ACL): {}",
                            e
                        )));
                    }
                }
            }

            Ok(())
        }
        .await;
        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;
        operation_result
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
