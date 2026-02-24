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

use cheetah_string::CheetahString;
use clap::ArgGroup;
use clap::Parser;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::body::acl_info::AclInfo;
use rocketmq_remoting::protocol::body::acl_info::PolicyEntryInfo;
use rocketmq_remoting::protocol::body::acl_info::PolicyInfo;
use rocketmq_remoting::runtime::RPCHook;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::commands::command_util::CommandUtil;
use crate::commands::CommandExecute;

#[derive(Debug, Clone, Parser)]
#[command(group(ArgGroup::new("target")
    .required(true)
    .args(&["cluster_name", "broker_addr"])))]
pub struct CreateAclSubCommand {
    #[arg(
        short = 'c',
        long = "clusterName",
        required = false,
        help = "create acl to which cluster"
    )]
    cluster_name: Option<String>,

    #[arg(
        short = 'b',
        long = "brokerAddr",
        required = false,
        help = "create acl to which broker"
    )]
    broker_addr: Option<String>,

    #[arg(
        short = 's',
        long = "subject",
        required = true,
        help = "the subject of acl to create"
    )]
    subject: String,

    #[arg(
        short = 'r',
        long = "resources",
        required = true,
        help = "the resources of acl to create"
    )]
    resources: String,

    #[arg(
        short = 'a',
        long = "actions",
        required = true,
        help = "the actions of acl to create"
    )]
    actions: String,

    #[arg(
        short = 'd',
        long = "decision",
        required = true,
        help = "the decision of acl to create"
    )]
    decision: String,

    #[arg(
        short = 'i',
        long = "sourceIp",
        required = false,
        help = "the sourceIps of acl to create"
    )]
    source_ip: Option<String>,
}

#[derive(Clone)]
struct ParsedCreateAclSubCommand {
    cluster_name: Option<CheetahString>,
    broker_addr: Option<CheetahString>,
    subject: CheetahString,
    resources: Vec<CheetahString>,
    actions: Vec<CheetahString>,
    decision: CheetahString,
    source_ips: Vec<CheetahString>,
}

impl ParsedCreateAclSubCommand {
    fn new(command: &CreateAclSubCommand) -> Result<Self, RocketMQError> {
        let cluster_name: Option<CheetahString> = command
            .cluster_name
            .as_ref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.into());

        let broker_addr: Option<CheetahString> = command
            .broker_addr
            .as_ref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.into());

        let subject: CheetahString = command.subject.trim().into();
        let resources: Vec<CheetahString> = command
            .resources
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.into())
            .collect();
        let actions: Vec<CheetahString> = command
            .actions
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.into())
            .collect();
        let decision: CheetahString = command.decision.trim().into();
        let source_ips: Vec<CheetahString> = command
            .source_ip
            .as_ref()
            .map(|s| {
                s.split(',')
                    .map(|ip| ip.trim())
                    .filter(|ip| !ip.is_empty())
                    .map(|ip| ip.into())
                    .collect()
            })
            .unwrap_or_default();

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

    fn build_acl_info(&self) -> AclInfo {
        let actions_str: CheetahString = self
            .actions
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
            .join(",")
            .into();
        let resources_str: CheetahString = self
            .resources
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
            .join(",")
            .into();

        let entry = PolicyEntryInfo {
            resource: Some(resources_str),
            actions: Some(actions_str),
            source_ips: if self.source_ips.is_empty() {
                None
            } else {
                Some(self.source_ips.clone())
            },
            decision: Some(self.decision.clone()),
        };

        let policy = PolicyInfo {
            policy_type: None,
            entries: Some(vec![entry]),
        };

        AclInfo {
            subject: Some(self.subject.clone()),
            policies: Some(vec![policy]),
        }
    }
}

impl CommandExecute for CreateAclSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let command = ParsedCreateAclSubCommand::new(self)?;

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
            .map_err(|e| RocketMQError::Internal(format!("CreateAclSubCommand: Failed to start MQAdminExt: {}", e)))?;

        let operation_result = create_acl(&command, &default_mqadmin_ext).await;

        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;
        operation_result
    }
}

async fn create_acl(
    parsed_command: &ParsedCreateAclSubCommand,
    default_mqadmin_ext: &DefaultMQAdminExt,
) -> Result<(), RocketMQError> {
    let acl_info = parsed_command.build_acl_info();

    if let Some(ref broker_addr) = parsed_command.broker_addr {
        default_mqadmin_ext
            .create_acl_with_acl_info(broker_addr.clone(), acl_info)
            .await?;
        println!("create acl to {} success.", broker_addr);
    } else if let Some(ref cluster_name) = parsed_command.cluster_name {
        let cluster_info = default_mqadmin_ext.examine_broker_cluster_info().await?;
        let broker_addrs = CommandUtil::fetch_master_and_slave_addr_by_cluster_name(&cluster_info, cluster_name)?;

        for addr in broker_addrs {
            default_mqadmin_ext
                .create_acl_with_acl_info(addr.clone(), acl_info.clone())
                .await?;
            println!("create acl to {} success.", addr);
        }
    }

    Ok(())
}
