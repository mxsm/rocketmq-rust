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
use rocketmq_remoting::runtime::RPCHook;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::commands::command_util::CommandUtil;
use crate::commands::CommandExecute;

#[derive(Debug, Clone, Parser)]
#[command(group(ArgGroup::new("target")
    .required(true)
    .args(&["cluster_name", "broker_addr"])))]
pub struct DeleteAclSubCommand {
    #[arg(
        short = 'c',
        long = "clusterName",
        required = false,
        help = "delete acl from which cluster"
    )]
    cluster_name: Option<String>,

    #[arg(
        short = 'b',
        long = "brokerAddr",
        required = false,
        help = "delete acl from which broker"
    )]
    broker_addr: Option<String>,

    #[arg(
        short = 's',
        long = "subject",
        required = true,
        help = "the subject of acl to delete"
    )]
    subject: String,

    #[arg(
        short = 'r',
        long = "resources",
        required = false,
        help = "the resources of acl to delete"
    )]
    resources: Option<String>,
}

#[derive(Clone)]
struct ParsedDeleteAclSubCommand {
    cluster_name: Option<CheetahString>,
    broker_addr: Option<CheetahString>,
    subject: CheetahString,
    resource: Option<CheetahString>,
}

impl ParsedDeleteAclSubCommand {
    fn new(command: &DeleteAclSubCommand) -> Result<Self, RocketMQError> {
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

        if cluster_name.is_none() && broker_addr.is_none() {
            return Err(RocketMQError::IllegalArgument(
                "DeleteAclSubCommand: Specify either --brokerAddr (-b) or --clusterName (-c)".into(),
            ));
        }

        let subject: CheetahString = {
            let subject = command.subject.trim();
            if subject.is_empty() {
                return Err(RocketMQError::IllegalArgument(
                    "DeleteAclSubCommand: subject cannot be empty".into(),
                ));
            } else {
                subject.into()
            }
        };

        let resource: Option<CheetahString> = command
            .resources
            .as_ref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.into());

        Ok(Self {
            cluster_name,
            broker_addr,
            subject,
            resource,
        })
    }
}

impl CommandExecute for DeleteAclSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let command = ParsedDeleteAclSubCommand::new(self)?;

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
            .map_err(|e| RocketMQError::Internal(format!("DeleteAclSubCommand: Failed to start MQAdminExt: {}", e)))?;

        let operation_result = delete_acl(&command, &default_mqadmin_ext).await;

        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;
        operation_result
    }
}

async fn delete_acl(
    parsed_command: &ParsedDeleteAclSubCommand,
    default_mqadmin_ext: &DefaultMQAdminExt,
) -> Result<(), RocketMQError> {
    let subject = parsed_command.subject.clone();
    let resource = parsed_command.resource.clone().unwrap_or_default();

    if let Some(ref broker_addr) = parsed_command.broker_addr {
        default_mqadmin_ext
            .delete_acl(broker_addr.clone(), subject.clone(), resource.clone())
            .await?;
        println!("delete acl to {} success.", broker_addr);
    } else if let Some(ref cluster_name) = parsed_command.cluster_name {
        let cluster_info = default_mqadmin_ext.examine_broker_cluster_info().await?;
        let broker_addrs = CommandUtil::fetch_master_and_slave_addr_by_cluster_name(&cluster_info, cluster_name)?;

        for addr in broker_addrs {
            default_mqadmin_ext
                .delete_acl(addr.clone(), subject.clone(), resource.clone())
                .await?;
            println!("delete acl to {} success.", addr);
        }
    }

    Ok(())
}
