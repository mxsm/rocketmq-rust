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
use clap::Parser;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::body::acl_info::AclInfo;
use rocketmq_remoting::runtime::RPCHook;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::commands::CommandExecute;

#[derive(Debug, Clone, Parser)]
pub struct CopyAclSubCommand {
    #[arg(
        short = 'f',
        long = "fromBroker",
        required = true,
        help = "the source broker that the acls copy from"
    )]
    from_broker: String,

    #[arg(
        short = 't',
        long = "toBroker",
        required = true,
        help = "the target broker that the acls copy to"
    )]
    to_broker: String,

    #[arg(
        short = 's',
        long = "subjects",
        required = false,
        help = "the subject list of acl to copy"
    )]
    subjects: Option<String>,
}

#[derive(Clone)]
struct ParsedCopyAclSubCommand {
    from_broker: CheetahString,
    to_broker: CheetahString,
    subjects: Option<Vec<CheetahString>>,
}

impl ParsedCopyAclSubCommand {
    fn new(command: &CopyAclSubCommand) -> Result<Self, RocketMQError> {
        let from_broker: CheetahString = {
            let from_broker = command.from_broker.trim();
            if from_broker.is_empty() {
                return Err(RocketMQError::IllegalArgument(
                    "CopyAclSubCommand: fromBroker cannot be empty".into(),
                ));
            }
            from_broker.into()
        };

        let to_broker: CheetahString = {
            let to_broker = command.to_broker.trim();
            if to_broker.is_empty() {
                return Err(RocketMQError::IllegalArgument(
                    "CopyAclSubCommand: toBroker cannot be empty".into(),
                ));
            }
            to_broker.into()
        };

        let subjects: Option<Vec<CheetahString>> = command
            .subjects
            .as_ref()
            .map(|subjects| {
                subjects
                    .split(',')
                    .map(|subject| subject.trim())
                    .filter(|subject| !subject.is_empty())
                    .map(|subject| subject.into())
                    .collect::<Vec<_>>()
            })
            .filter(|v: &Vec<CheetahString>| !v.is_empty());

        Ok(Self {
            from_broker,
            to_broker,
            subjects,
        })
    }
}

impl CommandExecute for CopyAclSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let command = ParsedCopyAclSubCommand::new(self)?;

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
            .map_err(|e| RocketMQError::Internal(format!("CopyAclSubCommand: Failed to start MQAdminExt: {}", e)))?;

        let operation_result = copy_acls(&command, &default_mqadmin_ext).await;

        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;
        operation_result
    }
}

async fn copy_acls(
    parsed_command: &ParsedCopyAclSubCommand,
    default_mqadmin_ext: &DefaultMQAdminExt,
) -> Result<(), RocketMQError> {
    let source_broker = parsed_command.from_broker.as_str();
    let target_broker = parsed_command.to_broker.as_str();

    let acl_infos: Vec<AclInfo> = if let Some(ref subjects) = parsed_command.subjects {
        let mut acl_list = Vec::new();
        for subject in subjects {
            match default_mqadmin_ext.get_acl(source_broker.into(), subject.clone()).await {
                Ok(acl_info) => {
                    acl_list.push(acl_info);
                }
                Err(e) => {
                    eprintln!(
                        "Warning: Failed to get ACL for subject {} from {}: {}",
                        subject, source_broker, e
                    );
                }
            }
        }
        acl_list
    } else {
        default_mqadmin_ext
            .list_acl(source_broker.into(), CheetahString::default(), CheetahString::default())
            .await
            .map_err(|e| {
                RocketMQError::Internal(format!(
                    "CopyAclSubCommand: Failed to list ACLs from {}: {}",
                    source_broker, e
                ))
            })?
    };

    if acl_infos.is_empty() {
        println!("No ACLs found to copy from {}.", source_broker);
        return Ok(());
    }

    for acl_info in acl_infos {
        let subject = match acl_info.subject.as_ref() {
            Some(s) => s.clone(),
            None => {
                eprintln!("Warning: ACL has no subject, skipping.");
                continue;
            }
        };

        let target_acl_result = default_mqadmin_ext.get_acl(target_broker.into(), subject.clone()).await;

        let copy_result = if target_acl_result.is_err() {
            default_mqadmin_ext
                .create_acl_with_acl_info(target_broker.into(), acl_info.clone())
                .await
        } else {
            default_mqadmin_ext
                .update_acl_with_acl_info(target_broker.into(), acl_info.clone())
                .await
        };

        match copy_result {
            Ok(_) => {
                println!(
                    "copy acl of {} from {} to {} success.",
                    subject, source_broker, target_broker
                );
            }
            Err(e) => {
                eprintln!(
                    "copy acl of {} from {} to {} failed: {}",
                    subject, source_broker, target_broker, e
                );
            }
        }
    }

    Ok(())
}
