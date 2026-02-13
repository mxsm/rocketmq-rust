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
use crate::commands::CommandExecute;

#[derive(Debug, Clone, Parser)]
#[command(group(ArgGroup::new("target")
    .required(false)
    .args(&["broker_addr", "cluster_name"]))
)]
pub struct DeleteExpiredCommitLogCommand {
    #[arg(short = 'b', long = "brokerAddr", required = false, help = "Broker address")]
    broker_addr: Option<String>,

    #[arg(short = 'c', long = "cluster", required = false, help = "Cluster name")]
    cluster_name: Option<String>,
}

impl CommandExecute for DeleteExpiredCommitLogCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let mut default_mqadmin_ext = if let Some(rpc_hook) = rpc_hook {
            DefaultMQAdminExt::with_rpc_hook(rpc_hook)
        } else {
            DefaultMQAdminExt::new()
        };
        default_mqadmin_ext
            .client_config_mut()
            .set_instance_name(get_current_millis().to_string().into());

        MQAdminExt::start(&mut default_mqadmin_ext).await.map_err(|e| {
            RocketMQError::Internal(format!(
                "DeleteExpiredCommitLogCommand: Failed to start MQAdminExt: {}",
                e
            ))
        })?;

        let operation_result = delete_expired_commit_log(&default_mqadmin_ext, self).await;

        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;
        operation_result
    }
}

async fn delete_expired_commit_log(
    default_mqadmin_ext: &DefaultMQAdminExt,
    command: &DeleteExpiredCommitLogCommand,
) -> RocketMQResult<()> {
    let addr = command
        .broker_addr
        .as_ref()
        .map(|s| CheetahString::from(s.trim().to_string()));
    let cluster = command
        .cluster_name
        .as_ref()
        .map(|s| CheetahString::from(s.trim().to_string()));

    let result = default_mqadmin_ext.delete_expired_commit_log(cluster, addr).await?;

    if result {
        println!("success");
    } else {
        println!("false");
    }

    Ok(())
}
