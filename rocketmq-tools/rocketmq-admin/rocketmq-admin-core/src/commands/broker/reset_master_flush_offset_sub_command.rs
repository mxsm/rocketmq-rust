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

use clap::Parser;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::commands::CommandExecute;

#[derive(Debug, Clone, Parser)]
pub struct ResetMasterFlushOffsetSubCommand {
    #[arg(short = 'b', long = "brokerAddr", required = false, help = "which broker to reset")]
    broker_addr: Option<String>,

    #[arg(short = 'o', long = "offset", required = false, help = "the offset to reset at")]
    offset: Option<i64>,
}

impl CommandExecute for ResetMasterFlushOffsetSubCommand {
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
                "ResetMasterFlushOffsetSubCommand: Failed to start MQAdminExt: {}",
                e
            ))
        })?;

        let broker_addr = self
            .broker_addr
            .as_ref()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| RocketMQError::Internal("brokerAddr is required".into()))?;
        let master_flush_offset = self
            .offset
            .ok_or_else(|| RocketMQError::Internal("offset is required".into()))?;

        let result = default_mqadmin_ext
            .reset_master_flush_offset(broker_addr.into(), master_flush_offset as u64)
            .await;

        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;

        match result {
            Ok(()) => {
                println!("reset master flush offset to {} success", master_flush_offset);
                Ok(())
            }
            Err(e) => Err(RocketMQError::Internal(format!(
                "ResetMasterFlushOffsetSubCommand command failed: {}",
                e
            ))),
        }
    }
}
