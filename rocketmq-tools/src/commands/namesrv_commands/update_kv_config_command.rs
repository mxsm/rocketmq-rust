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
use crate::commands::CommonArgs;

#[derive(Debug, Clone, Parser)]
pub struct UpdateKvConfigCommand {
    #[command(flatten)]
    common_args: CommonArgs,

    #[arg(short = 's', long = "namespace", required = true, help = "set the namespace")]
    namespace: String,

    #[arg(short = 'k', long = "key", required = true, help = "set the key name")]
    key: String,

    #[arg(short = 'v', long = "value", required = true, help = "set the key value")]
    value: String,
}

impl CommandExecute for UpdateKvConfigCommand {
    async fn execute(&self, _rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let mut default_mqadmin_ext = DefaultMQAdminExt::new();
        default_mqadmin_ext
            .client_config_mut()
            .set_instance_name(get_current_millis().to_string().into());

        let operation_result = async {
            if let Some(addr) = &self.common_args.namesrv_addr {
                default_mqadmin_ext.set_namesrv_addr(addr.trim());
            }

            MQAdminExt::start(&mut default_mqadmin_ext).await.map_err(|e| {
                RocketMQError::Internal(format!("UpdateKvConfigCommand: Failed to start MQAdminExt: {}", e))
            })?;

            default_mqadmin_ext
                .create_and_update_kv_config(
                    self.namespace.parse().unwrap(),
                    self.key.parse().unwrap(),
                    self.value.parse().unwrap(),
                )
                .await
                .map_err(|e| {
                    RocketMQError::Internal(format!("UpdateKvConfigCommand: Failed to update kv config: {}", e))
                })?;

            println!("update kv config in namespace success.");
            Ok(())
        }
        .await;
        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;
        operation_result
    }
}
