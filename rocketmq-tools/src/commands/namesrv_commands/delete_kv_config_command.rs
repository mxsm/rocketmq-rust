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
pub struct DeleteKvConfigCommand {
    #[arg(short = 's', long = "namespace", required = true)]
    namespace: String,

    #[arg(short = 'k', long = "key", required = true)]
    key: String,
}

impl CommandExecute for DeleteKvConfigCommand {
    async fn execute(&self, _rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let mut default_mqadmin_ext = DefaultMQAdminExt::new();
        default_mqadmin_ext
            .client_config_mut()
            .set_instance_name(get_current_millis().to_string().into());

        let operation_result = async {
            MQAdminExt::start(&mut default_mqadmin_ext).await.map_err(|e| {
                RocketMQError::Internal(format!("DeleteKvConfigCommand: Failed to start MQAdminExt: {}", e))
            })?;

            MQAdminExt::delete_kv_config(
                &default_mqadmin_ext,
                self.namespace.parse().unwrap(),
                self.key.parse().unwrap(),
            )
            .await
            .map_err(|e| {
                RocketMQError::Internal(format!("DeleteKvConfigCommand: Failed to delete kv config: {}", e))
            })?;

            println!("delete kv config from namespace success.");
            Ok(())
        }
        .await;
        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;
        operation_result
    }
}
