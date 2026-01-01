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

use std::collections::HashMap;
use std::sync::Arc;

use cheetah_string::CheetahString;
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
pub struct UpdateNamesrvConfig {
    #[command(flatten)]
    common_args: CommonArgs,

    #[arg(short = 'k', long = "key", required = true, help = "config key")]
    key: String,

    #[arg(short = 'v', long = "value", required = true, help = "config value")]
    value: String,
}

impl CommandExecute for UpdateNamesrvConfig {
    async fn execute(&self, _rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let mut default_mqadmin_ext = DefaultMQAdminExt::new();
        default_mqadmin_ext
            .client_config_mut()
            .set_instance_name(get_current_millis().to_string().into());

        let operation_result = async {
            // key name
            let key: CheetahString = self.key.trim().into();

            // key name
            let value: CheetahString = self.value.trim().into();
            let mut properties = HashMap::with_capacity(1);
            properties.insert(key.clone(), value.clone());

            let mut server_list = None;
            if let Some(servers) = &self.common_args.namesrv_addr {
                if !servers.is_empty() {
                    let servers_split: Vec<&str> = servers.split(';').collect();
                    if !servers_split.is_empty() {
                        let mut vec = Vec::with_capacity(servers_split.len());
                        for server in servers_split {
                            vec.push(server.into());
                        }
                        server_list = Some(vec);
                    }
                }
            }

            MQAdminExt::start(&mut default_mqadmin_ext).await.map_err(|e| {
                RocketMQError::Internal(format!("UpdateNamesrvConfig: Failed to start MQAdminExt: {}", e))
            })?;

            default_mqadmin_ext
                .update_name_server_config(properties, server_list.clone())
                .await
                .map_err(|e| RocketMQError::Internal(format!("UpdateNamesrvConfig: Failed to update config: {}", e)))?;

            println!(
                "update name server config success!{}\n{} : {}\n",
                server_list.unwrap_or_default().join(";"),
                key,
                value
            );
            Ok(())
        }
        .await;
        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;
        operation_result
    }
}
