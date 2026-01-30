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
use rocketmq_remoting::runtime::RPCHook;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::commands::CommandExecute;

#[derive(Debug, Clone, Parser)]
pub struct GetControllerConfigSubCommand {
    #[arg(
        short = 'a',
        long = "controllerAddress",
        value_name = "HOST:PORT[;HOST:PORT...]",
        required = true,
        help = "Controller address list, eg: '192.168.0.1:9878;192.168.0.2:9878'"
    )]
    controller_address: String,
}

impl CommandExecute for GetControllerConfigSubCommand {
    async fn execute(&self, _rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let mut default_mqadmin_ext = DefaultMQAdminExt::new();
        default_mqadmin_ext
            .client_config_mut()
            .set_instance_name(get_current_millis().to_string().into());

        let controller_servers: Vec<CheetahString> = self
            .controller_address
            .split(';')
            .filter(|s| !s.trim().is_empty())
            .map(|s| CheetahString::from(s.trim()))
            .collect();

        if controller_servers.is_empty() {
            return Err(RocketMQError::Internal(
                "GetControllerConfigSubCommand: No valid controller address provided".to_string(),
            ));
        }

        let operation_result = async {
            MQAdminExt::start(&mut default_mqadmin_ext).await.map_err(|e| {
                RocketMQError::Internal(format!(
                    "GetControllerConfigSubCommand: Failed to start MQAdminExt: {}",
                    e
                ))
            })?;

            let controller_configs = default_mqadmin_ext
                .get_controller_config(controller_servers)
                .await
                .map_err(|e| {
                    RocketMQError::Internal(format!(
                        "GetControllerConfigSubCommand: Failed to get controller config: {}",
                        e
                    ))
                })?;

            for (controller_addr, config) in controller_configs {
                println!("============{}============", controller_addr);
                for (key, value) in config {
                    println!("{:<50}=  {}", key, value);
                }
            }

            Ok(())
        }
        .await;

        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;
        operation_result
    }
}
