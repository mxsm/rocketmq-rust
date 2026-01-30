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
pub struct AddWritePermSubCommand {
    #[command(flatten)]
    common_args: CommonArgs,

    #[arg(short = 'b', long = "brokerName", required = true, help = "broker name")]
    broker_name: String,
}

impl CommandExecute for AddWritePermSubCommand {
    async fn execute(&self, _rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let mut default_mqadmin_ext = DefaultMQAdminExt::new();
        default_mqadmin_ext
            .client_config_mut()
            .set_instance_name(get_current_millis().to_string().into());

        let operation_result = async {
            MQAdminExt::start(&mut default_mqadmin_ext).await.map_err(|e| {
                RocketMQError::Internal(format!("AddWritePermSubCommand: Failed to start MQAdminExt: {}", e))
            })?;
            let broker_name = self.broker_name.trim();
            for namesrv_addr in default_mqadmin_ext.get_name_server_address_list().await {
                match default_mqadmin_ext
                    .add_write_perm_of_broker(namesrv_addr.clone(), broker_name.into())
                    .await
                    .map_err(|e| {
                        RocketMQError::Internal(format!("AddWritePermSubCommand: Failed to add write perm: {}", e))
                    }) {
                    Ok(add_topic_count) => {
                        println!(
                            "add write perm of broker[{broker_name}] in name server[{namesrv_addr}] OK, \
                             {add_topic_count}"
                        );
                    }
                    Err(e) => {
                        println!("add write perm of broker[{broker_name}] in name server[{namesrv_addr}] Failed",);
                        println!("{e}")
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
