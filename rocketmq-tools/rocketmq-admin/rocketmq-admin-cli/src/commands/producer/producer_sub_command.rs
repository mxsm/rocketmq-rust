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
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use rocketmq_admin_core::admin::default_mq_admin_ext::DefaultMQAdminExt;

#[derive(Debug, Clone, Parser)]
pub struct ProducerSubCommand {
    #[arg(short = 'b', long = "broker", required = true, help = "broker address")]
    broker_addr: String,
}

impl CommandExecute for ProducerSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let mut default_mqadmin_ext = if let Some(rpc_hook) = rpc_hook {
            DefaultMQAdminExt::with_rpc_hook(rpc_hook)
        } else {
            DefaultMQAdminExt::new()
        };

        default_mqadmin_ext
            .client_config_mut()
            .set_instance_name(current_millis().to_string().into());

        let operation_result = async {
            MQAdminExt::start(&mut default_mqadmin_ext).await.map_err(|e| {
                RocketMQError::Internal(format!("ProducerSubCommand: Failed to start MQAdminExt: {}", e))
            })?;

            let broker_addr = self.broker_addr.trim();
            let producer_table_info = default_mqadmin_ext
                .get_all_producer_info(CheetahString::from(broker_addr))
                .await
                .map_err(|e| {
                    RocketMQError::Internal(format!("ProducerSubCommand: Failed to get all producer info: {}", e))
                })?;

            let data = producer_table_info.data();
            if data.is_empty() {
                println!("No producer groups found on broker {}", broker_addr);
                return Ok(());
            }

            for (group, producers) in data {
                if producers.is_empty() {
                    println!("producer group ({}) instances are empty", group);
                    continue;
                }
                for producer in producers {
                    println!("producer group ({}) instance : {}", group, producer);
                }
            }

            Ok(())
        }
        .await;

        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;
        operation_result
    }
}
