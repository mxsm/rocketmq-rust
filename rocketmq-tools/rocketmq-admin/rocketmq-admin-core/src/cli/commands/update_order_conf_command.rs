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

use cheetah_string::CheetahString;
use clap::Parser;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use crate::commands::CommonArgs;
use crate::core::admin::AdminBuilder;
use crate::core::topic::TopicOperations;
use crate::core::RocketMQResult;
use crate::core::ToolsError;

/// Update order configuration for a topic
#[derive(Debug, Clone, Parser)]
pub struct UpdateOrderConfCommand {
    /// Topic name
    #[arg(short = 't', long = "topic", required = true)]
    pub topic: String,

    /// Method: put, get, delete
    #[arg(short = 'm', long = "method", required = true)]
    pub method: String,

    /// Order configuration (required for put method)
    /// Format: brokerName1:num;brokerName2:num
    #[arg(short = 'v', long = "order-conf", required_if_eq("method", "put"))]
    pub order_conf: Option<String>,

    #[command(flatten)]
    pub common: CommonArgs,
}

impl CommandExecute for UpdateOrderConfCommand {
    async fn execute(&self, _rpc_hook: Option<std::sync::Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let mut builder = AdminBuilder::new();
        if let Some(addr) = &self.common.namesrv_addr {
            builder = builder.namesrv_addr(addr.trim());
        }
        let mut admin = builder.build_with_guard().await?;

        let method = self.method.to_lowercase();

        match method.as_str() {
            "put" => {
                let order_conf = self.order_conf.as_ref().ok_or_else(|| ToolsError::ValidationFailed {
                    message: "order_conf is required for 'put' method".to_string(),
                })?;

                TopicOperations::create_or_update_order_conf(
                    &mut admin,
                    CheetahString::from(self.topic.clone()),
                    CheetahString::from(order_conf.clone()),
                )
                .await?;

                println!(
                    "Successfully updated order config for topic '{}': {}",
                    self.topic, order_conf
                );
            }
            "get" => {
                let order_conf =
                    TopicOperations::get_order_conf(&mut admin, CheetahString::from(self.topic.clone())).await?;

                println!("Order config for topic '{}': {}", self.topic, order_conf);
            }
            "delete" => {
                TopicOperations::delete_order_conf(&mut admin, CheetahString::from(self.topic.clone())).await?;

                println!("Successfully deleted order config for topic '{}'", self.topic);
            }
            _ => {
                return Err(ToolsError::ValidationFailed {
                    message: format!("Invalid method '{}'. Allowed values: put, get, delete", method),
                }
                .into());
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_update_order_conf_put() {
        let cmd = UpdateOrderConfCommand::try_parse_from([
            "update_order_conf",
            "-t",
            "TestTopic",
            "-m",
            "put",
            "-v",
            "broker-a:4;broker-b:4",
            "-n",
            "127.0.0.1:9876",
        ])
        .unwrap();

        assert_eq!(cmd.topic, "TestTopic");
        assert_eq!(cmd.method, "put");
        assert!(cmd.order_conf.is_some());
    }

    #[test]
    fn test_update_order_conf_get() {
        let cmd = UpdateOrderConfCommand::try_parse_from([
            "update_order_conf",
            "-t",
            "TestTopic",
            "-m",
            "get",
            "-n",
            "127.0.0.1:9876",
        ])
        .unwrap();

        assert_eq!(cmd.method, "get");
        assert!(cmd.order_conf.is_none());
    }
}
