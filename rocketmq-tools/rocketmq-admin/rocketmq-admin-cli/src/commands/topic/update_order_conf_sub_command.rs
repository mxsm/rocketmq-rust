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

use clap::Parser;

use crate::commands::CommandExecute;
use crate::commands::CommonArgs;
use rocketmq_admin_core::core::topic::OrderConfMethod;
use rocketmq_admin_core::core::topic::OrderConfRequest;
use rocketmq_admin_core::core::topic::OrderConfResult;
use rocketmq_admin_core::core::topic::TopicService;

#[derive(Debug, Clone, Parser)]
pub struct UpdateOrderConfSubCommand {
    #[command(flatten)]
    common_args: CommonArgs,

    #[arg(short = 't', long = "topic", required = true, help = "topic name")]
    topic: String,

    #[arg(
        short = 'm',
        long = "method",
        required = true,
        help = "option type [eg. put|get|delete"
    )]
    method: String,

    #[arg(
        short = 'v',
        long = "orderConf",
        required = false,
        help = "set order conf [eg. brokerName1:num;brokerName2:num]"
    )]
    order_conf: Option<String>,
}

impl UpdateOrderConfSubCommand {
    fn request(&self) -> rocketmq_error::RocketMQResult<OrderConfRequest> {
        Ok(
            OrderConfRequest::try_new(self.topic.clone(), self.method.as_str(), self.order_conf.clone())?
                .with_optional_namesrv_addr(self.common_args.namesrv_addr.clone()),
        )
    }

    fn print_result(result: OrderConfResult) {
        match result.method {
            OrderConfMethod::Get => println!(
                "get orderConf success. topic={}, orderConf={}",
                result.topic,
                result.order_conf.unwrap_or_default()
            ),
            OrderConfMethod::Put => println!(
                "update orderConf success. topic={}, orderConf={}",
                result.topic,
                result.order_conf.unwrap_or_default()
            ),
            OrderConfMethod::Delete => println!("delete orderConf success. topic={}", result.topic),
        }
        println!("mqadmin UpdateOrderConf");
    }
}

impl CommandExecute for UpdateOrderConfSubCommand {
    async fn execute(
        &self,
        _rpc_hook: Option<std::sync::Arc<dyn rocketmq_remoting::runtime::RPCHook>>,
    ) -> rocketmq_error::RocketMQResult<()> {
        let result = TopicService::apply_order_conf(self.request()?).await?;
        Self::print_result(result);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_order_conf_sub_command_parse_put() {
        let cmd = UpdateOrderConfSubCommand::try_parse_from([
            "updateOrderConf",
            "-t",
            "TestTopic",
            "-m",
            "put",
            "-v",
            "broker-a:4",
            "-n",
            "127.0.0.1:9876",
        ])
        .unwrap();

        assert_eq!(cmd.topic, "TestTopic");
        assert_eq!(cmd.method, "put");
        assert_eq!(cmd.order_conf, Some("broker-a:4".to_string()));
    }
}
