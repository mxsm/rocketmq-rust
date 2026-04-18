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
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use rocketmq_admin_core::core::lite::LiteService;
use rocketmq_admin_core::core::lite::TriggerLiteDispatchRequest;
use rocketmq_admin_core::core::lite::TriggerLiteDispatchResult;

#[derive(Debug, Clone, Parser)]
pub struct TriggerLiteDispatchSubCommand {
    #[arg(short = 'p', long = "parentTopic", required = true, help = "Parent topic name")]
    parent_topic: String,

    #[arg(short = 'g', long = "group", required = true, help = "Consumer group")]
    group: String,

    #[arg(short = 'c', long = "clientId", required = false, help = "clientId (optional)")]
    client_id: Option<String>,

    #[arg(short = 'b', long = "brokerName", required = false, help = "brokerName (optional)")]
    broker_name: Option<String>,
}

impl CommandExecute for TriggerLiteDispatchSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let result = LiteService::trigger_lite_dispatch_by_request_with_rpc_hook(self.request()?, rpc_hook).await?;
        Self::print_result(&result);
        Ok(())
    }
}

impl TriggerLiteDispatchSubCommand {
    fn request(&self) -> RocketMQResult<TriggerLiteDispatchRequest> {
        TriggerLiteDispatchRequest::try_new(
            self.parent_topic.clone(),
            self.group.clone(),
            self.client_id.clone(),
            self.broker_name.clone(),
        )
    }

    fn print_result(result: &TriggerLiteDispatchResult) {
        println!("Group And Topic Info: [{}] [{}]\n", result.group, result.parent_topic);

        for entry in &result.entries {
            println!(
                "{:<30} {:<12}",
                entry.broker_name,
                if entry.dispatched { "dispatched" } else { "error" }
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trigger_lite_dispatch_sub_command_builds_request() {
        let cmd = TriggerLiteDispatchSubCommand::try_parse_from([
            "triggerLiteDispatch",
            "-p",
            " ParentTopic ",
            "-g",
            " GroupA ",
            "-c",
            " ClientA ",
            "-b",
            " BrokerA ",
        ])
        .unwrap();

        let request = cmd.request().unwrap();
        assert_eq!(request.parent_topic().as_str(), "ParentTopic");
        assert_eq!(request.group().as_str(), "GroupA");
        assert_eq!(request.client_id().map(|client| client.as_str()), Some("ClientA"));
        assert_eq!(request.broker_name().map(|broker| broker.as_str()), Some("BrokerA"));
    }
}
