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
use rocketmq_admin_core::core::producer::ProducerService;
use rocketmq_admin_core::core::producer::SendMessageStatusRequest;
use rocketmq_admin_core::core::producer::SendMessageStatusResult;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;

#[derive(Debug, Clone, Parser)]
pub struct SendMsgStatusSubCommand {
    #[arg(
        short = 'b',
        long = "brokerName",
        required = true,
        help = "Broker Name e.g. clusterName_brokerName as DefaultCluster_broker-a"
    )]
    broker_name: String,

    #[arg(
        short = 's',
        long = "messageSize",
        required = false,
        default_value = "128",
        help = "Message Size, Default: 128"
    )]
    message_size: usize,

    #[arg(
        short = 'c',
        long = "count",
        required = false,
        default_value = "50",
        help = "send message count, Default: 50"
    )]
    count: u32,
}

impl SendMsgStatusSubCommand {
    fn request(&self) -> RocketMQResult<SendMessageStatusRequest> {
        SendMessageStatusRequest::try_new(self.broker_name.clone(), self.message_size, self.count)
    }

    fn print_result(result: &SendMessageStatusResult) {
        for row in &result.rows {
            println!("rt={}ms, SendResult={}", row.rt_millis, row.send_result);
        }
    }
}

impl CommandExecute for SendMsgStatusSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let result = ProducerService::send_message_status_by_request_with_rpc_hook(self.request()?, rpc_hook).await?;
        Self::print_result(&result);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn send_msg_status_sub_command_builds_core_request() {
        let command = SendMsgStatusSubCommand {
            broker_name: " broker-a ".to_string(),
            message_size: 256,
            count: 3,
        };

        let request = command.request().unwrap();

        assert_eq!(request.broker_name().as_str(), "broker-a");
        assert_eq!(request.message_size(), 256);
        assert_eq!(request.count(), 3);
    }
}
