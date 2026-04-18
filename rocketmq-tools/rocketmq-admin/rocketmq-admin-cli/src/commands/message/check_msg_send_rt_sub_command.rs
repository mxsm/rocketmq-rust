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
use rocketmq_admin_core::core::producer::CheckMessageSendRtRequest;
use rocketmq_admin_core::core::producer::CheckMessageSendRtResult;
use rocketmq_admin_core::core::producer::ProducerService;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;

#[derive(Debug, Clone, Parser)]
pub struct CheckMsgSendRTSubCommand {
    #[arg(short = 't', long = "topic", required = true, help = "topic name")]
    topic: String,

    #[arg(
        short = 'a',
        long = "amount",
        required = false,
        default_value = "100",
        help = "message amount | default 100"
    )]
    amount: u64,

    #[arg(
        short = 's',
        long = "size",
        required = false,
        default_value = "128",
        help = "message size | default 128 Byte"
    )]
    size: usize,
}

impl CheckMsgSendRTSubCommand {
    fn request(&self) -> RocketMQResult<CheckMessageSendRtRequest> {
        CheckMessageSendRtRequest::try_new(self.topic.clone(), self.amount, self.size)
    }

    fn print_result(result: &CheckMessageSendRtResult) {
        println!("{:<32}  {:<4}  {:<20}    #RT", "#Broker Name", "#QID", "#Send Result");
        for row in &result.rows {
            println!(
                "{:<32}  {:<4}  {:<20}    {}",
                row.broker_name, row.queue_id, row.send_success, row.rt_millis
            );
        }
        println!("Avg RT: {:.2}", result.avg_rt);
    }
}

impl CommandExecute for CheckMsgSendRTSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let result = ProducerService::check_message_send_rt_by_request_with_rpc_hook(self.request()?, rpc_hook).await?;
        Self::print_result(&result);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_msg_send_rt_sub_command_builds_core_request() {
        let command = CheckMsgSendRTSubCommand {
            topic: " TopicA ".to_string(),
            amount: 10,
            size: 256,
        };

        let request = command.request().unwrap();

        assert_eq!(request.topic().as_str(), "TopicA");
        assert_eq!(request.amount(), 10);
        assert_eq!(request.size(), 256);
    }
}
