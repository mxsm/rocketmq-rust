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
use rocketmq_admin_core::core::broker::BrokerService;
use rocketmq_admin_core::core::broker::ResetMasterFlushOffsetRequest;

#[derive(Debug, Clone, Parser)]
pub struct ResetMasterFlushOffsetSubCommand {
    #[arg(short = 'b', long = "brokerAddr", required = false, help = "which broker to reset")]
    broker_addr: Option<String>,

    #[arg(short = 'o', long = "offset", required = false, help = "the offset to reset at")]
    offset: Option<i64>,
}

impl ResetMasterFlushOffsetSubCommand {
    fn request(&self) -> RocketMQResult<ResetMasterFlushOffsetRequest> {
        ResetMasterFlushOffsetRequest::try_new(self.broker_addr.clone(), self.offset)
    }
}

impl CommandExecute for ResetMasterFlushOffsetSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let request = self.request()?;
        BrokerService::reset_master_flush_offset_by_request_with_rpc_hook(request.clone(), rpc_hook).await?;
        println!("reset master flush offset to {} success", request.master_flush_offset());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[test]
    fn parses_reset_master_flush_offset_request() {
        let cmd = ResetMasterFlushOffsetSubCommand::try_parse_from([
            "resetMasterFlushOffset",
            "-b",
            " 127.0.0.1:10911 ",
            "-o",
            "1024",
        ])
        .unwrap();
        let request = cmd.request().unwrap();

        assert_eq!(request.broker_addr().as_str(), "127.0.0.1:10911");
        assert_eq!(request.master_flush_offset(), 1024);
    }
}
