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

/// Query message queue allocation for a topic
#[derive(Debug, Clone, Parser)]
pub struct AllocateMqCommand {
    /// Topic name
    #[arg(short = 't', long = "topic", required = true)]
    pub topic: String,

    /// Comma-separated IP addresses
    #[arg(short = 'i', long = "ip-list", required = true)]
    pub ip_list: String,

    #[command(flatten)]
    pub common: CommonArgs,
}

impl CommandExecute for AllocateMqCommand {
    async fn execute(&self, _rpc_hook: Option<std::sync::Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let mut builder = AdminBuilder::new();
        if let Some(addr) = &self.common.namesrv_addr {
            builder = builder.namesrv_addr(addr.trim());
        }
        let mut admin = builder.build_with_guard().await?;

        TopicOperations::query_allocated_mq(
            &mut admin,
            CheetahString::from(self.topic.clone()),
            CheetahString::from(self.ip_list.clone()),
        )
        .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_allocate_mq_command() {
        let cmd = AllocateMqCommand::try_parse_from([
            "allocate_mq",
            "-t",
            "TestTopic",
            "-i",
            "192.168.1.1,192.168.1.2",
            "-n",
            "127.0.0.1:9876",
        ])
        .unwrap();

        assert_eq!(cmd.topic, "TestTopic");
        assert_eq!(cmd.ip_list, "192.168.1.1,192.168.1.2");
    }
}
