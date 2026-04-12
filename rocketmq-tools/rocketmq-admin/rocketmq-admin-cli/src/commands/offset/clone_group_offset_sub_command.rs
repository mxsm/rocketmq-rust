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
use rocketmq_admin_core::core::offset::CloneGroupOffsetRequest;
use rocketmq_admin_core::core::offset::OffsetService;

#[derive(Debug, Clone, Parser)]
pub struct CloneGroupOffsetSubCommand {
    #[arg(short = 's', long = "srcGroup", required = true, help = "set source consumer group")]
    src_group: String,

    #[arg(
        short = 'd',
        long = "destGroup",
        required = true,
        help = "set destination consumer group"
    )]
    dest_group: String,

    #[arg(short = 't', long = "topic", required = true, help = "set the topic")]
    topic: String,

    #[arg(
        short = 'o',
        long = "offline",
        required = false,
        help = "the group or the topic is offline"
    )]
    offline: bool,
}

impl CloneGroupOffsetSubCommand {
    fn request(&self) -> RocketMQResult<CloneGroupOffsetRequest> {
        CloneGroupOffsetRequest::try_new(
            self.src_group.clone(),
            self.dest_group.clone(),
            self.topic.clone(),
            self.offline,
        )
    }
}

impl CommandExecute for CloneGroupOffsetSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let request = self.request()?;
        OffsetService::clone_group_offset_by_request_with_rpc_hook(request.clone(), rpc_hook).await?;
        println!(
            "clone group offset success. srcGroup[{}], destGroup=[{}], topic[{}]",
            request.src_group(),
            request.dest_group(),
            request.topic()
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[test]
    fn parses_clone_group_offset_request() {
        let cmd = CloneGroupOffsetSubCommand::try_parse_from([
            "cloneGroupOffset",
            "-s",
            " SourceGroup ",
            "-d",
            " DestGroup ",
            "-t",
            " TestTopic ",
            "-o",
        ])
        .unwrap();
        let request = cmd.request().unwrap();

        assert_eq!(request.src_group().as_str(), "SourceGroup");
        assert_eq!(request.dest_group().as_str(), "DestGroup");
        assert_eq!(request.topic().as_str(), "TestTopic");
        assert!(request.offline());
    }
}
