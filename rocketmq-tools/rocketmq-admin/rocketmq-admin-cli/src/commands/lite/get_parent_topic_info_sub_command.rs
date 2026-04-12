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
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::body::get_parent_topic_info_response_body::GetParentTopicInfoResponseBody;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use rocketmq_admin_core::core::lite::LiteService;
use rocketmq_admin_core::core::lite::ParentTopicInfoQueryRequest;
use rocketmq_admin_core::core::lite::ParentTopicInfoQueryResult;

#[derive(Debug, Clone, Parser)]
pub struct GetParentTopicInfoSubCommand {
    #[arg(short = 'p', long = "parentTopic", required = true, help = "Parent topic name")]
    parent_topic: String,
}

impl CommandExecute for GetParentTopicInfoSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let result = LiteService::query_parent_topic_info_by_request_with_rpc_hook(self.request()?, rpc_hook).await?;
        Self::print_result(&result);
        Ok(())
    }
}

impl GetParentTopicInfoSubCommand {
    fn request(&self) -> RocketMQResult<ParentTopicInfoQueryRequest> {
        ParentTopicInfoQueryRequest::try_new(self.parent_topic.clone())
    }

    fn print_result(result: &ParentTopicInfoQueryResult) {
        println!("Parent Topic Info: [{}]", result.parent_topic);
        Self::print_header();
        for entry in &result.entries {
            if let Some(body) = &entry.body {
                Self::print_row(&entry.broker_name, body);
            } else {
                println!("[{}] error.", entry.broker_name);
            }
        }
    }

    fn print_header() {
        println!(
            "{:<50} {:<8} {:<14} {:<14} {:<100}",
            "#Broker Name", "#TTL", "#Lite Count", "#LMQ NUM", "#GROUPS"
        );
    }

    fn print_row(broker_name: &CheetahString, body: &GetParentTopicInfoResponseBody) {
        let display_name = Self::front_string_at_least(broker_name.as_str(), 40);
        let groups_display = format!("{:?}", body.get_groups());
        println!(
            "{:<50} {:<8} {:<14} {:<14} {:<100}",
            display_name,
            body.get_ttl(),
            body.get_lite_topic_count(),
            body.get_lmq_num(),
            groups_display
        );
    }

    fn front_string_at_least(s: &str, size: usize) -> String {
        s.chars().take(size).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_parent_topic_info_sub_command_builds_request() {
        let cmd = GetParentTopicInfoSubCommand::try_parse_from(["getParentTopicInfo", "-p", " ParentTopic "]).unwrap();

        assert_eq!(cmd.request().unwrap().parent_topic().as_str(), "ParentTopic");
    }
}
