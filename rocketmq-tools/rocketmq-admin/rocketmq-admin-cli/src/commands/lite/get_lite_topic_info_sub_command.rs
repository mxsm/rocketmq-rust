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
use rocketmq_common::UtilAll::time_millis_to_human_string2;
use rocketmq_common::common::lite;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::body::get_lite_topic_info_response_body::GetLiteTopicInfoResponseBody;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use rocketmq_admin_core::core::lite::LiteService;
use rocketmq_admin_core::core::lite::LiteTopicInfoQueryRequest;
use rocketmq_admin_core::core::lite::LiteTopicInfoQueryResult;

#[derive(Debug, Clone, Parser)]
pub struct GetLiteTopicInfoSubCommand {
    #[arg(short = 'p', long = "parentTopic", required = true, help = "Parent topic name")]
    parent_topic: String,

    #[arg(short = 'l', long = "liteTopic", required = true, help = "Lite topic name")]
    lite_topic: String,

    #[arg(short = 's', long = "showClientId", help = "Show all clientId")]
    show_client_id: bool,
}

impl CommandExecute for GetLiteTopicInfoSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let result = LiteService::query_lite_topic_info_by_request_with_rpc_hook(self.request()?, rpc_hook).await?;
        Self::print_result(&result, self.show_client_id);
        Ok(())
    }
}

impl GetLiteTopicInfoSubCommand {
    fn request(&self) -> RocketMQResult<LiteTopicInfoQueryRequest> {
        LiteTopicInfoQueryRequest::try_new(self.parent_topic.clone(), self.lite_topic.clone())
    }

    fn print_result(result: &LiteTopicInfoQueryResult, show_client_id: bool) {
        let lmq_name = lite::to_lmq_name(result.parent_topic.as_str(), result.lite_topic.as_str()).unwrap_or_default();
        println!(
            "Lite Topic Info: [{}] [{}] [{}]",
            result.parent_topic, result.lite_topic, lmq_name
        );
        println!(
            "{:<50} {:<14} {:<14} {:<30} {:<12} {:<18}",
            "#Broker Name", "#MinOffset", "#MaxOffset", "#LastUpdate", "#Sharding", "#SubClientCount"
        );

        for entry in &result.entries {
            if let Some(body) = &entry.body {
                Self::print_row(entry.broker_name.as_str(), body, show_client_id);
            } else {
                println!(
                    "[{}] error: {}",
                    entry.broker_name,
                    entry.error.as_deref().unwrap_or_default()
                );
            }
        }
    }

    fn print_row(broker_name: &str, body: &GetLiteTopicInfoResponseBody, show_client_id: bool) {
        let (min_offset, max_offset, last_update) = if let Some(topic_offset) = body.topic_offset() {
            (
                topic_offset.get_min_offset(),
                topic_offset.get_max_offset(),
                topic_offset.get_last_update_timestamp(),
            )
        } else {
            (0, 0, 0)
        };

        let last_update_str = if last_update > 0 {
            time_millis_to_human_string2(last_update)
        } else {
            "-".to_string()
        };

        let display_name: String = if broker_name.chars().count() > 40 {
            broker_name.chars().take(40).collect()
        } else {
            broker_name.to_string()
        };

        println!(
            "{:<50} {:<14} {:<14} {:<30} {:<12} {:<18}",
            display_name,
            min_offset,
            max_offset,
            last_update_str,
            body.sharding_to_broker(),
            body.subscriber().len()
        );

        if show_client_id {
            let display_list: Vec<String> = body
                .subscriber()
                .iter()
                .map(|client_group| format!("{}@{}", client_group.client_id, client_group.group))
                .collect();
            println!("{:?}", display_list);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_lite_topic_info_sub_command_builds_request() {
        let cmd = GetLiteTopicInfoSubCommand::try_parse_from([
            "getLiteTopicInfo",
            "-p",
            " ParentTopic ",
            "-l",
            " LiteTopic ",
            "-s",
        ])
        .unwrap();

        let request = cmd.request().unwrap();
        assert_eq!(request.parent_topic().as_str(), "ParentTopic");
        assert_eq!(request.lite_topic().as_str(), "LiteTopic");
        assert!(cmd.show_client_id);
    }
}
