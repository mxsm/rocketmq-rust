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
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::body::get_lite_client_info_response_body::GetLiteClientInfoResponseBody;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use rocketmq_admin_core::core::lite::LiteClientInfoQueryRequest;
use rocketmq_admin_core::core::lite::LiteClientInfoQueryResult;
use rocketmq_admin_core::core::lite::LiteService;

#[derive(Debug, Clone, Parser)]
pub struct GetLiteClientInfoSubCommand {
    #[arg(short = 'p', long = "parentTopic", required = true, help = "Parent topic name")]
    parent_topic: String,

    #[arg(short = 'g', long = "group", required = true, help = "Consumer group")]
    group: String,

    #[arg(short = 'c', long = "clientId", required = true, help = "Client id")]
    client_id: String,

    #[arg(short = 's', long = "showDetail", help = "Show details")]
    show_detail: bool,
}

impl CommandExecute for GetLiteClientInfoSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let result = LiteService::query_lite_client_info_by_request_with_rpc_hook(self.request()?, rpc_hook).await?;
        Self::print_result(&result, self.show_detail);
        Ok(())
    }
}

impl GetLiteClientInfoSubCommand {
    fn request(&self) -> RocketMQResult<LiteClientInfoQueryRequest> {
        LiteClientInfoQueryRequest::try_new(self.parent_topic.clone(), self.group.clone(), self.client_id.clone())
    }

    fn print_result(result: &LiteClientInfoQueryResult, show_detail: bool) {
        println!(
            "Lite Client Info: [{}] [{}] [{}]",
            result.parent_topic, result.group, result.client_id
        );
        Self::print_header();

        for entry in &result.entries {
            match &entry.body {
                Some(body) => Self::print_row(body, entry.broker_name.as_str(), show_detail),
                None => println!("[{}] error.", entry.broker_name),
            }
        }
    }

    fn print_header() {
        println!(
            "{:<30} {:<20} {:<30} {:<30}",
            "#Broker", "#LiteTopicCount", "#LastAccessTime", "#LastConsumeTime"
        );
    }

    fn print_row(response_body: &GetLiteClientInfoResponseBody, broker_name: &str, show_detail: bool) {
        let lite_topic_count = if response_body.lite_topic_count() > 0 {
            response_body.lite_topic_count().to_string()
        } else {
            "N/A".to_string()
        };

        let last_access_time = if response_body.last_access_time() > 0 {
            time_millis_to_human_string2(response_body.last_access_time() as i64)
        } else {
            "N/A".to_string()
        };

        let last_consume_time = if response_body.last_consume_time() > 0 {
            time_millis_to_human_string2(response_body.last_consume_time() as i64)
        } else {
            "N/A".to_string()
        };

        println!(
            "{:<30} {:<20} {:<30} {:<30}",
            broker_name, lite_topic_count, last_access_time, last_consume_time
        );

        if show_detail && !response_body.lite_topic_set().is_empty() {
            println!("Lite Topics: {:?}\n", response_body.lite_topic_set());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_lite_client_info_sub_command_builds_request() {
        let cmd = GetLiteClientInfoSubCommand::try_parse_from([
            "getLiteClientInfo",
            "-p",
            " ParentTopic ",
            "-g",
            " GroupA ",
            "-c",
            " ClientA ",
            "-s",
        ])
        .unwrap();

        let request = cmd.request().unwrap();
        assert_eq!(request.parent_topic().as_str(), "ParentTopic");
        assert_eq!(request.group().as_str(), "GroupA");
        assert_eq!(request.client_id().as_str(), "ClientA");
        assert!(cmd.show_detail);
    }
}
