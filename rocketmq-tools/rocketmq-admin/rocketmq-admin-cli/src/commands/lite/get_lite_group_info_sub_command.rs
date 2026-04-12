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
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_common::utils::util_all::time_millis_to_human_string2;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::admin::offset_wrapper::OffsetWrapper;
use rocketmq_remoting::protocol::body::lite_lag_info::LiteLagInfo;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use rocketmq_admin_core::core::lite::LiteGroupInfoQueryRequest;
use rocketmq_admin_core::core::lite::LiteGroupInfoQueryResult;
use rocketmq_admin_core::core::lite::LiteService;

#[derive(Debug, Clone, Parser)]
pub struct GetLiteGroupInfoSubCommand {
    #[arg(short = 'p', long = "parentTopic", required = true, help = "Parent topic name")]
    parent_topic: String,

    #[arg(short = 'g', long = "group", required = true, help = "Consumer group")]
    group: String,

    #[arg(short = 'l', long = "liteTopic", required = false, help = "Query lite topic detail")]
    lite_topic: Option<String>,

    #[arg(short = 'k', long = "topK", required = false, help = "TopK value of each broker")]
    top_k: Option<i32>,
}

impl CommandExecute for GetLiteGroupInfoSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let result = LiteService::query_lite_group_info_by_request_with_rpc_hook(self.request()?, rpc_hook).await?;
        Self::print_result(&result);
        Ok(())
    }
}

impl GetLiteGroupInfoSubCommand {
    fn request(&self) -> RocketMQResult<LiteGroupInfoQueryRequest> {
        LiteGroupInfoQueryRequest::try_new(
            self.parent_topic.clone(),
            self.group.clone(),
            self.lite_topic.clone(),
            self.top_k,
        )
    }

    fn print_result(result: &LiteGroupInfoQueryResult) {
        println!("Lite Group Info: [{}] [{}]", result.group, result.parent_topic);

        if result.query_by_lite_topic() {
            println!(
                "{:<50} {:<16} {:<16} {:<16} {:<30}",
                "#Broker Name", "#BrokerOffset", "#ConsumeOffset", "#LagCount", "#LastUpdate"
            );
        }

        for entry in &result.entries {
            match &entry.body {
                Some(body) => Self::print_offset_wrapper(
                    result.query_by_lite_topic(),
                    &entry.broker_name,
                    body.lite_topic_offset_wrapper(),
                ),
                None => println!("[{}] error.", entry.broker_name),
            }
        }

        println!("Total Lag Count: {}", result.total_lag_count);
        let lag_time = current_millis() as i64 - result.earliest_unconsumed_timestamp;
        println!(
            "Min Unconsumed Timestamp: {} ({} s ago)\n",
            result.earliest_unconsumed_timestamp,
            lag_time / 1000
        );

        if result.query_by_lite_topic() {
            return;
        }

        Self::print_lag_top_k("------TopK by lag count-----", &result.lag_count_top_k);
        Self::print_lag_top_k("\n------TopK by lag time------", &result.lag_timestamp_top_k);
    }

    fn print_lag_top_k(title: &str, lag_infos: &[LiteLagInfo]) {
        println!("{title}");
        println!(
            "{:<6} {:<40} {:<12} {:<30}",
            "NO", "Lite Topic", "Lag Count", "UnconsumedTimestamp"
        );
        for (i, info) in lag_infos.iter().enumerate() {
            let timestamp_str = if info.earliest_unconsumed_timestamp() > 0 {
                time_millis_to_human_string2(info.earliest_unconsumed_timestamp())
            } else {
                "-".to_string()
            };
            println!(
                "{:<6} {:<40} {:<12} {:<30}",
                i + 1,
                info.lite_topic(),
                info.lag_count(),
                timestamp_str
            );
        }
    }

    fn print_offset_wrapper(
        query_by_lite_topic: bool,
        broker_name: &CheetahString,
        offset_wrapper: Option<&OffsetWrapper>,
    ) {
        if !query_by_lite_topic {
            return;
        }

        match offset_wrapper {
            None => {
                println!("{:<50} {:<16} {:<16} {:<16} {:<30}", broker_name, "-", "-", "-", "-");
            }
            Some(wrapper) => {
                let last_update_str = if wrapper.get_last_timestamp() > 0 {
                    time_millis_to_human_string2(wrapper.get_last_timestamp())
                } else {
                    "-".to_string()
                };
                println!(
                    "{:<50} {:<16} {:<16} {:<16} {:<30}",
                    broker_name,
                    wrapper.get_broker_offset(),
                    wrapper.get_consumer_offset(),
                    wrapper.get_broker_offset() - wrapper.get_consumer_offset(),
                    last_update_str
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_lite_group_info_sub_command_builds_default_top_k_request() {
        let cmd =
            GetLiteGroupInfoSubCommand::try_parse_from(["getLiteGroupInfo", "-p", " ParentTopic ", "-g", " GroupA "])
                .unwrap();

        let request = cmd.request().unwrap();

        assert_eq!(request.parent_topic().as_str(), "ParentTopic");
        assert_eq!(request.group().as_str(), "GroupA");
        assert_eq!(request.lite_topic(), None);
        assert_eq!(request.top_k(), 20);
    }

    #[test]
    fn get_lite_group_info_sub_command_builds_lite_topic_request() {
        let cmd = GetLiteGroupInfoSubCommand::try_parse_from([
            "getLiteGroupInfo",
            "-p",
            "ParentTopic",
            "-g",
            "GroupA",
            "-l",
            " LiteTopic ",
            "-k",
            "5",
        ])
        .unwrap();

        let request = cmd.request().unwrap();

        assert_eq!(request.lite_topic().map(|topic| topic.as_str()), Some("LiteTopic"));
        assert_eq!(request.top_k(), 5);
    }
}
