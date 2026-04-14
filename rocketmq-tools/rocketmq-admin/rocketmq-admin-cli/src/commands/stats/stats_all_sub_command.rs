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
use rocketmq_admin_core::core::stats::StatsAllQueryRequest;
use rocketmq_admin_core::core::stats::StatsAllRow;
use rocketmq_admin_core::core::stats::StatsService;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;
use tracing::warn;

use crate::commands::CommandExecute;

#[derive(Debug, Clone, Parser)]
pub struct StatsAllSubCommand {
    #[arg(
        short = 'a',
        long = "activeTopic",
        help = "print active topic only",
        required = false
    )]
    active_topic: bool,

    #[arg(short = 't', long = "topic", required = false, help = "print select topic only")]
    topic: Option<String>,
}

impl StatsAllSubCommand {
    fn request(&self) -> StatsAllQueryRequest {
        StatsAllQueryRequest::new(self.active_topic, self.topic.clone())
    }

    fn print_row(row: &StatsAllRow) {
        let topic = Self::front_string_at_least(row.topic.as_str(), 64);
        let consumer_group = row
            .consumer_group
            .as_ref()
            .map(|group| Self::front_string_at_least(group.as_str(), 64))
            .unwrap_or_default();

        match (row.out_tps, row.out_msg_count_24h) {
            (Some(out_tps), Some(out_msg_count_24h)) => println!(
                "{:<64}  {:<64} {:>12} {:>11.2} {:>11.2} {:>14} {:>14}",
                topic, consumer_group, row.accumulation, row.in_tps, out_tps, row.in_msg_count_24h, out_msg_count_24h,
            ),
            _ => println!(
                "{:<64}  {:<64} {:>12} {:>11.2} {:>11} {:>14} {:>14}",
                topic, "", 0, row.in_tps, "", row.in_msg_count_24h, "NO_CONSUMER",
            ),
        }
    }

    fn front_string_at_least(s: &str, size: usize) -> String {
        s.chars().take(size).collect()
    }
}

impl CommandExecute for StatsAllSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let result = StatsService::query_stats_all_by_request_with_rpc_hook(self.request(), rpc_hook).await?;

        println!(
            "{:<64}  {:<64} {:>12} {:>11} {:>11} {:>14} {:>14}",
            "#Topic", "#Consumer Group", "#Accumulation", "#InTPS", "#OutTPS", "#InMsg24Hour", "#OutMsg24Hour",
        );

        for row in &result.rows {
            Self::print_row(row);
        }

        for failure in &result.failures {
            warn!(
                "statsAll: failed to collect stats for topic '{}': {}",
                failure.topic, failure.error
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stats_all_sub_command_builds_core_request() {
        let cmd = StatsAllSubCommand::try_parse_from(["statsAll", "-a", "-t", " TopicA "]).unwrap();

        let request = cmd.request();

        assert!(request.active_topic());
        assert_eq!(request.topic().map(|topic| topic.as_str()), Some("TopicA"));
    }
}
