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
use tabled::settings::Style;
use tabled::Table;
use tabled::Tabled;

use crate::commands::CommandExecute;
use crate::commands::CommonArgs;
use crate::core::admin::AdminBuilder;
use crate::core::topic::TopicOperations;
use crate::core::RocketMQResult;

/// Show topic statistics
#[derive(Debug, Clone, Parser)]
pub struct TopicStatusCommand {
    /// Topic name
    #[arg(short = 't', long = "topic", required = true)]
    pub topic: String,

    /// Broker address (optional)
    #[arg(short = 'b', long = "broker-addr")]
    pub broker_addr: Option<String>,

    #[command(flatten)]
    pub common: CommonArgs,
}

impl CommandExecute for TopicStatusCommand {
    async fn execute(&self, _rpc_hook: Option<std::sync::Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let mut builder = AdminBuilder::new();
        if let Some(addr) = &self.common.namesrv_addr {
            builder = builder.namesrv_addr(addr.trim());
        }
        let mut admin = builder.build_with_guard().await?;

        let stats = TopicOperations::get_topic_stats(
            &mut admin,
            CheetahString::from(self.topic.clone()),
            self.broker_addr.as_ref().map(|s| CheetahString::from(s.clone())),
        )
        .await?;

        let stats_list: Vec<_> = stats
            .get_offset_table()
            .iter()
            .map(|(mq, offset_info)| TopicStatsInfo {
                broker_name: mq.get_broker_name().to_string(),
                queue_id: mq.get_queue_id(),
                min_offset: offset_info.get_min_offset(),
                max_offset: offset_info.get_max_offset(),
                last_update_timestamp: offset_info.get_last_update_timestamp(),
            })
            .collect();

        if stats_list.is_empty() {
            println!("No stats found for topic: {}", self.topic);
            return Ok(());
        }

        let total_min: i64 = stats_list.iter().map(|s| s.min_offset).sum();
        let total_max: i64 = stats_list.iter().map(|s| s.max_offset).sum();
        let total_diff = total_max - total_min;

        println!("Topic: {}\n", self.topic);

        let mut table = Table::new(stats_list);
        table.with(Style::modern());
        println!("{table}\n");

        println!("Summary:");
        println!("  Total Min Offset: {total_min}");
        println!("  Total Max Offset: {total_max}");
        println!("  Total Diff:       {total_diff}");

        Ok(())
    }
}

#[derive(Debug, Clone, Tabled)]
struct TopicStatsInfo {
    #[tabled(rename = "Broker Name")]
    broker_name: String,
    #[tabled(rename = "Queue ID")]
    queue_id: i32,
    #[tabled(rename = "Min Offset")]
    min_offset: i64,
    #[tabled(rename = "Max Offset")]
    max_offset: i64,
    #[tabled(rename = "Last Update")]
    last_update_timestamp: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_status_command() {
        let cmd =
            TopicStatusCommand::try_parse_from(["topic_status", "-t", "TestTopic", "-n", "127.0.0.1:9876"]).unwrap();

        assert_eq!(cmd.topic, "TestTopic");
        assert_eq!(cmd.common.namesrv_addr, Some("127.0.0.1:9876".to_string()));
        assert!(cmd.broker_addr.is_none());
    }

    #[test]
    fn test_topic_status_with_broker() {
        let cmd = TopicStatusCommand::try_parse_from([
            "topic_status",
            "-t",
            "TestTopic",
            "-n",
            "127.0.0.1:9876",
            "-b",
            "127.0.0.1:10911",
        ])
        .unwrap();

        assert_eq!(cmd.topic, "TestTopic");
        assert!(cmd.broker_addr.is_some());
        assert_eq!(cmd.broker_addr.as_ref().unwrap(), "127.0.0.1:10911");
    }
}
