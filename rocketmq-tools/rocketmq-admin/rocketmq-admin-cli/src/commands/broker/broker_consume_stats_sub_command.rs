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
use rocketmq_admin_core::core::broker::BrokerConsumeStatsQueryRequest;
use rocketmq_admin_core::core::broker::BrokerConsumeStatsResult;
use rocketmq_admin_core::core::broker::BrokerService;

#[derive(Debug, Clone, Parser)]
pub struct BrokerConsumeStatsSubCommand {
    #[arg(short = 'b', long = "brokerAddr", required = true, help = "Broker address")]
    broker_addr: String,

    #[arg(
        short = 't',
        long = "timeoutMillis",
        required = false,
        default_value = "50000",
        help = "request timeout Millis"
    )]
    timeout_millis: u64,

    #[arg(
        short = 'l',
        long = "level",
        required = false,
        default_value = "0",
        help = "threshold of print diff"
    )]
    diff_level: i64,

    #[arg(
        short = 'o',
        long = "order",
        required = false,
        default_value = "false",
        help = "order topic"
    )]
    is_order: String,
}

impl BrokerConsumeStatsSubCommand {
    fn request(&self) -> RocketMQResult<BrokerConsumeStatsQueryRequest> {
        BrokerConsumeStatsQueryRequest::try_new(
            self.broker_addr.clone(),
            self.timeout_millis,
            self.diff_level,
            self.is_order.trim().parse::<bool>().unwrap_or(false),
        )
    }
}

fn format_timestamp(timestamp: i64) -> String {
    if timestamp <= 0 {
        return "-".to_string();
    }
    let secs = timestamp / 1000;
    let nanos = ((timestamp % 1000) * 1_000_000) as u32;
    match chrono::DateTime::from_timestamp(secs, nanos) {
        Some(dt) => {
            use chrono::Local;
            let local_dt = dt.with_timezone(&Local);
            local_dt.format("%Y-%m-%d %H:%M:%S").to_string()
        }
        None => "-".to_string(),
    }
}

impl CommandExecute for BrokerConsumeStatsSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let result =
            BrokerService::query_broker_consume_stats_by_request_with_rpc_hook(self.request()?, rpc_hook).await?;
        print_broker_consume_stats_result(&result);
        Ok(())
    }
}

fn print_broker_consume_stats_result(result: &BrokerConsumeStatsResult) {
    println!(
        "{:<64}  {:<64}  {:<32}  {:<4}  {:<20}  {:<20}  {:<20}  #LastTime",
        "#Topic", "#Group", "#Broker Name", "#QID", "#Broker Offset", "#Consumer Offset", "#Diff"
    );

    for row in &result.rows {
        if row.last_timestamp <= 0 {
            continue;
        }

        println!(
            "{:<64}  {:<64}  {:<32}  {:<4}  {:<20}  {:<20}  {:<20}  {}",
            row.topic,
            row.group,
            row.broker_name,
            row.queue_id,
            row.broker_offset,
            row.consumer_offset,
            row.diff,
            format_timestamp(row.last_timestamp)
        );
    }

    println!("\nDiff Total: {}", result.total_diff);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_broker_consume_stats_request() {
        let cmd = BrokerConsumeStatsSubCommand::try_parse_from([
            "brokerConsumeStats",
            "-b",
            " 127.0.0.1:10911 ",
            "-t",
            "3000",
            "-l",
            "42",
            "-o",
            "true",
        ])
        .unwrap();

        let request = cmd.request().unwrap();

        assert_eq!(request.broker_addr().as_str(), "127.0.0.1:10911");
        assert_eq!(request.timeout_millis(), 3_000);
        assert_eq!(request.diff_level(), 42);
        assert!(request.is_order());
    }

    #[test]
    fn parses_invalid_order_flag_as_false_for_compatibility() {
        let cmd =
            BrokerConsumeStatsSubCommand::try_parse_from(["brokerConsumeStats", "-b", "127.0.0.1:10911", "-o", "bad"])
                .unwrap();

        assert!(!cmd.request().unwrap().is_order());
    }
}
