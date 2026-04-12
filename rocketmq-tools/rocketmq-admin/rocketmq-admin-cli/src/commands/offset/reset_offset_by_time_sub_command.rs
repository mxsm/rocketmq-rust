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

use chrono::Local;
use chrono::NaiveDateTime;
use chrono::TimeZone;
use clap::Parser;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::admin::rollback_stats::RollbackStats;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use crate::commands::CommonArgs;
use rocketmq_admin_core::core::offset::OffsetService;
use rocketmq_admin_core::core::offset::ResetOffsetByTimeRequest;
use rocketmq_admin_core::core::offset::ResetOffsetByTimeResult;

/// Timestamp format used by the Java reference implementation.
const TIMESTAMP_FORMAT: &str = "%Y-%m-%d#%H:%M:%S:%3f";

/// Parse a timestamp string in one of the three supported forms:
///   - `"now"`  -> current system time in milliseconds
///   - a plain decimal integer -> milliseconds since epoch
///   - `"yyyy-MM-dd#HH:mm:ss:SSS"` formatted string
fn parse_timestamp(s: &str) -> RocketMQResult<u64> {
    let s = s.trim();
    if s.eq_ignore_ascii_case("now") {
        return Ok(current_millis());
    }
    if let Ok(ms) = s.parse::<u64>() {
        return Ok(ms);
    }
    if let Ok(ndt) = NaiveDateTime::parse_from_str(s, TIMESTAMP_FORMAT) {
        let millis = Local
            .from_local_datetime(&ndt)
            .single()
            .ok_or_else(|| RocketMQError::IllegalArgument(format!("Ambiguous local datetime: {s}")))?
            .timestamp_millis();
        if millis < 0 {
            return Err(RocketMQError::IllegalArgument(format!(
                "Parsed timestamp is negative (before epoch): {millis}"
            )));
        }
        return Ok(millis as u64);
    }
    Err(RocketMQError::IllegalArgument(format!(
        "Cannot parse timestamp '{}'. Supported formats: 'now', milliseconds (integer), 'yyyy-MM-dd#HH:mm:ss:SSS'",
        s
    )))
}

fn format_timestamp(ms: u64) -> String {
    match Local.timestamp_millis_opt(ms as i64) {
        chrono::LocalResult::Single(dt) => dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
        _ => ms.to_string(),
    }
}

#[derive(Debug, Clone, Parser)]
pub struct ResetOffsetByTimeSubCommand {
    #[command(flatten)]
    common_args: CommonArgs,

    #[arg(short = 'g', long = "group", required = true, help = "consumer group name")]
    group: String,

    #[arg(short = 't', long = "topic", required = true, help = "topic name")]
    topic: String,

    #[arg(
        short = 's',
        long = "timestamp",
        required = true,
        help = "target timestamp: 'now', milliseconds, or 'yyyy-MM-dd#HH:mm:ss:SSS'"
    )]
    timestamp: String,
}

impl ResetOffsetByTimeSubCommand {
    fn request(&self) -> RocketMQResult<ResetOffsetByTimeRequest> {
        let group = self.group.trim();
        if group.is_empty() {
            return Err(RocketMQError::IllegalArgument(
                "Consumer group name (--group / -g) cannot be empty".into(),
            ));
        }

        let topic = self.topic.trim();
        if topic.is_empty() {
            return Err(RocketMQError::IllegalArgument(
                "Topic name (--topic / -t) cannot be empty".into(),
            ));
        }

        Ok(
            ResetOffsetByTimeRequest::try_new(group, topic, parse_timestamp(&self.timestamp)?)?
                .with_optional_namesrv_addr(self.common_args.namesrv_addr.clone()),
        )
    }

    fn print_result(request: &ResetOffsetByTimeRequest, result: ResetOffsetByTimeResult) {
        println!("Reset Consumer Offset by Time");
        println!("==============================");
        println!();
        println!("Consumer Group: {}", request.group());
        println!("Topic: {}", request.topic());
        println!(
            "Target Timestamp: {} ({})",
            format_timestamp(request.timestamp()),
            request.timestamp()
        );
        println!();
        println!("Resetting offsets...");
        println!("{}", "-".repeat(50));
        println!();

        match result {
            ResetOffsetByTimeResult::Current(offset_map) => {
                let mut entries: Vec<_> = offset_map.iter().collect();
                entries.sort_by(|(a, _), (b, _)| {
                    a.broker_name()
                        .cmp(b.broker_name())
                        .then_with(|| a.queue_id().cmp(&b.queue_id()))
                });

                for (mq, new_offset) in &entries {
                    println!(
                        "Broker: {:<35} Queue: {:<6} New Offset: {}",
                        mq.broker_name(),
                        mq.queue_id(),
                        new_offset
                    );
                }

                println!();
                println!("{}", "-".repeat(50));
                println!("Reset Summary:");
                println!("  Total Queues: {}", entries.len());
                println!(
                    "  All offsets reset to timestamp: {}",
                    format_timestamp(request.timestamp())
                );
                println!();
                println!("Note: Consumers will automatically resume from new offsets (no restart required)");
            }
            ResetOffsetByTimeResult::Legacy {
                rollback_stats,
                current_error,
            } => {
                eprintln!("New reset method failed ({current_error}). Trying legacy method.");
                Self::print_legacy_result(request, rollback_stats);
            }
        }
    }

    fn print_legacy_result(request: &ResetOffsetByTimeRequest, rollback_stats: Vec<RollbackStats>) {
        println!();
        println!("Consumer group is offline - using legacy reset method");
        println!();
        println!("Reset consumer offset by specified:");
        println!("  consumerGroup[{}]", request.group());
        println!("  topic[{}]", request.topic());
        println!(
            "  timestamp(string)[{}]",
            Local
                .timestamp_millis_opt(request.timestamp() as i64)
                .single()
                .map(|dt| dt.format(TIMESTAMP_FORMAT).to_string())
                .unwrap_or_else(|| request.timestamp().to_string())
        );
        println!("  timestamp(long)[{}]", request.timestamp());
        println!();

        println!(
            "{:<24} {:<10} {:<14} {:<16} {:<16} {:<12}",
            "BrokerName", "QueueId", "BrokerOffset", "ConsumerOffset", "TimestampOffset", "ResetOffset"
        );
        println!("{}", "-".repeat(95));

        for stat in rollback_stats {
            println!(
                "{:<24} {:<10} {:<14} {:<16} {:<16} {:<12}",
                stat.broker_name,
                stat.queue_id,
                stat.broker_offset,
                stat.consumer_offset,
                stat.timestamp_offset,
                stat.rollback_offset
            );
        }

        println!();
        println!("Note: Consumer restart required for old method to take effect");
    }
}

impl CommandExecute for ResetOffsetByTimeSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let request = self.request()?;
        let result = OffsetService::reset_offset_by_time_by_request_with_rpc_hook(request.clone(), rpc_hook).await?;
        Self::print_result(&request, result);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[test]
    fn parses_reset_offset_by_time_request() {
        let cmd = ResetOffsetByTimeSubCommand::try_parse_from([
            "resetOffsetByTime",
            "-g",
            " TestGroup ",
            "-t",
            " TestTopic ",
            "-s",
            "1234",
            "-n",
            " 127.0.0.1:9876 ",
        ])
        .unwrap();
        let request = cmd.request().unwrap();

        assert_eq!(request.group().as_str(), "TestGroup");
        assert_eq!(request.topic().as_str(), "TestTopic");
        assert_eq!(request.timestamp(), 1234);
        assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
    }
}
