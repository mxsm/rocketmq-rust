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
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use crate::commands::CommonArgs;
use rocketmq_admin_core::admin::default_mq_admin_ext::DefaultMQAdminExt;

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
    // Try parsing as plain milliseconds integer first.
    if let Ok(ms) = s.parse::<u64>() {
        return Ok(ms);
    }
    // Try parsing as formatted datetime string.
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

/// Format a millisecond timestamp for human-readable output.
fn format_timestamp(ms: u64) -> String {
    match Local.timestamp_millis_opt(ms as i64) {
        chrono::LocalResult::Single(dt) => dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
        _ => ms.to_string(),
    }
}

/// `resetOffsetByTime` - reset consumer group offsets to a specific timestamp without
/// requiring a client restart.
///
/// Resets all queues for the topic.  Active consumers are notified by the broker
/// and apply the new offsets immediately - no restart required.
///
/// If the new method is unavailable (consumer group offline), the command falls back
/// to the legacy `resetOffsetByTimestampOld` API, which **does** require a restart.
#[derive(Debug, Clone, Parser)]
pub struct ResetOffsetByTimeSubCommand {
    #[command(flatten)]
    common_args: CommonArgs,

    /// Consumer group name.
    #[arg(short = 'g', long = "group", required = true, help = "consumer group name")]
    group: String,

    /// Topic name.
    #[arg(short = 't', long = "topic", required = true, help = "topic name")]
    topic: String,

    /// Target timestamp.  Accepts: `now`, milliseconds since epoch, or
    /// `yyyy-MM-dd#HH:mm:ss:SSS` (e.g. `2024-02-19#10:00:00:000`).
    #[arg(
        short = 's',
        long = "timestamp",
        required = true,
        help = "target timestamp: 'now', milliseconds, or 'yyyy-MM-dd#HH:mm:ss:SSS'"
    )]
    timestamp: String,
}

impl ResetOffsetByTimeSubCommand {
    /// Perform a topic-level reset (all queues) using the new method that does
    /// not require a consumer restart.  Falls back to the old method on error.
    async fn reset_topic_level(
        admin: &mut DefaultMQAdminExt,
        group: &str,
        topic: &str,
        timestamp: u64,
    ) -> RocketMQResult<()> {
        println!("Reset Consumer Offset by Time");
        println!("==============================");
        println!();
        println!("Consumer Group: {group}");
        println!("Topic: {topic}");
        println!("Target Timestamp: {} ({timestamp})", format_timestamp(timestamp));
        println!();
        println!("Resetting offsets...");
        println!("{}", "-".repeat(50));
        println!();

        let result = admin
            .reset_offset_by_timestamp(None, topic.into(), group.into(), timestamp, false)
            .await;

        match result {
            Ok(offset_map) => {
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
                println!("  All offsets reset to timestamp: {}", format_timestamp(timestamp));
                println!();
                println!("Note: Consumers will automatically resume from new offsets (no restart required)");
                Ok(())
            }
            Err(e) => {
                // Attempt the legacy fallback path.
                eprintln!("New reset method failed ({e}). Trying legacy method.");
                Self::reset_topic_level_old(admin, group, topic, timestamp).await
            }
        }
    }

    /// Fallback: old topic-level reset via `resetOffsetByTimestampOld`.
    /// This method **requires** a consumer restart.
    async fn reset_topic_level_old(
        admin: &mut DefaultMQAdminExt,
        group: &str,
        topic: &str,
        timestamp: u64,
    ) -> RocketMQResult<()> {
        println!();
        println!("Consumer group is offline - using legacy reset method");
        println!();
        println!("Reset consumer offset by specified:");
        println!("  consumerGroup[{group}]");
        println!("  topic[{topic}]");
        println!(
            "  timestamp(string)[{}]",
            Local
                .timestamp_millis_opt(timestamp as i64)
                .single()
                .map(|dt| dt.format(TIMESTAMP_FORMAT).to_string())
                .unwrap_or_else(|| timestamp.to_string())
        );
        println!("  timestamp(long)[{timestamp}]");
        println!();

        let rollback_stats = admin
            .reset_offset_by_timestamp_old(None, group.into(), topic.into(), timestamp, false)
            .await?;

        println!(
            "{:<24} {:<10} {:<14} {:<16} {:<16} {:<12}",
            "BrokerName", "QueueId", "BrokerOffset", "ConsumerOffset", "TimestampOffset", "ResetOffset"
        );
        println!("{}", "-".repeat(95));

        for stat in &rollback_stats {
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
        Ok(())
    }
}

impl CommandExecute for ResetOffsetByTimeSubCommand {
    async fn execute(&self, _rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        // ── Validate arguments ───────────────────────────────────────────────
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

        let timestamp = parse_timestamp(&self.timestamp)?;

        // ── Initialise admin client ───────────────────────────────────────────
        let mut admin = DefaultMQAdminExt::new();
        admin
            .client_config_mut()
            .set_instance_name(current_millis().to_string().into());

        if let Some(addr) = &self.common_args.namesrv_addr {
            admin.set_namesrv_addr(addr.trim());
        }

        admin.start().await.map_err(|e| {
            RocketMQError::Internal(format!("ResetOffsetByTimeSubCommand: Failed to start MQAdminExt: {e}"))
        })?;

        let result = Self::reset_topic_level(&mut admin, group, topic, timestamp).await;

        admin.shutdown().await;
        result
    }
}
