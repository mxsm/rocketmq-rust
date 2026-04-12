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

use clap::ArgGroup;
use clap::Parser;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use rocketmq_admin_core::core::broker::BrokerService;
use rocketmq_admin_core::core::broker::CleanExpiredConsumeQueueReport;
use rocketmq_admin_core::core::broker::CleanExpiredConsumeQueueRequest;

#[derive(Debug, Clone, Parser)]
#[command(group(ArgGroup::new("target")
    .required(false)
    .args(&["broker_addr", "cluster_name"]))
)]
pub struct CleanExpiredCQSubCommand {
    #[arg(short = 'b', long = "brokerAddr", required = false, help = "Broker address")]
    broker_addr: Option<String>,

    #[arg(short = 'c', long = "cluster", required = false, help = "Cluster name")]
    cluster_name: Option<String>,

    #[arg(
        short = 't',
        long = "topic",
        required = false,
        help = "Topic scope (select target brokers by topic route)"
    )]
    topic: Option<String>,

    #[arg(
        long = "dryRun",
        visible_alias = "dry-run",
        required = false,
        help = "Preview cleanup targets without executing cleanup"
    )]
    dry_run: bool,
}

impl CleanExpiredCQSubCommand {
    fn request(&self) -> RocketMQResult<CleanExpiredConsumeQueueRequest> {
        CleanExpiredConsumeQueueRequest::try_new(
            self.broker_addr.clone(),
            self.cluster_name.clone(),
            self.topic.clone(),
            self.dry_run,
        )
    }
}

impl CommandExecute for CleanExpiredCQSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let request = self.request()?;
        let report =
            BrokerService::clean_expired_consume_queue_by_request_with_rpc_hook(request.clone(), rpc_hook).await?;
        print_scan_summary(
            &report,
            request.cluster_name().map(|value| value.as_str()),
            request.topic().map(|value| value.as_str()),
        );
        if request.topic().is_some() {
            println!("Note: cleanup is broker-scoped; topic is only used to select target brokers.");
        }
        print_cleanup_results(&report);
        print_cleanup_report(&report);

        if !report.failures.is_empty() {
            return Err(RocketMQError::Internal(format!(
                "CleanExpiredCQSubCommand: cleanup failed on {} broker(s)",
                report.failures.len()
            )));
        }

        Ok(())
    }
}

fn print_scan_summary(report: &CleanExpiredConsumeQueueReport, cluster: Option<&str>, topic: Option<&str>) {
    println!("Scan result:");
    if report.dry_run {
        println!("- Mode: DRY RUN (no changes will be applied)");
    } else {
        println!("- Mode: LIVE");
    }
    if let Some(cluster) = cluster {
        println!("- Cluster: {}", cluster);
    }
    if let Some(topic) = topic {
        println!("- Topic scope: {}", topic);
    }
    if report.is_global_mode {
        println!("- Scope: global");
    }

    if report.targets.is_empty() {
        if report.is_global_mode {
            println!("- Target brokers: all brokers in the current nameserver scope");
        } else {
            println!("- Target brokers: 0");
        }
        return;
    }

    println!("- Target brokers ({}):", report.targets.len());
    for target in &report.targets {
        println!("  - {}", target);
    }
}

fn print_cleanup_results(report: &CleanExpiredConsumeQueueReport) {
    if report.dry_run {
        return;
    }

    if report.is_global_mode {
        for result in &report.target_results {
            if result.success {
                println!("cleanExpiredCQ success (global)");
            } else {
                println!("cleanExpiredCQ false (global)");
            }
        }
        for failure in &report.failures {
            eprintln!("cleanExpiredCQ failed, broker: global, {}", failure.error);
        }
        return;
    }

    for result in &report.target_results {
        if result.success {
            println!("cleanExpiredCQ success, broker: {}", result.broker_addr);
        } else {
            println!("cleanExpiredCQ false, broker: {}", result.broker_addr);
        }
    }
    for failure in &report.failures {
        eprintln!(
            "cleanExpiredCQ failed, broker: {}, {}",
            failure.broker_addr, failure.error
        );
    }
}

fn print_cleanup_report(report: &CleanExpiredConsumeQueueReport) {
    println!("Cleanup report:");
    println!("- Dry run: {}", report.dry_run);
    println!("- Scanned brokers: {}", report.scanned_brokers);
    println!("- Cleanup invocations: {}", report.cleanup_invocations);
    println!("- Cleanup success results: {}", report.cleanup_successes);
    println!("- Cleanup false results: {}", report.cleanup_false_results);
    println!("- Cleanup failures: {}", report.failures.len());
    println!("- Queues removed: unavailable (broker API returns bool only)");
    println!("- Space freed: unavailable (broker API returns bool only)");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_with_broker_addr() {
        let cmd = CleanExpiredCQSubCommand::try_parse_from(["cleanExpiredCQ", "-b", "127.0.0.1:10911"]).unwrap();
        let request = cmd.request().unwrap();

        assert_eq!(request.broker_addr().unwrap().as_str(), "127.0.0.1:10911");
        assert!(request.cluster_name().is_none());
        assert!(request.topic().is_none());
        assert!(!request.dry_run());
    }

    #[test]
    fn parse_with_cluster_topic_and_dry_run() {
        let cmd = CleanExpiredCQSubCommand::try_parse_from([
            "cleanExpiredCQ",
            "-c",
            "DefaultCluster",
            "-t",
            "TestTopic",
            "--dryRun",
        ])
        .unwrap();
        let request = cmd.request().unwrap();

        assert_eq!(request.cluster_name().unwrap().as_str(), "DefaultCluster");
        assert_eq!(request.topic().unwrap().as_str(), "TestTopic");
        assert!(request.dry_run());
    }

    #[test]
    fn parse_with_no_target() {
        let cmd = CleanExpiredCQSubCommand::try_parse_from(["cleanExpiredCQ"]).unwrap();
        assert!(cmd.request().unwrap().is_global_mode());
    }

    #[test]
    fn parse_conflicting_broker_and_cluster() {
        let result = CleanExpiredCQSubCommand::try_parse_from([
            "cleanExpiredCQ",
            "-b",
            "127.0.0.1:10911",
            "-c",
            "DefaultCluster",
        ]);
        assert!(result.is_err());
    }
}
