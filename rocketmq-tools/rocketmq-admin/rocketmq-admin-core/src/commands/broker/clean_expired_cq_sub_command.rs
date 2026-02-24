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
use clap::ArgGroup;
use clap::Parser;
use futures::future::join_all;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::commands::command_util::CommandUtil;
use crate::commands::CommandExecute;

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

#[derive(Default)]
struct CleanExpiredCQReport {
    scanned_brokers: usize,
    cleanup_invocations: usize,
    cleanup_successes: usize,
    cleanup_false_results: usize,
    cleanup_failures: usize,
}

impl CommandExecute for CleanExpiredCQSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let mut default_mqadmin_ext = if let Some(rpc_hook) = rpc_hook {
            DefaultMQAdminExt::with_rpc_hook(rpc_hook)
        } else {
            DefaultMQAdminExt::new()
        };
        default_mqadmin_ext
            .client_config_mut()
            .set_instance_name(get_current_millis().to_string().into());

        MQAdminExt::start(&mut default_mqadmin_ext).await.map_err(|e| {
            RocketMQError::Internal(format!("CleanExpiredCQSubCommand: Failed to start MQAdminExt: {}", e))
        })?;

        let operation_result = clean_expired_cq(&default_mqadmin_ext, self).await;

        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;
        operation_result
    }
}

async fn clean_expired_cq(
    default_mqadmin_ext: &DefaultMQAdminExt,
    command: &CleanExpiredCQSubCommand,
) -> RocketMQResult<()> {
    let broker_addr = normalize_optional_arg(command.broker_addr.as_deref(), "brokerAddr")?;
    let cluster_name = normalize_optional_arg(command.cluster_name.as_deref(), "cluster")?;
    let topic = normalize_optional_arg(command.topic.as_deref(), "topic")?;

    let is_global_mode = broker_addr.is_none() && cluster_name.is_none() && topic.is_none();

    let mut targets = resolve_targets(
        default_mqadmin_ext,
        broker_addr.as_deref(),
        cluster_name.as_deref(),
        topic.as_deref(),
    )
    .await?;

    if is_global_mode {
        targets = fetch_broker_targets(default_mqadmin_ext, None).await?;
    }

    print_scan_summary(
        &targets,
        cluster_name.as_deref(),
        topic.as_deref(),
        command.dry_run,
        is_global_mode,
    );

    if topic.is_some() {
        println!("Note: cleanup is broker-scoped; topic is only used to select target brokers.");
    }

    let mut report = CleanExpiredCQReport {
        scanned_brokers: targets.len(),
        ..Default::default()
    };

    if command.dry_run {
        print_cleanup_report(&report, true);
        return Ok(());
    }

    if is_global_mode {
        report.cleanup_invocations = 1;
        let result = default_mqadmin_ext
            .clean_expired_consumer_queue(None, None)
            .await
            .map_err(|e| {
                RocketMQError::Internal(format!(
                    "CleanExpiredCQSubCommand: Failed to clean expired consume queues globally: {}",
                    e
                ))
            })?;

        if result {
            report.cleanup_successes = 1;
            println!("cleanExpiredCQ success (global)");
        } else {
            report.cleanup_false_results = 1;
            println!("cleanExpiredCQ false (global)");
        }
    } else {
        if targets.is_empty() {
            return Err(RocketMQError::Internal(
                "CleanExpiredCQSubCommand: No broker targets matched the specified scope".to_string(),
            ));
        }

        let ft: Vec<_> = targets
            .into_iter()
            .map(|target| async move {
                let result = default_mqadmin_ext
                    .clean_expired_consumer_queue(None, Some(CheetahString::from(target.as_str())))
                    .await;
                (target, result)
            })
            .collect();

        for (target, result) in join_all(ft).await {
            report.cleanup_invocations += 1;
            match result {
                Ok(true) => {
                    report.cleanup_successes += 1;
                    println!("cleanExpiredCQ success, broker: {}", target);
                }
                Ok(false) => {
                    report.cleanup_false_results += 1;
                    println!("cleanExpiredCQ false, broker: {}", target);
                }
                Err(e) => {
                    report.cleanup_failures += 1;
                    eprintln!("cleanExpiredCQ failed, broker: {}, {}", target, e);
                }
            }
        }
    }

    print_cleanup_report(&report, false);

    if report.cleanup_failures > 0 {
        return Err(RocketMQError::Internal(format!(
            "CleanExpiredCQSubCommand: cleanup failed on {} broker(s)",
            report.cleanup_failures
        )));
    }

    Ok(())
}

async fn resolve_targets(
    default_mqadmin_ext: &DefaultMQAdminExt,
    broker_addr: Option<&str>,
    cluster_name: Option<&str>,
    topic: Option<&str>,
) -> RocketMQResult<Vec<String>> {
    if let Some(broker_addr) = broker_addr {
        if let Some(topic) = topic {
            let topic_targets = fetch_topic_broker_targets(default_mqadmin_ext, topic).await?;
            if !topic_targets.iter().any(|target| target == broker_addr) {
                return Err(RocketMQError::Internal(format!(
                    "CleanExpiredCQSubCommand: Broker {} is not found in route info of topic {}",
                    broker_addr, topic
                )));
            }
        }
        return Ok(vec![broker_addr.to_string()]);
    }

    let cluster_targets = if let Some(cluster_name) = cluster_name {
        Some(fetch_broker_targets(default_mqadmin_ext, Some(cluster_name)).await?)
    } else {
        None
    };

    let topic_targets = if let Some(topic) = topic {
        Some(fetch_topic_broker_targets(default_mqadmin_ext, topic).await?)
    } else {
        None
    };

    let targets = match (cluster_targets, topic_targets) {
        (Some(mut cluster_targets), Some(topic_targets)) => {
            cluster_targets.retain(|addr| topic_targets.iter().any(|topic_addr| topic_addr == addr));
            sort_and_dedup(&mut cluster_targets);
            cluster_targets
        }
        (Some(mut cluster_targets), None) => {
            sort_and_dedup(&mut cluster_targets);
            cluster_targets
        }
        (None, Some(mut topic_targets)) => {
            sort_and_dedup(&mut topic_targets);
            topic_targets
        }
        (None, None) => Vec::new(),
    };

    Ok(targets)
}

async fn fetch_broker_targets(
    default_mqadmin_ext: &DefaultMQAdminExt,
    cluster_name: Option<&str>,
) -> RocketMQResult<Vec<String>> {
    let cluster_info = default_mqadmin_ext.examine_broker_cluster_info().await.map_err(|e| {
        RocketMQError::Internal(format!(
            "CleanExpiredCQSubCommand: Failed to examine broker cluster info: {}",
            e
        ))
    })?;

    let mut targets = if let Some(cluster_name) = cluster_name {
        CommandUtil::fetch_master_and_slave_addr_by_cluster_name(&cluster_info, cluster_name)?
            .into_iter()
            .map(|addr| addr.to_string())
            .collect::<Vec<_>>()
    } else {
        let mut all_targets = Vec::new();
        if let Some(broker_addr_table) = cluster_info.broker_addr_table {
            for broker_data in broker_addr_table.values() {
                for broker_addr in broker_data.broker_addrs().values() {
                    all_targets.push(broker_addr.to_string());
                }
            }
        }
        all_targets
    };

    sort_and_dedup(&mut targets);
    Ok(targets)
}

async fn fetch_topic_broker_targets(
    default_mqadmin_ext: &DefaultMQAdminExt,
    topic: &str,
) -> RocketMQResult<Vec<String>> {
    let route_data = default_mqadmin_ext
        .examine_topic_route_info(CheetahString::from(topic))
        .await
        .map_err(|e| {
            RocketMQError::Internal(format!(
                "CleanExpiredCQSubCommand: Failed to examine topic route info for {}: {}",
                topic, e
            ))
        })?
        .ok_or_else(|| {
            RocketMQError::Internal(format!(
                "CleanExpiredCQSubCommand: Topic {} route info is unavailable",
                topic
            ))
        })?;

    let mut targets = Vec::new();
    for broker_data in route_data.broker_datas {
        for broker_addr in broker_data.broker_addrs().values() {
            targets.push(broker_addr.to_string());
        }
    }

    sort_and_dedup(&mut targets);
    if targets.is_empty() {
        return Err(RocketMQError::Internal(format!(
            "CleanExpiredCQSubCommand: Topic {} has no broker targets",
            topic
        )));
    }

    Ok(targets)
}

fn normalize_optional_arg(value: Option<&str>, field_name: &str) -> RocketMQResult<Option<String>> {
    match value.map(str::trim) {
        Some("") => Err(RocketMQError::Internal(format!(
            "CleanExpiredCQSubCommand: {} cannot be empty",
            field_name
        ))),
        Some(value) => Ok(Some(value.to_string())),
        None => Ok(None),
    }
}

fn sort_and_dedup(values: &mut Vec<String>) {
    values.sort();
    values.dedup();
}

fn print_scan_summary(
    targets: &[String],
    cluster: Option<&str>,
    topic: Option<&str>,
    dry_run: bool,
    is_global_mode: bool,
) {
    println!("Scan result:");
    if dry_run {
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
    if is_global_mode {
        println!("- Scope: global");
    }

    if targets.is_empty() {
        if is_global_mode {
            println!("- Target brokers: all brokers in the current nameserver scope");
        } else {
            println!("- Target brokers: 0");
        }
        return;
    }

    println!("- Target brokers ({}):", targets.len());
    for target in targets {
        println!("  - {}", target);
    }
}

fn print_cleanup_report(report: &CleanExpiredCQReport, dry_run: bool) {
    println!("Cleanup report:");
    println!("- Dry run: {}", dry_run);
    println!("- Scanned brokers: {}", report.scanned_brokers);
    println!("- Cleanup invocations: {}", report.cleanup_invocations);
    println!("- Cleanup success results: {}", report.cleanup_successes);
    println!("- Cleanup false results: {}", report.cleanup_false_results);
    println!("- Cleanup failures: {}", report.cleanup_failures);
    println!("- Queues removed: unavailable (broker API returns bool only)");
    println!("- Space freed: unavailable (broker API returns bool only)");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_with_broker_addr() {
        let args = vec!["cleanExpiredCQ", "-b", "127.0.0.1:10911"];

        let cmd = CleanExpiredCQSubCommand::try_parse_from(args).unwrap();
        assert_eq!(cmd.broker_addr, Some("127.0.0.1:10911".to_string()));
        assert_eq!(cmd.cluster_name, None);
        assert_eq!(cmd.topic, None);
        assert!(!cmd.dry_run);
    }

    #[test]
    fn parse_with_cluster_topic_and_dry_run() {
        let args = vec!["cleanExpiredCQ", "-c", "DefaultCluster", "-t", "TestTopic", "--dryRun"];

        let cmd = CleanExpiredCQSubCommand::try_parse_from(args).unwrap();
        assert_eq!(cmd.broker_addr, None);
        assert_eq!(cmd.cluster_name, Some("DefaultCluster".to_string()));
        assert_eq!(cmd.topic, Some("TestTopic".to_string()));
        assert!(cmd.dry_run);
    }

    #[test]
    fn parse_with_no_target() {
        let args = vec!["cleanExpiredCQ"];

        let cmd = CleanExpiredCQSubCommand::try_parse_from(args).unwrap();
        assert_eq!(cmd.broker_addr, None);
        assert_eq!(cmd.cluster_name, None);
        assert_eq!(cmd.topic, None);
        assert!(!cmd.dry_run);
    }

    #[test]
    fn parse_conflicting_broker_and_cluster() {
        let args = vec!["cleanExpiredCQ", "-b", "127.0.0.1:10911", "-c", "DefaultCluster"];

        let cmd = CleanExpiredCQSubCommand::try_parse_from(args);
        assert!(cmd.is_err());
    }

    #[test]
    fn normalize_optional_arg_rejects_empty_value() {
        let result = normalize_optional_arg(Some("   "), "topic");
        assert!(result.is_err());
    }
}
