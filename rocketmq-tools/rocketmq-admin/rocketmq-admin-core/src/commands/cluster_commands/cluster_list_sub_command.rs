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

use std::collections::BTreeSet;
use std::sync::Arc;

use cheetah_string::CheetahString;
use clap::Parser;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::body::broker_body::cluster_info::ClusterInfo;
use rocketmq_remoting::protocol::body::kv_table::KVTable;
use rocketmq_remoting::runtime::RPCHook;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::commands::CommandExecute;

#[derive(Debug, Clone, Parser)]
pub struct ClusterListSubCommand {
    #[arg(
        short = 'm',
        long = "moreStats",
        required = false,
        default_value = "false",
        help = "Print more stats"
    )]
    more_stats: bool,

    #[arg(
        short = 'i',
        long = "interval",
        required = false,
        help = "specify intervals numbers, it is in seconds"
    )]
    interval: Option<u64>,

    #[arg(short = 'c', long = "clusterName", required = false, help = "which cluster")]
    cluster_name: Option<String>,
}

impl CommandExecute for ClusterListSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let mut default_mq_admin_ext = if let Some(ref hook) = rpc_hook {
            DefaultMQAdminExt::with_rpc_hook(hook.clone())
        } else {
            DefaultMQAdminExt::new()
        };

        default_mq_admin_ext
            .client_config_mut()
            .set_instance_name(get_current_millis().to_string().into());

        let print_interval = self.interval.map(|i| i * 1000);
        let cluster_name = self
            .cluster_name
            .as_ref()
            .map(|s| s.trim().to_string())
            .unwrap_or_default();

        let operation_result = async {
            MQAdminExt::start(&mut default_mq_admin_ext).await.map_err(|e| {
                RocketMQError::Internal(format!("ClusterListSubCommand: Failed to start MQAdminExt: {}", e))
            })?;

            let mut iteration = 0u64;
            loop {
                if iteration > 0 {
                    if let Some(interval_ms) = print_interval {
                        tokio::time::sleep(tokio::time::Duration::from_millis(interval_ms)).await;
                    }
                }
                iteration += 1;

                let cluster_info = default_mq_admin_ext.examine_broker_cluster_info().await.map_err(|e| {
                    RocketMQError::Internal(format!(
                        "ClusterListSubCommand: Failed to examine broker cluster info: {}",
                        e
                    ))
                })?;

                let cluster_names = get_target_cluster_names(&cluster_name, &cluster_info);

                if self.more_stats {
                    print_cluster_more_stats(&cluster_names, &default_mq_admin_ext, &cluster_info).await;
                } else {
                    print_cluster_base_info(&cluster_names, &default_mq_admin_ext, &cluster_info).await;
                }

                if print_interval.is_none() {
                    break;
                }
            }

            Ok::<(), RocketMQError>(())
        }
        .await;

        MQAdminExt::shutdown(&mut default_mq_admin_ext).await;
        operation_result
    }
}

fn get_target_cluster_names(cluster_name: &str, cluster_info: &ClusterInfo) -> BTreeSet<String> {
    if cluster_name.is_empty() {
        if let Some(ref table) = cluster_info.cluster_addr_table {
            table.keys().map(|k| k.to_string()).collect()
        } else {
            BTreeSet::new()
        }
    } else {
        let mut set = BTreeSet::new();
        set.insert(cluster_name.to_string());
        set
    }
}

async fn print_cluster_more_stats(
    cluster_names: &BTreeSet<String>,
    admin_ext: &DefaultMQAdminExt,
    cluster_info: &ClusterInfo,
) {
    println!(
        "{:<16}  {:<32} {:>14} {:>14} {:>14} {:>14}",
        "#Cluster Name", "#Broker Name", "#InTotalYest", "#OutTotalYest", "#InTotalToday", "#OutTotalToday"
    );

    let cluster_addr_table = match cluster_info.cluster_addr_table.as_ref() {
        Some(table) => table,
        None => return,
    };

    let broker_addr_table = match cluster_info.broker_addr_table.as_ref() {
        Some(table) => table,
        None => return,
    };

    for cluster_name in cluster_names {
        let broker_names: BTreeSet<String> = if let Some(names) = cluster_addr_table.get(cluster_name.as_str()) {
            names.iter().map(|n| n.to_string()).collect()
        } else {
            continue;
        };

        for broker_name in &broker_names {
            let broker_data = match broker_addr_table.get(broker_name.as_str()) {
                Some(data) => data,
                None => continue,
            };

            for (bid, addr) in broker_data.broker_addrs() {
                let mut in_total_yest: i64 = 0;
                let mut out_total_yest: i64 = 0;
                let mut in_total_today: i64 = 0;
                let mut out_total_today: i64 = 0;

                if let Ok(kv_table) = admin_ext
                    .fetch_broker_runtime_stats(CheetahString::from(addr.as_str()))
                    .await
                {
                    if let (Some(put_yest_morning), Some(put_today_morning), Some(put_today_now)) = (
                        get_kv_value(&kv_table, "msgPutTotalYesterdayMorning"),
                        get_kv_value(&kv_table, "msgPutTotalTodayMorning"),
                        get_kv_value(&kv_table, "msgPutTotalTodayNow"),
                    ) {
                        in_total_yest = put_today_morning - put_yest_morning;
                        in_total_today = put_today_now - put_today_morning;
                    }

                    if let (Some(get_yest_morning), Some(get_today_morning), Some(get_today_now)) = (
                        get_kv_value(&kv_table, "msgGetTotalYesterdayMorning"),
                        get_kv_value(&kv_table, "msgGetTotalTodayMorning"),
                        get_kv_value(&kv_table, "msgGetTotalTodayNow"),
                    ) {
                        out_total_yest = get_today_morning - get_yest_morning;
                        out_total_today = get_today_now - get_today_morning;
                    }
                }

                println!(
                    "{:<16}  {:<32} {:>14} {:>14} {:>14} {:>14}",
                    cluster_name, broker_name, in_total_yest, out_total_yest, in_total_today, out_total_today
                );
                let _ = bid; // suppress unused warning
            }
        }
    }
}

async fn print_cluster_base_info(
    cluster_names: &BTreeSet<String>,
    admin_ext: &DefaultMQAdminExt,
    cluster_info: &ClusterInfo,
) {
    println!(
        "{:<22}  {:<22}  {:<4}  {:<22} {:<16}  {:>16}  {:>30}  {:<22}  {:>11}  {:<12}  {:<8}  {:>10}",
        "#Cluster Name",
        "#Broker Name",
        "#BID",
        "#Addr",
        "#Version",
        "#InTPS(LOAD)",
        "#OutTPS(LOAD)",
        "#Timer(Progress)",
        "#PCWait(ms)",
        "#Hour",
        "#SPACE",
        "#ACTIVATED"
    );

    let cluster_addr_table = match cluster_info.cluster_addr_table.as_ref() {
        Some(table) => table,
        None => return,
    };

    let broker_addr_table = match cluster_info.broker_addr_table.as_ref() {
        Some(table) => table,
        None => return,
    };

    for cluster_name in cluster_names {
        let broker_names: BTreeSet<String> = if let Some(names) = cluster_addr_table.get(cluster_name.as_str()) {
            names.iter().map(|n| n.to_string()).collect()
        } else {
            continue;
        };

        for broker_name in &broker_names {
            let broker_data = match broker_addr_table.get(broker_name.as_str()) {
                Some(data) => data,
                None => continue,
            };

            for (bid, addr) in broker_data.broker_addrs() {
                let mut in_tps: f64 = 0.0;
                let mut out_tps: f64 = 0.0;
                let mut version = String::new();
                let mut send_thread_pool_queue_size = String::new();
                let mut pull_thread_pool_queue_size = String::new();
                let mut ack_thread_pool_queue_size = String::from("N");
                let mut send_thread_pool_queue_head_wait_time_mills = String::new();
                let mut pull_thread_pool_queue_head_wait_time_mills = String::new();
                let mut ack_thread_pool_queue_head_wait_time_mills = String::from("N");
                let mut page_cache_lock_time_mills = String::new();
                let mut earliest_message_time_stamp = String::new();
                let mut commit_log_disk_ratio = String::new();
                let mut timer_read_behind: i64 = 0;
                let mut timer_offset_behind: i64 = 0;
                let mut timer_congest_num: i64 = 0;
                let mut timer_enqueue_tps: f32 = 0.0;
                let mut timer_dequeue_tps: f32 = 0.0;
                let mut is_broker_active = false;

                if let Ok(kv_table) = admin_ext
                    .fetch_broker_runtime_stats(CheetahString::from(addr.as_str()))
                    .await
                {
                    is_broker_active = get_kv_str(&kv_table, "brokerActive")
                        .map(|s| s == "true")
                        .unwrap_or(false);

                    if let Some(put_tps) = get_kv_str(&kv_table, "putTps") {
                        if let Some(first) = put_tps.split_whitespace().next() {
                            in_tps = first.parse::<f64>().unwrap_or(0.0);
                        }
                    }

                    if let Some(get_transferred_tps) = get_kv_str(&kv_table, "getTransferredTps") {
                        if let Some(first) = get_transferred_tps.split_whitespace().next() {
                            out_tps = first.parse::<f64>().unwrap_or(0.0);
                        }
                    }

                    send_thread_pool_queue_size = get_kv_str(&kv_table, "sendThreadPoolQueueSize")
                        .unwrap_or_default()
                        .to_string();
                    pull_thread_pool_queue_size = get_kv_str(&kv_table, "pullThreadPoolQueueSize")
                        .unwrap_or_default()
                        .to_string();
                    ack_thread_pool_queue_size = get_kv_str(&kv_table, "ackThreadPoolQueueSize")
                        .unwrap_or("N")
                        .to_string();

                    send_thread_pool_queue_head_wait_time_mills =
                        get_kv_str(&kv_table, "sendThreadPoolQueueHeadWaitTimeMills")
                            .unwrap_or_default()
                            .to_string();
                    pull_thread_pool_queue_head_wait_time_mills =
                        get_kv_str(&kv_table, "pullThreadPoolQueueHeadWaitTimeMills")
                            .unwrap_or_default()
                            .to_string();
                    ack_thread_pool_queue_head_wait_time_mills =
                        get_kv_str(&kv_table, "ackThreadPoolQueueHeadWaitTimeMills")
                            .unwrap_or("N")
                            .to_string();

                    page_cache_lock_time_mills = get_kv_str(&kv_table, "pageCacheLockTimeMills")
                        .unwrap_or_default()
                        .to_string();
                    earliest_message_time_stamp = get_kv_str(&kv_table, "earliestMessageTimeStamp")
                        .unwrap_or_default()
                        .to_string();
                    commit_log_disk_ratio = get_kv_str(&kv_table, "commitLogDiskRatio")
                        .unwrap_or_default()
                        .to_string();

                    if let Some(v) = get_kv_str(&kv_table, "timerReadBehind") {
                        timer_read_behind = v.parse::<i64>().unwrap_or(0);
                    }
                    if let Some(v) = get_kv_str(&kv_table, "timerOffsetBehind") {
                        timer_offset_behind = v.parse::<i64>().unwrap_or(0);
                    }
                    if let Some(v) = get_kv_str(&kv_table, "timerCongestNum") {
                        timer_congest_num = v.parse::<i64>().unwrap_or(0);
                    }
                    if let Some(v) = get_kv_str(&kv_table, "timerEnqueueTps") {
                        timer_enqueue_tps = v.parse::<f32>().unwrap_or(0.0);
                    }
                    if let Some(v) = get_kv_str(&kv_table, "timerDequeueTps") {
                        timer_dequeue_tps = v.parse::<f32>().unwrap_or(0.0);
                    }

                    version = get_kv_str(&kv_table, "brokerVersionDesc")
                        .unwrap_or_default()
                        .to_string();
                }

                let mut hour: f64 = 0.0;
                let mut space: f64 = 0.0;

                if !earliest_message_time_stamp.is_empty() {
                    if let Ok(ts) = earliest_message_time_stamp.parse::<u64>() {
                        let mills = get_current_millis() - ts;
                        hour = mills as f64 / 1000.0 / 60.0 / 60.0;
                    }
                }

                if !commit_log_disk_ratio.is_empty() {
                    space = commit_log_disk_ratio.parse::<f64>().unwrap_or(0.0);
                }

                let in_tps_str = format!(
                    "{:>9.2}({},{}ms)",
                    in_tps, send_thread_pool_queue_size, send_thread_pool_queue_head_wait_time_mills
                );
                let out_tps_str = format!(
                    "{:>9.2}({},{}ms|{},{}ms)",
                    out_tps,
                    pull_thread_pool_queue_size,
                    pull_thread_pool_queue_head_wait_time_mills,
                    ack_thread_pool_queue_size,
                    ack_thread_pool_queue_head_wait_time_mills
                );
                let timer_str = format!(
                    "{}-{}({:.1}w, {:.1}, {:.1})",
                    timer_read_behind,
                    timer_offset_behind,
                    timer_congest_num as f32 / 10000.0,
                    timer_enqueue_tps,
                    timer_dequeue_tps
                );

                println!(
                    "{:<22}  {:<22}  {:<4}  {:<22} {:<16}  {:>16}  {:>30}  {:<22}  {:>11}  {:<12}  {:<8}  {:>10}",
                    cluster_name,
                    broker_name,
                    bid,
                    addr,
                    version,
                    in_tps_str,
                    out_tps_str,
                    timer_str,
                    page_cache_lock_time_mills,
                    format!("{:.2}", hour),
                    format!("{:.4}", space),
                    is_broker_active
                );
            }
        }
    }
}

fn get_kv_str<'a>(kv_table: &'a KVTable, key: &str) -> Option<&'a str> {
    kv_table.table.get(&CheetahString::from(key)).map(|v| v.as_str())
}

fn get_kv_value(kv_table: &KVTable, key: &str) -> Option<i64> {
    kv_table
        .table
        .get(&CheetahString::from(key))
        .and_then(|v| v.parse::<i64>().ok())
}
