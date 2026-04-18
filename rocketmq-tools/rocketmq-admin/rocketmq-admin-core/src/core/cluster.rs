// Copyright 2026 The RocketMQ Rust Authors
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

//! Cluster admin service models and operations.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_client_rust::base::client_config::ClientConfig;
use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_client_rust::producer::mq_producer::MQProducer;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_remoting::protocol::body::broker_body::cluster_info::ClusterInfo;
use rocketmq_remoting::protocol::body::kv_table::KVTable;
use rocketmq_remoting::runtime::RPCHook;
use serde::Deserialize;
use serde::Serialize;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::core::admin::AdminBuilder;
use crate::core::RocketMQError;
use crate::core::RocketMQResult;
use crate::core::ToolsError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClusterListMode {
    Base,
    MoreStats,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterListQueryRequest {
    mode: ClusterListMode,
    cluster_name: Option<CheetahString>,
    namesrv_addr: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterBrokerNameQueryRequest {
    cluster_name: Option<CheetahString>,
    namesrv_addr: Option<String>,
}

impl ClusterBrokerNameQueryRequest {
    pub fn new(cluster_name: Option<String>) -> Self {
        Self {
            cluster_name: trim_optional_string(cluster_name).map(CheetahString::from),
            namesrv_addr: None,
        }
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn cluster_name(&self) -> Option<&CheetahString> {
        self.cluster_name.as_ref()
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        builder_with_namesrv(self.namesrv_addr())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterSendMessageRtRequest {
    amount: u64,
    size: u64,
    cluster_name: Option<CheetahString>,
    namesrv_addr: Option<String>,
}

impl ClusterSendMessageRtRequest {
    pub fn try_new(amount: u64, size: u64, cluster_name: Option<String>) -> RocketMQResult<Self> {
        if amount == 0 {
            return Err(ToolsError::validation_error("amount", "amount must be greater than 0").into());
        }
        Ok(Self {
            amount,
            size,
            cluster_name: trim_optional_string(cluster_name).map(CheetahString::from),
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn amount(&self) -> u64 {
        self.amount
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn cluster_name(&self) -> Option<&CheetahString> {
        self.cluster_name.as_ref()
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    fn broker_name_query_request(&self) -> ClusterBrokerNameQueryRequest {
        ClusterBrokerNameQueryRequest {
            cluster_name: self.cluster_name.clone(),
            namesrv_addr: self.namesrv_addr.clone(),
        }
    }
}

impl ClusterListQueryRequest {
    pub fn new(more_stats: bool, cluster_name: Option<String>) -> Self {
        Self {
            mode: if more_stats {
                ClusterListMode::MoreStats
            } else {
                ClusterListMode::Base
            },
            cluster_name: trim_optional_string(cluster_name).map(CheetahString::from),
            namesrv_addr: None,
        }
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn mode(&self) -> ClusterListMode {
        self.mode
    }

    pub fn cluster_name(&self) -> Option<&CheetahString> {
        self.cluster_name.as_ref()
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        builder_with_namesrv(self.namesrv_addr())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClusterBaseInfoRow {
    pub cluster_name: String,
    pub broker_name: String,
    pub broker_id: u64,
    pub broker_addr: CheetahString,
    pub version: String,
    pub in_tps: String,
    pub out_tps: String,
    pub timer_progress: String,
    pub page_cache_lock_time_millis: String,
    pub hour: String,
    pub space: String,
    pub broker_active: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterMoreStatsRow {
    pub cluster_name: String,
    pub broker_name: String,
    pub in_total_yesterday: i64,
    pub out_total_yesterday: i64,
    pub in_total_today: i64,
    pub out_total_today: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClusterListQueryResult {
    pub mode: ClusterListMode,
    pub base_rows: Vec<ClusterBaseInfoRow>,
    pub more_stats_rows: Vec<ClusterMoreStatsRow>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterBrokerNameQueryResult {
    pub broker_names_by_cluster: BTreeMap<String, Vec<String>>,
    pub missing_clusters: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClusterSendMessageRtRow {
    pub cluster_name: String,
    pub broker_name: String,
    pub rt: f64,
    pub success_count: u64,
    pub fail_count: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClusterSendMessageRtResult {
    pub missing_clusters: Vec<String>,
    pub rows: Vec<ClusterSendMessageRtRow>,
}

pub struct ClusterService;

impl ClusterService {
    pub async fn query_cluster_list_by_request_with_rpc_hook(
        request: ClusterListQueryRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<ClusterListQueryResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::query_cluster_list_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn query_cluster_list_with_admin(
        admin: &DefaultMQAdminExt,
        request: &ClusterListQueryRequest,
    ) -> RocketMQResult<ClusterListQueryResult> {
        let cluster_info = admin.examine_broker_cluster_info().await?;
        let cluster_names = target_cluster_names(request.cluster_name(), &cluster_info);

        let mut result = ClusterListQueryResult {
            mode: request.mode(),
            base_rows: Vec::new(),
            more_stats_rows: Vec::new(),
        };

        match request.mode() {
            ClusterListMode::Base => {
                result.base_rows = collect_cluster_base_rows(&cluster_names, admin, &cluster_info).await;
            }
            ClusterListMode::MoreStats => {
                result.more_stats_rows = collect_cluster_more_stats_rows(&cluster_names, admin, &cluster_info).await;
            }
        }

        Ok(result)
    }

    pub async fn query_cluster_broker_names_by_request_with_rpc_hook(
        request: ClusterBrokerNameQueryRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<ClusterBrokerNameQueryResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::query_cluster_broker_names_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn query_cluster_broker_names_with_admin(
        admin: &DefaultMQAdminExt,
        request: &ClusterBrokerNameQueryRequest,
    ) -> RocketMQResult<ClusterBrokerNameQueryResult> {
        let cluster_info = admin.examine_broker_cluster_info().await?;
        Ok(collect_cluster_broker_names(request.cluster_name(), &cluster_info))
    }

    pub async fn send_message_rt_by_request_with_rpc_hook(
        request: ClusterSendMessageRtRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<ClusterSendMessageRtResult> {
        let broker_names = Self::query_cluster_broker_names_by_request_with_rpc_hook(
            request.broker_name_query_request(),
            rpc_hook.clone(),
        )
        .await?;

        let instance_name = format!("PID_ClusterRTCommand_{}", current_millis());
        let mut client_config = ClientConfig::default();
        client_config.set_instance_name(instance_name.into());
        let mut builder = DefaultMQProducer::builder()
            .producer_group(current_millis().to_string())
            .client_config(client_config);
        if let Some(hook) = rpc_hook {
            builder = builder.rpc_hook(hook);
        }
        let mut producer = builder.build();

        let result = Self::send_message_rt_with_producer(&mut producer, &request, &broker_names).await;
        producer.shutdown().await;
        result
    }

    pub async fn send_message_rt_with_producer(
        producer: &mut DefaultMQProducer,
        request: &ClusterSendMessageRtRequest,
        broker_names: &ClusterBrokerNameQueryResult,
    ) -> RocketMQResult<ClusterSendMessageRtResult> {
        producer.start().await.map_err(|error| {
            RocketMQError::Internal(format!("ClusterService: failed to start cluster RT producer: {error}"))
        })?;

        let mut rows = Vec::new();
        for (cluster_name, broker_names) in &broker_names.broker_names_by_cluster {
            for broker_name in broker_names {
                rows.push(send_cluster_rt_to_broker(producer, cluster_name, broker_name, request).await);
            }
        }

        Ok(ClusterSendMessageRtResult {
            missing_clusters: broker_names.missing_clusters.clone(),
            rows,
        })
    }
}

async fn send_cluster_rt_to_broker(
    producer: &mut DefaultMQProducer,
    cluster_name: &str,
    broker_name: &str,
    request: &ClusterSendMessageRtRequest,
) -> ClusterSendMessageRtRow {
    let msg_body = vec![b'a'; request.size() as usize];
    let msg = Message::builder()
        .topic(broker_name)
        .body_slice(&msg_body)
        .build_unchecked();
    let mut elapsed = 0;
    let mut success_count = 0;
    let mut fail_count = 0;

    for index in 0..request.amount() {
        let start = current_millis();
        match producer.send(msg.clone()).await {
            Ok(_) => {
                success_count += 1;
            }
            Err(_) => {
                fail_count += 1;
            }
        }
        let end = current_millis();
        if index != 0 {
            elapsed += end - start;
        }
    }

    let rt = if request.amount() > 1 {
        elapsed as f64 / (request.amount() - 1) as f64
    } else {
        elapsed as f64
    };

    ClusterSendMessageRtRow {
        cluster_name: cluster_name.to_string(),
        broker_name: broker_name.to_string(),
        rt,
        success_count,
        fail_count,
    }
}

fn collect_cluster_broker_names(
    cluster_name: Option<&CheetahString>,
    cluster_info: &ClusterInfo,
) -> ClusterBrokerNameQueryResult {
    let target_cluster_names = target_cluster_names(cluster_name, cluster_info);
    let Some(cluster_addr_table) = cluster_info.cluster_addr_table.as_ref() else {
        return ClusterBrokerNameQueryResult {
            broker_names_by_cluster: BTreeMap::new(),
            missing_clusters: target_cluster_names.into_iter().collect(),
        };
    };

    let mut broker_names_by_cluster = BTreeMap::new();
    let mut missing_clusters = Vec::new();
    for cluster_name in target_cluster_names {
        if let Some(broker_names) = cluster_addr_table.get(cluster_name.as_str()) {
            let mut broker_names: Vec<String> = broker_names.iter().map(ToString::to_string).collect();
            broker_names.sort();
            broker_names_by_cluster.insert(cluster_name, broker_names);
        } else {
            missing_clusters.push(cluster_name);
        }
    }

    ClusterBrokerNameQueryResult {
        broker_names_by_cluster,
        missing_clusters,
    }
}

fn target_cluster_names(cluster_name: Option<&CheetahString>, cluster_info: &ClusterInfo) -> BTreeSet<String> {
    if let Some(cluster_name) = cluster_name {
        let mut set = BTreeSet::new();
        set.insert(cluster_name.to_string());
        return set;
    }

    cluster_info
        .cluster_addr_table
        .as_ref()
        .map(|table| table.keys().map(ToString::to_string).collect())
        .unwrap_or_default()
}

async fn collect_cluster_more_stats_rows(
    cluster_names: &BTreeSet<String>,
    admin: &DefaultMQAdminExt,
    cluster_info: &ClusterInfo,
) -> Vec<ClusterMoreStatsRow> {
    let Some(cluster_addr_table) = cluster_info.cluster_addr_table.as_ref() else {
        return Vec::new();
    };
    let Some(broker_addr_table) = cluster_info.broker_addr_table.as_ref() else {
        return Vec::new();
    };

    let mut rows = Vec::new();
    for cluster_name in cluster_names {
        let broker_names: BTreeSet<String> = match cluster_addr_table.get(cluster_name.as_str()) {
            Some(names) => names.iter().map(ToString::to_string).collect(),
            None => continue,
        };

        for broker_name in &broker_names {
            let Some(broker_data) = broker_addr_table.get(broker_name.as_str()) else {
                continue;
            };

            for addr in broker_data.broker_addrs().values() {
                let mut row = ClusterMoreStatsRow {
                    cluster_name: cluster_name.clone(),
                    broker_name: broker_name.clone(),
                    in_total_yesterday: 0,
                    out_total_yesterday: 0,
                    in_total_today: 0,
                    out_total_today: 0,
                };

                if let Ok(kv_table) = admin.fetch_broker_runtime_stats(addr.clone()).await {
                    if let (Some(put_yest_morning), Some(put_today_morning), Some(put_today_now)) = (
                        get_kv_value(&kv_table, "msgPutTotalYesterdayMorning"),
                        get_kv_value(&kv_table, "msgPutTotalTodayMorning"),
                        get_kv_value(&kv_table, "msgPutTotalTodayNow"),
                    ) {
                        row.in_total_yesterday = put_today_morning - put_yest_morning;
                        row.in_total_today = put_today_now - put_today_morning;
                    }

                    if let (Some(get_yest_morning), Some(get_today_morning), Some(get_today_now)) = (
                        get_kv_value(&kv_table, "msgGetTotalYesterdayMorning"),
                        get_kv_value(&kv_table, "msgGetTotalTodayMorning"),
                        get_kv_value(&kv_table, "msgGetTotalTodayNow"),
                    ) {
                        row.out_total_yesterday = get_today_morning - get_yest_morning;
                        row.out_total_today = get_today_now - get_today_morning;
                    }
                }

                rows.push(row);
            }
        }
    }

    rows
}

async fn collect_cluster_base_rows(
    cluster_names: &BTreeSet<String>,
    admin: &DefaultMQAdminExt,
    cluster_info: &ClusterInfo,
) -> Vec<ClusterBaseInfoRow> {
    let Some(cluster_addr_table) = cluster_info.cluster_addr_table.as_ref() else {
        return Vec::new();
    };
    let Some(broker_addr_table) = cluster_info.broker_addr_table.as_ref() else {
        return Vec::new();
    };

    let mut rows = Vec::new();
    for cluster_name in cluster_names {
        let broker_names: BTreeSet<String> = match cluster_addr_table.get(cluster_name.as_str()) {
            Some(names) => names.iter().map(ToString::to_string).collect(),
            None => continue,
        };

        for broker_name in &broker_names {
            let Some(broker_data) = broker_addr_table.get(broker_name.as_str()) else {
                continue;
            };

            for (broker_id, addr) in broker_data.broker_addrs() {
                rows.push(build_cluster_base_row(
                    cluster_name.clone(),
                    broker_name.clone(),
                    *broker_id,
                    addr.clone(),
                    admin.fetch_broker_runtime_stats(addr.clone()).await.ok(),
                ));
            }
        }
    }

    rows
}

fn build_cluster_base_row(
    cluster_name: String,
    broker_name: String,
    broker_id: u64,
    broker_addr: CheetahString,
    kv_table: Option<KVTable>,
) -> ClusterBaseInfoRow {
    let mut in_tps: f64 = 0.0;
    let mut out_tps: f64 = 0.0;
    let mut version = String::new();
    let mut send_thread_pool_queue_size = String::new();
    let mut pull_thread_pool_queue_size = String::new();
    let mut ack_thread_pool_queue_size = String::from("N");
    let mut send_thread_pool_queue_head_wait_time_mills = String::new();
    let mut pull_thread_pool_queue_head_wait_time_mills = String::new();
    let mut ack_thread_pool_queue_head_wait_time_mills = String::from("N");
    let mut page_cache_lock_time_millis = String::new();
    let mut earliest_message_time_stamp = String::new();
    let mut commit_log_disk_ratio = String::new();
    let mut timer_read_behind: i64 = 0;
    let mut timer_offset_behind: i64 = 0;
    let mut timer_congest_num: i64 = 0;
    let mut timer_enqueue_tps: f32 = 0.0;
    let mut timer_dequeue_tps: f32 = 0.0;
    let mut broker_active = false;

    if let Some(kv_table) = kv_table.as_ref() {
        broker_active = get_kv_str(kv_table, "brokerActive")
            .map(|value| value == "true")
            .unwrap_or(false);

        if let Some(put_tps) = get_kv_str(kv_table, "putTps") {
            if let Some(first) = put_tps.split_whitespace().next() {
                in_tps = first.parse::<f64>().unwrap_or(0.0);
            }
        }

        if let Some(get_transferred_tps) = get_kv_str(kv_table, "getTransferredTps") {
            if let Some(first) = get_transferred_tps.split_whitespace().next() {
                out_tps = first.parse::<f64>().unwrap_or(0.0);
            }
        }

        send_thread_pool_queue_size = get_kv_str(kv_table, "sendThreadPoolQueueSize")
            .unwrap_or_default()
            .to_string();
        pull_thread_pool_queue_size = get_kv_str(kv_table, "pullThreadPoolQueueSize")
            .unwrap_or_default()
            .to_string();
        ack_thread_pool_queue_size = get_kv_str(kv_table, "ackThreadPoolQueueSize")
            .unwrap_or("N")
            .to_string();

        send_thread_pool_queue_head_wait_time_mills = get_kv_str(kv_table, "sendThreadPoolQueueHeadWaitTimeMills")
            .unwrap_or_default()
            .to_string();
        pull_thread_pool_queue_head_wait_time_mills = get_kv_str(kv_table, "pullThreadPoolQueueHeadWaitTimeMills")
            .unwrap_or_default()
            .to_string();
        ack_thread_pool_queue_head_wait_time_mills = get_kv_str(kv_table, "ackThreadPoolQueueHeadWaitTimeMills")
            .unwrap_or("N")
            .to_string();

        page_cache_lock_time_millis = get_kv_str(kv_table, "pageCacheLockTimeMills")
            .unwrap_or_default()
            .to_string();
        earliest_message_time_stamp = get_kv_str(kv_table, "earliestMessageTimeStamp")
            .unwrap_or_default()
            .to_string();
        commit_log_disk_ratio = get_kv_str(kv_table, "commitLogDiskRatio")
            .unwrap_or_default()
            .to_string();

        timer_read_behind = parse_i64(kv_table, "timerReadBehind");
        timer_offset_behind = parse_i64(kv_table, "timerOffsetBehind");
        timer_congest_num = parse_i64(kv_table, "timerCongestNum");
        timer_enqueue_tps = parse_f32(kv_table, "timerEnqueueTps");
        timer_dequeue_tps = parse_f32(kv_table, "timerDequeueTps");

        version = get_kv_str(kv_table, "brokerVersionDesc")
            .unwrap_or_default()
            .to_string();
    }

    let mut hour: f64 = 0.0;
    let mut space: f64 = 0.0;

    if !earliest_message_time_stamp.is_empty() {
        if let Ok(ts) = earliest_message_time_stamp.parse::<u64>() {
            let mills = current_millis().saturating_sub(ts);
            hour = mills as f64 / 1000.0 / 60.0 / 60.0;
        }
    }

    if !commit_log_disk_ratio.is_empty() {
        space = commit_log_disk_ratio.parse::<f64>().unwrap_or(0.0);
    }

    ClusterBaseInfoRow {
        cluster_name,
        broker_name,
        broker_id,
        broker_addr,
        version,
        in_tps: format!(
            "{:>9.2}({},{}ms)",
            in_tps, send_thread_pool_queue_size, send_thread_pool_queue_head_wait_time_mills
        ),
        out_tps: format!(
            "{:>9.2}({},{}ms|{},{}ms)",
            out_tps,
            pull_thread_pool_queue_size,
            pull_thread_pool_queue_head_wait_time_mills,
            ack_thread_pool_queue_size,
            ack_thread_pool_queue_head_wait_time_mills
        ),
        timer_progress: format!(
            "{}-{}({:.1}w, {:.1}, {:.1})",
            timer_read_behind,
            timer_offset_behind,
            timer_congest_num as f32 / 10000.0,
            timer_enqueue_tps,
            timer_dequeue_tps
        ),
        page_cache_lock_time_millis,
        hour: format!("{:.2}", hour),
        space: format!("{:.4}", space),
        broker_active,
    }
}

fn parse_i64(kv_table: &KVTable, key: &str) -> i64 {
    get_kv_str(kv_table, key)
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(0)
}

fn parse_f32(kv_table: &KVTable, key: &str) -> f32 {
    get_kv_str(kv_table, key)
        .and_then(|value| value.parse::<f32>().ok())
        .unwrap_or(0.0)
}

fn get_kv_str<'a>(kv_table: &'a KVTable, key: &str) -> Option<&'a str> {
    kv_table
        .table
        .get(&CheetahString::from(key))
        .map(|value| value.as_str())
}

fn get_kv_value(kv_table: &KVTable, key: &str) -> Option<i64> {
    kv_table
        .table
        .get(&CheetahString::from(key))
        .and_then(|value| value.parse::<i64>().ok())
}

fn trim_optional_string(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn builder_with_namesrv(namesrv_addr: Option<&str>) -> AdminBuilder {
    let builder = AdminBuilder::new();
    match namesrv_addr {
        Some(addr) => builder.namesrv_addr(addr),
        None => builder,
    }
}

fn admin_builder_with_rpc_hook(builder: AdminBuilder, rpc_hook: Option<Arc<dyn RPCHook>>) -> AdminBuilder {
    match rpc_hook {
        Some(hook) => builder.rpc_hook(hook),
        None => builder,
    }
}
