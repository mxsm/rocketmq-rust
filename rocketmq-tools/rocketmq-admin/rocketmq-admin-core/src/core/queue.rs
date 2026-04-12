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

//! Queue-related admin service models and operations.

use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_remoting::protocol::body::check_rocksdb_cqwrite_progress_response_body::CheckRocksdbCqWriteResult;
use rocketmq_remoting::protocol::body::query_consume_queue_response_body::QueryConsumeQueueResponseBody;
use rocketmq_remoting::runtime::RPCHook;
use serde::Deserialize;
use serde::Serialize;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::core::admin::AdminBuilder;
use crate::core::RocketMQError;
use crate::core::RocketMQResult;
use crate::core::ToolsError;

const THIRTY_DAYS_MILLIS: i64 = 30 * 24 * 60 * 60 * 1000;

fn trim_optional_string(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn trim_required_cheetah(field: &'static str, value: impl Into<String>) -> RocketMQResult<CheetahString> {
    let value = value.into();
    let value = value.trim();
    if value.is_empty() {
        return Err(ToolsError::validation_error(field, format!("{field} must not be empty")).into());
    }
    Ok(CheetahString::from(value))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryConsumeQueueRequest {
    topic: CheetahString,
    queue_id: i32,
    index: u64,
    count: i32,
    broker_addr: Option<CheetahString>,
    consumer_group: CheetahString,
    namesrv_addr: Option<String>,
}

impl QueryConsumeQueueRequest {
    pub fn try_new(
        topic: impl Into<String>,
        queue_id: i32,
        index: u64,
        count: i32,
        broker_addr: Option<String>,
        consumer_group: Option<String>,
    ) -> RocketMQResult<Self> {
        Ok(Self {
            topic: trim_required_cheetah("topic", topic)?,
            queue_id,
            index,
            count,
            broker_addr: trim_optional_string(broker_addr)
                .map(|addr| trim_required_cheetah("brokerAddr", addr))
                .transpose()?,
            consumer_group: CheetahString::from(trim_optional_string(consumer_group).unwrap_or_default()),
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn topic(&self) -> &CheetahString {
        &self.topic
    }

    pub fn queue_id(&self) -> i32 {
        self.queue_id
    }

    pub fn index(&self) -> u64 {
        self.index
    }

    pub fn count(&self) -> i32 {
        self.count
    }

    pub fn broker_addr(&self) -> Option<&CheetahString> {
        self.broker_addr.as_ref()
    }

    pub fn consumer_group(&self) -> &CheetahString {
        &self.consumer_group
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        let builder = AdminBuilder::new();
        match self.namesrv_addr() {
            Some(addr) => builder.namesrv_addr(addr),
            None => builder,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryConsumeQueueResult {
    pub broker_addr: CheetahString,
    pub response_body: QueryConsumeQueueResponseBody,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckRocksdbCqWriteProgressRequest {
    cluster_name: CheetahString,
    namesrv_addr: String,
    topic: CheetahString,
    check_store_time: i64,
}

impl CheckRocksdbCqWriteProgressRequest {
    pub fn try_new(
        cluster_name: impl Into<String>,
        namesrv_addr: impl Into<String>,
        topic: Option<String>,
        check_from: Option<i64>,
    ) -> RocketMQResult<Self> {
        let namesrv_addr = namesrv_addr.into();
        let namesrv_addr = namesrv_addr.trim();
        if namesrv_addr.is_empty() {
            return Err(ToolsError::validation_error("nameserverAddr", "nameserverAddr must not be empty").into());
        }

        Ok(Self {
            cluster_name: trim_required_cheetah("cluster", cluster_name)?,
            namesrv_addr: namesrv_addr.to_string(),
            topic: CheetahString::from(trim_optional_string(topic).unwrap_or_default()),
            check_store_time: check_from.unwrap_or_else(|| current_millis() as i64 - THIRTY_DAYS_MILLIS),
        })
    }

    pub fn cluster_name(&self) -> &CheetahString {
        &self.cluster_name
    }

    pub fn namesrv_addr(&self) -> &str {
        &self.namesrv_addr
    }

    pub fn topic(&self) -> &CheetahString {
        &self.topic
    }

    pub fn check_store_time(&self) -> i64 {
        self.check_store_time
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        AdminBuilder::new().namesrv_addr(&self.namesrv_addr)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckRocksdbCqWriteProgressEntry {
    pub broker_name: CheetahString,
    pub broker_addr: CheetahString,
    pub result: CheckRocksdbCqWriteResult,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueOperationFailure {
    pub broker_name: CheetahString,
    pub broker_addr: CheetahString,
    pub error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckRocksdbCqWriteProgressResult {
    pub cluster_found: bool,
    pub entries: Vec<CheckRocksdbCqWriteProgressEntry>,
    pub failures: Vec<QueueOperationFailure>,
}

pub struct QueueService;

impl QueueService {
    pub async fn query_consume_queue_by_request_with_rpc_hook(
        request: QueryConsumeQueueRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<QueryConsumeQueueResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::query_consume_queue_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn query_consume_queue_with_admin(
        admin: &DefaultMQAdminExt,
        request: &QueryConsumeQueueRequest,
    ) -> RocketMQResult<QueryConsumeQueueResult> {
        let broker_addr = match request.broker_addr() {
            Some(addr) => addr.clone(),
            None => resolve_topic_master_broker(admin, request.topic()).await?,
        };

        let response_body = admin
            .query_consume_queue(
                broker_addr.clone(),
                request.topic().clone(),
                request.queue_id(),
                request.index(),
                request.count(),
                request.consumer_group().clone(),
            )
            .await
            .map_err(|error| {
                RocketMQError::Internal(format!(
                    "QueueService: failed to query consume queue from {}: {}",
                    broker_addr, error
                ))
            })?;

        Ok(QueryConsumeQueueResult {
            broker_addr,
            response_body,
        })
    }

    pub async fn check_rocksdb_cq_write_progress_by_request_with_rpc_hook(
        request: CheckRocksdbCqWriteProgressRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<CheckRocksdbCqWriteProgressResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::check_rocksdb_cq_write_progress_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn check_rocksdb_cq_write_progress_with_admin(
        admin: &DefaultMQAdminExt,
        request: &CheckRocksdbCqWriteProgressRequest,
    ) -> RocketMQResult<CheckRocksdbCqWriteProgressResult> {
        let cluster_info = admin.examine_broker_cluster_info().await.map_err(|error| {
            RocketMQError::Internal(format!("QueueService: failed to examine broker cluster info: {error}"))
        })?;
        let Some(cluster_addr_table) = cluster_info.cluster_addr_table.as_ref() else {
            return Ok(CheckRocksdbCqWriteProgressResult {
                cluster_found: false,
                entries: Vec::new(),
                failures: Vec::new(),
            });
        };
        let Some(cluster_broker_names) = cluster_addr_table.get(request.cluster_name().as_str()) else {
            return Ok(CheckRocksdbCqWriteProgressResult {
                cluster_found: false,
                entries: Vec::new(),
                failures: Vec::new(),
            });
        };

        let Some(broker_addr_table) = cluster_info.broker_addr_table.as_ref() else {
            return Err(RocketMQError::Internal("brokerAddrTable is empty".into()));
        };

        let mut broker_names = cluster_broker_names.iter().cloned().collect::<Vec<_>>();
        broker_names.sort();
        let mut entries = Vec::new();
        let mut failures = Vec::new();

        for broker_name in broker_names {
            let Some(broker_data) = broker_addr_table.get(&broker_name) else {
                continue;
            };
            let Some(broker_addr) = broker_data.broker_addrs().get(&0u64).cloned() else {
                continue;
            };

            match admin
                .check_rocksdb_cq_write_progress(
                    broker_addr.clone(),
                    request.topic().clone(),
                    request.check_store_time(),
                )
                .await
            {
                Ok(result) => entries.push(CheckRocksdbCqWriteProgressEntry {
                    broker_name,
                    broker_addr,
                    result,
                }),
                Err(error) => failures.push(QueueOperationFailure {
                    broker_name,
                    broker_addr,
                    error: error.to_string(),
                }),
            }
        }

        Ok(CheckRocksdbCqWriteProgressResult {
            cluster_found: true,
            entries,
            failures,
        })
    }
}

async fn resolve_topic_master_broker(
    admin: &DefaultMQAdminExt,
    topic: &CheetahString,
) -> RocketMQResult<CheetahString> {
    let topic_route_data = admin.examine_topic_route_info(topic.clone()).await.map_err(|error| {
        RocketMQError::Internal(format!("QueueService: failed to examine topic route info: {error}"))
    })?;

    let topic_route_data = topic_route_data.ok_or_else(|| RocketMQError::Internal("No topic route data!".into()))?;
    if topic_route_data.broker_datas.is_empty() {
        return Err(RocketMQError::Internal(
            "No topic route data! broker_datas is empty".into(),
        ));
    }

    topic_route_data.broker_datas[0]
        .broker_addrs()
        .get(&0u64)
        .cloned()
        .ok_or_else(|| RocketMQError::Internal("No master broker address found in topic route data".into()))
}

fn admin_builder_with_rpc_hook(builder: AdminBuilder, rpc_hook: Option<Arc<dyn RPCHook>>) -> AdminBuilder {
    match rpc_hook {
        Some(hook) => builder.rpc_hook(hook),
        None => builder,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_consume_queue_request_trims_fields() {
        let request = QueryConsumeQueueRequest::try_new(
            " TestTopic ",
            1,
            10,
            20,
            Some(" 127.0.0.1:10911 ".into()),
            Some(" TestGroup ".into()),
        )
        .unwrap()
        .with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".into()));

        assert_eq!(request.topic().as_str(), "TestTopic");
        assert_eq!(request.broker_addr().unwrap().as_str(), "127.0.0.1:10911");
        assert_eq!(request.consumer_group().as_str(), "TestGroup");
        assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
    }

    #[test]
    fn check_rocksdb_cq_write_progress_request_trims_fields() {
        let request = CheckRocksdbCqWriteProgressRequest::try_new(
            " DefaultCluster ",
            " 127.0.0.1:9876 ",
            Some(" TestTopic ".into()),
            Some(1024),
        )
        .unwrap();

        assert_eq!(request.cluster_name().as_str(), "DefaultCluster");
        assert_eq!(request.namesrv_addr(), "127.0.0.1:9876");
        assert_eq!(request.topic().as_str(), "TestTopic");
        assert_eq!(request.check_store_time(), 1024);
    }
}
