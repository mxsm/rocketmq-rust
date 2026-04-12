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

//! Consumer offset-related admin service models and operations.

use std::collections::HashMap;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::protocol::admin::rollback_stats::RollbackStats;
use rocketmq_remoting::runtime::RPCHook;
use serde::Deserialize;
use serde::Serialize;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::core::admin::AdminBuilder;
use crate::core::RocketMQError;
use crate::core::RocketMQResult;
use crate::core::ToolsError;

pub const SKIP_TO_LATEST_TIMESTAMP: u64 = u64::MAX;

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
pub struct CloneGroupOffsetRequest {
    src_group: CheetahString,
    dest_group: CheetahString,
    topic: CheetahString,
    offline: bool,
    namesrv_addr: Option<String>,
}

impl CloneGroupOffsetRequest {
    pub fn try_new(
        src_group: impl Into<String>,
        dest_group: impl Into<String>,
        topic: impl Into<String>,
        offline: bool,
    ) -> RocketMQResult<Self> {
        Ok(Self {
            src_group: trim_required_cheetah("srcGroup", src_group)?,
            dest_group: trim_required_cheetah("destGroup", dest_group)?,
            topic: trim_required_cheetah("topic", topic)?,
            offline,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn src_group(&self) -> &CheetahString {
        &self.src_group
    }

    pub fn dest_group(&self) -> &CheetahString {
        &self.dest_group
    }

    pub fn topic(&self) -> &CheetahString {
        &self.topic
    }

    pub fn offline(&self) -> bool {
        self.offline
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        let builder = AdminBuilder::new();
        match self.namesrv_addr.as_deref() {
            Some(addr) => builder.namesrv_addr(addr),
            None => builder,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerStatusQueryRequest {
    group: CheetahString,
    topic: CheetahString,
    origin_client_id: CheetahString,
    namesrv_addr: Option<String>,
}

impl ConsumerStatusQueryRequest {
    pub fn try_new(
        group: impl Into<String>,
        topic: impl Into<String>,
        origin_client_id: Option<String>,
    ) -> RocketMQResult<Self> {
        Ok(Self {
            group: trim_required_cheetah("group", group)?,
            topic: trim_required_cheetah("topic", topic)?,
            origin_client_id: CheetahString::from(trim_optional_string(origin_client_id).unwrap_or_default()),
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn group(&self) -> &CheetahString {
        &self.group
    }

    pub fn topic(&self) -> &CheetahString {
        &self.topic
    }

    pub fn origin_client_id(&self) -> &CheetahString {
        &self.origin_client_id
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        let builder = AdminBuilder::new();
        match self.namesrv_addr.as_deref() {
            Some(addr) => builder.namesrv_addr(addr),
            None => builder,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerStatusRow {
    pub client_id: CheetahString,
    pub broker_name: CheetahString,
    pub queue_id: i32,
    pub offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerStatusResult {
    pub rows: Vec<ConsumerStatusRow>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkipAccumulatedMessageRequest {
    group: CheetahString,
    topic: CheetahString,
    cluster: Option<CheetahString>,
    force: bool,
    namesrv_addr: Option<String>,
}

impl SkipAccumulatedMessageRequest {
    pub fn try_new(
        group: impl Into<String>,
        topic: impl Into<String>,
        cluster: Option<String>,
        force: Option<bool>,
        namesrv_addr: Option<String>,
    ) -> RocketMQResult<Self> {
        Ok(Self {
            group: trim_required_cheetah("group", group)?,
            topic: trim_required_cheetah("topic", topic)?,
            cluster: trim_optional_string(cluster)
                .map(|cluster| trim_required_cheetah("cluster", cluster))
                .transpose()?,
            force: force.unwrap_or(true),
            namesrv_addr: trim_optional_string(namesrv_addr),
        })
    }

    pub fn group(&self) -> &CheetahString {
        &self.group
    }

    pub fn topic(&self) -> &CheetahString {
        &self.topic
    }

    pub fn cluster(&self) -> Option<&CheetahString> {
        self.cluster.as_ref()
    }

    pub fn force(&self) -> bool {
        self.force
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        let builder = AdminBuilder::new();
        match self.namesrv_addr.as_deref() {
            Some(addr) => builder.namesrv_addr(addr),
            None => builder,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SkipAccumulatedMessageResult {
    Current(HashMap<MessageQueue, u64>),
    Legacy(Vec<RollbackStats>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResetOffsetByTimeRequest {
    group: CheetahString,
    topic: CheetahString,
    timestamp: u64,
    namesrv_addr: Option<String>,
}

impl ResetOffsetByTimeRequest {
    pub fn try_new(group: impl Into<String>, topic: impl Into<String>, timestamp: u64) -> RocketMQResult<Self> {
        Ok(Self {
            group: trim_required_cheetah("group", group)?,
            topic: trim_required_cheetah("topic", topic)?,
            timestamp,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn group(&self) -> &CheetahString {
        &self.group
    }

    pub fn topic(&self) -> &CheetahString {
        &self.topic
    }

    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        let builder = AdminBuilder::new();
        match self.namesrv_addr.as_deref() {
            Some(addr) => builder.namesrv_addr(addr),
            None => builder,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResetOffsetByTimeOldRequest {
    group: CheetahString,
    topic: CheetahString,
    timestamp: u64,
    force: bool,
    cluster: Option<CheetahString>,
    namesrv_addr: Option<String>,
}

impl ResetOffsetByTimeOldRequest {
    pub fn try_new(
        group: impl Into<String>,
        topic: impl Into<String>,
        timestamp: u64,
        force: Option<bool>,
        cluster: Option<String>,
        namesrv_addr: Option<String>,
    ) -> RocketMQResult<Self> {
        Ok(Self {
            group: trim_required_cheetah("group", group)?,
            topic: trim_required_cheetah("topic", topic)?,
            timestamp,
            force: force.unwrap_or(true),
            cluster: trim_optional_string(cluster)
                .map(|cluster| trim_required_cheetah("cluster", cluster))
                .transpose()?,
            namesrv_addr: trim_optional_string(namesrv_addr),
        })
    }

    pub fn group(&self) -> &CheetahString {
        &self.group
    }

    pub fn topic(&self) -> &CheetahString {
        &self.topic
    }

    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    pub fn force(&self) -> bool {
        self.force
    }

    pub fn cluster(&self) -> Option<&CheetahString> {
        self.cluster.as_ref()
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        let builder = AdminBuilder::new();
        match self.namesrv_addr.as_deref() {
            Some(addr) => builder.namesrv_addr(addr),
            None => builder,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResetOffsetByTimeResult {
    Current(HashMap<MessageQueue, u64>),
    Legacy {
        rollback_stats: Vec<RollbackStats>,
        current_error: String,
    },
}

pub struct OffsetService;

impl OffsetService {
    pub async fn clone_group_offset_by_request_with_rpc_hook(
        request: CloneGroupOffsetRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<()> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = admin
            .clone_group_offset(
                request.src_group().clone(),
                request.dest_group().clone(),
                request.topic().clone(),
                request.offline(),
            )
            .await
            .map_err(|error| RocketMQError::Internal(format!("OffsetService: failed to clone group offset: {error}")));
        admin.shutdown().await;
        result
    }

    pub async fn query_consumer_status_by_request_with_rpc_hook(
        request: ConsumerStatusQueryRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<ConsumerStatusResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::query_consumer_status_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn query_consumer_status_with_admin(
        admin: &DefaultMQAdminExt,
        request: &ConsumerStatusQueryRequest,
    ) -> RocketMQResult<ConsumerStatusResult> {
        let consumer_status_table = admin
            .get_consume_status(
                request.topic().clone(),
                request.group().clone(),
                request.origin_client_id().clone(),
            )
            .await?;
        let mut rows = Vec::new();
        for (client_id, mq_table) in consumer_status_table {
            for (mq, offset) in mq_table {
                rows.push(ConsumerStatusRow {
                    client_id: client_id.clone(),
                    broker_name: mq.broker_name().clone(),
                    queue_id: mq.queue_id(),
                    offset,
                });
            }
        }
        rows.sort_by(|left, right| {
            left.client_id
                .cmp(&right.client_id)
                .then_with(|| left.broker_name.cmp(&right.broker_name))
                .then_with(|| left.queue_id.cmp(&right.queue_id))
        });

        Ok(ConsumerStatusResult { rows })
    }

    pub async fn skip_accumulated_message_by_request_with_rpc_hook(
        request: SkipAccumulatedMessageRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<SkipAccumulatedMessageResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::skip_accumulated_message_with_admin(&mut admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn skip_accumulated_message_with_admin(
        admin: &mut DefaultMQAdminExt,
        request: &SkipAccumulatedMessageRequest,
    ) -> RocketMQResult<SkipAccumulatedMessageResult> {
        match admin
            .reset_offset_by_timestamp(
                request.cluster().cloned(),
                request.topic().clone(),
                request.group().clone(),
                SKIP_TO_LATEST_TIMESTAMP,
                request.force(),
            )
            .await
        {
            Ok(offset_table) => Ok(SkipAccumulatedMessageResult::Current(offset_table)),
            Err(err) => {
                if matches!(
                    err,
                    RocketMQError::BrokerOperationFailed { code, .. }
                        if ResponseCode::from(code) == ResponseCode::ConsumerNotOnline
                ) {
                    let rollback_stats = admin
                        .reset_offset_by_timestamp_old(
                            request.cluster().cloned(),
                            request.group().clone(),
                            request.topic().clone(),
                            SKIP_TO_LATEST_TIMESTAMP,
                            request.force(),
                        )
                        .await?;
                    Ok(SkipAccumulatedMessageResult::Legacy(rollback_stats))
                } else {
                    Err(err)
                }
            }
        }
    }

    pub async fn reset_offset_by_time_by_request_with_rpc_hook(
        request: ResetOffsetByTimeRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<ResetOffsetByTimeResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::reset_offset_by_time_with_admin(&mut admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn reset_offset_by_time_with_admin(
        admin: &mut DefaultMQAdminExt,
        request: &ResetOffsetByTimeRequest,
    ) -> RocketMQResult<ResetOffsetByTimeResult> {
        match admin
            .reset_offset_by_timestamp(
                None,
                request.topic().clone(),
                request.group().clone(),
                request.timestamp(),
                false,
            )
            .await
        {
            Ok(offset_table) => Ok(ResetOffsetByTimeResult::Current(offset_table)),
            Err(err) => {
                let current_error = err.to_string();
                let rollback_stats = admin
                    .reset_offset_by_timestamp_old(
                        None,
                        request.group().clone(),
                        request.topic().clone(),
                        request.timestamp(),
                        false,
                    )
                    .await?;
                Ok(ResetOffsetByTimeResult::Legacy {
                    rollback_stats,
                    current_error,
                })
            }
        }
    }

    pub async fn reset_offset_by_time_old_by_request_with_rpc_hook(
        request: ResetOffsetByTimeOldRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<Vec<RollbackStats>> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::reset_offset_by_time_old_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn reset_offset_by_time_old_with_admin(
        admin: &DefaultMQAdminExt,
        request: &ResetOffsetByTimeOldRequest,
    ) -> RocketMQResult<Vec<RollbackStats>> {
        admin
            .reset_offset_by_timestamp_old(
                request.cluster().cloned(),
                request.group().clone(),
                request.topic().clone(),
                request.timestamp(),
                request.force(),
            )
            .await
    }
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
    fn clone_group_offset_request_trims_fields() {
        let request = CloneGroupOffsetRequest::try_new(" SourceGroup ", " DestGroup ", " TestTopic ", true).unwrap();
        assert_eq!(request.src_group().as_str(), "SourceGroup");
        assert_eq!(request.dest_group().as_str(), "DestGroup");
        assert_eq!(request.topic().as_str(), "TestTopic");
        assert!(request.offline());
    }

    #[test]
    fn consumer_status_query_request_trims_fields() {
        let request =
            ConsumerStatusQueryRequest::try_new(" TestGroup ", " TestTopic ", Some(" client-a ".into())).unwrap();
        assert_eq!(request.group().as_str(), "TestGroup");
        assert_eq!(request.topic().as_str(), "TestTopic");
        assert_eq!(request.origin_client_id().as_str(), "client-a");
    }

    #[test]
    fn skip_accumulated_message_request_defaults_force_and_trims() {
        let request = SkipAccumulatedMessageRequest::try_new(
            " TestGroup ",
            " TestTopic ",
            Some(" DefaultCluster ".into()),
            None,
            Some(" 127.0.0.1:9876 ".into()),
        )
        .unwrap();
        assert_eq!(request.group().as_str(), "TestGroup");
        assert_eq!(request.topic().as_str(), "TestTopic");
        assert_eq!(request.cluster().unwrap().as_str(), "DefaultCluster");
        assert!(request.force());
    }

    #[test]
    fn reset_offset_by_time_request_trims_fields() {
        let request = ResetOffsetByTimeRequest::try_new(" TestGroup ", " TestTopic ", 1234)
            .unwrap()
            .with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".into()));

        assert_eq!(request.group().as_str(), "TestGroup");
        assert_eq!(request.topic().as_str(), "TestTopic");
        assert_eq!(request.timestamp(), 1234);
        assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
    }

    #[test]
    fn reset_offset_by_time_old_request_trims_fields() {
        let request = ResetOffsetByTimeOldRequest::try_new(
            " TestGroup ",
            " TestTopic ",
            1234,
            Some(false),
            Some(" DefaultCluster ".into()),
            Some(" 127.0.0.1:9876 ".into()),
        )
        .unwrap();

        assert_eq!(request.group().as_str(), "TestGroup");
        assert_eq!(request.topic().as_str(), "TestTopic");
        assert_eq!(request.cluster().unwrap().as_str(), "DefaultCluster");
        assert_eq!(request.timestamp(), 1234);
        assert!(!request.force());
        assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
    }
}
