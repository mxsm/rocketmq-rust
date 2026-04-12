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

//! Consumer-related admin service models and operations.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::common::key_builder::KeyBuilder;
use rocketmq_common::common::message::message_enum::MessageRequestMode;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::mix_all::get_dlq_topic;
use rocketmq_common::common::mix_all::get_retry_topic;
use rocketmq_remoting::protocol::body::consumer_running_info::ConsumerRunningInfo;
use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use rocketmq_remoting::runtime::RPCHook;
use serde::Deserialize;
use serde::Serialize;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::core::admin::AdminBuilder;
use crate::core::broker::BrokerTarget;
use crate::core::resolver::BrokerAddressResolver;
use crate::core::RocketMQResult;
use crate::core::ToolsError;

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

fn target_from_options(broker_addr: Option<String>, cluster_name: Option<String>) -> RocketMQResult<BrokerTarget> {
    let broker_addr = trim_optional_string(broker_addr);
    let cluster_name = trim_optional_string(cluster_name);
    match (broker_addr, cluster_name) {
        (Some(addr), None) => Ok(BrokerTarget::BrokerAddr(trim_required_cheetah("brokerAddr", addr)?)),
        (None, Some(cluster)) => Ok(BrokerTarget::ClusterName(trim_required_cheetah(
            "clusterName",
            cluster,
        )?)),
        (None, None) => {
            Err(ToolsError::validation_error("target", "either brokerAddr or clusterName must be provided").into())
        }
        (Some(_), Some(_)) => {
            Err(ToolsError::validation_error("target", "brokerAddr and clusterName cannot be provided together").into())
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerOperationFailure {
    pub broker_addr: CheetahString,
    pub error: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerOperationResult {
    pub broker_addrs: Vec<CheetahString>,
    pub failures: Vec<ConsumerOperationFailure>,
    pub warnings: Vec<String>,
}

impl ConsumerOperationResult {
    fn empty() -> Self {
        Self {
            broker_addrs: Vec::new(),
            failures: Vec::new(),
            warnings: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteSubscriptionGroupRequest {
    target: BrokerTarget,
    group_name: CheetahString,
    remove_offset: bool,
    namesrv_addr: Option<String>,
}

impl DeleteSubscriptionGroupRequest {
    pub fn try_new(
        broker_addr: Option<String>,
        cluster_name: Option<String>,
        group_name: impl Into<String>,
        remove_offset: bool,
    ) -> RocketMQResult<Self> {
        Ok(Self {
            target: target_from_options(broker_addr, cluster_name)?,
            group_name: trim_required_cheetah("groupName", group_name)?,
            remove_offset,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn target(&self) -> &BrokerTarget {
        &self.target
    }

    pub fn group_name(&self) -> &CheetahString {
        &self.group_name
    }

    pub fn remove_offset(&self) -> bool {
        self.remove_offset
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        builder_with_namesrv(self.namesrv_addr.as_deref())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerConfigQueryRequest {
    group_name: CheetahString,
    namesrv_addr: Option<String>,
}

impl ConsumerConfigQueryRequest {
    pub fn try_new(group_name: impl Into<String>) -> RocketMQResult<Self> {
        Ok(Self {
            group_name: trim_required_cheetah("groupName", group_name)?,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn group_name(&self) -> &CheetahString {
        &self.group_name
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        builder_with_namesrv(self.namesrv_addr.as_deref())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerConfigEntry {
    pub cluster_name: String,
    pub broker_name: String,
    pub subscription_group_config: SubscriptionGroupConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerConfigQueryResult {
    pub entries: Vec<ConsumerConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetConsumeModeRequest {
    target: BrokerTarget,
    topic_name: CheetahString,
    group_name: CheetahString,
    mode: MessageRequestMode,
    pop_share_queue_num: i32,
    namesrv_addr: Option<String>,
}

impl SetConsumeModeRequest {
    pub fn try_new(
        broker_addr: Option<String>,
        cluster_name: Option<String>,
        topic_name: impl Into<String>,
        group_name: impl Into<String>,
        mode: MessageRequestMode,
        pop_share_queue_num: Option<i32>,
    ) -> RocketMQResult<Self> {
        Ok(Self {
            target: target_from_options(broker_addr, cluster_name)?,
            topic_name: trim_required_cheetah("topicName", topic_name)?,
            group_name: trim_required_cheetah("groupName", group_name)?,
            mode,
            pop_share_queue_num: pop_share_queue_num.unwrap_or(0),
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn topic_name(&self) -> &CheetahString {
        &self.topic_name
    }

    pub fn group_name(&self) -> &CheetahString {
        &self.group_name
    }

    pub fn mode(&self) -> MessageRequestMode {
        self.mode
    }

    pub fn pop_share_queue_num(&self) -> i32 {
        self.pop_share_queue_num
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        builder_with_namesrv(self.namesrv_addr.as_deref())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateSubscriptionGroupRequest {
    target: BrokerTarget,
    config: SubscriptionGroupConfig,
    namesrv_addr: Option<String>,
}

impl UpdateSubscriptionGroupRequest {
    pub fn try_new(
        broker_addr: Option<String>,
        cluster_name: Option<String>,
        config: SubscriptionGroupConfig,
    ) -> RocketMQResult<Self> {
        Ok(Self {
            target: target_from_options(broker_addr, cluster_name)?,
            config,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn target(&self) -> &BrokerTarget {
        &self.target
    }

    pub fn config(&self) -> &SubscriptionGroupConfig {
        &self.config
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        builder_with_namesrv(self.namesrv_addr.as_deref())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateSubscriptionGroupListRequest {
    target: BrokerTarget,
    configs: Vec<SubscriptionGroupConfig>,
    namesrv_addr: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerRunningInfoRequest {
    group_name: CheetahString,
    client_id: Option<CheetahString>,
    broker_addr: Option<CheetahString>,
    jstack: bool,
    namesrv_addr: Option<String>,
}

impl ConsumerRunningInfoRequest {
    pub fn try_new(
        group_name: impl Into<String>,
        client_id: Option<String>,
        broker_addr: Option<String>,
        jstack: bool,
        namesrv_addr: Option<String>,
    ) -> RocketMQResult<Self> {
        Ok(Self {
            group_name: trim_required_cheetah("groupName", group_name)?,
            client_id: trim_optional_string(client_id)
                .map(|client_id| trim_required_cheetah("clientId", client_id))
                .transpose()?,
            broker_addr: trim_optional_string(broker_addr)
                .map(|broker_addr| trim_required_cheetah("brokerAddr", broker_addr))
                .transpose()?,
            jstack,
            namesrv_addr: trim_optional_string(namesrv_addr),
        })
    }

    pub fn group_name(&self) -> &CheetahString {
        &self.group_name
    }

    pub fn client_id(&self) -> Option<&CheetahString> {
        self.client_id.as_ref()
    }

    pub fn broker_addr(&self) -> Option<&CheetahString> {
        self.broker_addr.as_ref()
    }

    pub fn jstack(&self) -> bool {
        self.jstack
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        builder_with_namesrv(self.namesrv_addr.as_deref())
    }
}

#[derive(Debug, Clone)]
pub struct ConsumerRunningInfoItem {
    pub client_id: CheetahString,
    pub version: i32,
    pub running_info: ConsumerRunningInfo,
}

#[derive(Debug, Clone)]
pub struct ConsumerRunningInfoResult {
    pub items: Vec<ConsumerRunningInfoItem>,
    pub subscription_consistent: Option<bool>,
    pub process_queue_analysis: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerProgressRequest {
    consumer_group: Option<CheetahString>,
    topic_name: Option<CheetahString>,
    show_client_ip: bool,
    cluster: Option<CheetahString>,
    namesrv_addr: Option<String>,
}

impl ConsumerProgressRequest {
    pub fn try_new(
        consumer_group: Option<String>,
        topic_name: Option<String>,
        show_client_ip: bool,
        cluster: Option<String>,
        namesrv_addr: Option<String>,
    ) -> RocketMQResult<Self> {
        Ok(Self {
            consumer_group: trim_optional_string(consumer_group)
                .map(|group| trim_required_cheetah("groupName", group))
                .transpose()?,
            topic_name: trim_optional_string(topic_name)
                .map(|topic| trim_required_cheetah("topicName", topic))
                .transpose()?,
            show_client_ip,
            cluster: trim_optional_string(cluster)
                .map(|cluster| trim_required_cheetah("cluster", cluster))
                .transpose()?,
            namesrv_addr: trim_optional_string(namesrv_addr),
        })
    }

    pub fn consumer_group(&self) -> Option<&CheetahString> {
        self.consumer_group.as_ref()
    }

    pub fn show_client_ip(&self) -> bool {
        self.show_client_ip
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        builder_with_namesrv(self.namesrv_addr.as_deref())
    }
}

#[derive(Debug, Clone)]
pub enum ConsumerProgressResult {
    Group(ConsumerGroupProgressResult),
    All(Vec<GroupConsumeInfo>),
}

#[derive(Debug, Clone)]
pub struct ConsumerGroupProgressResult {
    pub rows: Vec<ConsumerProgressRow>,
    pub consume_tps: f64,
    pub diff_total: i64,
    pub inflight_total: i64,
    pub show_client_ip: bool,
}

#[derive(Debug, Clone)]
pub struct ConsumerProgressRow {
    pub topic: CheetahString,
    pub broker_name: CheetahString,
    pub queue_id: i32,
    pub broker_offset: i64,
    pub consumer_offset: i64,
    pub client_ip: Option<String>,
    pub diff: i64,
    pub inflight: i64,
    pub last_timestamp: i64,
}

#[derive(Debug, Clone)]
pub struct GroupConsumeInfo {
    pub group: String,
    pub version: i32,
    pub count: i32,
    pub consume_type: ConsumeType,
    pub message_model: MessageModel,
    pub consume_tps: f64,
    pub diff_total: i64,
}

impl UpdateSubscriptionGroupListRequest {
    pub fn try_new(
        broker_addr: Option<String>,
        cluster_name: Option<String>,
        configs: Vec<SubscriptionGroupConfig>,
    ) -> RocketMQResult<Self> {
        Ok(Self {
            target: target_from_options(broker_addr, cluster_name)?,
            configs,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn configs(&self) -> &[SubscriptionGroupConfig] {
        &self.configs
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        builder_with_namesrv(self.namesrv_addr.as_deref())
    }
}

pub struct ConsumerService;

impl ConsumerService {
    pub async fn delete_subscription_group_by_request_with_rpc_hook(
        request: DeleteSubscriptionGroupRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<ConsumerOperationResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::delete_subscription_group_with_admin(&mut admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn delete_subscription_group_with_admin(
        admin: &mut DefaultMQAdminExt,
        request: &DeleteSubscriptionGroupRequest,
    ) -> RocketMQResult<ConsumerOperationResult> {
        let mut result = ConsumerOperationResult::empty();
        let broker_addrs = resolve_master_targets(admin, request.target()).await?;

        for broker_addr in broker_addrs {
            match admin
                .delete_subscription_group(
                    broker_addr.clone(),
                    request.group_name().clone(),
                    Some(request.remove_offset()),
                )
                .await
            {
                Ok(()) => result.broker_addrs.push(broker_addr),
                Err(error) => result.failures.push(ConsumerOperationFailure {
                    broker_addr,
                    error: error.to_string(),
                }),
            }
        }

        if let BrokerTarget::ClusterName(cluster_name) = request.target() {
            for topic in [
                get_retry_topic(request.group_name().as_str()),
                get_dlq_topic(request.group_name().as_str()),
            ] {
                if let Err(error) = admin.delete_topic(topic.into(), cluster_name.clone()).await {
                    result.warnings.push(error.to_string());
                }
            }
        }

        Ok(result)
    }

    pub async fn query_consumer_config_by_request_with_rpc_hook(
        request: ConsumerConfigQueryRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<ConsumerConfigQueryResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::query_consumer_config_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn query_consumer_config_with_admin(
        admin: &DefaultMQAdminExt,
        request: &ConsumerConfigQueryRequest,
    ) -> RocketMQResult<ConsumerConfigQueryResult> {
        let mut entries = Vec::new();
        let cluster_info = admin.examine_broker_cluster_info().await?;
        let broker_addr_table = cluster_info.broker_addr_table.unwrap_or_default();
        let cluster_addr_table = cluster_info.cluster_addr_table.unwrap_or_default();

        for (broker_name, broker_data) in &broker_addr_table {
            let Some(broker_address) = broker_data.select_broker_addr() else {
                continue;
            };
            if let Ok(config) = admin
                .examine_subscription_group_config(broker_address, request.group_name().clone())
                .await
            {
                entries.push(ConsumerConfigEntry {
                    cluster_name: get_cluster_name(broker_name, &cluster_addr_table).unwrap_or_default(),
                    broker_name: broker_name.to_string(),
                    subscription_group_config: config,
                });
            }
        }
        entries.sort_by(|left, right| {
            left.cluster_name
                .cmp(&right.cluster_name)
                .then_with(|| left.broker_name.cmp(&right.broker_name))
        });
        Ok(ConsumerConfigQueryResult { entries })
    }

    pub async fn set_consume_mode_by_request_with_rpc_hook(
        request: SetConsumeModeRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<ConsumerOperationResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::set_consume_mode_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn set_consume_mode_with_admin(
        admin: &DefaultMQAdminExt,
        request: &SetConsumeModeRequest,
    ) -> RocketMQResult<ConsumerOperationResult> {
        let mut result = ConsumerOperationResult::empty();
        let broker_addrs = resolve_master_targets(admin, &request.target).await?;
        for broker_addr in broker_addrs {
            match admin
                .set_message_request_mode(
                    broker_addr.clone(),
                    request.topic_name().clone(),
                    request.group_name().clone(),
                    request.mode(),
                    request.pop_share_queue_num(),
                    5000,
                )
                .await
            {
                Ok(()) => result.broker_addrs.push(broker_addr),
                Err(error) => result.failures.push(ConsumerOperationFailure {
                    broker_addr,
                    error: error.to_string(),
                }),
            }
        }
        Ok(result)
    }

    pub async fn update_subscription_group_by_request_with_rpc_hook(
        request: UpdateSubscriptionGroupRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<ConsumerOperationResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::update_subscription_group_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn update_subscription_group_with_admin(
        admin: &DefaultMQAdminExt,
        request: &UpdateSubscriptionGroupRequest,
    ) -> RocketMQResult<ConsumerOperationResult> {
        let mut result = ConsumerOperationResult::empty();
        let broker_addrs = resolve_master_targets(admin, request.target()).await?;
        for broker_addr in broker_addrs {
            match admin
                .create_and_update_subscription_group_config(broker_addr.clone(), request.config().clone())
                .await
            {
                Ok(()) => result.broker_addrs.push(broker_addr),
                Err(error) => result.failures.push(ConsumerOperationFailure {
                    broker_addr,
                    error: error.to_string(),
                }),
            }
        }
        Ok(result)
    }

    pub async fn update_subscription_group_list_by_request_with_rpc_hook(
        request: UpdateSubscriptionGroupListRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<ConsumerOperationResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::update_subscription_group_list_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn update_subscription_group_list_with_admin(
        admin: &DefaultMQAdminExt,
        request: &UpdateSubscriptionGroupListRequest,
    ) -> RocketMQResult<ConsumerOperationResult> {
        let mut result = ConsumerOperationResult::empty();
        if request.configs().is_empty() {
            return Ok(result);
        }
        let broker_addrs = resolve_master_targets(admin, &request.target).await?;
        for broker_addr in broker_addrs {
            match admin
                .create_and_update_subscription_group_config_list(broker_addr.clone(), request.configs.clone())
                .await
            {
                Ok(()) => result.broker_addrs.push(broker_addr),
                Err(error) => result.failures.push(ConsumerOperationFailure {
                    broker_addr,
                    error: error.to_string(),
                }),
            }
        }
        Ok(result)
    }

    pub async fn query_consumer_running_info_by_request_with_rpc_hook(
        request: ConsumerRunningInfoRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<ConsumerRunningInfoResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::query_consumer_running_info_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn query_consumer_running_info_with_admin(
        admin: &DefaultMQAdminExt,
        request: &ConsumerRunningInfoRequest,
    ) -> RocketMQResult<ConsumerRunningInfoResult> {
        let consumer_connection = admin
            .examine_consumer_connection_info(request.group_name().clone(), request.broker_addr().cloned())
            .await?;

        let mut items = Vec::new();
        if let Some(client_id) = request.client_id() {
            if let Ok(running_info) = admin
                .get_consumer_running_info(request.group_name().clone(), client_id.clone(), request.jstack(), None)
                .await
            {
                let version = consumer_connection
                    .get_connection_set()
                    .iter()
                    .find(|connection| connection.get_client_id() == *client_id)
                    .map(|connection| connection.get_version())
                    .unwrap_or_default();
                items.push(ConsumerRunningInfoItem {
                    client_id: client_id.clone(),
                    version,
                    running_info,
                });
            }
        } else {
            for connection in consumer_connection.get_connection_set() {
                let client_id = connection.get_client_id();
                if let Ok(running_info) = admin
                    .get_consumer_running_info(request.group_name().clone(), client_id.clone(), request.jstack(), None)
                    .await
                {
                    items.push(ConsumerRunningInfoItem {
                        client_id: client_id.clone(),
                        version: connection.get_version(),
                        running_info,
                    });
                }
            }
        }

        let mut cri_table = BTreeMap::new();
        for item in &items {
            cri_table.insert(item.client_id.to_string(), item.running_info.clone());
        }

        let subscription_consistent = if cri_table.is_empty() {
            None
        } else {
            Some(
                ConsumerRunningInfo::analyze_subscription(cri_table.clone())
                    .await
                    .is_ok(),
            )
        };
        let mut process_queue_analysis = Vec::new();
        if subscription_consistent == Some(true) {
            for (client_id, running_info) in cri_table {
                if let Ok(result) = ConsumerRunningInfo::analyze_process_queue(client_id, running_info).await {
                    if !result.is_empty() {
                        process_queue_analysis.push(result);
                    }
                }
            }
        }

        Ok(ConsumerRunningInfoResult {
            items,
            subscription_consistent,
            process_queue_analysis,
        })
    }

    pub async fn query_consumer_progress_by_request_with_rpc_hook(
        request: ConsumerProgressRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<ConsumerProgressResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::query_consumer_progress_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn query_consumer_progress_with_admin(
        admin: &DefaultMQAdminExt,
        request: &ConsumerProgressRequest,
    ) -> RocketMQResult<ConsumerProgressResult> {
        if let Some(consumer_group) = request.consumer_group() {
            let consume_stats = admin
                .examine_consume_stats(
                    consumer_group.clone(),
                    request.topic_name.clone(),
                    request.cluster.clone(),
                    None,
                    None,
                )
                .await?;
            let allocation = if request.show_client_ip() {
                get_message_queue_allocation_result_with_admin(admin, consumer_group.as_str()).await
            } else {
                HashMap::new()
            };

            let offset_table = consume_stats.get_offset_table();
            let mut mq_list: Vec<_> = offset_table.keys().cloned().collect();
            mq_list.sort();
            let mut rows = Vec::new();
            let mut diff_total = 0;
            let mut inflight_total = 0;
            for mq in mq_list {
                if let Some(offset_wrapper) = offset_table.get(&mq) {
                    let diff = offset_wrapper.get_broker_offset() - offset_wrapper.get_consumer_offset();
                    let inflight = offset_wrapper.get_pull_offset() - offset_wrapper.get_consumer_offset();
                    diff_total += diff;
                    inflight_total += inflight;
                    rows.push(ConsumerProgressRow {
                        topic: mq.topic().clone(),
                        broker_name: mq.broker_name().clone(),
                        queue_id: mq.queue_id(),
                        broker_offset: offset_wrapper.get_broker_offset(),
                        consumer_offset: offset_wrapper.get_consumer_offset(),
                        client_ip: allocation.get(&mq).cloned(),
                        diff,
                        inflight,
                        last_timestamp: offset_wrapper.get_last_timestamp(),
                    });
                }
            }
            Ok(ConsumerProgressResult::Group(ConsumerGroupProgressResult {
                rows,
                consume_tps: consume_stats.get_consume_tps(),
                diff_total,
                inflight_total,
                show_client_ip: request.show_client_ip(),
            }))
        } else {
            let mut results = Vec::new();
            let topic_list = admin.fetch_all_topic_list().await?;
            for topic in topic_list.topic_list {
                if topic.starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX) {
                    let consumer_group = KeyBuilder::parse_group(&topic);
                    let mut group_consume_info = GroupConsumeInfo {
                        group: consumer_group.clone(),
                        version: 0,
                        count: 0,
                        consume_type: ConsumeType::ConsumePassively,
                        message_model: MessageModel::Clustering,
                        consume_tps: 0.0,
                        diff_total: 0,
                    };

                    if let Ok(consume_stats) = admin
                        .examine_consume_stats(consumer_group.clone().into(), None, None, None, None)
                        .await
                    {
                        group_consume_info.consume_tps = consume_stats.get_consume_tps();
                        group_consume_info.diff_total = consume_stats.compute_total_diff();
                    }

                    if let Ok(cc) = admin
                        .examine_consumer_connection_info(consumer_group.into(), None)
                        .await
                    {
                        group_consume_info.count = cc.get_connection_set().len() as i32;
                        group_consume_info.message_model = cc.get_message_model().unwrap_or(MessageModel::Clustering);
                        group_consume_info.consume_type =
                            cc.get_consume_type().unwrap_or(ConsumeType::ConsumePassively);
                        group_consume_info.version = cc.compute_min_version();
                    }
                    results.push(group_consume_info);
                }
            }
            Ok(ConsumerProgressResult::All(results))
        }
    }
}

async fn get_message_queue_allocation_result_with_admin(
    admin: &DefaultMQAdminExt,
    group_name: &str,
) -> HashMap<MessageQueue, String> {
    let mut results = HashMap::new();
    if let Ok(consumer_connection) = admin.examine_consumer_connection_info(group_name.into(), None).await {
        for connection in consumer_connection.get_connection_set() {
            let client_id = connection.get_client_id().clone();
            if let Ok(consumer_running_info) = admin
                .get_consumer_running_info(group_name.into(), client_id.clone(), false, None)
                .await
            {
                for mq in consumer_running_info.mq_table.keys() {
                    results.insert(mq.clone(), client_id.split('@').next().unwrap_or("").to_string());
                }
            }
        }
    }
    results
}

async fn resolve_master_targets(
    admin: &DefaultMQAdminExt,
    target: &BrokerTarget,
) -> RocketMQResult<Vec<CheetahString>> {
    match target {
        BrokerTarget::BrokerAddr(addr) => Ok(vec![addr.clone()]),
        BrokerTarget::ClusterName(cluster_name) => {
            let cluster_info = admin.examine_broker_cluster_info().await?;
            BrokerAddressResolver::fetch_master_addr_by_cluster_name(&cluster_info, cluster_name.as_str())
        }
    }
}

fn get_cluster_name(
    broker_name: &CheetahString,
    cluster_addr_table: &HashMap<CheetahString, HashSet<CheetahString>>,
) -> Option<String> {
    for (cluster_name, broker_name_set) in cluster_addr_table {
        if broker_name_set.contains(broker_name) {
            return Some(cluster_name.to_string());
        }
    }
    None
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delete_subscription_group_request_validates_target_and_group() {
        let request = DeleteSubscriptionGroupRequest::try_new(Some(" 127.0.0.1:10911 ".into()), None, " GroupA ", true)
            .unwrap()
            .with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".into()));

        assert!(matches!(
            request.target(),
            BrokerTarget::BrokerAddr(addr) if addr.as_str() == "127.0.0.1:10911"
        ));
        assert_eq!(request.group_name().as_str(), "GroupA");
        assert!(request.remove_offset());
        assert!(DeleteSubscriptionGroupRequest::try_new(None, None, "GroupA", false).is_err());
    }

    #[test]
    fn set_consume_mode_request_trims_fields() {
        let request = SetConsumeModeRequest::try_new(
            None,
            Some(" DefaultCluster ".into()),
            " TestTopic ",
            " GroupA ",
            MessageRequestMode::Pop,
            Some(8),
        )
        .unwrap();

        assert_eq!(request.topic_name().as_str(), "TestTopic");
        assert_eq!(request.group_name().as_str(), "GroupA");
        assert_eq!(request.mode(), MessageRequestMode::Pop);
        assert_eq!(request.pop_share_queue_num(), 8);
    }

    #[test]
    fn consumer_config_query_request_trims_group() {
        let request = ConsumerConfigQueryRequest::try_new(" GroupA ").unwrap();
        assert_eq!(request.group_name().as_str(), "GroupA");
    }

    #[test]
    fn consumer_running_info_request_trims_fields() {
        let request = ConsumerRunningInfoRequest::try_new(
            " GroupA ",
            Some(" client-a ".into()),
            Some(" 127.0.0.1:10911 ".into()),
            true,
            Some(" 127.0.0.1:9876 ".into()),
        )
        .unwrap();

        assert_eq!(request.group_name().as_str(), "GroupA");
        assert_eq!(request.client_id().unwrap().as_str(), "client-a");
        assert_eq!(request.broker_addr().unwrap().as_str(), "127.0.0.1:10911");
        assert!(request.jstack());
    }

    #[test]
    fn consumer_progress_request_trims_optional_fields() {
        let request = ConsumerProgressRequest::try_new(
            Some(" GroupA ".into()),
            Some(" TopicA ".into()),
            true,
            Some(" DefaultCluster ".into()),
            Some(" 127.0.0.1:9876 ".into()),
        )
        .unwrap();

        assert_eq!(request.consumer_group().unwrap().as_str(), "GroupA");
        assert!(request.show_client_ip());
    }
}
