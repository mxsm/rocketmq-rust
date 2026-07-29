// Copyright 2026 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Mutation-only public adapter over the explicit client mutation capability.

use std::collections::HashMap;
use std::collections::HashSet;
use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_client_rust::BrokerConfigPatchOutcome as ClientBrokerConfigPatchOutcome;
use rocketmq_client_rust::MQAdminMutationExt;
use rocketmq_client_rust::SubscriptionGroupConfigPatch as ClientSubscriptionGroupConfigPatch;
use rocketmq_client_rust::SubscriptionGroupConfigPatchOutcome as ClientSubscriptionGroupConfigPatchOutcome;
use rocketmq_client_rust::TopicConfigPatch as ClientTopicConfigPatch;
use rocketmq_client_rust::TopicConfigPatchOutcome as ClientTopicConfigPatchOutcome;
use rocketmq_error::RocketMQError;
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_model::topic::TopicConfig;
use rocketmq_model::topic::DLQ_GROUP_TOPIC_PREFIX;
use rocketmq_model::topic::RETRY_GROUP_TOPIC_PREFIX;
use rocketmq_protocol::common::wire_constants::MASTER_ID;
use rocketmq_protocol::protocol::body::broker_body::cluster_info::ClusterInfo;
use rocketmq_protocol::protocol::body::cm_result::CMResult;
use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_protocol::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;

use crate::client_adapter::producer::*;
use crate::core::broker::BrokerMutationAdmin;
use crate::core::broker::PatchBrokerConfigOutcome;
use crate::core::broker::PatchBrokerConfigRequest;
use crate::core::broker::QueryBrokerConfigGenerationRequest;
use crate::core::broker::QueryBrokerConfigGenerationResult;
use crate::core::broker::RestoreBrokerLogFilterRequest;
use crate::core::broker::SetBrokerLogFilterTtlRequest;
use crate::core::clock::Clock;
use crate::core::consumer;
use crate::core::consumer::ConsumerMutationAdmin;
use crate::core::consumer::DashboardConsumerDeleteRequest;
use crate::core::consumer::DashboardConsumerMutationResult;
use crate::core::consumer::DashboardConsumerUpsertRequest;
use crate::core::consumer::SetConsumerRequestModeRequest;
use crate::core::consumer::SetConsumerRequestModeResult;
use crate::core::dashboard::DashboardMutationAdmin;
use crate::core::message::DirectConsumeRequest;
use crate::core::message::DirectConsumeResult;
use crate::core::message::DlqMessageLookupRequest;
use crate::core::message::DlqResendResult;
use crate::core::message::MessageMutationAdmin;
use crate::core::proxy::ProxyDrainOperationRequest;
use crate::core::proxy::ProxyDrainPending;
use crate::core::proxy::ProxyDrainState;
use crate::core::proxy::ProxyMutationAdmin;
use crate::core::security::AdminCredentials;
use crate::core::topic::DeleteTopicAdminRequest;
use crate::core::topic::PatchTopicConfigOutcome;
use crate::core::topic::PatchTopicConfigRequest;
use crate::core::topic::ResetTopicConsumerOffsetRequest;
use crate::core::topic::TopicMutationAdmin;
use crate::core::topic::TopicMutationOutcome;
use crate::core::topic::TopicSendRequest;
use crate::core::topic::TopicSendResult;
use crate::core::topic::UpsertTopicRequest;
use crate::core::AdminError;
use crate::core::AdminFuture;
use crate::core::AdminResult;

pub use rocketmq_client_rust::ClientRuntime;
pub use rocketmq_client_rust::ClientRuntimeConfig;

const MESSAGE_TYPE_ATTRIBUTE: &str = "message.type";
const PROPERTY_RETRY_TOPIC: &str = "RETRY_TOPIC";
const PROPERTY_ORIGIN_MESSAGE_ID: &str = "ORIGIN_MESSAGE_ID";
const PROPERTY_DLQ_ORIGIN_MESSAGE_ID: &str = "DLQ_ORIGIN_MESSAGE_ID";

/// Builds a session whose public contract contains mutation capabilities only.
#[derive(Clone)]
pub struct MutationAdminBuilder {
    inner: crate::client_adapter::lifecycle::AdminBuilder,
}

impl MutationAdminBuilder {
    pub fn new(client_runtime: Arc<ClientRuntime>) -> Self {
        Self {
            inner: crate::client_adapter::lifecycle::AdminBuilder::new(client_runtime),
        }
    }

    pub fn namesrv_addr(mut self, addr: impl Into<String>) -> Self {
        self.inner = self.inner.namesrv_addr(addr);
        self
    }

    pub fn admin_group(mut self, group: impl Into<String>) -> Self {
        self.inner = self.inner.admin_group(group);
        self
    }

    pub fn instance_name(mut self, name: impl Into<String>) -> Self {
        self.inner = self.inner.instance_name(name);
        self
    }

    pub fn timeout_millis(mut self, timeout_millis: u64) -> Self {
        self.inner = self.inner.timeout_millis(timeout_millis);
        self
    }

    pub fn unit_name(mut self, name: impl Into<String>) -> Self {
        self.inner = self.inner.unit_name(name);
        self
    }

    pub fn vip_channel_enabled(mut self, enabled: bool) -> Self {
        self.inner = self.inner.vip_channel_enabled(enabled);
        self
    }

    pub fn use_tls(mut self, use_tls: bool) -> Self {
        self.inner = self.inner.use_tls(use_tls);
        self
    }

    pub fn clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.inner = self.inner.clock(clock);
        self
    }

    /// Configures request signing for the isolated mutation identity.
    pub fn credentials(mut self, credentials: AdminCredentials) -> Self {
        self.inner = self.inner.credentials(credentials);
        self
    }

    pub async fn build_and_start(self) -> AdminResult<MutationAdminSession> {
        self.inner
            .build_and_start()
            .await
            .map(|inner| MutationAdminSession { inner })
    }

    pub async fn build_with_guard(self) -> AdminResult<MutationAdminGuard> {
        self.build_and_start().await.map(MutationAdminGuard::new)
    }
}

impl std::fmt::Debug for MutationAdminBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MutationAdminBuilder")
            .field("inner", &self.inner)
            .finish()
    }
}

/// Live session that deliberately implements no query administration trait.
#[must_use = "a started mutation admin session must be explicitly shut down"]
pub struct MutationAdminSession {
    inner: crate::client_adapter::lifecycle::AdminSession,
}

impl MutationAdminSession {
    pub async fn shutdown(&mut self) {
        self.inner.shutdown().await;
    }

    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    pub fn client_runtime(&self) -> Arc<ClientRuntime> {
        self.inner.client_runtime()
    }
}

impl TopicMutationAdmin for MutationAdminSession {
    fn patch_config_if_version<'a>(
        &'a mut self,
        request: &'a PatchTopicConfigRequest,
    ) -> AdminFuture<'a, PatchTopicConfigOutcome> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            let request = PatchTopicConfigRequest::try_new(
                &request.broker_addr,
                &request.topic,
                request.expected_version,
                request.patch,
            )?;
            let outcome = self
                .inner
                .inner
                .patch_topic_config_if_version(
                    CheetahString::from(request.broker_addr),
                    CheetahString::from(request.topic),
                    request.expected_version,
                    ClientTopicConfigPatch {
                        read_queue_nums: request.patch.read_queue_nums,
                        write_queue_nums: request.patch.write_queue_nums,
                        order: request.patch.order,
                    },
                )
                .await
                .map_err(|error| backend_error("patch_topic_config_if_version", error))?;
            Ok(match outcome {
                ClientTopicConfigPatchOutcome::Applied {
                    previous_version,
                    version,
                } => PatchTopicConfigOutcome::Applied {
                    previous_version,
                    version,
                },
                ClientTopicConfigPatchOutcome::VersionConflict {
                    expected_version,
                    actual_version,
                } => PatchTopicConfigOutcome::VersionConflict {
                    expected_version,
                    actual_version,
                },
            })
        })
    }

    fn upsert_topic<'a>(&'a mut self, request: &'a UpsertTopicRequest) -> AdminFuture<'a, TopicMutationOutcome> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            let topic = require_non_empty("topic", &request.topic)?;
            if request.cluster_names.is_empty() && request.broker_names.is_empty() {
                return Err(AdminError::invalid_argument(
                    "targets",
                    "select at least one cluster or broker",
                ));
            }

            let cluster_info = self
                .inner
                .inner
                .mutation_cluster_info()
                .await
                .map_err(|error| backend_error("mutation_cluster_info", error))?;
            let mut target_addrs = HashSet::new();
            let mut target_broker_names = HashSet::new();
            for cluster_name in &request.cluster_names {
                for (broker_name, broker_addr) in master_targets_by_cluster_name(&cluster_info, cluster_name)? {
                    target_broker_names.insert(broker_name);
                    target_addrs.insert(broker_addr);
                }
            }
            for broker_name in &request.broker_names {
                let address = find_master_addr_by_broker_name(&cluster_info, broker_name).ok_or_else(|| {
                    AdminError::invalid_argument(
                        "brokerName",
                        format!("broker `{broker_name}` was not found in the current cluster view"),
                    )
                })?;
                target_broker_names.insert(broker_name.clone());
                target_addrs.insert(address);
            }
            if target_addrs.is_empty() {
                return Err(AdminError::invalid_argument(
                    "targets",
                    "no writable broker target could be resolved",
                ));
            }

            let mut attributes = HashMap::new();
            attributes.insert(
                CheetahString::from(format!("+{MESSAGE_TYPE_ATTRIBUTE}")),
                CheetahString::from(normalize_message_type(request.message_type.as_deref())),
            );
            let topic_config = TopicConfig {
                topic_name: Some(CheetahString::from(topic)),
                read_queue_nums: request.read_queue_nums.max(1),
                write_queue_nums: request.write_queue_nums.max(1),
                perm: request.perm,
                order: request.order,
                attributes,
                ..TopicConfig::default()
            };
            for broker_addr in &target_addrs {
                self.inner
                    .inner
                    .upsert_topic_config(broker_addr.clone(), topic_config.clone())
                    .await
                    .map_err(|error| backend_error("upsert_topic_config", error))?;
            }
            if request.order {
                self.inner
                    .inner
                    .upsert_order_topic_config(
                        CheetahString::from(topic),
                        CheetahString::from(build_order_conf(&target_broker_names, topic_config.write_queue_nums)),
                        true,
                    )
                    .await
                    .map_err(|error| backend_error("upsert_order_topic_config", error))?;
            }

            Ok(TopicMutationOutcome {
                message: format!("Topic `{topic}` was saved successfully."),
                target_count: target_addrs.len(),
            })
        })
    }

    fn delete_topic<'a>(&'a mut self, request: &'a DeleteTopicAdminRequest) -> AdminFuture<'a, TopicMutationOutcome> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            let topic = require_non_empty("topic", &request.topic)?;
            if let Some(broker_name) = request.broker_name.as_deref() {
                let cluster_info = self
                    .inner
                    .inner
                    .mutation_cluster_info()
                    .await
                    .map_err(|error| backend_error("mutation_cluster_info", error))?;
                let broker_addr = find_master_addr_by_broker_name(&cluster_info, broker_name).ok_or_else(|| {
                    AdminError::invalid_argument(
                        "brokerName",
                        format!("broker `{broker_name}` was not found in the current cluster view"),
                    )
                })?;
                self.inner
                    .inner
                    .remove_topic_from_brokers(HashSet::from([broker_addr]), CheetahString::from(topic))
                    .await
                    .map_err(|error| backend_error("remove_topic_from_brokers", error))?;
                return Ok(TopicMutationOutcome {
                    message: format!("Topic `{topic}` was deleted from broker `{broker_name}`."),
                    target_count: 1,
                });
            }

            let clusters = if let Some(cluster_name) = request.cluster_name.as_ref() {
                vec![cluster_name.clone()]
            } else {
                let route = require_topic_route(&self.inner.inner, topic).await?;
                let mut clusters = route
                    .broker_datas
                    .iter()
                    .map(|broker| broker.cluster().to_string())
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>();
                clusters.sort();
                clusters
            };
            if clusters.is_empty() {
                return Err(AdminError::invalid_argument(
                    "topic",
                    format!("topic `{topic}` has no cluster mapping to delete"),
                ));
            }
            for cluster_name in &clusters {
                self.inner
                    .inner
                    .remove_topic(CheetahString::from(topic), CheetahString::from(cluster_name.as_str()))
                    .await
                    .map_err(|error| backend_error("remove_topic", error))?;
            }
            Ok(TopicMutationOutcome {
                message: format!("Topic `{topic}` was deleted from {} cluster(s).", clusters.len()),
                target_count: clusters.len(),
            })
        })
    }

    fn reset_topic_consumer_offset<'a>(
        &'a mut self,
        request: &'a ResetTopicConsumerOffsetRequest,
    ) -> AdminFuture<'a, TopicMutationOutcome> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            let offsets = self
                .inner
                .inner
                .reset_consumer_offset(
                    None,
                    require_non_empty("topic", &request.topic)?.into(),
                    require_non_empty("consumerGroup", &request.consumer_group)?.into(),
                    request.reset_timestamp,
                    request.force,
                )
                .await;
            let affected_queues = match offsets {
                Ok(offsets) => offsets.len(),
                Err(error) if is_consumer_not_online_error(&error) => self
                    .inner
                    .inner
                    .reset_consumer_offset_legacy(
                        None,
                        request.consumer_group.as_str().into(),
                        request.topic.as_str().into(),
                        request.reset_timestamp,
                        request.force,
                    )
                    .await
                    .map_err(|error| backend_error("reset_consumer_offset_legacy", error))?
                    .len(),
                Err(error) => return Err(backend_error("reset_consumer_offset", error)),
            };
            Ok(TopicMutationOutcome {
                message: format!(
                    "Consumer group `{}` offset was reset for {affected_queues} queue(s).",
                    request.consumer_group
                ),
                target_count: affected_queues,
            })
        })
    }

    fn send_topic_test_message<'a>(&'a mut self, request: &'a TopicSendRequest) -> AdminFuture<'a, TopicSendResult> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            require_non_empty("topic", &request.topic)?;
            if request.message_body.is_empty() {
                return Err(AdminError::invalid_argument("messageBody", "must not be empty"));
            }
            let route = require_topic_route(&self.inner.inner, &request.topic).await?;
            let master_addr = route
                .broker_datas
                .iter()
                .find_map(|broker| broker.broker_addrs().get(&MASTER_ID).cloned())
                .ok_or_else(|| {
                    AdminError::invalid_argument("topic", format!("topic `{}` has no online broker", request.topic))
                })?;
            let config = self
                .inner
                .inner
                .mutation_topic_config(master_addr, request.topic.as_str().into())
                .await
                .map_err(|error| backend_error("mutation_topic_config", error))?;
            let transactional = config.get_topic_message_type().to_string() == "TRANSACTION";
            let producer_group = unique_producer_group(self.inner.clock.now_millis(), transactional);
            let client_config = self.inner.inner.client_config().clone_client_config();
            if transactional {
                send_transaction_message(self.client_runtime(), client_config, producer_group, request).await
            } else {
                send_normal_message(self.client_runtime(), client_config, producer_group, request).await
            }
        })
    }
}

impl ConsumerMutationAdmin for MutationAdminSession {
    fn patch_config_if_version<'a>(
        &'a mut self,
        request: &'a consumer::PatchSubscriptionGroupConfigRequest,
    ) -> AdminFuture<'a, consumer::PatchSubscriptionGroupConfigOutcome> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            let request = consumer::PatchSubscriptionGroupConfigRequest::try_new(
                &request.broker_addr,
                &request.group,
                request.expected_version,
                request.patch,
            )?;
            let outcome = self
                .inner
                .inner
                .patch_subscription_group_config_if_version(
                    CheetahString::from(request.broker_addr),
                    CheetahString::from(request.group),
                    request.expected_version,
                    ClientSubscriptionGroupConfigPatch {
                        retry_max_times: request.patch.retry_max_times,
                        retry_queue_nums: request.patch.retry_queue_nums,
                        consume_timeout_minutes: request.patch.consume_timeout_minutes,
                    },
                )
                .await
                .map_err(|error| backend_error("patch_subscription_group_config_if_version", error))?;
            Ok(match outcome {
                ClientSubscriptionGroupConfigPatchOutcome::Applied {
                    previous_version,
                    version,
                } => consumer::PatchSubscriptionGroupConfigOutcome::Applied {
                    previous_version,
                    version,
                },
                ClientSubscriptionGroupConfigPatchOutcome::VersionConflict {
                    expected_version,
                    actual_version,
                } => consumer::PatchSubscriptionGroupConfigOutcome::VersionConflict {
                    expected_version,
                    actual_version,
                },
            })
        })
    }

    fn upsert_dashboard_consumer_group<'a>(
        &'a mut self,
        request: &'a DashboardConsumerUpsertRequest,
    ) -> AdminFuture<'a, DashboardConsumerMutationResult> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            let group = normalize_consumer_group(&request.consumer_group)?;
            validate_consumer_limits(request)?;
            let cluster_info = self
                .inner
                .inner
                .mutation_cluster_info()
                .await
                .map_err(|error| backend_error("mutation_cluster_info", error))?;
            let broker_names = resolve_consumer_target_broker_names(
                &cluster_info,
                &request.cluster_name_list,
                &request.broker_name_list,
            )?;
            let mut config = SubscriptionGroupConfig::default();
            config.set_group_name(group.as_str().into());
            config.set_consume_enable(request.consume_enable);
            config.set_consume_from_min_enable(request.consume_from_min_enable);
            config.set_consume_broadcast_enable(request.consume_broadcast_enable);
            config.set_consume_message_orderly(request.consume_message_orderly);
            config.set_retry_queue_nums(request.retry_queue_nums);
            config.set_retry_max_times(request.retry_max_times);
            config.set_broker_id(request.broker_id);
            config.set_which_broker_when_consume_slowly(request.which_broker_when_consume_slowly);
            config.set_notify_consumer_ids_changed_enable(request.notify_consumer_ids_changed_enable);
            config.set_group_sys_flag(request.group_sys_flag);
            config.set_consume_timeout_minute(request.consume_timeout_minute);
            for broker_name in &broker_names {
                let broker_addr = find_master_addr_by_broker_name(&cluster_info, broker_name).ok_or_else(|| {
                    AdminError::invalid_argument(
                        "brokerNameList",
                        format!("Broker `{broker_name}` does not have a reachable master address."),
                    )
                })?;
                self.inner
                    .inner
                    .upsert_subscription_group(broker_addr, config.clone())
                    .await
                    .map_err(|error| backend_error("upsert_subscription_group", error))?;
            }
            Ok(DashboardConsumerMutationResult {
                consumer_group: group,
                broker_names,
                updated: true,
            })
        })
    }

    fn delete_dashboard_consumer_group<'a>(
        &'a mut self,
        request: &'a DashboardConsumerDeleteRequest,
    ) -> AdminFuture<'a, DashboardConsumerMutationResult> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            let group = normalize_consumer_group(&request.consumer_group)?;
            let mut broker_names = request
                .broker_name_list
                .iter()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .collect::<Vec<_>>();
            broker_names.sort();
            broker_names.dedup();
            if broker_names.is_empty() {
                return Err(AdminError::invalid_argument(
                    "brokerNameList",
                    "Select at least one broker before deleting the consumer group.",
                ));
            }
            let cluster_info = self
                .inner
                .inner
                .mutation_cluster_info()
                .await
                .map_err(|error| backend_error("mutation_cluster_info", error))?;
            for broker_name in &broker_names {
                let broker_addr = find_master_addr_by_broker_name(&cluster_info, broker_name).ok_or_else(|| {
                    AdminError::invalid_argument(
                        "brokerNameList",
                        format!("Broker `{broker_name}` does not have a reachable master address."),
                    )
                })?;
                self.inner
                    .inner
                    .remove_subscription_group(broker_addr.clone(), group.as_str().into(), Some(true))
                    .await
                    .map_err(|error| backend_error("remove_subscription_group", error))?;
                for topic in consumer_internal_topics(&group) {
                    self.inner
                        .inner
                        .remove_topic_from_brokers(HashSet::from([broker_addr.clone()]), CheetahString::from(topic))
                        .await
                        .map_err(|error| backend_error("remove_topic_from_brokers", error))?;
                }
            }
            Ok(DashboardConsumerMutationResult {
                consumer_group: group,
                broker_names,
                updated: false,
            })
        })
    }

    fn set_consumer_request_mode<'a>(
        &'a mut self,
        request: &'a SetConsumerRequestModeRequest,
    ) -> AdminFuture<'a, SetConsumerRequestModeResult> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            let route = require_topic_route(&self.inner.inner, require_non_empty("topic", &request.topic)?).await?;
            let mode = match request.mode {
                consumer::ConsumerRequestMode::Pull => {
                    rocketmq_model::common::message::message_enum::MessageRequestMode::Pull
                }
                consumer::ConsumerRequestMode::Pop => {
                    rocketmq_model::common::message::message_enum::MessageRequestMode::Pop
                }
            };
            let mut broker_addrs = route
                .broker_datas
                .into_iter()
                .flat_map(|broker| broker.broker_addrs().values().cloned().collect::<Vec<_>>())
                .collect::<HashSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            broker_addrs.sort();
            for broker_addr in &broker_addrs {
                self.inner
                    .inner
                    .configure_message_request_mode(
                        broker_addr.clone(),
                        request.topic.as_str().into(),
                        require_non_empty("consumerGroup", &request.consumer_group)?.into(),
                        mode,
                        request.pop_share_queue_num,
                        request.timeout_millis,
                    )
                    .await
                    .map_err(|error| backend_error("configure_message_request_mode", error))?;
            }
            Ok(SetConsumerRequestModeResult {
                broker_addrs: broker_addrs.into_iter().map(|addr| addr.to_string()).collect(),
            })
        })
    }
}

impl MessageMutationAdmin for MutationAdminSession {
    fn consume_message_directly<'a>(
        &'a mut self,
        request: &'a DirectConsumeRequest,
    ) -> AdminFuture<'a, DirectConsumeResult> {
        Box::pin(async move { consume_directly(self, request).await })
    }

    fn resend_dlq_message<'a>(&'a mut self, request: &'a DlqMessageLookupRequest) -> AdminFuture<'a, DlqResendResult> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            let group = require_non_empty("consumerGroup", &request.consumer_group)?;
            let dlq_topic = if group.starts_with(DLQ_GROUP_TOPIC_PREFIX) {
                group.to_string()
            } else {
                format!("{DLQ_GROUP_TOPIC_PREFIX}{group}")
            };
            let message = self
                .inner
                .inner
                .view_message_for_mutation(
                    dlq_topic.into(),
                    require_non_empty("messageId", &request.message_id)?.into(),
                )
                .await
                .map_err(|error| backend_error("view_message_for_mutation", error))?;
            let topic = message_property(&message, PROPERTY_RETRY_TOPIC).ok_or_else(|| {
                AdminError::invalid_argument(
                    "message",
                    "DLQ message is missing `RETRY_TOPIC`, so it cannot be resent safely.",
                )
            })?;
            let message_id = message_property(&message, PROPERTY_ORIGIN_MESSAGE_ID)
                .or_else(|| message_property(&message, PROPERTY_DLQ_ORIGIN_MESSAGE_ID))
                .ok_or_else(|| {
                    AdminError::invalid_argument(
                        "message",
                        "DLQ message is missing `ORIGIN_MESSAGE_ID`, so it cannot be resent safely.",
                    )
                })?;
            let consume = consume_directly(
                self,
                &DirectConsumeRequest {
                    topic: topic.clone(),
                    consumer_group: group.to_string(),
                    message_id: message_id.clone(),
                    client_id: None,
                },
            )
            .await?;
            Ok(DlqResendResult {
                topic,
                message_id,
                consume,
            })
        })
    }
}

impl BrokerMutationAdmin for MutationAdminSession {
    fn query_config_generation<'a>(
        &'a mut self,
        request: &'a QueryBrokerConfigGenerationRequest,
    ) -> AdminFuture<'a, QueryBrokerConfigGenerationResult> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            let broker_addr = require_non_empty("brokerAddr", &request.broker_addr)?;
            let generation = self
                .inner
                .inner
                .broker_config_generation(CheetahString::from(broker_addr))
                .await
                .map_err(|error| backend_error("broker_config_generation", error))?;
            Ok(QueryBrokerConfigGenerationResult { generation })
        })
    }

    fn patch_config_if_generation<'a>(
        &'a mut self,
        request: &'a PatchBrokerConfigRequest,
    ) -> AdminFuture<'a, PatchBrokerConfigOutcome> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            let broker_addr = require_non_empty("brokerAddr", &request.broker_addr)?;
            if request.expected_generation == 0 {
                return Err(AdminError::invalid_argument(
                    "expectedGeneration",
                    "must be greater than zero",
                ));
            }
            if request.properties.is_empty() {
                return Err(AdminError::invalid_argument("properties", "must not be empty"));
            }
            let mut properties = HashMap::with_capacity(request.properties.len());
            for (key, value) in &request.properties {
                let key = require_non_empty("propertyKey", key)?;
                let value = require_non_empty("propertyValue", value)?;
                properties.insert(CheetahString::from(key), CheetahString::from(value));
            }

            let outcome = self
                .inner
                .inner
                .patch_broker_config_if_generation(
                    CheetahString::from(broker_addr),
                    request.expected_generation,
                    properties,
                )
                .await
                .map_err(|error| backend_error("patch_broker_config_if_generation", error))?;
            Ok(match outcome {
                ClientBrokerConfigPatchOutcome::Applied {
                    previous_generation,
                    generation,
                } => PatchBrokerConfigOutcome::Applied {
                    previous_generation,
                    generation,
                },
                ClientBrokerConfigPatchOutcome::GenerationConflict {
                    expected_generation,
                    actual_generation,
                } => PatchBrokerConfigOutcome::GenerationConflict {
                    expected_generation,
                    actual_generation,
                },
            })
        })
    }

    fn set_log_filter_ttl<'a>(&'a mut self, request: &'a SetBrokerLogFilterTtlRequest) -> AdminFuture<'a, ()> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            let request = SetBrokerLogFilterTtlRequest::try_new(
                request.broker_addr.clone(),
                request.logger.clone(),
                request.level,
                request.ttl_seconds,
                request.operation_id.clone(),
            )?;
            self.inner
                .inner
                .set_broker_log_filter_ttl(
                    CheetahString::from(request.broker_addr.as_str()),
                    CheetahString::from(request.logger.as_str()),
                    CheetahString::from(request.level.as_uppercase()),
                    request.ttl_seconds,
                    CheetahString::from(request.operation_id.as_str()),
                )
                .await
                .map_err(|error| backend_error("set_log_filter_ttl", error))
        })
    }

    fn restore_log_filter<'a>(&'a mut self, request: &'a RestoreBrokerLogFilterRequest) -> AdminFuture<'a, ()> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            let request =
                RestoreBrokerLogFilterRequest::try_new(request.broker_addr.clone(), request.operation_id.clone())?;
            self.inner
                .inner
                .restore_broker_log_filter(
                    CheetahString::from(request.broker_addr.as_str()),
                    CheetahString::from(request.operation_id.as_str()),
                )
                .await
                .map_err(|error| backend_error("restore_log_filter", error))
        })
    }
}

impl ProxyMutationAdmin for MutationAdminSession {
    fn begin_drain<'a>(&'a mut self, request: &'a ProxyDrainOperationRequest) -> AdminFuture<'a, ProxyDrainState> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            let proxy_addr = require_non_empty("proxyAddr", &request.proxy_addr)?;
            let operation_id = require_non_empty("operationId", &request.operation_id)?;
            let state = self
                .inner
                .inner
                .begin_proxy_drain(CheetahString::from(proxy_addr), CheetahString::from(operation_id))
                .await
                .map_err(|error| backend_error("begin_proxy_drain", error))?;
            map_proxy_drain_state(state)
        })
    }

    fn cancel_drain<'a>(&'a mut self, request: &'a ProxyDrainOperationRequest) -> AdminFuture<'a, ProxyDrainState> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            let proxy_addr = require_non_empty("proxyAddr", &request.proxy_addr)?;
            let operation_id = require_non_empty("operationId", &request.operation_id)?;
            let state = self
                .inner
                .inner
                .cancel_proxy_drain(CheetahString::from(proxy_addr), CheetahString::from(operation_id))
                .await
                .map_err(|error| backend_error("cancel_proxy_drain", error))?;
            map_proxy_drain_state(state)
        })
    }
}

impl DashboardMutationAdmin for MutationAdminSession {}

async fn require_topic_route(
    admin: &rocketmq_client_rust::DefaultMQAdminExt,
    topic: &str,
) -> Result<TopicRouteData, AdminError> {
    admin
        .mutation_topic_route(CheetahString::from(topic))
        .await
        .map_err(|error| backend_error("mutation_topic_route", error))?
        .ok_or_else(|| AdminError::invalid_argument("topic", format!("topic `{topic}` was not found")))
}

fn normalize_message_type(message_type: Option<&str>) -> String {
    match message_type.unwrap_or("NORMAL").trim().to_uppercase().as_str() {
        "FIFO" => "FIFO".to_string(),
        "DELAY" => "DELAY".to_string(),
        "TRANSACTION" => "TRANSACTION".to_string(),
        "UNSPECIFIED" => "UNSPECIFIED".to_string(),
        _ => "NORMAL".to_string(),
    }
}

fn find_master_addr_by_broker_name(cluster_info: &ClusterInfo, broker_name: &str) -> Option<CheetahString> {
    cluster_info
        .broker_addr_table
        .as_ref()
        .and_then(|table| table.get(broker_name))
        .and_then(|broker| broker.broker_addrs().get(&MASTER_ID).cloned())
}

fn master_targets_by_cluster_name(
    cluster_info: &ClusterInfo,
    cluster_name: &str,
) -> Result<Vec<(String, CheetahString)>, AdminError> {
    let cluster_table = cluster_info
        .cluster_addr_table
        .as_ref()
        .ok_or_else(|| AdminError::backend("mutation_cluster_info", "missing cluster address data"))?;
    let broker_table = cluster_info
        .broker_addr_table
        .as_ref()
        .ok_or_else(|| AdminError::backend("mutation_cluster_info", "missing broker address data"))?;
    let broker_names = cluster_table.get(cluster_name).ok_or_else(|| {
        AdminError::invalid_argument(
            "clusterName",
            format!("cluster `{cluster_name}` was not found in the current NameServer view"),
        )
    })?;
    let mut targets = Vec::new();
    for broker_name in broker_names {
        if let Some(master_addr) = broker_table
            .get(broker_name)
            .and_then(|broker| broker.broker_addrs().get(&MASTER_ID))
        {
            targets.push((broker_name.to_string(), master_addr.clone()));
        }
    }
    targets.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(targets)
}

fn build_order_conf(broker_names: &HashSet<String>, write_queue_nums: u32) -> String {
    let mut broker_names = broker_names.iter().cloned().collect::<Vec<_>>();
    broker_names.sort();
    broker_names
        .into_iter()
        .map(|broker_name| format!("{broker_name}:{write_queue_nums}"))
        .collect::<Vec<_>>()
        .join(";")
}

fn normalize_consumer_group(value: &str) -> AdminResult<String> {
    let value = value.strip_prefix("%SYS%").unwrap_or(value).trim();
    if value.is_empty() {
        Err(AdminError::invalid_argument(
            "consumerGroup",
            "Consumer group is required.",
        ))
    } else {
        Ok(value.to_string())
    }
}

fn validate_consumer_limits(request: &DashboardConsumerUpsertRequest) -> AdminResult<()> {
    if request.retry_queue_nums < 0 {
        return Err(AdminError::invalid_argument(
            "retryQueueNums",
            "Retry queues must be zero or greater.",
        ));
    }
    if request.retry_max_times < -1 {
        return Err(AdminError::invalid_argument(
            "retryMaxTimes",
            "Max retries must be -1 or greater.",
        ));
    }
    if request.consume_timeout_minute <= 0 {
        return Err(AdminError::invalid_argument(
            "consumeTimeoutMinute",
            "Consume timeout must be greater than zero.",
        ));
    }
    Ok(())
}

fn resolve_consumer_target_broker_names(
    cluster_info: &ClusterInfo,
    cluster_names: &[String],
    broker_names: &[String],
) -> AdminResult<Vec<String>> {
    let mut targets = HashSet::new();
    if let Some(cluster_table) = cluster_info.cluster_addr_table.as_ref() {
        for cluster_name in cluster_names {
            let cluster_name = cluster_name.trim();
            if cluster_name.is_empty() {
                continue;
            }
            let brokers = cluster_table.get(cluster_name).ok_or_else(|| {
                AdminError::invalid_argument(
                    "clusterNameList",
                    format!("Cluster `{cluster_name}` was not found in the current cluster view."),
                )
            })?;
            targets.extend(brokers.iter().map(ToString::to_string));
        }
    }
    for broker_name in broker_names {
        let broker_name = broker_name.trim();
        if broker_name.is_empty() {
            continue;
        }
        if find_master_addr_by_broker_name(cluster_info, broker_name).is_none() {
            return Err(AdminError::invalid_argument(
                "brokerNameList",
                format!("Broker `{broker_name}` was not found in the current cluster view."),
            ));
        }
        targets.insert(broker_name.to_string());
    }
    if targets.is_empty() {
        return Err(AdminError::invalid_argument(
            "brokerNameList",
            "Select at least one cluster or broker before saving the consumer group.",
        ));
    }
    let mut values = targets.into_iter().collect::<Vec<_>>();
    values.sort();
    Ok(values)
}

fn consumer_internal_topics(group: &str) -> [String; 2] {
    [
        format!("{RETRY_GROUP_TOPIC_PREFIX}{group}"),
        format!("{DLQ_GROUP_TOPIC_PREFIX}{group}"),
    ]
}

async fn consume_directly(
    session: &mut MutationAdminSession,
    request: &DirectConsumeRequest,
) -> Result<DirectConsumeResult, AdminError> {
    session.inner.ensure_open()?;
    let result = session
        .inner
        .inner
        .consume_directly(
            require_non_empty("consumerGroup", &request.consumer_group)?.into(),
            request.client_id.as_deref().unwrap_or_default().into(),
            require_non_empty("topic", &request.topic)?.into(),
            require_non_empty("messageId", &request.message_id)?.into(),
        )
        .await
        .map_err(|error| backend_error("consume_directly", error))?;
    Ok(DirectConsumeResult {
        success: matches!(result.consume_result(), Some(value) if *value == CMResult::CRSuccess),
        consume_result: result.consume_result().map(ToString::to_string),
        remark: result
            .remark()
            .map(ToString::to_string)
            .and_then(|value| non_empty(&value)),
    })
}

fn message_property(message: &MessageExt, key: &str) -> Option<String> {
    message
        .properties()
        .get(key)
        .map(ToString::to_string)
        .and_then(|value| non_empty(&value))
}

fn require_non_empty<'a>(field: &'static str, value: &'a str) -> Result<&'a str, AdminError> {
    let value = value.trim();
    if value.is_empty() {
        Err(AdminError::invalid_argument(field, "must not be empty"))
    } else {
        Ok(value)
    }
}

fn non_empty(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_string())
}

fn backend_error(operation: &'static str, error: RocketMQError) -> AdminError {
    let view = error.boundary_view();
    let context = (!view.context().is_empty()).then(|| view.context().to_string());
    AdminError::backend_view(
        operation,
        view.code().as_str(),
        view.message(),
        context,
        view.http().status.as_u16(),
        view.is_retryable(),
    )
}

fn map_proxy_drain_state(
    state: rocketmq_protocol::protocol::body::proxy_drain::ProxyDrainStateResponseBody,
) -> AdminResult<ProxyDrainState> {
    ProxyDrainState::try_from_wire_parts(
        state.schema_version,
        state.phase.as_str(),
        state.operation_id,
        state.admission_open,
        state.routing_open,
        state.readiness_published,
        state.zero_pending,
        ProxyDrainPending {
            active_connections: state.pending.active_connections,
            sessions: state.pending.sessions,
            receipt_handles: state.pending.receipt_handles,
            prepared_transactions: state.pending.prepared_transactions,
            telemetry_links: state.pending.telemetry_links,
            remoting_channels: state.pending.remoting_channels,
            telemetry_commands: state.pending.telemetry_commands,
            rpc_in_flight: state.pending.rpc_in_flight,
        },
    )
}

/// Owns a mutation session and guarantees explicit async shutdown on the
/// successful workflow path.
#[must_use = "the guard owns a live mutation admin session; call shutdown when complete"]
pub struct MutationAdminGuard {
    session: Option<MutationAdminSession>,
}

impl MutationAdminGuard {
    fn new(session: MutationAdminSession) -> Self {
        Self { session: Some(session) }
    }

    pub async fn shutdown(mut self) {
        if let Some(mut session) = self.session.take() {
            session.shutdown().await;
        }
    }

    /// Returns the live mutation session.
    ///
    /// # Panics
    ///
    /// Panics only after this guard has been consumed by [`Self::shutdown`].
    pub fn inner(&self) -> &MutationAdminSession {
        self.session.as_ref().expect("MutationAdminGuard already consumed")
    }

    /// Returns the live mutation session mutably.
    ///
    /// # Panics
    ///
    /// Panics only after this guard has been consumed by [`Self::shutdown`].
    pub fn inner_mut(&mut self) -> &mut MutationAdminSession {
        self.session.as_mut().expect("MutationAdminGuard already consumed")
    }
}

impl Deref for MutationAdminGuard {
    type Target = MutationAdminSession;

    fn deref(&self) -> &Self::Target {
        self.inner()
    }
}

impl DerefMut for MutationAdminGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner_mut()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mutation_session_exposes_all_mutation_contracts() {
        fn assert_mutation_contracts<
            T: TopicMutationAdmin
                + ConsumerMutationAdmin
                + MessageMutationAdmin
                + BrokerMutationAdmin
                + DashboardMutationAdmin,
        >() {
        }

        assert_mutation_contracts::<MutationAdminSession>();
    }

    #[test]
    fn topic_cas_patch_mapping_is_closed() {
        let patch = crate::core::topic::TopicConfigCasPatch {
            read_queue_nums: Some(4),
            write_queue_nums: Some(6),
            order: Some(true),
        };
        let client = ClientTopicConfigPatch {
            read_queue_nums: patch.read_queue_nums,
            write_queue_nums: patch.write_queue_nums,
            order: patch.order,
        };
        assert_eq!(client.read_queue_nums, Some(4));
        assert_eq!(client.write_queue_nums, Some(6));
        assert_eq!(client.order, Some(true));
    }
}
