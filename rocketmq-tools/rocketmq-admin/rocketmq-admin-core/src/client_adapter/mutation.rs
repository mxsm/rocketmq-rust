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

#[path = "mutation/supervised.rs"]
mod supervised;

use std::collections::HashMap;
use std::collections::HashSet;
use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_client_rust::BrokerConfigPatchOutcome as ClientBrokerConfigPatchOutcome;
use rocketmq_client_rust::BrokerMutationConfigState as ClientBrokerMutationConfigState;
use rocketmq_client_rust::MQAdminMutationExt;
use rocketmq_client_rust::MutationExpectedMessageRequestMode as ClientExpectedMessageRequestMode;
use rocketmq_client_rust::MutationExpectedState as ClientExpectedState;
use rocketmq_client_rust::MutationMessageRequestMode as ClientMessageRequestMode;
use rocketmq_client_rust::MutationPersistenceState as ClientMutationPersistenceState;
use rocketmq_client_rust::MutationSubscriptionGroupConfig as ClientSubscriptionGroupConfig;
use rocketmq_client_rust::MutationTopicConfig as ClientTopicConfig;
use rocketmq_client_rust::MutationTopicMessageType as ClientTopicMessageType;
use rocketmq_client_rust::SubscriptionGroupConfigPatch as ClientSubscriptionGroupConfigPatch;
use rocketmq_client_rust::SubscriptionGroupConfigPatchOutcome as ClientSubscriptionGroupConfigPatchOutcome;
use rocketmq_client_rust::TopicConfigPatch as ClientTopicConfigPatch;
use rocketmq_client_rust::TopicConfigPatchOutcome as ClientTopicConfigPatchOutcome;
use rocketmq_client_rust::TopicOffsetMutationFailureCode as ClientTopicOffsetMutationFailureCode;
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
use crate::core::consumer::ConsumerBatchMutationAdmin;
use crate::core::consumer::ConsumerBatchMutationOutcome;
use crate::core::consumer::ConsumerExactBatchDeleteRequest;
use crate::core::consumer::ConsumerExactBatchDeleteTarget;
use crate::core::consumer::ConsumerExactBatchMutationAdmin;
use crate::core::consumer::ConsumerExactBatchUpsertMutationAdmin;
use crate::core::consumer::ConsumerExactBatchUpsertRequest;
use crate::core::consumer::ConsumerExactBatchUpsertTarget;
use crate::core::consumer::ConsumerMutationAdmin;
use crate::core::consumer::DashboardConsumerDeleteRequest;
use crate::core::consumer::DashboardConsumerMutationResult;
use crate::core::consumer::DashboardConsumerUpsertRequest;
use crate::core::consumer::DeleteSubscriptionGroupsRequest;
use crate::core::consumer::SetConsumerRequestModeRequest;
use crate::core::consumer::SetConsumerRequestModeResult;
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
use crate::core::supervised_mutation::*;
use crate::core::topic::DeleteTopicAdminRequest;
use crate::core::topic::DeleteTopicsInBrokerRequest;
use crate::core::topic::PatchTopicConfigOutcome;
use crate::core::topic::PatchTopicConfigRequest;
use crate::core::topic::QueryTopicConfigCasRequest;
use crate::core::topic::ResetTopicConsumerOffsetRequest;
use crate::core::topic::SkipTopicAccumulatedRequest;
use crate::core::topic::TopicBatchDeleteAdmin;
use crate::core::topic::TopicBatchDeleteOutcome;
use crate::core::topic::TopicBatchDeleteRequest;
use crate::core::topic::TopicBatchMutationAdmin;
use crate::core::topic::TopicBatchMutationOutcome;
use crate::core::topic::TopicBatchOrderConfigOutcome;
use crate::core::topic::TopicBatchTargetOutcome;
use crate::core::topic::TopicBatchUpsertRequest;
use crate::core::topic::TopicConfigCasState;
use crate::core::topic::TopicMutationAdmin;
use crate::core::topic::TopicMutationOutcome;
use crate::core::topic::TopicMutationPreflightAdmin;
use crate::core::topic::TopicOffsetMutationAdmin;
use crate::core::topic::TopicOffsetMutationFailureCode;
use crate::core::topic::TopicOffsetMutationOutcome;
use crate::core::topic::TopicOffsetMutationRequest;
use crate::core::topic::TopicOffsetTargetOutcome;
use crate::core::topic::TopicSendRequest;
use crate::core::topic::TopicSendResult;
use crate::core::topic::TopicSkipMutationAdmin;
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
        self.inner.build_and_start().await.map(|inner| MutationAdminSession {
            inner,
            plan_seal: Arc::new(MutationPlanSeal),
        })
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
    plan_seal: Arc<MutationPlanSeal>,
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

    fn ensure_plan_owned(&self, seal: &Arc<MutationPlanSeal>) -> AdminResult<()> {
        ensure_same_plan_seal(&self.plan_seal, seal)
    }
}

fn ensure_same_plan_seal(session_seal: &Arc<MutationPlanSeal>, plan_seal: &Arc<MutationPlanSeal>) -> AdminResult<()> {
    if Arc::ptr_eq(session_seal, plan_seal) {
        Ok(())
    } else {
        Err(AdminError::invalid_argument(
            "plan",
            "was created by a different mutation admin session",
        ))
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

    fn delete_topics_in_broker<'a>(
        &'a mut self,
        request: &'a DeleteTopicsInBrokerRequest,
    ) -> AdminFuture<'a, TopicMutationOutcome> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            self.inner
                .inner
                .remove_topics_from_broker(
                    CheetahString::from(request.broker_addr.as_str()),
                    request
                        .topics
                        .iter()
                        .map(|topic| CheetahString::from(topic.as_str()))
                        .collect(),
                )
                .await
                .map_err(|error| backend_error("remove_topics_from_broker", error))?;
            Ok(TopicMutationOutcome {
                message: format!(
                    "deleted {} topics through one broker batch request",
                    request.topics.len()
                ),
                target_count: 1,
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

impl TopicMutationPreflightAdmin for MutationAdminSession {
    fn query_config_cas_state<'a>(
        &'a mut self,
        request: &'a QueryTopicConfigCasRequest,
    ) -> AdminFuture<'a, TopicConfigCasState> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            let request = QueryTopicConfigCasRequest::try_new(&request.broker_addr, &request.topic)?;
            let snapshot = self
                .inner
                .inner
                .mutation_topic_config_with_version(
                    CheetahString::from(request.broker_addr),
                    CheetahString::from(request.topic),
                )
                .await
                .map_err(|error| backend_error("query_topic_config_cas_state", error))?;
            Ok(TopicConfigCasState {
                version: snapshot.version,
                read_queue_nums: snapshot.config.read_queue_nums,
                write_queue_nums: snapshot.config.write_queue_nums,
                order: snapshot.config.order,
            })
        })
    }
}

impl TopicBatchMutationAdmin for MutationAdminSession {
    fn upsert_topic_batch<'a>(
        &'a mut self,
        request: &'a TopicBatchUpsertRequest,
    ) -> AdminFuture<'a, TopicBatchMutationOutcome> {
        Box::pin(async move { self.upsert_topic_batch_inner(request).await })
    }
}

impl TopicBatchDeleteAdmin for MutationAdminSession {
    fn delete_topic_batch<'a>(
        &'a mut self,
        request: &'a TopicBatchDeleteRequest,
    ) -> AdminFuture<'a, TopicBatchDeleteOutcome> {
        Box::pin(async move { self.delete_topic_batch_inner(request).await })
    }
}

impl TopicSkipMutationAdmin for MutationAdminSession {
    fn skip_accumulated<'a>(
        &'a mut self,
        request: &'a SkipTopicAccumulatedRequest,
    ) -> AdminFuture<'a, TopicMutationOutcome> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            let affected_queues = self
                .inner
                .inner
                .skip_accumulated_message(
                    request.cluster_name().map(CheetahString::from),
                    CheetahString::from(request.topic()),
                    CheetahString::from(request.consumer_group()),
                    request.force(),
                )
                .await
                .map_err(|error| backend_error("skip_accumulated_message", error))?;
            Ok(TopicMutationOutcome {
                message: "Accumulated messages were skipped to the latest offsets.".to_string(),
                target_count: affected_queues,
            })
        })
    }
}

impl TopicOffsetMutationAdmin for MutationAdminSession {
    fn reset_consumer_offset_detailed<'a>(
        &'a mut self,
        request: &'a TopicOffsetMutationRequest,
    ) -> AdminFuture<'a, TopicOffsetMutationOutcome> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            let timestamp = request
                .timestamp()
                .ok_or_else(|| AdminError::invalid_argument("timestamp", "is required for reset"))?;
            let outcome = self
                .inner
                .inner
                .reset_consumer_offset_detailed(
                    CheetahString::from(request.cluster_name()),
                    CheetahString::from(request.topic()),
                    CheetahString::from(request.consumer_group()),
                    timestamp,
                    request.force(),
                )
                .await
                .map_err(|error| backend_error("reset_consumer_offset_detailed", error))?;
            Ok(map_offset_outcome(outcome))
        })
    }

    fn skip_accumulated_detailed<'a>(
        &'a mut self,
        request: &'a TopicOffsetMutationRequest,
    ) -> AdminFuture<'a, TopicOffsetMutationOutcome> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            if request.timestamp().is_some() {
                return Err(AdminError::invalid_argument("timestamp", "must be absent for skip"));
            }
            let outcome = self
                .inner
                .inner
                .skip_accumulated_message_detailed(
                    CheetahString::from(request.cluster_name()),
                    CheetahString::from(request.topic()),
                    CheetahString::from(request.consumer_group()),
                    request.force(),
                )
                .await
                .map_err(|error| backend_error("skip_accumulated_message_detailed", error))?;
            Ok(map_offset_outcome(outcome))
        })
    }
}

impl MutationAdminSession {
    async fn upsert_topic_batch_inner(
        &mut self,
        request: &TopicBatchUpsertRequest,
    ) -> AdminResult<TopicBatchMutationOutcome> {
        self.inner.ensure_open()?;
        let request = request.canonical_for_execution()?;
        let cluster_info = self
            .inner
            .inner
            .mutation_cluster_info()
            .await
            .map_err(|error| backend_error("mutation_cluster_info", error))?;
        let mut attributes = HashMap::new();
        attributes.insert(
            CheetahString::from(format!("+{MESSAGE_TYPE_ATTRIBUTE}")),
            CheetahString::from(normalize_message_type(request.message_type.as_deref())),
        );
        let topic_config = TopicConfig {
            topic_name: Some(CheetahString::from(request.topic.as_str())),
            read_queue_nums: request.read_queue_nums,
            write_queue_nums: request.write_queue_nums,
            perm: request.perm,
            order: request.order,
            attributes,
            ..TopicConfig::default()
        };
        let mut targets = Vec::with_capacity(request.broker_names.len());
        for broker_name in &request.broker_names {
            let result = match find_master_addr_by_broker_name(&cluster_info, broker_name) {
                Some(address) => self
                    .inner
                    .inner
                    .upsert_topic_config(address, topic_config.clone())
                    .await
                    .map_err(|error| backend_error("upsert_topic_config", error)),
                None => Err(AdminError::invalid_argument(
                    "brokerNames",
                    format!("broker `{broker_name}` has no reachable master"),
                )),
            };
            targets.push(match result {
                Ok(()) => TopicBatchTargetOutcome {
                    broker_name: broker_name.clone(),
                    success: true,
                    message: "Topic configuration saved".to_string(),
                },
                Err(error) => TopicBatchTargetOutcome {
                    broker_name: broker_name.clone(),
                    success: false,
                    message: crate::core::stable_error_message(&error),
                },
            });
        }
        let successful_brokers = targets
            .iter()
            .filter(|target| target.success)
            .map(|target| target.broker_name.clone())
            .collect::<Vec<_>>();
        let order_config = if successful_brokers.is_empty() {
            None
        } else {
            let result = if request.order {
                self.inner
                    .inner
                    .upsert_order_topic_config(
                        CheetahString::from(request.topic.as_str()),
                        CheetahString::from(build_order_conf(
                            &successful_brokers.iter().cloned().collect(),
                            request.write_queue_nums,
                        )),
                        true,
                    )
                    .await
                    .map_err(|error| backend_error("upsert_order_topic_config", error))
            } else {
                self.inner
                    .inner
                    .delete_order_topic_config(CheetahString::from(request.topic.as_str()))
                    .await
                    .map_err(|error| backend_error("delete_order_topic_config", error))
            };
            Some(match result {
                Ok(()) => TopicBatchOrderConfigOutcome {
                    success: true,
                    message: "Order topic configuration reconciled".to_string(),
                },
                Err(error) => TopicBatchOrderConfigOutcome {
                    success: false,
                    message: crate::core::stable_error_message(&error),
                },
            })
        };
        Ok(TopicBatchMutationOutcome { targets, order_config })
    }

    async fn delete_topic_batch_inner(
        &mut self,
        request: &TopicBatchDeleteRequest,
    ) -> AdminResult<TopicBatchDeleteOutcome> {
        self.inner.ensure_open()?;
        let request = request.canonical_for_execution()?;
        let mut targets = Vec::with_capacity(request.cluster_names().len());
        for cluster_name in request.cluster_names() {
            let result = self
                .inner
                .inner
                .remove_topic(
                    CheetahString::from(request.topic()),
                    CheetahString::from(cluster_name.as_str()),
                )
                .await
                .map_err(|error| backend_error("remove_topic", error));
            targets.push(match result {
                Ok(()) => TopicBatchTargetOutcome {
                    broker_name: cluster_name.clone(),
                    success: true,
                    message: "Topic deleted from cluster".to_string(),
                },
                Err(error) => TopicBatchTargetOutcome {
                    broker_name: cluster_name.clone(),
                    success: false,
                    message: crate::core::stable_error_message(&error),
                },
            });
        }
        let order_config = if targets.iter().all(|target| target.success) {
            let route_check = self
                .inner
                .inner
                .mutation_topic_route(CheetahString::from(request.topic()))
                .await
                .map_err(|error| backend_error("mutation_topic_route_after_delete", error));
            Some(match route_check {
                Ok(route) if topic_route_is_absent(route.as_ref()) => {
                    self.delete_order_config_after_route_absent(request.topic()).await
                }
                Ok(_) => TopicBatchOrderConfigOutcome {
                    success: false,
                    message: "authoritative Topic route still contains targets; order configuration was retained"
                        .to_string(),
                },
                Err(error) => TopicBatchOrderConfigOutcome {
                    success: false,
                    message: crate::core::stable_error_message(&error),
                },
            })
        } else {
            None
        };
        Ok(TopicBatchDeleteOutcome { targets, order_config })
    }

    async fn delete_order_config_after_route_absent(&self, topic: &str) -> TopicBatchOrderConfigOutcome {
        match self
            .inner
            .inner
            .delete_order_topic_config(CheetahString::from(topic))
            .await
            .map_err(|error| backend_error("delete_order_topic_config", error))
        {
            Ok(()) => TopicBatchOrderConfigOutcome {
                success: true,
                message: "Order topic configuration deleted".to_string(),
            },
            Err(error) => TopicBatchOrderConfigOutcome {
                success: false,
                message: crate::core::stable_error_message(&error),
            },
        }
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

    fn delete_subscription_groups<'a>(
        &'a mut self,
        request: &'a DeleteSubscriptionGroupsRequest,
    ) -> AdminFuture<'a, ConsumerBatchMutationOutcome> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            self.inner
                .inner
                .remove_subscription_groups(
                    CheetahString::from(request.broker_addr.as_str()),
                    request
                        .group_names
                        .iter()
                        .map(|group_name| CheetahString::from(group_name.as_str()))
                        .collect(),
                    request.clean_offset,
                )
                .await
                .map_err(|error| backend_error("remove_subscription_groups", error))?;
            Ok(ConsumerBatchMutationOutcome {
                message: format!(
                    "deleted {} subscription groups through one broker batch request",
                    request.group_names.len()
                ),
                broker_count: 1,
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

impl ConsumerBatchMutationAdmin for MutationAdminSession {
    fn upsert_consumer_group_batch<'a>(
        &'a mut self,
        request: &'a consumer::ConsumerBatchUpsertRequest,
    ) -> AdminFuture<'a, consumer::DashboardConsumerBatchResult> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            let request = consumer::ConsumerBatchUpsertRequest::try_new(request.inner().clone())?;
            let cluster_info = self
                .inner
                .inner
                .mutation_cluster_info()
                .await
                .map_err(|error| backend_error("mutation_cluster_info", error))?;
            let targets = resolve_consumer_target_broker_names(
                &cluster_info,
                &request.inner().cluster_name_list,
                &request.inner().broker_name_list,
            )?;
            let inner = request.inner();
            let config = consumer_subscription_group_config(inner);

            let mut outcomes = Vec::with_capacity(targets.len());
            for target in targets {
                let result = match find_master_addr_by_broker_name(&cluster_info, &target) {
                    Some(address) => self
                        .inner
                        .inner
                        .upsert_subscription_group(address, config.clone())
                        .await
                        .map_err(|error| backend_error("upsert_subscription_group", error)),
                    None => Err(AdminError::invalid_argument(
                        "brokerNameList",
                        format!("Broker `{target}` does not have a reachable master address."),
                    )),
                };
                outcomes.push(consumer_batch_target_outcome(
                    target,
                    "BROKER",
                    "Consumer group updated.",
                    result,
                ));
            }
            let success = outcomes.iter().all(|outcome| outcome.success);
            Ok(consumer::DashboardConsumerBatchResult {
                consumer_group: inner.consumer_group.clone(),
                success,
                targets: outcomes,
            })
        })
    }

    fn delete_consumer_group_batch<'a>(
        &'a mut self,
        request: &'a consumer::ConsumerBatchDeleteRequest,
    ) -> AdminFuture<'a, consumer::DashboardConsumerBatchResult> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            let request = consumer::ConsumerBatchDeleteRequest::try_new(
                request.consumer_group(),
                request.selected_broker_names().to_vec(),
                request.all_broker_names().to_vec(),
            )?;
            let cluster_info = self
                .inner
                .inner
                .mutation_cluster_info()
                .await
                .map_err(|error| backend_error("mutation_cluster_info", error))?;
            let mut authoritative_addresses = HashSet::with_capacity(request.all_broker_names().len());
            for target in request.all_broker_names() {
                let address = find_master_addr_by_broker_name(&cluster_info, target).ok_or_else(|| {
                    AdminError::invalid_argument(
                        "allBrokerNames",
                        format!("Broker `{target}` does not have a reachable master address."),
                    )
                })?;
                authoritative_addresses.insert(address);
            }

            let mut outcomes = Vec::with_capacity(request.selected_broker_names().len() + 2);
            for target in request.selected_broker_names() {
                let result = match find_master_addr_by_broker_name(&cluster_info, target) {
                    Some(address) => self
                        .inner
                        .inner
                        .remove_subscription_group(address, CheetahString::from(request.consumer_group()), Some(true))
                        .await
                        .map_err(|error| backend_error("remove_subscription_group", error)),
                    None => Err(AdminError::invalid_argument(
                        "selectedBrokerNames",
                        format!("Broker `{target}` does not have a reachable master address."),
                    )),
                };
                outcomes.push(consumer_batch_target_outcome(
                    target.clone(),
                    "BROKER",
                    "Consumer group deleted.",
                    result,
                ));
            }

            let all_brokers_succeeded = outcomes.iter().all(|outcome| outcome.success);
            let all_real_targets_selected = request.selected_broker_names() == request.all_broker_names();
            if all_real_targets_selected && all_brokers_succeeded {
                let nameservers = self
                    .inner
                    .inner
                    .mutation_name_server_addresses()
                    .await
                    .map_err(|error| backend_error("mutation_name_server_addresses", error))?
                    .into_iter()
                    .collect::<HashSet<_>>();
                for topic in consumer_internal_topics(request.consumer_group()) {
                    let broker_result = self
                        .inner
                        .inner
                        .remove_topic_from_brokers(authoritative_addresses.clone(), CheetahString::from(&topic))
                        .await
                        .map_err(|error| backend_error("remove_topic_from_brokers", error));
                    let result = match broker_result {
                        Ok(()) => self
                            .inner
                            .inner
                            .remove_topic_from_name_servers(
                                nameservers.clone(),
                                None,
                                CheetahString::from(topic.as_str()),
                            )
                            .await
                            .map_err(|error| backend_error("remove_topic_from_name_servers", error)),
                        Err(error) => Err(error),
                    };
                    outcomes.push(consumer_batch_target_outcome(
                        topic,
                        "INTERNAL_TOPIC_CLEANUP",
                        "Internal consumer topic deleted.",
                        result,
                    ));
                }
            }
            let success = outcomes.iter().all(|outcome| outcome.success);
            Ok(consumer::DashboardConsumerBatchResult {
                consumer_group: request.consumer_group().to_owned(),
                success,
                targets: outcomes,
            })
        })
    }
}

impl ConsumerExactBatchUpsertMutationAdmin for MutationAdminSession {
    fn upsert_consumer_group_exact_batch<'a>(
        &'a mut self,
        request: &'a ConsumerExactBatchUpsertRequest,
    ) -> AdminFuture<'a, consumer::DashboardConsumerBatchResult> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            let request =
                ConsumerExactBatchUpsertRequest::try_new(request.inner().clone(), request.targets().iter().cloned())?;
            let cluster_info = self
                .inner
                .inner
                .mutation_cluster_info()
                .await
                .map_err(|error| backend_error("mutation_cluster_info", error))?;
            validate_exact_upsert_targets(&cluster_info, request.targets())?;

            let inner = request.inner();
            let config = consumer_subscription_group_config(inner);
            let mut outcomes = Vec::with_capacity(request.targets().len());
            for target in request.targets() {
                let result = self
                    .inner
                    .inner
                    .upsert_subscription_group(CheetahString::from(target.broker_address()), config.clone())
                    .await
                    .map_err(|error| backend_error("upsert_subscription_group", error));
                outcomes.push(consumer_batch_target_outcome(
                    exact_target_label(target),
                    "BROKER",
                    "Consumer group updated.",
                    result,
                ));
            }
            let success = outcomes.iter().all(|outcome| outcome.success);
            Ok(consumer::DashboardConsumerBatchResult {
                consumer_group: inner.consumer_group.clone(),
                success,
                targets: outcomes,
            })
        })
    }
}

fn consumer_subscription_group_config(inner: &DashboardConsumerUpsertRequest) -> SubscriptionGroupConfig {
    let mut config = SubscriptionGroupConfig::default();
    config.set_group_name(CheetahString::from(inner.consumer_group.as_str()));
    config.set_consume_enable(inner.consume_enable);
    config.set_consume_from_min_enable(inner.consume_from_min_enable);
    config.set_consume_broadcast_enable(inner.consume_broadcast_enable);
    config.set_consume_message_orderly(inner.consume_message_orderly);
    config.set_retry_queue_nums(inner.retry_queue_nums);
    config.set_retry_max_times(inner.retry_max_times);
    config.set_broker_id(inner.broker_id);
    config.set_which_broker_when_consume_slowly(inner.which_broker_when_consume_slowly);
    config.set_notify_consumer_ids_changed_enable(inner.notify_consumer_ids_changed_enable);
    config.set_group_sys_flag(inner.group_sys_flag);
    config.set_consume_timeout_minute(inner.consume_timeout_minute);
    config
}

impl ConsumerExactBatchMutationAdmin for MutationAdminSession {
    fn delete_consumer_group_exact_batch<'a>(
        &'a mut self,
        request: &'a ConsumerExactBatchDeleteRequest,
    ) -> AdminFuture<'a, consumer::DashboardConsumerBatchResult> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            let request = ConsumerExactBatchDeleteRequest::try_new(
                request.consumer_group(),
                request.selected_targets().iter().cloned(),
                request.authoritative_targets().iter().cloned(),
            )?;
            let cluster_info = self
                .inner
                .inner
                .mutation_cluster_info()
                .await
                .map_err(|error| backend_error("mutation_cluster_info", error))?;
            validate_exact_delete_targets(&cluster_info, request.authoritative_targets())?;

            let mut outcomes = Vec::with_capacity(request.selected_targets().len() + 2);
            for target in request.selected_targets() {
                let result = self
                    .inner
                    .inner
                    .remove_subscription_group(
                        CheetahString::from(target.broker_address()),
                        CheetahString::from(request.consumer_group()),
                        Some(true),
                    )
                    .await
                    .map_err(|error| backend_error("remove_subscription_group", error));
                outcomes.push(consumer_batch_target_outcome(
                    exact_target_label(target),
                    "BROKER",
                    "Consumer group deleted.",
                    result,
                ));
            }

            let all_brokers_succeeded = outcomes.iter().all(|outcome| outcome.success);
            let all_real_targets_selected = request.selected_targets() == request.authoritative_targets();
            if all_real_targets_selected && all_brokers_succeeded {
                let nameservers = self
                    .inner
                    .inner
                    .mutation_name_server_addresses()
                    .await
                    .map_err(|error| backend_error("mutation_name_server_addresses", error))?
                    .into_iter()
                    .collect::<HashSet<_>>();
                let authoritative_addresses = request
                    .authoritative_targets()
                    .iter()
                    .map(|target| CheetahString::from(target.broker_address()))
                    .collect::<HashSet<_>>();
                for topic in consumer_internal_topics(request.consumer_group()) {
                    let broker_result = self
                        .inner
                        .inner
                        .remove_topic_from_brokers(authoritative_addresses.clone(), CheetahString::from(&topic))
                        .await
                        .map_err(|error| backend_error("remove_topic_from_brokers", error));
                    let result = match broker_result {
                        Ok(()) => self
                            .inner
                            .inner
                            .remove_topic_from_name_servers(
                                nameservers.clone(),
                                None,
                                CheetahString::from(topic.as_str()),
                            )
                            .await
                            .map_err(|error| backend_error("remove_topic_from_name_servers", error)),
                        Err(error) => Err(error),
                    };
                    outcomes.push(consumer_batch_target_outcome(
                        topic,
                        "INTERNAL_TOPIC_CLEANUP",
                        "Internal consumer topic deleted.",
                        result,
                    ));
                }
            }
            let success = outcomes.iter().all(|outcome| outcome.success);
            Ok(consumer::DashboardConsumerBatchResult {
                consumer_group: request.consumer_group().to_owned(),
                success,
                targets: outcomes,
            })
        })
    }
}

fn validate_exact_delete_targets(
    cluster_info: &ClusterInfo,
    confirmed: &[ConsumerExactBatchDeleteTarget],
) -> AdminResult<()> {
    validate_exact_targets(cluster_info, confirmed, "validate_consumer_delete_targets", "delete")
}

fn validate_exact_upsert_targets(
    cluster_info: &ClusterInfo,
    confirmed: &[ConsumerExactBatchUpsertTarget],
) -> AdminResult<()> {
    validate_exact_targets(
        cluster_info,
        confirmed,
        "validate_consumer_upsert_targets",
        "create-or-update",
    )
}

fn validate_exact_targets(
    cluster_info: &ClusterInfo,
    confirmed: &[ConsumerExactBatchDeleteTarget],
    operation: &'static str,
    action: &'static str,
) -> AdminResult<()> {
    let mut current = Vec::with_capacity(confirmed.len());
    for target in confirmed {
        let belongs_to_cluster = cluster_info
            .cluster_addr_table
            .as_ref()
            .and_then(|clusters| clusters.get(target.cluster_name()))
            .is_some_and(|brokers| brokers.iter().any(|broker| broker.as_str() == target.broker_name()));
        if !belongs_to_cluster {
            return Err(exact_target_drift(
                operation,
                format!(
                    "Broker `{}` is no longer in confirmed cluster `{}`.",
                    target.broker_name(),
                    target.cluster_name()
                ),
            ));
        }
        let address = find_master_addr_by_broker_name(cluster_info, target.broker_name()).ok_or_else(|| {
            exact_target_drift(
                operation,
                format!(
                    "Broker `{}` no longer has the confirmed master target.",
                    target.broker_name()
                ),
            )
        })?;
        current.push(ConsumerExactBatchDeleteTarget::try_new(
            target.cluster_name(),
            target.broker_name(),
            address.to_string(),
        )?);
    }
    current.sort();
    if current != confirmed {
        return Err(exact_target_drift(
            operation,
            format!("The broker address set changed after confirmation; no {action} was attempted."),
        ));
    }
    Ok(())
}

fn exact_target_drift(operation: &'static str, reason: impl Into<String>) -> AdminError {
    AdminError::backend_view(operation, "TARGET_DRIFT", reason, None, 409, false)
}

fn exact_target_label(target: &ConsumerExactBatchDeleteTarget) -> String {
    format!(
        "{}/{}/{}",
        target.cluster_name(),
        target.broker_name(),
        target.broker_address()
    )
}

fn consumer_batch_target_outcome(
    target: String,
    kind: &str,
    success_message: &str,
    result: AdminResult<()>,
) -> consumer::DashboardConsumerTargetOutcome {
    match result {
        Ok(()) => consumer::DashboardConsumerTargetOutcome {
            target,
            kind: kind.to_owned(),
            success: true,
            message: success_message.to_owned(),
        },
        Err(error) => consumer::DashboardConsumerTargetOutcome {
            target,
            kind: kind.to_owned(),
            success: false,
            message: crate::core::stable_error_message(&error),
        },
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

async fn require_topic_route<A: MQAdminMutationExt + ?Sized>(
    admin: &A,
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
    if broker_names.is_empty() {
        return Err(AdminError::backend(
            "mutation_cluster_info",
            "selected cluster has no broker members",
        ));
    }
    let mut targets = Vec::with_capacity(broker_names.len());
    let mut endpoints = HashSet::with_capacity(broker_names.len());
    for broker_name in broker_names {
        let broker = broker_table.get(broker_name).ok_or_else(|| {
            AdminError::backend(
                "mutation_cluster_info",
                "selected cluster membership references a missing broker",
            )
        })?;
        if broker.broker_name() != broker_name || broker.cluster() != cluster_name {
            return Err(AdminError::backend(
                "mutation_cluster_info",
                "selected cluster broker identity is inconsistent",
            ));
        }
        let master_addr = broker
            .broker_addrs()
            .get(&MASTER_ID)
            .ok_or_else(|| AdminError::backend("mutation_cluster_info", "selected cluster broker has no master"))?;
        if !endpoints.insert(master_addr.clone()) {
            return Err(AdminError::backend(
                "mutation_cluster_info",
                "selected cluster has duplicate master endpoints",
            ));
        }
        targets.push((broker_name.to_string(), master_addr.clone()));
    }
    targets.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(targets)
}

/// Creates the isolated client runtime used by mutation-only applications.
///
/// This constructor keeps telemetry and the concrete client dependency behind
/// the mutation adapter boundary.
///
/// # Errors
///
/// Returns an administration error when the bounded client runtime cannot be
/// initialized from the supplied lifecycle context.
pub fn create_mutation_client_runtime(
    service_context: rocketmq_runtime::ChildServiceContext,
) -> AdminResult<Arc<ClientRuntime>> {
    ClientRuntime::try_new(
        service_context,
        ClientRuntimeConfig::default(),
        rocketmq_observability::TelemetryHandle::noop(),
    )
    .map_err(|_| AdminError::backend("mutation_client_runtime", "client runtime initialization failed"))
}

fn select_metadata_targets(
    all_targets: Vec<(String, CheetahString)>,
    selected_broker_names: Option<&[String]>,
) -> AdminResult<Vec<(String, CheetahString)>> {
    let Some(selected_broker_names) = selected_broker_names else {
        return Ok(all_targets);
    };
    if selected_broker_names.is_empty() || selected_broker_names.len() > MAX_METADATA_MUTATION_TARGETS {
        return Err(AdminError::invalid_argument(
            "brokerNames",
            format!("must contain between 1 and {MAX_METADATA_MUTATION_TARGETS} broker names"),
        ));
    }
    let selected = selected_broker_names.iter().collect::<HashSet<_>>();
    if selected.len() != selected_broker_names.len() {
        return Err(AdminError::invalid_argument(
            "brokerNames",
            "must not contain duplicates",
        ));
    }
    let mut targets = all_targets
        .into_iter()
        .filter(|(broker_name, _)| selected.contains(broker_name))
        .collect::<Vec<_>>();
    if targets.len() != selected.len() {
        return Err(AdminError::invalid_argument(
            "brokerNames",
            "contains a broker outside the selected cluster master topology",
        ));
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

fn topic_route_is_absent(route: Option<&TopicRouteData>) -> bool {
    route.is_none_or(|route| route.broker_datas.is_empty() && route.queue_datas.is_empty())
}

fn map_offset_outcome(outcome: rocketmq_client_rust::TopicOffsetMutationOutcome) -> TopicOffsetMutationOutcome {
    TopicOffsetMutationOutcome {
        targets: outcome
            .targets
            .into_iter()
            .map(|target| TopicOffsetTargetOutcome {
                broker_name: target.broker_name,
                queue_id: target.queue_id,
                applied: target.applied,
                failure: target.failure.map(|failure| match failure {
                    ClientTopicOffsetMutationFailureCode::InvalidData => TopicOffsetMutationFailureCode::InvalidData,
                    ClientTopicOffsetMutationFailureCode::Unavailable => TopicOffsetMutationFailureCode::Unavailable,
                }),
                retryable: target.retryable,
            })
            .collect(),
    }
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
                + TopicOffsetMutationAdmin
                + ConsumerMutationAdmin
                + ConsumerExactBatchMutationAdmin
                + ConsumerExactBatchUpsertMutationAdmin
                + MessageMutationAdmin
                + BrokerMutationAdmin,
        >() {
        }

        assert_mutation_contracts::<MutationAdminSession>();
    }

    #[test]
    fn exact_consumer_delete_rejects_address_drift_before_mutation_loop() {
        let cluster_info = exact_delete_cluster("10.0.0.2:10911");
        let confirmed = [
            ConsumerExactBatchDeleteTarget::try_new("cluster-a", "broker-a", "10.0.0.1:10911").expect("valid target"),
        ];

        let error = validate_exact_delete_targets(&cluster_info, &confirmed).expect_err("address drift");
        assert_eq!(error.code(), Some("TARGET_DRIFT"));
        assert_eq!(error.http_status(), Some(409));
    }

    #[test]
    fn exact_consumer_delete_accepts_only_same_cluster_broker_and_address() {
        let cluster_info = exact_delete_cluster("10.0.0.1:10911");
        let confirmed = [
            ConsumerExactBatchDeleteTarget::try_new("cluster-a", "broker-a", "10.0.0.1:10911").expect("valid target"),
        ];

        validate_exact_delete_targets(&cluster_info, &confirmed).expect("unchanged identity");
    }

    #[test]
    fn exact_consumer_upsert_rejects_address_drift_before_mutation_loop() {
        let cluster_info = exact_delete_cluster("10.0.0.2:10911");
        let confirmed = [
            ConsumerExactBatchUpsertTarget::try_new("cluster-a", "broker-a", "10.0.0.1:10911").expect("valid target"),
        ];

        let error = validate_exact_upsert_targets(&cluster_info, &confirmed).expect_err("address drift");
        assert_eq!(error.code(), Some("TARGET_DRIFT"));
        assert_eq!(error.http_status(), Some(409));
    }

    fn exact_delete_cluster(address: &str) -> ClusterInfo {
        let broker = rocketmq_protocol::protocol::route::route_data_view::BrokerData::new(
            "cluster-a".into(),
            "broker-a".into(),
            HashMap::from([(MASTER_ID, CheetahString::from(address))]),
            None,
        );
        ClusterInfo::new(
            Some(HashMap::from([(CheetahString::from("broker-a"), broker)])),
            Some(HashMap::from([(
                CheetahString::from("cluster-a"),
                HashSet::from([CheetahString::from("broker-a")]),
            )])),
        )
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

    #[test]
    fn order_config_cleanup_requires_the_authoritative_route_to_be_empty() {
        assert!(topic_route_is_absent(None));
        assert!(topic_route_is_absent(Some(&TopicRouteData::default())));
        let mut route = TopicRouteData::default();
        route.queue_datas.push(Default::default());
        assert!(!topic_route_is_absent(Some(&route)));
    }

    #[test]
    fn detailed_offset_mapping_retains_applied_and_failed_targets_without_backend_text() {
        let outcome = map_offset_outcome(rocketmq_client_rust::TopicOffsetMutationOutcome {
            targets: vec![
                rocketmq_client_rust::TopicOffsetMutationTargetOutcome {
                    broker_name: "broker-a".into(),
                    queue_id: Some(0),
                    applied: true,
                    offset: Some(42),
                    failure: None,
                    retryable: false,
                },
                rocketmq_client_rust::TopicOffsetMutationTargetOutcome {
                    broker_name: "broker-a".into(),
                    queue_id: Some(1),
                    applied: false,
                    offset: None,
                    failure: Some(ClientTopicOffsetMutationFailureCode::Unavailable),
                    retryable: true,
                },
            ],
        });

        assert_eq!(outcome.targets.len(), 2);
        assert!(outcome.targets[0].applied);
        assert_eq!(outcome.targets[0].queue_id, Some(0));
        assert!(!outcome.targets[1].applied);
        assert_eq!(outcome.targets[1].queue_id, Some(1));
        assert_eq!(
            outcome.targets[1].failure,
            Some(TopicOffsetMutationFailureCode::Unavailable)
        );
        assert!(outcome.targets[1].retryable);
    }

    #[test]
    fn supervised_plan_seal_accepts_clones_and_rejects_other_sessions() {
        let session = Arc::new(MutationPlanSeal);
        let cloned_plan = Arc::clone(&session);
        let other_session = Arc::new(MutationPlanSeal);

        assert!(ensure_same_plan_seal(&session, &cloned_plan).is_ok());
        assert!(ensure_same_plan_seal(&session, &other_session).is_err());
    }
}
