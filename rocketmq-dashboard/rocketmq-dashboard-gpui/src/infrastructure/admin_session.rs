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

//! Narrow real Admin session seam used only by the GPUI provider.

use std::{future::Future, pin::Pin, sync::Arc};

#[cfg(test)]
use rocketmq_admin_core::core::dashboard::{DashboardConsumerList, DashboardTopicStats};
use rocketmq_admin_core::{
    client_adapter::{AdminBuilder, AdminSession, ClientRuntime},
    core::{
        AdminResult,
        broker::{
            BrokerConfigPatchResult, BrokerMutationAdmin, PatchBrokerConfigRequest, QueryBrokerConfigGenerationRequest,
            QueryBrokerConfigGenerationResult,
        },
        consumer::{
            ConsumerExactBatchDeleteRequest, ConsumerExactBatchMutationAdmin, ConsumerExactBatchUpsertMutationAdmin,
            ConsumerExactBatchUpsertRequest, ConsumerMutationAdmin, DashboardConsumerBatchResult,
            DashboardConsumerRunningInfo, DashboardConsumerRunningInfoRequest, PatchSubscriptionGroupConfigRequest,
            SubscriptionGroupConfigPatchResult,
        },
        consumer_workspace::{
            ConsumerClientsResult, ConsumerConfigPresenceResult, ConsumerConfigurationResult,
            ConsumerConnectionsAtTargetsResult, ConsumerExactTargetsRequest, ConsumerInventoryRequest,
            ConsumerInventoryResult, ConsumerProgressResult, ConsumerResourceRequest, ConsumerWorkspaceAdmin,
            ProducerConnectionsRequest, ProducerConnectionsResult, ProducerInventoryResult,
        },
        dashboard::{
            DashboardAdmin, DashboardBrokerConfig, DashboardBrokerList, DashboardBrokerRuntime, DashboardBrokerTarget,
            DashboardProducerInfo, DashboardTopicList,
        },
        dashboard_metrics::{DashboardConsumerSummary, DashboardMetricsAdmin, DashboardTopicRates},
        message::{
            DirectConsumeRequest, DirectConsumeResult, MessageLookupReadAdmin, MessageLookupRequest, MessageRecord,
            MessageScopedMutationAdmin,
        },
        security::AdminCredentials,
        topic::{
            DeleteTopicsInBrokerRequest, DetailedTopicCatalog, DetailedTopicConfig, DetailedTopicConsumers,
            DetailedTopicStats, GetTopicRouteRequest, TopicBatchDeleteAdmin, TopicBatchDeleteReport,
            TopicBatchDeleteRequest, TopicBatchMutationAdmin, TopicBatchMutationReport, TopicBatchUpsertRequest,
            TopicConfigFieldsMutationAdmin, TopicConfigFieldsPatchReport, TopicConfigFieldsPatchRequest,
            TopicInspectionAdmin, TopicMutationAdmin, TopicMutationSummary, TopicOffsetMutationAdmin,
            TopicOffsetMutationReport, TopicOffsetMutationRequest, TopicRoute, TopicSendRequest, TopicSendResult,
        },
    },
    mutation_client_adapter::{MutationAdminBuilder, MutationAdminSession},
};
use rocketmq_dashboard_common::ConnectionSnapshot;

pub(crate) type SessionFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Concurrent query capability of one started Admin session.
pub(crate) trait DashboardQuerySession: Send + Sync {
    fn health(&self) -> SessionFuture<'_, AdminResult<()>>;
    fn list_topics(&self) -> SessionFuture<'_, AdminResult<DashboardTopicList>>;
    #[cfg(test)]
    fn topic_stats<'a>(&'a self, topic: &'a str) -> SessionFuture<'a, AdminResult<DashboardTopicStats>>;
    fn topic_rates<'a>(&'a self, _topics: &'a [String]) -> SessionFuture<'a, AdminResult<Vec<DashboardTopicRates>>> {
        Box::pin(async { Err(unsupported_topic_session("topic_rates")) })
    }
    fn consumer_summary(&self) -> SessionFuture<'_, AdminResult<DashboardConsumerSummary>> {
        Box::pin(async { Err(unsupported_consumer_session("consumer_summary")) })
    }
    #[cfg(test)]
    fn list_consumers(&self) -> SessionFuture<'_, AdminResult<DashboardConsumerList>>;
    fn list_producers(&self) -> SessionFuture<'_, AdminResult<Vec<DashboardProducerInfo>>>;
    fn list_brokers(&self) -> SessionFuture<'_, AdminResult<DashboardBrokerList>>;
    fn broker_runtime<'a>(
        &'a self,
        target: &'a DashboardBrokerTarget,
    ) -> SessionFuture<'a, AdminResult<DashboardBrokerRuntime>>;
    fn broker_config<'a>(
        &'a self,
        target: &'a DashboardBrokerTarget,
    ) -> SessionFuture<'a, AdminResult<DashboardBrokerConfig>>;
    fn topic_catalog(&self) -> SessionFuture<'_, AdminResult<DetailedTopicCatalog>> {
        Box::pin(async { Err(unsupported_topic_session("topic_catalog")) })
    }
    fn topic_route<'a>(&'a self, _topic: &'a str) -> SessionFuture<'a, AdminResult<Option<TopicRoute>>> {
        Box::pin(async { Err(unsupported_topic_session("topic_route")) })
    }
    fn detailed_topic_stats<'a>(&'a self, _topic: &'a str) -> SessionFuture<'a, AdminResult<DetailedTopicStats>> {
        Box::pin(async { Err(unsupported_topic_session("topic_stats")) })
    }
    fn topic_config<'a>(&'a self, _topic: &'a str) -> SessionFuture<'a, AdminResult<DetailedTopicConfig>> {
        Box::pin(async { Err(unsupported_topic_session("topic_config")) })
    }
    fn topic_consumers<'a>(&'a self, _topic: &'a str) -> SessionFuture<'a, AdminResult<DetailedTopicConsumers>> {
        Box::pin(async { Err(unsupported_topic_session("topic_consumers")) })
    }
    fn consumer_inventory<'a>(
        &'a self,
        _request: &'a ConsumerInventoryRequest,
    ) -> SessionFuture<'a, AdminResult<ConsumerInventoryResult>> {
        Box::pin(async { Err(unsupported_consumer_session("consumer_inventory")) })
    }
    fn consumer_clients<'a>(
        &'a self,
        _request: &'a ConsumerResourceRequest,
    ) -> SessionFuture<'a, AdminResult<ConsumerClientsResult>> {
        Box::pin(async { Err(unsupported_consumer_session("consumer_clients")) })
    }
    fn consumer_progress<'a>(
        &'a self,
        _request: &'a ConsumerResourceRequest,
    ) -> SessionFuture<'a, AdminResult<ConsumerProgressResult>> {
        Box::pin(async { Err(unsupported_consumer_session("consumer_progress")) })
    }
    fn consumer_configuration<'a>(
        &'a self,
        _group: &'a str,
    ) -> SessionFuture<'a, AdminResult<ConsumerConfigurationResult>> {
        Box::pin(async { Err(unsupported_consumer_session("consumer_configuration")) })
    }
    fn consumer_config_presence<'a>(
        &'a self,
        _request: &'a ConsumerExactTargetsRequest,
    ) -> SessionFuture<'a, AdminResult<ConsumerConfigPresenceResult>> {
        Box::pin(async { Err(unsupported_consumer_session("consumer_config_presence")) })
    }
    fn consumer_connections_at_targets<'a>(
        &'a self,
        _request: &'a ConsumerExactTargetsRequest,
    ) -> SessionFuture<'a, AdminResult<ConsumerConnectionsAtTargetsResult>> {
        Box::pin(async { Err(unsupported_consumer_session("consumer_connections_at_targets")) })
    }
    fn consumer_diagnostic<'a>(
        &'a self,
        _request: &'a DashboardConsumerRunningInfoRequest,
    ) -> SessionFuture<'a, AdminResult<DashboardConsumerRunningInfo>> {
        Box::pin(async { Err(unsupported_consumer_session("consumer_diagnostic")) })
    }
    fn producer_inventory(&self) -> SessionFuture<'_, AdminResult<ProducerInventoryResult>> {
        Box::pin(async { Err(unsupported_consumer_session("producer_inventory")) })
    }
    fn producer_connections<'a>(
        &'a self,
        _request: &'a ProducerConnectionsRequest,
    ) -> SessionFuture<'a, AdminResult<ProducerConnectionsResult>> {
        Box::pin(async { Err(unsupported_consumer_session("producer_connections")) })
    }
    fn query_messages<'a>(
        &'a self,
        _request: &'a rocketmq_admin_core::core::dashboard::DashboardMessageQuery,
    ) -> SessionFuture<'a, AdminResult<rocketmq_admin_core::core::dashboard::DashboardMessageList>> {
        Box::pin(async { Err(unsupported_message_session()) })
    }
    fn lookup_message<'a>(
        &'a self,
        _request: &'a MessageLookupRequest,
    ) -> SessionFuture<'a, AdminResult<MessageRecord>> {
        Box::pin(async { Err(unsupported_message_session()) })
    }
    fn shutdown(self: Box<Self>) -> SessionFuture<'static, ()>;
}

/// Serialized generation-aware Broker mutation capability.
pub(crate) trait DashboardMutationSession: Send {
    fn patch_topic_fields<'a>(
        &'a mut self,
        _request: &'a TopicConfigFieldsPatchRequest,
    ) -> SessionFuture<'a, AdminResult<TopicConfigFieldsPatchReport>> {
        Box::pin(async { Err(unsupported_topic_session("patch_topic_fields")) })
    }
    fn query_config_generation<'a>(
        &'a mut self,
        request: &'a QueryBrokerConfigGenerationRequest,
    ) -> SessionFuture<'a, AdminResult<QueryBrokerConfigGenerationResult>>;
    fn patch_config_if_generation<'a>(
        &'a mut self,
        request: &'a PatchBrokerConfigRequest,
    ) -> SessionFuture<'a, AdminResult<BrokerConfigPatchResult>>;
    fn upsert_topic_batch<'a>(
        &'a mut self,
        _request: &'a TopicBatchUpsertRequest,
    ) -> SessionFuture<'a, AdminResult<TopicBatchMutationReport>> {
        Box::pin(async { Err(unsupported_topic_session("upsert_topic_batch")) })
    }
    fn delete_topic_batch<'a>(
        &'a mut self,
        _request: &'a TopicBatchDeleteRequest,
    ) -> SessionFuture<'a, AdminResult<TopicBatchDeleteReport>> {
        Box::pin(async { Err(unsupported_topic_session("delete_topic_batch")) })
    }
    fn delete_topics_in_broker<'a>(
        &'a mut self,
        _request: &'a DeleteTopicsInBrokerRequest,
    ) -> SessionFuture<'a, AdminResult<TopicMutationSummary>> {
        Box::pin(async { Err(unsupported_topic_session("delete_topics_in_broker")) })
    }
    fn send_topic_message<'a>(
        &'a mut self,
        _request: &'a TopicSendRequest,
    ) -> SessionFuture<'a, AdminResult<TopicSendResult>> {
        Box::pin(async { Err(unsupported_topic_session("send_topic_message")) })
    }
    fn reset_topic_offset_detailed<'a>(
        &'a mut self,
        _request: &'a TopicOffsetMutationRequest,
    ) -> SessionFuture<'a, AdminResult<TopicOffsetMutationReport>> {
        Box::pin(async { Err(unsupported_topic_session("reset_topic_offset_detailed")) })
    }
    fn skip_topic_accumulated_detailed<'a>(
        &'a mut self,
        _request: &'a TopicOffsetMutationRequest,
    ) -> SessionFuture<'a, AdminResult<TopicOffsetMutationReport>> {
        Box::pin(async { Err(unsupported_topic_session("skip_topic_accumulated_detailed")) })
    }
    fn upsert_consumer_group_exact_batch<'a>(
        &'a mut self,
        _request: &'a ConsumerExactBatchUpsertRequest,
    ) -> SessionFuture<'a, AdminResult<DashboardConsumerBatchResult>> {
        Box::pin(async { Err(unsupported_consumer_session("upsert_consumer_group_exact_batch")) })
    }
    fn delete_consumer_group_exact_batch<'a>(
        &'a mut self,
        _request: &'a ConsumerExactBatchDeleteRequest,
    ) -> SessionFuture<'a, AdminResult<DashboardConsumerBatchResult>> {
        Box::pin(async { Err(unsupported_consumer_session("delete_consumer_group_exact_batch")) })
    }
    fn patch_consumer_config<'a>(
        &'a mut self,
        _request: &'a PatchSubscriptionGroupConfigRequest,
    ) -> SessionFuture<'a, AdminResult<SubscriptionGroupConfigPatchResult>> {
        Box::pin(async { Err(unsupported_consumer_session("patch_consumer_config")) })
    }
    fn consume_message_directly<'a>(
        &'a mut self,
        _request: &'a DirectConsumeRequest,
    ) -> SessionFuture<'a, AdminResult<DirectConsumeResult>> {
        Box::pin(async { Err(unsupported_message_session()) })
    }
    fn shutdown(self: Box<Self>) -> SessionFuture<'static, ()>;
}

fn unsupported_message_session() -> rocketmq_admin_core::core::AdminError {
    rocketmq_admin_core::core::AdminError::operation_failed(
        "messages",
        "Message capability is unavailable in this session",
    )
}

fn unsupported_topic_session(operation: &'static str) -> rocketmq_admin_core::core::AdminError {
    rocketmq_admin_core::core::AdminError::operation_failed(
        operation,
        "Topic capability is not implemented by this session",
    )
}

fn unsupported_consumer_session(operation: &'static str) -> rocketmq_admin_core::core::AdminError {
    rocketmq_admin_core::core::AdminError::operation_failed(
        operation,
        "Consumer or Producer capability is not implemented by this session",
    )
}

/// Factory kept injectable so concurrency and shutdown are tested without a network.
pub(crate) trait DashboardSessionFactory: Send + Sync {
    fn create_query(
        &self,
        snapshot: ConnectionSnapshot,
        credentials: Option<AdminCredentials>,
    ) -> SessionFuture<'_, AdminResult<Box<dyn DashboardQuerySession>>>;
    fn create_mutation(
        &self,
        snapshot: ConnectionSnapshot,
        credentials: Option<AdminCredentials>,
    ) -> SessionFuture<'_, AdminResult<Box<dyn DashboardMutationSession>>>;
}

pub(crate) struct RealDashboardSessionFactory {
    client_runtime: Arc<ClientRuntime>,
}

impl RealDashboardSessionFactory {
    pub(crate) fn new(client_runtime: Arc<ClientRuntime>) -> Self {
        Self { client_runtime }
    }
}

impl DashboardSessionFactory for RealDashboardSessionFactory {
    fn create_query(
        &self,
        snapshot: ConnectionSnapshot,
        credentials: Option<AdminCredentials>,
    ) -> SessionFuture<'_, AdminResult<Box<dyn DashboardQuerySession>>> {
        let mut builder = AdminBuilder::new(Arc::clone(&self.client_runtime))
            .vip_channel_enabled(snapshot.transport.use_vip_channel)
            .use_tls(snapshot.transport.use_tls)
            .timeout_millis(5_000)
            .instance_name(format!("gpui-query-{}", snapshot.revision));
        if let Some(nameserver) = snapshot.nameserver {
            builder = builder.namesrv_addr(nameserver);
        }
        if let Some(credentials) = credentials {
            builder = builder.credentials(credentials);
        }
        Box::pin(async move {
            builder
                .build_and_start()
                .await
                .map(|inner| Box::new(RealQuerySession { inner }) as Box<dyn DashboardQuerySession>)
        })
    }

    fn create_mutation(
        &self,
        snapshot: ConnectionSnapshot,
        credentials: Option<AdminCredentials>,
    ) -> SessionFuture<'_, AdminResult<Box<dyn DashboardMutationSession>>> {
        let mut builder = MutationAdminBuilder::new(Arc::clone(&self.client_runtime))
            .vip_channel_enabled(snapshot.transport.use_vip_channel)
            .use_tls(snapshot.transport.use_tls)
            .timeout_millis(5_000)
            .instance_name(format!("gpui-mutation-{}", snapshot.revision));
        if let Some(nameserver) = snapshot.nameserver {
            builder = builder.namesrv_addr(nameserver);
        }
        if let Some(credentials) = credentials {
            builder = builder.credentials(credentials);
        }
        Box::pin(async move {
            builder
                .build_and_start()
                .await
                .map(|inner| Box::new(RealMutationSession { inner }) as Box<dyn DashboardMutationSession>)
        })
    }
}

struct RealQuerySession {
    inner: AdminSession,
}

impl DashboardQuerySession for RealQuerySession {
    fn health(&self) -> SessionFuture<'_, AdminResult<()>> {
        Box::pin(async {
            DashboardMetricsAdmin::dashboard_topic_names(&self.inner)
                .await
                .map(|_| ())
        })
    }

    fn list_topics(&self) -> SessionFuture<'_, AdminResult<DashboardTopicList>> {
        Box::pin(async move {
            let names = DashboardMetricsAdmin::dashboard_topic_names(&self.inner).await?;
            Ok(DashboardTopicList {
                items: names
                    .into_iter()
                    .map(|topic| {
                        let category =
                            if rocketmq_admin_core::core::dashboard_metrics::is_dashboard_application_topic(&topic) {
                                "NORMAL"
                            } else {
                                "SYSTEM"
                            };
                        rocketmq_admin_core::core::dashboard::DashboardTopicInfo {
                            topic,
                            category: category.into(),
                            broker_name: None,
                            read_queue_count: 0,
                            write_queue_count: 0,
                            perm: 0,
                        }
                    })
                    .collect(),
            })
        })
    }

    fn topic_rates<'a>(&'a self, topics: &'a [String]) -> SessionFuture<'a, AdminResult<Vec<DashboardTopicRates>>> {
        DashboardMetricsAdmin::dashboard_topic_rates(&self.inner, topics)
    }

    fn consumer_summary(&self) -> SessionFuture<'_, AdminResult<DashboardConsumerSummary>> {
        DashboardMetricsAdmin::dashboard_consumer_summary(&self.inner)
    }

    #[cfg(test)]
    fn topic_stats<'a>(&'a self, topic: &'a str) -> SessionFuture<'a, AdminResult<DashboardTopicStats>> {
        DashboardAdmin::dashboard_topic_stats(&self.inner, topic)
    }

    #[cfg(test)]
    fn list_consumers(&self) -> SessionFuture<'_, AdminResult<DashboardConsumerList>> {
        DashboardAdmin::dashboard_list_consumers(&self.inner)
    }

    fn list_producers(&self) -> SessionFuture<'_, AdminResult<Vec<DashboardProducerInfo>>> {
        DashboardAdmin::dashboard_list_producers(&self.inner)
    }

    fn list_brokers(&self) -> SessionFuture<'_, AdminResult<DashboardBrokerList>> {
        DashboardAdmin::dashboard_list_brokers(&self.inner)
    }

    fn broker_runtime<'a>(
        &'a self,
        target: &'a DashboardBrokerTarget,
    ) -> SessionFuture<'a, AdminResult<DashboardBrokerRuntime>> {
        DashboardAdmin::dashboard_broker_runtime(&self.inner, target)
    }

    fn broker_config<'a>(
        &'a self,
        target: &'a DashboardBrokerTarget,
    ) -> SessionFuture<'a, AdminResult<DashboardBrokerConfig>> {
        DashboardAdmin::dashboard_broker_config(&self.inner, target)
    }

    fn topic_catalog(&self) -> SessionFuture<'_, AdminResult<DetailedTopicCatalog>> {
        Box::pin(async move {
            let request = Default::default();
            TopicInspectionAdmin::inspect_topic_catalog(&self.inner, &request).await
        })
    }

    fn topic_route<'a>(&'a self, topic: &'a str) -> SessionFuture<'a, AdminResult<Option<TopicRoute>>> {
        let request = GetTopicRouteRequest {
            topic: topic.to_owned(),
        };
        Box::pin(async move { TopicInspectionAdmin::inspect_topic_route(&self.inner, &request).await })
    }

    fn detailed_topic_stats<'a>(&'a self, topic: &'a str) -> SessionFuture<'a, AdminResult<DetailedTopicStats>> {
        TopicInspectionAdmin::inspect_topic_stats(&self.inner, topic)
    }

    fn topic_config<'a>(&'a self, topic: &'a str) -> SessionFuture<'a, AdminResult<DetailedTopicConfig>> {
        TopicInspectionAdmin::inspect_topic_config(&self.inner, topic)
    }

    fn topic_consumers<'a>(&'a self, topic: &'a str) -> SessionFuture<'a, AdminResult<DetailedTopicConsumers>> {
        TopicInspectionAdmin::inspect_topic_consumers(&self.inner, topic)
    }

    fn consumer_inventory<'a>(
        &'a self,
        request: &'a ConsumerInventoryRequest,
    ) -> SessionFuture<'a, AdminResult<ConsumerInventoryResult>> {
        ConsumerWorkspaceAdmin::consumer_inventory(&self.inner, request)
    }

    fn consumer_clients<'a>(
        &'a self,
        request: &'a ConsumerResourceRequest,
    ) -> SessionFuture<'a, AdminResult<ConsumerClientsResult>> {
        ConsumerWorkspaceAdmin::consumer_clients(&self.inner, request)
    }

    fn consumer_progress<'a>(
        &'a self,
        request: &'a ConsumerResourceRequest,
    ) -> SessionFuture<'a, AdminResult<ConsumerProgressResult>> {
        ConsumerWorkspaceAdmin::consumer_progress(&self.inner, request)
    }

    fn consumer_configuration<'a>(
        &'a self,
        group: &'a str,
    ) -> SessionFuture<'a, AdminResult<ConsumerConfigurationResult>> {
        ConsumerWorkspaceAdmin::consumer_configuration(&self.inner, group)
    }

    fn consumer_config_presence<'a>(
        &'a self,
        request: &'a ConsumerExactTargetsRequest,
    ) -> SessionFuture<'a, AdminResult<ConsumerConfigPresenceResult>> {
        ConsumerWorkspaceAdmin::consumer_config_presence(&self.inner, request)
    }

    fn consumer_connections_at_targets<'a>(
        &'a self,
        request: &'a ConsumerExactTargetsRequest,
    ) -> SessionFuture<'a, AdminResult<ConsumerConnectionsAtTargetsResult>> {
        ConsumerWorkspaceAdmin::consumer_connections_at_targets(&self.inner, request)
    }

    fn consumer_diagnostic<'a>(
        &'a self,
        request: &'a DashboardConsumerRunningInfoRequest,
    ) -> SessionFuture<'a, AdminResult<DashboardConsumerRunningInfo>> {
        ConsumerWorkspaceAdmin::consumer_diagnostic(&self.inner, request)
    }

    fn producer_inventory(&self) -> SessionFuture<'_, AdminResult<ProducerInventoryResult>> {
        ConsumerWorkspaceAdmin::producer_inventory(&self.inner)
    }

    fn producer_connections<'a>(
        &'a self,
        request: &'a ProducerConnectionsRequest,
    ) -> SessionFuture<'a, AdminResult<ProducerConnectionsResult>> {
        ConsumerWorkspaceAdmin::producer_connections(&self.inner, request)
    }

    fn query_messages<'a>(
        &'a self,
        request: &'a rocketmq_admin_core::core::dashboard::DashboardMessageQuery,
    ) -> SessionFuture<'a, AdminResult<rocketmq_admin_core::core::dashboard::DashboardMessageList>> {
        DashboardAdmin::dashboard_query_messages(&self.inner, request)
    }
    fn lookup_message<'a>(
        &'a self,
        request: &'a MessageLookupRequest,
    ) -> SessionFuture<'a, AdminResult<MessageRecord>> {
        MessageLookupReadAdmin::lookup_message(&self.inner, request)
    }
    fn shutdown(mut self: Box<Self>) -> SessionFuture<'static, ()> {
        Box::pin(async move { self.inner.shutdown().await })
    }
}

struct RealMutationSession {
    inner: MutationAdminSession,
}

impl DashboardMutationSession for RealMutationSession {
    fn query_config_generation<'a>(
        &'a mut self,
        request: &'a QueryBrokerConfigGenerationRequest,
    ) -> SessionFuture<'a, AdminResult<QueryBrokerConfigGenerationResult>> {
        BrokerMutationAdmin::query_config_generation(&mut self.inner, request)
    }

    fn patch_config_if_generation<'a>(
        &'a mut self,
        request: &'a PatchBrokerConfigRequest,
    ) -> SessionFuture<'a, AdminResult<BrokerConfigPatchResult>> {
        BrokerMutationAdmin::patch_config_if_generation(&mut self.inner, request)
    }

    fn patch_topic_fields<'a>(
        &'a mut self,
        request: &'a TopicConfigFieldsPatchRequest,
    ) -> SessionFuture<'a, AdminResult<TopicConfigFieldsPatchReport>> {
        TopicConfigFieldsMutationAdmin::patch_topic_config_fields(&mut self.inner, request)
    }

    fn upsert_topic_batch<'a>(
        &'a mut self,
        request: &'a TopicBatchUpsertRequest,
    ) -> SessionFuture<'a, AdminResult<TopicBatchMutationReport>> {
        TopicBatchMutationAdmin::upsert_topic_batch(&mut self.inner, request)
    }

    fn delete_topic_batch<'a>(
        &'a mut self,
        request: &'a TopicBatchDeleteRequest,
    ) -> SessionFuture<'a, AdminResult<TopicBatchDeleteReport>> {
        TopicBatchDeleteAdmin::delete_topic_batch(&mut self.inner, request)
    }

    fn delete_topics_in_broker<'a>(
        &'a mut self,
        request: &'a DeleteTopicsInBrokerRequest,
    ) -> SessionFuture<'a, AdminResult<TopicMutationSummary>> {
        TopicMutationAdmin::delete_topics_in_broker(&mut self.inner, request)
    }

    fn send_topic_message<'a>(
        &'a mut self,
        request: &'a TopicSendRequest,
    ) -> SessionFuture<'a, AdminResult<TopicSendResult>> {
        TopicMutationAdmin::send_topic_test_message(&mut self.inner, request)
    }

    fn reset_topic_offset_detailed<'a>(
        &'a mut self,
        request: &'a TopicOffsetMutationRequest,
    ) -> SessionFuture<'a, AdminResult<TopicOffsetMutationReport>> {
        TopicOffsetMutationAdmin::reset_consumer_offset_detailed(&mut self.inner, request)
    }

    fn skip_topic_accumulated_detailed<'a>(
        &'a mut self,
        request: &'a TopicOffsetMutationRequest,
    ) -> SessionFuture<'a, AdminResult<TopicOffsetMutationReport>> {
        TopicOffsetMutationAdmin::skip_accumulated_detailed(&mut self.inner, request)
    }

    fn upsert_consumer_group_exact_batch<'a>(
        &'a mut self,
        request: &'a ConsumerExactBatchUpsertRequest,
    ) -> SessionFuture<'a, AdminResult<DashboardConsumerBatchResult>> {
        ConsumerExactBatchUpsertMutationAdmin::upsert_consumer_group_exact_batch(&mut self.inner, request)
    }

    fn delete_consumer_group_exact_batch<'a>(
        &'a mut self,
        request: &'a ConsumerExactBatchDeleteRequest,
    ) -> SessionFuture<'a, AdminResult<DashboardConsumerBatchResult>> {
        ConsumerExactBatchMutationAdmin::delete_consumer_group_exact_batch(&mut self.inner, request)
    }

    fn patch_consumer_config<'a>(
        &'a mut self,
        request: &'a PatchSubscriptionGroupConfigRequest,
    ) -> SessionFuture<'a, AdminResult<SubscriptionGroupConfigPatchResult>> {
        ConsumerMutationAdmin::patch_config_if_version(&mut self.inner, request)
    }

    fn consume_message_directly<'a>(
        &'a mut self,
        request: &'a DirectConsumeRequest,
    ) -> SessionFuture<'a, AdminResult<DirectConsumeResult>> {
        MessageScopedMutationAdmin::consume_scoped_message(&mut self.inner, request)
    }
    fn shutdown(mut self: Box<Self>) -> SessionFuture<'static, ()> {
        Box::pin(async move { self.inner.shutdown().await })
    }
}
