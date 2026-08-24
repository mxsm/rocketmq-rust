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

use rocketmq_admin_core::{
    client_adapter::{AdminBuilder, AdminSession, ClientRuntime},
    core::{
        AdminResult,
        broker::{
            BrokerMutationAdmin, PatchBrokerConfigOutcome, PatchBrokerConfigRequest,
            QueryBrokerConfigGenerationRequest, QueryBrokerConfigGenerationResult,
        },
        dashboard::{
            DashboardAdmin, DashboardBrokerConfig, DashboardBrokerList, DashboardBrokerRuntime, DashboardBrokerTarget,
            DashboardConsumerList, DashboardProducerInfo, DashboardTopicList, DashboardTopicStats,
        },
        security::AdminCredentials,
        topic::{
            DeleteTopicsInBrokerRequest, DetailedTopicCatalog, DetailedTopicConfig, DetailedTopicConsumers,
            DetailedTopicStats, GetTopicRouteRequest, PatchTopicConfigOutcome, PatchTopicConfigRequest,
            QueryTopicConfigCasRequest, TopicBatchDeleteAdmin, TopicBatchDeleteOutcome, TopicBatchDeleteRequest,
            TopicBatchMutationAdmin, TopicBatchMutationOutcome, TopicBatchUpsertRequest, TopicConfigCasState,
            TopicInspectionAdmin, TopicMutationAdmin, TopicMutationOutcome, TopicMutationPreflightAdmin,
            TopicOffsetMutationAdmin, TopicOffsetMutationOutcome, TopicOffsetMutationRequest, TopicRoute,
            TopicSendRequest, TopicSendResult,
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
    fn topic_stats<'a>(&'a self, topic: &'a str) -> SessionFuture<'a, AdminResult<DashboardTopicStats>>;
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
    fn shutdown(self: Box<Self>) -> SessionFuture<'static, ()>;
}

/// Serialized generation-aware Broker mutation capability.
pub(crate) trait DashboardMutationSession: Send {
    fn query_config_generation<'a>(
        &'a mut self,
        request: &'a QueryBrokerConfigGenerationRequest,
    ) -> SessionFuture<'a, AdminResult<QueryBrokerConfigGenerationResult>>;
    fn patch_config_if_generation<'a>(
        &'a mut self,
        request: &'a PatchBrokerConfigRequest,
    ) -> SessionFuture<'a, AdminResult<PatchBrokerConfigOutcome>>;
    fn topic_config_cas_state<'a>(
        &'a mut self,
        _request: &'a QueryTopicConfigCasRequest,
    ) -> SessionFuture<'a, AdminResult<TopicConfigCasState>> {
        Box::pin(async { Err(unsupported_topic_session("topic_config_cas_state")) })
    }
    fn patch_topic_config<'a>(
        &'a mut self,
        _request: &'a PatchTopicConfigRequest,
    ) -> SessionFuture<'a, AdminResult<PatchTopicConfigOutcome>> {
        Box::pin(async { Err(unsupported_topic_session("patch_topic_config")) })
    }
    fn upsert_topic_batch<'a>(
        &'a mut self,
        _request: &'a TopicBatchUpsertRequest,
    ) -> SessionFuture<'a, AdminResult<TopicBatchMutationOutcome>> {
        Box::pin(async { Err(unsupported_topic_session("upsert_topic_batch")) })
    }
    fn delete_topic_batch<'a>(
        &'a mut self,
        _request: &'a TopicBatchDeleteRequest,
    ) -> SessionFuture<'a, AdminResult<TopicBatchDeleteOutcome>> {
        Box::pin(async { Err(unsupported_topic_session("delete_topic_batch")) })
    }
    fn delete_topics_in_broker<'a>(
        &'a mut self,
        _request: &'a DeleteTopicsInBrokerRequest,
    ) -> SessionFuture<'a, AdminResult<TopicMutationOutcome>> {
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
    ) -> SessionFuture<'a, AdminResult<TopicOffsetMutationOutcome>> {
        Box::pin(async { Err(unsupported_topic_session("reset_topic_offset_detailed")) })
    }
    fn skip_topic_accumulated_detailed<'a>(
        &'a mut self,
        _request: &'a TopicOffsetMutationRequest,
    ) -> SessionFuture<'a, AdminResult<TopicOffsetMutationOutcome>> {
        Box::pin(async { Err(unsupported_topic_session("skip_topic_accumulated_detailed")) })
    }
    fn shutdown(self: Box<Self>) -> SessionFuture<'static, ()>;
}

fn unsupported_topic_session(operation: &'static str) -> rocketmq_admin_core::core::AdminError {
    rocketmq_admin_core::core::AdminError::backend(operation, "Topic capability is not implemented by this session")
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
        Box::pin(async { DashboardAdmin::dashboard_list_topics(&self.inner).await.map(|_| ()) })
    }

    fn list_topics(&self) -> SessionFuture<'_, AdminResult<DashboardTopicList>> {
        DashboardAdmin::dashboard_list_topics(&self.inner)
    }

    fn topic_stats<'a>(&'a self, topic: &'a str) -> SessionFuture<'a, AdminResult<DashboardTopicStats>> {
        DashboardAdmin::dashboard_topic_stats(&self.inner, topic)
    }

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
    ) -> SessionFuture<'a, AdminResult<PatchBrokerConfigOutcome>> {
        BrokerMutationAdmin::patch_config_if_generation(&mut self.inner, request)
    }

    fn topic_config_cas_state<'a>(
        &'a mut self,
        request: &'a QueryTopicConfigCasRequest,
    ) -> SessionFuture<'a, AdminResult<TopicConfigCasState>> {
        TopicMutationPreflightAdmin::query_config_cas_state(&mut self.inner, request)
    }

    fn patch_topic_config<'a>(
        &'a mut self,
        request: &'a PatchTopicConfigRequest,
    ) -> SessionFuture<'a, AdminResult<PatchTopicConfigOutcome>> {
        TopicMutationAdmin::patch_config_if_version(&mut self.inner, request)
    }

    fn upsert_topic_batch<'a>(
        &'a mut self,
        request: &'a TopicBatchUpsertRequest,
    ) -> SessionFuture<'a, AdminResult<TopicBatchMutationOutcome>> {
        TopicBatchMutationAdmin::upsert_topic_batch(&mut self.inner, request)
    }

    fn delete_topic_batch<'a>(
        &'a mut self,
        request: &'a TopicBatchDeleteRequest,
    ) -> SessionFuture<'a, AdminResult<TopicBatchDeleteOutcome>> {
        TopicBatchDeleteAdmin::delete_topic_batch(&mut self.inner, request)
    }

    fn delete_topics_in_broker<'a>(
        &'a mut self,
        request: &'a DeleteTopicsInBrokerRequest,
    ) -> SessionFuture<'a, AdminResult<TopicMutationOutcome>> {
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
    ) -> SessionFuture<'a, AdminResult<TopicOffsetMutationOutcome>> {
        TopicOffsetMutationAdmin::reset_consumer_offset_detailed(&mut self.inner, request)
    }

    fn skip_topic_accumulated_detailed<'a>(
        &'a mut self,
        request: &'a TopicOffsetMutationRequest,
    ) -> SessionFuture<'a, AdminResult<TopicOffsetMutationOutcome>> {
        TopicOffsetMutationAdmin::skip_accumulated_detailed(&mut self.inner, request)
    }

    fn shutdown(mut self: Box<Self>) -> SessionFuture<'static, ()> {
        Box::pin(async move { self.inner.shutdown().await })
    }
}
