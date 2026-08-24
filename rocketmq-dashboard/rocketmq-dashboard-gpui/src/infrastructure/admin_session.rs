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
    fn shutdown(self: Box<Self>) -> SessionFuture<'static, ()>;
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

    fn shutdown(mut self: Box<Self>) -> SessionFuture<'static, ()> {
        Box::pin(async move { self.inner.shutdown().await })
    }
}
