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

//! Client-backed implementations of Proxy Core service ports.

use std::sync::Arc;

use rocketmq_model::common::message::message_queue_assignment::MessageQueueAssignment;
use rocketmq_protocol::protocol::body::acl_info::AclInfo;
use rocketmq_protocol::protocol::body::user_info::UserInfo;
use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_proxy_core::AckMessageRequest;
use rocketmq_proxy_core::AckMessageResultEntry;
use rocketmq_proxy_core::AssignmentService;
use rocketmq_proxy_core::ChangeInvisibleDurationPlan;
use rocketmq_proxy_core::ChangeInvisibleDurationRequest;
use rocketmq_proxy_core::ConsumerService;
use rocketmq_proxy_core::DefaultAssignmentService;
use rocketmq_proxy_core::DefaultConsumerService;
use rocketmq_proxy_core::DefaultMessageService;
use rocketmq_proxy_core::DefaultTransactionService;
use rocketmq_proxy_core::EndTransactionPlan;
use rocketmq_proxy_core::EndTransactionRequest;
use rocketmq_proxy_core::ForwardMessageToDeadLetterQueuePlan;
use rocketmq_proxy_core::ForwardMessageToDeadLetterQueueRequest;
use rocketmq_proxy_core::GetOffsetPlan;
use rocketmq_proxy_core::GetOffsetRequest;
use rocketmq_proxy_core::MessageService;
use rocketmq_proxy_core::MetadataService;
use rocketmq_proxy_core::ProxyContext;
use rocketmq_proxy_core::ProxyMode;
use rocketmq_proxy_core::ProxyServiceFuture;
use rocketmq_proxy_core::ProxyTopicMessageType;
use rocketmq_proxy_core::PullMessagePlan;
use rocketmq_proxy_core::PullMessageRequest;
use rocketmq_proxy_core::QueryOffsetPlan;
use rocketmq_proxy_core::QueryOffsetRequest;
use rocketmq_proxy_core::RecallMessagePlan;
use rocketmq_proxy_core::RecallMessageRequest;
use rocketmq_proxy_core::ReceiveMessagePlan;
use rocketmq_proxy_core::ReceiveMessageRequest;
use rocketmq_proxy_core::ResolvedEndpoint;
use rocketmq_proxy_core::ResourceIdentity;
use rocketmq_proxy_core::RouteService;
use rocketmq_proxy_core::SendMessageRequest;
use rocketmq_proxy_core::SendMessageResultEntry;
use rocketmq_proxy_core::ServiceManager;
use rocketmq_proxy_core::SubscriptionGroupMetadata;
use rocketmq_proxy_core::TransactionService;
use rocketmq_proxy_core::UpdateOffsetPlan;
use rocketmq_proxy_core::UpdateOffsetRequest;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_security_api::OutboundSigner;

use crate::cluster::ClusterClient;
use crate::cluster::RocketmqClusterClient;
use crate::config::ClusterConfig;

pub struct ClusterRouteService {
    client: Arc<dyn ClusterClient>,
}

impl ClusterRouteService {
    pub fn new(client: Arc<dyn ClusterClient>) -> Self {
        Self { client }
    }
}

impl RouteService for ClusterRouteService {
    fn query_route<'a>(
        &'a self,
        _context: &'a ProxyContext,
        topic: &'a ResourceIdentity,
        _endpoints: &'a [ResolvedEndpoint],
    ) -> ProxyServiceFuture<'a, TopicRouteData> {
        Box::pin(async move { self.client.query_route(topic).await })
    }
}

pub struct ClusterMetadataService {
    client: Arc<dyn ClusterClient>,
}

impl ClusterMetadataService {
    pub fn new(client: Arc<dyn ClusterClient>) -> Self {
        Self { client }
    }
}

impl MetadataService for ClusterMetadataService {
    fn readiness_check(&self) -> ProxyServiceFuture<'_, ()> {
        Box::pin(async move { self.client.readiness_check().await })
    }

    fn topic_message_type<'a>(
        &'a self,
        _context: &'a ProxyContext,
        topic: &'a ResourceIdentity,
    ) -> ProxyServiceFuture<'a, ProxyTopicMessageType> {
        Box::pin(async move { self.client.query_topic_message_type(topic).await })
    }

    fn subscription_group<'a>(
        &'a self,
        _context: &'a ProxyContext,
        topic: &'a ResourceIdentity,
        group: &'a ResourceIdentity,
    ) -> ProxyServiceFuture<'a, Option<SubscriptionGroupMetadata>> {
        Box::pin(async move { self.client.query_subscription_group(topic, group).await })
    }

    fn user<'a>(&'a self, _context: &'a ProxyContext, username: &'a str) -> ProxyServiceFuture<'a, Option<UserInfo>> {
        Box::pin(async move { self.client.query_user(username).await })
    }

    fn acl<'a>(&'a self, _context: &'a ProxyContext, subject: &'a str) -> ProxyServiceFuture<'a, Option<AclInfo>> {
        Box::pin(async move { self.client.query_acl(subject).await })
    }
}

pub struct ClusterAssignmentService {
    client: Arc<dyn ClusterClient>,
}

impl ClusterAssignmentService {
    pub fn new(client: Arc<dyn ClusterClient>) -> Self {
        Self { client }
    }
}

impl AssignmentService for ClusterAssignmentService {
    fn query_assignment<'a>(
        &'a self,
        context: &'a ProxyContext,
        topic: &'a ResourceIdentity,
        group: &'a ResourceIdentity,
        _endpoints: &'a [ResolvedEndpoint],
    ) -> ProxyServiceFuture<'a, Option<Vec<MessageQueueAssignment>>> {
        Box::pin(async move {
            self.client
                .query_assignment(topic, group, context.require_client_id()?)
                .await
        })
    }
}

pub struct ClusterMessageService {
    client: Arc<dyn ClusterClient>,
}

impl ClusterMessageService {
    pub fn new(client: Arc<dyn ClusterClient>) -> Self {
        Self { client }
    }
}

impl MessageService for ClusterMessageService {
    fn send_message<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: &'a SendMessageRequest,
    ) -> ProxyServiceFuture<'a, Vec<SendMessageResultEntry>> {
        Box::pin(async move { self.client.send_message(context, request).await })
    }

    fn recall_message<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: &'a RecallMessageRequest,
    ) -> ProxyServiceFuture<'a, RecallMessagePlan> {
        Box::pin(async move { self.client.recall_message(context, request).await })
    }
}

pub struct ClusterConsumerService {
    client: Arc<dyn ClusterClient>,
}

impl ClusterConsumerService {
    pub fn new(client: Arc<dyn ClusterClient>) -> Self {
        Self { client }
    }
}

impl ConsumerService for ClusterConsumerService {
    fn receive_message<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: &'a ReceiveMessageRequest,
    ) -> ProxyServiceFuture<'a, ReceiveMessagePlan> {
        Box::pin(async move { self.client.receive_message(context, request).await })
    }

    fn pull_message<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: &'a PullMessageRequest,
    ) -> ProxyServiceFuture<'a, PullMessagePlan> {
        Box::pin(async move { self.client.pull_message(context, request).await })
    }

    fn ack_message<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: &'a AckMessageRequest,
    ) -> ProxyServiceFuture<'a, Vec<AckMessageResultEntry>> {
        Box::pin(async move { self.client.ack_message(context, request).await })
    }

    fn forward_message_to_dead_letter_queue<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: &'a ForwardMessageToDeadLetterQueueRequest,
    ) -> ProxyServiceFuture<'a, ForwardMessageToDeadLetterQueuePlan> {
        Box::pin(async move { self.client.forward_message_to_dead_letter_queue(context, request).await })
    }

    fn change_invisible_duration<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: &'a ChangeInvisibleDurationRequest,
    ) -> ProxyServiceFuture<'a, ChangeInvisibleDurationPlan> {
        Box::pin(async move { self.client.change_invisible_duration(context, request).await })
    }

    fn update_offset<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: &'a UpdateOffsetRequest,
    ) -> ProxyServiceFuture<'a, UpdateOffsetPlan> {
        Box::pin(async move { self.client.update_offset(context, request).await })
    }

    fn get_offset<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: &'a GetOffsetRequest,
    ) -> ProxyServiceFuture<'a, GetOffsetPlan> {
        Box::pin(async move { self.client.get_offset(context, request).await })
    }

    fn query_offset<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: &'a QueryOffsetRequest,
    ) -> ProxyServiceFuture<'a, QueryOffsetPlan> {
        Box::pin(async move { self.client.query_offset(context, request).await })
    }
}

pub struct ClusterTransactionService {
    client: Arc<dyn ClusterClient>,
}

impl ClusterTransactionService {
    pub fn new(client: Arc<dyn ClusterClient>) -> Self {
        Self { client }
    }
}

impl TransactionService for ClusterTransactionService {
    fn transaction_producer_group(&self, context: &ProxyContext) -> Option<String> {
        self.client.transaction_producer_group(context)
    }

    fn end_transaction<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: &'a EndTransactionRequest,
    ) -> ProxyServiceFuture<'a, EndTransactionPlan> {
        Box::pin(async move { self.client.end_transaction(context, request).await })
    }
}

pub struct ClusterServiceManager {
    route_service: Arc<dyn RouteService>,
    metadata_service: Arc<dyn MetadataService>,
    assignment_service: Arc<dyn AssignmentService>,
    message_service: Arc<dyn MessageService>,
    consumer_service: Arc<dyn ConsumerService>,
    transaction_service: Arc<dyn TransactionService>,
}

impl ClusterServiceManager {
    pub fn new(route_service: Arc<dyn RouteService>, metadata_service: Arc<dyn MetadataService>) -> Self {
        Self::with_services(
            route_service,
            metadata_service,
            Arc::new(DefaultAssignmentService),
            Arc::new(DefaultMessageService),
            Arc::new(DefaultConsumerService),
            Arc::new(DefaultTransactionService),
        )
    }

    pub fn with_assignment_service(
        route_service: Arc<dyn RouteService>,
        metadata_service: Arc<dyn MetadataService>,
        assignment_service: Arc<dyn AssignmentService>,
    ) -> Self {
        Self::with_services(
            route_service,
            metadata_service,
            assignment_service,
            Arc::new(DefaultMessageService),
            Arc::new(DefaultConsumerService),
            Arc::new(DefaultTransactionService),
        )
    }

    pub fn with_services(
        route_service: Arc<dyn RouteService>,
        metadata_service: Arc<dyn MetadataService>,
        assignment_service: Arc<dyn AssignmentService>,
        message_service: Arc<dyn MessageService>,
        consumer_service: Arc<dyn ConsumerService>,
        transaction_service: Arc<dyn TransactionService>,
    ) -> Self {
        Self {
            route_service,
            metadata_service,
            assignment_service,
            message_service,
            consumer_service,
            transaction_service,
        }
    }

    pub fn from_cluster_client(client: Arc<dyn ClusterClient>) -> Self {
        Self::with_services(
            Arc::new(ClusterRouteService::new(Arc::clone(&client))),
            Arc::new(ClusterMetadataService::new(Arc::clone(&client))),
            Arc::new(ClusterAssignmentService::new(Arc::clone(&client))),
            Arc::new(ClusterMessageService::new(Arc::clone(&client))),
            Arc::new(ClusterConsumerService::new(Arc::clone(&client))),
            Arc::new(ClusterTransactionService::new(client)),
        )
    }

    pub fn from_cluster_config(
        config: ClusterConfig,
        signer: Option<Arc<dyn OutboundSigner>>,
        service_context: &ChildServiceContext,
        telemetry_handle: rocketmq_client_rust::TelemetryHandle,
    ) -> rocketmq_proxy_core::ProxyResult<Self> {
        Ok(Self::from_cluster_client(Arc::new(RocketmqClusterClient::new(
            config,
            signer,
            service_context,
            telemetry_handle,
        )?)))
    }
}

impl ServiceManager for ClusterServiceManager {
    fn mode(&self) -> ProxyMode {
        ProxyMode::Cluster
    }

    fn route_service(&self) -> Arc<dyn RouteService> {
        Arc::clone(&self.route_service)
    }

    fn metadata_service(&self) -> Arc<dyn MetadataService> {
        Arc::clone(&self.metadata_service)
    }

    fn assignment_service(&self) -> Arc<dyn AssignmentService> {
        Arc::clone(&self.assignment_service)
    }

    fn message_service(&self) -> Arc<dyn MessageService> {
        Arc::clone(&self.message_service)
    }

    fn consumer_service(&self) -> Arc<dyn ConsumerService> {
        Arc::clone(&self.consumer_service)
    }

    fn transaction_service(&self) -> Arc<dyn TransactionService> {
        Arc::clone(&self.transaction_service)
    }
}
