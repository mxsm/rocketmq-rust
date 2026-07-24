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

use async_trait::async_trait;
use rocketmq_protocol::common::message::message_queue_assignment::MessageQueueAssignment;
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
use rocketmq_proxy_core::ProxyResult;
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
use rocketmq_runtime::ServiceContext;
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

#[async_trait]
impl RouteService for ClusterRouteService {
    async fn query_route(
        &self,
        _context: &ProxyContext,
        topic: &ResourceIdentity,
        _endpoints: &[ResolvedEndpoint],
    ) -> ProxyResult<TopicRouteData> {
        self.client.query_route(topic).await
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

#[async_trait]
impl MetadataService for ClusterMetadataService {
    async fn readiness_check(&self) -> ProxyResult<()> {
        self.client.readiness_check().await
    }

    async fn topic_message_type(
        &self,
        _context: &ProxyContext,
        topic: &ResourceIdentity,
    ) -> ProxyResult<ProxyTopicMessageType> {
        self.client.query_topic_message_type(topic).await
    }

    async fn subscription_group(
        &self,
        _context: &ProxyContext,
        topic: &ResourceIdentity,
        group: &ResourceIdentity,
    ) -> ProxyResult<Option<SubscriptionGroupMetadata>> {
        self.client.query_subscription_group(topic, group).await
    }

    async fn user(&self, _context: &ProxyContext, username: &str) -> ProxyResult<Option<UserInfo>> {
        self.client.query_user(username).await
    }

    async fn acl(&self, _context: &ProxyContext, subject: &str) -> ProxyResult<Option<AclInfo>> {
        self.client.query_acl(subject).await
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

#[async_trait]
impl AssignmentService for ClusterAssignmentService {
    async fn query_assignment(
        &self,
        context: &ProxyContext,
        topic: &ResourceIdentity,
        group: &ResourceIdentity,
        _endpoints: &[ResolvedEndpoint],
    ) -> ProxyResult<Option<Vec<MessageQueueAssignment>>> {
        self.client
            .query_assignment(topic, group, context.require_client_id()?)
            .await
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

#[async_trait]
impl MessageService for ClusterMessageService {
    async fn send_message(
        &self,
        context: &ProxyContext,
        request: &SendMessageRequest,
    ) -> ProxyResult<Vec<SendMessageResultEntry>> {
        self.client.send_message(context, request).await
    }

    async fn recall_message(
        &self,
        context: &ProxyContext,
        request: &RecallMessageRequest,
    ) -> ProxyResult<RecallMessagePlan> {
        self.client.recall_message(context, request).await
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

#[async_trait]
impl ConsumerService for ClusterConsumerService {
    async fn receive_message(
        &self,
        context: &ProxyContext,
        request: &ReceiveMessageRequest,
    ) -> ProxyResult<ReceiveMessagePlan> {
        self.client.receive_message(context, request).await
    }

    async fn pull_message(&self, context: &ProxyContext, request: &PullMessageRequest) -> ProxyResult<PullMessagePlan> {
        self.client.pull_message(context, request).await
    }

    async fn ack_message(
        &self,
        context: &ProxyContext,
        request: &AckMessageRequest,
    ) -> ProxyResult<Vec<AckMessageResultEntry>> {
        self.client.ack_message(context, request).await
    }

    async fn forward_message_to_dead_letter_queue(
        &self,
        context: &ProxyContext,
        request: &ForwardMessageToDeadLetterQueueRequest,
    ) -> ProxyResult<ForwardMessageToDeadLetterQueuePlan> {
        self.client.forward_message_to_dead_letter_queue(context, request).await
    }

    async fn change_invisible_duration(
        &self,
        context: &ProxyContext,
        request: &ChangeInvisibleDurationRequest,
    ) -> ProxyResult<ChangeInvisibleDurationPlan> {
        self.client.change_invisible_duration(context, request).await
    }

    async fn update_offset(
        &self,
        context: &ProxyContext,
        request: &UpdateOffsetRequest,
    ) -> ProxyResult<UpdateOffsetPlan> {
        self.client.update_offset(context, request).await
    }

    async fn get_offset(&self, context: &ProxyContext, request: &GetOffsetRequest) -> ProxyResult<GetOffsetPlan> {
        self.client.get_offset(context, request).await
    }

    async fn query_offset(&self, context: &ProxyContext, request: &QueryOffsetRequest) -> ProxyResult<QueryOffsetPlan> {
        self.client.query_offset(context, request).await
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

#[async_trait]
impl TransactionService for ClusterTransactionService {
    fn transaction_producer_group(&self, context: &ProxyContext) -> Option<String> {
        self.client.transaction_producer_group(context)
    }

    async fn end_transaction(
        &self,
        context: &ProxyContext,
        request: &EndTransactionRequest,
    ) -> ProxyResult<EndTransactionPlan> {
        self.client.end_transaction(context, request).await
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

    pub fn from_cluster_config(config: ClusterConfig) -> Self {
        Self::from_cluster_client(Arc::new(RocketmqClusterClient::new(config)))
    }

    pub fn from_cluster_config_with_outbound_signer(
        config: ClusterConfig,
        signer: Option<Arc<dyn OutboundSigner>>,
    ) -> Self {
        Self::from_cluster_client(Arc::new(RocketmqClusterClient::with_outbound_signer(config, signer)))
    }

    /// Compatibility constructor for the legacy Proxy Client hook path.
    #[doc(hidden)]
    pub fn from_cluster_config_with_rpc_hook(
        config: ClusterConfig,
        rpc_hook: Option<Arc<rocketmq_client_rust::proxy_adapter_compat::ClientRpcHook>>,
    ) -> Self {
        Self::from_cluster_client(Arc::new(RocketmqClusterClient::with_rpc_hook(config, rpc_hook)))
    }

    pub fn from_cluster_config_with_service_context(
        config: ClusterConfig,
        signer: Option<Arc<dyn OutboundSigner>>,
        service_context: &ServiceContext,
    ) -> Self {
        Self::from_cluster_client(Arc::new(RocketmqClusterClient::with_service_context(
            config,
            signer,
            service_context,
        )))
    }
}

impl Default for ClusterServiceManager {
    fn default() -> Self {
        Self::from_cluster_config(ClusterConfig::default())
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
