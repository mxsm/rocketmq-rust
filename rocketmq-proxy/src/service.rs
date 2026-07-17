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

//! Provider-backed service implementations and compatibility exports.

use std::sync::Arc;

use async_trait::async_trait;
use rocketmq_common::common::message::message_queue_assignment::MessageQueueAssignment;
use rocketmq_remoting::protocol::body::acl_info::AclInfo;
use rocketmq_remoting::protocol::body::user_info::UserInfo;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_remoting::runtime::RPCHook;

use crate::cluster::ClusterClient;
use crate::config::ClusterConfig;
use crate::config::LocalConfig;
use crate::config::ProxyMode;
use crate::context::ResolvedEndpoint;
use crate::error::ProxyResult;
use crate::local::local_service_manager_from_config;
use crate::processor::AckMessageRequest;
use crate::processor::AckMessageResultEntry;
use crate::processor::ChangeInvisibleDurationPlan;
use crate::processor::ChangeInvisibleDurationRequest;
use crate::processor::EndTransactionPlan;
use crate::processor::EndTransactionRequest;
use crate::processor::ForwardMessageToDeadLetterQueuePlan;
use crate::processor::ForwardMessageToDeadLetterQueueRequest;
use crate::processor::GetOffsetPlan;
use crate::processor::GetOffsetRequest;
use crate::processor::PullMessagePlan;
use crate::processor::PullMessageRequest;
use crate::processor::QueryOffsetPlan;
use crate::processor::QueryOffsetRequest;
use crate::processor::RecallMessagePlan;
use crate::processor::RecallMessageRequest;
use crate::processor::ReceiveMessagePlan;
use crate::processor::ReceiveMessageRequest;
use crate::processor::SendMessageRequest;
use crate::processor::SendMessageResultEntry;
use crate::processor::UpdateOffsetPlan;
use crate::processor::UpdateOffsetRequest;
use rocketmq_proxy_core::ProxyContext;

pub use rocketmq_proxy_core::service::*;
pub use rocketmq_proxy_core::ResourceIdentity;

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
        let route_client = Arc::clone(&client);
        let metadata_client = Arc::clone(&client);
        let assignment_client = Arc::clone(&client);
        let consumer_client = Arc::clone(&client);
        let transaction_client = Arc::clone(&client);
        Self::with_services(
            Arc::new(ClusterRouteService::new(route_client)),
            Arc::new(ClusterMetadataService::new(metadata_client)),
            Arc::new(ClusterAssignmentService::new(assignment_client)),
            Arc::new(ClusterMessageService::new(client)),
            Arc::new(ClusterConsumerService::new(consumer_client)),
            Arc::new(ClusterTransactionService::new(transaction_client)),
        )
    }

    pub fn from_cluster_config(config: ClusterConfig) -> Self {
        Self::from_cluster_client(Arc::new(crate::cluster::RocketmqClusterClient::new(config)))
    }

    pub fn from_cluster_config_with_rpc_hook(config: ClusterConfig, rpc_hook: Option<Arc<dyn RPCHook>>) -> Self {
        Self::from_cluster_client(Arc::new(crate::cluster::RocketmqClusterClient::with_rpc_hook(
            config, rpc_hook,
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

pub struct LocalServiceManager {
    route_service: Arc<dyn RouteService>,
    metadata_service: Arc<dyn MetadataService>,
    assignment_service: Arc<dyn AssignmentService>,
    message_service: Arc<dyn MessageService>,
    consumer_service: Arc<dyn ConsumerService>,
    transaction_service: Arc<dyn TransactionService>,
}

impl LocalServiceManager {
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

    pub fn from_local_config(config: LocalConfig, assignment_strategy_name: impl Into<String>) -> Self {
        local_service_manager_from_config(config, assignment_strategy_name)
    }
}

impl Default for LocalServiceManager {
    fn default() -> Self {
        Self::new(
            Arc::new(StaticRouteService::default()),
            Arc::new(StaticMetadataService::default()),
        )
    }
}

impl ServiceManager for LocalServiceManager {
    fn mode(&self) -> ProxyMode {
        ProxyMode::Local
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
