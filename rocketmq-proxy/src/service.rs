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

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_client_rust::producer::send_result::SendResult;
use rocketmq_client_rust::producer::send_status::SendStatus;
use rocketmq_common::common::message::message_queue_assignment::MessageQueueAssignment;
use rocketmq_error::RocketMQError;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;

use crate::cluster::ClusterClient;
use crate::config::ClusterConfig;
use crate::config::ProxyMode;
use crate::context::ProxyContext;
use crate::context::ResolvedEndpoint;
use crate::error::ProxyError;
use crate::error::ProxyResult;
use crate::processor::AckMessageRequest;
use crate::processor::AckMessageResultEntry;
use crate::processor::ChangeInvisibleDurationPlan;
use crate::processor::ChangeInvisibleDurationRequest;
use crate::processor::EndTransactionPlan;
use crate::processor::EndTransactionRequest;
use crate::processor::ReceiveMessagePlan;
use crate::processor::ReceiveMessageRequest;
use crate::processor::SendMessageRequest;
use crate::processor::SendMessageResultEntry;
use crate::status::ProxyStatusMapper;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResourceIdentity {
    namespace: String,
    name: String,
}

impl ResourceIdentity {
    pub fn new(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
        }
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl fmt::Display for ResourceIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.namespace.is_empty() {
            write!(f, "{}", self.name)
        } else {
            write!(f, "{}%{}", self.namespace, self.name)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProxyTopicMessageType {
    Unspecified,
    #[default]
    Normal,
    Fifo,
    Delay,
    Transaction,
    Mixed,
    Lite,
    Priority,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SubscriptionGroupMetadata {
    pub consume_message_orderly: bool,
    pub lite_bind_topic: Option<String>,
}

#[async_trait]
pub trait RouteService: Send + Sync {
    async fn query_route(
        &self,
        context: &ProxyContext,
        topic: &ResourceIdentity,
        endpoints: &[ResolvedEndpoint],
    ) -> ProxyResult<TopicRouteData>;
}

#[async_trait]
pub trait MetadataService: Send + Sync {
    async fn topic_message_type(
        &self,
        context: &ProxyContext,
        topic: &ResourceIdentity,
    ) -> ProxyResult<ProxyTopicMessageType>;

    async fn subscription_group(
        &self,
        context: &ProxyContext,
        topic: &ResourceIdentity,
        group: &ResourceIdentity,
    ) -> ProxyResult<Option<SubscriptionGroupMetadata>>;
}

#[async_trait]
pub trait AssignmentService: Send + Sync {
    async fn query_assignment(
        &self,
        context: &ProxyContext,
        topic: &ResourceIdentity,
        group: &ResourceIdentity,
        endpoints: &[ResolvedEndpoint],
    ) -> ProxyResult<Option<Vec<MessageQueueAssignment>>>;
}

#[async_trait]
pub trait MessageService: Send + Sync {
    async fn send_message(
        &self,
        context: &ProxyContext,
        request: &SendMessageRequest,
    ) -> ProxyResult<Vec<SendMessageResultEntry>>;
}

#[async_trait]
pub trait ConsumerService: Send + Sync {
    async fn receive_message(
        &self,
        context: &ProxyContext,
        request: &ReceiveMessageRequest,
    ) -> ProxyResult<ReceiveMessagePlan>;

    async fn ack_message(
        &self,
        context: &ProxyContext,
        request: &AckMessageRequest,
    ) -> ProxyResult<Vec<AckMessageResultEntry>>;

    async fn change_invisible_duration(
        &self,
        context: &ProxyContext,
        request: &ChangeInvisibleDurationRequest,
    ) -> ProxyResult<ChangeInvisibleDurationPlan>;
}

#[async_trait]
pub trait TransactionService: Send + Sync {
    async fn end_transaction(
        &self,
        context: &ProxyContext,
        request: &EndTransactionRequest,
    ) -> ProxyResult<EndTransactionPlan>;
}

pub trait ServiceManager: Send + Sync {
    fn mode(&self) -> ProxyMode;

    fn route_service(&self) -> Arc<dyn RouteService>;

    fn metadata_service(&self) -> Arc<dyn MetadataService>;

    fn assignment_service(&self) -> Arc<dyn AssignmentService>;

    fn message_service(&self) -> Arc<dyn MessageService>;

    fn consumer_service(&self) -> Arc<dyn ConsumerService>;

    fn transaction_service(&self) -> Arc<dyn TransactionService>;
}

#[derive(Debug, Default)]
pub struct UnsupportedRouteService;

pub type DefaultRouteService = UnsupportedRouteService;

#[async_trait]
impl RouteService for UnsupportedRouteService {
    async fn query_route(
        &self,
        _context: &ProxyContext,
        _topic: &ResourceIdentity,
        _endpoints: &[ResolvedEndpoint],
    ) -> ProxyResult<TopicRouteData> {
        Err(ProxyError::not_implemented("route service"))
    }
}

#[derive(Debug, Default)]
pub struct DefaultMetadataService;

#[async_trait]
impl MetadataService for DefaultMetadataService {
    async fn topic_message_type(
        &self,
        _context: &ProxyContext,
        _topic: &ResourceIdentity,
    ) -> ProxyResult<ProxyTopicMessageType> {
        Ok(ProxyTopicMessageType::Normal)
    }

    async fn subscription_group(
        &self,
        _context: &ProxyContext,
        _topic: &ResourceIdentity,
        _group: &ResourceIdentity,
    ) -> ProxyResult<Option<SubscriptionGroupMetadata>> {
        Ok(None)
    }
}

#[derive(Debug, Default)]
pub struct DefaultAssignmentService;

#[async_trait]
impl AssignmentService for DefaultAssignmentService {
    async fn query_assignment(
        &self,
        _context: &ProxyContext,
        _topic: &ResourceIdentity,
        _group: &ResourceIdentity,
        _endpoints: &[ResolvedEndpoint],
    ) -> ProxyResult<Option<Vec<MessageQueueAssignment>>> {
        Ok(None)
    }
}

#[derive(Debug, Default)]
pub struct DefaultMessageService;

#[async_trait]
impl MessageService for DefaultMessageService {
    async fn send_message(
        &self,
        _context: &ProxyContext,
        _request: &SendMessageRequest,
    ) -> ProxyResult<Vec<SendMessageResultEntry>> {
        Err(ProxyError::not_implemented("message service"))
    }
}

#[derive(Debug, Default)]
pub struct DefaultConsumerService;

#[async_trait]
impl ConsumerService for DefaultConsumerService {
    async fn receive_message(
        &self,
        _context: &ProxyContext,
        _request: &ReceiveMessageRequest,
    ) -> ProxyResult<ReceiveMessagePlan> {
        Err(ProxyError::not_implemented("consumer service"))
    }

    async fn ack_message(
        &self,
        _context: &ProxyContext,
        _request: &AckMessageRequest,
    ) -> ProxyResult<Vec<AckMessageResultEntry>> {
        Err(ProxyError::not_implemented("consumer service"))
    }

    async fn change_invisible_duration(
        &self,
        _context: &ProxyContext,
        _request: &ChangeInvisibleDurationRequest,
    ) -> ProxyResult<ChangeInvisibleDurationPlan> {
        Err(ProxyError::not_implemented("consumer service"))
    }
}

#[derive(Debug, Default)]
pub struct DefaultTransactionService;

#[async_trait]
impl TransactionService for DefaultTransactionService {
    async fn end_transaction(
        &self,
        _context: &ProxyContext,
        _request: &EndTransactionRequest,
    ) -> ProxyResult<EndTransactionPlan> {
        Err(ProxyError::not_implemented("transaction service"))
    }
}

#[derive(Clone, Default)]
pub struct StaticRouteService {
    routes: Arc<DashMap<ResourceIdentity, TopicRouteData>>,
}

impl StaticRouteService {
    pub fn insert(&self, topic: ResourceIdentity, route: TopicRouteData) {
        self.routes.insert(topic, route);
    }
}

#[async_trait]
impl RouteService for StaticRouteService {
    async fn query_route(
        &self,
        _context: &ProxyContext,
        topic: &ResourceIdentity,
        _endpoints: &[ResolvedEndpoint],
    ) -> ProxyResult<TopicRouteData> {
        self.routes
            .get(topic)
            .map(|entry| entry.clone())
            .ok_or_else(|| RocketMQError::route_not_found(topic.name()).into())
    }
}

#[derive(Clone, Default)]
pub struct StaticMetadataService {
    topic_message_types: Arc<DashMap<ResourceIdentity, ProxyTopicMessageType>>,
    subscription_groups: Arc<DashMap<ResourceIdentity, SubscriptionGroupMetadata>>,
}

impl StaticMetadataService {
    pub fn set_topic_message_type(&self, topic: ResourceIdentity, message_type: ProxyTopicMessageType) {
        self.topic_message_types.insert(topic, message_type);
    }

    pub fn set_subscription_group(&self, group: ResourceIdentity, metadata: SubscriptionGroupMetadata) {
        self.subscription_groups.insert(group, metadata);
    }
}

#[async_trait]
impl MetadataService for StaticMetadataService {
    async fn topic_message_type(
        &self,
        _context: &ProxyContext,
        topic: &ResourceIdentity,
    ) -> ProxyResult<ProxyTopicMessageType> {
        Ok(self
            .topic_message_types
            .get(topic)
            .map(|entry| *entry)
            .unwrap_or(ProxyTopicMessageType::Normal))
    }

    async fn subscription_group(
        &self,
        _context: &ProxyContext,
        _topic: &ResourceIdentity,
        group: &ResourceIdentity,
    ) -> ProxyResult<Option<SubscriptionGroupMetadata>> {
        Ok(self.subscription_groups.get(group).map(|entry| entry.clone()))
    }
}

#[derive(Clone, Default)]
pub struct StaticMessageService {
    send_status: SendStatus,
}

impl StaticMessageService {
    pub fn with_send_status(send_status: SendStatus) -> Self {
        Self { send_status }
    }
}

#[async_trait]
impl MessageService for StaticMessageService {
    async fn send_message(
        &self,
        _context: &ProxyContext,
        request: &SendMessageRequest,
    ) -> ProxyResult<Vec<SendMessageResultEntry>> {
        Ok(request
            .messages
            .iter()
            .enumerate()
            .map(|(index, message)| {
                let send_result = SendResult::new(
                    self.send_status,
                    Some(CheetahString::from(message.client_message_id.as_str())),
                    None,
                    None,
                    index as u64,
                );
                SendMessageResultEntry {
                    status: ProxyStatusMapper::from_send_result_payload(&send_result),
                    send_result: Some(send_result),
                }
            })
            .collect())
    }
}

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

    async fn ack_message(
        &self,
        context: &ProxyContext,
        request: &AckMessageRequest,
    ) -> ProxyResult<Vec<AckMessageResultEntry>> {
        self.client.ack_message(context, request).await
    }

    async fn change_invisible_duration(
        &self,
        context: &ProxyContext,
        request: &ChangeInvisibleDurationRequest,
    ) -> ProxyResult<ChangeInvisibleDurationPlan> {
        self.client.change_invisible_duration(context, request).await
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
