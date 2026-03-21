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
use dashmap::DashMap;
use rocketmq_error::RocketMQError;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;

use crate::config::ProxyMode;
use crate::context::ProxyContext;
use crate::context::ResolvedEndpoint;
use crate::error::ProxyError;
use crate::error::ProxyResult;

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
        group: &ResourceIdentity,
    ) -> ProxyResult<Option<SubscriptionGroupMetadata>>;
}

pub trait ServiceManager: Send + Sync {
    fn mode(&self) -> ProxyMode;

    fn route_service(&self) -> Arc<dyn RouteService>;

    fn metadata_service(&self) -> Arc<dyn MetadataService>;
}

#[derive(Debug, Default)]
pub struct UnsupportedRouteService;

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
        _group: &ResourceIdentity,
    ) -> ProxyResult<Option<SubscriptionGroupMetadata>> {
        Ok(None)
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
        group: &ResourceIdentity,
    ) -> ProxyResult<Option<SubscriptionGroupMetadata>> {
        Ok(self.subscription_groups.get(group).map(|entry| entry.clone()))
    }
}

pub struct ClusterServiceManager {
    route_service: Arc<dyn RouteService>,
    metadata_service: Arc<dyn MetadataService>,
}

impl ClusterServiceManager {
    pub fn new(route_service: Arc<dyn RouteService>, metadata_service: Arc<dyn MetadataService>) -> Self {
        Self {
            route_service,
            metadata_service,
        }
    }
}

impl Default for ClusterServiceManager {
    fn default() -> Self {
        Self::new(
            Arc::new(StaticRouteService::default()),
            Arc::new(StaticMetadataService::default()),
        )
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
}

pub struct LocalServiceManager {
    route_service: Arc<dyn RouteService>,
    metadata_service: Arc<dyn MetadataService>,
}

impl LocalServiceManager {
    pub fn new(route_service: Arc<dyn RouteService>, metadata_service: Arc<dyn MetadataService>) -> Self {
        Self {
            route_service,
            metadata_service,
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
}
