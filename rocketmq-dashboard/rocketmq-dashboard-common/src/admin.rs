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

//! Feature-gated Dashboard Admin facade shared by Dashboard frontends.
//!
//! This module intentionally does not depend on RocketMQ protocol crates. Each
//! frontend can adapt its own admin implementation into this contract while the
//! HTTP/UI service layers keep a stable shape.

use serde::Deserialize;
use serde::Serialize;
use std::future::Future;
use std::pin::Pin;

pub type AdminResult<T, E> = Result<T, E>;

pub type AdminFuture<'a, T, E> = Pin<Box<dyn Future<Output = AdminResult<T, E>> + Send + 'a>>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AdminErrorCode {
    ValidationError,
    ConfigError,
    RocketmqError,
    NotFound,
    NotImplemented,
    InternalError,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminErrorView {
    pub code: AdminErrorCode,
    pub message: String,
}

impl AdminErrorView {
    pub fn new(code: AdminErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminList<T> {
    pub items: Vec<T>,
    pub total: usize,
}

impl<T> AdminList<T> {
    pub fn new(items: Vec<T>) -> Self {
        let total = items.len();
        Self { items, total }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminMutationResult {
    pub message: String,
}

impl AdminMutationResult {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

pub trait DashboardAdminProvider: Clone + Send + Sync + 'static {
    type Error: Send + Sync + 'static;
    type DashboardOverview: Send + 'static;
    type TopicList: Send + 'static;
    type Topic: Send + 'static;
    type TopicRoute: Send + 'static;
    type TopicStats: Send + 'static;
    type TopicMutationRequest: Send + 'static;
    type TopicMutationResult: Send + 'static;
    type ConsumerList: Send + 'static;
    type ConsumerProgress: Send + 'static;
    type ConsumerOffsetResetRequest: Send + 'static;
    type ConsumerOffsetResetResult: Send + 'static;
    type ProducerList: Send + 'static;
    type ProducerConnections: Send + 'static;
    type BrokerList: Send + 'static;
    type BrokerRuntimeStats: Send + 'static;
    type BrokerConfig: Send + 'static;
    type BrokerConfigUpdateRequest: Send + 'static;
    type BrokerConfigUpdateResult: Send + 'static;
    type MessageList: Send + 'static;
    type Message: Send + 'static;
    type MessageTrace: Send + 'static;
    type MessageResendRequest: Send + 'static;
    type MessageResendResult: Send + 'static;

    fn dashboard_overview(&self) -> AdminFuture<'_, Self::DashboardOverview, Self::Error>;

    fn list_topics(&self) -> AdminFuture<'_, Self::TopicList, Self::Error>;

    fn get_topic<'a>(&'a self, topic: &'a str) -> AdminFuture<'a, Self::Topic, Self::Error>;

    fn topic_route<'a>(&'a self, topic: &'a str) -> AdminFuture<'a, Self::TopicRoute, Self::Error>;

    fn topic_stats<'a>(&'a self, topic: &'a str) -> AdminFuture<'a, Self::TopicStats, Self::Error>;

    fn create_or_update_topic(
        &self,
        request: Self::TopicMutationRequest,
    ) -> AdminFuture<'_, Self::TopicMutationResult, Self::Error>;

    fn delete_topic<'a>(&'a self, topic: &'a str) -> AdminFuture<'a, Self::TopicMutationResult, Self::Error>;

    fn list_consumer_groups(&self) -> AdminFuture<'_, Self::ConsumerList, Self::Error>;

    fn consumer_progress<'a>(&'a self, group: &'a str) -> AdminFuture<'a, Self::ConsumerProgress, Self::Error>;

    fn reset_consumer_offset(
        &self,
        group: String,
        request: Self::ConsumerOffsetResetRequest,
    ) -> AdminFuture<'_, Self::ConsumerOffsetResetResult, Self::Error>;

    fn list_producers(&self) -> AdminFuture<'_, Self::ProducerList, Self::Error>;

    fn producer_connections(
        &self,
        topic: Option<String>,
        producer_group: Option<String>,
    ) -> AdminFuture<'_, Self::ProducerConnections, Self::Error>;

    fn list_brokers(&self) -> AdminFuture<'_, Self::BrokerList, Self::Error>;

    fn broker_runtime_stats<'a>(
        &'a self,
        broker_name: &'a str,
    ) -> AdminFuture<'a, Self::BrokerRuntimeStats, Self::Error>;

    fn broker_config<'a>(&'a self, broker_name: &'a str) -> AdminFuture<'a, Self::BrokerConfig, Self::Error>;

    fn update_broker_config(
        &self,
        broker_name: String,
        request: Self::BrokerConfigUpdateRequest,
    ) -> AdminFuture<'_, Self::BrokerConfigUpdateResult, Self::Error>;

    fn query_message_by_key(&self, topic: String, key: String) -> AdminFuture<'_, Self::MessageList, Self::Error>;

    fn query_message_by_id(&self, message_id: String) -> AdminFuture<'_, Self::Message, Self::Error>;

    fn message_trace(
        &self,
        message_id: String,
        topic: String,
        trace_topic: Option<String>,
    ) -> AdminFuture<'_, Self::MessageTrace, Self::Error>;

    fn resend_message(
        &self,
        message_id: String,
        request: Self::MessageResendRequest,
    ) -> AdminFuture<'_, Self::MessageResendResult, Self::Error>;
}

#[derive(Debug, Clone)]
pub struct DashboardAdminFacade<P> {
    provider: P,
}

impl<P> DashboardAdminFacade<P>
where
    P: DashboardAdminProvider,
{
    pub fn new(provider: P) -> Self {
        Self { provider }
    }

    pub fn provider(&self) -> &P {
        &self.provider
    }

    pub async fn dashboard_overview(&self) -> AdminResult<P::DashboardOverview, P::Error> {
        self.provider.dashboard_overview().await
    }

    pub async fn list_topics(&self) -> AdminResult<P::TopicList, P::Error> {
        self.provider.list_topics().await
    }

    pub async fn get_topic(&self, topic: &str) -> AdminResult<P::Topic, P::Error> {
        self.provider.get_topic(topic).await
    }

    pub async fn topic_route(&self, topic: &str) -> AdminResult<P::TopicRoute, P::Error> {
        self.provider.topic_route(topic).await
    }

    pub async fn topic_stats(&self, topic: &str) -> AdminResult<P::TopicStats, P::Error> {
        self.provider.topic_stats(topic).await
    }

    pub async fn create_or_update_topic(
        &self,
        request: P::TopicMutationRequest,
    ) -> AdminResult<P::TopicMutationResult, P::Error> {
        self.provider.create_or_update_topic(request).await
    }

    pub async fn delete_topic(&self, topic: &str) -> AdminResult<P::TopicMutationResult, P::Error> {
        self.provider.delete_topic(topic).await
    }

    pub async fn list_consumer_groups(&self) -> AdminResult<P::ConsumerList, P::Error> {
        self.provider.list_consumer_groups().await
    }

    pub async fn consumer_progress(&self, group: &str) -> AdminResult<P::ConsumerProgress, P::Error> {
        self.provider.consumer_progress(group).await
    }

    pub async fn reset_consumer_offset(
        &self,
        group: String,
        request: P::ConsumerOffsetResetRequest,
    ) -> AdminResult<P::ConsumerOffsetResetResult, P::Error> {
        self.provider.reset_consumer_offset(group, request).await
    }

    pub async fn list_producers(&self) -> AdminResult<P::ProducerList, P::Error> {
        self.provider.list_producers().await
    }

    pub async fn producer_connections(
        &self,
        topic: Option<String>,
        producer_group: Option<String>,
    ) -> AdminResult<P::ProducerConnections, P::Error> {
        self.provider.producer_connections(topic, producer_group).await
    }

    pub async fn list_brokers(&self) -> AdminResult<P::BrokerList, P::Error> {
        self.provider.list_brokers().await
    }

    pub async fn broker_runtime_stats(&self, broker_name: &str) -> AdminResult<P::BrokerRuntimeStats, P::Error> {
        self.provider.broker_runtime_stats(broker_name).await
    }

    pub async fn broker_config(&self, broker_name: &str) -> AdminResult<P::BrokerConfig, P::Error> {
        self.provider.broker_config(broker_name).await
    }

    pub async fn update_broker_config(
        &self,
        broker_name: String,
        request: P::BrokerConfigUpdateRequest,
    ) -> AdminResult<P::BrokerConfigUpdateResult, P::Error> {
        self.provider.update_broker_config(broker_name, request).await
    }

    pub async fn query_message_by_key(&self, topic: String, key: String) -> AdminResult<P::MessageList, P::Error> {
        self.provider.query_message_by_key(topic, key).await
    }

    pub async fn query_message_by_id(&self, message_id: String) -> AdminResult<P::Message, P::Error> {
        self.provider.query_message_by_id(message_id).await
    }

    pub async fn message_trace(
        &self,
        message_id: String,
        topic: String,
        trace_topic: Option<String>,
    ) -> AdminResult<P::MessageTrace, P::Error> {
        self.provider.message_trace(message_id, topic, trace_topic).await
    }

    pub async fn resend_message(
        &self,
        message_id: String,
        request: P::MessageResendRequest,
    ) -> AdminResult<P::MessageResendResult, P::Error> {
        self.provider.resend_message(message_id, request).await
    }
}

#[cfg(test)]
mod tests {
    use super::AdminList;
    use super::DashboardAdminFacade;
    use super::DashboardAdminProvider;

    #[derive(Clone)]
    struct FakeProvider;

    impl DashboardAdminProvider for FakeProvider {
        type Error = String;
        type DashboardOverview = String;
        type TopicList = AdminList<String>;
        type Topic = String;
        type TopicRoute = String;
        type TopicStats = String;
        type TopicMutationRequest = String;
        type TopicMutationResult = String;
        type ConsumerList = AdminList<String>;
        type ConsumerProgress = String;
        type ConsumerOffsetResetRequest = String;
        type ConsumerOffsetResetResult = String;
        type ProducerList = AdminList<String>;
        type ProducerConnections = AdminList<String>;
        type BrokerList = AdminList<String>;
        type BrokerRuntimeStats = String;
        type BrokerConfig = String;
        type BrokerConfigUpdateRequest = String;
        type BrokerConfigUpdateResult = String;
        type MessageList = AdminList<String>;
        type Message = String;
        type MessageTrace = String;
        type MessageResendRequest = String;
        type MessageResendResult = String;

        fn dashboard_overview(&self) -> super::AdminFuture<'_, Self::DashboardOverview, Self::Error> {
            Box::pin(async { Ok("UP".to_string()) })
        }

        fn list_topics(&self) -> super::AdminFuture<'_, Self::TopicList, Self::Error> {
            Box::pin(async { Ok(AdminList::new(vec!["TopicTest".to_string()])) })
        }

        fn get_topic<'a>(&'a self, topic: &'a str) -> super::AdminFuture<'a, Self::Topic, Self::Error> {
            Box::pin(async move { Ok(topic.to_string()) })
        }

        fn topic_route<'a>(&'a self, topic: &'a str) -> super::AdminFuture<'a, Self::TopicRoute, Self::Error> {
            Box::pin(async move { Ok(format!("{topic}-route")) })
        }

        fn topic_stats<'a>(&'a self, topic: &'a str) -> super::AdminFuture<'a, Self::TopicStats, Self::Error> {
            Box::pin(async move { Ok(format!("{topic}-stats")) })
        }

        fn create_or_update_topic(
            &self,
            request: Self::TopicMutationRequest,
        ) -> super::AdminFuture<'_, Self::TopicMutationResult, Self::Error> {
            Box::pin(async move { Ok(request) })
        }

        fn delete_topic<'a>(
            &'a self,
            topic: &'a str,
        ) -> super::AdminFuture<'a, Self::TopicMutationResult, Self::Error> {
            Box::pin(async move { Ok(format!("deleted {topic}")) })
        }

        fn list_consumer_groups(&self) -> super::AdminFuture<'_, Self::ConsumerList, Self::Error> {
            Box::pin(async { Ok(AdminList::new(vec!["group-a".to_string()])) })
        }

        fn consumer_progress<'a>(
            &'a self,
            group: &'a str,
        ) -> super::AdminFuture<'a, Self::ConsumerProgress, Self::Error> {
            Box::pin(async move { Ok(group.to_string()) })
        }

        fn reset_consumer_offset(
            &self,
            group: String,
            request: Self::ConsumerOffsetResetRequest,
        ) -> super::AdminFuture<'_, Self::ConsumerOffsetResetResult, Self::Error> {
            Box::pin(async move { Ok(format!("{group}:{request}")) })
        }

        fn list_producers(&self) -> super::AdminFuture<'_, Self::ProducerList, Self::Error> {
            Box::pin(async { Ok(AdminList::new(vec!["producer-a".to_string()])) })
        }

        fn producer_connections(
            &self,
            topic: Option<String>,
            producer_group: Option<String>,
        ) -> super::AdminFuture<'_, Self::ProducerConnections, Self::Error> {
            Box::pin(async move {
                Ok(AdminList::new(vec![format!(
                    "{}:{}",
                    topic.unwrap_or_default(),
                    producer_group.unwrap_or_default()
                )]))
            })
        }

        fn list_brokers(&self) -> super::AdminFuture<'_, Self::BrokerList, Self::Error> {
            Box::pin(async { Ok(AdminList::new(vec!["broker-a".to_string()])) })
        }

        fn broker_runtime_stats<'a>(
            &'a self,
            broker_name: &'a str,
        ) -> super::AdminFuture<'a, Self::BrokerRuntimeStats, Self::Error> {
            Box::pin(async move { Ok(format!("{broker_name}-runtime")) })
        }

        fn broker_config<'a>(
            &'a self,
            broker_name: &'a str,
        ) -> super::AdminFuture<'a, Self::BrokerConfig, Self::Error> {
            Box::pin(async move { Ok(format!("{broker_name}-config")) })
        }

        fn update_broker_config(
            &self,
            broker_name: String,
            request: Self::BrokerConfigUpdateRequest,
        ) -> super::AdminFuture<'_, Self::BrokerConfigUpdateResult, Self::Error> {
            Box::pin(async move { Ok(format!("{broker_name}:{request}")) })
        }

        fn query_message_by_key(
            &self,
            topic: String,
            key: String,
        ) -> super::AdminFuture<'_, Self::MessageList, Self::Error> {
            Box::pin(async move { Ok(AdminList::new(vec![format!("{topic}:{key}")])) })
        }

        fn query_message_by_id(&self, message_id: String) -> super::AdminFuture<'_, Self::Message, Self::Error> {
            Box::pin(async move { Ok(message_id) })
        }

        fn message_trace(
            &self,
            message_id: String,
            topic: String,
            trace_topic: Option<String>,
        ) -> super::AdminFuture<'_, Self::MessageTrace, Self::Error> {
            Box::pin(async move { Ok(format!("{topic}:{message_id}:{}", trace_topic.unwrap_or_default())) })
        }

        fn resend_message(
            &self,
            message_id: String,
            request: Self::MessageResendRequest,
        ) -> super::AdminFuture<'_, Self::MessageResendResult, Self::Error> {
            Box::pin(async move { Ok(format!("{message_id}:{request}")) })
        }
    }

    #[tokio::test]
    async fn facade_forwards_to_provider() {
        let facade = DashboardAdminFacade::new(FakeProvider);

        let topics = facade.list_topics().await.expect("list topics");
        assert_eq!(topics.total, 1);
        assert_eq!(topics.items[0], "TopicTest");

        let route = facade.topic_route("TopicTest").await.expect("topic route");
        assert_eq!(route, "TopicTest-route");
    }
}
