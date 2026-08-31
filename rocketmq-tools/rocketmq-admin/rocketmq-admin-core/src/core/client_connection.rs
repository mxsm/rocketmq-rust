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

//! Read-only producer and consumer client-connection contracts.

use serde::Deserialize;
use serde::Serialize;

use crate::core::error::required;
use crate::core::query::AdminQueryResult;
use crate::core::AdminError;
use crate::core::AdminFuture;
use crate::core::AdminResult;

pub const MAX_CLIENT_CONNECTION_ROWS: usize = 10_000;
pub const MAX_TOPIC_PRODUCER_BROKERS: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryConsumerConnectionsRequest {
    pub cluster: String,
    pub consumer_group: String,
    pub broker_name: Option<String>,
    pub max_connections: usize,
}

impl QueryConsumerConnectionsRequest {
    pub fn try_new(
        cluster: impl Into<String>,
        consumer_group: impl Into<String>,
        max_connections: usize,
    ) -> AdminResult<Self> {
        Ok(Self {
            cluster: required("cluster", cluster)?,
            consumer_group: required("consumer_group", consumer_group)?,
            broker_name: None,
            max_connections: validated_limit(max_connections)?,
        })
    }

    pub fn with_broker_name(mut self, broker_name: impl Into<String>) -> AdminResult<Self> {
        self.broker_name = Some(required("broker_name", broker_name)?);
        Ok(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListProducerConnectionsRequest {
    pub cluster: String,
    pub producer_group: Option<String>,
    pub broker_name: Option<String>,
    pub max_connections: usize,
}

impl ListProducerConnectionsRequest {
    pub fn try_new(cluster: impl Into<String>, max_connections: usize) -> AdminResult<Self> {
        Ok(Self {
            cluster: required("cluster", cluster)?,
            producer_group: None,
            broker_name: None,
            max_connections: validated_limit(max_connections)?,
        })
    }

    pub fn with_producer_group(mut self, producer_group: impl Into<String>) -> AdminResult<Self> {
        self.producer_group = Some(required("producer_group", producer_group)?);
        Ok(self)
    }

    pub fn with_broker_name(mut self, broker_name: impl Into<String>) -> AdminResult<Self> {
        self.broker_name = Some(required("broker_name", broker_name)?);
        Ok(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientConnectionObservation {
    pub broker_name: String,
    pub client_id: String,
    pub client_addr: String,
    pub language: String,
    pub version: i32,
    pub last_update_timestamp: Option<i64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryConsumerConnectionsResult {
    pub consumer_group: String,
    pub connections: Vec<ClientConnectionObservation>,
    pub queried_broker_count: usize,
    pub failed_brokers: Vec<String>,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProducerConnectionObservation {
    pub producer_group: String,
    pub connection: ClientConnectionObservation,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListProducerConnectionsResult {
    pub connections: Vec<ProducerConnectionObservation>,
    pub queried_broker_count: usize,
    pub failed_brokers: Vec<String>,
    pub truncated: bool,
}

/// Exact Topic and Producer-group selection for a bounded connection query.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryTopicProducerConnectionsRequest {
    pub cluster: String,
    pub topic: String,
    pub producer_group: String,
    pub max_connections: usize,
}

impl QueryTopicProducerConnectionsRequest {
    pub fn try_new(
        cluster: impl Into<String>,
        topic: impl Into<String>,
        producer_group: impl Into<String>,
        max_connections: usize,
    ) -> AdminResult<Self> {
        Ok(Self {
            cluster: required("cluster", cluster)?,
            topic: required("topic", topic)?,
            producer_group: required("producer_group", producer_group)?,
            max_connections: validated_limit(max_connections)?,
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryTopicProducerConnectionsResult {
    pub topic: String,
    pub producer_group: String,
    pub connections: Vec<ClientConnectionObservation>,
    pub queried_broker_count: usize,
    pub failed_brokers: Vec<String>,
    pub truncated: bool,
}

/// Producer and consumer connection queries available to read-only
/// integrations. The contract deliberately contains no mutation operations.
pub trait ClientConnectionQueryAdmin: Send {
    fn query_consumer_connections<'a>(
        &'a mut self,
        request: &'a QueryConsumerConnectionsRequest,
    ) -> AdminFuture<'a, QueryConsumerConnectionsResult>;

    /// Evidence-aware sibling of [`Self::query_consumer_connections`].
    fn query_consumer_connections_with_evidence<'a>(
        &'a mut self,
        request: &'a QueryConsumerConnectionsRequest,
    ) -> AdminFuture<'a, AdminQueryResult<QueryConsumerConnectionsResult>> {
        Box::pin(async move {
            self.query_consumer_connections(request)
                .await
                .map(AdminQueryResult::complete)
        })
    }

    fn list_producer_connections<'a>(
        &'a mut self,
        request: &'a ListProducerConnectionsRequest,
    ) -> AdminFuture<'a, ListProducerConnectionsResult>;

    /// Evidence-aware sibling of [`Self::list_producer_connections`].
    fn list_producer_connections_with_evidence<'a>(
        &'a mut self,
        request: &'a ListProducerConnectionsRequest,
    ) -> AdminFuture<'a, AdminQueryResult<ListProducerConnectionsResult>> {
        Box::pin(async move {
            self.list_producer_connections(request)
                .await
                .map(AdminQueryResult::complete)
        })
    }

    /// Queries one exact Producer group only at Brokers advertised by both the
    /// selected cluster and the selected Topic route.
    fn query_topic_producer_connections<'a>(
        &'a mut self,
        _request: &'a QueryTopicProducerConnectionsRequest,
    ) -> AdminFuture<'a, QueryTopicProducerConnectionsResult> {
        Box::pin(async {
            Err(AdminError::backend(
                "query_topic_producer_connections",
                "Topic-scoped Producer connections are not implemented by this adapter",
            ))
        })
    }

    /// Evidence-aware sibling of [`Self::query_topic_producer_connections`].
    fn query_topic_producer_connections_with_evidence<'a>(
        &'a mut self,
        request: &'a QueryTopicProducerConnectionsRequest,
    ) -> AdminFuture<'a, AdminQueryResult<QueryTopicProducerConnectionsResult>> {
        Box::pin(async move {
            self.query_topic_producer_connections(request)
                .await
                .map(AdminQueryResult::complete)
        })
    }
}

fn validated_limit(limit: usize) -> AdminResult<usize> {
    if (1..=MAX_CLIENT_CONNECTION_ROWS).contains(&limit) {
        Ok(limit)
    } else {
        Err(AdminError::invalid_argument(
            "max_connections",
            format!("must be between 1 and {MAX_CLIENT_CONNECTION_ROWS}"),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_reject_unbounded_or_blank_queries() {
        assert!(QueryConsumerConnectionsRequest::try_new("cluster-a", "group-a", 1).is_ok());
        assert!(QueryConsumerConnectionsRequest::try_new("", "group-a", 1).is_err());
        assert!(QueryConsumerConnectionsRequest::try_new("cluster-a", "", 1).is_err());
        assert!(QueryConsumerConnectionsRequest::try_new("cluster-a", "group-a", 0).is_err());
        assert!(ListProducerConnectionsRequest::try_new("cluster-a", MAX_CLIENT_CONNECTION_ROWS + 1).is_err());
        assert!(QueryTopicProducerConnectionsRequest::try_new("cluster-a", "orders", "producer-a", 1).is_ok());
        assert!(QueryTopicProducerConnectionsRequest::try_new("cluster-a", "", "producer-a", 1).is_err());
        assert!(QueryTopicProducerConnectionsRequest::try_new("cluster-a", "orders", "", 1).is_err());
    }

    #[test]
    fn optional_filters_are_trimmed_and_required() {
        let producer = ListProducerConnectionsRequest::try_new(" cluster-a ", 100)
            .expect("request")
            .with_producer_group(" producer-a ")
            .expect("producer filter")
            .with_broker_name(" broker-a ")
            .expect("broker filter");
        assert_eq!(producer.cluster, "cluster-a");
        assert_eq!(producer.producer_group.as_deref(), Some("producer-a"));
        assert_eq!(producer.broker_name.as_deref(), Some("broker-a"));
        assert!(QueryConsumerConnectionsRequest::try_new("cluster-a", "group-a", 100)
            .expect("request")
            .with_broker_name(" ")
            .is_err());
    }
}
