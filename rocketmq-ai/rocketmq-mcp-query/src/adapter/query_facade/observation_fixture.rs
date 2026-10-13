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

//! A session for the facade tests of the observations that read one thing.
//!
//! A test stores what a read answers and then counts the sessions and reads its workflow used.
//! A read that has nothing stored answers as a source that is down.

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;

use rocketmq_admin_core::core::broker::BrokerRuntimeTargetStatus;

use crate::adapter::admin_session::AdminSession;
use crate::adapter::admin_session::AdminSessionFactory;
use crate::adapter::admin_session::ResolvedCluster;
use crate::adapter::admin_session::SessionConnections;
use crate::adapter::admin_session::SessionConsumerClientRuntime;
use crate::adapter::admin_session::SessionConsumerLag;
use crate::adapter::admin_session::SessionMessageKeyMatches;
use crate::adapter::admin_session::SessionMessageTrace;
use crate::adapter::admin_session::SessionTopicConsumers;
use crate::adapter::admin_session::SessionTopicRoute;
use crate::adapter::admin_session::SessionTopicStats;
use crate::config::McpConfig;
use crate::model::contract::QueryPayload;
use crate::tools::broker_tools::GetLiteBrokerInfoOutput;
use crate::tools::cluster_tools::BrokerSummary;
use crate::tools::consumer_tools::ConsumerGroupSummary;
use crate::tools::consumer_tools::GetConsumerGroupDetailsOutput;
use crate::tools::executor::ToolExecutionError;
use crate::tools::executor::ToolFailure;

/// The Topic, the key and the window of one key search.
pub(super) type AskedSearch = (String, String, i64, i64);

#[derive(Clone, Default)]
pub(super) struct StubFactory {
    pub(super) started: Arc<AtomicUsize>,
    pub(super) closed: Arc<AtomicUsize>,
    pub(super) reads: Arc<AtomicUsize>,
    pub(super) topic_consumers: Option<QueryPayload<SessionTopicConsumers>>,
    pub(super) consumer_connections: Option<QueryPayload<SessionConnections>>,
    pub(super) consumer_client_runtime: Option<SessionConsumerClientRuntime>,
    /// The group, the Broker and the identifier of every client a session was asked about.
    pub(super) asked_clients: Arc<Mutex<Vec<(String, String, String)>>>,
    pub(super) topic_stats: Option<Result<QueryPayload<SessionTopicStats>, ToolFailure>>,
    pub(super) consumer_group_details: Option<Result<QueryPayload<GetConsumerGroupDetailsOutput>, ToolFailure>>,
    /// Every Topic a session was asked the statistics of.
    pub(super) asked_topics: Arc<Mutex<Vec<String>>>,
    pub(super) message_trace: Option<SessionMessageTrace>,
    /// Every message identifier a session was asked the trace of.
    pub(super) asked_messages: Arc<Mutex<Vec<String>>>,
    pub(super) message_key_matches: Option<SessionMessageKeyMatches>,
    /// Every key search a session ran.
    pub(super) asked_searches: Arc<Mutex<Vec<AskedSearch>>>,
    pub(super) lite_broker_info: Option<GetLiteBrokerInfoOutput>,
    /// Every Broker a session was asked the Lite Topic state of.
    pub(super) asked_brokers: Arc<Mutex<Vec<String>>>,
    #[cfg(feature = "security-admin")]
    pub(super) acl_users: Option<crate::tools::security_tools::ListAclUsersOutput>,
}

impl StubFactory {
    /// Returns how many sessions were started, how many were closed, and how many reads ran.
    pub(super) fn used(&self) -> (usize, usize, usize) {
        (
            self.started.load(Ordering::SeqCst),
            self.closed.load(Ordering::SeqCst),
            self.reads.load(Ordering::SeqCst),
        )
    }
}

impl AdminSessionFactory for StubFactory {
    type Session = StubSession;

    async fn start(&self, _cluster: ResolvedCluster) -> Result<Self::Session, ToolFailure> {
        self.started.fetch_add(1, Ordering::SeqCst);
        Ok(StubSession(self.clone()))
    }
}

pub(super) struct StubSession(StubFactory);

impl StubSession {
    fn answer<T: Clone>(&self, stored: &Option<T>) -> Result<T, ToolFailure> {
        self.0.reads.fetch_add(1, Ordering::SeqCst);
        stored
            .clone()
            .ok_or(ToolFailure::Operational(ToolExecutionError::Backend(None)))
    }

    /// Like [`answer`](Self::answer) for a read whose stored answer may itself be a failure.
    fn outcome<T: Clone>(&self, stored: &Option<Result<T, ToolFailure>>) -> Result<T, ToolFailure> {
        self.answer(stored)?
    }
}

impl AdminSession for StubSession {
    async fn broker_rows(&mut self) -> Result<QueryPayload<Vec<BrokerSummary>>, ToolFailure> {
        Ok(QueryPayload::complete(Vec::new()))
    }

    async fn topic_inventory(&mut self) -> Result<Vec<String>, ToolFailure> {
        Ok(Vec::new())
    }

    async fn topic_route(&mut self, _topic: &str) -> Result<SessionTopicRoute, ToolFailure> {
        Ok(SessionTopicRoute {
            brokers: Vec::new(),
            queues: Vec::new(),
        })
    }

    async fn topic_consumers(&mut self, _topic: &str) -> Result<QueryPayload<SessionTopicConsumers>, ToolFailure> {
        self.answer(&self.0.topic_consumers)
    }

    async fn topic_stats(&mut self, topic: &str) -> Result<QueryPayload<SessionTopicStats>, ToolFailure> {
        self.0.asked_topics.lock().unwrap().push(topic.to_string());
        self.outcome(&self.0.topic_stats)
    }

    async fn consumer_group_details(
        &mut self,
        _consumer_group: &str,
    ) -> Result<QueryPayload<GetConsumerGroupDetailsOutput>, ToolFailure> {
        self.outcome(&self.0.consumer_group_details)
    }

    async fn consumer_groups(&mut self) -> Result<QueryPayload<Vec<ConsumerGroupSummary>>, ToolFailure> {
        Ok(QueryPayload::complete(Vec::new()))
    }

    #[cfg(feature = "security-admin")]
    async fn acl_users(&mut self) -> Result<crate::tools::security_tools::ListAclUsersOutput, ToolFailure> {
        self.answer(&self.0.acl_users)
    }

    async fn lite_broker_info(&mut self, broker_name: &str) -> Result<GetLiteBrokerInfoOutput, ToolFailure> {
        self.0.asked_brokers.lock().unwrap().push(broker_name.to_string());
        self.answer(&self.0.lite_broker_info)
    }

    async fn message_trace(
        &mut self,
        message_id: &str,
        _trace_topic: Option<&str>,
    ) -> Result<SessionMessageTrace, ToolFailure> {
        self.0.asked_messages.lock().unwrap().push(message_id.to_string());
        self.answer(&self.0.message_trace)
    }

    async fn find_messages_by_key(
        &mut self,
        topic: &str,
        key: &str,
        begin_millis: i64,
        end_millis: i64,
    ) -> Result<SessionMessageKeyMatches, ToolFailure> {
        self.0
            .asked_searches
            .lock()
            .unwrap()
            .push((topic.to_string(), key.to_string(), begin_millis, end_millis));
        self.answer(&self.0.message_key_matches)
    }

    async fn consumer_connections(
        &mut self,
        _consumer_group: &str,
    ) -> Result<QueryPayload<SessionConnections>, ToolFailure> {
        self.answer(&self.0.consumer_connections)
    }

    async fn consumer_client_runtime(
        &mut self,
        consumer_group: &str,
        broker_name: &str,
        client_id: &str,
    ) -> Result<SessionConsumerClientRuntime, ToolFailure> {
        self.0.asked_clients.lock().unwrap().push((
            consumer_group.to_string(),
            broker_name.to_string(),
            client_id.to_string(),
        ));
        self.answer(&self.0.consumer_client_runtime)
    }

    async fn consumer_lag(
        &mut self,
        _topic: &str,
        _consumer_group: &str,
    ) -> Result<QueryPayload<SessionConsumerLag>, ToolFailure> {
        Ok(QueryPayload::complete(SessionConsumerLag {
            queues: Vec::new(),
            total_lag: 0,
            consume_tps: 0.0,
            inflight_total: 0,
        }))
    }

    async fn probe_broker_runtime_target(
        &mut self,
        _broker_name: &str,
    ) -> Result<BrokerRuntimeTargetStatus, ToolFailure> {
        Ok(BrokerRuntimeTargetStatus::NotFound)
    }

    async fn shutdown(self) -> Result<(), ToolFailure> {
        self.0.closed.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// The checked-in example configuration, whose only cluster is `local-dev`.
pub(super) fn config() -> McpConfig {
    McpConfig::load(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("conf")
            .join("mcp.example.toml"),
    )
    .unwrap()
}
