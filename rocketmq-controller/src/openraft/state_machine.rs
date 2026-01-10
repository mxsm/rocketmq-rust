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

//! Raft state machine implementation
//!
//! This module provides the state machine for OpenRaft, handling:
//! - Applying committed log entries
//! - Managing broker metadata
//! - Managing topic configurations
//! - Snapshot creation and installation

use std::sync::Arc;

use dashmap::DashMap;
use openraft::storage::RaftStateMachine;
use openraft::storage::Snapshot;
use openraft::EntryPayload;
use openraft::OptionalSend;
use openraft::RaftSnapshotBuilder;
use openraft::StoredMembership;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::RwLock;

use crate::typ::ControllerRequest;
use crate::typ::ControllerResponse;
use crate::typ::LogId;
use crate::typ::TypeConfig;

/// Broker metadata stored in state machine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerMetadata {
    pub broker_name: String,
    pub broker_addr: String,
    pub broker_id: u64,
    pub cluster_name: String,
    pub epoch: i32,
    pub max_offset: i64,
    pub election_priority: i64,
    pub last_heartbeat: i64,
}

/// Topic configuration stored in state machine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicConfig {
    pub topic_name: String,
    pub read_queue_nums: i32,
    pub write_queue_nums: i32,
    pub perm: i32,
}

/// Snapshot data structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotData {
    pub brokers: Vec<(String, BrokerMetadata)>,
    pub topics: Vec<(String, TopicConfig)>,
    pub configs: Vec<(String, String)>,
    pub last_applied: Option<LogId>,
    pub last_membership: Option<StoredMembership<TypeConfig>>,
}

/// Raft state machine for controller
///
/// This stores all the metadata and state for the controller cluster.
#[derive(Debug, Clone)]
pub struct StateMachine {
    /// Broker metadata indexed by broker name
    brokers: Arc<DashMap<String, BrokerMetadata>>,
    /// Topic configurations indexed by topic name
    topics: Arc<DashMap<String, TopicConfig>>,
    /// Controller configurations
    configs: Arc<DashMap<String, String>>,
    /// Last applied log ID
    last_applied: Arc<RwLock<Option<LogId>>>,
    /// Last membership configuration
    last_membership: Arc<RwLock<StoredMembership<TypeConfig>>>,
}

impl Default for StateMachine {
    fn default() -> Self {
        Self::new()
    }
}

impl StateMachine {
    /// Create a new state machine
    pub fn new() -> Self {
        Self {
            brokers: Arc::new(DashMap::new()),
            topics: Arc::new(DashMap::new()),
            configs: Arc::new(DashMap::new()),
            last_applied: Arc::new(RwLock::new(None)),
            last_membership: Arc::new(RwLock::new(StoredMembership::default())),
        }
    }

    /// Apply a request to the state machine
    fn apply_request(&self, request: &ControllerRequest) -> ControllerResponse {
        match request {
            ControllerRequest::RegisterBroker {
                broker_name,
                broker_addr,
                broker_id,
                cluster_name,
                epoch,
                max_offset,
                election_priority,
            } => {
                let metadata = BrokerMetadata {
                    broker_name: broker_name.clone(),
                    broker_addr: broker_addr.clone(),
                    broker_id: *broker_id,
                    cluster_name: cluster_name.clone(),
                    epoch: *epoch,
                    max_offset: *max_offset,
                    election_priority: *election_priority,
                    last_heartbeat: chrono::Utc::now().timestamp_millis(),
                };

                self.brokers.insert(broker_name.clone(), metadata);

                ControllerResponse::RegisterBroker {
                    success: true,
                    error: None,
                    master_addr: Some(broker_addr.clone()),
                    master_epoch: Some(*epoch),
                    sync_state_set_epoch: Some(0),
                }
            }
            ControllerRequest::BrokerHeartbeat {
                broker_name,
                broker_addr,
                broker_id,
                epoch,
                max_offset,
                confirm_offset: _,
                heartbeat_timeout_millis: _,
            } => {
                if let Some(mut broker) = self.brokers.get_mut(broker_name) {
                    broker.broker_addr = broker_addr.clone();
                    broker.broker_id = *broker_id;
                    broker.epoch = *epoch;
                    broker.max_offset = *max_offset;
                    broker.last_heartbeat = chrono::Utc::now().timestamp_millis();

                    ControllerResponse::BrokerHeartbeat {
                        success: true,
                        error: None,
                        is_master: true,
                        master_addr: Some(broker_addr.clone()),
                        master_epoch: Some(*epoch),
                        sync_state_set_epoch: Some(0),
                    }
                } else {
                    ControllerResponse::BrokerHeartbeat {
                        success: false,
                        error: Some(format!("Broker {} not registered", broker_name)),
                        is_master: false,
                        master_addr: None,
                        master_epoch: None,
                        sync_state_set_epoch: None,
                    }
                }
            }
            ControllerRequest::UpdateConfig { key, value } => {
                self.configs.insert(key.clone(), value.clone());
                ControllerResponse::Success
            }
            ControllerRequest::UpdateTopic {
                topic_name,
                read_queue_nums,
                write_queue_nums,
                perm,
            } => {
                let config = TopicConfig {
                    topic_name: topic_name.clone(),
                    read_queue_nums: *read_queue_nums,
                    write_queue_nums: *write_queue_nums,
                    perm: *perm,
                };

                self.topics.insert(topic_name.clone(), config);
                ControllerResponse::Success
            }
            ControllerRequest::ApplyBrokerId {
                cluster_name,
                broker_name,
                broker_addr,
                applied_broker_id,
                register_check_code: _,
            } => {
                if let Some(existing_broker) = self.brokers.get(broker_name) {
                    if existing_broker.broker_id == *applied_broker_id {
                        return ControllerResponse::ApplyBrokerId {
                            success: true,
                            error: None,
                            cluster_name: cluster_name.clone(),
                            broker_name: broker_name.clone(),
                        };
                    }
                }

                let id_in_use = self
                    .brokers
                    .iter()
                    .any(|entry| entry.value().broker_id == *applied_broker_id && entry.key() != broker_name);

                if id_in_use {
                    return ControllerResponse::ApplyBrokerId {
                        success: false,
                        error: Some(format!(
                            "Broker ID {} is already in use by another broker",
                            applied_broker_id
                        )),
                        cluster_name: cluster_name.clone(),
                        broker_name: broker_name.clone(),
                    };
                }

                let metadata = BrokerMetadata {
                    broker_name: broker_name.clone(),
                    broker_addr: broker_addr.clone(),
                    broker_id: *applied_broker_id,
                    cluster_name: cluster_name.clone(),
                    epoch: 0,
                    max_offset: 0,
                    election_priority: 0,
                    last_heartbeat: chrono::Utc::now().timestamp_millis(),
                };

                self.brokers.insert(broker_name.clone(), metadata);

                tracing::info!(
                    "Applied broker ID {} to broker {} in cluster {}",
                    applied_broker_id,
                    broker_name,
                    cluster_name
                );

                ControllerResponse::ApplyBrokerId {
                    success: true,
                    error: None,
                    cluster_name: cluster_name.clone(),
                    broker_name: broker_name.clone(),
                }
            }
        }
    }

    /// Build a snapshot of current state
    async fn build_snapshot_data(&self) -> SnapshotData {
        let brokers: Vec<_> = self
            .brokers
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();

        let topics: Vec<_> = self
            .topics
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();

        let configs: Vec<_> = self
            .configs
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();

        let last_applied = *self.last_applied.read().await;
        let last_membership = self.last_membership.read().await.clone();

        SnapshotData {
            brokers,
            topics,
            configs,
            last_applied,
            last_membership: Some(last_membership),
        }
    }

    /// Install snapshot data
    async fn install_snapshot_data(&self, data: SnapshotData) {
        self.brokers.clear();
        for (key, value) in data.brokers {
            self.brokers.insert(key, value);
        }

        self.topics.clear();
        for (key, value) in data.topics {
            self.topics.insert(key, value);
        }

        self.configs.clear();
        for (key, value) in data.configs {
            self.configs.insert(key, value);
        }

        *self.last_applied.write().await = data.last_applied;
        *self.last_membership.write().await = data.last_membership.unwrap_or_default();
    }

    /// Get broker metadata
    pub fn get_broker(&self, broker_name: &str) -> Option<BrokerMetadata> {
        self.brokers.get(broker_name).map(|v| v.clone())
    }

    /// Get all brokers
    pub fn get_all_brokers(&self) -> Vec<BrokerMetadata> {
        self.brokers.iter().map(|v| v.value().clone()).collect()
    }

    /// Get topic config
    pub fn get_topic(&self, topic_name: &str) -> Option<TopicConfig> {
        self.topics.get(topic_name).map(|v| v.clone())
    }

    /// Get all topics
    pub fn get_all_topics(&self) -> Vec<TopicConfig> {
        self.topics.iter().map(|v| v.value().clone()).collect()
    }
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, std::io::Error> {
        let data = self.build_snapshot_data().await;
        let last_applied = data.last_applied;
        let last_membership = data.last_membership.clone().unwrap_or_default();

        let snapshot_data = serde_json::to_vec(&data)
            .map_err(|e| std::io::Error::other(format!("Failed to serialize snapshot: {}", e)))?;

        Ok(Snapshot {
            meta: openraft::SnapshotMeta {
                last_log_id: last_applied,
                last_membership,
                snapshot_id: format!("snapshot-{}", last_applied.map_or(0, |id| id.index)),
            },
            snapshot: std::io::Cursor::new(snapshot_data),
        })
    }
}

impl RaftStateMachine<TypeConfig> for StateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(&mut self) -> Result<(Option<LogId>, StoredMembership<TypeConfig>), std::io::Error> {
        let last_applied = *self.last_applied.read().await;
        let last_membership = self.last_membership.read().await.clone();
        Ok((last_applied, last_membership))
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn apply<Strm>(&mut self, entries: Strm) -> Result<(), std::io::Error>
    where
        Strm: futures::Stream<Item = Result<openraft::storage::EntryResponder<TypeConfig>, std::io::Error>>
            + Unpin
            + OptionalSend,
    {
        use futures::StreamExt;

        futures::pin_mut!(entries);

        while let Some(entry_result) = entries.next().await {
            let (entry, responder) = entry_result?;
            let log_id = entry.log_id;
            tracing::debug!("Applying entry to state machine: {}", log_id);

            *self.last_applied.write().await = Some(log_id);

            let response = match entry.payload {
                EntryPayload::Blank => ControllerResponse::Success,
                EntryPayload::Normal(ref request) => self.apply_request(request),
                EntryPayload::Membership(ref membership) => {
                    *self.last_membership.write().await = StoredMembership::new(Some(log_id), membership.clone());
                    ControllerResponse::Success
                }
            };

            if let Some(tx) = responder {
                tx.send(response);
            }
        }

        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        Self {
            brokers: self.brokers.clone(),
            topics: self.topics.clone(),
            configs: self.configs.clone(),
            last_applied: self.last_applied.clone(),
            last_membership: self.last_membership.clone(),
        }
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<std::io::Cursor<Vec<u8>>, std::io::Error> {
        Ok(std::io::Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &openraft::SnapshotMeta<TypeConfig>,
        snapshot: std::io::Cursor<Vec<u8>>,
    ) -> Result<(), std::io::Error> {
        let snapshot_data: SnapshotData = serde_json::from_slice(snapshot.get_ref()).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to deserialize snapshot: {}", e),
            )
        })?;

        self.install_snapshot_data(snapshot_data).await;

        tracing::info!("Installed snapshot at {:?}", meta.last_log_id);
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, std::io::Error> {
        // For simplicity, we don't keep snapshots in memory
        // In production, you might want to cache the last snapshot
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_apply_broker_id_success() {
        let sm = StateMachine::new();

        let request = ControllerRequest::ApplyBrokerId {
            cluster_name: "test_cluster".to_string(),
            broker_name: "broker-1".to_string(),
            broker_addr: "127.0.0.1:10911".to_string(),
            applied_broker_id: 1,
            register_check_code: "check123".to_string(),
        };

        let response = sm.apply_request(&request);

        match response {
            ControllerResponse::ApplyBrokerId {
                success,
                error,
                cluster_name,
                broker_name,
            } => {
                assert!(success, "Should succeed for new broker");
                assert!(error.is_none(), "Should have no error");
                assert_eq!(cluster_name, "test_cluster");
                assert_eq!(broker_name, "broker-1");
            }
            _ => panic!("Expected ApplyBrokerId response"),
        }

        let broker = sm.get_broker("broker-1");
        assert!(broker.is_some());
        let broker = broker.unwrap();
        assert_eq!(broker.broker_id, 1);
        assert_eq!(broker.cluster_name, "test_cluster");
    }

    #[test]
    fn test_apply_broker_id_idempotent() {
        let sm = StateMachine::new();

        let request = ControllerRequest::ApplyBrokerId {
            cluster_name: "test_cluster".to_string(),
            broker_name: "broker-1".to_string(),
            broker_addr: "127.0.0.1:10911".to_string(),
            applied_broker_id: 1,
            register_check_code: "check123".to_string(),
        };

        sm.apply_request(&request);

        let response = sm.apply_request(&request);

        match response {
            ControllerResponse::ApplyBrokerId { success, .. } => {
                assert!(success, "Should succeed for idempotent request");
            }
            _ => panic!("Expected ApplyBrokerId response"),
        }
    }

    #[test]
    fn test_apply_broker_id_conflict() {
        let sm = StateMachine::new();

        let request1 = ControllerRequest::ApplyBrokerId {
            cluster_name: "test_cluster".to_string(),
            broker_name: "broker-1".to_string(),
            broker_addr: "127.0.0.1:10911".to_string(),
            applied_broker_id: 1,
            register_check_code: "check1".to_string(),
        };
        sm.apply_request(&request1);

        let request2 = ControllerRequest::ApplyBrokerId {
            cluster_name: "test_cluster".to_string(),
            broker_name: "broker-2".to_string(),
            broker_addr: "127.0.0.1:10912".to_string(),
            applied_broker_id: 1,
            register_check_code: "check2".to_string(),
        };
        let response = sm.apply_request(&request2);

        match response {
            ControllerResponse::ApplyBrokerId { success, error, .. } => {
                assert!(!success, "Should fail for conflicting ID");
                assert!(error.is_some(), "Should have error message");
                assert!(error.unwrap().contains("already in use"));
            }
            _ => panic!("Expected ApplyBrokerId response"),
        }
    }

    #[test]
    fn test_apply_broker_id_master() {
        let sm = StateMachine::new();

        let request = ControllerRequest::ApplyBrokerId {
            cluster_name: "production".to_string(),
            broker_name: "broker-master".to_string(),
            broker_addr: "127.0.0.1:10911".to_string(),
            applied_broker_id: 0,
            register_check_code: "master_code".to_string(),
        };

        let response = sm.apply_request(&request);

        match response {
            ControllerResponse::ApplyBrokerId { success, .. } => {
                assert!(success, "Should succeed for master broker ID 0");
            }
            _ => panic!("Expected ApplyBrokerId response"),
        }

        let broker = sm.get_broker("broker-master").unwrap();
        assert_eq!(broker.broker_id, 0);
    }

    #[test]
    fn test_apply_broker_id_different_brokers_different_ids() {
        let sm = StateMachine::new();

        let request1 = ControllerRequest::ApplyBrokerId {
            cluster_name: "cluster".to_string(),
            broker_name: "broker-1".to_string(),
            broker_addr: "127.0.0.1:10911".to_string(),
            applied_broker_id: 1,
            register_check_code: "".to_string(),
        };
        sm.apply_request(&request1);

        let request2 = ControllerRequest::ApplyBrokerId {
            cluster_name: "cluster".to_string(),
            broker_name: "broker-2".to_string(),
            broker_addr: "127.0.0.1:10912".to_string(),
            applied_broker_id: 2,
            register_check_code: "".to_string(),
        };
        let response = sm.apply_request(&request2);

        match response {
            ControllerResponse::ApplyBrokerId { success, .. } => {
                assert!(success, "Different brokers should get different IDs");
            }
            _ => panic!("Expected ApplyBrokerId response"),
        }

        assert_eq!(sm.get_broker("broker-1").unwrap().broker_id, 1);
        assert_eq!(sm.get_broker("broker-2").unwrap().broker_id, 2);
    }
}
