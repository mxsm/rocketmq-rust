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

use std::sync::Arc;
use std::time::SystemTime;

use rocketmq_rust::ArcMut;
use tracing::debug;
use tracing::error;
use tracing::info;

use crate::controller::raft_controller::RaftController;
use crate::controller::Controller;
use crate::error::ControllerError;
use crate::error::Result;
use crate::metadata::BrokerInfo;
use crate::metadata::MetadataStore;
use crate::processor::request::BrokerHeartbeatRequest;
use crate::processor::request::BrokerHeartbeatResponse;
use crate::processor::request::ElectMasterRequest;
use crate::processor::request::ElectMasterResponse;
use crate::processor::request::RegisterBrokerRequest;
use crate::processor::request::RegisterBrokerResponse;
use crate::processor::request::UnregisterBrokerRequest;
use crate::processor::request::UnregisterBrokerResponse;
use crate::processor::RequestProcessor;

/// Register broker processor
pub struct RegisterBrokerProcessor {
    /// Metadata store
    metadata: Arc<MetadataStore>,

    /// Raft controller
    raft: ArcMut<RaftController>,
}

impl RegisterBrokerProcessor {
    /// Create a new register broker processor
    pub fn new(metadata: Arc<MetadataStore>, raft: ArcMut<RaftController>) -> Self {
        Self { metadata, raft }
    }

    /// Process register broker request
    pub async fn process_request(&self, request: RegisterBrokerRequest) -> Result<RegisterBrokerResponse> {
        info!("Processing register broker request: {}", request.broker_name);

        // Check if we are the leader
        if !self.raft.is_leader() {
            error!("Not leader");
            return Ok(RegisterBrokerResponse {
                success: false,
                error: Some("Not leader".to_string()),
                broker_id: None,
            });
        }

        // Create broker info
        let broker_info = BrokerInfo {
            name: request.broker_name.clone(),
            broker_id: request.broker_id,
            cluster_name: request.cluster_name.clone(),
            addr: request.broker_addr,
            last_heartbeat: SystemTime::now(),
            version: request.version.clone(),
            role: request.role,
            metadata: request.metadata.clone(),
        };

        // Register broker
        match self.metadata.broker_manager().register(broker_info).await {
            Ok(()) => {
                info!("Successfully registered broker: {}", request.broker_name);
                Ok(RegisterBrokerResponse {
                    success: true,
                    error: None,
                    broker_id: Some(request.broker_id),
                })
            }
            Err(e) => {
                error!("Failed to register broker {}: {}", request.broker_name, e);
                Ok(RegisterBrokerResponse {
                    success: false,
                    error: Some(e.to_string()),
                    broker_id: None,
                })
            }
        }
    }
}

#[async_trait::async_trait]
impl RequestProcessor for RegisterBrokerProcessor {
    async fn process(&self, request: &[u8]) -> Result<Vec<u8>> {
        // Deserialize request
        let req: RegisterBrokerRequest =
            serde_json::from_slice(request).map_err(|e| ControllerError::InvalidRequest(e.to_string()))?;

        // Process request
        let response = self.process_request(req).await?;

        // Serialize response
        serde_json::to_vec(&response).map_err(|e| ControllerError::SerializationError(e.to_string()))
    }
}

/// Unregister broker processor
pub struct UnregisterBrokerProcessor {
    /// Metadata store
    metadata: Arc<MetadataStore>,

    /// Raft controller
    raft: ArcMut<RaftController>,
}

impl UnregisterBrokerProcessor {
    /// Create a new unregister broker processor
    pub fn new(metadata: Arc<MetadataStore>, raft: ArcMut<RaftController>) -> Self {
        Self { metadata, raft }
    }

    /// Process unregister broker request
    pub async fn process_request(&self, request: UnregisterBrokerRequest) -> Result<UnregisterBrokerResponse> {
        info!("Processing unregister broker request: {}", request.broker_name);

        // Check if we are the leader
        if !self.raft.is_leader() {
            return Ok(UnregisterBrokerResponse {
                success: false,
                error: Some("Not leader".to_string()),
            });
        }

        // Unregister broker
        match self.metadata.broker_manager().unregister(&request.broker_name).await {
            Ok(()) => {
                info!("Successfully unregistered broker: {}", request.broker_name);
                Ok(UnregisterBrokerResponse {
                    success: true,
                    error: None,
                })
            }
            Err(e) => {
                error!("Failed to unregister broker {}: {}", request.broker_name, e);
                Ok(UnregisterBrokerResponse {
                    success: false,
                    error: Some(e.to_string()),
                })
            }
        }
    }
}

#[async_trait::async_trait]
impl RequestProcessor for UnregisterBrokerProcessor {
    async fn process(&self, request: &[u8]) -> Result<Vec<u8>> {
        let req: UnregisterBrokerRequest =
            serde_json::from_slice(request).map_err(|e| ControllerError::InvalidRequest(e.to_string()))?;

        let response = self.process_request(req).await?;

        serde_json::to_vec(&response).map_err(|e| ControllerError::SerializationError(e.to_string()))
    }
}

/// Broker heartbeat processor
pub struct BrokerHeartbeatProcessor {
    /// Metadata store
    metadata: Arc<MetadataStore>,
}

impl BrokerHeartbeatProcessor {
    /// Create a new broker heartbeat processor
    pub fn new(metadata: Arc<MetadataStore>) -> Self {
        Self { metadata }
    }

    /// Process broker heartbeat request
    pub async fn process_request(&self, request: BrokerHeartbeatRequest) -> Result<BrokerHeartbeatResponse> {
        debug!("Processing broker heartbeat: {}", request.broker_name);

        // Update heartbeat
        match self.metadata.broker_manager().heartbeat(&request.broker_name).await {
            Ok(()) => {
                debug!("Successfully updated heartbeat for broker: {}", request.broker_name);
                Ok(BrokerHeartbeatResponse {
                    success: true,
                    error: None,
                })
            }
            Err(e) => {
                error!("Failed to update heartbeat for broker {}: {}", request.broker_name, e);
                Ok(BrokerHeartbeatResponse {
                    success: false,
                    error: Some(e.to_string()),
                })
            }
        }
    }
}

#[async_trait::async_trait]
impl RequestProcessor for BrokerHeartbeatProcessor {
    async fn process(&self, request: &[u8]) -> Result<Vec<u8>> {
        let req: BrokerHeartbeatRequest =
            serde_json::from_slice(request).map_err(|e| ControllerError::InvalidRequest(e.to_string()))?;

        let response = self.process_request(req).await?;

        serde_json::to_vec(&response).map_err(|e| ControllerError::SerializationError(e.to_string()))
    }
}

/// Elect master processor
pub struct ElectMasterProcessor {
    /// Metadata store
    metadata: Arc<MetadataStore>,

    /// Raft controller
    raft: ArcMut<RaftController>,
}

impl ElectMasterProcessor {
    /// Create a new elect master processor
    pub fn new(metadata: Arc<MetadataStore>, raft: ArcMut<RaftController>) -> Self {
        Self { metadata, raft }
    }

    /// Process elect master request
    pub async fn process_request(&self, request: ElectMasterRequest) -> Result<ElectMasterResponse> {
        info!(
            "Processing elect master request for cluster: {}, broker: {}",
            request.cluster_name, request.broker_name
        );

        // Check if we are the leader
        if !self.raft.is_leader() {
            return Ok(ElectMasterResponse {
                success: false,
                error: Some("Not leader".to_string()),
                master_broker: None,
                master_addr: None,
            });
        }

        // Get brokers in the cluster
        let brokers = self
            .metadata
            .broker_manager()
            .list_brokers_by_cluster(&request.cluster_name)
            .await;

        if brokers.is_empty() {
            return Ok(ElectMasterResponse {
                success: false,
                error: Some("No brokers found in cluster".to_string()),
                master_broker: None,
                master_addr: None,
            });
        }

        // Find the master broker (simple logic: first broker with Master role)
        let master = brokers.iter().find(|b| b.role == crate::metadata::BrokerRole::Master);

        match master {
            Some(broker) => {
                info!("Elected master broker: {}", broker.name);
                Ok(ElectMasterResponse {
                    success: true,
                    error: None,
                    master_broker: Some(broker.name.clone()),
                    master_addr: Some(broker.addr),
                })
            }
            None => Ok(ElectMasterResponse {
                success: false,
                error: Some("No master broker found in cluster".to_string()),
                master_broker: None,
                master_addr: None,
            }),
        }
    }
}

#[async_trait::async_trait]
impl RequestProcessor for ElectMasterProcessor {
    async fn process(&self, request: &[u8]) -> Result<Vec<u8>> {
        let req: ElectMasterRequest =
            serde_json::from_slice(request).map_err(|e| ControllerError::InvalidRequest(e.to_string()))?;

        let response = self.process_request(req).await?;

        serde_json::to_vec(&response).map_err(|e| ControllerError::SerializationError(e.to_string()))
    }
}
