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

use rocketmq_rust::ArcMut;
use tracing::error;
use tracing::info;

use crate::controller::raft_controller::RaftController;
use crate::controller::Controller;
use crate::error::ControllerError;
use crate::error::Result;
use crate::metadata::MetadataStore;
use crate::metadata::TopicConfig;
use crate::processor::request::CreateTopicRequest;
use crate::processor::request::CreateTopicResponse;
use crate::processor::request::DeleteTopicRequest;
use crate::processor::request::DeleteTopicResponse;
use crate::processor::request::UpdateTopicRequest;
use crate::processor::request::UpdateTopicResponse;
use crate::processor::RequestProcessor;

/// Create topic processor
pub struct CreateTopicProcessor {
    /// Metadata store
    metadata: Arc<MetadataStore>,

    /// Raft controller
    raft: ArcMut<RaftController>,
}

impl CreateTopicProcessor {
    /// Create a new create topic processor
    pub fn new(metadata: Arc<MetadataStore>, raft: ArcMut<RaftController>) -> Self {
        Self { metadata, raft }
    }

    /// Process create topic request
    pub async fn process_request(&self, request: CreateTopicRequest) -> Result<CreateTopicResponse> {
        info!("Processing create topic request: {}", request.topic_name);

        // Check if we are the leader
        if !self.raft.is_leader() {
            error!("Not leader");
            return Ok(CreateTopicResponse {
                success: false,
                error: Some("Not leader".to_string()),
            });
        }

        // Validate request
        if request.read_queue_nums == 0 || request.write_queue_nums == 0 {
            return Ok(CreateTopicResponse {
                success: false,
                error: Some("Queue nums must be greater than 0".to_string()),
            });
        }

        // Create topic config
        let config = TopicConfig {
            topic_name: request.topic_name.clone(),
            read_queue_nums: request.read_queue_nums,
            write_queue_nums: request.write_queue_nums,
            perm: request.perm,
            topic_filter_type: 0,
            topic_sys_flag: request.topic_sys_flag,
            order: false,
            attributes: serde_json::Value::Null,
        };

        // Create topic
        match self.metadata.topic_manager().create_topic(config).await {
            Ok(()) => {
                info!("Successfully created topic: {}", request.topic_name);
                Ok(CreateTopicResponse {
                    success: true,
                    error: None,
                })
            }
            Err(e) => {
                error!("Failed to create topic {}: {}", request.topic_name, e);
                Ok(CreateTopicResponse {
                    success: false,
                    error: Some(e.to_string()),
                })
            }
        }
    }
}

#[async_trait::async_trait]
impl RequestProcessor for CreateTopicProcessor {
    async fn process(&self, request: &[u8]) -> Result<Vec<u8>> {
        let req: CreateTopicRequest =
            serde_json::from_slice(request).map_err(|e| ControllerError::InvalidRequest(e.to_string()))?;

        let response = self.process_request(req).await?;

        serde_json::to_vec(&response).map_err(|e| ControllerError::SerializationError(e.to_string()))
    }
}

/// Update topic processor
pub struct UpdateTopicProcessor {
    /// Metadata store
    metadata: Arc<MetadataStore>,

    /// Raft controller
    raft: ArcMut<RaftController>,
}

impl UpdateTopicProcessor {
    /// Create a new update topic processor
    pub fn new(metadata: Arc<MetadataStore>, raft: ArcMut<RaftController>) -> Self {
        Self { metadata, raft }
    }

    /// Process update topic request
    pub async fn process_request(&self, request: UpdateTopicRequest) -> Result<UpdateTopicResponse> {
        info!("Processing update topic request: {}", request.topic_name);

        // Check if we are the leader
        if !self.raft.is_leader() {
            return Ok(UpdateTopicResponse {
                success: false,
                error: Some("Not leader".to_string()),
            });
        }

        // Create updated config
        let config = TopicConfig {
            topic_name: request.topic_name.clone(),
            read_queue_nums: request.topic_info.read_queue_nums,
            write_queue_nums: request.topic_info.write_queue_nums,
            perm: request.topic_info.perm,
            topic_filter_type: 0,
            topic_sys_flag: request.topic_info.topic_sys_flag,
            order: false,
            attributes: request.topic_info.metadata.clone(),
        };

        // Update topic
        match self.metadata.topic_manager().update_topic(config).await {
            Ok(()) => {
                info!("Successfully updated topic: {}", request.topic_name);
                Ok(UpdateTopicResponse {
                    success: true,
                    error: None,
                })
            }
            Err(e) => {
                error!("Failed to update topic {}: {}", request.topic_name, e);
                Ok(UpdateTopicResponse {
                    success: false,
                    error: Some(e.to_string()),
                })
            }
        }
    }
}

#[async_trait::async_trait]
impl RequestProcessor for UpdateTopicProcessor {
    async fn process(&self, request: &[u8]) -> Result<Vec<u8>> {
        let req: UpdateTopicRequest =
            serde_json::from_slice(request).map_err(|e| ControllerError::InvalidRequest(e.to_string()))?;

        let response = self.process_request(req).await?;

        serde_json::to_vec(&response).map_err(|e| ControllerError::SerializationError(e.to_string()))
    }
}

/// Delete topic processor
pub struct DeleteTopicProcessor {
    /// Metadata store
    metadata: Arc<MetadataStore>,

    /// Raft controller
    raft: ArcMut<RaftController>,
}

impl DeleteTopicProcessor {
    /// Create a new delete topic processor
    pub fn new(metadata: Arc<MetadataStore>, raft: ArcMut<RaftController>) -> Self {
        Self { metadata, raft }
    }

    /// Process delete topic request
    pub async fn process_request(&self, request: DeleteTopicRequest) -> Result<DeleteTopicResponse> {
        info!("Processing delete topic request: {}", request.topic_name);

        // Check if we are the leader
        if !self.raft.is_leader() {
            return Ok(DeleteTopicResponse {
                success: false,
                error: Some("Not leader".to_string()),
            });
        }

        // Delete topic
        match self.metadata.topic_manager().delete_topic(&request.topic_name).await {
            Ok(()) => {
                info!("Successfully deleted topic: {}", request.topic_name);
                Ok(DeleteTopicResponse {
                    success: true,
                    error: None,
                })
            }
            Err(e) => {
                error!("Failed to delete topic {}: {}", request.topic_name, e);
                Ok(DeleteTopicResponse {
                    success: false,
                    error: Some(e.to_string()),
                })
            }
        }
    }
}

#[async_trait::async_trait]
impl RequestProcessor for DeleteTopicProcessor {
    async fn process(&self, request: &[u8]) -> Result<Vec<u8>> {
        let req: DeleteTopicRequest =
            serde_json::from_slice(request).map_err(|e| ControllerError::InvalidRequest(e.to_string()))?;

        let response = self.process_request(req).await?;

        serde_json::to_vec(&response).map_err(|e| ControllerError::SerializationError(e.to_string()))
    }
}
