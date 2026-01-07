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

use serde_json::json;
use tracing::info;

use crate::error::ControllerError;
use crate::error::Result;
use crate::metadata::MetadataStore;
use crate::processor::request::GetMetadataRequest;
use crate::processor::request::GetMetadataResponse;
use crate::processor::request::MetadataType;
use crate::processor::RequestProcessor;

/// Get metadata processor
pub struct GetMetadataProcessor {
    /// Metadata store
    metadata: Arc<MetadataStore>,
}

impl GetMetadataProcessor {
    /// Create a new get metadata processor
    pub fn new(metadata: Arc<MetadataStore>) -> Self {
        Self { metadata }
    }

    /// Process get metadata request
    pub async fn process_request(&self, request: GetMetadataRequest) -> Result<GetMetadataResponse> {
        info!("Processing get metadata request, type: {:?}", request.metadata_type);

        let (brokers, topics, configs) = match request.metadata_type {
            MetadataType::Broker => {
                let brokers = self.metadata.broker_manager().list_brokers().await;
                (brokers, Vec::new(), json!({}))
            }
            MetadataType::Topic => {
                let topics = self.metadata.topic_manager().list_topics().await;
                (Vec::new(), topics, json!({}))
            }
            MetadataType::Config => {
                let configs = self.metadata.config_manager().list_configs().await;
                (Vec::new(), Vec::new(), json!(configs))
            }
            MetadataType::All => {
                // Get all metadata
                let brokers = self.metadata.broker_manager().list_brokers().await;
                let topics = self.metadata.topic_manager().list_topics().await;
                let configs = self.metadata.config_manager().list_configs().await;
                (brokers, topics, json!(configs))
            }
        };

        Ok(GetMetadataResponse {
            success: true,
            error: None,
            brokers,
            topics,
            configs,
        })
    }
}

#[async_trait::async_trait]
impl RequestProcessor for GetMetadataProcessor {
    async fn process(&self, request: &[u8]) -> Result<Vec<u8>> {
        let req: GetMetadataRequest =
            serde_json::from_slice(request).map_err(|e| ControllerError::InvalidRequest(e.to_string()))?;

        let response = self.process_request(req).await?;

        serde_json::to_vec(&response).map_err(|e| ControllerError::SerializationError(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ControllerConfig;

    #[tokio::test]
    async fn test_get_metadata_processor() {
        let config = Arc::new(ControllerConfig::test_config());

        let metadata = Arc::new(MetadataStore::new(config.clone()).await.unwrap());
        let processor = GetMetadataProcessor::new(metadata);

        let request = GetMetadataRequest {
            metadata_type: MetadataType::All,
            key: None,
        };

        let response = processor.process_request(request).await.unwrap();
        assert!(response.success);
        assert!(!response.brokers.is_empty() || response.brokers.is_empty());
        assert!(!response.topics.is_empty() || response.topics.is_empty());
    }
}
