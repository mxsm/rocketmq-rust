/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

pub mod broker_processor;
pub mod controller_request_processor;
pub mod metadata_processor;
pub mod request;
pub mod topic_processor;

use std::collections::HashMap;
use std::sync::Arc;

// Re-export processors
use crate::config::ControllerConfig;
use crate::controller::raft_controller::RaftController;
use crate::error::ControllerError;
use crate::error::Result;
use crate::metadata::MetadataStore;
use crate::processor::controller_request_processor::ControllerRequestProcessor;
pub use broker_processor::BrokerHeartbeatProcessor;
pub use broker_processor::ElectMasterProcessor;
pub use broker_processor::RegisterBrokerProcessor;
pub use broker_processor::UnregisterBrokerProcessor;
pub use metadata_processor::GetMetadataProcessor;
pub use request::RequestType;
pub use request::*;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::processor::RejectRequestResponse;
use rocketmq_rust::ArcMut;
pub use topic_processor::CreateTopicProcessor;
pub use topic_processor::DeleteTopicProcessor;
pub use topic_processor::UpdateTopicProcessor;
use tracing::info;

/// Request processor trait
#[async_trait::async_trait]
pub trait RequestProcessor: Send + Sync {
    /// Process a request
    async fn process(&self, request: &[u8]) -> Result<Vec<u8>>;
}

/// Processor manager
///
/// This component manages all request processors for handling
/// RPC requests from brokers and clients.
pub struct ProcessorManager {
    /// Configuration
    config: Arc<ControllerConfig>,

    /// Raft controller
    raft: ArcMut<RaftController>,

    /// Metadata store
    metadata: Arc<MetadataStore>,

    /// Processor registry
    processors: HashMap<RequestType, Arc<dyn RequestProcessor>>,
}

impl ProcessorManager {
    /// Create a new processor manager
    pub fn new(config: Arc<ControllerConfig>, raft: ArcMut<RaftController>, metadata: Arc<MetadataStore>) -> Self {
        // Initialize processors
        let mut processors: HashMap<RequestType, Arc<dyn RequestProcessor>> = HashMap::new();

        // Register broker processors
        processors.insert(
            RequestType::RegisterBroker,
            Arc::new(RegisterBrokerProcessor::new(metadata.clone(), raft.clone())),
        );
        processors.insert(
            RequestType::UnregisterBroker,
            Arc::new(UnregisterBrokerProcessor::new(metadata.clone(), raft.clone())),
        );
        processors.insert(
            RequestType::BrokerHeartbeat,
            Arc::new(BrokerHeartbeatProcessor::new(metadata.clone())),
        );
        processors.insert(
            RequestType::ElectMaster,
            Arc::new(ElectMasterProcessor::new(metadata.clone(), raft.clone())),
        );

        // Register metadata processor
        processors.insert(
            RequestType::GetMetadata,
            Arc::new(GetMetadataProcessor::new(metadata.clone())),
        );

        // Register topic processors
        processors.insert(
            RequestType::CreateTopic,
            Arc::new(CreateTopicProcessor::new(metadata.clone(), raft.clone())),
        );
        processors.insert(
            RequestType::UpdateTopic,
            Arc::new(UpdateTopicProcessor::new(metadata.clone(), raft.clone())),
        );
        processors.insert(
            RequestType::DeleteTopic,
            Arc::new(DeleteTopicProcessor::new(metadata.clone(), raft.clone())),
        );

        Self {
            config,
            raft,
            metadata,
            processors,
        }
    }

    /// Process a request
    pub async fn process_request(&self, request_type: RequestType, data: &[u8]) -> Result<Vec<u8>> {
        // Find the processor
        let processor = self
            .processors
            .get(&request_type)
            .ok_or_else(|| ControllerError::InvalidRequest(format!("Unknown request type: {:?}", request_type)))?;

        // Process the request
        processor.process(data).await
    }

    /// Start the processor manager
    pub async fn start(&self) -> Result<()> {
        info!("Starting processor manager with {} processors", self.processors.len());
        // TODO: Start network server to handle incoming requests
        Ok(())
    }

    /// Shutdown the processor manager
    pub async fn shutdown(&self) -> Result<()> {
        info!("Shutting down processor manager");
        // TODO: Stop network server and cleanup
        Ok(())
    }
}

pub(crate) type RequestCodeType = i32;

#[derive(Clone)]
pub enum ControllerRequestProcessorWrapper {
    ControllerRequestProcessor(ArcMut<ControllerRequestProcessor>),
}

impl rocketmq_remoting::runtime::processor::RequestProcessor for ControllerRequestProcessorWrapper {
    async fn process_request(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        match self {
            ControllerRequestProcessorWrapper::ControllerRequestProcessor(processor) => {
                processor.process_request(channel, ctx, request).await
            }
        }
    }

    fn reject_request(&self, code: i32) -> RejectRequestResponse {
        match self {
            ControllerRequestProcessorWrapper::ControllerRequestProcessor(processor) => {
                rocketmq_remoting::runtime::processor::RequestProcessor::reject_request(processor.as_ref(), code)
            }
        }
    }
}

#[derive(Clone, Default)]
pub struct ControllerServerRequestProcessor {
    default_request_processor: Option<ControllerRequestProcessorWrapper>,
}

impl ControllerServerRequestProcessor {
    pub fn new() -> Self {
        Self {
            default_request_processor: None,
        }
    }

    pub fn register_default_processor(&mut self, processor: ControllerRequestProcessorWrapper) {
        self.default_request_processor = Some(processor);
    }
}

impl rocketmq_remoting::runtime::processor::RequestProcessor for ControllerServerRequestProcessor {
    async fn process_request(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        match self.default_request_processor.as_mut() {
            None => {
                let response_command = RemotingCommand::create_response_command_with_code_remark(
                    ResponseCode::SystemError,
                    format!("The request code {} is not supported.", request.code_ref()),
                );
                Ok(Some(response_command.set_opaque(request.opaque())))
            }
            Some(processor) => {
                rocketmq_remoting::runtime::processor::RequestProcessor::process_request(
                    processor, channel, ctx, request,
                )
                .await
            }
        }
    }
}
