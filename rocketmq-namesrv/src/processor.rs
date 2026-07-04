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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::error_response;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::processor::RejectRequestResponse;
use rocketmq_remoting::runtime::processor::RequestProcessor;
use rocketmq_rust::ArcMut;

pub use self::client_request_processor::ClientRequestProcessor;
pub use self::cluster_test_request_processor::ClusterTestRequestProcessor;
pub(crate) use self::cluster_test_request_processor::ClusterTestRouteLookup;
use crate::bootstrap::InFlightRequestTracker;
use crate::processor::default_request_processor::DefaultRequestProcessor;

mod client_request_processor;
mod cluster_test_request_processor;
pub mod default_request_processor;

const NAMESPACE_ORDER_TOPIC_CONFIG: &str = "ORDER_TOPIC_CONFIG";

#[derive(Clone)]
pub enum NameServerRequestProcessorWrapper {
    ClientRequestProcessor(ArcMut<ClientRequestProcessor>),
    ClusterTestRequestProcessor(ArcMut<ClusterTestRequestProcessor>),
    DefaultRequestProcessor(ArcMut<DefaultRequestProcessor>),
}

impl RequestProcessor for NameServerRequestProcessorWrapper {
    async fn process_request(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        match self {
            NameServerRequestProcessorWrapper::ClientRequestProcessor(processor) => {
                processor.process_request(channel, ctx, request).await
            }
            NameServerRequestProcessorWrapper::ClusterTestRequestProcessor(processor) => {
                processor.process_request(channel, ctx, request).await
            }
            NameServerRequestProcessorWrapper::DefaultRequestProcessor(processor) => {
                processor.process_request(channel, ctx, request).await
            }
        }
    }

    fn reject_request(&self, code: i32) -> RejectRequestResponse {
        match self {
            NameServerRequestProcessorWrapper::ClientRequestProcessor(processor) => {
                RequestProcessor::reject_request(processor.as_ref(), code)
            }
            NameServerRequestProcessorWrapper::ClusterTestRequestProcessor(processor) => {
                RequestProcessor::reject_request(processor.as_ref(), code)
            }
            NameServerRequestProcessorWrapper::DefaultRequestProcessor(processor) => {
                RequestProcessor::reject_request(processor.as_ref(), code)
            }
        }
    }
}

pub(crate) type RequestCodeType = i32;

#[derive(Clone, Default)]
pub struct NameServerRequestProcessor {
    processor_table: HashMap<RequestCodeType, NameServerRequestProcessorWrapper>,
    default_request_processor: Option<NameServerRequestProcessorWrapper>,
    in_flight_requests: Option<Arc<InFlightRequestTracker>>,
}

impl NameServerRequestProcessor {
    pub fn new() -> Self {
        Self {
            processor_table: HashMap::new(),
            default_request_processor: None,
            in_flight_requests: None,
        }
    }

    pub(crate) fn new_with_in_flight_tracker(in_flight_requests: Arc<InFlightRequestTracker>) -> Self {
        Self {
            in_flight_requests: Some(in_flight_requests),
            ..Self::new()
        }
    }

    pub fn register_processor(&mut self, request_code: RequestCode, processor: NameServerRequestProcessorWrapper) {
        self.processor_table.insert(request_code as i32, processor);
    }

    pub fn register_default_processor(&mut self, processor: NameServerRequestProcessorWrapper) {
        self.default_request_processor = Some(processor);
    }
}

impl RequestProcessor for NameServerRequestProcessor {
    async fn process_request(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let _in_flight_guard = self
            .in_flight_requests
            .as_ref()
            .map(|in_flight_requests| in_flight_requests.enter());
        let route_request_started = (request.code() == RequestCode::GetRouteinfoByTopic as i32).then(Instant::now);
        let response = match self.processor_table.get_mut(request.code_ref()) {
            None => match self.default_request_processor.as_mut() {
                None => {
                    let response =
                        error_response::request_code_not_supported_with_opaque(request.code(), request.opaque());
                    Ok(Some(response))
                }
                Some(processor) => RequestProcessor::process_request(processor, channel, ctx, request).await,
            },
            Some(processor) => RequestProcessor::process_request(processor, channel, ctx, request).await,
        };
        if let Some(started) = route_request_started {
            rocketmq_observability::metrics::namesrv::record_route_request(started.elapsed());
        }
        response
    }
}
