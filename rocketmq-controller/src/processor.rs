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

pub mod controller_request_processor;

use std::sync::Arc;

use crate::metrics::RequestType as MetricsRequestType;
use crate::processor::controller_request_processor::ControllerRequestProcessor;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::processor::RejectRequestResponse;

pub(crate) type RequestCodeType = i32;

#[derive(Clone)]
pub enum ControllerRequestProcessorWrapper {
    ControllerRequestProcessor(Arc<ControllerRequestProcessor>),
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
                let request_name = RequestCode::from(request.code()).get_controller_request_name();
                let dispatch = processor.handle_request(channel, ctx, request);
                processor.complete_request(request_name, dispatch).await
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
