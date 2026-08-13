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
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::remoting_command_defaults::application_remoting_command_factory;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;
use rocketmq_transport::api::v1::Channel;
use rocketmq_transport::api::v1::ConnectionHandlerContext;
use rocketmq_transport::api::v1::RejectRequestResponse;

pub(crate) type RequestCodeType = i32;

#[derive(Clone)]
pub enum ControllerRequestProcessorWrapper {
    ControllerRequestProcessor(Arc<ControllerRequestProcessor>),
}

impl rocketmq_transport::api::v1::RequestProcessor for ControllerRequestProcessorWrapper {
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
                rocketmq_transport::api::v1::RequestProcessor::reject_request(processor.as_ref(), code)
            }
        }
    }
}

#[derive(Clone)]
pub struct ControllerServerRequestProcessor {
    default_request_processor: Option<ControllerRequestProcessorWrapper>,
    command_factory: RemotingCommandFactory,
}

impl ControllerServerRequestProcessor {
    pub fn new() -> Self {
        Self::new_with_remoting_command_factory(application_remoting_command_factory())
    }

    /// Creates the server processor with explicit immutable remoting defaults.
    pub fn new_with_remoting_command_factory(command_factory: RemotingCommandFactory) -> Self {
        Self {
            default_request_processor: None,
            command_factory,
        }
    }

    pub fn register_default_processor(&mut self, processor: ControllerRequestProcessorWrapper) {
        self.default_request_processor = Some(processor);
    }
}

impl rocketmq_transport::api::v1::RequestProcessor for ControllerServerRequestProcessor {
    async fn process_request(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        match self.default_request_processor.as_mut() {
            None => {
                let response_command = self.command_factory.create_response_command_with_code_remark(
                    ResponseCode::SystemError,
                    format!("The request code {} is not supported.", request.code_ref()),
                );
                Ok(Some(response_command.set_opaque(request.opaque())))
            }
            Some(processor) => {
                rocketmq_transport::api::v1::RequestProcessor::process_request(processor, channel, ctx, request).await
            }
        }
    }
}

impl Default for ControllerServerRequestProcessor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use rocketmq_protocol::protocol::remoting_command_defaults::{RemotingCommandDefaults, RemotingCommandFactory};
    use rocketmq_protocol::protocol::SerializeType;
    use rocketmq_runtime::RuntimeContext;
    use rocketmq_transport::api::v1::ConnectionHandlerContextWrapper;
    use rocketmq_transport::api::v1::RequestProcessor;
    use rocketmq_transport::test_support::Connection;

    use super::*;

    async fn test_channel() -> Channel {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind local listener");
        let local_addr = listener.local_addr().expect("local listener address");
        let stream = std::net::TcpStream::connect(local_addr).expect("connect local listener");
        stream.set_nonblocking(true).expect("set nonblocking");
        drop(listener);
        let connection = Connection::new(tokio::net::TcpStream::from_std(stream).expect("convert stream"));
        let task_group = RuntimeContext::from_current("controller-server-processor-test")
            .service_context("processor")
            .task_group()
            .clone();
        rocketmq_transport::test_support::TestChannelBuilder::new(connection, task_group)
            .addresses(local_addr, SocketAddr::from(([127, 0, 0, 1], 0)))
            .build()
            .expect("build channel")
    }

    #[tokio::test]
    async fn unsupported_response_uses_the_injected_command_factory() {
        let factory = RemotingCommandFactory::new(RemotingCommandDefaults::new(670, SerializeType::ROCKETMQ));
        let mut processor = ControllerServerRequestProcessor::new_with_remoting_command_factory(factory);
        let channel = test_channel().await;
        let ctx = Arc::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let mut request = factory.create_remoting_command(-44).set_opaque(91);

        let response = processor
            .process_request(channel, ctx, &mut request)
            .await
            .expect("unsupported request dispatch")
            .expect("unsupported request response");

        assert_eq!(response.version(), 670);
        assert_eq!(response.serialize_type(), SerializeType::ROCKETMQ);
        assert_eq!(response.opaque(), 91);
    }
}
