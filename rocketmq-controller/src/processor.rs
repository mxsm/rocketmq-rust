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

use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::remoting_command_defaults::application_remoting_command_factory;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;
use rocketmq_transport::api::v2::HandlerOutcome;
use rocketmq_transport::api::v2::IngressRequestView;
use rocketmq_transport::api::v2::RejectRequestDecision;
use rocketmq_transport::api::v2::RemotingRequest;
use rocketmq_transport::api::v2::RequestOrdering;
use rocketmq_transport::api::v2::RequestProcessorV2;
use rocketmq_transport::api::v2::ResponsePlan;
use rocketmq_transport::api::v2::ResponseWriteObservationV2;

use crate::processor::controller_request_processor::ControllerRequestProcessor;

/// V2 wrapper retained for composition sites that register controller processor variants.
#[derive(Clone)]
pub enum ControllerRequestProcessorWrapper {
    ControllerRequestProcessor(Arc<ControllerRequestProcessor>),
}

impl RequestProcessorV2 for ControllerRequestProcessorWrapper {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        match self {
            Self::ControllerRequestProcessor(processor) => {
                let mut processor = processor.as_ref().clone();
                processor.process(request).await
            }
        }
    }

    fn reject_request(&self, code: i32) -> RejectRequestDecision {
        match self {
            Self::ControllerRequestProcessor(processor) => processor.reject_request(code),
        }
    }

    fn request_ordering(&self, ingress: IngressRequestView<'_>) -> RequestOrdering {
        match self {
            Self::ControllerRequestProcessor(processor) => processor.request_ordering(ingress),
        }
    }

    fn observe_response_write(&self, observation: ResponseWriteObservationV2) {
        match self {
            Self::ControllerRequestProcessor(processor) => processor.observe_response_write(observation),
        }
    }
}

/// Dormant V2 aggregate retained for compatibility and explicit default routing.
///
/// Production intentionally wires [`ControllerRequestProcessor`] directly. This
/// aggregate remains available until the final V1 compatibility cleanup stage.
#[derive(Clone)]
pub struct ControllerServerRequestProcessor {
    default_request_processor: Option<ControllerRequestProcessorWrapper>,
    command_factory: RemotingCommandFactory,
}

impl ControllerServerRequestProcessor {
    #[must_use]
    pub fn new() -> Self {
        Self::new_with_remoting_command_factory(application_remoting_command_factory())
    }

    /// Creates the aggregate with explicit immutable remoting defaults.
    #[must_use]
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

impl RequestProcessorV2 for ControllerServerRequestProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        if let Some(processor) = self.default_request_processor.as_mut() {
            return processor.process(request).await;
        }

        let response = self
            .command_factory
            .create_response_command_with_code_remark(
                ResponseCode::SystemError,
                format!("The request code {} is not supported.", request.command().code_ref()),
            )
            .set_opaque(request.original_identity().original_opaque());
        let plan = ResponsePlan::command(response).map_err(|error| {
            rocketmq_error::RocketMQError::response_process_failed(
                "controller.aggregate.unsupported_response",
                error.to_string(),
            )
        })?;
        Ok(HandlerOutcome::Reply(plan))
    }

    fn reject_request(&self, code: i32) -> RejectRequestDecision {
        self.default_request_processor
            .as_ref()
            .map_or(RejectRequestDecision::Proceed, |processor| {
                processor.reject_request(code)
            })
    }

    fn request_ordering(&self, ingress: IngressRequestView<'_>) -> RequestOrdering {
        self.default_request_processor
            .as_ref()
            .map_or(RequestOrdering::Concurrent, |processor| {
                processor.request_ordering(ingress)
            })
    }

    fn observe_response_write(&self, observation: ResponseWriteObservationV2) {
        if let Some(processor) = self.default_request_processor.as_ref() {
            processor.observe_response_write(observation);
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
    use std::time::Duration;

    use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandDefaults;
    use rocketmq_protocol::protocol::SerializeType;
    use rocketmq_runtime::RuntimeContext;
    use rocketmq_transport::api::v1::ServerConfig;
    use rocketmq_transport::api::v2::TransportServerV2;
    use rocketmq_transport::test_support::Connection;
    use tokio::net::TcpListener;
    use tokio::net::TcpStream;
    use tokio::sync::oneshot;

    use super::*;

    #[tokio::test]
    async fn unsupported_response_uses_the_injected_command_factory() {
        let factory = RemotingCommandFactory::new(RemotingCommandDefaults::new(670, SerializeType::ROCKETMQ));
        let processor = ControllerServerRequestProcessor::new_with_remoting_command_factory(factory);
        let runtime = RuntimeContext::from_current("controller-server-processor-test");
        let service = runtime.service_context("v2-aggregate");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind V2 aggregate test listener");
        let address = listener.local_addr().expect("V2 aggregate listener address");
        let server = TransportServerV2::new(Arc::new(ServerConfig::default()), service, processor);
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let server_task = tokio::spawn(async move {
            server
                .try_serve_bound_listener_until(listener, None, async {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        let stream = TcpStream::connect(address)
            .await
            .expect("connect V2 aggregate test client");
        let mut client = Connection::new(stream);
        let request = factory.create_remoting_command(-44).set_opaque(91);
        client.send_command(request).await.expect("send unsupported request");
        let response = tokio::time::timeout(Duration::from_secs(2), client.receive_command())
            .await
            .expect("unsupported response deadline")
            .expect("V2 aggregate connection remains open")
            .expect("decode unsupported response");

        assert_eq!(response.version(), 670);
        assert_eq!(response.serialize_type(), SerializeType::ROCKETMQ);
        assert_eq!(response.opaque(), 91);
        assert_eq!(ResponseCode::from(response.code()), ResponseCode::SystemError);

        drop(client);
        let _ = shutdown_tx.send(());
        let report = tokio::time::timeout(Duration::from_secs(3), server_task)
            .await
            .expect("V2 aggregate server shutdown deadline")
            .expect("V2 aggregate server task")
            .expect("V2 aggregate server report");
        assert!(report.is_healthy(), "{report:?}");
    }
}
