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

use super::*;
use rocketmq_error::fields;
use rocketmq_error::Error;
use rocketmq_error::ErrorContext;
use rocketmq_transport::api::OutboundRequestContractReason;
use rocketmq_transport::api::OutboundRequestRejectionReason;
use rocketmq_transport::api::OutboundRequestStage;

pub(super) fn route_lookup_error(input: RetryInput) -> RocketMQError {
    match input {
        RetryInput::Transport(error) => RocketMQError::Shared(error.into_shared_error()),
        RetryInput::Rejected(rejection) => match rejection.reason() {
            OutboundRequestRejectionReason::DeadlineExpired => {
                let mut context = ErrorContext::new().with_text(fields::OPERATION_DIAGNOSTIC, "topic_route_lookup");
                if let Some(timeout_millis) = rejection.timeout_millis() {
                    context = context.with_u64(fields::TIMEOUT_MS, timeout_millis);
                }
                RocketMQError::Shared(Arc::new(
                    Error::new(&rocketmq_error::CORE_OPERATION_TIMED_OUT).with_context(context),
                ))
            }
            OutboundRequestRejectionReason::ClientStopping => {
                RocketMQError::Shared(Arc::new(Error::new(&rocketmq_error::CLIENT_LIFECYCLE_NOT_STARTED)))
            }
            OutboundRequestRejectionReason::QueueSaturated => RocketMQError::Shared(Arc::new(Error::new(
                &rocketmq_error::TRANSPORT_ADMISSION_QUEUE_SATURATED,
            ))),
            OutboundRequestRejectionReason::Cancelled => {
                route_connection_error(request_stage_phase(rejection.stage()), rejection.remote_addr_present())
            }
            OutboundRequestRejectionReason::SessionClosed => {
                route_connection_error("closed", rejection.remote_addr_present())
            }
            OutboundRequestRejectionReason::EndpointUnavailable => {
                route_connection_error("connect", rejection.remote_addr_present())
            }
        },
        RetryInput::Contract(contract) => match contract.reason() {
            OutboundRequestContractReason::NameServerEndpointMissing => {
                route_connection_error("connect", contract.remote_addr_present())
            }
        },
        RetryInput::Response { terminal_error, .. } | RetryInput::BusinessError(terminal_error) => terminal_error,
        RetryInput::SendStatus(status) => mq_client_err!(format!("Unexpected route send status: {status:?}")),
        RetryInput::RouteUnavailable => mq_client_err!("No route available"),
    }
}

fn route_connection_error(phase: &'static str, remote_addr_present: bool) -> RocketMQError {
    let mut context = ErrorContext::new().with_text(fields::PHASE, phase);
    if remote_addr_present {
        context = context.with_secret_presence(fields::REMOTE_ADDR_PRESENT);
    }
    RocketMQError::Shared(Arc::new(
        Error::new(&rocketmq_error::TRANSPORT_CONNECTION_FAILED).with_context(context),
    ))
}

const fn request_stage_phase(stage: OutboundRequestStage) -> &'static str {
    match stage {
        OutboundRequestStage::BeforeWrite => "before_write",
        OutboundRequestStage::Writing => "writing",
        OutboundRequestStage::AwaitingResponse => "awaiting_response",
        OutboundRequestStage::ResponseReceived => "response_received",
    }
}
