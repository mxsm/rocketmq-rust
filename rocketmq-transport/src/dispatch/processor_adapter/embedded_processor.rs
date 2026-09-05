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

//! Channel-free embedded execution for the explicit processor wrapper.

use std::time::Duration;

use crate::contract::TransportContractViolation;
use crate::dispatch::DeferredRegistration;
use crate::dispatch::HandlerOutcome;
use crate::dispatch::IngressRequestView;
use crate::dispatch::InternalFailureOrigin;
use crate::dispatch::InternalProcessorCandidate;
use crate::dispatch::InternalProcessorOutcome;
use crate::dispatch::OriginalRequestIdentity;
use crate::dispatch::ProtocolNoResponse;
use crate::dispatch::RemotingRequest;
use crate::dispatch::RemotingResponse;
use crate::dispatch::ResponseBodyKind;
use crate::dispatch::ResponseCompletionOutcome;
use crate::dispatch::ResponseOperationalFailure;
use crate::request_ordering::RequestOrdering;
use crate::runtime::processor::RejectRequestDecision;
use crate::runtime::processor::RequestProcessor;
use crate::runtime::processor::ResponseObservation;
use crate::runtime::processor::ResponseWritePath;

use super::DispatchProcessorError;
use super::ExplicitProcessor;

pub(crate) enum EmbeddedResolvedOutcome {
    Reply(RemotingResponse),
    OneWay,
    Deferred(DeferredRegistration),
    NoReply(ProtocolNoResponse),
}

impl<P> ExplicitProcessor<P>
where
    P: RequestProcessor + Clone + Sync + 'static,
{
    pub(crate) fn embedded_request_ordering(&self, ingress: IngressRequestView<'_>) -> RequestOrdering {
        self.processor.request_ordering(ingress)
    }

    pub(crate) fn embedded_reject_request(&self, request_code: i32) -> Option<InternalProcessorCandidate> {
        match self.processor.reject_request(request_code) {
            RejectRequestDecision::Proceed => None,
            RejectRequestDecision::Reject(response) => Some(InternalProcessorCandidate::success(
                InternalProcessorOutcome::Handled(HandlerOutcome::Reply(response)),
            )),
        }
    }

    pub(crate) async fn process_embedded(
        &mut self,
        request: &mut RemotingRequest,
    ) -> Result<InternalProcessorCandidate, DispatchProcessorError> {
        let processed = match request.meta().deadline() {
            Some(deadline) => deadline.timeout(self.processor.process(request)).await,
            None => Ok(self.processor.process(request).await),
        };
        match processed {
            Ok(Ok(outcome)) => Ok(InternalProcessorCandidate::success(InternalProcessorOutcome::Handled(
                outcome,
            ))),
            Ok(Err(error)) => Ok(InternalProcessorCandidate::failure(
                InternalProcessorOutcome::Handled(HandlerOutcome::Reply(
                    crate::error_response::remoting_response_from_error(&error)?,
                )),
                InternalFailureOrigin::ProcessorError,
            )),
            Err(_) => Ok(InternalProcessorCandidate::failure(
                InternalProcessorOutcome::Handled(HandlerOutcome::Reply(RemotingResponse::command(
                    super::super::authorized_dispatcher::deadline_response(
                        request.original_identity().original_opaque(),
                    ),
                )?)),
                InternalFailureOrigin::Deadline,
            )),
        }
    }

    pub(crate) fn resolve_embedded_outcome(
        &self,
        request: &mut RemotingRequest,
        outcome: InternalProcessorOutcome,
    ) -> Result<EmbeddedResolvedOutcome, TransportContractViolation> {
        let InternalProcessorOutcome::Handled(outcome) = outcome;
        if request.original_identity().is_one_way() {
            return match outcome {
                HandlerOutcome::Reply(response) => {
                    let resolved = request.resolve_handler_outcome(HandlerOutcome::Reply(response))?;
                    drop(resolved);
                    Ok(EmbeddedResolvedOutcome::OneWay)
                }
                HandlerOutcome::Deferred(registration) => {
                    request.consume_oneway_deferred(registration)?;
                    Err(TransportContractViolation::OneWayDeferredHandlerOutcome)
                }
                HandlerOutcome::NoReply(marker) => {
                    request.consume_oneway_no_reply(marker)?;
                    Err(TransportContractViolation::OneWayNoReplyHandlerOutcome)
                }
            };
        }
        match request.resolve_handler_outcome(outcome)? {
            HandlerOutcome::Reply(response) => Ok(EmbeddedResolvedOutcome::Reply(response)),
            HandlerOutcome::Deferred(registration) => Ok(EmbeddedResolvedOutcome::Deferred(registration)),
            HandlerOutcome::NoReply(marker) => Ok(EmbeddedResolvedOutcome::NoReply(marker)),
        }
    }

    pub(crate) fn observe_embedded_response(
        &self,
        original: OriginalRequestIdentity,
        response_code: i32,
        body_kind: ResponseBodyKind,
        write_elapsed: Duration,
        end_to_end_elapsed: Duration,
        result: &Result<ResponseCompletionOutcome, ResponseOperationalFailure>,
    ) {
        self.processor.observe_response(ResponseObservation::from_write_result(
            original.request_id(),
            original.original_code(),
            response_code,
            body_kind,
            ResponseWritePath::Inline,
            write_elapsed,
            end_to_end_elapsed,
            result,
        ));
    }
}
