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

//! request processor and response-write observation contracts.

use std::time::Duration;

use crate::dispatch::DeferredTerminalReason;
use crate::dispatch::HandlerOutcome;
use crate::dispatch::IngressRequestView;
use crate::dispatch::RemotingRequest;
use crate::dispatch::RemotingResponse;
use crate::dispatch::RequestId;
use crate::dispatch::ResponseBodyKind;
use crate::dispatch::ResponseCompletionOutcome;
use crate::dispatch::ResponseOperationalFailure;
use crate::dispatch::ResponseReceipt;
use crate::dispatch::WriteProgress;
use crate::request_ordering::RequestOrdering;

/// Decision returned before a request enters processor execution.
///
/// A rejection owns its remoting response. The decision is affine because cloning
/// it would duplicate response ownership.
///
/// ```compile_fail
/// use rocketmq_transport::api::RejectRequestDecision;
///
/// fn decisions_are_affine(decision: &RejectRequestDecision) {
///     let _: RejectRequestDecision = decision.clone();
/// }
/// ```
#[must_use]
#[derive(Debug, Default)]
#[allow(
    clippy::large_enum_variant,
    reason = "the public contract requires direct affine RemotingResponse ownership without indirection"
)]
pub enum RejectRequestDecision {
    /// Continue with normal request processing.
    #[default]
    Proceed,
    /// Reject processing and deliver the owned remoting response.
    Reject(RemotingResponse),
}

/// Response lifecycle path observed at the canonical write boundary.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ResponseWritePath {
    /// The handler completed its response during inline dispatch.
    Inline,
    /// A deferred registration completed the response later.
    Deferred,
}

/// Lifecycle mode for a body-free response observation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ResponseObservationMode {
    /// The request completed during inline dispatch.
    Inline,
    /// The request transferred response ownership and completed later.
    Deferred,
    /// The request intentionally produced no response frame.
    NoResponse,
}

/// One typed response lifecycle event.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ResponseObservationOutcome {
    /// The canonical response owner reached its transport disposition.
    Written(ResponseReceipt),
    /// The immutable ingress request was one-way.
    Oneway,
    /// Protocol policy explicitly permits no response.
    ProtocolNoResponse,
    /// A deferred response ended without attempting a remoting response.
    Cancelled(DeferredTerminalReason),
    /// Response processing or delivery failed with redacted metadata.
    Failed {
        /// Source-free completion state when the failure was a normal rejection.
        completion: Option<ResponseCompletionOutcome>,
        /// Socket-write progress, when known.
        progress: Option<WriteProgress>,
    },
}

/// Body-free metadata for one response lifecycle observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResponseMetadata {
    request_id: RequestId,
    original_code: i32,
    response_code: Option<i32>,
    body_kind: Option<ResponseBodyKind>,
    mode: ResponseObservationMode,
    outcome: ResponseObservationOutcome,
}

impl ResponseMetadata {
    pub(crate) const fn new(
        request_id: RequestId,
        original_code: i32,
        response_code: Option<i32>,
        body_kind: Option<ResponseBodyKind>,
        mode: ResponseObservationMode,
        outcome: ResponseObservationOutcome,
    ) -> Self {
        Self {
            request_id,
            original_code,
            response_code,
            body_kind,
            mode,
            outcome,
        }
    }

    /// Returns the immutable process-local request identity.
    #[must_use]
    pub const fn request_id(self) -> RequestId {
        self.request_id
    }

    /// Returns the original raw request code captured at ingress.
    #[must_use]
    pub const fn original_code(self) -> i32 {
        self.original_code
    }

    /// Returns the response code without exposing a response command.
    #[must_use]
    pub const fn response_code(self) -> Option<i32> {
        self.response_code
    }

    /// Returns the remoting response's storage category without exposing its body.
    #[must_use]
    pub const fn body_kind(self) -> Option<ResponseBodyKind> {
        self.body_kind
    }

    /// Returns the response lifecycle mode.
    #[must_use]
    pub const fn mode(self) -> ResponseObservationMode {
        self.mode
    }

    /// Returns the typed response lifecycle outcome.
    #[must_use]
    pub const fn outcome(self) -> ResponseObservationOutcome {
        self.outcome
    }
}

/// Body-free  response observation correlated to one request span.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResponseObservation {
    metadata: ResponseMetadata,
    write_elapsed: Option<Duration>,
    end_to_end_elapsed: Duration,
}

impl ResponseObservation {
    pub(crate) const fn new(
        metadata: ResponseMetadata,
        write_elapsed: Option<Duration>,
        end_to_end_elapsed: Duration,
    ) -> Self {
        Self {
            metadata,
            write_elapsed,
            end_to_end_elapsed,
        }
    }

    /// Returns body-free response metadata.
    #[must_use]
    pub const fn metadata(self) -> ResponseMetadata {
        self.metadata
    }

    /// Returns canonical delivery time when a write was attempted.
    #[must_use]
    pub const fn write_elapsed(self) -> Option<Duration> {
        self.write_elapsed
    }

    /// Returns elapsed time from trusted ingress to this observation.
    #[must_use]
    pub const fn end_to_end_elapsed(self) -> Duration {
        self.end_to_end_elapsed
    }

    /// Projects write-specific metadata when this lifecycle event attempted a response write.
    #[must_use]
    pub fn write_projection(self) -> Option<ResponseWriteObservation> {
        let response_code = self.metadata.response_code?;
        let body_kind = self.metadata.body_kind?;
        let path = match self.metadata.mode {
            ResponseObservationMode::Inline => ResponseWritePath::Inline,
            ResponseObservationMode::Deferred => ResponseWritePath::Deferred,
            ResponseObservationMode::NoResponse => return None,
        };
        let outcome = match self.metadata.outcome {
            ResponseObservationOutcome::Written(receipt) => ResponseWriteOutcome::Written(receipt),
            ResponseObservationOutcome::Failed { completion, progress } => {
                ResponseWriteOutcome::Failed { completion, progress }
            }
            ResponseObservationOutcome::Oneway
            | ResponseObservationOutcome::ProtocolNoResponse
            | ResponseObservationOutcome::Cancelled(_) => return None,
        };
        Some(ResponseWriteObservation {
            request_id: self.metadata.request_id,
            original_code: self.metadata.original_code,
            response_code,
            body_kind,
            path,
            write_elapsed: self.write_elapsed.unwrap_or(Duration::ZERO),
            end_to_end_elapsed: self.end_to_end_elapsed,
            outcome,
        })
    }

    pub(crate) fn from_write_result(
        request_id: RequestId,
        original_code: i32,
        response_code: i32,
        body_kind: ResponseBodyKind,
        path: ResponseWritePath,
        write_elapsed: Duration,
        end_to_end_elapsed: Duration,
        result: &Result<ResponseCompletionOutcome, ResponseOperationalFailure>,
    ) -> Self {
        let mode = match path {
            ResponseWritePath::Inline => ResponseObservationMode::Inline,
            ResponseWritePath::Deferred => ResponseObservationMode::Deferred,
        };
        let outcome = match result {
            Ok(ResponseCompletionOutcome::Completed(receipt)) => ResponseObservationOutcome::Written(*receipt),
            Ok(outcome) => ResponseObservationOutcome::Failed {
                completion: Some(*outcome),
                progress: response_completion_progress(*outcome),
            },
            Err(error) => ResponseObservationOutcome::Failed {
                completion: None,
                progress: Some(error.write_progress()),
            },
        };
        Self::new(
            ResponseMetadata::new(
                request_id,
                original_code,
                Some(response_code),
                Some(body_kind),
                mode,
                outcome,
            ),
            Some(write_elapsed),
            end_to_end_elapsed,
        )
    }
}

/// Typed result recorded at the canonical response-write boundary.
///
/// Failure values retain only a stable error category and optional write
/// progress. Source errors and their messages are discarded before an
/// observation is created.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ResponseWriteOutcome {
    /// The canonical response path reached its transport-specific disposition.
    Written(ResponseReceipt),
    /// Response delivery failed with stable, body-free metadata.
    Failed {
        /// Source-free completion state when the failure was a normal rejection.
        completion: Option<ResponseCompletionOutcome>,
        /// Socket-write progress, when the failure describes a write attempt.
        progress: Option<WriteProgress>,
    },
}

impl ResponseWriteOutcome {
    #[cfg(test)]
    fn from_result(result: Result<ResponseCompletionOutcome, ResponseOperationalFailure>) -> Self {
        match result {
            Ok(ResponseCompletionOutcome::Completed(receipt)) => Self::Written(receipt),
            Ok(outcome) => Self::Failed {
                completion: Some(outcome),
                progress: response_completion_progress(outcome),
            },
            Err(error) => Self::Failed {
                completion: None,
                progress: Some(error.write_progress()),
            },
        }
    }
}

const fn response_completion_progress(outcome: ResponseCompletionOutcome) -> Option<WriteProgress> {
    match outcome {
        ResponseCompletionOutcome::Completed(_) | ResponseCompletionOutcome::AlreadyCompleted(_) => None,
        ResponseCompletionOutcome::DeadlineExpired
        | ResponseCompletionOutcome::Cancelled
        | ResponseCompletionOutcome::SessionClosed
        | ResponseCompletionOutcome::QueueSaturated => Some(WriteProgress::NotStarted),
    }
}

/// Body-free metadata for one completed response write.
///
/// The observation identifies the immutable ingress request and response
/// metadata without retaining the response body, source error, channel,
/// connection context, or response sink.
///
/// ```compile_fail
/// use rocketmq_transport::api::ResponseWriteObservation;
///
/// fn bodies_are_not_observable(observation: &ResponseWriteObservation) {
///     let _ = observation.body();
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::{
///    RequestId, ResponseBodyKind, ResponseWriteObservation, ResponseWriteOutcome,
///     ResponseWritePath,
/// };
/// use std::time::Duration;
///
/// fn observations_cannot_be_forged(
///     request_id: RequestId,
///    outcome: ResponseWriteOutcome,
/// ) -> ResponseWriteObservation {
///    ResponseWriteObservation {
///         request_id,
///         original_code: 39,
///         response_code: 0,
///         body_kind: ResponseBodyKind::Empty,
///         path: ResponseWritePath::Inline,
///         write_elapsed: Duration::ZERO,
///         end_to_end_elapsed: Duration::ZERO,
///         outcome,
///     }
/// }
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResponseWriteObservation {
    request_id: RequestId,
    original_code: i32,
    response_code: i32,
    body_kind: ResponseBodyKind,
    path: ResponseWritePath,
    write_elapsed: Duration,
    end_to_end_elapsed: Duration,
    outcome: ResponseWriteOutcome,
}

impl ResponseWriteObservation {
    #[cfg(test)]
    pub(crate) fn from_result(
        request_id: RequestId,
        original_code: i32,
        response_code: i32,
        body_kind: ResponseBodyKind,
        path: ResponseWritePath,
        write_elapsed: Duration,
        end_to_end_elapsed: Duration,
        result: Result<ResponseCompletionOutcome, ResponseOperationalFailure>,
    ) -> Self {
        Self {
            request_id,
            original_code,
            response_code,
            body_kind,
            path,
            write_elapsed,
            end_to_end_elapsed,
            outcome: ResponseWriteOutcome::from_result(result),
        }
    }

    /// Returns the exact process-local request identity captured at ingress.
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// Returns the original raw request code captured at ingress.
    #[must_use]
    pub const fn original_code(&self) -> i32 {
        self.original_code
    }

    /// Returns the response code that was written or attempted.
    #[must_use]
    pub const fn response_code(&self) -> i32 {
        self.response_code
    }

    /// Returns the storage category of the response body without exposing it.
    #[must_use]
    pub const fn body_kind(&self) -> ResponseBodyKind {
        self.body_kind
    }

    /// Returns the response lifecycle path that reached the write boundary.
    #[must_use]
    pub const fn path(&self) -> ResponseWritePath {
        self.path
    }

    /// Returns the time spent awaiting canonical response delivery.
    #[must_use]
    pub const fn write_elapsed(&self) -> Duration {
        self.write_elapsed
    }

    /// Returns elapsed time from request ingress through response completion.
    #[must_use]
    pub const fn end_to_end_elapsed(&self) -> Duration {
        self.end_to_end_elapsed
    }

    /// Returns the typed, body-free response-write result.
    #[must_use]
    pub const fn outcome(&self) -> ResponseWriteOutcome {
        self.outcome
    }
}

const fn default_request_ordering() -> RequestOrdering {
    RequestOrdering::Concurrent
}

/// Local and production-send variants of the request processor contract.
///
/// The generated [`RequestProcessor`] trait requires both the processor and
/// the future returned by [`RequestProcessor::process`] to be `Send`.
/// [`LocalRequestProcessor`] permits a local future for embedded or test
/// environments that do not cross executor threads.
///
/// ```
/// use rocketmq_error::RocketMQError;
/// use rocketmq_transport::api::{
///    HandlerOutcome, LocalRequestProcessor, RemotingRequest,
/// };
///
/// struct Processor;
///
/// impl LocalRequestProcessor for Processor {
///     async fn process(
///         &mut self,
///         _request: &mut RemotingRequest,
///     ) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
///         Err(RocketMQError::illegal_argument("unsupported request"))
///     }
/// }
/// ```
///
/// ```compile_fail
/// use std::rc::Rc;
///
/// use rocketmq_error::RocketMQError;
/// use rocketmq_transport::api::{
///    HandlerOutcome, RemotingRequest, RequestProcessor,
/// };
///
/// struct NonSendFutureProcessor;
///
/// impl RequestProcessor for NonSendFutureProcessor {
///     async fn process(
///         &mut self,
///         _request: &mut RemotingRequest,
///     ) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
///         let local = Rc::new(());
///         std::future::ready(()).await;
///         drop(local);
///         Err(RocketMQError::illegal_argument("test processor"))
///     }
/// }
/// ```
#[trait_variant::make(RequestProcessor: Send)]
pub trait LocalRequestProcessor {
    /// Asynchronously processes one owned mutable request aggregate.
    ///
    /// This method does not block the calling thread. The returned
    /// [`HandlerOutcome`] is the only handler response contract.
    ///
    /// # Errors
    ///
    /// Returns [`rocketmq_error::RocketMQError`] when request processing
    /// cannot produce a terminal handler outcome.
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome>;

    /// Decides whether a raw request code should enter processor execution.
    ///
    /// The default permits processing.
    fn reject_request(&self, _code: i32) -> RejectRequestDecision {
        RejectRequestDecision::Proceed
    }

    /// Declares per-session ordering from immutable ingress metadata.
    ///
    /// The default permits concurrent execution. The borrowed ingress view
    /// contains no request body.
    fn request_ordering(&self, _ingress: IngressRequestView<'_>) -> RequestOrdering {
        default_request_ordering()
    }

    /// Observes response lifecycle metadata without access to request or response bodies.
    ///
    /// The default performs no work.
    fn observe_response(&self, _observation: ResponseObservation) {}
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use rocketmq_error::RocketMQError;

    use super::*;
    use crate::dispatch::ResponseBody;
    use crate::dispatch::ResponseDisposition;
    use crate::dispatch::ResponseTerminalState;

    struct LocalFutureProcessor;

    impl LocalRequestProcessor for LocalFutureProcessor {
        async fn process(&mut self, _request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
            let local = Rc::new(());
            std::future::ready(()).await;
            drop(local);
            Err(RocketMQError::illegal_argument("test processor"))
        }
    }

    fn assert_local_processor<T: LocalRequestProcessor>() {}

    #[test]
    fn local_processor_accepts_a_non_send_future_and_defaults_are_low_cost() {
        assert_local_processor::<LocalFutureProcessor>();
        let processor = LocalFutureProcessor;

        assert!(matches!(processor.reject_request(39), RejectRequestDecision::Proceed));
        assert_eq!(default_request_ordering(), RequestOrdering::Concurrent);

        let request_id = RequestId::real(7, 9).expect("real request identity");
        let observation = ResponseObservation::from_write_result(
            request_id,
            39,
            0,
            ResponseBodyKind::Empty,
            ResponseWritePath::Inline,
            Duration::from_millis(2),
            Duration::from_millis(5),
            &Ok(ResponseCompletionOutcome::Completed(ResponseReceipt::new(
                request_id,
                ResponseDisposition::TransportWritten,
            ))),
        );
        processor.observe_response(observation);
    }

    #[test]
    fn rejection_moves_the_owned_remoting_response() {
        let response = RemotingResponse::bytes(
            rocketmq_protocol::protocol::remoting_command::RemotingCommand::create_response_command_with_code(7),
            bytes::Bytes::from_static(b"owned response"),
        )
        .expect("remoting response");
        let ResponseBody::Bytes(body) = response.test_body() else {
            panic!("byte response should retain contiguous storage");
        };
        let pointer = body.as_ptr();

        let decision = RejectRequestDecision::Reject(response);
        let RejectRequestDecision::Reject(response) = decision else {
            panic!("rejection should retain its response");
        };
        let ResponseBody::Bytes(body) = response.test_body() else {
            panic!("rejection should retain contiguous storage");
        };
        assert_eq!(body.as_ptr(), pointer);
        assert_eq!(response.body_kind(), ResponseBodyKind::Bytes);
        assert_eq!(response.body_len(), 14);
    }

    #[test]
    fn observation_maps_written_receipts_without_retaining_response_payloads() {
        let request_id = RequestId::real(11, 13).expect("real request identity");
        let receipt = ResponseReceipt::new(request_id, ResponseDisposition::InProcessAccepted);
        let observation = ResponseWriteObservation::from_result(
            request_id,
            220,
            17,
            ResponseBodyKind::Segments,
            ResponseWritePath::Deferred,
            Duration::from_millis(3),
            Duration::from_millis(8),
            Ok(ResponseCompletionOutcome::Completed(receipt)),
        );

        assert_eq!(observation.request_id(), request_id);
        assert_eq!(observation.original_code(), 220);
        assert_eq!(observation.response_code(), 17);
        assert_eq!(observation.body_kind(), ResponseBodyKind::Segments);
        assert_eq!(observation.path(), ResponseWritePath::Deferred);
        assert_eq!(observation.write_elapsed(), Duration::from_millis(3));
        assert_eq!(observation.end_to_end_elapsed(), Duration::from_millis(8));
        assert_eq!(observation.outcome(), ResponseWriteOutcome::Written(receipt));
    }

    #[test]
    fn observation_maps_failure_kind_and_progress_without_the_source_error() {
        let request_id = RequestId::real(17, 19).expect("real request identity");
        let observation = ResponseWriteObservation::from_result(
            request_id,
            40,
            1,
            ResponseBodyKind::FileRegions,
            ResponseWritePath::Inline,
            Duration::from_micros(7),
            Duration::from_micros(11),
            Err(ResponseOperationalFailure::Transport {
                progress: WriteProgress::PossiblyPartial,
                source: RocketMQError::illegal_argument("transport source must not be retained"),
            }),
        );
        assert_eq!(
            observation.outcome(),
            ResponseWriteOutcome::Failed {
                completion: None,
                progress: Some(WriteProgress::PossiblyPartial),
            }
        );
        assert!(!format!("{observation:?}").contains("transport source must not be retained"));

        let already_completed = ResponseWriteObservation::from_result(
            request_id,
            40,
            1,
            ResponseBodyKind::Empty,
            ResponseWritePath::Inline,
            Duration::ZERO,
            Duration::ZERO,
            Ok(ResponseCompletionOutcome::AlreadyCompleted(
                ResponseTerminalState::Completed,
            )),
        );
        assert_eq!(
            already_completed.outcome(),
            ResponseWriteOutcome::Failed {
                completion: Some(ResponseCompletionOutcome::AlreadyCompleted(
                    ResponseTerminalState::Completed,
                )),
                progress: None,
            }
        );
    }
}
