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

//! V2 request processor and response-write observation contracts.

use std::time::Duration;

use crate::dispatch::HandlerOutcome;
use crate::dispatch::IngressRequestView;
use crate::dispatch::RemotingRequest;
use crate::dispatch::RequestId;
use crate::dispatch::ResponseBodyKind;
use crate::dispatch::ResponseError;
use crate::dispatch::ResponseErrorKind;
use crate::dispatch::ResponsePlan;
use crate::dispatch::ResponseReceipt;
use crate::dispatch::WriteProgress;
use crate::request_ordering::RequestOrdering;

/// Decision returned before a V2 request enters processor execution.
///
/// A rejection owns its response plan. The decision is affine because cloning
/// it would duplicate response ownership.
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::RejectRequestDecision;
///
/// fn decisions_are_affine(decision: &RejectRequestDecision) {
///     let _: RejectRequestDecision = decision.clone();
/// }
/// ```
#[must_use]
#[derive(Debug, Default)]
#[allow(
    clippy::large_enum_variant,
    reason = "the public V2 contract requires direct affine ResponsePlan ownership without indirection"
)]
pub enum RejectRequestDecision {
    /// Continue with normal request processing.
    #[default]
    Proceed,
    /// Reject processing and deliver the owned response plan.
    Reject(ResponsePlan),
}

/// Response lifecycle path observed at the canonical write boundary.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ResponseWritePath {
    /// The handler completed its response during inline dispatch.
    Inline,
    /// A deferred registration completed the response later.
    Deferred,
}

/// Typed result recorded at the canonical response-write boundary.
///
/// Failure values retain only a stable error category and optional write
/// progress. Source errors and their messages are discarded before an
/// observation is created.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ResponseWriteOutcomeV2 {
    /// The canonical response path reached its transport-specific disposition.
    Written(ResponseReceipt),
    /// Response delivery failed with stable, body-free metadata.
    Failed {
        /// Stable response failure category.
        kind: ResponseErrorKind,
        /// Socket-write progress, when the failure describes a write attempt.
        progress: Option<WriteProgress>,
    },
}

impl ResponseWriteOutcomeV2 {
    fn from_result(result: Result<ResponseReceipt, ResponseError>) -> Self {
        match result {
            Ok(receipt) => Self::Written(receipt),
            Err(error) => Self::Failed {
                kind: error.kind(),
                progress: error.write_progress(),
            },
        }
    }
}

/// Body-free metadata for one completed V2 response write.
///
/// The observation identifies the immutable ingress request and response
/// metadata without retaining the response body, source error, channel,
/// connection context, or response sink.
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::ResponseWriteObservationV2;
///
/// fn bodies_are_not_observable(observation: &ResponseWriteObservationV2) {
///     let _ = observation.body();
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::{
///     RequestId, ResponseBodyKind, ResponseWriteObservationV2, ResponseWriteOutcomeV2,
///     ResponseWritePath,
/// };
/// use std::time::Duration;
///
/// fn observations_cannot_be_forged(
///     request_id: RequestId,
///     outcome: ResponseWriteOutcomeV2,
/// ) -> ResponseWriteObservationV2 {
///     ResponseWriteObservationV2 {
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
pub struct ResponseWriteObservationV2 {
    request_id: RequestId,
    original_code: i32,
    response_code: i32,
    body_kind: ResponseBodyKind,
    path: ResponseWritePath,
    write_elapsed: Duration,
    end_to_end_elapsed: Duration,
    outcome: ResponseWriteOutcomeV2,
}

impl ResponseWriteObservationV2 {
    #[allow(
        dead_code,
        reason = "DSP-01 reserves observation construction for the later V2 dispatcher"
    )]
    pub(crate) fn from_result(
        request_id: RequestId,
        original_code: i32,
        response_code: i32,
        body_kind: ResponseBodyKind,
        path: ResponseWritePath,
        write_elapsed: Duration,
        end_to_end_elapsed: Duration,
        result: Result<ResponseReceipt, ResponseError>,
    ) -> Self {
        Self {
            request_id,
            original_code,
            response_code,
            body_kind,
            path,
            write_elapsed,
            end_to_end_elapsed,
            outcome: ResponseWriteOutcomeV2::from_result(result),
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
    pub const fn outcome(&self) -> ResponseWriteOutcomeV2 {
        self.outcome
    }
}

const fn default_request_ordering() -> RequestOrdering {
    RequestOrdering::Concurrent
}

/// Local and production-send variants of the V2 request processor contract.
///
/// The generated [`RequestProcessorV2`] trait requires both the processor and
/// the future returned by [`RequestProcessorV2::process`] to be `Send`.
/// [`LocalRequestProcessorV2`] permits a local future for embedded or test
/// environments that do not cross executor threads.
///
/// ```
/// use rocketmq_error::RocketMQError;
/// use rocketmq_transport::api::v2::{
///     HandlerOutcome, LocalRequestProcessorV2, RemotingRequest,
/// };
///
/// struct Processor;
///
/// impl LocalRequestProcessorV2 for Processor {
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
/// use rocketmq_transport::api::v2::{
///     HandlerOutcome, RemotingRequest, RequestProcessorV2,
/// };
///
/// struct NonSendFutureProcessor;
///
/// impl RequestProcessorV2 for NonSendFutureProcessor {
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
#[trait_variant::make(RequestProcessorV2: Send)]
pub trait LocalRequestProcessorV2 {
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

    /// Observes one completed response write without retaining its payload.
    ///
    /// The default performs no work.
    fn observe_response_write(&self, _observation: ResponseWriteObservationV2) {}
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

    impl LocalRequestProcessorV2 for LocalFutureProcessor {
        async fn process(&mut self, _request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
            let local = Rc::new(());
            std::future::ready(()).await;
            drop(local);
            Err(RocketMQError::illegal_argument("test processor"))
        }
    }

    fn assert_local_processor<T: LocalRequestProcessorV2>() {}

    #[test]
    fn local_processor_accepts_a_non_send_future_and_defaults_are_low_cost() {
        assert_local_processor::<LocalFutureProcessor>();
        let processor = LocalFutureProcessor;

        assert!(matches!(processor.reject_request(39), RejectRequestDecision::Proceed));
        assert_eq!(default_request_ordering(), RequestOrdering::Concurrent);

        let request_id = RequestId::real(7, 9).expect("real request identity");
        let observation = ResponseWriteObservationV2::from_result(
            request_id,
            39,
            0,
            ResponseBodyKind::Empty,
            ResponseWritePath::Inline,
            Duration::from_millis(2),
            Duration::from_millis(5),
            Ok(ResponseReceipt::new(request_id, ResponseDisposition::TransportWritten)),
        );
        processor.observe_response_write(observation);
    }

    #[test]
    fn rejection_moves_the_owned_response_plan() {
        let plan = ResponsePlan::bytes(
            rocketmq_protocol::protocol::remoting_command::RemotingCommand::create_response_command_with_code(7),
            bytes::Bytes::from_static(b"owned response"),
        )
        .expect("response plan");
        let ResponseBody::Bytes(body) = plan.test_body() else {
            panic!("byte response should retain contiguous storage");
        };
        let pointer = body.as_ptr();

        let decision = RejectRequestDecision::Reject(plan);
        let RejectRequestDecision::Reject(plan) = decision else {
            panic!("rejection should retain its plan");
        };
        let ResponseBody::Bytes(body) = plan.test_body() else {
            panic!("rejection should retain contiguous storage");
        };
        assert_eq!(body.as_ptr(), pointer);
        assert_eq!(plan.body_kind(), ResponseBodyKind::Bytes);
        assert_eq!(plan.body_len(), 14);
    }

    #[test]
    fn observation_maps_written_receipts_without_retaining_response_payloads() {
        let request_id = RequestId::real(11, 13).expect("real request identity");
        let receipt = ResponseReceipt::new(request_id, ResponseDisposition::InProcessAccepted);
        let observation = ResponseWriteObservationV2::from_result(
            request_id,
            220,
            17,
            ResponseBodyKind::Segments,
            ResponseWritePath::Deferred,
            Duration::from_millis(3),
            Duration::from_millis(8),
            Ok(receipt),
        );

        assert_eq!(observation.request_id(), request_id);
        assert_eq!(observation.original_code(), 220);
        assert_eq!(observation.response_code(), 17);
        assert_eq!(observation.body_kind(), ResponseBodyKind::Segments);
        assert_eq!(observation.path(), ResponseWritePath::Deferred);
        assert_eq!(observation.write_elapsed(), Duration::from_millis(3));
        assert_eq!(observation.end_to_end_elapsed(), Duration::from_millis(8));
        assert_eq!(observation.outcome(), ResponseWriteOutcomeV2::Written(receipt));
    }

    #[test]
    fn observation_maps_failure_kind_and_progress_without_the_source_error() {
        let request_id = RequestId::real(17, 19).expect("real request identity");
        let observation = ResponseWriteObservationV2::from_result(
            request_id,
            40,
            1,
            ResponseBodyKind::FileRegions,
            ResponseWritePath::Inline,
            Duration::from_micros(7),
            Duration::from_micros(11),
            Err(ResponseError::Transport {
                progress: WriteProgress::PossiblyPartial,
                source: RocketMQError::illegal_argument("transport source must not be retained"),
            }),
        );
        assert_eq!(
            observation.outcome(),
            ResponseWriteOutcomeV2::Failed {
                kind: ResponseErrorKind::Transport,
                progress: Some(WriteProgress::PossiblyPartial),
            }
        );
        assert!(!format!("{observation:?}").contains("transport source must not be retained"));

        let already_completed = ResponseWriteObservationV2::from_result(
            request_id,
            40,
            1,
            ResponseBodyKind::Empty,
            ResponseWritePath::Inline,
            Duration::ZERO,
            Duration::ZERO,
            Err(ResponseError::AlreadyCompleted {
                state: ResponseTerminalState::Completed,
            }),
        );
        assert_eq!(
            already_completed.outcome(),
            ResponseWriteOutcomeV2::Failed {
                kind: ResponseErrorKind::AlreadyCompleted,
                progress: None,
            }
        );
    }
}
