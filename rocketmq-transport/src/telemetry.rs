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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use rocketmq_protocol::code::request_code::RequestCode;

use parking_lot::Mutex;

use crate::dispatch::AuthenticationState;
use crate::dispatch::DeferredTerminalReason;
use crate::dispatch::OriginalRequestIdentity;
use crate::dispatch::RequestOrigin;
use crate::dispatch::ResponseBodyKind;
#[cfg(any(test, feature = "observability"))]
use crate::dispatch::ResponseDisposition;
use crate::dispatch::ResponseErrorKind;
use crate::dispatch::ResponseReceipt;
use crate::dispatch::WriteProgress;
use crate::runtime::processor_v2::ResponseMetadataV2;
use crate::runtime::processor_v2::ResponseObservationModeV2;
use crate::runtime::processor_v2::ResponseObservationOutcomeV2;
use crate::runtime::processor_v2::ResponseObservationV2;

#[cfg(feature = "observability")]
const NO_RESPONSE_CODE: i32 = -1;

#[cfg(any(test, feature = "observability", feature = "observability-traces"))]
fn v2_request_span(
    identity: OriginalRequestIdentity,
    code_class: TransportRequestCodeClass,
    origin: &RequestOrigin,
    authentication: &AuthenticationState,
    deadline: Option<crate::deadline::RequestDeadline>,
) -> tracing::Span {
    tracing::info_span!(
        "RocketMQ REMOTING V2 REQUEST",
        rocketmq.request.owner_id = identity.request_id().owner_id(),
        rocketmq.request.sequence = identity.request_id().sequence(),
        rocketmq.request.original_code = identity.original_code(),
        rocketmq.request.operation_name = code_class.as_str(),
        rocketmq.request.session_id = identity.request_id().owner_id(),
        rocketmq.request.origin_kind = origin_kind(origin),
        rocketmq.request.peer_class = peer_class(origin),
        rocketmq.request.authentication_state = authentication_state(authentication),
        rocketmq.request.principal_kind = principal_kind(authentication),
        rocketmq.request.deadline_bucket = deadline_bucket(deadline),
        outcome = tracing::field::Empty,
        response_plan_kind = tracing::field::Empty,
        response_disposition = tracing::field::Empty,
        error_kind = tracing::field::Empty,
        write_progress = tracing::field::Empty,
    )
}

#[cfg(any(test, feature = "observability", feature = "observability-traces"))]
const fn origin_kind(origin: &RequestOrigin) -> &'static str {
    match origin {
        RequestOrigin::Network { .. } => "network",
        RequestOrigin::Embedded { .. } => "embedded_proxy",
    }
}

#[cfg(any(test, feature = "observability", feature = "observability-traces"))]
fn peer_class(origin: &RequestOrigin) -> &'static str {
    let RequestOrigin::Network { peer } = origin else {
        return "not_applicable";
    };
    let ip = peer.address().ip();
    if ip.is_loopback() {
        "loopback"
    } else if match ip {
        std::net::IpAddr::V4(ip) => ip.is_private() || ip.is_link_local(),
        std::net::IpAddr::V6(ip) => ip.is_unique_local() || ip.is_unicast_link_local(),
    } {
        "private"
    } else {
        "public"
    }
}

#[cfg(any(test, feature = "observability", feature = "observability-traces"))]
const fn authentication_state(authentication: &AuthenticationState) -> &'static str {
    match authentication {
        AuthenticationState::Authenticated(..) => "authenticated",
        AuthenticationState::Anonymous => "anonymous",
        AuthenticationState::SecurityDisabled => "security_disabled",
    }
}

#[cfg(any(test, feature = "observability", feature = "observability-traces"))]
const fn principal_kind(authentication: &AuthenticationState) -> &'static str {
    match authentication {
        AuthenticationState::Authenticated(..) => "trusted_principal",
        AuthenticationState::Anonymous => "anonymous",
        AuthenticationState::SecurityDisabled => "not_applicable",
    }
}

#[cfg(any(test, feature = "observability", feature = "observability-traces"))]
fn deadline_bucket(deadline: Option<crate::deadline::RequestDeadline>) -> &'static str {
    let Some(deadline) = deadline else {
        return "none";
    };
    let remaining = deadline.remaining();
    if remaining.is_zero() {
        "expired"
    } else if remaining <= Duration::from_secs(1) {
        "le_1s"
    } else if remaining <= Duration::from_secs(10) {
        "le_10s"
    } else {
        "gt_10s"
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransportRequestCodeClass {
    PullMessage,
    PopMessage,
    Notification,
    Other,
}

impl TransportRequestCodeClass {
    pub(crate) fn from_code(code: i32) -> Self {
        match RequestCode::from(code) {
            RequestCode::PullMessage => Self::PullMessage,
            RequestCode::PopMessage => Self::PopMessage,
            RequestCode::Notification => Self::Notification,
            _ => Self::Other,
        }
    }

    #[cfg(any(test, feature = "observability", feature = "observability-traces"))]
    const fn as_str(self) -> &'static str {
        match self {
            Self::PullMessage => "pull_message",
            Self::PopMessage => "pop_message",
            Self::Notification => "notification",
            Self::Other => "other",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum V2BoundaryRejectionReason {
    DeadlineExpired,
    SecurityDenied,
    AdmissionRejected,
}

impl V2BoundaryRejectionReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::DeadlineExpired => "deadline_expired",
            Self::SecurityDenied => "security_denied",
            Self::AdmissionRejected => "admission_rejected",
        }
    }
}

type V2ResponseObservationCallback = Box<dyn FnOnce(ResponseObservationV2) + Send + 'static>;

#[derive(Default)]
struct V2ResponseObservationCallbackState {
    callback: Option<V2ResponseObservationCallback>,
    pending: Option<ResponseObservationV2>,
    delivered: bool,
}

#[derive(Default)]
struct V2DeferredMetricState {
    armed: bool,
    retained_bytes: usize,
}

struct V2RequestObservationInner {
    telemetry: TransportTelemetry,
    callback: Mutex<V2ResponseObservationCallbackState>,
    span: tracing::Span,
    original: OriginalRequestIdentity,
    started: Instant,
    code_class: TransportRequestCodeClass,
    request_metrics: Mutex<TransportRequestMetricsGuard>,
    terminal: AtomicBool,
    deferred_registration_recorded: AtomicBool,
    deferred_metrics: Mutex<V2DeferredMetricState>,
}

impl Drop for V2RequestObservationInner {
    fn drop(&mut self) {
        if self.terminal.swap(true, Ordering::AcqRel) {
            return;
        }
        let deferred_metrics = self.deferred_metrics.get_mut();
        if deferred_metrics.armed {
            deferred_metrics.armed = false;
            self.telemetry.adjust_v2_deferred(
                self.code_class,
                -1,
                -retained_bytes_delta(deferred_metrics.retained_bytes),
            );
            deferred_metrics.retained_bytes = 0;
        }
        let metadata = ResponseMetadataV2::new(
            self.original.request_id(),
            self.original.original_code(),
            None,
            None,
            ResponseObservationModeV2::NoResponse,
            ResponseObservationOutcomeV2::Failed {
                kind: None,
                progress: Some(WriteProgress::NotStarted),
            },
        );
        self.request_metrics
            .get_mut()
            .complete_process_request_failed(metadata.response_code().unwrap_or(-1));
        self.telemetry.record_v2_request_lifecycle();
        self.telemetry.record_v2_response(metadata);
        self.span.record("outcome", "failed");
        self.span.record("write_progress", WriteProgress::NotStarted.as_str());
        let observation = ResponseObservationV2::new(metadata, None, self.started.elapsed());
        if let Some((callback, observation)) = take_or_defer_response_observation(self.callback.get_mut(), observation)
        {
            self.span.in_scope(|| callback(observation));
        }
    }
}

/// Shared observation owner retained across inline dispatch and deferred resume.
#[derive(Clone)]
pub(crate) struct V2RequestObservation {
    inner: Arc<V2RequestObservationInner>,
}

impl V2RequestObservation {
    fn new(
        telemetry: TransportTelemetry,
        original: OriginalRequestIdentity,
        started: Instant,
        origin: &RequestOrigin,
        authentication: &AuthenticationState,
        deadline: Option<crate::deadline::RequestDeadline>,
        request_bytes: u64,
    ) -> Self {
        let code_class = TransportRequestCodeClass::from_code(original.original_code());
        let span = telemetry.v2_request_span(original, code_class, origin, authentication, deadline);
        let request_metrics = telemetry.v2_request_guard(original.original_code(), request_bytes);
        telemetry.record_v2_span_started();
        Self {
            inner: Arc::new(V2RequestObservationInner {
                telemetry,
                callback: Mutex::new(V2ResponseObservationCallbackState::default()),
                span,
                original,
                started,
                code_class,
                request_metrics: Mutex::new(request_metrics),
                terminal: AtomicBool::new(false),
                deferred_registration_recorded: AtomicBool::new(false),
                deferred_metrics: Mutex::new(V2DeferredMetricState::default()),
            }),
        }
    }

    pub(crate) fn span(&self) -> tracing::Span {
        self.inner.span.clone()
    }

    pub(crate) fn bind_response_observer(&self, callback: impl FnOnce(ResponseObservationV2) + Send + 'static) {
        let delivery = {
            let mut state = self.inner.callback.lock();
            if state.delivered || state.callback.is_some() {
                None
            } else if let Some(observation) = state.pending.take() {
                state.delivered = true;
                Some((Box::new(callback) as V2ResponseObservationCallback, observation))
            } else {
                state.callback = Some(Box::new(callback));
                None
            }
        };
        if let Some((callback, observation)) = delivery {
            self.inner.span.in_scope(|| callback(observation));
        }
    }

    #[cfg(test)]
    pub(crate) fn with_span_for_test(mut self, span: tracing::Span) -> Self {
        Arc::get_mut(&mut self.inner)
            .expect("a test observation must be uniquely owned before span injection")
            .span = span;
        self
    }

    pub(crate) fn arm_deferred_metrics(&self, retained_bytes: usize) {
        let mut deferred_metrics = self.inner.deferred_metrics.lock();
        if deferred_metrics.armed || self.inner.terminal.load(Ordering::Acquire) {
            return;
        }
        deferred_metrics.armed = true;
        deferred_metrics.retained_bytes = retained_bytes;
        self.inner
            .telemetry
            .adjust_v2_deferred(self.inner.code_class, 1, retained_bytes_delta(retained_bytes));
    }

    pub(crate) fn record_deferred_registered(&self) {
        if self
            .inner
            .deferred_registration_recorded
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        self.inner.request_metrics.lock().record_deferred_registered();
        self.inner.telemetry.record_v2_request_lifecycle();
        self.inner.telemetry.record_v2_deferred_registration();
    }

    pub(crate) fn complete_reply(
        &self,
        mode: ResponseObservationModeV2,
        response_code: i32,
        plan_kind: ResponseBodyKind,
        write_elapsed: Duration,
        result: Result<ResponseReceipt, (ResponseErrorKind, Option<WriteProgress>)>,
    ) {
        let outcome = match result {
            Ok(receipt) => ResponseObservationOutcomeV2::Written(receipt),
            Err((kind, progress)) => ResponseObservationOutcomeV2::Failed {
                kind: Some(kind),
                progress,
            },
        };
        self.complete(
            ResponseMetadataV2::new(
                self.inner.original.request_id(),
                self.inner.original.original_code(),
                Some(response_code),
                Some(plan_kind),
                mode,
                outcome,
            ),
            Some(write_elapsed),
        );
    }

    pub(crate) fn complete_boundary_rejection(
        &self,
        reason: V2BoundaryRejectionReason,
        response_code: Option<i32>,
        plan_kind: Option<ResponseBodyKind>,
        write_elapsed: Option<Duration>,
        outcome: ResponseObservationOutcomeV2,
    ) {
        self.complete_with_rejection(
            ResponseMetadataV2::new(
                self.inner.original.request_id(),
                self.inner.original.original_code(),
                response_code,
                plan_kind,
                if response_code.is_some() {
                    ResponseObservationModeV2::Inline
                } else {
                    ResponseObservationModeV2::NoResponse
                },
                outcome,
            ),
            write_elapsed,
            reason,
        );
    }

    pub(crate) fn complete_no_response(&self, outcome: ResponseObservationOutcomeV2) {
        self.complete(
            ResponseMetadataV2::new(
                self.inner.original.request_id(),
                self.inner.original.original_code(),
                None,
                None,
                ResponseObservationModeV2::NoResponse,
                outcome,
            ),
            None,
        );
    }

    pub(crate) fn complete_failure_without_kind(
        &self,
        mode: ResponseObservationModeV2,
        response_code: Option<i32>,
        plan_kind: Option<ResponseBodyKind>,
        progress: Option<WriteProgress>,
    ) {
        self.complete(
            ResponseMetadataV2::new(
                self.inner.original.request_id(),
                self.inner.original.original_code(),
                response_code,
                plan_kind,
                mode,
                ResponseObservationOutcomeV2::Failed { kind: None, progress },
            ),
            None,
        );
    }

    pub(crate) fn complete_cancelled(&self, reason: DeferredTerminalReason) {
        self.complete_no_response(ResponseObservationOutcomeV2::Cancelled(reason));
    }

    pub(crate) fn complete_request_failed(&self, response_code: i32) {
        self.inner
            .request_metrics
            .lock()
            .complete_process_request_failed(response_code);
    }

    pub(crate) fn complete_write_failed(&self, response_code: i32) {
        self.inner
            .request_metrics
            .lock()
            .complete_write_channel_failed(response_code);
    }

    fn complete(&self, metadata: ResponseMetadataV2, write_elapsed: Option<Duration>) {
        self.complete_inner(metadata, write_elapsed, None);
    }

    fn complete_with_rejection(
        &self,
        metadata: ResponseMetadataV2,
        write_elapsed: Option<Duration>,
        reason: V2BoundaryRejectionReason,
    ) {
        self.complete_inner(metadata, write_elapsed, Some(reason));
    }

    fn complete_inner(
        &self,
        metadata: ResponseMetadataV2,
        write_elapsed: Option<Duration>,
        rejection: Option<V2BoundaryRejectionReason>,
    ) {
        if self
            .inner
            .terminal
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            self.inner.telemetry.record_v2_response_duplicate(self.inner.code_class);
            return;
        }
        let mut deferred_metrics = self.inner.deferred_metrics.lock();
        if deferred_metrics.armed {
            deferred_metrics.armed = false;
            self.inner.telemetry.adjust_v2_deferred(
                self.inner.code_class,
                -1,
                -retained_bytes_delta(deferred_metrics.retained_bytes),
            );
            deferred_metrics.retained_bytes = 0;
        }
        drop(deferred_metrics);
        let mut request_metrics = self.inner.request_metrics.lock();
        if rejection.is_some() {
            request_metrics.complete_process_request_failed(metadata.response_code().unwrap_or(-1));
        } else {
            complete_v2_request_metrics(&mut request_metrics, metadata);
        }
        drop(request_metrics);
        self.inner.telemetry.record_v2_request_lifecycle();
        self.inner.telemetry.record_v2_response(metadata);
        self.inner.span.record(
            "outcome",
            rejection.map_or_else(|| observation_outcome_label(metadata.outcome()), |_| "rejected"),
        );
        if let Some(plan_kind) = metadata.plan_kind() {
            self.inner.span.record("response_plan_kind", plan_kind.as_str());
        }
        match metadata.outcome() {
            ResponseObservationOutcomeV2::Written(receipt) => {
                self.inner
                    .span
                    .record("response_disposition", receipt.disposition().as_str());
            }
            ResponseObservationOutcomeV2::Failed { kind, progress } => {
                if rejection.is_none() {
                    if let Some(kind) = kind {
                        self.inner.span.record("error_kind", kind.as_str());
                    }
                }
                if let Some(progress) = progress {
                    self.inner.span.record("write_progress", progress.as_str());
                }
            }
            ResponseObservationOutcomeV2::Cancelled(reason) => {
                self.inner.span.record("error_kind", reason.as_str());
                self.inner
                    .span
                    .record("write_progress", WriteProgress::NotStarted.as_str());
            }
            ResponseObservationOutcomeV2::Oneway | ResponseObservationOutcomeV2::ProtocolNoResponse => {}
        }
        if let Some(reason) = rejection {
            self.inner.span.record("error_kind", reason.as_str());
            self.inner.telemetry.record_v2_boundary_rejection(reason, metadata);
        }
        let observation = ResponseObservationV2::new(metadata, write_elapsed, self.inner.started.elapsed());
        let delivery = {
            let mut callback = self.inner.callback.lock();
            take_or_defer_response_observation(&mut callback, observation)
        };
        if let Some((callback, observation)) = delivery {
            self.inner.span.in_scope(|| callback(observation));
        }
    }
}

fn take_or_defer_response_observation(
    state: &mut V2ResponseObservationCallbackState,
    observation: ResponseObservationV2,
) -> Option<(V2ResponseObservationCallback, ResponseObservationV2)> {
    if state.delivered {
        return None;
    }
    let Some(callback) = state.callback.take() else {
        state.pending = Some(observation);
        return None;
    };
    state.delivered = true;
    Some((callback, observation))
}

fn complete_v2_request_metrics(metrics: &mut TransportRequestMetricsGuard, metadata: ResponseMetadataV2) {
    match metadata.outcome() {
        ResponseObservationOutcomeV2::Written(_) if metadata.mode() == ResponseObservationModeV2::Deferred => {
            metrics.complete_deferred_resumed(metadata.response_code().unwrap_or(-1));
        }
        ResponseObservationOutcomeV2::Written(_) => {
            if let (Some(response_code), Some(body_kind)) = (metadata.response_code(), metadata.plan_kind()) {
                metrics.complete_v2_reply(response_code, body_kind);
            } else {
                metrics.complete_process_request_failed(metadata.response_code().unwrap_or(-1));
            }
        }
        ResponseObservationOutcomeV2::Oneway => metrics.complete_oneway(),
        ResponseObservationOutcomeV2::ProtocolNoResponse => {
            metrics.complete_protocol_no_response();
        }
        ResponseObservationOutcomeV2::Cancelled(_) => metrics.complete_cancelled(),
        ResponseObservationOutcomeV2::Failed { .. } => {
            metrics.complete_process_request_failed(metadata.response_code().unwrap_or(-1));
        }
    }
}

fn retained_bytes_delta(retained_bytes: usize) -> i64 {
    retained_bytes.min(i64::MAX as usize) as i64
}

const fn observation_outcome_label(outcome: ResponseObservationOutcomeV2) -> &'static str {
    match outcome {
        ResponseObservationOutcomeV2::Written(_) => "written",
        ResponseObservationOutcomeV2::Oneway => "oneway",
        ResponseObservationOutcomeV2::ProtocolNoResponse => "protocol_no_response",
        ResponseObservationOutcomeV2::Cancelled(_) => "cancelled",
        ResponseObservationOutcomeV2::Failed { .. } => "failed",
    }
}

#[cfg(test)]
const fn response_mode_label(mode: ResponseObservationModeV2) -> &'static str {
    match mode {
        ResponseObservationModeV2::Inline => "inline",
        ResponseObservationModeV2::Deferred => "deferred",
        ResponseObservationModeV2::NoResponse => "no_response",
    }
}

#[cfg(test)]
const fn response_result_label(outcome: ResponseObservationOutcomeV2) -> &'static str {
    match outcome {
        ResponseObservationOutcomeV2::Written(receipt) => match receipt.disposition() {
            ResponseDisposition::TransportWritten => "transport_written",
            ResponseDisposition::InProcessAccepted => "in_process_accepted",
        },
        ResponseObservationOutcomeV2::Oneway => "oneway",
        ResponseObservationOutcomeV2::ProtocolNoResponse => "protocol_no_response",
        ResponseObservationOutcomeV2::Cancelled(_) => "cancelled",
        ResponseObservationOutcomeV2::Failed { .. } => "failed",
    }
}

#[cfg(test)]
type LifecycleEventCapture = std::sync::Arc<parking_lot::Mutex<Vec<(&'static str, &'static str)>>>;
#[cfg(test)]
type DeferredTerminalCapture = std::sync::Arc<parking_lot::Mutex<Vec<(&'static str, &'static str)>>>;
#[cfg(test)]
type DeferredStateConstructionCapture = std::sync::Arc<std::sync::atomic::AtomicUsize>;
#[cfg(test)]
type DeferredMetricAdjustmentCapture = std::sync::Arc<parking_lot::Mutex<Vec<(i64, i64)>>>;
#[cfg(test)]
type DeferredRegistrationCapture = std::sync::Arc<std::sync::atomic::AtomicUsize>;
#[cfg(test)]
type V2BoundaryRejectionCapture = (&'static str, &'static str, &'static str, &'static str, &'static str);
#[cfg(test)]
#[derive(Default)]
pub(crate) struct V2BoundaryMetricCapture {
    spans: std::sync::atomic::AtomicUsize,
    requests: std::sync::atomic::AtomicUsize,
    request_durations: std::sync::atomic::AtomicUsize,
    responses: std::sync::atomic::AtomicUsize,
    response_queue_waits: std::sync::atomic::AtomicUsize,
    response_events: parking_lot::Mutex<Vec<(&'static str, &'static str)>>,
    rejections: parking_lot::Mutex<Vec<V2BoundaryRejectionCapture>>,
}

#[cfg(test)]
impl V2BoundaryMetricCapture {
    pub(crate) fn snapshot(&self) -> (usize, usize, usize, usize) {
        (
            self.spans.load(std::sync::atomic::Ordering::SeqCst),
            self.requests.load(std::sync::atomic::Ordering::SeqCst),
            self.request_durations.load(std::sync::atomic::Ordering::SeqCst),
            self.responses.load(std::sync::atomic::Ordering::SeqCst),
        )
    }

    pub(crate) fn rejections(&self) -> Vec<V2BoundaryRejectionCapture> {
        self.rejections.lock().clone()
    }

    pub(crate) fn response_queue_waits(&self) -> usize {
        self.response_queue_waits.load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// Cloneable metrics and tracing capability for one transport composition.
///
/// A no-op value is explicit and never consults process-global OpenTelemetry state.
#[derive(Clone, Default)]
pub struct TransportTelemetry {
    #[cfg(feature = "observability")]
    remoting: rocketmq_observability::metrics::remoting::RemotingMetrics,
    #[cfg(feature = "observability")]
    client: rocketmq_observability::metrics::client::ClientMetrics,
    #[cfg(any(feature = "observability", feature = "observability-traces"))]
    handle: Option<rocketmq_observability::TelemetryHandle>,
    #[cfg(test)]
    lifecycle_events: Option<LifecycleEventCapture>,
    #[cfg(test)]
    deferred_terminals: Option<DeferredTerminalCapture>,
    #[cfg(test)]
    deferred_state_constructions: Option<DeferredStateConstructionCapture>,
    #[cfg(test)]
    deferred_metric_adjustments: Option<DeferredMetricAdjustmentCapture>,
    #[cfg(test)]
    deferred_registrations: Option<DeferredRegistrationCapture>,
    #[cfg(test)]
    v2_boundary_metrics: Option<std::sync::Arc<V2BoundaryMetricCapture>>,
}

impl TransportTelemetry {
    /// Creates a transport telemetry capability that never records.
    #[must_use]
    pub fn noop() -> Self {
        Self::default()
    }

    /// Binds transport metrics and tracing to one explicit telemetry runtime.
    #[cfg(any(feature = "observability", feature = "observability-traces"))]
    #[must_use]
    pub fn from_handle(telemetry: &rocketmq_observability::TelemetryHandle) -> Self {
        Self {
            #[cfg(feature = "observability")]
            remoting: rocketmq_observability::metrics::remoting::RemotingMetrics::from_handle(telemetry),
            #[cfg(feature = "observability")]
            client: rocketmq_observability::metrics::client::ClientMetrics::from_handle(telemetry),
            handle: Some(telemetry.clone()),
            #[cfg(test)]
            lifecycle_events: None,
            #[cfg(test)]
            deferred_terminals: None,
            #[cfg(test)]
            deferred_state_constructions: None,
            #[cfg(test)]
            deferred_metric_adjustments: None,
            #[cfg(test)]
            deferred_registrations: None,
            #[cfg(test)]
            v2_boundary_metrics: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_lifecycle_event_capture() -> (Self, LifecycleEventCapture) {
        let capture = std::sync::Arc::new(parking_lot::Mutex::new(Vec::new()));
        let telemetry = Self {
            #[cfg(feature = "observability")]
            remoting: Default::default(),
            #[cfg(feature = "observability")]
            client: Default::default(),
            #[cfg(any(feature = "observability", feature = "observability-traces"))]
            handle: None,
            lifecycle_events: Some(std::sync::Arc::clone(&capture)),
            deferred_terminals: None,
            deferred_state_constructions: None,
            deferred_metric_adjustments: None,
            deferred_registrations: None,
            v2_boundary_metrics: None,
        };
        (telemetry, capture)
    }

    #[cfg(test)]
    pub(crate) fn with_deferred_terminal_capture() -> (Self, DeferredTerminalCapture) {
        let capture = std::sync::Arc::new(parking_lot::Mutex::new(Vec::new()));
        let telemetry = Self {
            #[cfg(feature = "observability")]
            remoting: Default::default(),
            #[cfg(feature = "observability")]
            client: Default::default(),
            #[cfg(any(feature = "observability", feature = "observability-traces"))]
            handle: None,
            lifecycle_events: None,
            deferred_terminals: Some(std::sync::Arc::clone(&capture)),
            deferred_state_constructions: None,
            deferred_metric_adjustments: None,
            deferred_registrations: None,
            v2_boundary_metrics: None,
        };
        (telemetry, capture)
    }

    #[cfg(test)]
    pub(crate) fn with_deferred_state_construction_capture() -> (Self, DeferredStateConstructionCapture) {
        let capture = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let telemetry = Self {
            #[cfg(feature = "observability")]
            remoting: Default::default(),
            #[cfg(feature = "observability")]
            client: Default::default(),
            #[cfg(any(feature = "observability", feature = "observability-traces"))]
            handle: None,
            lifecycle_events: None,
            deferred_terminals: None,
            deferred_state_constructions: Some(std::sync::Arc::clone(&capture)),
            deferred_metric_adjustments: None,
            deferred_registrations: None,
            v2_boundary_metrics: None,
        };
        (telemetry, capture)
    }

    #[cfg(test)]
    pub(crate) fn with_v2_deferred_metric_capture(
    ) -> (Self, DeferredMetricAdjustmentCapture, DeferredRegistrationCapture) {
        let adjustments = std::sync::Arc::new(parking_lot::Mutex::new(Vec::new()));
        let registrations = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let telemetry = Self {
            #[cfg(feature = "observability")]
            remoting: Default::default(),
            #[cfg(feature = "observability")]
            client: Default::default(),
            #[cfg(any(feature = "observability", feature = "observability-traces"))]
            handle: None,
            lifecycle_events: None,
            deferred_terminals: None,
            deferred_state_constructions: None,
            deferred_metric_adjustments: Some(std::sync::Arc::clone(&adjustments)),
            deferred_registrations: Some(std::sync::Arc::clone(&registrations)),
            v2_boundary_metrics: None,
        };
        (telemetry, adjustments, registrations)
    }

    #[cfg(test)]
    pub(crate) fn with_v2_boundary_metric_capture() -> (Self, std::sync::Arc<V2BoundaryMetricCapture>) {
        let capture = std::sync::Arc::new(V2BoundaryMetricCapture::default());
        let telemetry = Self {
            #[cfg(feature = "observability")]
            remoting: Default::default(),
            #[cfg(feature = "observability")]
            client: Default::default(),
            #[cfg(any(feature = "observability", feature = "observability-traces"))]
            handle: None,
            lifecycle_events: None,
            deferred_terminals: None,
            deferred_state_constructions: None,
            deferred_metric_adjustments: None,
            deferred_registrations: None,
            v2_boundary_metrics: Some(std::sync::Arc::clone(&capture)),
        };
        (telemetry, capture)
    }

    #[cfg(test)]
    #[inline]
    pub(crate) fn record_deferred_state_construction(&self) {
        if let Some(capture) = &self.deferred_state_constructions {
            capture.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    pub(crate) fn begin_v2_observation(
        &self,
        original: OriginalRequestIdentity,
        started: Instant,
        origin: &RequestOrigin,
        authentication: &AuthenticationState,
        deadline: Option<crate::deadline::RequestDeadline>,
        request_bytes: u64,
    ) -> V2RequestObservation {
        V2RequestObservation::new(
            self.clone(),
            original,
            started,
            origin,
            authentication,
            deadline,
            request_bytes,
        )
    }

    fn v2_request_span(
        &self,
        original: OriginalRequestIdentity,
        code_class: TransportRequestCodeClass,
        origin: &RequestOrigin,
        authentication: &AuthenticationState,
        deadline: Option<crate::deadline::RequestDeadline>,
    ) -> tracing::Span {
        #[cfg(any(feature = "observability", feature = "observability-traces"))]
        if self
            .handle
            .as_ref()
            .is_some_and(|handle| handle.is_active() && handle.trace_policy().enabled)
        {
            return v2_request_span(original, code_class, origin, authentication, deadline);
        }

        let _ = (original, code_class, origin, authentication, deadline);
        tracing::Span::none()
    }

    pub(crate) fn record_response_queue_wait(&self, duration: Duration) {
        #[cfg(test)]
        if let Some(capture) = &self.v2_boundary_metrics {
            capture
                .response_queue_waits
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }

        #[cfg(feature = "observability")]
        self.remoting.record_response_queue_wait(duration.as_secs_f64());

        #[cfg(not(feature = "observability"))]
        let _ = duration;
    }

    fn adjust_v2_deferred(&self, code: TransportRequestCodeClass, inflight_delta: i64, retained_bytes_delta: i64) {
        #[cfg(test)]
        if let Some(capture) = &self.deferred_metric_adjustments {
            capture.lock().push((inflight_delta, retained_bytes_delta));
        }

        #[cfg(feature = "observability")]
        self.remoting
            .adjust_deferred(observable_code_class(code), inflight_delta, retained_bytes_delta);

        #[cfg(not(feature = "observability"))]
        let _ = (code, inflight_delta, retained_bytes_delta);
    }

    #[inline]
    fn record_v2_deferred_registration(&self) {
        #[cfg(test)]
        if let Some(capture) = &self.deferred_registrations {
            capture.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[inline]
    fn record_v2_span_started(&self) {
        #[cfg(test)]
        if let Some(capture) = &self.v2_boundary_metrics {
            capture.spans.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[inline]
    fn record_v2_request_lifecycle(&self) {
        #[cfg(test)]
        if let Some(capture) = &self.v2_boundary_metrics {
            capture.requests.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            capture
                .request_durations
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    fn record_v2_response_duplicate(&self, code: TransportRequestCodeClass) {
        #[cfg(feature = "observability")]
        self.remoting.record_response_duplicate(observable_code_class(code));

        #[cfg(not(feature = "observability"))]
        let _ = code;
    }

    pub(crate) fn record_v2_response(&self, metadata: ResponseMetadataV2) {
        #[cfg(test)]
        if let Some(capture) = &self.v2_boundary_metrics {
            capture.responses.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            capture.response_events.lock().push((
                response_mode_label(metadata.mode()),
                response_result_label(metadata.outcome()),
            ));
        }

        #[cfg(feature = "observability")]
        {
            use rocketmq_observability::metrics::remoting::ResponseAbandonedReason;
            use rocketmq_observability::metrics::remoting::ResponseMode;
            use rocketmq_observability::metrics::remoting::ResponseResult;

            let mode = match metadata.mode() {
                ResponseObservationModeV2::Inline => ResponseMode::Inline,
                ResponseObservationModeV2::Deferred => ResponseMode::Deferred,
                ResponseObservationModeV2::NoResponse => ResponseMode::NoResponse,
            };
            let result = match metadata.outcome() {
                ResponseObservationOutcomeV2::Written(receipt) => match receipt.disposition() {
                    ResponseDisposition::TransportWritten => ResponseResult::TransportWritten,
                    ResponseDisposition::InProcessAccepted => ResponseResult::InProcessAccepted,
                },
                ResponseObservationOutcomeV2::Oneway => ResponseResult::Oneway,
                ResponseObservationOutcomeV2::ProtocolNoResponse => ResponseResult::ProtocolNoResponse,
                ResponseObservationOutcomeV2::Cancelled(reason) => {
                    let reason = match reason {
                        DeferredTerminalReason::Explicit => ResponseAbandonedReason::Explicit,
                        DeferredTerminalReason::ReceiverDropped => ResponseAbandonedReason::ReceiverDropped,
                        DeferredTerminalReason::Abandoned => ResponseAbandonedReason::Abandoned,
                        DeferredTerminalReason::ClaimDropped => ResponseAbandonedReason::ClaimDropped,
                        DeferredTerminalReason::OwnerDeadline => ResponseAbandonedReason::OwnerDeadline,
                        DeferredTerminalReason::ParentCancelled => ResponseAbandonedReason::ParentCancelled,
                        DeferredTerminalReason::ProcessorUnavailable => ResponseAbandonedReason::ProcessorUnavailable,
                        DeferredTerminalReason::ServiceStopping => ResponseAbandonedReason::ServiceStopping,
                        DeferredTerminalReason::SessionClosed => ResponseAbandonedReason::SessionClosed,
                    };
                    self.remoting.record_response_abandoned(reason);
                    ResponseResult::Cancelled
                }
                ResponseObservationOutcomeV2::Failed { .. } => ResponseResult::Failed,
            };
            self.remoting.record_response(mode, result);
        }

        #[cfg(not(feature = "observability"))]
        let _ = metadata;
    }

    #[inline]
    fn record_v2_boundary_rejection(&self, reason: V2BoundaryRejectionReason, metadata: ResponseMetadataV2) {
        #[cfg(test)]
        if let Some(capture) = &self.v2_boundary_metrics {
            capture.rejections.lock().push((
                reason.as_str(),
                "failed",
                "rejected",
                response_mode_label(metadata.mode()),
                response_result_label(metadata.outcome()),
            ));
        }

        #[cfg(not(test))]
        let _ = (reason, metadata);
    }

    #[inline]
    pub(crate) fn record_outbound_attempted_plaintext_bytes(&self, bytes: usize) {
        #[cfg(feature = "observability")]
        self.remoting.record_outbound_attempted_plaintext_bytes(bytes as u64);

        #[cfg(not(feature = "observability"))]
        let _ = bytes;
    }

    #[inline]
    pub(crate) fn record_outbound_accepted_plaintext_bytes(&self, bytes: usize) {
        #[cfg(feature = "observability")]
        self.remoting.record_outbound_accepted_plaintext_bytes(bytes as u64);

        #[cfg(not(feature = "observability"))]
        let _ = bytes;
    }

    #[inline]
    pub(crate) fn record_outbound_written_plaintext_bytes(&self, bytes: usize) {
        #[cfg(feature = "observability")]
        self.remoting.record_outbound_written_plaintext_bytes(bytes as u64);

        #[cfg(not(feature = "observability"))]
        let _ = bytes;
    }

    #[inline]
    pub(crate) fn record_inbound_decoded_plaintext_bytes(&self, bytes: usize) {
        #[cfg(feature = "observability")]
        self.remoting.record_inbound_decoded_plaintext_bytes(bytes as u64);

        #[cfg(not(feature = "observability"))]
        let _ = bytes;
    }

    #[inline]
    pub(crate) fn record_lifecycle_event(&self, event: &'static str, result: &'static str) {
        #[cfg(test)]
        if let Some(capture) = &self.lifecycle_events {
            capture.lock().push((event, result));
        }

        #[cfg(feature = "observability")]
        self.remoting.record_lifecycle_event(event, result);

        #[cfg(not(feature = "observability"))]
        let _ = (event, result);
    }

    #[inline]
    pub(crate) fn record_deferred_terminal(&self, request_code_bucket: &'static str, reason: &'static str) {
        #[cfg(test)]
        if let Some(capture) = &self.deferred_terminals {
            capture.lock().push((request_code_bucket, reason));
        }

        #[cfg(feature = "observability")]
        self.remoting.record_deferred_terminal(request_code_bucket, reason);

        #[cfg(not(feature = "observability"))]
        let _ = (request_code_bucket, reason);
    }

    #[inline]
    pub(crate) fn record_nameserver_failover(&self, reason: TransportNameServerFailoverReason) {
        #[cfg(feature = "observability")]
        self.client.record_nameserver_failover(match reason {
            TransportNameServerFailoverReason::ConnectFailure => {
                rocketmq_observability::metrics::client::NameServerFailoverReason::ConnectFailure
            }
            TransportNameServerFailoverReason::Unhealthy => {
                rocketmq_observability::metrics::client::NameServerFailoverReason::Unhealthy
            }
            TransportNameServerFailoverReason::CircuitOpen => {
                rocketmq_observability::metrics::client::NameServerFailoverReason::CircuitOpen
            }
            TransportNameServerFailoverReason::Draining => {
                rocketmq_observability::metrics::client::NameServerFailoverReason::Draining
            }
        });

        #[cfg(not(feature = "observability"))]
        let _ = reason;
    }

    #[inline]
    pub(crate) fn record_go_away(&self, outcome: TransportGoAwayOutcome) {
        self.record_lifecycle_event("go_away", outcome.as_str());
    }

    #[inline]
    pub(crate) fn v2_request_guard(&self, request_code: i32, request_bytes: u64) -> TransportRequestMetricsGuard {
        #[cfg(feature = "observability")]
        {
            let code_class = TransportRequestCodeClass::from_code(request_code);
            TransportRequestMetricsGuard {
                inner: rocketmq_observability::metrics::remoting::RequestMetricsGuard::start_v2(
                    self.remoting.clone(),
                    request_code,
                    request_bytes,
                    matches!(
                        code_class,
                        TransportRequestCodeClass::PullMessage
                            | TransportRequestCodeClass::PopMessage
                            | TransportRequestCodeClass::Notification
                    ),
                    observable_code_class(code_class),
                ),
            }
        }

        #[cfg(not(feature = "observability"))]
        {
            let _ = (request_code, request_bytes);
            TransportRequestMetricsGuard {}
        }
    }
}

#[cfg(feature = "observability")]
const fn observable_code_class(
    code: TransportRequestCodeClass,
) -> rocketmq_observability::metrics::remoting::RequestCodeClass {
    use rocketmq_observability::metrics::remoting::RequestCodeClass;

    match code {
        TransportRequestCodeClass::PullMessage => RequestCodeClass::PullMessage,
        TransportRequestCodeClass::PopMessage => RequestCodeClass::PopMessage,
        TransportRequestCodeClass::Notification => RequestCodeClass::Notification,
        TransportRequestCodeClass::Other => RequestCodeClass::Other,
    }
}

#[derive(Clone, Copy)]
pub(crate) enum TransportNameServerFailoverReason {
    ConnectFailure,
    Unhealthy,
    CircuitOpen,
    Draining,
}

#[derive(Clone, Copy)]
pub(crate) enum TransportGoAwayOutcome {
    Received,
    RetrySuccess,
    RetryFailed,
}

impl TransportGoAwayOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Received => "received",
            Self::RetrySuccess => "retry_success",
            Self::RetryFailed => "retry_failed",
        }
    }
}

pub(crate) struct TransportRequestMetricsGuard {
    #[cfg(feature = "observability")]
    inner: rocketmq_observability::metrics::remoting::RequestMetricsGuard,
}

impl TransportRequestMetricsGuard {
    #[inline]
    pub(crate) fn complete_oneway(&mut self) {
        #[cfg(feature = "observability")]
        self.inner.complete_oneway();
    }

    #[inline]
    pub(crate) fn complete_v2_reply(&mut self, response_code: i32, body_kind: ResponseBodyKind) {
        #[cfg(feature = "observability")]
        self.inner.complete_v2(
            response_code,
            match body_kind {
                ResponseBodyKind::Empty => rocketmq_observability::metrics::remoting::RequestOutcome::ReplyEmpty,
                ResponseBodyKind::Bytes => rocketmq_observability::metrics::remoting::RequestOutcome::ReplyBytes,
                ResponseBodyKind::Segments => rocketmq_observability::metrics::remoting::RequestOutcome::ReplySegments,
                ResponseBodyKind::FileRegions => {
                    rocketmq_observability::metrics::remoting::RequestOutcome::ReplyFileRegions
                }
            },
        );

        #[cfg(not(feature = "observability"))]
        let _ = (response_code, body_kind);
    }

    #[inline]
    pub(crate) fn record_deferred_registered(&mut self) {
        #[cfg(feature = "observability")]
        self.inner.record_v2_deferred_registered();
    }

    #[inline]
    pub(crate) fn complete_deferred_resumed(&mut self, response_code: i32) {
        #[cfg(feature = "observability")]
        self.inner.complete_v2(
            response_code,
            rocketmq_observability::metrics::remoting::RequestOutcome::DeferredResumed,
        );

        #[cfg(not(feature = "observability"))]
        let _ = response_code;
    }

    #[inline]
    pub(crate) fn complete_protocol_no_response(&mut self) {
        #[cfg(feature = "observability")]
        self.inner.complete_v2(
            NO_RESPONSE_CODE,
            rocketmq_observability::metrics::remoting::RequestOutcome::ProtocolNoResponse,
        );
    }

    #[inline]
    pub(crate) fn complete_cancelled(&mut self) {
        #[cfg(feature = "observability")]
        self.inner.complete_cancelled();
    }

    #[inline]
    pub(crate) fn complete_process_request_failed(&mut self, response_code: i32) {
        #[cfg(feature = "observability")]
        self.inner.complete_process_request_failed(response_code);

        #[cfg(not(feature = "observability"))]
        let _ = response_code;
    }

    #[inline]
    pub(crate) fn complete_write_channel_failed(&mut self, response_code: i32) {
        #[cfg(feature = "observability")]
        self.inner.complete_write_channel_failed(response_code);

        #[cfg(not(feature = "observability"))]
        let _ = response_code;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fmt;
    use std::sync::atomic::AtomicU64;
    use std::sync::Arc;
    use std::sync::Mutex;

    use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
    use rocketmq_security_api::PeerInfo;
    use rocketmq_security_api::Principal;
    use tracing::field::Field;
    use tracing::field::Visit;
    use tracing::span::Attributes;
    use tracing::span::Id;
    use tracing::span::Record;
    use tracing::Event;
    use tracing::Metadata;
    use tracing::Subscriber;

    use super::v2_request_span;
    use super::TransportGoAwayOutcome;
    use super::TransportNameServerFailoverReason;
    use super::TransportRequestCodeClass;
    use super::TransportTelemetry;
    use crate::dispatch::AuthenticationState;
    use crate::dispatch::DeferredTerminalReason;
    use crate::dispatch::RequestOrigin;
    use crate::runtime::processor_v2::ResponseObservationOutcomeV2;

    struct FieldCapture<'a> {
        fields: &'a mut BTreeMap<String, String>,
    }

    impl Visit for FieldCapture<'_> {
        fn record_i64(&mut self, field: &Field, value: i64) {
            self.fields.insert(field.name().to_owned(), value.to_string());
        }

        fn record_u64(&mut self, field: &Field, value: u64) {
            self.fields.insert(field.name().to_owned(), value.to_string());
        }

        fn record_str(&mut self, field: &Field, value: &str) {
            self.fields.insert(field.name().to_owned(), value.to_owned());
        }

        fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
            self.fields.insert(field.name().to_owned(), format!("{value:?}"));
        }
    }

    struct SpanCapture {
        fields: Arc<Mutex<BTreeMap<String, String>>>,
    }

    impl Subscriber for SpanCapture {
        fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, attributes: &Attributes<'_>) -> Id {
            let mut fields = self.fields.lock().expect("span field capture lock");
            attributes.record(&mut FieldCapture { fields: &mut fields });
            Id::from_u64(1)
        }

        fn record(&self, _span: &Id, values: &Record<'_>) {
            let mut fields = self.fields.lock().expect("span field capture lock");
            values.record(&mut FieldCapture { fields: &mut fields });
        }

        fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

        fn event(&self, _event: &Event<'_>) {}

        fn enter(&self, _span: &Id) {}

        fn exit(&self, _span: &Id) {}
    }

    #[test]
    fn noop_transport_telemetry_covers_request_and_network_paths() {
        let telemetry = TransportTelemetry::noop();
        telemetry.record_outbound_attempted_plaintext_bytes(128);
        telemetry.record_outbound_accepted_plaintext_bytes(128);
        telemetry.record_outbound_written_plaintext_bytes(128);
        telemetry.record_lifecycle_event("connected", "queued");
        telemetry.record_deferred_terminal("other", "abandoned");
        telemetry.record_nameserver_failover(TransportNameServerFailoverReason::ConnectFailure);
        telemetry.record_go_away(TransportGoAwayOutcome::Received);
    }

    #[test]
    fn lifecycle_capture_records_the_exact_low_cardinality_pair() {
        let (telemetry, events) = TransportTelemetry::with_lifecycle_event_capture();
        telemetry.record_lifecycle_event("connected", "queued");
        assert_eq!(events.lock().as_slice(), [("connected", "queued")]);
    }

    #[test]
    fn v2_span_uses_only_trusted_classifications_and_redacted_terminal_fields() {
        let identity = crate::dispatch::OriginalRequestIdentity::capture(
            83,
            &AtomicU64::new(5),
            &RemotingCommand::create_remoting_command(-99_001).set_opaque(777),
        )
        .expect("test request identity");
        let origin = RequestOrigin::Network {
            peer: PeerInfo::new("192.168.10.4:10911".parse().expect("test address"), true),
        };
        let authentication = AuthenticationState::authenticated(Principal::new("must-not-appear"));
        let fields = Arc::new(Mutex::new(BTreeMap::new()));
        let subscriber = SpanCapture {
            fields: Arc::clone(&fields),
        };

        tracing::subscriber::with_default(subscriber, || {
            let span = v2_request_span(
                identity,
                TransportRequestCodeClass::Other,
                &origin,
                &authentication,
                None,
            );
            span.record("outcome", "failed");
            span.record("error_kind", "transport");
            span.record("write_progress", "not_started");
        });

        let fields = fields.lock().expect("captured V2 request span fields");
        assert_eq!(
            fields.get("rocketmq.request.origin_kind").map(String::as_str),
            Some("network")
        );
        assert_eq!(
            fields.get("rocketmq.request.peer_class").map(String::as_str),
            Some("private")
        );
        assert_eq!(
            fields.get("rocketmq.request.authentication_state").map(String::as_str),
            Some("authenticated")
        );
        assert_eq!(fields.get("outcome").map(String::as_str), Some("failed"));
        assert_eq!(fields.get("error_kind").map(String::as_str), Some("transport"));
        assert!(fields.keys().all(|field| {
            ![
                "opaque",
                "topic",
                "group",
                "client",
                "principal",
                "body",
                "credential",
                "token",
                "config",
            ]
            .iter()
            .any(|forbidden| field.contains(forbidden))
                || field == "rocketmq.request.principal_kind"
        }));
        assert!(!fields
            .values()
            .any(|value| value.contains("must-not-appear") || value.contains("777")));
    }

    #[test]
    fn deferred_registration_is_not_a_terminal_callback_and_completion_is_exactly_once() {
        let identity = crate::dispatch::OriginalRequestIdentity::capture(
            89,
            &AtomicU64::new(7),
            &RemotingCommand::create_remoting_command(11).set_opaque(991),
        )
        .expect("test request identity");
        let observed = Arc::new(Mutex::new(Vec::new()));
        let callback_observed = Arc::clone(&observed);
        let observation = TransportTelemetry::noop().begin_v2_observation(
            identity,
            std::time::Instant::now(),
            &RequestOrigin::Network {
                peer: PeerInfo::new("127.0.0.1:10911".parse().expect("test address"), false),
            },
            &AuthenticationState::Anonymous,
            None,
            0,
        );
        observation.bind_response_observer(move |event| {
            callback_observed.lock().expect("observation capture").push(event);
        });

        observation.arm_deferred_metrics(128);
        observation.record_deferred_registered();
        assert!(observed.lock().expect("observation capture").is_empty());

        observation.complete_no_response(ResponseObservationOutcomeV2::ProtocolNoResponse);
        observation.complete_cancelled(DeferredTerminalReason::Abandoned);
        let observed = observed.lock().expect("observation capture");
        assert_eq!(observed.len(), 1);
        assert_eq!(
            observed[0].metadata().outcome(),
            ResponseObservationOutcomeV2::ProtocolNoResponse
        );
    }

    #[test]
    fn terminal_observation_waits_for_late_processor_binding_and_delivers_once() {
        let identity = crate::dispatch::OriginalRequestIdentity::capture(
            97,
            &AtomicU64::new(3),
            &RemotingCommand::create_remoting_command(12).set_opaque(19),
        )
        .expect("test request identity");
        let observation = TransportTelemetry::noop().begin_v2_observation(
            identity,
            std::time::Instant::now(),
            &RequestOrigin::Network {
                peer: PeerInfo::new("127.0.0.1:10911".parse().expect("test address"), false),
            },
            &AuthenticationState::Anonymous,
            None,
            0,
        );
        observation.complete_no_response(ResponseObservationOutcomeV2::ProtocolNoResponse);

        let observed = Arc::new(Mutex::new(Vec::new()));
        let callback_observed = Arc::clone(&observed);
        observation.bind_response_observer(move |event| {
            callback_observed.lock().expect("observation capture").push(event);
        });
        observation.bind_response_observer(|_| panic!("terminal observation callback must remain exactly once"));

        let observed = observed.lock().expect("observation capture");
        assert_eq!(observed.len(), 1);
        assert_eq!(
            observed[0].metadata().outcome(),
            ResponseObservationOutcomeV2::ProtocolNoResponse
        );
    }
}
