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

use std::convert::Infallible;
use std::error::Error;
use std::fmt;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use rocketmq_error::RocketMQError;
use rocketmq_protocol::protocol::header::pop_lite_message_request_header::PopLiteMessageRequestHeader;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_transport::api::v2::DeferredAdmissionAcquireError;
use rocketmq_transport::api::v2::DeferredExpiryError;
use rocketmq_transport::api::v2::DeferredExpiryErrorKind;
use rocketmq_transport::api::v2::DeferredId;
use rocketmq_transport::api::v2::DeferredParts;
use rocketmq_transport::api::v2::DeferredRegistration;
use rocketmq_transport::api::v2::DeferredRegistry;
use rocketmq_transport::api::v2::DeferredRegistryError;
use rocketmq_transport::api::v2::DeferredRegistryErrorKind;
use rocketmq_transport::api::v2::DeferredRetainedSizeParts;
use rocketmq_transport::api::v2::DeferredWaitPermit;
use rocketmq_transport::api::v2::RemotingRequest;
use rocketmq_transport::api::v2::RequestId;
use rocketmq_transport::api::v2::RequestOrigin;
use rocketmq_transport::api::v2::SessionId;
use rocketmq_transport::api::v2::TakeDeferredResponderError;

use super::data::PopLiteRequestData;
use super::data::ResumePopLite;
use super::deadline::PopLiteWaitDeadline;
use super::deadline::PopLiteWaitDeadlineError;
use super::index::PopLiteCriteriaIndex;
use super::index::PopLiteIndexError;
use super::index::PopLiteIndexErrorKind;
use super::index::PopLiteIndexReservation;
use super::service::ObservationGuard;
use super::service::ObservationKind;
use super::service::PopLiteDeferredService;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct PopLiteRetainedEstimate {
    pub(crate) resume_bytes: usize,
    pub(crate) metadata_bytes: usize,
}

impl PopLiteRetainedEstimate {
    pub(crate) const fn new(resume_bytes: usize, metadata_bytes: usize) -> Self {
        Self {
            resume_bytes,
            metadata_bytes,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PreparedRequestProvenance {
    request_id: RequestId,
    session_id: SessionId,
}

impl PreparedRequestProvenance {
    fn capture(request: &RemotingRequest) -> Self {
        Self {
            request_id: request.original_identity().request_id(),
            session_id: request.session().id(),
        }
    }

    fn matches(self, request: &RemotingRequest) -> bool {
        self.request_id == request.original_identity().request_id() && self.session_id == request.session().id()
    }
}

#[must_use]
pub(crate) struct PreparedPopLiteRegistration {
    request: PopLiteRequestData,
    deadline: PopLiteWaitDeadline,
    reservation: PopLiteIndexReservation,
    permit: DeferredWaitPermit,
    provenance: PreparedRequestProvenance,
    _observation: ObservationGuard,
}

impl PreparedPopLiteRegistration {
    pub(crate) const fn deadline(&self) -> PopLiteWaitDeadline {
        self.deadline
    }

    pub(crate) const fn retained_bytes(&self) -> usize {
        self.permit.retained_bytes()
    }
}

impl PopLiteDeferredService {
    pub(crate) fn prepare(
        &self,
        request: &RemotingRequest,
        retained: PopLiteRetainedEstimate,
    ) -> Result<PreparedPopLiteRegistration, PopLiteDeferredPrepareError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(PopLiteDeferredPrepareError::ServiceClosed);
        }
        if request.command().is_oneway_rpc() {
            return Err(PopLiteDeferredPrepareError::OneWay);
        }
        match request.origin() {
            RequestOrigin::Network { .. } => {}
            _ => return Err(PopLiteDeferredPrepareError::EmbeddedOrigin),
        }
        if self.expiry_margins.recovery().is_zero() || self.expiry_margins.write().is_zero() {
            return Err(PopLiteDeferredPrepareError::InvalidExpiryMargins);
        }
        let header = request
            .command()
            .decode_command_custom_header::<PopLiteMessageRequestHeader>()
            .map_err(PopLiteDeferredPrepareError::Header)?;
        validate_header(&header)?;
        let wall_now = current_millis();
        let monotonic_now = tokio::time::Instant::now();
        let deadline = PopLiteWaitDeadline::checked(
            header.born_time,
            header.poll_time,
            wall_now,
            monotonic_now,
            self.max_age,
        )
        .map_err(PopLiteDeferredPrepareError::Deadline)?;
        let provenance = PreparedRequestProvenance::capture(request);
        let request = PopLiteRequestData::new(header);
        let dynamic_bytes = request
            .try_estimated_dynamic_bytes()
            .ok_or(PopLiteDeferredPrepareError::RetainedSizeOverflow)?;
        let resume_bytes = retained
            .resume_bytes
            .checked_add(dynamic_bytes)
            .ok_or(PopLiteDeferredPrepareError::RetainedSizeOverflow)?;
        let index_bytes = PopLiteCriteriaIndex::<DeferredId>::try_retained_bytes_per_entry()
            .ok_or(PopLiteDeferredPrepareError::RetainedSizeOverflow)?;
        let retained_parts = DeferredRetainedSizeParts::new(resume_bytes)
            .with_secondary_index_bytes(index_bytes)
            .with_metadata_bytes(retained.metadata_bytes);
        let retained_size = DeferredRegistry::<ResumePopLite>::try_retained_size(retained_parts)
            .map_err(PopLiteDeferredPrepareError::Admission)?;
        let reservation = self
            .index
            .reserve(request.client_id().clone(), monotonic_now)
            .map_err(PopLiteDeferredPrepareError::Index)?;
        let permit = self
            .admission
            .try_reserve(retained_size)
            .map_err(PopLiteDeferredPrepareError::Admission)?;
        let prepared = PreparedPopLiteRegistration {
            request,
            deadline,
            reservation,
            permit,
            provenance,
            _observation: ObservationGuard::new(Arc::clone(&self.observations), ObservationKind::Prepared),
        };
        if self.closed.load(Ordering::Acquire) {
            drop(prepared);
            return Err(PopLiteDeferredPrepareError::ServiceClosed);
        }
        Ok(prepared)
    }

    pub(crate) fn register(
        &self,
        prepared: PreparedPopLiteRegistration,
        request: &mut RemotingRequest,
    ) -> Result<DeferredRegistration, PopLiteDeferredRegisterError> {
        if !prepared.provenance.matches(request) {
            return Err(PopLiteDeferredRegisterError::ProvenanceMismatch);
        }
        if self.closed.load(Ordering::Acquire) {
            return Err(PopLiteDeferredRegisterError::ServiceClosed);
        }
        let client_id = prepared.request.client_id().clone();
        let responder = request
            .take_deferred_responder()
            .map_err(PopLiteDeferredRegisterError::Responder)?;
        #[cfg(test)]
        self.wait_register_after_take_hook();
        if self.closed.load(Ordering::Acquire) {
            drop(responder);
            return Err(PopLiteDeferredRegisterError::ServiceClosedAfterTake);
        }
        let PreparedPopLiteRegistration {
            request,
            deadline,
            reservation,
            permit,
            provenance: _,
            _observation,
        } = prepared;
        #[cfg(test)]
        let protocol_at = if self.fail_next_expiry_attachment.swap(false, Ordering::AcqRel) {
            tokio::time::Instant::now()
        } else {
            deadline.protocol_at()
        };
        #[cfg(not(test))]
        let protocol_at = deadline.protocol_at();
        let parts = DeferredParts::new(responder, permit)
            .try_with_expiry(protocol_at, self.expiry_margins)
            .map_err(PopLiteDeferredRegisterError::Expiry)?;
        let registration = self
            .registry
            .register_with(parts, move |id| {
                let index_lease = reservation.publish(id, deadline);
                Ok::<_, Infallible>(ResumePopLite::new(request, deadline, index_lease))
            })
            .map_err(PopLiteDeferredRegisterError::Registry)?;
        drop(_observation);
        self.observe_pending_event(&client_id);
        Ok(registration)
    }
}

fn validate_header(header: &PopLiteMessageRequestHeader) -> Result<(), PopLiteDeferredPrepareError> {
    if header.client_id.is_empty() || header.consumer_group.is_empty() || header.topic.is_empty() {
        return Err(PopLiteDeferredPrepareError::InvalidHeader);
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum PopLiteDeferredPrepareErrorKind {
    ServiceClosed,
    OneWay,
    EmbeddedOrigin,
    InvalidExpiryMargins,
    Header,
    InvalidHeader,
    Deadline,
    RetainedSizeOverflow,
    Index(PopLiteIndexErrorKind),
    Admission,
}

pub(crate) enum PopLiteDeferredPrepareError {
    ServiceClosed,
    OneWay,
    EmbeddedOrigin,
    InvalidExpiryMargins,
    Header(RocketMQError),
    InvalidHeader,
    Deadline(PopLiteWaitDeadlineError),
    RetainedSizeOverflow,
    Index(PopLiteIndexError),
    Admission(DeferredAdmissionAcquireError),
}

impl PopLiteDeferredPrepareError {
    pub(crate) const fn kind(&self) -> PopLiteDeferredPrepareErrorKind {
        match self {
            Self::ServiceClosed => PopLiteDeferredPrepareErrorKind::ServiceClosed,
            Self::OneWay => PopLiteDeferredPrepareErrorKind::OneWay,
            Self::EmbeddedOrigin => PopLiteDeferredPrepareErrorKind::EmbeddedOrigin,
            Self::InvalidExpiryMargins => PopLiteDeferredPrepareErrorKind::InvalidExpiryMargins,
            Self::Header(_) => PopLiteDeferredPrepareErrorKind::Header,
            Self::InvalidHeader => PopLiteDeferredPrepareErrorKind::InvalidHeader,
            Self::Deadline(_) => PopLiteDeferredPrepareErrorKind::Deadline,
            Self::RetainedSizeOverflow => PopLiteDeferredPrepareErrorKind::RetainedSizeOverflow,
            Self::Index(source) => PopLiteDeferredPrepareErrorKind::Index(source.kind()),
            Self::Admission(_) => PopLiteDeferredPrepareErrorKind::Admission,
        }
    }
}

impl fmt::Debug for PopLiteDeferredPrepareError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("PopLiteDeferredPrepareError")
            .field(&self.kind())
            .finish()
    }
}

impl fmt::Display for PopLiteDeferredPrepareError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "PopLite deferred preparation failed: {:?}", self.kind())
    }
}

impl Error for PopLiteDeferredPrepareError {}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum PopLiteDeferredRegisterErrorKind {
    ServiceClosed,
    ServiceClosedAfterTake,
    ProvenanceMismatch,
    Responder,
    Expiry(DeferredExpiryErrorKind),
    Registry(DeferredRegistryErrorKind),
}

pub(crate) enum PopLiteDeferredRegisterError {
    ServiceClosed,
    ServiceClosedAfterTake,
    ProvenanceMismatch,
    Responder(TakeDeferredResponderError),
    Expiry(DeferredExpiryError),
    Registry(DeferredRegistryError<ResumePopLite, Infallible>),
}

impl PopLiteDeferredRegisterError {
    pub(crate) const fn kind(&self) -> PopLiteDeferredRegisterErrorKind {
        match self {
            Self::ServiceClosed => PopLiteDeferredRegisterErrorKind::ServiceClosed,
            Self::ServiceClosedAfterTake => PopLiteDeferredRegisterErrorKind::ServiceClosedAfterTake,
            Self::ProvenanceMismatch => PopLiteDeferredRegisterErrorKind::ProvenanceMismatch,
            Self::Responder(_) => PopLiteDeferredRegisterErrorKind::Responder,
            Self::Expiry(source) => PopLiteDeferredRegisterErrorKind::Expiry(source.kind()),
            Self::Registry(source) => PopLiteDeferredRegisterErrorKind::Registry(source.kind()),
        }
    }
}

impl fmt::Debug for PopLiteDeferredRegisterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("PopLiteDeferredRegisterError")
            .field(&self.kind())
            .finish()
    }
}

impl fmt::Display for PopLiteDeferredRegisterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "PopLite deferred registration failed: {:?}", self.kind())
    }
}

impl Error for PopLiteDeferredRegisterError {}
