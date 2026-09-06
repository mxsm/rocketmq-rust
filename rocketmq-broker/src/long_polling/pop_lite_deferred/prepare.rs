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
use rocketmq_transport::api::DeferredAdmissionAcquireOutcome;
use rocketmq_transport::api::DeferredExpiryOutcome;
use rocketmq_transport::api::DeferredId;
use rocketmq_transport::api::DeferredParts;
use rocketmq_transport::api::DeferredRegistration;
use rocketmq_transport::api::DeferredRegistry;
use rocketmq_transport::api::DeferredRegistryOutcome;
use rocketmq_transport::api::DeferredRegistryRecovery;
use rocketmq_transport::api::DeferredResponderOutcome;
use rocketmq_transport::api::DeferredRetainedSizeParts;
use rocketmq_transport::api::DeferredWaitPermit;
use rocketmq_transport::api::RemotingRequest;
use rocketmq_transport::api::RequestId;
use rocketmq_transport::api::RequestOrigin;
use rocketmq_transport::api::SessionId;
use rocketmq_transport::api::TransportContractViolation;
use rocketmq_transport::api::TransportError;

use super::data::PopLiteRequestData;
use super::data::ResumePopLite;
use super::deadline::PopLiteWaitDeadline;
use super::deadline::PopLiteWaitDeadlineOperationalError;
use super::deadline::PopLiteWaitDeadlineOutcome;
use super::deadline::PopLiteWaitDeadlineRejection;
use super::index::PopLiteCriteriaIndex;
use super::index::PopLiteIndexOperationalError;
use super::index::PopLiteIndexReservation;
use super::index::PopLiteIndexReserveOutcome;
use super::index::PopLiteIndexReserveRejection;
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
    ) -> Result<PopLiteDeferredPrepareOutcome, PopLiteDeferredPrepareFailure> {
        if self.closed.load(Ordering::Acquire) {
            return Ok(PopLiteDeferredPrepareOutcome::Rejected(
                PopLiteDeferredPrepareRejection::ServiceClosed,
            ));
        }
        if request.command().is_oneway_rpc() {
            return Ok(PopLiteDeferredPrepareOutcome::Rejected(
                PopLiteDeferredPrepareRejection::OneWay,
            ));
        }
        match request.origin() {
            RequestOrigin::Network { .. } => {}
            _ => {
                return Ok(PopLiteDeferredPrepareOutcome::Rejected(
                    PopLiteDeferredPrepareRejection::EmbeddedOrigin,
                ));
            }
        }
        if self.expiry_margins.recovery().is_zero() || self.expiry_margins.write().is_zero() {
            return Err(PopLiteDeferredPrepareFailure::InvalidExpiryMargins);
        }
        let header = request
            .command()
            .decode_command_custom_header::<PopLiteMessageRequestHeader>()
            .map_err(PopLiteDeferredPrepareFailure::Header)?;
        if let Err(rejection) = validate_header(&header) {
            return Ok(PopLiteDeferredPrepareOutcome::Rejected(rejection));
        }
        let wall_now = current_millis();
        let monotonic_now = tokio::time::Instant::now();
        let deadline = match PopLiteWaitDeadline::checked(
            header.born_time,
            header.poll_time,
            wall_now,
            monotonic_now,
            self.max_age,
        ) {
            Ok(PopLiteWaitDeadlineOutcome::Pending(deadline)) => deadline,
            Ok(PopLiteWaitDeadlineOutcome::Rejected(rejection)) => {
                return Ok(PopLiteDeferredPrepareOutcome::Rejected(
                    PopLiteDeferredPrepareRejection::Deadline(rejection),
                ));
            }
            Err(error) => return Err(PopLiteDeferredPrepareFailure::Deadline(error)),
        };
        let provenance = PreparedRequestProvenance::capture(request);
        let request = PopLiteRequestData::new(header);
        let dynamic_bytes = request
            .try_estimated_dynamic_bytes()
            .ok_or(PopLiteDeferredPrepareFailure::RetainedSizeOverflow)?;
        let Some(resume_bytes) = retained.resume_bytes.checked_add(dynamic_bytes) else {
            return Err(PopLiteDeferredPrepareFailure::RetainedSizeOverflow);
        };
        let Some(index_bytes) = PopLiteCriteriaIndex::<DeferredId>::try_retained_bytes_per_entry() else {
            return Err(PopLiteDeferredPrepareFailure::RetainedSizeOverflow);
        };
        let retained_parts = DeferredRetainedSizeParts::new(resume_bytes)
            .with_secondary_index_bytes(index_bytes)
            .with_metadata_bytes(retained.metadata_bytes);
        let retained_size = DeferredRegistry::<ResumePopLite>::try_retained_size(retained_parts)
            .map_err(PopLiteDeferredPrepareFailure::Contract)?;
        let reservation = match self
            .index
            .reserve(request.client_id().clone(), monotonic_now)
            .map_err(PopLiteDeferredPrepareFailure::Index)?
        {
            PopLiteIndexReserveOutcome::Reserved(reservation) => reservation,
            PopLiteIndexReserveOutcome::Rejected(rejection) => {
                return Ok(PopLiteDeferredPrepareOutcome::Rejected(
                    PopLiteDeferredPrepareRejection::IndexCapacity(rejection),
                ));
            }
        };
        let permit = match self.admission.try_reserve(retained_size) {
            DeferredAdmissionAcquireOutcome::Acquired(permit) => permit,
            outcome => {
                return Ok(PopLiteDeferredPrepareOutcome::Rejected(
                    PopLiteDeferredPrepareRejection::Admission(outcome),
                ));
            }
        };
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
            return Ok(PopLiteDeferredPrepareOutcome::Rejected(
                PopLiteDeferredPrepareRejection::ServiceClosed,
            ));
        }
        Ok(PopLiteDeferredPrepareOutcome::Prepared(Box::new(prepared)))
    }

    pub(crate) fn register(
        &self,
        prepared: PreparedPopLiteRegistration,
        request: &mut RemotingRequest,
    ) -> Result<PopLiteDeferredRegisterOutcome, PopLiteDeferredRegisterFailure> {
        if !prepared.provenance.matches(request) {
            return Ok(PopLiteDeferredRegisterOutcome::Rejected(Box::new(
                PopLiteDeferredRegisterRejection::ProvenanceMismatch,
            )));
        }
        if self.closed.load(Ordering::Acquire) {
            return Ok(PopLiteDeferredRegisterOutcome::Rejected(Box::new(
                PopLiteDeferredRegisterRejection::ServiceClosed,
            )));
        }
        let client_id = prepared.request.client_id().clone();
        let responder = match request.take_deferred_responder() {
            DeferredResponderOutcome::Taken(responder) => responder,
            outcome => {
                return Ok(PopLiteDeferredRegisterOutcome::Rejected(Box::new(
                    PopLiteDeferredRegisterRejection::Responder(outcome),
                )));
            }
        };
        #[cfg(test)]
        self.wait_register_after_take_hook();
        if self.closed.load(Ordering::Acquire) {
            drop(responder);
            return Ok(PopLiteDeferredRegisterOutcome::Rejected(Box::new(
                PopLiteDeferredRegisterRejection::ServiceClosedAfterTake,
            )));
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
        let mut parts = DeferredParts::new(responder, permit);
        match parts.try_with_expiry(protocol_at, self.expiry_margins) {
            Ok(DeferredExpiryOutcome::Attached) => {}
            Ok(outcome) => {
                return Ok(PopLiteDeferredRegisterOutcome::Rejected(Box::new(
                    PopLiteDeferredRegisterRejection::Expiry { outcome, parts },
                )));
            }
            Err(violation) => {
                return Err(PopLiteDeferredRegisterFailure::Contract {
                    violation,
                    parts: Box::new(parts),
                });
            }
        }
        let registration = match self.registry.register_with(parts, move |id| {
            let index_lease = reservation.publish(id, deadline);
            Ok::<_, Infallible>(ResumePopLite::new(request, deadline, index_lease))
        }) {
            DeferredRegistryOutcome::Registered(registration) => registration,
            DeferredRegistryOutcome::DuplicateRequest(recovery) => {
                release_deferred_registry_recovery(recovery);
                return Ok(PopLiteDeferredRegisterOutcome::Rejected(Box::new(
                    PopLiteDeferredRegisterRejection::DuplicateRequest,
                )));
            }
            DeferredRegistryOutcome::IdentityExhausted(recovery) => {
                release_deferred_registry_recovery(recovery);
                return Err(PopLiteDeferredRegisterFailure::IdentityExhausted);
            }
            DeferredRegistryOutcome::ParentCancelled => {
                return Ok(PopLiteDeferredRegisterOutcome::Rejected(Box::new(
                    PopLiteDeferredRegisterRejection::ParentCancelled,
                )));
            }
            DeferredRegistryOutcome::SessionClosed => {
                return Ok(PopLiteDeferredRegisterOutcome::Rejected(Box::new(
                    PopLiteDeferredRegisterRejection::SessionClosed,
                )));
            }
            DeferredRegistryOutcome::DeadlineExpired => {
                return Ok(PopLiteDeferredRegisterOutcome::Rejected(Box::new(
                    PopLiteDeferredRegisterRejection::DeadlineExpired,
                )));
            }
            DeferredRegistryOutcome::BuilderRejected { error, parts } => {
                drop(parts);
                match error {}
            }
            DeferredRegistryOutcome::ContractViolation { violation, recovery } => {
                release_deferred_registry_recovery(recovery);
                return Err(PopLiteDeferredRegisterFailure::RegistryContract(violation));
            }
            DeferredRegistryOutcome::OperationalFailure { error, recovery } => {
                release_deferred_registry_recovery(recovery);
                return Err(PopLiteDeferredRegisterFailure::RegistryOperational(error));
            }
        };
        drop(_observation);
        self.observe_pending_event(&client_id);
        Ok(PopLiteDeferredRegisterOutcome::Registered(Box::new(registration)))
    }
}

fn validate_header(header: &PopLiteMessageRequestHeader) -> Result<(), PopLiteDeferredPrepareRejection> {
    if header.client_id.is_empty() || header.consumer_group.is_empty() || header.topic.is_empty() {
        return Err(PopLiteDeferredPrepareRejection::InvalidHeader);
    }
    Ok(())
}

pub(crate) enum PopLiteDeferredPrepareRejection {
    ServiceClosed,
    OneWay,
    EmbeddedOrigin,
    InvalidHeader,
    Deadline(PopLiteWaitDeadlineRejection),
    IndexCapacity(PopLiteIndexReserveRejection),
    Admission(DeferredAdmissionAcquireOutcome),
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum PopLiteDeferredPrepareRejectionKind {
    ServiceClosed,
    OneWay,
    EmbeddedOrigin,
    InvalidHeader,
    Deadline,
    Index,
    Admission,
}

impl PopLiteDeferredPrepareRejection {
    pub(crate) const fn kind(&self) -> PopLiteDeferredPrepareRejectionKind {
        match self {
            Self::ServiceClosed => PopLiteDeferredPrepareRejectionKind::ServiceClosed,
            Self::OneWay => PopLiteDeferredPrepareRejectionKind::OneWay,
            Self::EmbeddedOrigin => PopLiteDeferredPrepareRejectionKind::EmbeddedOrigin,
            Self::InvalidHeader => PopLiteDeferredPrepareRejectionKind::InvalidHeader,
            Self::Deadline(_) => PopLiteDeferredPrepareRejectionKind::Deadline,
            Self::IndexCapacity(_) => PopLiteDeferredPrepareRejectionKind::Index,
            Self::Admission(_) => PopLiteDeferredPrepareRejectionKind::Admission,
        }
    }
}

#[must_use]
pub(crate) enum PopLiteDeferredPrepareOutcome {
    Prepared(Box<PreparedPopLiteRegistration>),
    Rejected(PopLiteDeferredPrepareRejection),
}

pub(crate) enum PopLiteDeferredPrepareFailure {
    InvalidExpiryMargins,
    RetainedSizeOverflow,
    Deadline(PopLiteWaitDeadlineOperationalError),
    Header(RocketMQError),
    Index(PopLiteIndexOperationalError),
    Contract(TransportContractViolation),
}

impl fmt::Debug for PopLiteDeferredPrepareFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PopLiteDeferredPrepareFailure")
            .finish_non_exhaustive()
    }
}

impl fmt::Display for PopLiteDeferredPrepareFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PopLite deferred preparation failed")
    }
}

impl Error for PopLiteDeferredPrepareFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Header(source) => Some(source),
            Self::Index(source) => Some(source),
            Self::Contract(source) => Some(source),
            Self::Deadline(source) => Some(source),
            Self::InvalidExpiryMargins | Self::RetainedSizeOverflow => None,
        }
    }
}

pub(crate) enum PopLiteDeferredRegisterRejection {
    ServiceClosed,
    ServiceClosedAfterTake,
    ProvenanceMismatch,
    Responder(DeferredResponderOutcome),
    Expiry {
        outcome: DeferredExpiryOutcome,
        parts: DeferredParts,
    },
    DuplicateRequest,
    ParentCancelled,
    SessionClosed,
    DeadlineExpired,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum PopLiteDeferredRegisterRejectionKind {
    ServiceClosed,
    ServiceClosedAfterTake,
    ProvenanceMismatch,
    Responder,
    Expiry,
    Registry,
}

impl PopLiteDeferredRegisterRejection {
    pub(crate) const fn kind(&self) -> PopLiteDeferredRegisterRejectionKind {
        match self {
            Self::ServiceClosed => PopLiteDeferredRegisterRejectionKind::ServiceClosed,
            Self::ServiceClosedAfterTake => PopLiteDeferredRegisterRejectionKind::ServiceClosedAfterTake,
            Self::ProvenanceMismatch => PopLiteDeferredRegisterRejectionKind::ProvenanceMismatch,
            Self::Responder(_) => PopLiteDeferredRegisterRejectionKind::Responder,
            Self::Expiry { .. } => PopLiteDeferredRegisterRejectionKind::Expiry,
            Self::DuplicateRequest | Self::ParentCancelled | Self::SessionClosed | Self::DeadlineExpired => {
                PopLiteDeferredRegisterRejectionKind::Registry
            }
        }
    }
}

#[must_use]
pub(crate) enum PopLiteDeferredRegisterOutcome {
    Registered(Box<DeferredRegistration>),
    Rejected(Box<PopLiteDeferredRegisterRejection>),
}

pub(crate) enum PopLiteDeferredRegisterFailure {
    IdentityExhausted,
    RegistryContract(TransportContractViolation),
    RegistryOperational(TransportError),
    Contract {
        violation: TransportContractViolation,
        parts: Box<DeferredParts>,
    },
}

fn release_deferred_registry_recovery<R, F>(recovery: DeferredRegistryRecovery<R, F>) {
    match recovery {
        DeferredRegistryRecovery::None => {}
        DeferredRegistryRecovery::Request(request) => drop(request),
        DeferredRegistryRecovery::Parts(parts) => drop(parts),
        DeferredRegistryRecovery::Builder { builder, parts } => {
            drop(builder);
            drop(parts);
        }
    }
}

impl fmt::Debug for PopLiteDeferredRegisterFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PopLiteDeferredRegisterFailure")
            .finish_non_exhaustive()
    }
}

impl fmt::Display for PopLiteDeferredRegisterFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PopLite deferred registration failed")
    }
}

impl Error for PopLiteDeferredRegisterFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::RegistryContract(violation) => Some(violation),
            Self::RegistryOperational(error) => Some(error),
            Self::Contract { violation, .. } => Some(violation),
            Self::IdentityExhausted => None,
        }
    }
}
