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

mod authorized_dispatcher;
mod deferred_admission;
mod deferred_expiry;
mod deferred_registry;
mod deferred_responder;
mod deferred_response;
pub(crate) mod deferred_resume;
mod deferred_session_cleanup;
mod embedded_dispatch;
mod handler_outcome;
mod processor_adapter;
mod remoting_request;
mod remoting_response;
mod request_context;
mod request_control;
mod request_identity;
mod request_origin;
mod response;
mod response_sink;

pub use authorized_dispatcher::AuthorizedCommandDispatcher;
pub(crate) use authorized_dispatcher::AuthorizedDispatchBoundary;
pub(crate) use authorized_dispatcher::AuthorizedDispatchSession;
pub use deferred_admission::DeferredAdmission;
pub use deferred_admission::DeferredAdmissionAcquireOutcome;
pub use deferred_admission::DeferredAdmissionSnapshot;
pub use deferred_admission::DeferredRetainedSize;
pub use deferred_admission::DeferredRetainedSizeParts;
pub use deferred_admission::DeferredWaitLimits;
pub use deferred_admission::DeferredWaitPermit;
pub use deferred_expiry::DeferredExpiry;
pub use deferred_expiry::DeferredExpiryKind;
pub use deferred_expiry::DeferredExpiryMargins;
pub use deferred_expiry::DeferredExpiryOutcome;
pub use deferred_registry::ClaimedDeferred;
pub use deferred_registry::DeferredClaimOutcome;
pub(crate) use deferred_registry::DeferredCommitError;
pub(crate) use deferred_registry::DeferredCommitErrorKind;
pub use deferred_registry::DeferredExpiryBatch;
pub use deferred_registry::DeferredExpiryBatchStats;
pub use deferred_registry::DeferredId;
pub use deferred_registry::DeferredParts;
pub use deferred_registry::DeferredRegistration;
pub use deferred_registry::DeferredRegistry;
pub use deferred_registry::DeferredRegistryOutcome;
pub use deferred_registry::DeferredRegistryRecovery;
pub use deferred_registry::DeferredRegistryShutdownOutcome;
pub use deferred_registry::DeferredRegistryShutdownStats;
pub use deferred_registry::DeferredRequest;
pub use deferred_registry::DeferredResumeOutcome;
pub use deferred_registry::DeferredResumeRetainedSize;
pub use deferred_registry::DeferredResumeSubmitOutcome;
pub use deferred_registry::DeferredWakeReason;
pub(in crate::dispatch) use deferred_registry::RegistryFailure;
pub use deferred_responder::DeferredCancellationReason;
pub use deferred_responder::DeferredResponder;
pub use deferred_responder::DeferredResponderOutcome;
pub use deferred_responder::DeferredResponseOutcome;
pub(crate) use deferred_responder::DeferredResponseSeed;
pub use deferred_response::DeferredTerminalReason;
pub(crate) use deferred_response::DeferredTransportDropHandle;
pub(crate) use deferred_response::ResponseSendClaim;
pub(crate) use deferred_response::ResponseState;
pub(crate) use deferred_response::ResponseStateOutcome;
pub(crate) use deferred_response::ResponseStateSnapshot;
pub(crate) use deferred_session_cleanup::DeferredSessionCleanupOwner;
pub(crate) use deferred_session_cleanup::DeferredSessionCleanupRegistration;
pub(crate) use deferred_session_cleanup::DeferredSessionCleanupReport;
#[cfg(test)]
pub(crate) use deferred_session_cleanup::SessionCleanupCapability;
#[cfg(test)]
pub(crate) use deferred_session_cleanup::SessionCleanupEnrollment;
pub(crate) use embedded_dispatch::EmbeddedDispatchError;
pub use embedded_dispatch::EmbeddedDispatchOutcome;
pub use handler_outcome::HandlerOutcome;
pub(crate) use handler_outcome::InlineResponseSlot;
pub use handler_outcome::ProtocolNoResponse;
pub use handler_outcome::ProtocolNoResponseReason;
pub(crate) use processor_adapter::DispatchMetricsGuard;
pub(crate) use processor_adapter::DispatchProcessor;
pub(crate) use processor_adapter::DispatchProcessorError;
pub(crate) use processor_adapter::EmbeddedResolvedOutcome;
pub(crate) use processor_adapter::ExplicitProcessor;
pub(crate) use processor_adapter::InternalFailureOrigin;
pub(crate) use processor_adapter::InternalProcessorCandidate;
pub(crate) use processor_adapter::InternalProcessorOutcome;
pub(crate) use processor_adapter::NetworkSession;
pub use remoting_request::IngressRequestView;
pub use remoting_request::RemotingRequest;
pub(crate) use remoting_response::BoundResponse;
pub use remoting_response::EmbeddedResponse;
pub use remoting_response::EmbeddedResponseBody;
pub use remoting_response::RemotingResponse;
pub(crate) use remoting_response::ResponseBody;
pub use remoting_response::ResponseBodyKind;
pub(crate) use request_context::RequestContext;
pub(crate) use request_context::RequestTransport;
pub use request_control::RequestControlView;
pub use request_control::RequestMeta;
pub(crate) use request_identity::reserve_session_owner;
pub use request_identity::OriginalRequestIdentity;
pub use request_origin::AuthenticationState;
pub use request_origin::EmbeddedCaller;
pub use request_origin::RequestOrigin;
pub use response::RequestId;
pub use response::ResponseCompletionOutcome;
pub use response::ResponseDisposition;
pub(crate) use response::ResponseOperationalFailure;
pub use response::ResponseReceipt;
pub(crate) use response::ResponseSendOutcome;
pub use response::ResponseTerminalState;
pub use response::WriteProgress;
pub(crate) use response_sink::ResponseDeliveryContext;
pub(crate) use response_sink::ResponseSink;
pub(crate) use response_sink::ResponseTransportDropHandle;
