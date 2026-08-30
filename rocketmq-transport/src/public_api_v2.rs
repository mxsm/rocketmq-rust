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

//! Explicitly approved source API for the 2.x release line.
//!
//! This surface exposes the trusted mutable request aggregate, immutable
//! ingress facts, and owned V2 response plan approved for the 2.x request
//! model. It does not expose legacy channels, session handles, operation
//! contexts, raw cancellation authority, response bodies, or encoded frames.

pub use crate::admission::AdmissionController;
pub use crate::admission::AdmissionLimits;
pub use crate::clients::rocketmq_tokio_client::RemotingClientV2Builder;
pub use crate::clients::rocketmq_tokio_client::TransportClientV2Builder;
pub use crate::config::ServerConfig;
pub use crate::deadline::RequestDeadline;
pub use crate::dispatch::AuthenticationState;
pub use crate::dispatch::AuthorizedCommandDispatcherV2;
pub use crate::dispatch::ClaimedDeferred;
pub use crate::dispatch::DeferredAdmission;
pub use crate::dispatch::DeferredAdmissionAcquireError;
pub use crate::dispatch::DeferredAdmissionAcquireErrorKind;
pub use crate::dispatch::DeferredAdmissionConfigError;
pub use crate::dispatch::DeferredAdmissionConfigErrorKind;
pub use crate::dispatch::DeferredAdmissionSnapshot;
pub use crate::dispatch::DeferredCancellationReason;
pub use crate::dispatch::DeferredClaimError;
pub use crate::dispatch::DeferredClaimErrorKind;
pub use crate::dispatch::DeferredExpiry;
pub use crate::dispatch::DeferredExpiryBatch;
pub use crate::dispatch::DeferredExpiryBatchStats;
pub use crate::dispatch::DeferredExpiryError;
pub use crate::dispatch::DeferredExpiryErrorKind;
pub use crate::dispatch::DeferredExpiryKind;
pub use crate::dispatch::DeferredExpiryMargins;
pub use crate::dispatch::DeferredId;
pub use crate::dispatch::DeferredParts;
pub use crate::dispatch::DeferredRegistration;
pub use crate::dispatch::DeferredRegistry;
pub use crate::dispatch::DeferredRegistryError;
pub use crate::dispatch::DeferredRegistryErrorKind;
pub use crate::dispatch::DeferredRegistryShutdownOutcome;
pub use crate::dispatch::DeferredRegistryShutdownStats;
pub use crate::dispatch::DeferredRequest;
pub use crate::dispatch::DeferredResponder;
pub use crate::dispatch::DeferredResponseError;
pub use crate::dispatch::DeferredResponseErrorKind;
pub use crate::dispatch::DeferredResumeError;
pub use crate::dispatch::DeferredResumeErrorKind;
pub use crate::dispatch::DeferredResumeRetainedSize;
pub use crate::dispatch::DeferredRetainedSize;
pub use crate::dispatch::DeferredRetainedSizeParts;
pub use crate::dispatch::DeferredTerminalReason;
pub use crate::dispatch::DeferredWaitLimits;
pub use crate::dispatch::DeferredWaitPermit;
pub use crate::dispatch::DeferredWakeReason;
pub use crate::dispatch::EmbeddedCaller;
pub use crate::dispatch::EmbeddedDispatchError;
pub use crate::dispatch::EmbeddedDispatchErrorKind;
pub use crate::dispatch::EmbeddedDispatchOutcome;
pub use crate::dispatch::EmbeddedResponse;
pub use crate::dispatch::EmbeddedResponseBody;
pub use crate::dispatch::HandlerOutcome;
pub use crate::dispatch::IngressRequestView;
pub use crate::dispatch::OriginalRequestIdentity;
pub use crate::dispatch::ProtocolNoResponse;
pub use crate::dispatch::ProtocolNoResponseError;
pub use crate::dispatch::ProtocolNoResponseReason;
pub use crate::dispatch::RemotingRequest;
pub use crate::dispatch::RequestControlView;
pub use crate::dispatch::RequestId;
pub use crate::dispatch::RequestMeta;
pub use crate::dispatch::RequestOrigin;
pub use crate::dispatch::ResponseBodyKind;
pub use crate::dispatch::ResponseDisposition;
pub use crate::dispatch::ResponseErrorKind;
pub use crate::dispatch::ResponsePlan;
pub use crate::dispatch::ResponsePlanError;
pub use crate::dispatch::ResponseReceipt;
pub use crate::dispatch::ResponseTerminalState;
pub use crate::dispatch::TakeDeferredResponderError;
pub use crate::dispatch::WriteProgress;
pub use crate::file_region::FileRegion;
pub use crate::file_region::FileRegionSequence;
pub use crate::proxy_protocol::ProxyProtocolConfig;
pub use crate::remoting_server::rocketmq_tokio_server::TransportServerV2;
pub use crate::request_ordering::RequestOrdering;
pub use crate::request_ordering::RequestOrderingKey;
pub use crate::request_processor::default_request_processor::DefaultRequestProcessor;
pub use crate::runtime::processor_v2::LocalRequestProcessorV2;
pub use crate::runtime::processor_v2::RejectRequestDecision;
pub use crate::runtime::processor_v2::RequestProcessorV2;
pub use crate::runtime::processor_v2::ResponseMetadataV2;
pub use crate::runtime::processor_v2::ResponseObservationModeV2;
pub use crate::runtime::processor_v2::ResponseObservationOutcomeV2;
pub use crate::runtime::processor_v2::ResponseObservationV2;
pub use crate::runtime::processor_v2::ResponseWriteObservationV2;
pub use crate::runtime::processor_v2::ResponseWriteOutcomeV2;
pub use crate::runtime::processor_v2::ResponseWritePath;
pub use crate::security::TransportSecurity;
pub use crate::session_view::ProxyInfoSnapshot;
pub use crate::session_view::SessionId;
pub use crate::session_view::SessionStateView;
pub use crate::session_view::SessionView;
pub use crate::telemetry::TransportTelemetry;
pub use crate::v2_session_registry::ServerPushCommand;
pub use crate::v2_session_registry::ServerPushError;
pub use crate::v2_session_registry::ServerPushKind;
pub use crate::v2_session_registry::ServerPushReceipt;
pub use crate::v2_session_registry::ServerPushSender;
pub use crate::v2_session_registry::ServerRequestCommand;
pub use crate::v2_session_registry::ServerRequestError;
pub use crate::v2_session_registry::ServerRequestErrorStage;
pub use crate::v2_session_registry::ServerRequestKind;
pub use crate::v2_session_registry::ServerRequestResponse;
pub use crate::v2_session_registry::ServerRequestSender;
pub use crate::v2_session_registry::SessionCloseError;
pub use crate::v2_session_registry::SessionCloseHandle;
pub use crate::v2_session_registry::SessionCloseReason;
pub use crate::v2_session_registry::V2SessionEvent;
pub use crate::v2_session_registry::V2SessionLifecycleListener;
pub use crate::v2_session_registry::V2SessionRegistry;

/// Persistent endpoint client whose omitted processor parameter is V2-native.
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::TransportClient;
///
/// fn dynamic_registration_is_not_on_the_v2_default(client: &TransportClient) {
///     client.register_processor(());
/// }
/// ```
pub use crate::clients::rocketmq_tokio_client::V2TransportClient as TransportClient;

/// Nameserver-aware client whose omitted processor parameter is V2-native.
pub use crate::clients::rocketmq_tokio_client::V2RemotingClient as RemotingClient;
