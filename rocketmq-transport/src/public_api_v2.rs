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

pub use crate::deadline::RequestDeadline;
pub use crate::dispatch::AuthenticationState;
pub use crate::dispatch::AuthorizedCommandDispatcherV2;
pub use crate::dispatch::DeferredAdmission;
pub use crate::dispatch::DeferredAdmissionAcquireError;
pub use crate::dispatch::DeferredAdmissionAcquireErrorKind;
pub use crate::dispatch::DeferredAdmissionConfigError;
pub use crate::dispatch::DeferredAdmissionConfigErrorKind;
pub use crate::dispatch::DeferredAdmissionSnapshot;
pub use crate::dispatch::DeferredRegistration;
pub use crate::dispatch::DeferredResponder;
pub use crate::dispatch::DeferredResponseError;
pub use crate::dispatch::DeferredResponseErrorKind;
pub use crate::dispatch::DeferredRetainedSize;
pub use crate::dispatch::DeferredRetainedSizeParts;
pub use crate::dispatch::DeferredWaitLimits;
pub use crate::dispatch::DeferredWaitPermit;
pub use crate::dispatch::EmbeddedCaller;
pub use crate::dispatch::EmbeddedDispatchError;
pub use crate::dispatch::EmbeddedDispatchErrorKind;
pub use crate::dispatch::EmbeddedDispatchOutcome;
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
pub use crate::remoting_server::rocketmq_tokio_server::TransportServerV2;
pub use crate::request_ordering::RequestOrdering;
pub use crate::request_ordering::RequestOrderingKey;
pub use crate::runtime::processor_v2::LocalRequestProcessorV2;
pub use crate::runtime::processor_v2::RejectRequestDecision;
pub use crate::runtime::processor_v2::RequestProcessorV2;
pub use crate::runtime::processor_v2::ResponseWriteObservationV2;
pub use crate::runtime::processor_v2::ResponseWriteOutcomeV2;
pub use crate::runtime::processor_v2::ResponseWritePath;
pub use crate::session_view::ProxyInfoSnapshot;
pub use crate::session_view::SessionId;
pub use crate::session_view::SessionStateView;
pub use crate::session_view::SessionView;
