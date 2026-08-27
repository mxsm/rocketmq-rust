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
mod embedded_dispatch;
mod handler_outcome;
mod legacy_processor_adapter;
mod remoting_request;
mod request_context;
mod request_control;
mod request_identity;
mod request_origin;
mod response;
mod response_plan;
mod response_sink;

pub use authorized_dispatcher::AuthorizedCommandDispatcher;
pub use authorized_dispatcher::AuthorizedCommandDispatcherV2;
pub use authorized_dispatcher::AuthorizedDispatchBoundary;
pub(crate) use authorized_dispatcher::AuthorizedDispatchSession;
#[allow(
    unused_imports,
    reason = "DSP-03 exposes the private V2 dispatcher failure to later coexistence routing"
)]
pub(crate) use authorized_dispatcher::AuthorizedDispatchV2Error;
pub use authorized_dispatcher::DispatchError;
pub use authorized_dispatcher::DispatchOutcome;
pub use embedded_dispatch::EmbeddedDispatchError;
pub use embedded_dispatch::EmbeddedDispatchErrorKind;
pub use embedded_dispatch::EmbeddedDispatchOutcome;
pub use handler_outcome::DeferredRegistration;
pub use handler_outcome::HandlerOutcome;
#[allow(
    unused_imports,
    reason = "DSP-02 exposes the private inline handler contract to later dispatcher wiring"
)]
pub(crate) use handler_outcome::HandlerOutcomeContractError;
pub(crate) use handler_outcome::InlineResponseSlot;
pub use handler_outcome::ProtocolNoResponse;
pub use handler_outcome::ProtocolNoResponseError;
pub use handler_outcome::ProtocolNoResponseReason;
#[cfg(test)]
pub(crate) use legacy_processor_adapter::bridge_construction_counts;
pub(crate) use legacy_processor_adapter::DispatchMetricsGuard;
pub(crate) use legacy_processor_adapter::DispatchProcessor;
pub(crate) use legacy_processor_adapter::DispatchProcessorError;
pub(crate) use legacy_processor_adapter::EmbeddedProcessorResolveError;
pub(crate) use legacy_processor_adapter::EmbeddedResolvedOutcome;
pub(crate) use legacy_processor_adapter::ExplicitV2Processor;
pub(crate) use legacy_processor_adapter::InternalFailureOrigin;
pub(crate) use legacy_processor_adapter::InternalProcessorCandidate;
pub(crate) use legacy_processor_adapter::InternalProcessorOutcome;
pub(crate) use legacy_processor_adapter::LegacyNetworkSession;
pub(crate) use legacy_processor_adapter::LegacyProcessorAdapter;
pub(crate) use legacy_processor_adapter::LegacyProcessorAdapterError;
pub(crate) use legacy_processor_adapter::LegacyReplyCandidate;
#[allow(
    unused_imports,
    reason = "DSP-05 exposes the sealed legacy bridge to later coexistence routing"
)]
pub(crate) use legacy_processor_adapter::LegacyRequestBridge;
pub use remoting_request::IngressRequestView;
pub use remoting_request::RemotingRequest;
pub use request_context::RequestContext;
pub use request_context::RequestContextError;
pub use request_context::RequestTransport;
pub use request_control::RequestControlView;
pub use request_control::RequestMeta;
pub(crate) use request_identity::reserve_session_owner;
pub use request_identity::OriginalRequestIdentity;
pub use request_origin::AuthenticationState;
pub use request_origin::EmbeddedCaller;
pub use request_origin::RequestOrigin;
pub use response::RequestId;
pub use response::ResponseDisposition;
pub use response::ResponseError;
pub use response::ResponseErrorKind;
pub use response::ResponseReceipt;
pub use response::ResponseTerminalState;
pub use response::WriteProgress;
#[allow(
    unused_imports,
    reason = "later private response stages name the RSP-03 binding capability through dispatch"
)]
pub(crate) use response_plan::BoundResponsePlan;
#[allow(
    unused_imports,
    reason = "RSP-06 exposes the private legacy materialization failure to later embedded wiring"
)]
pub(crate) use response_plan::LegacyLocalMaterializationError;
#[allow(
    unused_imports,
    reason = "RSP-06 exposes the private bounded legacy materialization profile to later embedded wiring"
)]
pub(crate) use response_plan::LegacyMaterializationLimits;
#[allow(
    unused_imports,
    reason = "later private response stages handle the RSP-03 binding failure through dispatch"
)]
pub(crate) use response_plan::ResponseBindingError;
#[allow(
    unused_imports,
    reason = "later private response encoders recover the RSP-03 body owner through dispatch"
)]
pub(crate) use response_plan::ResponseBody;
pub use response_plan::ResponseBodyKind;
pub use response_plan::ResponsePlan;
pub use response_plan::ResponsePlanError;
#[allow(
    unused_imports,
    reason = "RSP-05 exposes the private local handoff seam consumed by RSP-06"
)]
pub(crate) use response_sink::LocalResponsePlanReceiver;
pub use response_sink::LocalResponseReceiver;
pub(crate) use response_sink::NetworkResponsePlanContext;
pub use response_sink::ResponseSink;
pub use response_sink::ResponseSinkError;
pub(crate) use response_sink::ResponseTransportDropHandle;
