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
mod remoting_request;
mod request_context;
mod request_control;
mod request_identity;
mod request_origin;
mod response;
mod response_plan;
mod response_sink;

pub use authorized_dispatcher::AuthorizedCommandDispatcher;
pub use authorized_dispatcher::AuthorizedDispatchBoundary;
pub use authorized_dispatcher::DispatchError;
pub use authorized_dispatcher::DispatchOutcome;
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
