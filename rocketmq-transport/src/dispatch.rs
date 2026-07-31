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
mod request_context;
mod response_sink;

pub use authorized_dispatcher::AuthorizedCommandDispatcher;
pub use authorized_dispatcher::AuthorizedDispatchBoundary;
pub use authorized_dispatcher::DispatchError;
pub use authorized_dispatcher::DispatchOutcome;
pub use request_context::RequestContext;
pub use request_context::RequestContextError;
pub use request_context::RequestTransport;
pub use response_sink::LocalResponseReceiver;
#[doc(hidden)]
pub use response_sink::LocalResponseSink;
pub use response_sink::ResponseSink;
pub use response_sink::ResponseSinkError;
