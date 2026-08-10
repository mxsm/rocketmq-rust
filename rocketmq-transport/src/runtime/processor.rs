// Copyright 2023 The RocketMQ Rust Authors
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

use std::time::Duration;

use crate::net::channel::Channel;
use crate::request_ordering::RequestOrdering;
use crate::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;

pub type RejectRequestResponse = (bool, Option<RemotingCommand>);

/// Result of dispatching a response to the transport channel.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResponseWriteOutcome {
    /// The channel accepted the response for delivery.
    Sent,
    /// Encoding or channel delivery failed.
    Failed,
}

/// Low-cost response write observation exposed to request processors.
///
/// `end_to_end_elapsed` starts at transport request dispatch and includes RPC
/// hooks, processor execution, and channel write. It does not measure peer
/// acknowledgement or application consumption.
#[derive(Clone, Copy, Debug)]
pub struct ResponseWriteObservation {
    /// Original request code.
    pub request_code: i32,
    /// Response code sent or attempted.
    pub response_code: i32,
    /// Time spent awaiting the transport channel write.
    pub write_elapsed: Duration,
    /// Time from request dispatch through channel write completion.
    pub end_to_end_elapsed: Duration,
    /// Channel write outcome.
    pub outcome: ResponseWriteOutcome,
}

/// Trait for processing requests.
#[trait_variant::make(RequestProcessor: Send )]
pub trait LocalRequestProcessor {
    /// Process a request.
    async fn process_request(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>>;

    fn reject_request(&self, _code: i32) -> RejectRequestResponse {
        (false, None)
    }

    /// Declares the per-session execution ordering required by this request.
    ///
    /// Requests are concurrent by default. Implementations should return an
    /// ordered key only when their domain contract requires arrival ordering.
    fn request_ordering(&self, _request: &RemotingCommand) -> RequestOrdering {
        RequestOrdering::Concurrent
    }

    /// Observes completion of response dispatch to the transport channel.
    ///
    /// The default is intentionally a no-op. This is a write-boundary signal,
    /// not acknowledgement that the peer consumed the response bytes.
    fn observe_response_write(&self, _observation: ResponseWriteObservation) {}
}
