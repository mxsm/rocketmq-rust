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

use std::sync::Arc;

use crate::net::channel::Channel;
use crate::request_ordering::RequestOrdering;
use crate::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;

pub type RejectRequestResponse = (bool, Option<RemotingCommand>);

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
}

/// Adapts the session-oriented compatibility processor to the canonical
/// remoting processor contract.
///
/// Install this adapter in [`crate::AuthorizedCommandDispatcher`] so requests
/// retain the shared authorization, admission, deadline, and error-projection
/// boundary. The compatibility request is cloned because remoting hooks retain
/// access to the original command after processor execution.
pub struct SessionRequestProcessorAdapter<P: ?Sized> {
    processor: Arc<P>,
}

impl<P: ?Sized> Clone for SessionRequestProcessorAdapter<P> {
    fn clone(&self) -> Self {
        Self {
            processor: Arc::clone(&self.processor),
        }
    }
}

impl<P> SessionRequestProcessorAdapter<P> {
    /// Wraps one session-oriented processor.
    #[must_use]
    pub fn new(processor: P) -> Self {
        Self {
            processor: Arc::new(processor),
        }
    }
}

impl<P: ?Sized> SessionRequestProcessorAdapter<P> {
    /// Wraps an already shared session-oriented processor, including a trait object.
    #[must_use]
    pub fn from_shared(processor: Arc<P>) -> Self {
        Self { processor }
    }
}

impl<P> RequestProcessor for SessionRequestProcessorAdapter<P>
where
    P: crate::server::RequestProcessor + ?Sized,
{
    async fn process_request(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        self.processor.process(request.clone()).await.map(Some)
    }

    fn request_ordering(&self, request: &RemotingCommand) -> RequestOrdering {
        self.processor.request_ordering(request)
    }
}
