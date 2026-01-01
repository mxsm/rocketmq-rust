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

use crate::net::channel::Channel;
use crate::protocol::remoting_command::RemotingCommand;
use crate::runtime::connection_handler_context::ConnectionHandlerContext;

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
}
