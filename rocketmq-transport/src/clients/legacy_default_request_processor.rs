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

//! Explicit V1 placeholder retained only for source-compatible transport clients and servers.

use rocketmq_protocol::protocol::remoting_command::RemotingCommand;

use crate::net::channel::Channel;
use crate::runtime::connection_handler_context::ConnectionHandlerContext;
use crate::runtime::processor::RequestProcessor;

/// Legacy no-op processor used by the V1 transport facade.
///
/// The V2 [`crate::request_processor::default_request_processor::DefaultRequestProcessor`]
/// is intentionally a different type and never receives a channel or mutable connection context.
#[derive(Clone)]
pub struct LegacyDefaultRequestProcessor;

impl RequestProcessor for LegacyDefaultRequestProcessor {
    async fn process_request(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        Ok(Some(RemotingCommand::create_response_command_with_code(request.code())))
    }
}
