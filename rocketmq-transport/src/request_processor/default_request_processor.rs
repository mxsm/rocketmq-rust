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

use crate::dispatch::HandlerOutcome;
use crate::dispatch::RemotingRequest;
use crate::dispatch::RemotingResponse;
use crate::runtime::processor::RequestProcessor;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;

#[derive(Clone)]
pub struct DefaultRequestProcessor;

impl RequestProcessor for DefaultRequestProcessor {
    #[inline]
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let response = RemotingCommand::create_response_command_with_code(request.command().code());
        let response = RemotingResponse::command(response).map_err(|error| {
            rocketmq_error::RocketMQError::response_process_failed(
                "default_request_processor.remoting_response",
                error.to_string(),
            )
        })?;
        Ok(HandlerOutcome::Reply(response))
    }
}
