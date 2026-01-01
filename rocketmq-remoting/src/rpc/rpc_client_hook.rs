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

use crate::protocol::command_custom_header::CommandCustomHeader;
use crate::protocol::header::message_operation_header::TopicRequestHeaderTrait;
use crate::rpc::rpc_request::RpcRequest;
use crate::rpc::rpc_response::RpcResponse;

pub trait RpcClientHook {
    fn before_request<H: CommandCustomHeader + TopicRequestHeaderTrait>(
        &self,
        rpc_request: &RpcRequest<H>,
    ) -> rocketmq_error::RocketMQResult<Option<RpcResponse>>;
    fn after_response(&self, rpc_response: &RpcResponse) -> rocketmq_error::RocketMQResult<Option<RpcResponse>>;
}

pub type RpcClientHookFn = Arc<
    dyn Fn(
            Option<&dyn TopicRequestHeaderTrait>,
            Option<&RpcResponse>,
        ) -> rocketmq_error::RocketMQResult<Option<RpcResponse>>
        + Send
        + Sync,
>;
