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

use std::any::Any;

use bytes::Bytes;
use bytes::BytesMut;

use crate::protocol::command_custom_header::CommandCustomHeader;
use crate::protocol::header::message_operation_header::TopicRequestHeaderTrait;
use crate::protocol::remoting_command::RemotingCommand;
use crate::protocol::RemotingSerializable;
use crate::rpc::rpc_request::RpcRequest;
use crate::rpc::rpc_response::RpcResponse;

pub struct RpcClientUtils;

impl RpcClientUtils {
    pub fn create_command_for_rpc_request<H: CommandCustomHeader + TopicRequestHeaderTrait>(
        rpc_request: RpcRequest<H>,
    ) -> RemotingCommand {
        let result = RemotingCommand::create_request_command(rpc_request.code, rpc_request.header);
        if let Some(body) = rpc_request.body {
            if let Some(body) = Self::encode_body(&*body) {
                return result.set_body(body);
            }
        }
        result
    }

    pub fn create_command_for_rpc_response(mut rpc_response: RpcResponse) -> RemotingCommand {
        let mut cmd = match rpc_response.header.take() {
            None => RemotingCommand::create_response_command_with_code(rpc_response.code),
            Some(value) => RemotingCommand::create_response_command().set_command_custom_header_origin(Some(value)),
        };
        match rpc_response.exception {
            None => {}
            Some(value) => cmd.set_remark_mut(value.to_string()),
        }
        if let Some(ref _body) = rpc_response.body {
            return cmd;
        }
        cmd
    }

    pub fn encode_body(body: &dyn Any) -> Option<Bytes> {
        if body.is::<()>() {
            None
        } else if let Some(bytes) = body.downcast_ref::<Bytes>() {
            Some(bytes.clone())
        } else if let Some(remoting_serializable) = body.downcast_ref::<&dyn RemotingSerializable>() {
            Some(Bytes::from(remoting_serializable.encode().expect("encode failed")))
        } else if let Some(buffer) = body.downcast_ref::<BytesMut>() {
            let data = buffer.clone().freeze();
            Some(data)
        } else {
            None
        }
    }
}
