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
use rocketmq_error::RocketMQResult;

use crate::protocol::command_custom_header::CommandCustomHeader;
use crate::protocol::header::message_operation_header::TopicRequestHeaderTrait;
use crate::protocol::remoting_command::RemotingCommand;
use crate::protocol::RemotingSerializable;
use crate::rpc::rpc_request::RpcRequest;
use crate::rpc::rpc_response::RpcResponse;

pub struct RpcClientUtils;

impl RpcClientUtils {
    pub fn try_create_command_for_rpc_request<H: CommandCustomHeader + TopicRequestHeaderTrait>(
        rpc_request: RpcRequest<H>,
    ) -> RocketMQResult<RemotingCommand> {
        let result = RemotingCommand::create_request_command(rpc_request.code, rpc_request.header);
        if let Some(body) = rpc_request.body {
            if let Some(body) = Self::try_encode_body(&*body)? {
                return Ok(result.set_body(body));
            }
        }
        Ok(result)
    }

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
        Self::try_encode_body(body).ok().flatten()
    }

    pub fn try_encode_body(body: &dyn Any) -> RocketMQResult<Option<Bytes>> {
        if body.is::<()>() {
            Ok(None)
        } else if let Some(bytes) = body.downcast_ref::<Bytes>() {
            Ok(Some(bytes.clone()))
        } else if let Some(remoting_serializable) = body.downcast_ref::<&dyn RemotingSerializable>() {
            remoting_serializable.encode().map(Bytes::from).map(Some)
        } else if let Some(buffer) = body.downcast_ref::<BytesMut>() {
            let data = buffer.clone().freeze();
            Ok(Some(data))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_error::RocketMQError;

    use super::*;

    struct FailingSerializable;

    impl RemotingSerializable for FailingSerializable {
        fn encode(&self) -> RocketMQResult<Vec<u8>> {
            Err(RocketMQError::response_process_failed(
                "encode remoting body",
                "forced encode failure",
            ))
        }

        fn serialize_json(&self) -> RocketMQResult<String> {
            Err(RocketMQError::response_process_failed(
                "serialize remoting body",
                "forced json failure",
            ))
        }

        fn serialize_json_pretty(&self) -> RocketMQResult<String> {
            Err(RocketMQError::response_process_failed(
                "serialize remoting body pretty",
                "forced pretty json failure",
            ))
        }
    }

    #[test]
    fn try_encode_body_returns_serializable_error_without_panicking() {
        static FAILING_SERIALIZABLE: FailingSerializable = FailingSerializable;
        let body: &'static dyn RemotingSerializable = &FAILING_SERIALIZABLE;

        let error =
            RpcClientUtils::try_encode_body(&body).expect_err("serializable body encoding failure should be returned");

        assert!(error.to_string().contains("forced encode failure"));
    }

    #[test]
    fn encode_body_keeps_legacy_none_on_serializable_error_without_panicking() {
        static FAILING_SERIALIZABLE: FailingSerializable = FailingSerializable;
        let body: &'static dyn RemotingSerializable = &FAILING_SERIALIZABLE;

        assert!(RpcClientUtils::encode_body(&body).is_none());
    }

    #[test]
    fn try_encode_body_preserves_bytes_zero_copy_path() {
        let bytes = Bytes::from_static(b"payload");

        assert_eq!(RpcClientUtils::try_encode_body(&bytes).unwrap(), Some(bytes));
    }
}
