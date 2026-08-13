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

use crate::rpc::rpc_request::RpcRequest;
use crate::rpc::rpc_response::RpcResponse;
use rocketmq_protocol::protocol::command_custom_header::CommandCustomHeader;
use rocketmq_protocol::protocol::header::message_operation_header::TopicRequestHeaderTrait;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::remoting_command_defaults::application_remoting_command_factory;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;
use rocketmq_protocol::protocol::RemotingSerializable;

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

    pub fn create_command_for_rpc_response(rpc_response: RpcResponse) -> RemotingCommand {
        Self::create_command_for_rpc_response_with_factory(&application_remoting_command_factory(), rpc_response)
    }

    /// Converts an RPC response using the caller-owned remoting defaults.
    ///
    /// [`Self::create_command_for_rpc_response`] is the compatibility facade that uses application defaults.
    pub fn create_command_for_rpc_response_with_factory(
        command_factory: &RemotingCommandFactory,
        mut rpc_response: RpcResponse,
    ) -> RemotingCommand {
        let mut cmd = match rpc_response.header.take() {
            None => command_factory.create_response_command_with_code(rpc_response.code),
            Some(value) => command_factory
                .create_response_command_with_code(rpc_response.code)
                .set_command_custom_header_boxed(value),
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
    use cheetah_string::CheetahString;
    use rocketmq_error::RocketMQError;
    use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandDefaults;
    use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;
    use rocketmq_protocol::protocol::SerializeType;

    use super::*;
    use rocketmq_protocol::protocol::header::client_request_header::GetRouteInfoRequestHeader;

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

    #[test]
    fn rpc_response_command_preserves_owned_boxed_header() {
        let response = RpcResponse::new(
            17,
            Box::new(GetRouteInfoRequestHeader::new("owned-topic", Some(true))),
            None,
        );

        let mut command = RpcClientUtils::create_command_for_rpc_response(response);
        assert_eq!(
            command
                .read_custom_header_ref::<GetRouteInfoRequestHeader>()
                .expect("owned response header should remain attached")
                .topic,
            CheetahString::from_static_str("owned-topic")
        );

        command.make_custom_header_to_net();
        assert_eq!(
            command
                .decode_command_custom_header::<GetRouteInfoRequestHeader>()
                .expect("materialized response header should decode")
                .topic,
            CheetahString::from_static_str("owned-topic")
        );
    }

    #[test]
    fn rpc_response_with_header_preserves_response_code() {
        let response = RpcResponse::new(
            17,
            Box::new(GetRouteInfoRequestHeader::new("owned-topic", Some(true))),
            None,
        );

        let command = RpcClientUtils::create_command_for_rpc_response(response);

        assert_eq!(command.code(), 17);
        assert!(command.is_response_type());
        let header = command
            .read_custom_header_ref::<GetRouteInfoRequestHeader>()
            .expect("RPC response should retain its typed header");
        assert_eq!(header.topic.as_str(), "owned-topic");
        assert_eq!(header.accept_standard_json_only, Some(true));
    }

    #[test]
    fn factory_aware_rpc_response_preserves_fields_and_owner_defaults() {
        let factory = RemotingCommandFactory::new(RemotingCommandDefaults::new(9343, SerializeType::ROCKETMQ));
        let exception = RocketMQError::response_process_failed("forward RPC response", "remote failure");
        let expected_remark = exception.to_string();
        let mut response = RpcResponse::new(
            17,
            Box::new(GetRouteInfoRequestHeader::new("owned-topic", Some(true))),
            None,
        );
        response.exception = Some(exception);

        let command = RpcClientUtils::create_command_for_rpc_response_with_factory(&factory, response);

        assert_eq!(command.code(), 17);
        assert!(command.is_response_type());
        assert_eq!(command.version(), 9343);
        assert_eq!(command.serialize_type(), SerializeType::ROCKETMQ);
        assert_eq!(
            command.remark().map(|remark| remark.as_str()),
            Some(expected_remark.as_str())
        );
        let header = command
            .read_custom_header_ref::<GetRouteInfoRequestHeader>()
            .expect("RPC response should retain its typed header");
        assert_eq!(header.topic.as_str(), "owned-topic");
        assert_eq!(header.accept_standard_json_only, Some(true));
    }

    #[test]
    fn factory_aware_rpc_response_matches_legacy_non_empty_body_behavior() {
        let body = Bytes::from_static(b"payload");
        let legacy_command =
            RpcClientUtils::create_command_for_rpc_response(RpcResponse::new_option(17, Some(Box::new(body.clone()))));
        let factory = RemotingCommandFactory::new(RemotingCommandDefaults::new(9343, SerializeType::ROCKETMQ));
        let factory_command = RpcClientUtils::create_command_for_rpc_response_with_factory(
            &factory,
            RpcResponse::new_option(17, Some(Box::new(body))),
        );

        assert_eq!(factory_command.body(), legacy_command.body());
        assert!(legacy_command.body().is_none());
    }
}
