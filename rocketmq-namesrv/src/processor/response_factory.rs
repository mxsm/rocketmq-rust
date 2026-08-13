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

use rocketmq_error::ProtocolError;
use rocketmq_error::RocketMQError;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;

pub(crate) trait NameServerResponseFactoryExt {
    fn command_from_error_with_remark(&self, error: &RocketMQError, remark: impl Into<String>) -> RemotingCommand;

    fn command_from_error_with_remark_and_opaque(
        &self,
        error: &RocketMQError,
        remark: impl Into<String>,
        opaque: i32,
    ) -> RemotingCommand;

    fn request_code_not_supported_with_remark(&self, request_code: i32, remark: impl Into<String>) -> RemotingCommand;

    fn request_code_not_supported_with_opaque(&self, request_code: i32, opaque: i32) -> RemotingCommand;

    fn invalid_parameter_with_remark(&self, remark: impl Into<String>) -> RemotingCommand;

    fn no_permission_with_remark(&self, remark: impl Into<String>) -> RemotingCommand;

    fn query_not_found_with_remark(&self, remark: impl Into<String>) -> RemotingCommand;

    fn internal_error(&self, remark: impl Into<String>) -> RemotingCommand;
}

impl NameServerResponseFactoryExt for RemotingCommandFactory {
    fn command_from_error_with_remark(&self, error: &RocketMQError, remark: impl Into<String>) -> RemotingCommand {
        self.create_response_command_from_error_with_remark(error, remark.into())
    }

    fn command_from_error_with_remark_and_opaque(
        &self,
        error: &RocketMQError,
        remark: impl Into<String>,
        opaque: i32,
    ) -> RemotingCommand {
        self.command_from_error_with_remark(error, remark).set_opaque(opaque)
    }

    fn request_code_not_supported_with_remark(&self, request_code: i32, remark: impl Into<String>) -> RemotingCommand {
        let error = RocketMQError::Protocol(ProtocolError::invalid_command(request_code));
        self.command_from_error_with_remark(&error, remark)
    }

    fn request_code_not_supported_with_opaque(&self, request_code: i32, opaque: i32) -> RemotingCommand {
        self.request_code_not_supported_with_remark(
            request_code,
            format!("The request code {request_code} is not supported."),
        )
        .set_opaque(opaque)
    }

    fn invalid_parameter_with_remark(&self, remark: impl Into<String>) -> RemotingCommand {
        let remark = remark.into();
        let error = RocketMQError::illegal_argument(remark.clone());
        self.command_from_error_with_remark(&error, remark)
    }

    fn no_permission_with_remark(&self, remark: impl Into<String>) -> RemotingCommand {
        let remark = remark.into();
        let error = RocketMQError::BrokerPermissionDenied {
            operation: remark.clone(),
        };
        self.command_from_error_with_remark(&error, remark)
    }

    fn query_not_found_with_remark(&self, remark: impl Into<String>) -> RemotingCommand {
        let remark = remark.into();
        let error = RocketMQError::query_not_found(remark.clone());
        self.command_from_error_with_remark(&error, remark)
    }

    fn internal_error(&self, remark: impl Into<String>) -> RemotingCommand {
        let remark = remark.into();
        let error = RocketMQError::invariant_violated("legacy remoting handler returned an internal response");
        self.command_from_error_with_remark(&error, remark)
    }
}
