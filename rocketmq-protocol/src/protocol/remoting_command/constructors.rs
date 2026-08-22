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

use bytes::Bytes;
use cheetah_string::CheetahString;

use super::next_request_id;
use super::ExtensionFields;
use super::RemotingCommand;
use super::SerializeType;
use crate::protocol::command_custom_header::CommandCustomHeader;
use crate::protocol::header_codec::BinaryHeaderFields;
use crate::protocol::remoting_command_defaults::application_remoting_command_factory;
use crate::protocol::remoting_command_defaults::RemotingCommandDefaults;
use crate::protocol::LanguageCode;

impl RemotingCommand {
    /// Constructs a command from defaults resolved by the owning facade.
    ///
    /// The protocol crate deliberately does not read process environment or configuration files.
    /// Legacy facades resolve those sources and pass the resulting wire values here.
    pub fn with_resolved_defaults(version: i32, serialize_type: SerializeType) -> Self {
        let opaque = next_request_id();
        RemotingCommand {
            code: 0,
            language: LanguageCode::RUST, // Replace with your actual enum variant
            version,
            opaque,
            flag: 0,
            remark: None,
            ext_fields: ExtensionFields::default(),
            body: None,
            suspended: false,
            command_custom_header: None,
            custom_header_to_net: false,
            serialize_type,
        }
    }

    pub(crate) fn from_binary_wire_parts(
        code: i32,
        language: LanguageCode,
        version: i32,
        opaque: i32,
        flag: i32,
        remark: Option<CheetahString>,
        ext_fields: BinaryHeaderFields,
    ) -> Self {
        Self {
            code,
            language,
            version,
            opaque,
            flag,
            remark,
            ext_fields: ExtensionFields::from_rocketmq_raw(ext_fields),
            body: None,
            suspended: false,
            command_custom_header: None,
            custom_header_to_net: false,
            serialize_type: SerializeType::ROCKETMQ,
        }
    }

    pub fn new_request(code: impl Into<i32>, body: impl Into<Bytes>) -> Self {
        application_remoting_command_factory().create_request(code, body)
    }

    pub fn create_request_command<T>(code: impl Into<i32>, header: T) -> Self
    where
        T: CommandCustomHeader + Sync + Send + 'static,
    {
        application_remoting_command_factory().create_request_command(code, header)
    }

    /// Creates a request using defaults resolved by the transport/facade owner.
    pub fn create_request_command_with_defaults<T>(
        code: impl Into<i32>,
        header: T,
        version: i32,
        serialize_type: SerializeType,
    ) -> Self
    where
        T: CommandCustomHeader + Sync + Send + 'static,
    {
        crate::protocol::remoting_command_defaults::RemotingCommandFactory::new(RemotingCommandDefaults::new(
            version,
            serialize_type,
        ))
        .create_request_command(code, header)
    }

    pub fn create_remoting_command(code: impl Into<i32>) -> Self {
        application_remoting_command_factory().create_remoting_command(code)
    }

    pub fn get_and_add() -> i32 {
        next_request_id()
    }

    pub fn create_response_command_with_code(code: impl Into<i32>) -> Self {
        application_remoting_command_factory().create_response_command_with_code(code)
    }

    /// Creates a response with an explicit code and typed custom header.
    pub fn create_response_command_with_code_and_header(
        code: impl Into<i32>,
        header: impl CommandCustomHeader + Sync + Send + 'static,
    ) -> Self {
        application_remoting_command_factory().create_response_command_with_code_and_header(code, header)
    }

    pub fn create_response_command_with_code_remark(code: impl Into<i32>, remark: impl Into<CheetahString>) -> Self {
        application_remoting_command_factory().create_response_command_with_code_remark(code, remark)
    }

    /// Creates an explicitly successful response.
    pub fn create_success_response_command() -> Self {
        application_remoting_command_factory().create_success_response_command()
    }

    /// Creates an explicitly successful response with a typed custom header.
    pub fn create_success_response_command_with_header(
        header: impl CommandCustomHeader + Sync + Send + 'static,
    ) -> Self {
        application_remoting_command_factory().create_success_response_command_with_header(header)
    }

    /// Creates the unset error response used by Java's typed-header factory.
    pub fn create_java_default_error_response_command() -> Self {
        application_remoting_command_factory().create_java_default_error_response_command()
    }

    /// Creates the unset Java-compatible error response with a typed header.
    pub fn create_java_default_error_response_command_with_header(
        header: impl CommandCustomHeader + Sync + Send + 'static,
    ) -> Self {
        application_remoting_command_factory().create_java_default_error_response_command_with_header(header)
    }

    pub fn create_new_request_id() -> i32 {
        next_request_id()
    }
}
