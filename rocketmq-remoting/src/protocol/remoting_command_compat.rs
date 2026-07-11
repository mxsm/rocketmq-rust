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

use std::collections::HashMap;
use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::LazyLock;

use bytes::Bytes;
use cheetah_string::CheetahString;
use rocketmq_common::common::mq_version::CURRENT_VERSION;
use rocketmq_protocol::protocol::command_custom_header::CommandCustomHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand as CanonicalRemotingCommand;
use rocketmq_protocol::protocol::LanguageCode;
use rocketmq_protocol::protocol::SerializeType;
use rocketmq_rust::ArcMut;
use serde::Deserialize;
use serde::Serialize;

use super::remoting_command::REMOTING_VERSION_KEY;
use super::remoting_command::SERIALIZE_TYPE_ENV;
use super::remoting_command::SERIALIZE_TYPE_PROPERTY;

#[derive(Clone, Copy)]
struct ResolvedDefaults {
    version: i32,
    serialize_type: SerializeType,
}

static LEGACY_DEFAULTS: LazyLock<ResolvedDefaults> = LazyLock::new(|| {
    let version = std::env::var(REMOTING_VERSION_KEY)
        .ok()
        .and_then(|value| value.parse::<i32>().ok())
        .unwrap_or(CURRENT_VERSION as i32);
    let serialize_type = std::env::var(SERIALIZE_TYPE_PROPERTY)
        .or_else(|_| std::env::var(SERIALIZE_TYPE_ENV))
        .ok()
        .and_then(|value| match value.as_str() {
            "JSON" => Some(SerializeType::JSON),
            "ROCKETMQ" => Some(SerializeType::ROCKETMQ),
            _ => None,
        })
        .unwrap_or(SerializeType::JSON);
    ResolvedDefaults {
        version,
        serialize_type,
    }
});

/// Compatibility wrapper that retains remoting-owned defaults and legacy consuming signatures.
#[repr(transparent)]
#[derive(Serialize, Deserialize)]
#[serde(transparent)]
pub struct RemotingCommand(CanonicalRemotingCommand);

impl Default for RemotingCommand {
    fn default() -> Self {
        Self(CanonicalRemotingCommand::with_resolved_defaults(
            0,
            LEGACY_DEFAULTS.serialize_type,
        ))
    }
}

impl Clone for RemotingCommand {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl std::fmt::Display for RemotingCommand {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

impl Deref for RemotingCommand {
    type Target = CanonicalRemotingCommand;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for RemotingCommand {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<CanonicalRemotingCommand> for RemotingCommand {
    fn from(command: CanonicalRemotingCommand) -> Self {
        Self(command)
    }
}

impl From<RemotingCommand> for CanonicalRemotingCommand {
    fn from(command: RemotingCommand) -> Self {
        command.0
    }
}

impl RemotingCommand {
    pub fn new_request(code: impl Into<i32>, body: impl Into<Bytes>) -> Self {
        Self::default().set_code(code).set_body(body)
    }

    pub fn create_request_command<T>(code: impl Into<i32>, header: T) -> Self
    where
        T: CommandCustomHeader + Sync + Send + 'static,
    {
        Self(CanonicalRemotingCommand::create_request_command_with_defaults(
            code,
            header,
            LEGACY_DEFAULTS.version,
            LEGACY_DEFAULTS.serialize_type,
        ))
    }

    pub fn create_remoting_command(code: impl Into<i32>) -> Self {
        Self::default().set_code(code)
    }

    pub fn get_and_add() -> i32 {
        CanonicalRemotingCommand::get_and_add()
    }

    pub fn create_response_command_with_code(code: impl Into<i32>) -> Self {
        Self::default().set_code(code).mark_response_type()
    }

    pub fn create_response_command_with_code_remark(code: impl Into<i32>, remark: impl Into<CheetahString>) -> Self {
        Self::default().set_code(code).set_remark(remark).mark_response_type()
    }

    pub fn create_response_command() -> Self {
        Self(CanonicalRemotingCommand::create_response_command()).set_serialize_type(LEGACY_DEFAULTS.serialize_type)
    }

    pub fn create_response_command_with_header(header: impl CommandCustomHeader + Sync + Send + 'static) -> Self {
        Self::default().set_command_custom_header(header).mark_response_type()
    }

    pub fn set_command_custom_header<T>(self, header: T) -> Self
    where
        T: CommandCustomHeader + Sync + Send + 'static,
    {
        Self(self.0.set_command_custom_header(header))
    }

    pub fn set_command_custom_header_origin(
        self,
        header: Option<ArcMut<Box<dyn CommandCustomHeader + Send + Sync + 'static>>>,
    ) -> Self {
        Self(self.0.set_command_custom_header_origin(header))
    }

    pub fn set_code(self, code: impl Into<i32>) -> Self {
        Self(self.0.set_code(code))
    }

    pub fn set_language(self, language: LanguageCode) -> Self {
        Self(self.0.set_language(language))
    }

    pub fn set_version(self, version: i32) -> Self {
        Self(self.0.set_version(version))
    }

    pub fn set_opaque(self, opaque: i32) -> Self {
        Self(self.0.set_opaque(opaque))
    }

    pub fn set_flag(self, flag: i32) -> Self {
        Self(self.0.set_flag(flag))
    }

    pub fn set_remark_option(self, remark: Option<impl Into<CheetahString>>) -> Self {
        Self(self.0.set_remark_option(remark))
    }

    pub fn set_remark(self, remark: impl Into<CheetahString>) -> Self {
        Self(self.0.set_remark(remark))
    }

    pub fn set_ext_fields(self, fields: HashMap<CheetahString, CheetahString>) -> Self {
        Self(self.0.set_ext_fields(fields))
    }

    pub fn set_body(self, body: impl Into<Bytes>) -> Self {
        Self(self.0.set_body(body))
    }

    pub fn set_suspended(self, suspended: bool) -> Self {
        Self(self.0.set_suspended(suspended))
    }

    pub fn set_serialize_type(self, serialize_type: SerializeType) -> Self {
        Self(self.0.set_serialize_type(serialize_type))
    }

    pub fn mark_response_type(self) -> Self {
        Self(self.0.mark_response_type())
    }

    pub fn mark_oneway_rpc(self) -> Self {
        Self(self.0.mark_oneway_rpc())
    }
}
