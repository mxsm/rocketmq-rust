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

//! Remoting-owned construction and source-compatibility helpers.
//!
//! `RemotingCommand` itself is always the canonical protocol type. Process
//! environment defaults and the historical `ArcMut` setter remain here so the
//! protocol owner does not acquire environment or synchronization dependencies.

use std::sync::LazyLock;

use rocketmq_protocol::protocol::command_custom_header::CommandCustomHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::SerializeType;

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
        .unwrap_or(rocketmq_protocol::version::CURRENT_VERSION as i32);
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

pub fn create_remoting_command(code: impl Into<i32>) -> RemotingCommand {
    RemotingCommand::with_resolved_defaults(LEGACY_DEFAULTS.version, LEGACY_DEFAULTS.serialize_type).set_code(code)
}

pub fn create_request_command<T>(code: impl Into<i32>, header: T) -> RemotingCommand
where
    T: CommandCustomHeader + Sync + Send + 'static,
{
    RemotingCommand::create_request_command_with_defaults(
        code,
        header,
        LEGACY_DEFAULTS.version,
        LEGACY_DEFAULTS.serialize_type,
    )
}

pub fn create_response_command() -> RemotingCommand {
    create_remoting_command(rocketmq_protocol::code::response_code::RemotingSysResponseCode::Success)
        .mark_response_type()
}

#[allow(deprecated)]
pub use super::remoting_command::set_command_custom_header_origin;
