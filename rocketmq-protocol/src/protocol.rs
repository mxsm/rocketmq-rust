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
use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;

use bytes::BytesMut;
use cheetah_string::CheetahString;
use serde::ser::SerializeStruct;
use serde::Deserialize;
use serde::Serialize;
use serde::Serializer;

use self::rocketmq_serializable::RocketMQSerializable;

pub mod admin;
pub mod body;
pub mod broker_sync_info;
pub mod command_custom_header;
pub mod filter;
pub mod forbidden_type;
pub mod header;
pub mod heartbeat;
pub mod namesrv;
pub mod remoting_command;
pub mod request_source;
pub mod request_type;
pub mod rocketmq_serializable;
pub mod route;
pub mod static_topic;
pub mod subscription;
pub mod topic;

pub use command_custom_header::CommandCustomHeader;
pub use command_custom_header::FromMap;
pub use remoting_command::RemotingCommand;

#[derive(Debug)]
pub struct DataVersion {
    state_version: i64,
    timestamp: i64,
    counter: AtomicI64,
}

impl Serialize for DataVersion {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("DataVersion", 3)?;
        state.serialize_field("stateVersion", &self.state_version)?;
        state.serialize_field("timestamp", &self.timestamp)?;
        state.serialize_field("counter", &self.counter.load(Ordering::SeqCst))?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for DataVersion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Helper {
            state_version: i64,
            timestamp: i64,
            counter: i64,
        }

        let helper = Helper::deserialize(deserializer)?;
        Ok(Self::with_values(
            helper.state_version,
            helper.timestamp,
            helper.counter,
        ))
    }
}

impl Clone for DataVersion {
    fn clone(&self) -> Self {
        Self::with_values(self.state_version, self.timestamp, self.get_counter())
    }
}

impl PartialEq for DataVersion {
    fn eq(&self, other: &Self) -> bool {
        self.state_version == other.state_version
            && self.timestamp == other.timestamp
            && self.get_counter() == other.get_counter()
    }
}

impl Eq for DataVersion {}

impl PartialOrd for DataVersion {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DataVersion {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.state_version
            .cmp(&other.state_version)
            .then_with(|| self.timestamp.cmp(&other.timestamp))
            .then_with(|| self.get_counter().cmp(&other.get_counter()))
    }
}

impl Default for DataVersion {
    fn default() -> Self {
        Self::with_values(0, 0, 0)
    }
}

impl DataVersion {
    pub const fn with_values(state_version: i64, timestamp: i64, counter: i64) -> Self {
        Self {
            state_version,
            timestamp,
            counter: AtomicI64::new(counter),
        }
    }

    pub fn assign_new_one(&mut self, data_version: &Self) {
        self.timestamp = data_version.timestamp;
        self.state_version = data_version.state_version;
        self.counter.store(data_version.get_counter(), Ordering::SeqCst);
    }

    pub fn get_state_version(&self) -> i64 {
        self.state_version
    }
    pub fn set_state_version(&mut self, value: i64) {
        self.state_version = value;
    }
    pub fn get_timestamp(&self) -> i64 {
        self.timestamp
    }
    pub fn set_timestamp(&mut self, value: i64) {
        self.timestamp = value;
    }
    pub fn get_counter(&self) -> i64 {
        self.counter.load(Ordering::Relaxed)
    }
    pub fn set_counter(&self, value: i64) {
        self.counter.store(value, Ordering::Relaxed);
    }
    pub fn increment_counter(&self) -> i64 {
        self.counter.fetch_add(1, Ordering::Relaxed) + 1
    }
    pub fn state_version(&self) -> i64 {
        self.state_version
    }
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }
    pub fn counter(&self) -> i64 {
        self.get_counter()
    }
    pub fn next_version_with_timestamp(&mut self, state_version: i64, timestamp: i64) {
        self.timestamp = timestamp;
        self.state_version = state_version;
        self.counter.fetch_add(1, Ordering::SeqCst);
    }
}

impl Display for DataVersion {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "State Version: {}, Timestamp: {}, Counter: {}",
            self.state_version,
            self.timestamp,
            self.get_counter()
        )
    }
}

pub trait RemotingSerializable {
    fn encode(&self) -> rocketmq_error::RocketMQResult<Vec<u8>>;
    fn serialize_json(&self) -> rocketmq_error::RocketMQResult<String>;
    fn serialize_json_pretty(&self) -> rocketmq_error::RocketMQResult<String>;
}

pub trait RemotingDeserializable {
    type Output;
    fn decode(bytes: &[u8]) -> rocketmq_error::RocketMQResult<Self::Output>;
    fn decode_str(value: &str) -> rocketmq_error::RocketMQResult<Self::Output> {
        Self::decode(value.as_bytes())
    }
    fn decode_string(value: String) -> rocketmq_error::RocketMQResult<Self::Output> {
        Self::decode_str(&value)
    }
}

impl<T: Serialize> RemotingSerializable for T {
    fn encode(&self) -> rocketmq_error::RocketMQResult<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }
    fn serialize_json(&self) -> rocketmq_error::RocketMQResult<String> {
        Ok(serde_json::to_string(self)?)
    }
    fn serialize_json_pretty(&self) -> rocketmq_error::RocketMQResult<String> {
        Ok(serde_json::to_string_pretty(self)?)
    }
}

impl<T: serde::de::DeserializeOwned> RemotingDeserializable for T {
    type Output = T;
    fn decode(bytes: &[u8]) -> rocketmq_error::RocketMQResult<Self::Output> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

pub trait FastCodesHeader {
    fn write_if_not_null(out: &mut BytesMut, key: &str, value: &str) {
        if !value.is_empty() {
            RocketMQSerializable::write_str(out, true, key);
            RocketMQSerializable::write_str(out, false, value);
        }
    }
    fn encode_fast(&mut self, out: &mut BytesMut);
    fn decode_fast(&mut self, fields: &HashMap<CheetahString, CheetahString>);
}

#[repr(u8)]
#[derive(Debug, PartialEq, Eq)]
pub enum RemotingCommandType {
    REQUEST,
    RESPONSE,
}

impl RemotingCommandType {
    pub fn value_of(code: u8) -> Option<Self> {
        match code {
            0 => Some(Self::REQUEST),
            1 => Some(Self::RESPONSE),
            _ => None,
        }
    }

    pub fn get_code(&self) -> u8 {
        match self {
            Self::REQUEST => 0,
            Self::RESPONSE => 1,
        }
    }

    pub fn get_type_from_name(name: &str) -> Option<Self> {
        match name.to_ascii_uppercase().as_str() {
            "REQUEST" => Some(Self::REQUEST),
            "RESPONSE" => Some(Self::RESPONSE),
            _ => None,
        }
    }
}

#[repr(u8)]
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, Default, Hash, Copy)]
pub enum LanguageCode {
    JAVA,
    CPP,
    DOTNET,
    PYTHON,
    DELPHI,
    ERLANG,
    RUBY,
    OTHER,
    HTTP,
    GO,
    PHP,
    OMS,
    #[default]
    RUST,
}

impl fmt::Display for LanguageCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl From<LanguageCode> for u8 {
    fn from(code: LanguageCode) -> Self {
        code.get_code()
    }
}

impl From<LanguageCode> for i32 {
    fn from(code: LanguageCode) -> Self {
        code.get_code() as i32
    }
}

impl From<u32> for LanguageCode {
    fn from(code: u32) -> Self {
        u8::try_from(code).map_or(Self::OTHER, Self::from)
    }
}

impl From<i32> for LanguageCode {
    fn from(code: i32) -> Self {
        u8::try_from(code).map_or(Self::OTHER, Self::from)
    }
}

impl From<u8> for LanguageCode {
    fn from(code: u8) -> Self {
        Self::value_of(code).unwrap_or(Self::OTHER)
    }
}

impl LanguageCode {
    pub fn value_of(code: u8) -> Option<Self> {
        Some(match code {
            0 => Self::JAVA,
            1 => Self::CPP,
            2 => Self::DOTNET,
            3 => Self::PYTHON,
            4 => Self::DELPHI,
            5 => Self::ERLANG,
            6 => Self::RUBY,
            7 => Self::OTHER,
            8 => Self::HTTP,
            9 => Self::GO,
            10 => Self::PHP,
            11 => Self::OMS,
            12 => Self::RUST,
            _ => Self::OTHER,
        })
    }

    pub fn get_code(&self) -> u8 {
        *self as u8
    }

    pub fn get_code_from_name(name: &str) -> Option<Self> {
        match name.to_ascii_uppercase().as_str() {
            "JAVA" => Some(Self::JAVA),
            "CPP" => Some(Self::CPP),
            "DOTNET" => Some(Self::DOTNET),
            "PYTHON" => Some(Self::PYTHON),
            "DELPHI" => Some(Self::DELPHI),
            "ERLANG" => Some(Self::ERLANG),
            "RUBY" => Some(Self::RUBY),
            "OTHER" => Some(Self::OTHER),
            "HTTP" => Some(Self::HTTP),
            "GO" => Some(Self::GO),
            "PHP" => Some(Self::PHP),
            "OMS" => Some(Self::OMS),
            "RUST" => Some(Self::RUST),
            _ => None,
        }
    }
}

#[repr(u8)]
#[derive(Debug, PartialEq, Eq, Clone, Copy, Deserialize, Serialize)]
pub enum SerializeType {
    JSON,
    ROCKETMQ,
}

impl fmt::Display for SerializeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl SerializeType {
    pub fn value_of(code: u8) -> Option<Self> {
        match code {
            0 => Some(Self::JSON),
            1 => Some(Self::ROCKETMQ),
            _ => None,
        }
    }

    pub fn get_code(&self) -> u8 {
        *self as u8
    }
}
