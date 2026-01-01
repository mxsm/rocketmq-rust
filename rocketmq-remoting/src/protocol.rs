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

use cheetah_string::CheetahString;
use rocketmq_common::utils::serde_json_utils::SerdeJsonUtils;
use rocketmq_common::utils::time_utils;
use serde::ser::SerializeStruct;
use serde::Deserialize;
use serde::Serialize;
use serde::Serializer;

use crate::rocketmq_serializable::RocketMQSerializable;

pub mod admin;
pub mod body;
pub mod broker_sync_info;
pub mod command_custom_header;
pub mod filter;
pub mod forbidden_type;
pub mod header;
pub mod heartbeat;
pub mod namespace_util;
pub mod namesrv;
pub mod remoting_command;
pub mod request_source;
pub mod request_type;
pub mod rocketmq_serializable;
pub mod route;
pub mod static_topic;
pub mod subscription;
pub mod topic;

#[repr(u8)]
#[derive(Debug, PartialEq, Eq)]
pub enum RemotingCommandType {
    REQUEST,
    RESPONSE,
}

impl RemotingCommandType {
    pub fn value_of(code: u8) -> Option<Self> {
        match code {
            0 => Some(RemotingCommandType::REQUEST),
            1 => Some(RemotingCommandType::RESPONSE),
            _ => None,
        }
    }

    pub fn get_code(&self) -> u8 {
        match self {
            RemotingCommandType::REQUEST => 0,
            RemotingCommandType::RESPONSE => 1,
        }
    }

    pub fn get_type_from_name(name: &str) -> Option<Self> {
        match name.to_ascii_uppercase().as_str() {
            "REQUEST" => Some(RemotingCommandType::REQUEST),
            "RESPONSE" => Some(RemotingCommandType::RESPONSE),
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
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            LanguageCode::JAVA => write!(f, "JAVA"),
            LanguageCode::CPP => write!(f, "CPP"),
            LanguageCode::DOTNET => write!(f, "DOTNET"),
            LanguageCode::PYTHON => write!(f, "PYTHON"),
            LanguageCode::DELPHI => write!(f, "DELPHI"),
            LanguageCode::ERLANG => write!(f, "ERLANG"),
            LanguageCode::RUBY => write!(f, "RUBY"),
            LanguageCode::OTHER => write!(f, "OTHER"),
            LanguageCode::HTTP => write!(f, "HTTP"),
            LanguageCode::GO => write!(f, "GO"),
            LanguageCode::PHP => write!(f, "PHP"),
            LanguageCode::OMS => write!(f, "OMS"),
            LanguageCode::RUST => write!(f, "RUST"),
        }
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
        if let Ok(c) = u8::try_from(code) {
            LanguageCode::value_of(c).unwrap_or(LanguageCode::OTHER)
        } else {
            LanguageCode::OTHER
        }
    }
}

impl From<i32> for LanguageCode {
    fn from(code: i32) -> Self {
        if let Ok(c) = u8::try_from(code) {
            LanguageCode::value_of(c).unwrap_or(LanguageCode::OTHER)
        } else {
            LanguageCode::OTHER
        }
    }
}

impl From<u8> for LanguageCode {
    fn from(code: u8) -> Self {
        LanguageCode::value_of(code).unwrap_or(LanguageCode::OTHER)
    }
}

impl LanguageCode {
    pub fn value_of(code: u8) -> Option<Self> {
        match code {
            0 => Some(LanguageCode::JAVA),
            1 => Some(LanguageCode::CPP),
            2 => Some(LanguageCode::DOTNET),
            3 => Some(LanguageCode::PYTHON),
            4 => Some(LanguageCode::DELPHI),
            5 => Some(LanguageCode::ERLANG),
            6 => Some(LanguageCode::RUBY),
            7 => Some(LanguageCode::OTHER),
            8 => Some(LanguageCode::HTTP),
            9 => Some(LanguageCode::GO),
            10 => Some(LanguageCode::PHP),
            11 => Some(LanguageCode::OMS),
            12 => Some(LanguageCode::RUST),
            _ => Some(LanguageCode::OTHER),
        }
    }

    pub fn get_code(&self) -> u8 {
        match self {
            LanguageCode::JAVA => 0,
            LanguageCode::CPP => 1,
            LanguageCode::DOTNET => 2,
            LanguageCode::PYTHON => 3,
            LanguageCode::DELPHI => 4,
            LanguageCode::ERLANG => 5,
            LanguageCode::RUBY => 6,
            LanguageCode::OTHER => 7,
            LanguageCode::HTTP => 8,
            LanguageCode::GO => 9,
            LanguageCode::PHP => 10,
            LanguageCode::OMS => 11,
            LanguageCode::RUST => 12,
        }
    }

    pub fn get_code_from_name(name: &str) -> Option<Self> {
        match name.to_ascii_uppercase().as_str() {
            "JAVA" => Some(LanguageCode::JAVA),
            "CPP" => Some(LanguageCode::CPP),
            "DOTNET" => Some(LanguageCode::DOTNET),
            "PYTHON" => Some(LanguageCode::PYTHON),
            "DELPHI" => Some(LanguageCode::DELPHI),
            "ERLANG" => Some(LanguageCode::ERLANG),
            "RUBY" => Some(LanguageCode::RUBY),
            "OTHER" => Some(LanguageCode::OTHER),
            "HTTP" => Some(LanguageCode::HTTP),
            "GO" => Some(LanguageCode::GO),
            "PHP" => Some(LanguageCode::PHP),
            "OMS" => Some(LanguageCode::OMS),
            "RUST" => Some(LanguageCode::RUST),
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
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            SerializeType::JSON => write!(f, "JSON"),
            SerializeType::ROCKETMQ => write!(f, "ROCKETMQ"),
        }
    }
}

impl SerializeType {
    pub fn value_of(code: u8) -> Option<Self> {
        match code {
            0 => Some(SerializeType::JSON),
            1 => Some(SerializeType::ROCKETMQ),
            _ => None,
        }
    }

    pub fn get_code(&self) -> u8 {
        match self {
            SerializeType::JSON => 0,
            SerializeType::ROCKETMQ => 1,
        }
    }
}

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
        struct DataVersionHelper {
            state_version: i64,
            timestamp: i64,
            counter: i64,
        }

        let helper = DataVersionHelper::deserialize(deserializer)?;
        Ok(DataVersion {
            state_version: helper.state_version,
            timestamp: helper.timestamp,
            counter: AtomicI64::new(helper.counter),
        })
    }
}

impl Clone for DataVersion {
    fn clone(&self) -> Self {
        DataVersion {
            state_version: self.state_version,
            timestamp: self.timestamp,
            counter: AtomicI64::new(self.counter.load(Ordering::SeqCst)),
        }
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
        DataVersion::new()
    }
}

impl DataVersion {
    pub fn new() -> Self {
        let timestamp = time_utils::get_current_millis() as i64;

        DataVersion {
            state_version: 0,
            timestamp,
            counter: AtomicI64::new(0),
        }
    }

    pub fn assign_new_one(&mut self, data_version: &DataVersion) {
        self.timestamp = data_version.timestamp;
        self.state_version = data_version.state_version;
        self.counter = AtomicI64::new(data_version.counter.load(Ordering::SeqCst));
    }

    pub fn get_state_version(&self) -> i64 {
        self.state_version
    }

    pub fn set_state_version(&mut self, state_version: i64) {
        self.state_version = state_version;
    }

    pub fn get_timestamp(&self) -> i64 {
        self.timestamp
    }

    pub fn set_timestamp(&mut self, timestamp: i64) {
        self.timestamp = timestamp;
    }

    pub fn get_counter(&self) -> i64 {
        self.counter.load(Ordering::Relaxed)
    }

    pub fn set_counter(&mut self, counter: i64) {
        self.counter.store(counter, Ordering::Relaxed);
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
        self.counter.load(Ordering::Relaxed)
    }

    pub fn next_version(&mut self) {
        self.next_version_with(0)
    }

    pub fn next_version_with(&mut self, state_version: i64) {
        self.timestamp = time_utils::get_current_millis() as i64;
        self.state_version = state_version;
        self.counter.fetch_add(1, Ordering::SeqCst);
    }
}

impl Display for DataVersion {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let counter_value = self.counter.load(Ordering::SeqCst);
        write!(
            f,
            "State Version: {}, Timestamp: {}, Counter: {}",
            self.state_version, self.timestamp, counter_value
        )
    }
}

/// Trait for serializable objects in a remoting context.
///
/// This trait defines methods for serializing objects into different formats,
/// including binary, standard JSON, and pretty-printed JSON. It is intended for
/// use with types that need to be transmitted over a network or stored in a format
/// that can be easily shared or read.
pub trait RemotingSerializable {
    /// Encodes the object into a vector of bytes.
    ///
    /// # Returns
    /// A `Result` containing a vector of bytes representing the encoded object,
    /// or an error if encoding fails.
    fn encode(&self) -> rocketmq_error::RocketMQResult<Vec<u8>>;

    /// Serializes the object into a JSON string.
    ///
    /// # Returns
    /// A `Result` containing a JSON string representing the object,
    /// or an error if serialization fails.
    fn serialize_json(&self) -> rocketmq_error::RocketMQResult<String>;

    /// Serializes the object into a pretty-printed JSON string.
    ///
    /// # Returns
    /// A `Result` containing a pretty-printed JSON string representing the object,
    /// or an error if serialization fails.
    fn serialize_json_pretty(&self) -> rocketmq_error::RocketMQResult<String>;
}

/// Trait for deserializing objects in a remoting context.
///
/// This trait defines a method for deserializing objects from a binary format,
/// typically received over a network. Implementors of this trait can specify
/// their own output types and deserialization logic, making it flexible for
/// various use cases.
///
/// # Type Parameters
/// - `Output`: The type of the object after deserialization.
pub trait RemotingDeserializable {
    /// The output type resulting from the deserialization.
    type Output;

    /// Deserializes an object from a slice of bytes.
    ///
    /// This method attempts to convert a slice of bytes into an instance of the `Output` type.
    /// It returns a `Result` indicating either success with the deserialized object or an error
    /// if deserialization fails.
    ///
    /// # Arguments
    /// * `bytes` - A slice of bytes representing the serialized object.
    ///
    /// # Returns
    /// A `Result` containing either the deserialized object of type `Output` or an `Error` if
    /// deserialization fails.
    fn decode(bytes: &[u8]) -> rocketmq_error::RocketMQResult<Self::Output>;

    /// Decodes a string slice (`&str`) into the output type defined by the implementor of the
    /// `RemotingDeserializable` trait.
    ///
    /// # Arguments
    /// * `s` - A string slice containing the serialized data to be decoded.
    ///
    /// # Returns
    /// A `RocketMQResult` containing the decoded object of type `Self::Output` if successful, or an
    /// error if decoding fails.
    fn decode_str(s: &str) -> rocketmq_error::RocketMQResult<Self::Output> {
        Self::decode(s.as_bytes())
    }

    /// Decodes an owned `String` into the output type defined by the implementor of the
    /// `RemotingDeserializable` trait.
    ///
    /// # Arguments
    /// * `s` - An owned `String` containing the serialized data to be decoded.
    ///
    /// # Returns
    /// A `RocketMQResult` containing the decoded object of type `Self::Output` if successful, or an
    /// error if decoding fails.
    fn decode_string(s: String) -> rocketmq_error::RocketMQResult<Self::Output> {
        Self::decode_str(&s)
    }
}

impl<T: Serialize> RemotingSerializable for T {
    fn encode(&self) -> rocketmq_error::RocketMQResult<Vec<u8>> {
        SerdeJsonUtils::serialize_json_vec(self)
    }

    fn serialize_json(&self) -> rocketmq_error::RocketMQResult<String> {
        SerdeJsonUtils::serialize_json(self)
    }

    fn serialize_json_pretty(&self) -> rocketmq_error::RocketMQResult<String> {
        SerdeJsonUtils::serialize_json_pretty(self)
    }
}

impl<T: serde::de::DeserializeOwned> RemotingDeserializable for T {
    type Output = T;
    fn decode(bytes: &[u8]) -> rocketmq_error::RocketMQResult<Self::Output> {
        SerdeJsonUtils::from_json_bytes(bytes)
    }
}

/// Trait for handling fast encoding and decoding headers in a RocketMQ message.
///
/// This trait provides methods for efficiently encoding and decoding message headers,
/// leveraging RocketMQ's serialization capabilities. It is designed for internal use
/// within the RocketMQ client to optimize performance in message processing.
pub trait FastCodesHeader {
    /// Writes a key-value pair to the output buffer if the value is not null or empty.
    ///
    /// This method is a utility function used during the encoding process to ensure
    /// that only non-empty values are written to the buffer, avoiding unnecessary
    /// serialization of empty strings.
    ///
    /// # Arguments
    /// * `out` - A mutable reference to the output buffer (`bytes::BytesMut`) where the key-value
    ///   pair is written.
    /// * `key` - The key as a string slice.
    /// * `value` - The value as a string slice.
    fn write_if_not_null(out: &mut bytes::BytesMut, key: &str, value: &str) {
        if !value.is_empty() {
            RocketMQSerializable::write_str(out, true, key);
            RocketMQSerializable::write_str(out, false, key);
        }
    }

    /// Encodes the implementing object's data into the provided output buffer.
    ///
    /// This method should be implemented to encode the specific header fields of a message
    /// or another entity into a compact binary format for transmission or storage.
    ///
    /// # Arguments
    /// * `out` - A mutable reference to the output buffer (`bytes::BytesMut`) where the encoded
    ///   data is written.
    fn encode_fast(&mut self, out: &mut bytes::BytesMut);

    /// Decodes data from a map of fields into the implementing object.
    ///
    /// This method should be implemented to populate the object's fields from a map
    /// containing header names and values. It is used to reconstruct an object from
    /// data received over the network or read from storage.
    ///
    /// # Arguments
    /// * `fields` - A reference to a `HashMap` containing the header fields as key-value pairs.
    fn decode_fast(&mut self, fields: &HashMap<CheetahString, CheetahString>);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_remoting_command_type() {
        // Test RemotingCommandType::value_of
        assert_eq!(Some(RemotingCommandType::REQUEST), RemotingCommandType::value_of(0));
        assert_eq!(Some(RemotingCommandType::RESPONSE), RemotingCommandType::value_of(1));
        assert_eq!(None, RemotingCommandType::value_of(2));

        // Test RemotingCommandType::get_code
        assert_eq!(0, RemotingCommandType::REQUEST.get_code());
        assert_eq!(1, RemotingCommandType::RESPONSE.get_code());

        // Test RemotingCommandType::get_type_from_name
        assert_eq!(
            Some(RemotingCommandType::REQUEST),
            RemotingCommandType::get_type_from_name("REQUEST")
        );
        assert_eq!(
            Some(RemotingCommandType::RESPONSE),
            RemotingCommandType::get_type_from_name("RESPONSE")
        );
        assert_eq!(None, RemotingCommandType::get_type_from_name("UNKNOWN"));
    }

    #[test]
    fn test_language_code() {
        // Test LanguageCode::value_of
        assert_eq!(Some(LanguageCode::JAVA), LanguageCode::value_of(0));
        assert_eq!(Some(LanguageCode::CPP), LanguageCode::value_of(1));
        assert_eq!(Some(LanguageCode::DOTNET), LanguageCode::value_of(2));
        // Add more value_of tests for other variants...

        // Test LanguageCode::get_code
        assert_eq!(0, LanguageCode::JAVA.get_code());
        assert_eq!(1, LanguageCode::CPP.get_code());
        assert_eq!(2, LanguageCode::DOTNET.get_code());

        // Test LanguageCode::get_code_from_name
        assert_eq!(Some(LanguageCode::JAVA), LanguageCode::get_code_from_name("JAVA"));
        assert_eq!(Some(LanguageCode::CPP), LanguageCode::get_code_from_name("CPP"));
        assert_eq!(Some(LanguageCode::DOTNET), LanguageCode::get_code_from_name("DOTNET"));
    }

    #[test]
    fn data_version_serialization_deserialization() {
        let mut data_version = DataVersion::new();
        data_version.set_state_version(10);
        let serialized = serde_json::to_string(&data_version).unwrap();
        let deserialized: DataVersion = serde_json::from_str(&serialized).unwrap();
        assert_eq!(data_version.state_version, deserialized.state_version);
        assert_eq!(data_version.timestamp, deserialized.timestamp);
        assert_eq!(
            data_version.counter.load(Ordering::SeqCst),
            deserialized.counter.load(Ordering::SeqCst)
        );
    }

    #[test]
    fn data_version_counter_increment() {
        let data_version = DataVersion::new();
        let initial_counter = data_version.counter.load(Ordering::SeqCst);
        data_version.increment_counter();
        assert_eq!(initial_counter + 1, data_version.counter.load(Ordering::SeqCst));
    }

    #[test]
    fn data_version_next_version() {
        let mut data_version = DataVersion::new();
        let initial_state_version = data_version.state_version;
        let initial_timestamp = data_version.timestamp;
        let initial_counter = data_version.counter.load(Ordering::SeqCst);
        data_version.next_version();
        assert_eq!(initial_state_version, data_version.state_version);
        assert!(data_version.timestamp >= initial_timestamp);
        assert_eq!(initial_counter + 1, data_version.counter.load(Ordering::SeqCst));
    }

    #[test]
    fn data_version_next_version_with_state() {
        let mut data_version = DataVersion::new();
        let initial_timestamp = data_version.timestamp;
        let initial_counter = data_version.counter.load(Ordering::SeqCst);
        data_version.next_version_with(20);
        assert_eq!(20, data_version.state_version);
        assert!(data_version.timestamp >= initial_timestamp);
        assert_eq!(initial_counter + 1, data_version.counter.load(Ordering::SeqCst));
    }

    #[test]
    fn data_version_equality() {
        let data_version1 = DataVersion::new();
        let data_version2 = data_version1.clone();
        assert_eq!(data_version1, data_version2);

        data_version2.increment_counter();
        assert_ne!(data_version1, data_version2);
    }

    #[test]
    fn data_version_partial_ordering() {
        let data_version1 = DataVersion::new();
        let mut data_version2 = data_version1.clone();

        assert_eq!(
            data_version1.partial_cmp(&data_version2),
            Some(std::cmp::Ordering::Equal)
        );

        data_version2.set_state_version(data_version1.get_state_version() + 1);
        assert_eq!(
            data_version1.partial_cmp(&data_version2),
            Some(std::cmp::Ordering::Less)
        );
        assert_eq!(
            data_version2.partial_cmp(&data_version1),
            Some(std::cmp::Ordering::Greater)
        );
    }

    #[test]
    fn data_version_total_ordering() {
        let data_version1 = DataVersion::new();
        let mut data_version2 = data_version1.clone();

        assert_eq!(data_version1.cmp(&data_version2), std::cmp::Ordering::Equal);

        data_version2.set_state_version(data_version1.get_state_version() + 1);
        assert_eq!(data_version1.cmp(&data_version2), std::cmp::Ordering::Less);
        assert_eq!(data_version2.cmp(&data_version1), std::cmp::Ordering::Greater);
    }
}
