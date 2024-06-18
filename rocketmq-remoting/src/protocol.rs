/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use std::collections::HashMap;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::SystemTime;

use rocketmq_common::common::mix_all;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_common::utils::time_utils;
use serde::de;
use serde::ser::SerializeStruct;
use serde::Deserialize;
use serde::Serialize;
use serde::Serializer;

use crate::RocketMQSerializable;

pub mod body;
pub mod command_custom_header;
pub mod header;
pub mod heartbeat;
pub mod namesrv;
pub mod remoting_command;
pub mod rocketmq_serializable;
pub mod route;
pub mod static_topic;
pub mod subscription;

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
            _ => None,
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
    counter: Arc<AtomicI64>,
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
            counter: Arc::new(AtomicI64::new(helper.counter)),
        })
    }
}

impl RemotingSerializable for DataVersion {
    type Output = DataVersion;
}

impl Clone for DataVersion {
    fn clone(&self) -> Self {
        DataVersion {
            state_version: self.state_version,
            timestamp: self.timestamp,
            counter: self.counter.clone(),
        }
    }
}

impl PartialEq for DataVersion {
    fn eq(&self, other: &Self) -> bool {
        self.state_version == other.state_version
            && self.timestamp == other.timestamp
            && self.counter.load(Ordering::Relaxed) == other.counter.load(Ordering::Relaxed)
    }
}

impl Default for DataVersion {
    fn default() -> Self {
        DataVersion::new()
    }
}

impl DataVersion {
    pub fn new() -> Self {
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as i64;

        DataVersion {
            state_version: 0,
            timestamp,
            counter: Arc::new(AtomicI64::new(0)),
        }
    }

    pub fn assign_new_one(&mut self, data_version: &DataVersion) {
        self.timestamp = data_version.timestamp;
        self.state_version = data_version.state_version;
        self.counter = Arc::new(AtomicI64::new(data_version.counter.load(Ordering::SeqCst)));
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

    pub fn get_counter(&self) -> i64 {
        self.counter.load(Ordering::Relaxed)
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

/// A trait for types that can be deserialized from a byte vector.
pub trait RemotingSerializable {
    /// The output type after deserialization.
    type Output;

    /// Decode a byte vector into the corresponding type.
    ///
    /// # Arguments
    ///
    /// * `bytes` - The byte vector to be deserialized.
    ///
    /// # Returns
    ///
    /// The deserialized output of type `Self::Output`.
    fn decode<'a>(bytes: &'a [u8]) -> Self::Output
    where
        Self::Output: de::Deserialize<'a>,
    {
        serde_json::from_slice::<Self::Output>(bytes).unwrap()
    }

    fn encode(&self) -> Vec<u8>
    where
        Self: Serialize,
    {
        serde_json::to_vec(self).unwrap()
    }

    fn to_json(&self) -> String
    where
        Self: Serialize,
    {
        serde_json::to_string(self).unwrap()
    }

    fn to_json_pretty(&self) -> String
    where
        Self: Serialize,
    {
        serde_json::to_string_pretty(self).unwrap()
    }
}

pub trait FastCodesHeader {
    fn write_if_not_null(out: &mut bytes::BytesMut, key: &str, value: &str) {
        if !value.is_empty() {
            RocketMQSerializable::write_str(out, true, key);
            RocketMQSerializable::write_str(out, false, key);
        }
    }

    fn encode_fast(&mut self, out: &mut bytes::BytesMut);

    fn decode_fast(&mut self, fields: &HashMap<String, String>);
}

pub struct NamespaceUtil;

impl NamespaceUtil {
    pub const DLQ_PREFIX_LENGTH: usize = mix_all::DLQ_GROUP_TOPIC_PREFIX.len();
    pub const NAMESPACE_SEPARATOR: char = '%';
    pub const RETRY_PREFIX_LENGTH: usize = mix_all::RETRY_GROUP_TOPIC_PREFIX.len();
    pub const STRING_BLANK: &'static str = "";

    pub fn without_namespace(resource_with_namespace: &str) -> String {
        if resource_with_namespace.is_empty()
            || NamespaceUtil::is_system_resource(resource_with_namespace)
        {
            return resource_with_namespace.to_string();
        }

        let mut string_builder = String::new();
        if NamespaceUtil::is_retry_topic(resource_with_namespace) {
            string_builder.push_str(mix_all::RETRY_GROUP_TOPIC_PREFIX);
        }
        if NamespaceUtil::is_dlq_topic(resource_with_namespace) {
            string_builder.push_str(mix_all::DLQ_GROUP_TOPIC_PREFIX);
        }

        if let Some(index) = NamespaceUtil::without_retry_and_dlq(resource_with_namespace)
            .find(NamespaceUtil::NAMESPACE_SEPARATOR)
        {
            let resource_without_namespace =
                &NamespaceUtil::without_retry_and_dlq(resource_with_namespace)[index + 1..];
            return string_builder + resource_without_namespace;
        }

        resource_with_namespace.to_string()
    }

    pub fn without_namespace_with_namespace(
        resource_with_namespace: &str,
        namespace: &str,
    ) -> String {
        if resource_with_namespace.is_empty() || namespace.is_empty() {
            return resource_with_namespace.to_string();
        }

        let resource_without_retry_and_dlq =
            NamespaceUtil::without_retry_and_dlq(resource_with_namespace);
        if resource_without_retry_and_dlq.starts_with(&format!(
            "{}{}",
            namespace,
            NamespaceUtil::NAMESPACE_SEPARATOR
        )) {
            return NamespaceUtil::without_namespace(resource_with_namespace);
        }

        resource_with_namespace.to_string()
    }

    pub fn wrap_namespace(namespace: &str, resource_without_namespace: &str) -> String {
        if namespace.is_empty() || resource_without_namespace.is_empty() {
            return resource_without_namespace.to_string();
        }

        if NamespaceUtil::is_system_resource(resource_without_namespace)
            || NamespaceUtil::is_already_with_namespace(resource_without_namespace, namespace)
        {
            return resource_without_namespace.to_string();
        }

        let mut string_builder = String::new();

        if NamespaceUtil::is_retry_topic(resource_without_namespace) {
            string_builder.push_str(mix_all::RETRY_GROUP_TOPIC_PREFIX);
        }

        if NamespaceUtil::is_dlq_topic(resource_without_namespace) {
            string_builder.push_str(mix_all::DLQ_GROUP_TOPIC_PREFIX);
        }
        let resource_without_retry_and_dlq =
            NamespaceUtil::without_retry_and_dlq(resource_without_namespace);
        string_builder
            + namespace
            + &NamespaceUtil::NAMESPACE_SEPARATOR.to_string()
            + resource_without_retry_and_dlq
    }

    pub fn is_already_with_namespace(resource: &str, namespace: &str) -> bool {
        if namespace.is_empty()
            || resource.is_empty()
            || NamespaceUtil::is_system_resource(resource)
        {
            return false;
        }

        let resource_without_retry_and_dlq = NamespaceUtil::without_retry_and_dlq(resource);

        resource_without_retry_and_dlq.starts_with(&format!(
            "{}{}",
            namespace,
            NamespaceUtil::NAMESPACE_SEPARATOR
        ))
    }

    pub fn wrap_namespace_and_retry(namespace: &str, consumer_group: &str) -> Option<String> {
        if consumer_group.is_empty() {
            return None;
        }

        Some(
            mix_all::RETRY_GROUP_TOPIC_PREFIX.to_string()
                + &NamespaceUtil::wrap_namespace(namespace, consumer_group),
        )
    }

    pub fn get_namespace_from_resource(resource: &str) -> String {
        if resource.is_empty() || NamespaceUtil::is_system_resource(resource) {
            return NamespaceUtil::STRING_BLANK.to_string();
        }
        let resource_without_retry_and_dlq = NamespaceUtil::without_retry_and_dlq(resource);
        if let Some(index) = resource_without_retry_and_dlq.find(NamespaceUtil::NAMESPACE_SEPARATOR)
        {
            return resource_without_retry_and_dlq[..index].to_string();
        }

        NamespaceUtil::STRING_BLANK.to_string()
    }

    fn without_retry_and_dlq(original_resource: &str) -> &str {
        if original_resource.is_empty() {
            return NamespaceUtil::STRING_BLANK;
        }
        if NamespaceUtil::is_retry_topic(original_resource) {
            return &original_resource[NamespaceUtil::RETRY_PREFIX_LENGTH..];
        }

        if NamespaceUtil::is_dlq_topic(original_resource) {
            return &original_resource[NamespaceUtil::DLQ_PREFIX_LENGTH..];
        }

        original_resource
    }

    fn is_system_resource(resource: &str) -> bool {
        if resource.is_empty() {
            return false;
        }
        TopicValidator::is_system_topic(resource) || mix_all::is_sys_consumer_group(resource)
    }

    fn is_retry_topic(resource: &str) -> bool {
        !resource.is_empty() && resource.starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX)
    }

    fn is_dlq_topic(resource: &str) -> bool {
        !resource.is_empty() && resource.starts_with(mix_all::DLQ_GROUP_TOPIC_PREFIX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn without_namespace_returns_original_when_empty() {
        assert_eq!(NamespaceUtil::without_namespace(""), "");
    }

    #[test]
    fn without_namespace_returns_original_when_system_resource() {
        assert_eq!(NamespaceUtil::without_namespace("SYS_TOPIC"), "SYS_TOPIC");
    }

    #[test]
    fn without_namespace_removes_namespace() {
        assert_eq!(
            NamespaceUtil::without_namespace("my_namespace%my_resource"),
            "my_resource"
        );
    }

    #[test]
    fn without_namespace_with_namespace_returns_original_when_empty() {
        assert_eq!(
            NamespaceUtil::without_namespace_with_namespace("", "my_namespace"),
            ""
        );
    }

    #[test]
    fn without_namespace_with_namespace_removes_namespace() {
        assert_eq!(
            NamespaceUtil::without_namespace_with_namespace(
                "my_namespace%my_resource",
                "my_namespace"
            ),
            "my_resource"
        );
    }

    #[test]
    fn wrap_namespace_returns_original_when_empty() {
        assert_eq!(NamespaceUtil::wrap_namespace("my_namespace", ""), "");
    }

    #[test]
    fn wrap_namespace_adds_namespace() {
        assert_eq!(
            NamespaceUtil::wrap_namespace("my_namespace", "my_resource"),
            "my_namespace%my_resource"
        );
    }

    #[test]
    fn is_already_with_namespace_returns_false_when_empty() {
        assert_eq!(
            NamespaceUtil::is_already_with_namespace("", "my_namespace"),
            false
        );
    }

    #[test]
    fn is_already_with_namespace_returns_true_when_with_namespace() {
        assert_eq!(
            NamespaceUtil::is_already_with_namespace("my_namespace%my_resource", "my_namespace"),
            true
        );
    }

    #[test]
    fn wrap_namespace_and_retry_returns_none_when_empty() {
        assert_eq!(
            NamespaceUtil::wrap_namespace_and_retry("my_namespace", ""),
            None
        );
    }

    #[test]
    fn wrap_namespace_and_retry_adds_namespace_and_retry() {
        assert_eq!(
            NamespaceUtil::wrap_namespace_and_retry("my_namespace", "my_group"),
            Some("%RETRY%my_namespace%my_group".to_string())
        );
    }

    #[test]
    fn get_namespace_from_resource_returns_blank_when_empty() {
        assert_eq!(NamespaceUtil::get_namespace_from_resource(""), "");
    }

    #[test]
    fn get_namespace_from_resource_returns_namespace() {
        assert_eq!(
            NamespaceUtil::get_namespace_from_resource("my_namespace%my_resource"),
            "my_namespace"
        );
    }

    #[test]
    fn without_retry_and_dlq_returns_original_when_empty() {
        assert_eq!(NamespaceUtil::without_retry_and_dlq(""), "");
    }

    #[test]
    fn without_retry_and_dlq_removes_retry_and_dlq() {
        assert_eq!(
            NamespaceUtil::without_retry_and_dlq("RETRY_GROUP_TOPIC_PREFIXmy_resource"),
            "RETRY_GROUP_TOPIC_PREFIXmy_resource"
        );
        assert_eq!(
            NamespaceUtil::without_retry_and_dlq("DLQ_GROUP_TOPIC_PREFIXmy_resource"),
            "DLQ_GROUP_TOPIC_PREFIXmy_resource"
        );
    }

    #[test]
    fn is_system_resource_returns_false_when_empty() {
        assert_eq!(NamespaceUtil::is_system_resource(""), false);
    }

    #[test]
    fn is_system_resource_returns_true_when_system_resource() {
        assert_eq!(NamespaceUtil::is_system_resource("CID_RMQ_SYS_"), true);
        assert_eq!(NamespaceUtil::is_system_resource("TBW102"), true);
    }

    #[test]
    fn is_retry_topic_returns_false_when_empty() {
        assert_eq!(NamespaceUtil::is_retry_topic(""), false);
    }

    #[test]
    fn is_retry_topic_returns_true_when_retry_topic() {
        assert_eq!(
            NamespaceUtil::is_retry_topic("RETRY_GROUP_TOPIC_PREFIXmy_topic"),
            false
        );
        assert_eq!(
            NamespaceUtil::is_retry_topic("%RETRY%RETRY_GROUP_TOPIC_PREFIXmy_topic"),
            true
        );
    }

    #[test]
    fn is_dlq_topic_returns_false_when_empty() {
        assert_eq!(NamespaceUtil::is_dlq_topic(""), false);
    }

    #[test]
    fn is_dlq_topic_returns_true_when_dlq_topic() {
        assert_eq!(
            NamespaceUtil::is_dlq_topic("DLQ_GROUP_TOPIC_PREFIXmy_topic"),
            false
        );
        assert_eq!(
            NamespaceUtil::is_dlq_topic("%DLQ%DLQ_GROUP_TOPIC_PREFIXmy_topic"),
            true
        );
    }

    #[test]
    fn test_remoting_command_type() {
        // Test RemotingCommandType::value_of
        assert_eq!(
            Some(RemotingCommandType::REQUEST),
            RemotingCommandType::value_of(0)
        );
        assert_eq!(
            Some(RemotingCommandType::RESPONSE),
            RemotingCommandType::value_of(1)
        );
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
        assert_eq!(
            Some(LanguageCode::JAVA),
            LanguageCode::get_code_from_name("JAVA")
        );
        assert_eq!(
            Some(LanguageCode::CPP),
            LanguageCode::get_code_from_name("CPP")
        );
        assert_eq!(
            Some(LanguageCode::DOTNET),
            LanguageCode::get_code_from_name("DOTNET")
        );
    }

    #[cfg(test)]
    mod tests {
        use std::sync::atomic::Ordering;

        use super::*;

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
            assert_eq!(
                initial_counter + 1,
                data_version.counter.load(Ordering::SeqCst)
            );
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
            assert_eq!(
                initial_counter + 1,
                data_version.counter.load(Ordering::SeqCst)
            );
        }

        #[test]
        fn data_version_next_version_with_state() {
            let mut data_version = DataVersion::new();
            let initial_timestamp = data_version.timestamp;
            let initial_counter = data_version.counter.load(Ordering::SeqCst);
            data_version.next_version_with(20);
            assert_eq!(20, data_version.state_version);
            assert!(data_version.timestamp == initial_timestamp);
            assert_eq!(
                initial_counter + 1,
                data_version.counter.load(Ordering::SeqCst)
            );
        }
    }
}
