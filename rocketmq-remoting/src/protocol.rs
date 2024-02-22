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
use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
    sync::atomic::{AtomicI64, Ordering},
    time::SystemTime,
};

use rocketmq_common::common::{mix_all, topic::TopicValidator};
use serde::{de, Deserialize, Serialize};

use crate::RocketMQSerializable;

pub mod body;
pub mod command_custom_header;
pub mod header;
pub mod namesrv;
pub mod remoting_command;
pub mod rocketmq_serializable;
pub mod route;
pub mod static_topic;

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
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Deserialize, Serialize)]
pub struct DataVersion {
    #[serde(rename = "stateVersion")]
    state_version: i64,
    timestamp: i64,
    counter: i64,
    #[serde(skip)]
    counter_inner: AtomicI64,
}

impl RemotingSerializable for DataVersion {
    type Output = DataVersion;
}

impl Clone for DataVersion {
    fn clone(&self) -> Self {
        DataVersion {
            state_version: self.state_version,
            timestamp: self.timestamp,
            counter: self.counter,
            counter_inner: AtomicI64::new(self.counter_inner.load(Ordering::Relaxed)),
        }
    }
}

impl PartialEq for DataVersion {
    fn eq(&self, other: &Self) -> bool {
        self.state_version == other.state_version
            && self.timestamp == other.timestamp
            && self.counter == other.counter
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
            counter: 0,
            counter_inner: AtomicI64::new(0),
        }
    }
    fn get_state_version(&self) -> i64 {
        self.state_version
    }
    fn set_state_version(&mut self, state_version: i64) {
        self.state_version = state_version;
    }
    fn get_timestamp(&self) -> i64 {
        self.timestamp
    }

    fn get_counter(&self) -> i64 {
        self.counter_inner.load(Ordering::Relaxed)
    }

    fn increment_counter(&self) -> i64 {
        self.counter_inner.fetch_add(1, Ordering::Relaxed)
    }
    pub fn state_version(&self) -> i64 {
        self.state_version
    }
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }
    pub fn counter(&self) -> i64 {
        self.counter
    }
    pub fn counter_inner(&self) -> &AtomicI64 {
        &self.counter_inner
    }
}

impl Display for DataVersion {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let counter_value = self.counter_inner.load(Ordering::SeqCst);
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
}

pub trait FastCodesHeader {
    fn write_if_not_null(&mut self, out: &mut bytes::BytesMut, key: &str, value: &str) {
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
    pub const NAMESPACE_SEPARATOR: char = '%';
    pub const STRING_BLANK: &'static str = "";
    pub const RETRY_PREFIX_LENGTH: usize = mix_all::RETRY_GROUP_TOPIC_PREFIX.len();
    pub const DLQ_PREFIX_LENGTH: usize = mix_all::DLQ_GROUP_TOPIC_PREFIX.len();

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
}
