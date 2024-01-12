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
    fmt::{Display, Formatter},
    sync::atomic::{AtomicI64, Ordering},
    time::SystemTime,
};

use serde::{Deserialize, Serialize};

pub mod body;
pub mod command_custom_header;
pub mod header;
pub mod namesrv;
pub mod remoting_command;
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
    fn new() -> Self {
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
    fn decode(bytes: &[u8]) -> Self::Output;

    fn encode(&self, compress: bool) -> Vec<u8>;
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
