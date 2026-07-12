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

use std::fmt;

use serde::Deserialize;
use serde::Serialize;

use crate::protocol::LanguageCode;

/// Information about a producer client connected to a broker.
///
/// This structure contains metadata about a producer, such as its identity,
/// network location, programming language, client version, and last heartbeat time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProducerInfo {
    /// Unique identifier for the producer client
    client_id: String,

    /// IP address of the producer client
    remote_ip: String,

    /// Programming language of the producer client SDK
    language: LanguageCode,

    /// Version of the producer client SDK
    version: i32,

    /// Last time this producer information was updated (timestamp in milliseconds)
    last_update_timestamp: i64,
}

impl ProducerInfo {
    /// Create a new ProducerInfo instance
    ///
    /// # Parameters
    /// * `client_id` - Unique identifier for the producer client
    /// * `remote_ip` - IP address of the producer client
    /// * `language` - Programming language of the producer client SDK
    /// * `version` - Version of the producer client SDK
    /// * `last_update_timestamp` - Last time this producer was updated (timestamp in milliseconds)
    pub fn new(
        client_id: impl Into<String>,
        remote_ip: impl Into<String>,
        language: LanguageCode,
        version: i32,
        last_update_timestamp: i64,
    ) -> Self {
        Self {
            client_id: client_id.into(),
            remote_ip: remote_ip.into(),
            language,
            version,
            last_update_timestamp,
        }
    }

    /// Get the client ID
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Set the client ID
    pub fn set_client_id(&mut self, client_id: impl Into<String>) {
        self.client_id = client_id.into();
    }

    /// Get the remote IP address
    pub fn remote_ip(&self) -> &str {
        &self.remote_ip
    }

    /// Set the remote IP address
    pub fn set_remote_ip(&mut self, remote_ip: impl Into<String>) {
        self.remote_ip = remote_ip.into();
    }

    /// Get the programming language of the client SDK
    pub fn language(&self) -> LanguageCode {
        self.language
    }

    /// Set the programming language of the client SDK
    pub fn set_language(&mut self, language: LanguageCode) {
        self.language = language;
    }

    /// Get the client SDK version
    pub fn version(&self) -> i32 {
        self.version
    }

    /// Set the client SDK version
    pub fn set_version(&mut self, version: i32) {
        self.version = version;
    }

    /// Get the last update timestamp
    pub fn last_update_timestamp(&self) -> i64 {
        self.last_update_timestamp
    }

    /// Set the last update timestamp
    pub fn set_last_update_timestamp(&mut self, timestamp: i64) {
        self.last_update_timestamp = timestamp;
    }
}

impl fmt::Display for ProducerInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "clientId={}, remoteIP={}, language={:?}, version={}, lastUpdateTimestamp={}",
            self.client_id, self.remote_ip, self.language, self.version, self.last_update_timestamp
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::LanguageCode;

    #[test]
    fn producer_info_display_format() {
        let producer_info = ProducerInfo::new("client123", "192.168.1.1", LanguageCode::RUST, 1, 1633024800000);
        assert_eq!(
            producer_info.to_string(),
            "clientId=client123, remoteIP=192.168.1.1, language=RUST, version=1, lastUpdateTimestamp=1633024800000"
        );
    }

    #[test]
    fn producer_info_update_client_id() {
        let mut producer_info = ProducerInfo::new("client123", "192.168.1.1", LanguageCode::RUST, 1, 1633024800000);
        producer_info.set_client_id("new_client_id");
        assert_eq!(producer_info.client_id(), "new_client_id");
    }

    #[test]
    fn producer_info_update_remote_ip() {
        let mut producer_info = ProducerInfo::new("client123", "192.168.1.1", LanguageCode::RUST, 1, 1633024800000);
        producer_info.set_remote_ip("10.0.0.1");
        assert_eq!(producer_info.remote_ip(), "10.0.0.1");
    }

    #[test]
    fn producer_info_update_language() {
        let mut producer_info = ProducerInfo::new("client123", "192.168.1.1", LanguageCode::RUST, 1, 1633024800000);
        producer_info.set_language(LanguageCode::JAVA);
        assert_eq!(producer_info.language(), LanguageCode::JAVA);
    }

    #[test]
    fn producer_info_update_version() {
        let mut producer_info = ProducerInfo::new("client123", "192.168.1.1", LanguageCode::RUST, 1, 1633024800000);
        producer_info.set_version(2);
        assert_eq!(producer_info.version(), 2);
    }

    #[test]
    fn producer_info_update_last_update_timestamp() {
        let mut producer_info = ProducerInfo::new("client123", "192.168.1.1", LanguageCode::RUST, 1, 1633024800000);
        producer_info.set_last_update_timestamp(1633024900000);
        assert_eq!(producer_info.last_update_timestamp(), 1633024900000);
    }
}
