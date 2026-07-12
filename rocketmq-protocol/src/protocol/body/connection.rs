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

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::LanguageCode;

#[derive(Deserialize, Serialize, Debug, Clone, Eq, PartialEq, Hash, Default)]
#[serde(rename_all = "camelCase")]
pub struct Connection {
    client_id: CheetahString,
    client_addr: CheetahString,
    language: LanguageCode,
    version: i32,
}

impl Connection {
    pub fn new() -> Self {
        Connection {
            client_id: CheetahString::default(),
            client_addr: CheetahString::default(),
            language: LanguageCode::default(),
            version: 0,
        }
    }
}

impl Connection {
    pub fn get_client_id(&self) -> CheetahString {
        self.client_id.clone()
    }

    pub fn set_client_id(&mut self, client_id: CheetahString) {
        self.client_id = client_id;
    }

    pub fn get_client_addr(&self) -> CheetahString {
        self.client_addr.clone()
    }

    pub fn set_client_addr(&mut self, client_addr: CheetahString) {
        self.client_addr = client_addr;
    }

    pub fn get_language(&self) -> LanguageCode {
        self.language
    }

    pub fn set_language(&mut self, language: LanguageCode) {
        self.language = language;
    }

    pub fn get_version(&self) -> i32 {
        self.version
    }

    pub fn set_version(&mut self, version: i32) {
        self.version = version;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hash;
    use std::hash::Hasher;

    #[test]
    fn connection_default_and_new() {
        let conn = Connection::default();
        assert!(conn.get_client_id().is_empty());
        assert!(conn.get_client_addr().is_empty());
        assert_eq!(conn.get_language(), LanguageCode::default());
        assert_eq!(conn.get_version(), 0);

        let conn = Connection::new();
        assert!(conn.get_client_id().is_empty());
        assert!(conn.get_client_addr().is_empty());
        assert_eq!(conn.get_language(), LanguageCode::default());
        assert_eq!(conn.get_version(), 0);
    }

    #[test]
    fn connection_getters_setters() {
        let mut conn = Connection::new();
        let client_id = CheetahString::from("client_id");
        let client_addr = CheetahString::from("127.0.0.1");
        let language = LanguageCode::JAVA;
        let version = 1;

        conn.set_client_id(client_id.clone());
        conn.set_client_addr(client_addr.clone());
        conn.set_language(language);
        conn.set_version(version);

        assert_eq!(conn.get_client_id(), client_id);
        assert_eq!(conn.get_client_addr(), client_addr);
        assert_eq!(conn.get_language(), language);
        assert_eq!(conn.get_version(), version);
    }

    #[test]
    fn connection_clone_eq_hash() {
        let mut conn1 = Connection::new();
        conn1.set_client_id(CheetahString::from("id"));
        let conn2 = conn1.clone();

        assert_eq!(conn1, conn2);

        let mut hasher1 = DefaultHasher::new();
        conn1.hash(&mut hasher1);
        let hash1 = hasher1.finish();

        let mut hasher2 = DefaultHasher::new();
        conn2.hash(&mut hasher2);
        let hash2 = hasher2.finish();

        assert_eq!(hash1, hash2);
    }

    #[test]
    fn connection_serde() {
        let mut conn = Connection::new();
        conn.set_client_id(CheetahString::from("id"));
        conn.set_client_addr(CheetahString::from("addr"));
        conn.set_language(LanguageCode::RUST);
        conn.set_version(100);

        let json = serde_json::to_string(&conn).unwrap();
        assert!(json.contains("\"clientId\":\"id\""));
        assert!(json.contains("\"clientAddr\":\"addr\""));
        assert!(json.contains("\"language\":\"RUST\""));
        assert!(json.contains("\"version\":100"));

        let deserialized: Connection = serde_json::from_str(&json).unwrap();
        assert_eq!(conn, deserialized);
    }
}
