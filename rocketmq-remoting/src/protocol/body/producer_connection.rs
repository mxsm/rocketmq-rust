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

use std::collections::HashSet;

use serde::Deserialize;
use serde::Serialize;

use crate::protocol::body::connection::Connection;

#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub struct ProducerConnection {
    connection_set: HashSet<Connection>,
}
impl ProducerConnection {
    pub fn new() -> Self {
        Self {
            connection_set: HashSet::new(),
        }
    }
    pub fn connection_set_mut(&mut self) -> &mut HashSet<Connection> {
        &mut self.connection_set
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use serde_json;

    use super::*;
    use crate::protocol::body::connection::Connection;

    #[test]
    fn producer_connection_default_values() {
        let connection = ProducerConnection::default();
        assert!(connection.connection_set.is_empty());
    }

    #[test]
    fn producer_connection_with_connections() {
        let mut connection_set = HashSet::new();
        connection_set.insert(Connection::default());
        let connection = ProducerConnection { connection_set };
        assert_eq!(connection.connection_set.len(), 1);
    }

    #[test]
    fn serialize_producer_connection() {
        let mut connection_set = HashSet::new();
        connection_set.insert(Connection::default());
        let connection = ProducerConnection { connection_set };
        let serialized = serde_json::to_string(&connection).unwrap();
        assert!(serialized.contains("\"connectionSet\":["));
    }

    #[test]
    fn deserialize_producer_connection_empty_set() {
        let json = r#"{"connectionSet":[]}"#;
        let deserialized: ProducerConnection = serde_json::from_str(json).unwrap();
        assert!(deserialized.connection_set.is_empty());
    }
}
