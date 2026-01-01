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

use std::fmt::Display;

use rocketmq_rust::WeakArcMut;
use tokio::net::TcpStream;
use uuid::Uuid;

use crate::ha::general_ha_connection::GeneralHAConnection;
use crate::ha::ha_connection_state::HAConnectionState;
use crate::ha::HAConnectionError;

#[trait_variant::make(HAConnection: Send)]
pub trait RocketmqHAConnection: Sync {
    /// Start the HA connection
    ///
    /// This initiates the connection threads and begins processing.
    async fn start(&mut self, conn: WeakArcMut<GeneralHAConnection>) -> Result<(), HAConnectionError>;

    /// Shutdown the HA connection gracefully
    ///
    /// This initiates a clean shutdown of the connection.
    async fn shutdown(&mut self);

    /// Close the HA connection immediately
    ///
    /// This forcibly closes the connection without waiting for pending operations.
    fn close(&self);

    /// Get the underlying TCP stream
    ///
    /// # Returns
    /// Reference to the TCP stream for this connection
    fn get_socket(&self) -> &TcpStream;

    /// Get the current state of the connection
    ///
    /// # Returns
    /// Current connection state
    async fn get_current_state(&self) -> HAConnectionState;

    /// Get the client address for this connection
    ///
    /// # Returns
    /// Socket address of the connected client
    fn get_client_address(&self) -> &str;

    /// Get the data transfer rate per second
    ///
    /// # Returns
    /// Number of bytes transferred per second
    fn get_transferred_byte_in_second(&self) -> i64;

    /// Get the current transfer offset to the slave
    ///
    /// # Returns
    /// Current offset being transferred
    fn get_transfer_from_where(&self) -> i64;

    /// Get the latest offset acknowledged by the slave
    ///
    /// # Returns
    /// The latest offset confirmed by the slave
    fn get_slave_ack_offset(&self) -> i64;

    /// Get the unique identifier for the HA connection.
    ///
    /// This function returns a reference to the `HAConnectionId` instance,
    /// which uniquely identifies the high availability connection.
    ///
    /// # Returns
    /// A reference to the `HAConnectionId` instance.
    fn get_ha_connection_id(&self) -> &HAConnectionId;

    /// Get the remote address of the connection
    ///
    /// # Returns
    /// Remote socket address as a string
    fn remote_address(&self) -> String;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct HAConnectionId {
    id: String,
    random: u64,
}

impl Default for HAConnectionId {
    fn default() -> Self {
        HAConnectionId {
            id: Uuid::new_v4().urn().to_string(),
            random: rand::random::<u64>(),
        }
    }
}

impl HAConnectionId {
    pub fn new(id: String, random: u64) -> Self {
        HAConnectionId { id, random }
    }

    pub fn get_id(&self) -> &str {
        &self.id
    }

    pub fn get_random(&self) -> u64 {
        self.random
    }
}

impl Display for HAConnectionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.id, self.random)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn test_ha_connection_id_default() {
        let id1 = HAConnectionId::default();
        let id2 = HAConnectionId::default();

        // IDs should be different
        assert_ne!(id1, id2);

        // ID should be a valid UUID URN format
        assert!(id1.get_id().starts_with("urn:uuid:"));
        assert!(id2.get_id().starts_with("urn:uuid:"));

        // Random values should be different (with very high probability)
        assert_ne!(id1.get_random(), id2.get_random());
    }

    #[test]
    fn test_ha_connection_id_new() {
        let custom_id = "custom-test-id".to_string();
        let custom_random = 12345u64;

        let connection_id = HAConnectionId::new(custom_id.clone(), custom_random);

        assert_eq!(connection_id.get_id(), &custom_id);
        assert_eq!(connection_id.get_random(), custom_random);
    }

    #[test]
    fn test_ha_connection_id_getters() {
        let test_id = "test-id-123".to_string();
        let test_random = 98765u64;

        let connection_id = HAConnectionId::new(test_id.clone(), test_random);

        assert_eq!(connection_id.get_id(), test_id.as_str());
        assert_eq!(connection_id.get_random(), test_random);
    }

    #[test]
    fn test_ha_connection_id_display() {
        let test_id = "test-display-id".to_string();
        let test_random = 555u64;

        let connection_id = HAConnectionId::new(test_id.clone(), test_random);
        let display_string = format!("{}", connection_id);

        assert_eq!(display_string, "test-display-id-555");
    }

    #[test]
    fn test_ha_connection_id_equality() {
        let id1 = HAConnectionId::new("same-id".to_string(), 123);
        let id2 = HAConnectionId::new("same-id".to_string(), 123);
        let id3 = HAConnectionId::new("different-id".to_string(), 123);
        let id4 = HAConnectionId::new("same-id".to_string(), 456);

        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
        assert_ne!(id1, id4);
    }

    #[test]
    fn test_ha_connection_id_hash() {
        let id1 = HAConnectionId::new("hash-test-1".to_string(), 111);
        let id2 = HAConnectionId::new("hash-test-2".to_string(), 222);

        let mut map = HashMap::new();
        map.insert(id1.clone(), "value1");
        map.insert(id2.clone(), "value2");

        assert_eq!(map.get(&id1), Some(&"value1"));
        assert_eq!(map.get(&id2), Some(&"value2"));
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn test_ha_connection_id_clone() {
        let original = HAConnectionId::new("clone-test".to_string(), 999);
        let cloned = original.clone();

        assert_eq!(original, cloned);
        assert_eq!(original.get_id(), cloned.get_id());
        assert_eq!(original.get_random(), cloned.get_random());
    }

    #[test]
    fn test_ha_connection_id_debug() {
        let connection_id = HAConnectionId::new("debug-test".to_string(), 777);
        let debug_string = format!("{:?}", connection_id);

        assert!(debug_string.contains("debug-test"));
        assert!(debug_string.contains("777"));
        assert!(debug_string.contains("HAConnectionId"));
    }

    #[test]
    fn test_ha_connection_id_edge_cases() {
        // Test with empty string
        let empty_id = HAConnectionId::new("".to_string(), 0);
        assert_eq!(empty_id.get_id(), "");
        assert_eq!(empty_id.get_random(), 0);
        assert_eq!(format!("{}", empty_id), "-0");

        // Test with maximum u64
        let max_random = HAConnectionId::new("max-test".to_string(), u64::MAX);
        assert_eq!(max_random.get_random(), u64::MAX);

        // Test with special characters in id
        let special_id = HAConnectionId::new("test-id-with-special-chars!@#$%".to_string(), 42);
        assert_eq!(special_id.get_id(), "test-id-with-special-chars!@#$%");
    }
}
