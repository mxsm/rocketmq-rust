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

use serde::Deserialize;
use serde::Serialize;

use crate::protocol::body::producer_info::ProducerInfo;

/// Producer information table
///
/// A collection of producer information organized by group.
/// This structure maps producer group names to lists of producer information objects.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProducerTableInfo {
    /// Mapping from producer group names to lists of producer information
    data: HashMap<String, Vec<ProducerInfo>>,
}

impl ProducerTableInfo {
    /// Create a new ProducerTableInfo
    ///
    /// # Parameters
    /// * `data` - Map of producer groups to lists of producer information
    pub fn new(data: HashMap<String, Vec<ProducerInfo>>) -> Self {
        Self { data }
    }

    /// Get the producer table data
    ///
    /// # Returns
    /// Reference to the internal map of producer groups to producer information
    pub fn data(&self) -> &HashMap<String, Vec<ProducerInfo>> {
        &self.data
    }

    /// Get mutable access to the producer table data
    ///
    /// # Returns
    /// Mutable reference to the internal map of producer groups to producer information
    pub fn data_mut(&mut self) -> &mut HashMap<String, Vec<ProducerInfo>> {
        &mut self.data
    }

    /// Set the producer table data
    ///
    /// # Parameters
    /// * `data` - New map of producer groups to lists of producer information
    pub fn set_data(&mut self, data: HashMap<String, Vec<ProducerInfo>>) {
        self.data = data;
    }
}

/// Default implementation that creates an empty producer table
impl Default for ProducerTableInfo {
    fn default() -> Self {
        Self { data: HashMap::new() }
    }
}

/// Implementation of the From trait to convert from a HashMap
impl From<HashMap<String, Vec<ProducerInfo>>> for ProducerTableInfo {
    fn from(data: HashMap<String, Vec<ProducerInfo>>) -> Self {
        Self::new(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::LanguageCode;
    use crate::protocol::RemotingDeserializable;
    use crate::protocol::RemotingSerializable;

    #[test]
    fn test_producer_table_info() {
        let mut table = ProducerTableInfo::default();
        assert!(table.data().is_empty());

        // Create some test data
        let producer1 = ProducerInfo::new("producer1", "192.168.1.1", LanguageCode::RUST, 1, 100);

        let producer2 = ProducerInfo::new("producer2", "192.168.1.2", LanguageCode::JAVA, 2, 200);

        let mut data = HashMap::new();
        data.insert("group1".to_string(), vec![producer1.clone()]);
        data.insert("group2".to_string(), vec![producer2.clone()]);

        // Test setters
        table.set_data(data);
        assert_eq!(table.data().len(), 2);
        assert!(table.data().contains_key("group1"));
        assert!(table.data().contains_key("group2"));

        // Test JSON serialization/deserialization
        let json = table.serialize_json().unwrap();
        let deserialized = ProducerTableInfo::decode(json.as_bytes()).unwrap();

        assert_eq!(deserialized.data().len(), 2);
        assert!(deserialized.data().contains_key("group1"));
        assert!(deserialized.data().contains_key("group2"));

        // Verify producer info was preserved
        let group1_producers = &deserialized.data()["group1"];
        assert_eq!(group1_producers.len(), 1);
        assert_eq!(group1_producers[0].client_id(), "producer1");

        // Test the From trait implementation
        let mut new_data = HashMap::new();
        new_data.insert("group3".to_string(), vec![producer1.clone(), producer2.clone()]);
        let from_table = ProducerTableInfo::from(new_data);
        assert_eq!(from_table.data().len(), 1);
        assert_eq!(from_table.data()["group3"].len(), 2);
    }
}
