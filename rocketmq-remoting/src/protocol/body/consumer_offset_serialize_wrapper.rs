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

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::DataVersion;

#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerOffsetSerializeWrapper {
    data_version: DataVersion,
    // Pop mode offset table
    offset_table: HashMap<CheetahString /* topic@group */, HashMap<i32 /* queue id */, i64>>,
}

impl ConsumerOffsetSerializeWrapper {
    /// Returns a reference to the data_version field.
    pub fn data_version(&self) -> &DataVersion {
        &self.data_version
    }
    /// Sets the data_version field.
    pub fn set_data_version(&mut self, data_version: DataVersion) {
        self.data_version = data_version;
    }
    /// Returns a reference to the offset_table field.
    pub fn offset_table_ref(&self) -> &HashMap<CheetahString, HashMap<i32, i64>> {
        &self.offset_table
    }
    pub fn offset_table(self) -> HashMap<CheetahString, HashMap<i32, i64>> {
        self.offset_table
    }
    /// Returns a mutable reference to the offset_table field.
    pub fn offset_table_mut(&mut self) -> &mut HashMap<CheetahString, HashMap<i32, i64>> {
        &mut self.offset_table
    }
    /// Sets the offset_table field.
    pub fn set_offset_table(&mut self, offset_table: HashMap<CheetahString, HashMap<i32, i64>>) {
        self.offset_table = offset_table;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn test_consumer_offset_serialize_wrapper_insert_and_retrieve() {
        let mut wrapper = ConsumerOffsetSerializeWrapper::default();

        let topic_group = CheetahString::from("test_topic@test_group");
        let mut queue_offsets = HashMap::new();
        queue_offsets.insert(0, 100i64);
        queue_offsets.insert(1, 200i64);

        wrapper.offset_table.insert(topic_group.clone(), queue_offsets);

        assert_eq!(wrapper.offset_table.len(), 1);
        assert!(wrapper.offset_table.contains_key(&topic_group));

        let retrieved_offsets = wrapper.offset_table.get(&topic_group).unwrap();
        assert_eq!(retrieved_offsets.get(&0), Some(&100i64));
        assert_eq!(retrieved_offsets.get(&1), Some(&200i64));
    }
}
