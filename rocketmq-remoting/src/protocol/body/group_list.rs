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

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

#[derive(Deserialize, Serialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct GroupList {
    #[serde(alias = "group_list")]
    pub group_list: HashSet<CheetahString>,
}

impl GroupList {
    pub fn new(group_list: HashSet<CheetahString>) -> Self {
        Self { group_list }
    }

    pub fn get_group_list(&self) -> &HashSet<CheetahString> {
        &self.group_list
    }

    pub fn set_group_list(&mut self, group_list: HashSet<CheetahString>) {
        self.group_list = group_list;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_list_serializes_with_java_field_name() {
        let group_list = GroupList::new(HashSet::from([CheetahString::from_static_str("consumer_group_a")]));

        let json = serde_json::to_string(&group_list).expect("serialize GroupList");

        assert!(json.contains("\"groupList\""));
        assert!(!json.contains("\"group_list\""));
    }

    #[test]
    fn group_list_deserializes_java_and_legacy_field_names() {
        let java_json = r#"{"groupList":["consumer_group_a"]}"#;
        let legacy_json = r#"{"group_list":["consumer_group_b"]}"#;

        let java_group_list: GroupList = serde_json::from_str(java_json).expect("deserialize Java GroupList");
        let legacy_group_list: GroupList = serde_json::from_str(legacy_json).expect("deserialize legacy GroupList");

        assert!(java_group_list
            .get_group_list()
            .contains(&CheetahString::from_static_str("consumer_group_a")));
        assert!(legacy_group_list
            .get_group_list()
            .contains(&CheetahString::from_static_str("consumer_group_b")));
    }
}
