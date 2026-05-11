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

use std::hash::Hash;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

use crate::migration::alc::plain_access_config::PlainAccessConfig;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct PlainAccessData {
    #[serde(alias = "global_white_remote_addresses")]
    pub global_white_remote_addresses: Vec<CheetahString>,
    pub accounts: Vec<PlainAccessConfig>,
    #[serde(alias = "data_version")]
    pub data_version: Vec<DataVersion>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub struct DataVersion {
    pub timestamp: u64,
    pub counter: u64,
}

impl PlainAccessData {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn global_white_remote_addresses(&self) -> &[CheetahString] {
        &self.global_white_remote_addresses
    }

    pub fn set_global_white_remote_addresses(&mut self, addrs: Vec<CheetahString>) {
        self.global_white_remote_addresses = addrs;
    }

    pub fn accounts(&self) -> &[PlainAccessConfig] {
        &self.accounts
    }

    pub fn set_accounts(&mut self, accounts: Vec<PlainAccessConfig>) {
        self.accounts = accounts;
    }

    pub fn data_version(&self) -> &[DataVersion] {
        &self.data_version
    }

    pub fn set_data_version(&mut self, versions: Vec<DataVersion>) {
        self.data_version = versions;
    }
}

impl PlainAccessData {
    pub fn has_changed(&self, other: &Self) -> bool {
        self.data_version != other.data_version
    }

    pub fn latest_version(&self) -> Option<&DataVersion> {
        self.data_version.last()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_access_data_default_and_new() {
        let data = PlainAccessData::default();
        assert!(data.global_white_remote_addresses().is_empty());
        assert!(data.accounts().is_empty());
        assert!(data.data_version().is_empty());

        let data = PlainAccessData::new();
        assert!(data.global_white_remote_addresses().is_empty());
        assert!(data.accounts().is_empty());
        assert!(data.data_version().is_empty());
    }

    #[test]
    fn plain_access_data_setters_and_getters() {
        let mut data = PlainAccessData::new();
        let addrs = vec![CheetahString::from("127.0.0.1")];
        data.set_global_white_remote_addresses(addrs.clone());
        assert_eq!(data.global_white_remote_addresses(), addrs.as_slice());

        let accounts = vec![PlainAccessConfig::default()];
        data.set_accounts(accounts.clone());
        assert_eq!(data.accounts(), accounts.as_slice());

        let versions = vec![DataVersion {
            timestamp: 100,
            counter: 1,
        }];
        data.set_data_version(versions.clone());
        assert_eq!(data.data_version(), versions.as_slice());
    }

    #[test]
    fn plain_access_data_has_changed() {
        let mut data1 = PlainAccessData::new();
        let data2 = PlainAccessData::new();
        assert!(!data1.has_changed(&data2));

        data1.set_data_version(vec![DataVersion {
            timestamp: 100,
            counter: 1,
        }]);
        assert!(data1.has_changed(&data2));
    }

    #[test]
    fn plain_access_data_latest_version() {
        let mut data = PlainAccessData::new();
        assert!(data.latest_version().is_none());

        let version = DataVersion {
            timestamp: 100,
            counter: 1,
        };
        data.set_data_version(vec![version]);
        assert_eq!(data.latest_version(), Some(&version));
    }

    #[test]
    fn plain_access_data_serialization_and_deserialization() {
        let mut data = PlainAccessData::new();
        data.set_global_white_remote_addresses(vec![CheetahString::from("127.0.0.1")]);
        let json = serde_json::to_string(&data).unwrap();
        let deserialized: PlainAccessData = serde_json::from_str(&json).unwrap();
        assert_eq!(data, deserialized);
    }

    #[test]
    fn plain_access_data_deserializes_java_camel_case_yaml() {
        let yaml = r#"
globalWhiteRemoteAddresses:
  - 10.10.*.*
accounts:
  - accessKey: RocketMQ
    secretKey: 12345678
    whiteRemoteAddress: 192.168.0.*
    admin: true
    defaultTopicPerm: PUB|SUB
    defaultGroupPerm: SUB
    topicPerms:
      - TopicA=PUB
    groupPerms:
      - GroupA=SUB
dataVersion:
  - timestamp: 1
    counter: 2
"#;

        let data: PlainAccessData = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(data.global_white_remote_addresses()[0].as_str(), "10.10.*.*");
        assert_eq!(data.accounts().len(), 1);
        let account = &data.accounts()[0];
        assert_eq!(account.access_key().unwrap().as_str(), "RocketMQ");
        assert_eq!(account.secret_key().unwrap().as_str(), "12345678");
        assert_eq!(account.white_remote_address().unwrap().as_str(), "192.168.0.*");
        assert!(account.is_admin());
        assert_eq!(account.default_topic_perm().unwrap().as_str(), "PUB|SUB");
        assert_eq!(account.default_group_perm().unwrap().as_str(), "SUB");
        assert_eq!(account.topic_perms().unwrap()[0].as_str(), "TopicA=PUB");
        assert_eq!(account.group_perms().unwrap()[0].as_str(), "GroupA=SUB");
        assert_eq!(
            data.data_version(),
            &[DataVersion {
                timestamp: 1,
                counter: 2
            }]
        );
    }

    #[test]
    fn plain_access_data_keeps_snake_case_yaml_compatibility() {
        let yaml = r#"
global_white_remote_addresses:
  - 127.0.0.1
accounts:
  - access_key: ak
    secret_key: sk
data_version:
  - timestamp: 3
    counter: 4
"#;

        let data: PlainAccessData = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(data.global_white_remote_addresses()[0].as_str(), "127.0.0.1");
        assert_eq!(data.accounts()[0].access_key().unwrap().as_str(), "ak");
        assert_eq!(data.accounts()[0].secret_key().unwrap().as_str(), "sk");
        assert_eq!(
            data.data_version(),
            &[DataVersion {
                timestamp: 3,
                counter: 4
            }]
        );
    }
}
