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

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Clone, Debug, Default, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PlainAccessConfig {
    pub access_key: Option<CheetahString>,
    pub secret_key: Option<CheetahString>,
    pub white_remote_address: Option<CheetahString>,
    pub admin: bool,
    pub default_topic_perm: Option<CheetahString>,
    pub default_group_perm: Option<CheetahString>,
    pub topic_perms: Vec<CheetahString>,
    pub group_perms: Vec<CheetahString>,
}

impl Display for PlainAccessConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PlainAccessConfig {{ access_key: {:?}, secret_key: {:?}, white_remote_address: {:?}, admin: {}, \
             default_topic_perm: {:?}, default_group_perm: {:?}, topic_perms: {:?}, group_perms: {:?} }}",
            self.access_key,
            self.secret_key,
            self.white_remote_address,
            self.admin,
            self.default_topic_perm,
            self.default_group_perm,
            self.topic_perms,
            self.group_perms
        )
    }
}

#[cfg(test)]
mod tests {
    use serde_json;

    use super::*;

    #[test]
    fn plain_access_config_default_values() {
        let config = PlainAccessConfig {
            access_key: None,
            secret_key: None,
            white_remote_address: None,
            admin: false,
            default_topic_perm: None,
            default_group_perm: None,
            topic_perms: Vec::new(),
            group_perms: Vec::new(),
        };
        assert!(config.access_key.is_none());
        assert!(config.secret_key.is_none());
        assert!(config.white_remote_address.is_none());
        assert!(!config.admin);
        assert!(config.default_topic_perm.is_none());
        assert!(config.default_group_perm.is_none());
        assert!(config.topic_perms.is_empty());
        assert!(config.group_perms.is_empty());
    }

    #[test]
    fn plain_access_config_equality() {
        let config1 = PlainAccessConfig {
            access_key: Some(CheetahString::from("key1")),
            secret_key: Some(CheetahString::from("secret1")),
            white_remote_address: Some(CheetahString::from("address1")),
            admin: true,
            default_topic_perm: Some(CheetahString::from("perm1")),
            default_group_perm: Some(CheetahString::from("perm2")),
            topic_perms: vec![CheetahString::from("topic1")],
            group_perms: vec![CheetahString::from("group1")],
        };

        let config2 = PlainAccessConfig {
            access_key: Some(CheetahString::from("key1")),
            secret_key: Some(CheetahString::from("secret1")),
            white_remote_address: Some(CheetahString::from("address1")),
            admin: true,
            default_topic_perm: Some(CheetahString::from("perm1")),
            default_group_perm: Some(CheetahString::from("perm2")),
            topic_perms: vec![CheetahString::from("topic1")],
            group_perms: vec![CheetahString::from("group1")],
        };

        assert_eq!(config1, config2);
    }

    #[test]
    fn plain_access_config_inequality() {
        let config1 = PlainAccessConfig {
            access_key: Some(CheetahString::from("key1")),
            secret_key: Some(CheetahString::from("secret1")),
            white_remote_address: Some(CheetahString::from("address1")),
            admin: true,
            default_topic_perm: Some(CheetahString::from("perm1")),
            default_group_perm: Some(CheetahString::from("perm2")),
            topic_perms: vec![CheetahString::from("topic1")],
            group_perms: vec![CheetahString::from("group1")],
        };

        let config2 = PlainAccessConfig {
            access_key: Some(CheetahString::from("key2")),
            secret_key: Some(CheetahString::from("secret2")),
            white_remote_address: Some(CheetahString::from("address2")),
            admin: false,
            default_topic_perm: Some(CheetahString::from("perm3")),
            default_group_perm: Some(CheetahString::from("perm4")),
            topic_perms: vec![CheetahString::from("topic2")],
            group_perms: vec![CheetahString::from("group2")],
        };

        assert_ne!(config1, config2);
    }

    #[test]
    fn serialize_plain_access_config() {
        let config = PlainAccessConfig {
            access_key: Some(CheetahString::from("key1")),
            secret_key: Some(CheetahString::from("secret1")),
            white_remote_address: Some(CheetahString::from("address1")),
            admin: true,
            default_topic_perm: Some(CheetahString::from("perm1")),
            default_group_perm: Some(CheetahString::from("perm2")),
            topic_perms: vec![CheetahString::from("topic1")],
            group_perms: vec![CheetahString::from("group1")],
        };
        let serialized = serde_json::to_string(&config).unwrap();
        assert_eq!(
            serialized,
            r#"{"accessKey":"key1","secretKey":"secret1","whiteRemoteAddress":"address1","admin":true,"defaultTopicPerm":"perm1","defaultGroupPerm":"perm2","topicPerms":["topic1"],"groupPerms":["group1"]}"#
        );
    }

    #[test]
    fn deserialize_plain_access_config() {
        let json = r#"{"accessKey":"key1","secretKey":"secret1","whiteRemoteAddress":"address1","admin":true,"defaultTopicPerm":"perm1","defaultGroupPerm":"perm2","topicPerms":["topic1"],"groupPerms":["group1"]}"#;
        let deserialized: PlainAccessConfig = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized.access_key, Some(CheetahString::from("key1")));
        assert_eq!(deserialized.secret_key, Some(CheetahString::from("secret1")));
        assert_eq!(deserialized.white_remote_address, Some(CheetahString::from("address1")));
        assert!(deserialized.admin);
        assert_eq!(deserialized.default_topic_perm, Some(CheetahString::from("perm1")));
        assert_eq!(deserialized.default_group_perm, Some(CheetahString::from("perm2")));
        assert_eq!(deserialized.topic_perms, vec![CheetahString::from("topic1")]);
        assert_eq!(deserialized.group_perms, vec![CheetahString::from("group1")]);
    }

    #[test]
    fn deserialize_plain_access_config_missing_optional_fields() {
        let json = r#"{"admin":true,"topicPerms":[],"groupPerms":[]}"#;
        let deserialized: PlainAccessConfig = serde_json::from_str(json).unwrap();
        assert!(deserialized.access_key.is_none());
        assert!(deserialized.secret_key.is_none());
        assert!(deserialized.white_remote_address.is_none());
        assert!(deserialized.admin);
        assert!(deserialized.default_topic_perm.is_none());
        assert!(deserialized.default_group_perm.is_none());
        assert!(deserialized.topic_perms.is_empty());
        assert!(deserialized.group_perms.is_empty());
    }
}
