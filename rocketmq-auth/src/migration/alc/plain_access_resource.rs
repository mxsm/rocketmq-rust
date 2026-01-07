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
use rocketmq_common::common::key_builder::KeyBuilder;
use rocketmq_common::common::mix_all;

#[derive(Debug, Clone, Default)]
pub struct PlainAccessResource {
    /// Identify the user
    pub access_key: Option<CheetahString>,
    pub secret_key: Option<CheetahString>,
    pub white_remote_address: Option<CheetahString>,

    pub admin: bool,

    /// default = 1
    pub default_topic_perm: u8,
    pub default_group_perm: u8,

    /// resource -> perm
    pub resource_perm_map: HashMap<CheetahString, u8>,

    pub request_code: i32,

    /// content for signature calculation
    pub content: Option<Vec<u8>>,

    pub signature: Option<CheetahString>,
    pub secret_token: Option<CheetahString>,
    pub recognition: Option<CheetahString>,
}

impl PlainAccessResource {
    pub fn new() -> Self {
        Self {
            default_topic_perm: 1,
            default_group_perm: 1,
            resource_perm_map: HashMap::new(),
            ..Default::default()
        }
    }
}

impl PlainAccessResource {
    pub fn get_group_from_retry_topic(retry_topic: Option<&CheetahString>) -> Option<CheetahString> {
        retry_topic.map(|t| CheetahString::from_string(KeyBuilder::parse_group(t)))
    }

    pub fn get_retry_topic(group: Option<&CheetahString>) -> Option<CheetahString> {
        group.map(|g| CheetahString::from_string(mix_all::get_retry_topic(g)))
    }
}

impl PlainAccessResource {
    pub fn access_key(&self) -> Option<&CheetahString> {
        self.access_key.as_ref()
    }

    pub fn set_access_key(&mut self, v: CheetahString) {
        self.access_key = Some(v);
    }

    pub fn set_secret_key(&mut self, v: CheetahString) {
        self.secret_key = Some(v);
    }

    pub fn set_white_remote_address(&mut self, v: CheetahString) {
        self.white_remote_address = Some(v);
    }

    pub fn set_admin(&mut self, admin: bool) {
        self.admin = admin;
    }

    pub fn set_default_topic_perm(&mut self, perm: u8) {
        self.default_topic_perm = perm;
    }

    pub fn set_default_group_perm(&mut self, perm: u8) {
        self.default_group_perm = perm;
    }

    pub fn set_request_code(&mut self, code: i32) {
        self.request_code = code;
    }

    pub fn set_signature(&mut self, v: CheetahString) {
        self.signature = Some(v);
    }

    pub fn set_secret_token(&mut self, v: CheetahString) {
        self.secret_token = Some(v);
    }

    pub fn set_recognition(&mut self, v: CheetahString) {
        self.recognition = Some(v);
    }

    pub fn set_content(&mut self, content: Vec<u8>) {
        self.content = Some(content);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_access_resource_new_and_getters() {
        let mut resource = PlainAccessResource::new();
        assert_eq!(resource.default_topic_perm, 1);
        assert_eq!(resource.default_group_perm, 1);
        assert!(resource.access_key().is_none());

        resource.set_access_key(CheetahString::from("ak"));
        assert_eq!(resource.access_key(), Some(&CheetahString::from("ak")));

        resource.set_secret_key(CheetahString::from("sk"));
        assert_eq!(resource.secret_key, Some(CheetahString::from("sk")));

        resource.set_white_remote_address(CheetahString::from("127.0.0.1"));
        assert_eq!(resource.white_remote_address, Some(CheetahString::from("127.0.0.1")));

        resource.set_admin(true);
        assert!(resource.admin);

        resource.set_default_topic_perm(2);
        assert_eq!(resource.default_topic_perm, 2);

        resource.set_default_group_perm(3);
        assert_eq!(resource.default_group_perm, 3);

        resource.set_request_code(10);
        assert_eq!(resource.request_code, 10);

        resource.set_signature(CheetahString::from("sig"));
        assert_eq!(resource.signature, Some(CheetahString::from("sig")));

        resource.set_secret_token(CheetahString::from("token"));
        assert_eq!(resource.secret_token, Some(CheetahString::from("token")));

        resource.set_recognition(CheetahString::from("rec"));
        assert_eq!(resource.recognition, Some(CheetahString::from("rec")));

        resource.set_content(vec![1, 2, 3]);
        assert_eq!(resource.content, Some(vec![1, 2, 3]));
    }

    #[test]
    fn plain_access_resource_get_group_from_retry_topic() {
        let retry_topic = CheetahString::from("%RETRY%group1");
        let group = PlainAccessResource::get_group_from_retry_topic(Some(&retry_topic));
        assert_eq!(group, Some(CheetahString::from("group1")));

        assert_eq!(PlainAccessResource::get_group_from_retry_topic(None), None);
    }

    #[test]
    fn plain_access_resource_get_retry_topic() {
        let group = CheetahString::from("group1");
        let retry_topic = PlainAccessResource::get_retry_topic(Some(&group));
        assert_eq!(retry_topic, Some(CheetahString::from("%RETRY%group1")));

        assert_eq!(PlainAccessResource::get_retry_topic(None), None);
    }
}
