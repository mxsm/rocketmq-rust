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
use std::hash::Hash;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub struct PlainAccessConfig {
    pub access_key: Option<CheetahString>,
    pub secret_key: Option<CheetahString>,
    pub white_remote_address: Option<CheetahString>,
    pub admin: bool,
    pub default_topic_perm: Option<CheetahString>,
    pub default_group_perm: Option<CheetahString>,
    pub topic_perms: Option<Vec<CheetahString>>,
    pub group_perms: Option<Vec<CheetahString>>,
}

impl PlainAccessConfig {
    pub fn new() -> Self {
        Self::default()
    }

    // accessKey
    pub fn access_key(&self) -> Option<&CheetahString> {
        self.access_key.as_ref()
    }

    pub fn set_access_key(&mut self, access_key: CheetahString) {
        self.access_key = Some(access_key);
    }

    // secretKey
    pub fn secret_key(&self) -> Option<&CheetahString> {
        self.secret_key.as_ref()
    }

    pub fn set_secret_key(&mut self, secret_key: CheetahString) {
        self.secret_key = Some(secret_key);
    }

    // whiteRemoteAddress
    pub fn white_remote_address(&self) -> Option<&CheetahString> {
        self.white_remote_address.as_ref()
    }

    pub fn set_white_remote_address(&mut self, addr: CheetahString) {
        self.white_remote_address = Some(addr);
    }

    // admin
    pub fn is_admin(&self) -> bool {
        self.admin
    }

    pub fn set_admin(&mut self, admin: bool) {
        self.admin = admin;
    }

    // defaultTopicPerm
    pub fn default_topic_perm(&self) -> Option<&CheetahString> {
        self.default_topic_perm.as_ref()
    }

    pub fn set_default_topic_perm(&mut self, perm: CheetahString) {
        self.default_topic_perm = Some(perm);
    }

    // defaultGroupPerm
    pub fn default_group_perm(&self) -> Option<&CheetahString> {
        self.default_group_perm.as_ref()
    }

    pub fn set_default_group_perm(&mut self, perm: CheetahString) {
        self.default_group_perm = Some(perm);
    }

    // topicPerms
    pub fn topic_perms(&self) -> Option<&[CheetahString]> {
        self.topic_perms.as_deref()
    }

    pub fn set_topic_perms(&mut self, perms: Vec<CheetahString>) {
        self.topic_perms = Some(perms);
    }

    // groupPerms
    pub fn group_perms(&self) -> Option<&[CheetahString]> {
        self.group_perms.as_deref()
    }

    pub fn set_group_perms(&mut self, perms: Vec<CheetahString>) {
        self.group_perms = Some(perms);
    }
}

impl fmt::Display for PlainAccessConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PlainAccessConfig{{ access_key={:?}, white_remote_address={:?}, admin={}, default_topic_perm={:?}, \
             default_group_perm={:?}, topic_perms={:?}, group_perms={:?} }}",
            self.access_key,
            self.white_remote_address,
            self.admin,
            self.default_topic_perm,
            self.default_group_perm,
            self.topic_perms,
            self.group_perms,
        )
    }
}
