//  Copyright 2023 The RocketMQ Rust Authors
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::hash::Hash;
use std::hash::Hasher;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize, Default, Eq)]
pub struct ClientGroup {
    #[serde(default)]
    pub client_id: CheetahString,

    #[serde(default)]
    pub group: CheetahString,
}

impl ClientGroup {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_parts(client_id: CheetahString, group: CheetahString) -> Self {
        Self { client_id, group }
    }

    #[must_use]
    pub fn client_id(&self) -> &CheetahString {
        &self.client_id
    }

    pub fn with_client_id(&mut self, client_id: CheetahString) -> &mut Self {
        self.client_id = client_id;
        self
    }

    #[must_use]
    pub fn group(&self) -> &CheetahString {
        &self.group
    }

    pub fn with_group(&mut self, group: CheetahString) -> &mut Self {
        self.group = group;
        self
    }
}

impl PartialEq for ClientGroup {
    fn eq(&self, other: &Self) -> bool {
        self.client_id == other.client_id && self.group == other.group
    }
}

impl Hash for ClientGroup {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.client_id.hash(state);
        self.group.hash(state);
    }
}
