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
pub struct GroupList {
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
