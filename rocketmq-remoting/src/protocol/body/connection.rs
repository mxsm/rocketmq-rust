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

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::LanguageCode;

#[derive(Deserialize, Serialize, Debug, Clone, Eq, PartialEq, Hash, Default)]
#[serde(rename_all = "camelCase")]
pub struct Connection {
    client_id: CheetahString,
    client_addr: CheetahString,
    language: LanguageCode,
    version: i32,
}

impl Connection {
    pub fn new() -> Self {
        Connection {
            client_id: CheetahString::default(),
            client_addr: CheetahString::default(),
            language: LanguageCode::default(),
            version: 0,
        }
    }
}

impl Connection {
    pub fn get_client_id(&self) -> CheetahString {
        self.client_id.clone()
    }

    pub fn set_client_id(&mut self, client_id: CheetahString) {
        self.client_id = client_id;
    }

    pub fn get_client_addr(&self) -> CheetahString {
        self.client_addr.clone()
    }

    pub fn set_client_addr(&mut self, client_addr: CheetahString) {
        self.client_addr = client_addr;
    }

    pub fn get_language(&self) -> LanguageCode {
        self.language
    }

    pub fn set_language(&mut self, language: LanguageCode) {
        self.language = language;
    }

    pub fn get_version(&self) -> i32 {
        self.version
    }

    pub fn set_version(&mut self, version: i32) {
        self.version = version;
    }
}
