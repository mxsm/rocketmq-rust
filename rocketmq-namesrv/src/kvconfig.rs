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

pub mod kvconfig_mananger;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KVConfigSerializeWrapper {
    #[serde(rename = "configTable")]
    pub config_table: Option<
        dashmap::DashMap<
            CheetahString, /* Namespace */
            HashMap<CheetahString /* Key */, CheetahString /* Value */>,
        >,
    >,
}

impl KVConfigSerializeWrapper {
    pub fn new_with_config_table(
        config_table: dashmap::DashMap<CheetahString, HashMap<CheetahString, CheetahString>>,
    ) -> KVConfigSerializeWrapper {
        KVConfigSerializeWrapper {
            config_table: Some(config_table),
        }
    }

    pub fn new() -> KVConfigSerializeWrapper {
        KVConfigSerializeWrapper {
            config_table: Some(dashmap::DashMap::new()),
        }
    }
}
