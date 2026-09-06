// Copyright 2026 The RocketMQ Rust Authors
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

use serde::de;
use serde::Deserialize;
use serde::Deserializer;
use std::collections::HashMap;

pub(crate) fn deserialize_optional_i32_key_map<'de, D, V>(deserializer: D) -> Result<Option<HashMap<i32, V>>, D::Error>
where
    D: Deserializer<'de>,
    V: Deserialize<'de>,
{
    let raw = Option::<HashMap<String, V>>::deserialize(deserializer)?;
    let Some(raw) = raw else {
        return Ok(None);
    };

    let mut parsed = HashMap::with_capacity(raw.len());
    for (key, value) in raw {
        let key = key
            .parse::<i32>()
            .map_err(|error| de::Error::custom(format!("invalid i32 map key `{key}`: {error}")))?;
        parsed.insert(key, value);
    }
    Ok(Some(parsed))
}
