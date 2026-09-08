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

#[cfg(test)]
mod tests {
    use super::*;

    /// Local wrapper so the crate-private helper can be exercised directly, without widening the
    /// visibility of production code.
    #[derive(Debug, Deserialize)]
    struct Wrapper {
        #[serde(default, deserialize_with = "deserialize_optional_i32_key_map")]
        map: Option<HashMap<i32, String>>,
    }

    fn parse(json: &str) -> Result<Option<HashMap<i32, String>>, serde_json::Error> {
        serde_json::from_str::<Wrapper>(json).map(|wrapper| wrapper.map)
    }

    #[test]
    fn missing_field_and_explicit_null_both_deserialize_to_none() {
        assert!(parse("{}").unwrap().is_none());
        assert!(parse(r#"{"map":null}"#).unwrap().is_none());
    }

    #[test]
    fn empty_object_deserializes_to_empty_map() {
        let map = parse(r#"{"map":{}}"#).unwrap().expect("map should be present");
        assert!(map.is_empty());
    }

    #[test]
    fn signed_boundary_keys_deserialize_to_matching_integers() {
        let json = format!(
            r#"{{"map":{{"0":"zero","7":"positive","-7":"negative","{}":"min","{}":"max"}}}}"#,
            i32::MIN,
            i32::MAX
        );
        let map = parse(&json).unwrap().expect("map should be present");
        assert_eq!(map.len(), 5);
        assert_eq!(map[&0], "zero");
        assert_eq!(map[&7], "positive");
        assert_eq!(map[&-7], "negative");
        assert_eq!(map[&i32::MIN], "min");
        assert_eq!(map[&i32::MAX], "max");
    }

    #[test]
    fn non_numeric_key_is_rejected_with_the_offending_key() {
        let error = parse(r#"{"map":{"queue-1":"value"}}"#).unwrap_err().to_string();
        assert!(error.contains("invalid i32 map key"), "unexpected error: {error}");
        assert!(error.contains("queue-1"), "unexpected error: {error}");
    }

    #[test]
    fn out_of_range_keys_are_rejected_instead_of_wrapping() {
        for key in [i64::from(i32::MIN) - 1, i64::from(i32::MAX) + 1] {
            let error = parse(&format!(r#"{{"map":{{"{key}":"value"}}}}"#))
                .unwrap_err()
                .to_string();
            assert!(error.contains("invalid i32 map key"), "unexpected error: {error}");
            assert!(error.contains(&key.to_string()), "unexpected error: {error}");
        }
    }
}
