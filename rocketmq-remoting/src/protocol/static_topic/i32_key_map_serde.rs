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
