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

use super::{NameserverObservationStatus as Failure, NameserverRouteView};
use serde::de::{Deserialize, Deserializer, Error, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Value};

pub(super) struct UniqueJson(pub Value);

impl<'de> Deserialize<'de> for UniqueJson {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct UniqueVisitor;
        impl<'de> Visitor<'de> for UniqueVisitor {
            type Value = UniqueJson;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a bounded unique JSON value")
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut values = Map::new();
                while let Some((key, value)) = map.next_entry::<String, UniqueJson>()? {
                    if values.insert(key, value.0).is_some() {
                        return Err(M::Error::custom("duplicate observation field"));
                    }
                }
                Ok(UniqueJson(Value::Object(values)))
            }
            fn visit_seq<S: SeqAccess<'de>>(self, mut seq: S) -> Result<Self::Value, S::Error> {
                let mut values = Vec::new();
                while let Some(value) = seq.next_element::<UniqueJson>()? {
                    values.push(value.0);
                }
                Ok(UniqueJson(Value::Array(values)))
            }
            fn visit_bool<E: Error>(self, value: bool) -> Result<Self::Value, E> {
                Ok(UniqueJson(value.into()))
            }
            fn visit_i64<E: Error>(self, value: i64) -> Result<Self::Value, E> {
                Ok(UniqueJson(value.into()))
            }
            fn visit_u64<E: Error>(self, value: u64) -> Result<Self::Value, E> {
                Ok(UniqueJson(value.into()))
            }
            fn visit_f64<E: Error>(self, value: f64) -> Result<Self::Value, E> {
                serde_json::Number::from_f64(value)
                    .map(|value| UniqueJson(Value::Number(value)))
                    .ok_or_else(|| E::custom("invalid observation number"))
            }
            fn visit_str<E: Error>(self, value: &str) -> Result<Self::Value, E> {
                Ok(UniqueJson(value.into()))
            }
            fn visit_string<E: Error>(self, value: String) -> Result<Self::Value, E> {
                Ok(UniqueJson(value.into()))
            }
            fn visit_none<E: Error>(self) -> Result<Self::Value, E> {
                Ok(UniqueJson(Value::Null))
            }
            fn visit_unit<E: Error>(self) -> Result<Self::Value, E> {
                Ok(UniqueJson(Value::Null))
            }
        }
        deserializer.deserialize_any(UniqueVisitor)
    }
}

pub(super) fn row_work(value: &Value) -> Option<usize> {
    match value {
        Value::Object(map) => map
            .values()
            .try_fold(map.len(), |count, value| count.checked_add(row_work(value)?)),
        Value::Array(rows) => rows
            .iter()
            .try_fold(rows.len(), |count, value| count.checked_add(row_work(value)?)),
        _ => Some(0),
    }
}

#[cfg(feature = "read-client-adapter")]
pub(crate) fn view_row_work(bytes: &[u8]) -> Option<usize> {
    let text = std::str::from_utf8(bytes).ok()?.trim();
    if text.starts_with('{') {
        row_work(&serde_json::from_str::<UniqueJson>(text).ok()?.0)
    } else {
        Some(
            text.lines()
                .map(str::trim)
                .filter(|line| !line.is_empty() && !line.starts_with(['#', '!']))
                .count(),
        )
    }
}

#[cfg(feature = "read-client-adapter")]
pub(crate) fn route_row_work(bytes: &[u8]) -> Option<usize> {
    let text = rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData::observation_json(bytes)?;
    row_work(&serde_json::from_str::<UniqueJson>(&text).ok()?.0)
}

/// Strictly retains three flags, discarding all other private configuration.
pub fn decode_nameserver_view(bytes: &[u8], max_rows: usize) -> Result<(NameserverRouteView, usize), Failure> {
    let text = std::str::from_utf8(bytes).map_err(|_| Failure::Invalid)?.trim();
    let (values, rows) = if text.starts_with('{') {
        let parsed: UniqueJson = serde_json::from_str(text).map_err(|_| Failure::Invalid)?;
        let rows = row_work(&parsed.0).ok_or(Failure::RowLimit)?;
        let map = parsed.0.as_object().ok_or(Failure::Invalid)?;
        (map.clone(), rows)
    } else {
        let mut values = Map::new();
        let mut rows = 0usize;
        for line in text
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && !line.starts_with(['#', '!']))
        {
            rows = rows.checked_add(1).ok_or(Failure::RowLimit)?;
            let index = line.find(['=', ':']).ok_or(Failure::Invalid)?;
            let key = line[..index].trim();
            // Escaped/continued property keys cannot disguise a duplicate flag.
            if key.contains('\\') || key.is_empty() {
                return Err(Failure::Invalid);
            }
            if values
                .insert(key.to_owned(), Value::String(line[index + 1..].trim().to_owned()))
                .is_some()
            {
                return Err(Failure::Invalid);
            }
        }
        (values, rows)
    };
    if rows > max_rows {
        return Err(Failure::RowLimit);
    }
    let flag = |key| match values.get(key) {
        Some(Value::Bool(value)) => Ok(*value),
        Some(Value::String(value)) if value == "true" => Ok(true),
        Some(Value::String(value)) if value == "false" => Ok(false),
        _ => Err(Failure::Invalid),
    };
    Ok((
        NameserverRouteView {
            cluster_test: flag("clusterTest")?,
            order_message_enable: flag("orderMessageEnable")?,
            support_acting_master: flag("supportActingMaster")?,
        },
        rows,
    ))
}
