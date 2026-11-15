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

use super::decode::{row_work, UniqueJson};
use super::{NameserverObservationStatus as Failure, NameserverRouteObservation};
use crate::core::broker::canonical_remoting_endpoint;
use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;
use serde_json::{json, Map, Value};
use std::collections::BTreeSet;

fn object(value: &Value) -> Result<&Map<String, Value>, Failure> {
    value.as_object().ok_or(Failure::Invalid)
}
fn string(value: &Value) -> Result<&str, Failure> {
    value
        .as_str()
        .filter(|text| !text.is_empty() && text.len() <= 512 && !text.chars().any(char::is_control))
        .ok_or(Failure::Invalid)
}
fn count(value: &Value) -> Result<u32, Failure> {
    value
        .as_u64()
        .and_then(|n| u32::try_from(n).ok())
        .ok_or(Failure::Invalid)
}
fn endpoint(value: &str) -> Result<String, Failure> {
    canonical_remoting_endpoint(value).ok_or(Failure::Invalid)
}
fn supported(map: &Map<String, Value>, fields: &[&str], unknown: &mut BTreeSet<&'static str>) {
    if map.keys().any(|key| !fields.contains(&key.as_str())) {
        unknown.insert("route_schema");
    }
}

/// Complete whitelisted route normalization before endpoint pseudonymization.
/// It deliberately does not use BrokerData::Eq, which omits route-view fields.
pub fn decode_nameserver_route(
    bytes: &[u8],
    cluster: &str,
    topic: &str,
    max_rows: usize,
) -> Result<NameserverRouteObservation, Failure> {
    let compatible = TopicRouteData::observation_json(bytes).ok_or(Failure::Invalid)?;
    let parsed: UniqueJson = serde_json::from_str(&compatible).map_err(|_| Failure::Invalid)?;
    let rows = row_work(&parsed.0).ok_or(Failure::RowLimit)?;
    if rows > max_rows {
        return Err(Failure::RowLimit);
    }
    let root = object(&parsed.0)?;
    let mut unknown = BTreeSet::new();
    supported(
        root,
        &[
            "orderTopicConf",
            "queueDatas",
            "brokerDatas",
            "filterServerTable",
            "topicQueueMappingInfo",
            "topicQueueMappingByBroker",
        ],
        &mut unknown,
    );
    let mut brokers = Map::new();
    let mut known_endpoints = BTreeSet::new();
    for row in root
        .get("brokerDatas")
        .and_then(Value::as_array)
        .ok_or(Failure::Invalid)?
    {
        let row = object(row)?;
        supported(
            row,
            &["cluster", "brokerName", "brokerAddrs", "zoneName", "enableActingMaster"],
            &mut unknown,
        );
        if row.get("cluster").and_then(Value::as_str) != Some(cluster) {
            return Err(Failure::ScopeMismatch);
        }
        let name = string(row.get("brokerName").ok_or(Failure::Invalid)?)?;
        let mut addresses = Map::new();
        for (id, address) in object(row.get("brokerAddrs").ok_or(Failure::Invalid)?)? {
            let numeric = id.parse::<u64>().map_err(|_| Failure::Invalid)?;
            if numeric.to_string() != *id {
                return Err(Failure::Invalid);
            }
            let address = endpoint(string(address)?)?;
            known_endpoints.insert(address.clone());
            addresses.insert(id.clone(), Value::String(address));
        }
        let zone = match row.get("zoneName") {
            Some(Value::Null) => Value::Null,
            Some(value) => Value::String(string(value)?.to_owned()),
            None => {
                unknown.insert("zone");
                Value::Null
            }
        };
        let acting = match row.get("enableActingMaster").and_then(Value::as_bool) {
            Some(value) => Value::Bool(value),
            None => {
                unknown.insert("acting_master");
                Value::Null
            }
        };
        if brokers
            .insert(
                name.to_owned(),
                json!({"cluster":cluster,"addresses":addresses,"zone":zone,"acting_master":acting}),
            )
            .is_some()
        {
            return Err(Failure::Invalid);
        }
    }
    let mut queues = Map::new();
    for row in root
        .get("queueDatas")
        .and_then(Value::as_array)
        .ok_or(Failure::Invalid)?
    {
        let row = object(row)?;
        supported(
            row,
            &["brokerName", "readQueueNums", "writeQueueNums", "perm", "topicSysFlag"],
            &mut unknown,
        );
        let name = string(row.get("brokerName").ok_or(Failure::Invalid)?)?;
        if !brokers.contains_key(name) {
            return Err(Failure::ScopeMismatch);
        }
        let mut normalized = Map::new();
        for (input, output) in [
            ("readQueueNums", "read_queue_nums"),
            ("writeQueueNums", "write_queue_nums"),
            ("perm", "perm"),
            ("topicSysFlag", "topic_sys_flag"),
        ] {
            normalized.insert(
                output.to_owned(),
                count(row.get(input).ok_or(Failure::Invalid)?)?.into(),
            );
        }
        if queues.insert(name.to_owned(), Value::Object(normalized)).is_some() {
            return Err(Failure::Invalid);
        }
    }
    let order = match root.get("orderTopicConf") {
        None => {
            unknown.insert("order_config");
            Value::Null
        }
        Some(Value::Null) => Value::Null,
        Some(Value::String(text)) if text.len() <= 64 * 1024 => {
            let mut entries = Vec::new();
            let mut seen = BTreeSet::new();
            if !text.is_empty() {
                for entry in text.split(';') {
                    let (name, number) = entry.split_once(':').ok_or(Failure::Invalid)?;
                    if !brokers.contains_key(name) || !seen.insert(name) {
                        return Err(Failure::ScopeMismatch);
                    }
                    let number = number
                        .parse::<u32>()
                        .ok()
                        .filter(|n| *n <= i32::MAX as u32)
                        .ok_or(Failure::Invalid)?;
                    entries.push(json!({"broker_name":name,"queue_count":number}));
                }
            }
            Value::Array(entries)
        }
        _ => return Err(Failure::Invalid),
    };
    let mut filters = Map::new();
    match root.get("filterServerTable") {
        None => {
            unknown.insert("filter_servers");
        }
        Some(value) => {
            for (broker, targets) in object(value)? {
                let broker = endpoint(broker)?;
                if !known_endpoints.contains(&broker) || filters.contains_key(&broker) {
                    return Err(Failure::ScopeMismatch);
                }
                let mut addresses = BTreeSet::new();
                for target in targets.as_array().ok_or(Failure::Invalid)? {
                    if !addresses.insert(endpoint(string(target)?)?) {
                        return Err(Failure::Invalid);
                    }
                }
                filters.insert(broker, json!(addresses));
            }
        }
    }
    if root.contains_key("topicQueueMappingInfo") && root.contains_key("topicQueueMappingByBroker") {
        return Err(Failure::Invalid);
    }
    let mapping = match root
        .get("topicQueueMappingInfo")
        .or_else(|| root.get("topicQueueMappingByBroker"))
    {
        None => {
            unknown.insert("static_mapping");
            Value::Null
        }
        Some(Value::Null) => Value::Null,
        Some(value) => {
            let mut mappings = Map::new();
            for (owner, mapping) in object(value)? {
                if !brokers.contains_key(owner) {
                    return Err(Failure::ScopeMismatch);
                }
                let mapping = object(mapping)?;
                supported(
                    mapping,
                    &["topic", "scope", "totalQueues", "bname", "epoch", "dirty", "currIdMap"],
                    &mut unknown,
                );
                if mapping.get("topic").and_then(Value::as_str) != Some(topic)
                    || mapping.get("bname").and_then(Value::as_str) != Some(owner.as_str())
                {
                    return Err(Failure::ScopeMismatch);
                }
                let total = count(mapping.get("totalQueues").ok_or(Failure::Invalid)?)?;
                if total > i32::MAX as u32 {
                    return Err(Failure::Invalid);
                }
                let epoch = mapping
                    .get("epoch")
                    .and_then(Value::as_i64)
                    .filter(|value| *value >= 0)
                    .ok_or(Failure::Invalid)?;
                let dirty = mapping.get("dirty").and_then(Value::as_bool).ok_or(Failure::Invalid)?;
                let scope = match mapping.get("scope") {
                    Some(value) if !value.is_null() => Value::String(string(value)?.to_owned()),
                    _ => {
                        unknown.insert("static_mapping");
                        Value::Null
                    }
                };
                let ids = match mapping.get("currIdMap") {
                    Some(Value::Null) => Value::Null,
                    Some(value) => {
                        let mut ids = Map::new();
                        for (id, queue) in object(value)? {
                            let logical = id.parse::<u32>().map_err(|_| Failure::Invalid)?;
                            if logical.to_string() != *id || logical >= total || count(queue)? > i32::MAX as u32 {
                                return Err(Failure::Invalid);
                            }
                            ids.insert(id.clone(), queue.clone());
                        }
                        Value::Object(ids)
                    }
                    None => {
                        unknown.insert("static_mapping");
                        Value::Null
                    }
                };
                mappings.insert(owner.clone(), json!({"topic":topic,"scope":scope,"total_queues":total,"broker_name":owner,"epoch":epoch.to_string(),"dirty":dirty,"current_ids":ids}));
            }
            Value::Object(mappings)
        }
    };
    Ok(NameserverRouteObservation {
        normalized: json!({"brokers":brokers,"queues":queues,"order_config":order,"filter_servers":filters,"static_mapping":mapping,"unknown_dimensions":unknown}),
        rows,
    })
}
