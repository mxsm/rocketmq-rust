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

use super::observation::NamesrvObservationIssue as Issue;
use rocketmq_sre_contracts::{ContractJsonValue as Value, is_sha256_digest};

pub(super) const DIMENSIONS: [&str; 14] = [
    "topic_presence",
    "brokers",
    "broker_ids",
    "addresses",
    "cluster",
    "zone",
    "acting_master",
    "queues",
    "permissions",
    "topic_sys_flags",
    "order_config",
    "filter_servers",
    "static_mapping",
    "route_schema",
];
pub(super) fn opaque(value: &Value) -> bool {
    value.as_str().is_some_and(|text| {
        !text.is_empty()
            && text.len() <= 64
            && text.as_bytes()[0].is_ascii_alphabetic()
            && text
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    })
}
pub(super) fn digest(value: &Value) -> bool {
    value.as_str().is_some_and(is_sha256_digest)
}
fn text(value: &Value) -> bool {
    value
        .as_str()
        .is_some_and(|text| !text.is_empty() && text.len() <= 512 && !text.chars().any(char::is_control))
}
fn count(value: &Value) -> bool {
    value.as_u64().is_some_and(|value| u32::try_from(value).is_ok())
}
fn fields(value: &Value, required: &[&str]) -> bool {
    value
        .as_object()
        .is_some_and(|map| map.len() == required.len() && required.iter().all(|key| map.contains_key(*key)))
}

pub(super) fn validate(route: &Value, physical_cluster: &str, topic: &str) -> Result<(), Issue> {
    if !fields(
        route,
        &[
            "brokers",
            "queues",
            "order_config",
            "filter_servers",
            "static_mapping",
            "unknown_dimensions",
        ],
    ) {
        return Err(Issue::Schema);
    }
    let brokers = route["brokers"]
        .as_object()
        .filter(|map| map.len() <= 256)
        .ok_or(Issue::Schema)?;
    let mut addresses = std::collections::BTreeSet::new();
    for (name, broker) in brokers {
        if name.is_empty()
            || name.len() > 255
            || name.chars().any(char::is_control)
            || !fields(broker, &["cluster", "addresses", "zone", "acting_master"])
            || broker["cluster"].as_str() != Some(physical_cluster)
            || !(broker["zone"].is_null() || text(&broker["zone"]))
            || !(broker["acting_master"].is_null() || broker["acting_master"].is_boolean())
        {
            return Err(Issue::Scope);
        }
        for (id, address) in broker["addresses"]
            .as_object()
            .filter(|map| map.len() <= 256)
            .ok_or(Issue::Schema)?
        {
            if id.parse::<u64>().ok().is_none_or(|number| number.to_string() != *id) || !digest(address) {
                return Err(Issue::Schema);
            }
            addresses.insert(address.as_str().ok_or(Issue::Schema)?);
        }
    }
    for (name, queue) in route["queues"]
        .as_object()
        .filter(|map| map.len() <= 256)
        .ok_or(Issue::Schema)?
    {
        if !brokers.contains_key(name)
            || !fields(
                queue,
                &["read_queue_nums", "write_queue_nums", "perm", "topic_sys_flag"],
            )
            || ["read_queue_nums", "write_queue_nums", "perm", "topic_sys_flag"]
                .iter()
                .any(|key| !count(&queue[*key]))
        {
            return Err(Issue::Schema);
        }
    }
    if !route["order_config"].is_null() {
        let mut seen = std::collections::BTreeSet::new();
        for entry in route["order_config"]
            .as_array()
            .filter(|rows| rows.len() <= 256)
            .ok_or(Issue::Schema)?
        {
            let name = entry["broker_name"].as_str().ok_or(Issue::Schema)?;
            if !fields(entry, &["broker_name", "queue_count"])
                || !brokers.contains_key(name)
                || !seen.insert(name)
                || entry["queue_count"].as_u64().is_none_or(|n| n > i32::MAX as u64)
            {
                return Err(Issue::Schema);
            }
        }
    }
    for (broker, servers) in route["filter_servers"]
        .as_object()
        .filter(|map| map.len() <= 256)
        .ok_or(Issue::Schema)?
    {
        if !addresses.contains(broker.as_str()) {
            return Err(Issue::Scope);
        }
        let mut seen = std::collections::BTreeSet::new();
        for server in servers
            .as_array()
            .filter(|rows| rows.len() <= 256)
            .ok_or(Issue::Schema)?
        {
            if !digest(server) || !seen.insert(server.as_str().ok_or(Issue::Schema)?) {
                return Err(Issue::Schema);
            }
        }
    }
    if !route["static_mapping"].is_null() {
        for (name, mapping) in route["static_mapping"]
            .as_object()
            .filter(|map| map.len() <= 256)
            .ok_or(Issue::Schema)?
        {
            if !brokers.contains_key(name)
                || !fields(
                    mapping,
                    &[
                        "topic",
                        "scope",
                        "total_queues",
                        "broker_name",
                        "epoch",
                        "dirty",
                        "current_ids",
                    ],
                )
                || mapping["topic"].as_str() != Some(topic)
                || mapping["broker_name"].as_str() != Some(name.as_str())
                || !(mapping["scope"].is_null() || text(&mapping["scope"]))
                || mapping["total_queues"].as_u64().is_none_or(|n| n > i32::MAX as u64)
                || mapping["epoch"].as_str().is_none_or(|value| {
                    value
                        .parse::<i64>()
                        .ok()
                        .is_none_or(|n| n < 0 || n.to_string() != value)
                })
                || !mapping["dirty"].is_boolean()
            {
                return Err(Issue::Schema);
            }
            if !mapping["current_ids"].is_null() {
                for (id, queue) in mapping["current_ids"]
                    .as_object()
                    .filter(|map| map.len() <= 4096)
                    .ok_or(Issue::Schema)?
                {
                    if id
                        .parse::<u32>()
                        .ok()
                        .is_none_or(|n| n.to_string() != *id || Some(n as u64) >= mapping["total_queues"].as_u64())
                        || queue.as_u64().is_none_or(|n| n > i32::MAX as u64)
                    {
                        return Err(Issue::Schema);
                    }
                }
            }
        }
    }
    let unknown = route["unknown_dimensions"]
        .as_array()
        .filter(|rows| rows.len() <= DIMENSIONS.len())
        .ok_or(Issue::Schema)?;
    let mut seen = std::collections::BTreeSet::new();
    for entry in unknown {
        let value = entry.as_str().ok_or(Issue::Schema)?;
        if !DIMENSIONS.contains(&value) || !seen.insert(value) {
            return Err(Issue::Schema);
        }
    }
    Ok(())
}

pub(super) fn differences(left: &Value, right: &Value) -> Vec<&'static str> {
    let mut differences = Vec::new();
    let unknown: std::collections::BTreeSet<_> = unknowns(left).into_iter().chain(unknowns(right)).collect();
    if left["brokers"].as_object().map(|map| map.keys().collect::<Vec<_>>())
        != right["brokers"].as_object().map(|map| map.keys().collect::<Vec<_>>())
    {
        differences.push("brokers");
    }
    if let (Some(left), Some(right)) = (left["brokers"].as_object(), right["brokers"].as_object()) {
        for (name, broker) in left {
            if let Some(other) = right.get(name) {
                for (field, dimension) in [
                    ("cluster", "cluster"),
                    ("zone", "zone"),
                    ("acting_master", "acting_master"),
                    ("addresses", "addresses"),
                ] {
                    if broker[field] != other[field] {
                        differences.push(dimension);
                    }
                }
                if broker["addresses"]
                    .as_object()
                    .map(|map| map.keys().collect::<Vec<_>>())
                    != other["addresses"].as_object().map(|map| map.keys().collect::<Vec<_>>())
                {
                    differences.push("broker_ids");
                }
            }
        }
    }
    if left["queues"] != right["queues"] {
        if left["queues"].as_object().map(|map| map.keys().collect::<Vec<_>>())
            != right["queues"].as_object().map(|map| map.keys().collect::<Vec<_>>())
        {
            differences.push("queues");
        }
        if let (Some(left), Some(right)) = (left["queues"].as_object(), right["queues"].as_object()) {
            for (name, queue) in left {
                if let Some(other) = right.get(name) {
                    if queue["read_queue_nums"] != other["read_queue_nums"]
                        || queue["write_queue_nums"] != other["write_queue_nums"]
                    {
                        differences.push("queues");
                    }
                    if queue["perm"] != other["perm"] {
                        differences.push("permissions");
                    }
                    if queue["topic_sys_flag"] != other["topic_sys_flag"] {
                        differences.push("topic_sys_flags");
                    }
                }
            }
        }
    }
    for (field, dimension) in [
        ("order_config", "order_config"),
        ("filter_servers", "filter_servers"),
        ("static_mapping", "static_mapping"),
    ] {
        if left[field] != right[field] {
            differences.push(dimension);
        }
    }
    differences.sort();
    differences.dedup();
    differences.retain(|dimension| !unknown.contains(dimension));
    differences
}

pub(super) fn unknowns(route: &Value) -> Vec<&'static str> {
    let mut unknown = std::collections::BTreeSet::new();
    if let Some(values) = route["unknown_dimensions"].as_array() {
        for value in values {
            if let Some(dimension) = DIMENSIONS.iter().find(|dimension| Some(**dimension) == value.as_str()) {
                unknown.insert(*dimension);
            }
        }
    }
    if route["brokers"]
        .as_object()
        .is_some_and(|brokers| brokers.values().any(|broker| broker["acting_master"].is_null()))
    {
        unknown.insert("acting_master");
    }
    if route["static_mapping"].as_object().is_some_and(|mappings| {
        mappings
            .values()
            .any(|mapping| mapping["scope"].is_null() || mapping["current_ids"].is_null())
    }) {
        unknown.insert("static_mapping");
    }
    unknown.into_iter().collect()
}
