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

use rocketmq_sre_contracts::{ContractJsonValue as Value, EvidenceContent, EvidenceSnapshot};

pub(super) fn fingerprint(
    snapshot: &EvidenceSnapshot,
) -> Result<Option<String>, rocketmq_sre_contracts::SreContractError> {
    let EvidenceContent::Inline(content) = &snapshot.content else {
        return Ok(None);
    };
    if content["schema_version"].as_str() != Some("rocketmq.sre-diagnostic-source.v1")
        || content["profile"].as_str() != Some("store_pressure")
    {
        return Ok(None);
    }
    let mut semantic = content.clone();
    if let Some(root) = semantic.as_object_mut() {
        root.remove("collected_at");
    }
    if let Some(rows) = semantic["brokers"].as_array_mut() {
        for row in rows {
            if let Some(row) = row.as_object_mut() {
                row.remove("observed_at");
                row.remove("observed_at_millis");
                row.remove("signals");
                row.remove("observation_status");
                if let Some(observation) = row.get_mut("store_pressure").and_then(Value::as_object_mut) {
                    observation.remove("observed_at_millis");
                }
            }
        }
    }
    rocketmq_sre_contracts::canonical_sha256(&(
        &snapshot.source,
        &snapshot.resource,
        &semantic,
        snapshot.coverage,
        snapshot.partial,
    ))
    .map(Some)
}

pub(super) fn accepts(snapshot: &EvidenceSnapshot, as_of_unix_ms: i64) -> bool {
    let EvidenceContent::Inline(content) = &snapshot.content else {
        return false;
    };
    if content["schema_version"].as_str() != Some("rocketmq.sre-diagnostic-source.v1")
        || content["profile"].as_str() != Some("store_pressure")
    {
        return false;
    }
    let Some(expected) = snapshot
        .resource
        .strip_prefix("store-pressure/")
        .filter(|name| !name.is_empty() && !name.contains('/'))
    else {
        return false;
    };
    let Some(rows) = content["brokers"]
        .as_array()
        .filter(|rows| !rows.is_empty() && rows.len() <= 256)
    else {
        return false;
    };
    if content["observed_brokers"].as_u64() != Some(rows.len() as u64) {
        return false;
    }
    let mut ids = std::collections::BTreeSet::new();
    let mut observed = false;
    for row in rows {
        if row["broker_name"].as_str() != Some(expected) || !row["broker_id"].as_u64().is_some_and(|id| ids.insert(id))
        {
            return false;
        }
        let observation = &row["store_pressure"];
        if observation.is_null() {
            continue;
        }
        if !valid_observation(observation) {
            return false;
        }
        let Some(point) = observation["observed_at_millis"]
            .as_u64()
            .and_then(|point| i64::try_from(point).ok())
        else {
            return false;
        };
        let instant = (point.div_euclid(1000), (point.rem_euclid(1000) as u32) * 1_000_000);
        if instant
            < (
                snapshot.time_range.start.timestamp(),
                snapshot.time_range.start.timestamp_subsec_nanos(),
            )
            || instant
                > (
                    snapshot.time_range.end.timestamp(),
                    snapshot.time_range.end.timestamp_subsec_nanos(),
                )
            || as_of_unix_ms
                .checked_sub(point)
                .is_none_or(|age| !(0..=300_000).contains(&age))
        {
            return false;
        }
        if let Some(generation) = row["config"]["generation"].as_u64()
            && observation["configuration_generation"].as_u64() != Some(generation)
        {
            return false;
        }
        observed = true;
    }
    observed
}

fn optional(value: &Value, accepts: impl Fn(&Value) -> bool) -> bool {
    value.is_null() || accepts(value)
}

fn valid_observation(value: &Value) -> bool {
    if value["schema_version"].as_str() != Some("rocketmq.store-pressure-observation.v1")
        || value["source"].as_str() != Some("broker_runtime_info")
    {
        return false;
    }
    if !value["runtime"].is_object() || !value["disk"].is_object() || !value["policy"].is_object() {
        return false;
    }
    match value["generation_status"].as_str() {
        Some("matched_runtime_configuration")
            if value["configuration_generation"]
                .as_u64()
                .is_some_and(|generation| generation > 0) => {}
        Some("store_policy_generation_unverified") if value["configuration_generation"].is_null() => {}
        _ => return false,
    }
    let runtime = &value["runtime"];
    for field in [
        "writeable",
        "last_flush_error",
        "os_page_cache_busy",
        "transient_store_pool_deficient",
        "shutdown",
    ] {
        if !optional(&runtime[field], Value::is_boolean) {
            return false;
        }
    }
    for field in [
        "dispatch_behind_bytes",
        "sync_flush_queue_depth",
        "sync_flush_timeout_total",
        "sync_flush_oldest_wait_millis",
    ] {
        if !optional(&runtime[field], |value| value.as_u64().is_some()) {
            return false;
        }
    }
    let disk = &value["disk"];
    let policy = &value["policy"];
    let ratio = |value: &Value| {
        value
            .as_f64()
            .is_some_and(|value| value.is_finite() && (0.0..=1.0).contains(&value))
    };
    for field in [
        "commit_log_max_used_ratio",
        "commit_log_legacy_min_used_ratio",
        "consume_queue_used_ratio",
    ] {
        if !optional(&disk[field], ratio) {
            return false;
        }
    }
    for field in [
        "disk_warning_used_ratio",
        "disk_clean_forcibly_used_ratio",
        "disk_max_used_ratio",
    ] {
        if !optional(&policy[field], ratio) {
            return false;
        }
    }
    for field in [
        "sync_flush_backlog_reject_depth",
        "sync_flush_backlog_reject_wait_millis",
        "dispatch_backlog_reject_bytes",
        "page_cache_busy_timeout_millis",
    ] {
        if !optional(&policy[field], |value| value.as_u64().is_some()) {
            return false;
        }
    }
    if !optional(&value["tiered_configured"], Value::is_boolean)
        || !optional(&policy["clean_file_forcibly_enabled"], Value::is_boolean)
    {
        return false;
    }
    let Some(observed) = disk["commit_log_observed_paths"]
        .as_u64()
        .filter(|count| *count <= u32::MAX as u64)
    else {
        return false;
    };
    let Some(unknown) = disk["commit_log_unknown_paths"]
        .as_u64()
        .filter(|count| *count <= observed)
    else {
        return false;
    };
    if !optional(&disk["commit_log_expected_paths"], |value| {
        value
            .as_u64()
            .is_some_and(|count| count > 0 && count <= u32::MAX as u64 && observed <= count)
    }) {
        return false;
    }
    match disk["commit_log_samples_complete"].as_bool() {
        Some(true) => {
            disk["commit_log_expected_paths"].as_u64() == Some(observed)
                && observed > 0
                && unknown == 0
                && disk["commit_log_max_used_ratio"].as_f64().is_some()
        }
        Some(false) => true,
        None => false,
    }
}
