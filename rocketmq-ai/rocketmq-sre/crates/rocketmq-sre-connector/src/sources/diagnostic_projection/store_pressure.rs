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

use chrono::Utc;
use rocketmq_admin_core::core::broker::{
    STORE_PRESSURE_DIAGNOSTICS_SCHEMA_VERSION, StoreHealthDiagnostics, StorePressureDiagnostics,
    StorePressureGenerationStatus, TieredDispatchDiagnostics,
};
use rocketmq_sre_contracts::CoverageStatus;
use rocketmq_sre_core::diagnostics::packs::assess_store_pressure_observation;
use serde_json::{Value, json};

use super::{DIAGNOSTIC_SCHEMA, array, copy_fields, object, schema_mismatch};
use crate::{ConnectorError, sources::common::SourceOutput};

pub(super) fn project(output: SourceOutput) -> Result<SourceOutput, ConnectorError> {
    project_at(output, Utc::now())
}

fn project_at(mut output: SourceOutput, collected_at: chrono::DateTime<Utc>) -> Result<SourceOutput, ConnectorError> {
    let mut source_age = output.freshness_seconds;
    let root = object(&output.content)?;
    let brokers = array(root, "brokers")?;
    let has_brokers = !brokers.is_empty();
    let mut rows = Vec::with_capacity(brokers.len());
    let mut observed = 0_usize;
    for broker in brokers {
        let broker = object(broker)?;
        let mut row = serde_json::Map::new();
        copy_fields(broker, &mut row, &["broker_name", "broker_id", "coverage"]);
        // Legacy values remain readable, but their configured=false fallback is
        // never used to infer a new pressure predicate or a disabled feature.
        if let Some(health) = broker
            .get("store_health")
            .and_then(|value| serde_json::from_value::<StoreHealthDiagnostics>(value.clone()).ok())
        {
            row.insert(
                "store_health".to_owned(),
                serde_json::to_value(health).map_err(|_| schema_mismatch())?,
            );
        }
        if let Some(tiered) = broker
            .get("tiered")
            .and_then(|value| serde_json::from_value::<TieredDispatchDiagnostics>(value.clone()).ok())
        {
            row.insert(
                "tiered".to_owned(),
                serde_json::to_value(tiered).map_err(|_| schema_mismatch())?,
            );
        }
        let value = broker.get("store_pressure");
        let pressure = value
            .and_then(|value| serde_json::from_value::<StorePressureDiagnostics>(value.clone()).ok())
            .filter(|value| value.schema_version == STORE_PRESSURE_DIAGNOSTICS_SCHEMA_VERSION);
        if let Some(mut pressure) = pressure {
            if let Some(generation) = broker
                .get("config")
                .and_then(|config| config.get("generation"))
                .and_then(Value::as_u64)
                && pressure.configuration_generation != Some(generation)
            {
                pressure.configuration_generation = None;
                pressure.generation_status = StorePressureGenerationStatus::StorePolicyGenerationUnverified;
            }
            // The public Admin DTO validates finite ratios and excludes unknown
            // wire fields. Do not retain arbitrary config or filesystem roots.
            let pressure = serde_json::to_value(pressure).map_err(|_| schema_mismatch())?;
            let age = pressure
                .get("observed_at_millis")
                .and_then(Value::as_u64)
                .and_then(|point| {
                    (collected_at.timestamp() as i128)
                        .checked_mul(1_000_000_000)
                        .and_then(|time| time.checked_add(collected_at.timestamp_subsec_nanos() as i128))
                        .and_then(|time| {
                            (point as i128)
                                .checked_mul(1_000_000)
                                .and_then(|point| time.checked_sub(point))
                        })
                })
                .filter(|age| *age >= 0);
            if let Some(age_seconds) = age
                .and_then(|age| age.checked_add(999_999_999))
                .and_then(|age| u64::try_from(age / 1_000_000_000).ok())
            {
                source_age = source_age.max(age_seconds);
            }
            let temporal_reason = match age {
                Some(age) if age <= 300_000_000_000 => None,
                Some(_) => Some("store_pressure_source_observation_stale"),
                None => Some("store_pressure_source_time_missing_or_future"),
            };
            let signals = assess_store_pressure_observation(&pressure)
                .into_iter()
                .map(|signal| {
                    json!({"key":signal.key,
                    "state":if temporal_reason.is_some() { "unknown" } else { signal.state.as_str() },
                    "reason_code":temporal_reason.unwrap_or(signal.reason_code)})
                })
                .collect::<Vec<_>>();
            row.insert("store_pressure".to_owned(), pressure);
            row.insert("signals".to_owned(), Value::Array(signals));
            row.insert(
                "observation_status".to_owned(),
                json!(temporal_reason.unwrap_or("instantaneous_point_not_range")),
            );
            observed += 1;
        } else {
            row.insert(
                "observation_status".to_owned(),
                json!(if value.is_some_and(|value| !value.is_null()) {
                    "store_pressure_observation_invalid"
                } else {
                    "store_pressure_observation_missing"
                }),
            );
        }
        rows.push(Value::Object(row));
    }
    // Preserve source-count absence and each instance independently. Missing
    // replicas cannot be filled from the fields of the available master.
    let summary = root.get("summary");
    let unavailable = summary
        .and_then(|summary| summary.get("unavailable_brokers"))
        .or_else(|| root.get("unavailable_brokers"))
        .and_then(Value::as_u64);
    let upstream_partial = output.partial;
    output.content = json!({
        "schema_version": DIAGNOSTIC_SCHEMA,
        "profile": "store_pressure",
        "observed_brokers": rows.len(),
        "pressure_observed_brokers": observed,
        "unavailable_brokers": unavailable,
        "upstream_partial": upstream_partial,
        "measurement_kind": "per_instance_instantaneous_store_observations",
        "collected_at": collected_at,
        "missing_dimensions": ["flush_dispatch_monotonic_progress", "tiered_wal_bytes_age_and_progress"],
        "brokers": rows
    });
    // An Admin envelope is dated at query start, while each Broker supplies its
    // own actual sample point. Date this projection at completion and retain
    // the oldest source age; a newly read envelope cannot refresh an old row.
    output.observed_at = collected_at;
    output.freshness_seconds = source_age;
    output.partial = true;
    output.coverage = match output.coverage {
        CoverageStatus::NotProductionVerified => CoverageStatus::NotProductionVerified,
        CoverageStatus::Missing => CoverageStatus::Missing,
        CoverageStatus::Available | CoverageStatus::Partial if observed > 0 || has_brokers => CoverageStatus::Partial,
        CoverageStatus::Available | CoverageStatus::Partial => CoverageStatus::Missing,
    };
    output
        .warnings
        .push("store_pressure_progress_and_wal_observations_unavailable".to_owned());
    Ok(output)
}

pub(super) fn validate_target(content: &Value, resource: &str) -> Result<(), ConnectorError> {
    let target = resource
        .strip_prefix("store-pressure/")
        .filter(|target| !target.is_empty() && !target.contains('/'))
        .ok_or_else(schema_mismatch)?;
    let mut ids = std::collections::BTreeSet::new();
    for row in array(object(content)?, "brokers")? {
        if row.get("broker_name").and_then(Value::as_str) != Some(target)
            || !row
                .get("broker_id")
                .and_then(Value::as_u64)
                .is_some_and(|id| ids.insert(id))
        {
            return Err(schema_mismatch());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
