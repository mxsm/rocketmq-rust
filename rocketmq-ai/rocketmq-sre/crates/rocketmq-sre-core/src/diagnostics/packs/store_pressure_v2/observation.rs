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

use rocketmq_sre_contracts::ContractJsonValue as Value;

/// Outcome for one observed Store predicate, not an overall health verdict.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StorePressureSignalState {
    Crossed,
    WithinObservedPolicy,
    NotConfigured,
    Unknown,
}

impl StorePressureSignalState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Crossed => "crossed",
            Self::WithinObservedPolicy => "within_observed_policy",
            Self::NotConfigured => "not_configured",
            Self::Unknown => "unknown",
        }
    }
}

/// A stable, independently interpreted dimension of a Store observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StorePressureSignal {
    pub key: &'static str,
    pub state: StorePressureSignalState,
    pub reason_code: &'static str,
}

impl StorePressureSignal {
    const fn new(key: &'static str, state: StorePressureSignalState, reason_code: &'static str) -> Self {
        Self {
            key,
            state,
            reason_code,
        }
    }
}

/// Interprets only real public Store observation fields. It does not infer
/// sustained stalls, rates or WAL pressure from cumulative counters or pins.
/// Invalid saved/replayed numeric values fail closed just like wire input.
#[must_use]
pub fn assess_store_pressure_observation(value: &Value) -> Vec<StorePressureSignal> {
    use StorePressureSignalState::{Crossed, NotConfigured, Unknown, WithinObservedPolicy};

    let supported = value.get("schema_version").and_then(Value::as_str)
        == Some("rocketmq.store-pressure-observation.v1")
        && value.get("source").and_then(Value::as_str) == Some("broker_runtime_info");
    if !supported {
        return keys()
            .iter()
            .map(|key| StorePressureSignal::new(key, Unknown, "store_observation_unsupported"))
            .collect();
    }
    let coherent = value.get("generation_status").and_then(Value::as_str) == Some("matched_runtime_configuration")
        && value
            .get("configuration_generation")
            .and_then(Value::as_u64)
            .is_some_and(|generation| generation > 0);
    let policy = &value["policy"];
    let runtime = &value["runtime"];
    let mut signals = disk_signals(&value["disk"], policy, coherent);
    let depth = admission(
        policy.get("sync_flush_backlog_reject_depth"),
        runtime.get("sync_flush_queue_depth"),
    );
    let wait = admission(
        policy.get("sync_flush_backlog_reject_wait_millis"),
        runtime.get("sync_flush_oldest_wait_millis"),
    );
    let (state, reason) = if !coherent {
        (Unknown, "store_policy_generation_unverified")
    } else if depth == Crossed || wait == Crossed {
        (Crossed, "sync_flush_admission_limit_reached")
    } else if depth == Unknown || wait == Unknown {
        (Unknown, "sync_flush_admission_measurement_or_policy_missing")
    } else if depth == NotConfigured && wait == NotConfigured {
        (NotConfigured, "sync_flush_admission_limits_disabled")
    } else {
        (WithinObservedPolicy, "sync_flush_admission_limits_not_reached")
    };
    signals.push(StorePressureSignal::new("sync_flush_admission", state, reason));
    let dispatch = if coherent {
        admission(
            policy.get("dispatch_backlog_reject_bytes"),
            runtime.get("dispatch_behind_bytes"),
        )
    } else {
        Unknown
    };
    signals.push(StorePressureSignal::new(
        "dispatch_admission",
        dispatch,
        match dispatch {
            Crossed => "dispatch_admission_byte_limit_reached",
            WithinObservedPolicy => "dispatch_admission_byte_limit_not_reached",
            NotConfigured => "dispatch_admission_byte_limit_disabled",
            Unknown if !coherent => "store_policy_generation_unverified",
            Unknown => "dispatch_admission_measurement_or_policy_missing",
        },
    ));
    for (key, field, expected, reason) in [
        (
            "page_cache_busy",
            "os_page_cache_busy",
            true,
            "producer_page_cache_lock_age_predicate",
        ),
        (
            "transient_pool_deficient",
            "transient_store_pool_deficient",
            true,
            "producer_transient_pool_predicate",
        ),
        (
            "last_flush_error",
            "last_flush_error",
            true,
            "retained_last_flush_failure_not_a_current_rate",
        ),
        (
            "write_admission",
            "writeable",
            false,
            "producer_write_admission_not_a_disk_cause",
        ),
    ] {
        let state = match runtime.get(field).and_then(Value::as_bool) {
            Some(actual) if actual == expected => Crossed,
            Some(_) => WithinObservedPolicy,
            None => Unknown,
        };
        signals.push(StorePressureSignal::new(
            key,
            state,
            if state == Unknown {
                "store_runtime_predicate_missing"
            } else {
                reason
            },
        ));
    }
    signals.push(StorePressureSignal::new(
        "flush_dispatch_progress",
        Unknown,
        "monotonic_progress_and_stall_policy_unavailable",
    ));
    let tiered_disabled = value.get("tiered_configured").and_then(Value::as_bool) == Some(false);
    signals.push(StorePressureSignal::new(
        "tiered_wal_pressure",
        if tiered_disabled { NotConfigured } else { Unknown },
        if tiered_disabled {
            "tiered_store_explicitly_not_configured"
        } else {
            "tiered_wal_bytes_age_and_progress_unavailable"
        },
    ));
    signals
}

fn keys() -> &'static [&'static str] {
    &[
        "disk_warning",
        "disk_forcible_cleanup",
        "disk_reclaim",
        "sync_flush_admission",
        "dispatch_admission",
        "page_cache_busy",
        "transient_pool_deficient",
        "last_flush_error",
        "write_admission",
        "flush_dispatch_progress",
        "tiered_wal_pressure",
    ]
}

fn admission(threshold: Option<&Value>, observed: Option<&Value>) -> StorePressureSignalState {
    use StorePressureSignalState::*;
    match threshold.and_then(Value::as_u64) {
        Some(0) => NotConfigured,
        Some(threshold) => match observed.and_then(Value::as_u64) {
            Some(observed) if observed >= threshold => Crossed,
            Some(_) => WithinObservedPolicy,
            None => Unknown,
        },
        None => Unknown,
    }
}

fn ratio(value: Option<&Value>) -> Option<f64> {
    value
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite() && (0.0..=1.0).contains(value))
}

fn disk_signals(disk: &Value, policy: &Value, coherent: bool) -> Vec<StorePressureSignal> {
    use StorePressureSignalState::*;
    let warning = ratio(policy.get("disk_warning_used_ratio"));
    let forcible = ratio(policy.get("disk_clean_forcibly_used_ratio"));
    let max_used = ratio(policy.get("disk_max_used_ratio"));
    let valid_policy = warning
        .zip(forcible)
        .zip(max_used)
        .is_some_and(|((warning, forcible), max_used)| {
            (0.35..=0.90).contains(&warning)
                && (0.30..=0.85).contains(&forcible)
                && (0.10..=0.95).contains(&max_used)
                && max_used <= forcible
                && forcible <= warning
        });
    let expected = disk.get("commit_log_expected_paths").and_then(Value::as_u64);
    let observed = disk.get("commit_log_observed_paths").and_then(Value::as_u64);
    let unknown = disk.get("commit_log_unknown_paths").and_then(Value::as_u64);
    let counts_valid = expected
        .zip(observed)
        .zip(unknown)
        .is_some_and(|((expected, observed), unknown)| {
            expected > 0 && expected <= u32::MAX as u64 && observed <= expected && unknown <= observed
        });
    let commit = ratio(disk.get("commit_log_max_used_ratio"));
    let consume = ratio(disk.get("consume_queue_used_ratio"));
    let has_known_root = counts_valid
        && observed
            .zip(unknown)
            .is_some_and(|(observed, unknown)| observed > unknown);
    let complete = counts_valid
        && expected == observed
        && unknown == Some(0)
        && disk.get("commit_log_samples_complete").and_then(Value::as_bool) == Some(true)
        && commit.is_some()
        && consume.is_some();
    [
        ("disk_warning", warning, false),
        ("disk_forcible_cleanup", forcible, true),
        ("disk_reclaim", max_used, false),
    ]
    .into_iter()
    .map(|(key, threshold, needs_enabled)| {
        let enabled = policy.get("clean_file_forcibly_enabled").and_then(Value::as_bool);
        let (state, reason) = if !coherent {
            (Unknown, "store_policy_generation_unverified")
        } else if !valid_policy {
            (Unknown, "store_disk_policy_invalid")
        } else if needs_enabled && enabled == Some(false) {
            (NotConfigured, "forcible_file_cleanup_disabled")
        } else if needs_enabled && enabled.is_none() {
            (Unknown, "forcible_file_cleanup_configuration_missing")
        } else if threshold.is_some_and(|threshold| {
            (has_known_root && commit.is_some_and(|ratio| ratio > threshold))
                || consume.is_some_and(|ratio| ratio > threshold)
        }) {
            (Crossed, "sampled_root_above_strict_cleanup_ratio")
        } else if !counts_valid {
            (Unknown, "store_disk_probe_counts_invalid")
        } else if complete {
            (WithinObservedPolicy, "all_configured_root_samples_at_or_below_ratio")
        } else {
            (Unknown, "store_disk_probes_incomplete")
        };
        StorePressureSignal::new(key, state, reason)
    })
    .collect()
}
