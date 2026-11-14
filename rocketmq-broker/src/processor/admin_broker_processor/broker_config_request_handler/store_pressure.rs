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

use std::collections::HashMap;

use rocketmq_store::MessageStoreConfig;

use crate::config::broker_config::BrokerConfig;

pub(super) struct PressurePolicyContext<'a> {
    pub(super) generation: u64,
    pub(super) broker: &'a BrokerConfig,
    pub(super) configured_store: &'a MessageStoreConfig,
    pub(super) active_store: &'a MessageStoreConfig,
}

/// Adds only safe policy values and sample completeness metadata. Disk probes
/// remain the Store's existing runtime-info probes; no filesystem roots or
/// additional blocking I/O are introduced here.
pub(super) fn append_runtime_info(info: &mut HashMap<String, String>, context: PressurePolicyContext<'_>) {
    let active = context.active_store.normalized_local_store_config();
    let configured = context.configured_store.normalized_local_store_config();
    #[cfg(feature = "tieredstore")]
    let (tiered_configured, configured_tiered) = (
        context.active_store.tiered_store_config.is_some(),
        context.configured_store.tiered_store_config.is_some(),
    );
    #[cfg(not(feature = "tieredstore"))]
    let (tiered_configured, configured_tiered) = (false, false);
    info.insert(
        "storePressureSchemaVersion".to_owned(),
        "rocketmq.store-pressure-observation.v1".to_owned(),
    );
    // These keys are emitted by the actual SyncFlush counter owner. Do not
    // manufacture zero from a backend's generic default health snapshot.
    for (key, native) in [
        ("storePressureSyncFlushQueueDepth", "syncFlushQueueDepth"),
        ("storePressureSyncFlushTimeoutTotal", "syncFlushTimeoutTotal"),
        ("storePressureSyncFlushOldestWaitMillis", "syncFlushOldestWaitMillis"),
    ] {
        if let Some(value) = info.get(native).cloned() {
            info.insert(key.to_owned(), value);
        }
    }
    if let Ok(count) = u64::try_from(active.commit_log_paths.len()) {
        info.insert("storePressureCommitLogPathCount".to_owned(), count.to_string());
    }
    let coherent = context.generation > 0
        && active.cleanup == configured.cleanup
        && active.commit_log_paths == configured.commit_log_paths
        && active.store_path_root_dir == configured.store_path_root_dir
        && tiered_configured == configured_tiered
        && context.active_store.store_type.get_store_type() == context.configured_store.store_type.get_store_type()
        && context.active_store.flush_disk_type.get_flush_disk_type()
            == context.configured_store.flush_disk_type.get_flush_disk_type()
        && context.active_store.transient_store_pool_enable == context.configured_store.transient_store_pool_enable
        && context.active_store.sync_flush_timeout == context.configured_store.sync_flush_timeout
        && context.active_store.os_page_cache_busy_timeout_mills
            == context.configured_store.os_page_cache_busy_timeout_mills;
    info.insert(
        "storePressurePolicyGenerationStatus".to_owned(),
        if coherent {
            "matched_runtime_configuration"
        } else {
            "store_policy_generation_unverified"
        }
        .to_owned(),
    );
    info.remove("storePressurePolicyGeneration");
    if coherent {
        info.insert(
            "storePressurePolicyGeneration".to_owned(),
            context.generation.to_string(),
        );
    }
    for (key, value) in [
        ("storePressureDiskWarningRatio", active.cleanup.disk_warning_ratio),
        (
            "storePressureDiskCleanForciblyRatio",
            active.cleanup.disk_clean_forcibly_ratio,
        ),
        ("storePressureDiskMaxUsedRatio", active.cleanup.disk_max_used_ratio),
    ] {
        info.insert(key.to_owned(), value.to_string());
    }
    info.insert(
        "storePressureCleanFileForciblyEnabled".to_owned(),
        active.cleanup.clean_file_forcibly_enabled.to_string(),
    );
    for (key, value) in [
        (
            "storePressureSyncFlushRejectDepth",
            context.broker.sync_flush_backlog_reject_depth,
        ),
        (
            "storePressureSyncFlushRejectWaitMillis",
            context.broker.sync_flush_backlog_reject_wait_millis,
        ),
        (
            "storePressurePageCacheBusyTimeoutMillis",
            context.active_store.os_page_cache_busy_timeout_mills,
        ),
    ] {
        info.insert(key.to_owned(), value.to_string());
    }
    info.insert(
        "storePressureDispatchRejectBytes".to_owned(),
        context.broker.reput_lag_reject_bytes.to_string(),
    );
    info.insert(
        "storePressureTieredConfigured".to_owned(),
        tiered_configured.to_string(),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "tieredstore")]
    #[test]
    fn tiered_presence_uses_the_active_store_and_mismatches_do_not_claim_a_generation() {
        for (active_enabled, configured_enabled) in [(false, false), (true, true), (false, true), (true, false)] {
            let active = MessageStoreConfig {
                tiered_store_config: active_enabled.then(Default::default),
                ..Default::default()
            };
            let configured = MessageStoreConfig {
                tiered_store_config: configured_enabled.then(Default::default),
                ..active.clone()
            };
            let broker = BrokerConfig::default();
            let mut info = HashMap::new();
            append_runtime_info(
                &mut info,
                PressurePolicyContext {
                    generation: 11,
                    broker: &broker,
                    configured_store: &configured,
                    active_store: &active,
                },
            );
            assert_eq!(info["storePressureTieredConfigured"], active_enabled.to_string());
            let coherent = active_enabled == configured_enabled;
            assert_eq!(
                info.get("storePressurePolicyGeneration").map(String::as_str),
                coherent.then_some("11")
            );
            assert_eq!(
                info["storePressurePolicyGenerationStatus"],
                if coherent {
                    "matched_runtime_configuration"
                } else {
                    "store_policy_generation_unverified"
                }
            );
        }
    }

    #[test]
    fn pressure_policy_is_actual_normalized_store_configuration_and_zero_remains_disabled() {
        let store = MessageStoreConfig {
            disk_space_warning_level_ratio: 1,
            disk_space_clean_forcibly_ratio: 100,
            disk_max_used_space_ratio: 1,
            ..Default::default()
        };
        let broker = BrokerConfig::default();
        let mut info = HashMap::from([
            ("syncFlushQueueDepth".to_owned(), "3".to_owned()),
            ("syncFlushTimeoutTotal".to_owned(), "37".to_owned()),
        ]);
        append_runtime_info(
            &mut info,
            PressurePolicyContext {
                generation: 7,
                broker: &broker,
                configured_store: &store,
                active_store: &store,
            },
        );
        assert_eq!(info["storePressureDiskWarningRatio"], "0.35");
        assert_eq!(info["storePressureDiskCleanForciblyRatio"], "0.85");
        assert_eq!(info["storePressureDiskMaxUsedRatio"], "0.1");
        assert_eq!(info["storePressureSyncFlushRejectDepth"], "0");
        assert_eq!(info["storePressureSyncFlushRejectWaitMillis"], "0");
        assert_eq!(info["storePressurePolicyGeneration"], "7");
        assert_eq!(info["storePressureSyncFlushQueueDepth"], "3");
        assert_eq!(info["storePressureSyncFlushTimeoutTotal"], "37");
        assert!(!info.contains_key("storePressureSyncFlushOldestWaitMillis"));
        // Neither a source path nor a fictional 500ms/1000-count policy is emitted.
        let private_root = store.get_store_path_commit_log();
        assert!(!info.values().any(|value| value.as_str() == private_root.as_str()));
    }

    #[test]
    fn a_changed_store_policy_cannot_borrow_the_runtime_snapshot_generation() {
        let active = MessageStoreConfig::default();
        let configured = MessageStoreConfig {
            disk_space_warning_level_ratio: 80,
            ..active.clone()
        };
        let broker = BrokerConfig {
            sync_flush_backlog_reject_depth: 32,
            sync_flush_backlog_reject_wait_millis: 150,
            reput_lag_reject_bytes: 4096,
            ..Default::default()
        };
        let mut info = HashMap::new();
        append_runtime_info(
            &mut info,
            PressurePolicyContext {
                generation: 9,
                broker: &broker,
                configured_store: &configured,
                active_store: &active,
            },
        );
        assert!(!info.contains_key("storePressurePolicyGeneration"));
        assert_eq!(
            info["storePressurePolicyGenerationStatus"],
            "store_policy_generation_unverified"
        );
        assert_eq!(info["storePressureDiskWarningRatio"], "0.9");
        assert_eq!(info["storePressureSyncFlushRejectDepth"], "32");
        assert_eq!(info["storePressureSyncFlushRejectWaitMillis"], "150");
        assert_eq!(info["storePressureDispatchRejectBytes"], "4096");
    }
}
