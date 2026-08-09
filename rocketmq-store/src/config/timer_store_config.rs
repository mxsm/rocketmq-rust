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

use serde::Deserialize;
use thiserror::Error;

const MIB: usize = 1024 * 1024;
const GIB: u64 = 1024 * 1024 * 1024;

/// Resource limits for the native long-horizon timer store.
///
/// These limits are intentionally independent from the Java-compatible wheel.
/// They bound every page, batch, file handle set, and on-disk partition used by
/// the Extended Timeline implementation.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TimerStoreConfig {
    /// Stable number of delivery lanes. Changing this requires a new format generation.
    pub lane_count: usize,
    /// Maximum source records materialized per iteration.
    pub materialize_batch_messages: usize,
    /// Maximum source bytes materialized per iteration.
    pub materialize_batch_bytes: usize,
    /// Maximum Timeline records scanned per page.
    pub due_scan_messages: usize,
    /// Maximum encoded Timeline bytes scanned per page.
    pub due_scan_bytes: usize,
    /// Payload segment target size.
    pub payload_segment_bytes: u64,
    /// Maximum simultaneously open payload segment handles.
    pub payload_open_handles: usize,
    /// Maximum bytes in one payload fsync batch.
    pub payload_batch_bytes: usize,
    /// Maximum encoded payload record size.
    pub payload_record_bytes: usize,
    /// Maximum live bytes in one due-day/lane partition.
    pub payload_partition_live_bytes: u64,
    /// Scanner overlap protecting materialize/scan races.
    pub safety_overlap_ms: i64,
    /// Supported long-horizon retention boundary.
    pub horizon_days: u16,
    /// Maximum persisted shadow differences retained per class.
    pub shadow_diff_sample_limit: usize,
    /// Fixed-delay interval used by the shadow workers.
    pub scheduler_interval_ms: u64,
}

impl Default for TimerStoreConfig {
    fn default() -> Self {
        Self {
            lane_count: 16,
            materialize_batch_messages: 64,
            materialize_batch_bytes: 8 * MIB,
            due_scan_messages: 1_024,
            due_scan_bytes: 8 * MIB,
            payload_segment_bytes: 256 * MIB as u64,
            payload_open_handles: 64,
            payload_batch_bytes: 8 * MIB,
            payload_record_bytes: 4 * MIB,
            payload_partition_live_bytes: 64 * GIB,
            safety_overlap_ms: 30_000,
            horizon_days: 400,
            shadow_diff_sample_limit: 1_024,
            scheduler_interval_ms: 1_000,
        }
    }
}

impl TimerStoreConfig {
    /// Validates limits before any Extended Timeline resource is opened.
    ///
    /// # Errors
    ///
    /// Returns a field-specific error for an unsafe or internally inconsistent limit.
    pub fn validate(&self) -> Result<(), TimerStoreConfigError> {
        if self.lane_count == 0 || self.lane_count > usize::from(u16::MAX) {
            return Err(TimerStoreConfigError::Invalid("laneCount"));
        }
        if self.materialize_batch_messages == 0
            || self.materialize_batch_bytes < self.payload_record_bytes
            || self.due_scan_messages == 0
            || self.due_scan_bytes < 128
        {
            return Err(TimerStoreConfigError::Invalid("boundedBatch"));
        }
        if self.payload_segment_bytes == 0
            || self.payload_segment_bytes > self.payload_partition_live_bytes
            || self.payload_open_handles == 0
            || self.payload_batch_bytes < self.payload_record_bytes
            || self.payload_record_bytes == 0
        {
            return Err(TimerStoreConfigError::Invalid("payloadStore"));
        }
        if self.safety_overlap_ms <= 0 || !(180..=400).contains(&self.horizon_days) {
            return Err(TimerStoreConfigError::Invalid("horizon"));
        }
        if self.shadow_diff_sample_limit == 0 || self.scheduler_interval_ms == 0 {
            return Err(TimerStoreConfigError::Invalid("shadowRuntime"));
        }
        Ok(())
    }
}

/// Invalid Extended Timeline resource configuration.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum TimerStoreConfigError {
    /// One named group contains a zero, overflow, or inconsistent limit.
    #[error("invalid Extended Timeline timer configuration group: {0}")]
    Invalid(&'static str),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_limits_are_bounded_and_support_a_year() {
        let config = TimerStoreConfig::default();
        assert_eq!(config.horizon_days, 400);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn inconsistent_payload_budget_is_rejected() {
        let mut config = TimerStoreConfig::default();
        config.payload_batch_bytes = config.payload_record_bytes - 1;
        assert_eq!(config.validate(), Err(TimerStoreConfigError::Invalid("payloadStore")));
    }
}
