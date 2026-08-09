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

use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use parking_lot::Mutex;
use rocketmq_store_api::TimerId;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub(crate) enum RetryClass {
    CommitLogUnavailable,
    StorageBusy,
    DeliveryRejected,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub(crate) enum CorruptionReason {
    BadMagic,
    MissingPayload,
    ShortRead,
    ChecksumMismatch,
    UnsupportedRecord,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum TimerWorkResult {
    Complete,
    Retry(RetryClass),
    Quarantine(CorruptionReason),
    Cancelled,
    StaleGeneration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RetryPolicy {
    max_attempts: u32,
    initial_backoff: Duration,
    max_backoff: Duration,
}

impl RetryPolicy {
    pub(crate) fn new(max_attempts: u32, initial_backoff: Duration, max_backoff: Duration) -> Self {
        Self {
            max_attempts,
            initial_backoff,
            max_backoff: max_backoff.max(initial_backoff),
        }
    }

    pub(crate) const fn max_attempts(self) -> u32 {
        self.max_attempts
    }

    pub(crate) fn delay(self, attempt: u32, entropy: u64) -> Duration {
        let exponent = attempt.saturating_sub(1).min(31);
        let base_ms = self
            .initial_backoff
            .as_millis()
            .saturating_mul(1u128 << exponent)
            .min(self.max_backoff.as_millis());
        let jitter_window = (base_ms / 5).max(1);
        let jitter = u128::from(entropy) % jitter_window;
        let delay_ms = base_ms
            .saturating_add(jitter)
            .min(self.max_backoff.as_millis())
            .min(u128::from(u64::MAX));
        Duration::from_millis(delay_ms as u64)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub(crate) struct QuarantineRecord {
    pub(crate) timer_id: TimerId,
    pub(crate) reason: CorruptionReason,
    pub(crate) source_offset: i64,
    pub(crate) attempts: u32,
}

pub(crate) struct QuarantineManifest {
    path: PathBuf,
    records: Mutex<BTreeMap<TimerId, QuarantineRecord>>,
}

impl QuarantineManifest {
    pub(crate) fn new(store_root: &str) -> Self {
        Self {
            path: Path::new(store_root).join("timer-v2").join("quarantine.jsonl"),
            records: Mutex::new(BTreeMap::new()),
        }
    }

    pub(crate) fn load(&self) -> std::io::Result<()> {
        let content = match std::fs::read(&self.path) {
            Ok(content) => content,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
        };
        let mut recovered = BTreeMap::new();
        let mut valid_end = 0usize;
        let mut cursor = 0usize;
        let mut append_newline = false;
        let mut truncate_tail = false;
        for encoded_line in content.split_inclusive(|byte| *byte == b'\n') {
            cursor = cursor.saturating_add(encoded_line.len());
            let terminated = encoded_line.ends_with(b"\n");
            let line = encoded_line.strip_suffix(b"\n").unwrap_or(encoded_line);
            let line = line.strip_suffix(b"\r").unwrap_or(line);
            if line.iter().all(|byte| byte.is_ascii_whitespace()) {
                if terminated {
                    valid_end = cursor;
                }
                continue;
            }
            match serde_json::from_slice::<QuarantineRecord>(line) {
                Ok(record) => {
                    recovered.insert(record.timer_id, record);
                    if terminated {
                        valid_end = cursor;
                    } else {
                        append_newline = true;
                    }
                }
                Err(_) if !terminated => {
                    truncate_tail = true;
                    break;
                }
                Err(error) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("invalid timer quarantine record: {error}"),
                    ));
                }
            }
        }
        if truncate_tail {
            let file = OpenOptions::new().write(true).open(&self.path)?;
            file.set_len(valid_end as u64)?;
            file.sync_data()?;
        } else if append_newline {
            let mut file = OpenOptions::new().append(true).open(&self.path)?;
            file.write_all(b"\n")?;
            file.sync_data()?;
        }
        *self.records.lock() = recovered;
        Ok(())
    }

    pub(crate) fn record(&self, record: QuarantineRecord) -> std::io::Result<bool> {
        let mut records = self.records.lock();
        if records.get(&record.timer_id) == Some(&record) {
            return Ok(false);
        }
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new().create(true).append(true).open(&self.path)?;
        serde_json::to_writer(&mut file, &record)
            .map_err(|error| std::io::Error::other(format!("encode timer quarantine record: {error}")))?;
        file.write_all(b"\n")?;
        file.sync_data()?;
        records.insert(record.timer_id, record);
        Ok(true)
    }

    pub(crate) fn snapshot(&self) -> Vec<QuarantineRecord> {
        self.records.lock().values().cloned().collect()
    }
}

#[derive(Debug, Error)]
pub(crate) enum TimerEngineError {
    #[error("timer engine is not loaded")]
    NotLoaded,
    #[error("timer engine mode is unsupported: {0}")]
    UnsupportedMode(&'static str),
    #[error("timer work budget is invalid")]
    InvalidBudget,
    #[error("timer pipeline is closed")]
    PipelineClosed,
    #[error("timer storage operation failed: {0}")]
    Storage(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timer_retry_delay_is_bounded_and_contains_deterministic_jitter() {
        let policy = RetryPolicy::new(8, Duration::from_millis(10), Duration::from_millis(100));
        assert_eq!(policy.delay(1, 7), policy.delay(1, 7));
        assert!(policy.delay(1, 7) >= Duration::from_millis(10));
        assert!(policy.delay(20, 7) <= Duration::from_millis(100));
    }

    #[test]
    fn timer_quarantine_manifest_is_durable_and_idempotent() {
        let directory = tempfile::tempdir().expect("quarantine root");
        let manifest = QuarantineManifest::new(&directory.path().to_string_lossy());
        manifest.load().expect("empty manifest");
        let record = QuarantineRecord {
            timer_id: TimerId::new(7),
            reason: CorruptionReason::MissingPayload,
            source_offset: 9,
            attempts: 3,
        };
        assert!(manifest.record(record.clone()).expect("record"));
        assert!(!manifest.record(record.clone()).expect("deduplicate"));

        let recovered = QuarantineManifest::new(&directory.path().to_string_lossy());
        recovered.load().expect("recover");
        assert_eq!(recovered.snapshot(), vec![record]);
    }

    #[test]
    fn timer_quarantine_manifest_repairs_a_torn_tail_before_appending() {
        let directory = tempfile::tempdir().expect("quarantine root");
        let manifest = QuarantineManifest::new(&directory.path().to_string_lossy());
        let first = QuarantineRecord {
            timer_id: TimerId::new(7),
            reason: CorruptionReason::MissingPayload,
            source_offset: 9,
            attempts: 3,
        };
        assert!(manifest.record(first.clone()).expect("first record"));
        OpenOptions::new()
            .append(true)
            .open(&manifest.path)
            .expect("manifest")
            .write_all(b"{\"timer_id\":")
            .expect("torn tail");

        let recovered = QuarantineManifest::new(&directory.path().to_string_lossy());
        recovered.load().expect("repair torn tail");
        assert_eq!(recovered.snapshot(), vec![first]);
        let second = QuarantineRecord {
            timer_id: TimerId::new(8),
            reason: CorruptionReason::UnsupportedRecord,
            source_offset: 10,
            attempts: 1,
        };
        assert!(recovered.record(second.clone()).expect("append after repair"));

        let reloaded = QuarantineManifest::new(&directory.path().to_string_lossy());
        reloaded.load().expect("re-read repaired manifest");
        assert_eq!(
            reloaded.snapshot(),
            vec![
                QuarantineRecord {
                    timer_id: TimerId::new(7),
                    reason: CorruptionReason::MissingPayload,
                    source_offset: 9,
                    attempts: 3,
                },
                second,
            ]
        );
    }
}
