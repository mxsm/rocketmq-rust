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

//! Stable timer routing and storage identities shared across storage backends.

use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

/// Current route format written by the Java-compatible timer engine.
pub const JAVA_COMPAT_TIMER_FORMAT_VERSION: u16 = 1;

/// Configured timer storage mode.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TimerStoreMode {
    /// RocketMQ Java-compatible timer wheel and timer log.
    #[default]
    JavaCompat,
    /// Native long-horizon timeline. This mode must fail closed until the capability is installed.
    ExtendedTimeline,
}

impl TimerStoreMode {
    /// Returns the stable configuration value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::JavaCompat => "java_compat",
            Self::ExtendedTimeline => "extended_timeline",
        }
    }
}

/// Stable identifier of the engine that owns one accepted timer record.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Hash, Serialize)]
pub enum TimerEngineId {
    /// File timer wheel, encoded as `F` by RocketMQ Java.
    #[serde(rename = "F")]
    JavaCompat,
    /// Extended timeline, encoded as `R` for the existing RocksDB protocol value.
    #[serde(rename = "R")]
    ExtendedTimeline,
}

impl TimerEngineId {
    /// Returns the persistent wire value used in internal message properties.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::JavaCompat => "F",
            Self::ExtendedTimeline => "R",
        }
    }

    /// Parses a persistent engine identifier and rejects unknown owners.
    ///
    /// # Errors
    ///
    /// Returns [`TimerContractError::UnknownEngine`] when `value` is not a known engine id.
    pub fn parse(value: &str) -> Result<Self, TimerContractError> {
        match value {
            "F" => Ok(Self::JavaCompat),
            "R" => Ok(Self::ExtendedTimeline),
            value => Err(TimerContractError::UnknownEngine(value.to_owned())),
        }
    }
}

/// Stable logical timer identity.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
pub struct TimerId(u128);

impl TimerId {
    /// Creates a timer id from its persisted integer representation.
    pub const fn new(value: u128) -> Self {
        Self(value)
    }

    /// Returns the persisted integer representation.
    pub const fn get(self) -> u128 {
        self.0
    }
}

/// Generation fencing stale recall and delivery records for one timer identity.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
pub struct TimerGeneration(u64);

impl TimerGeneration {
    /// Creates a generation.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the persisted generation.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Epoch fencing work produced by an earlier engine owner.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
pub struct TimerEngineEpoch(u64);

impl TimerEngineEpoch {
    /// Creates an engine epoch.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the persisted epoch.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Offset of a source record in the internal timer consume queue.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
pub struct TimerSourceCqOffset(i64);

impl TimerSourceCqOffset {
    /// Creates a source offset.
    pub const fn new(value: i64) -> Self {
        Self(value)
    }

    /// Returns the source offset.
    pub const fn get(self) -> i64 {
        self.0
    }
}

/// Location of the canonical message payload in CommitLog.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Hash, Serialize)]
pub struct TimerPayloadLocator {
    commit_log_offset: i64,
    size: u32,
}

impl TimerPayloadLocator {
    /// Creates a payload locator.
    ///
    /// # Errors
    ///
    /// Returns [`TimerContractError::InvalidPayloadLocator`] for a negative offset or zero size.
    pub const fn try_new(commit_log_offset: i64, size: u32) -> Result<Self, TimerContractError> {
        if commit_log_offset < 0 || size == 0 {
            return Err(TimerContractError::InvalidPayloadLocator);
        }
        Ok(Self {
            commit_log_offset,
            size,
        })
    }

    /// Returns the physical CommitLog offset.
    pub const fn commit_log_offset(self) -> i64 {
        self.commit_log_offset
    }

    /// Returns the encoded payload size.
    pub const fn size(self) -> u32 {
        self.size
    }
}

/// Ordered position in a timer timeline.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
pub struct TimerTimelineCursor {
    due_time_ms: i64,
    sequence: u64,
}

impl TimerTimelineCursor {
    /// Creates a timeline cursor.
    pub const fn new(due_time_ms: i64, sequence: u64) -> Self {
        Self { due_time_ms, sequence }
    }

    /// Returns the inclusive due timestamp represented by this cursor.
    pub const fn due_time_ms(self) -> i64 {
        self.due_time_ms
    }

    /// Returns the deterministic tie-break sequence.
    pub const fn sequence(self) -> u64 {
        self.sequence
    }
}

/// Immutable route persisted on first durable admission.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct PersistedTimerRoute {
    engine_id: TimerEngineId,
    format_version: u16,
    normalization_policy_fingerprint: u64,
    generation: TimerGeneration,
    delivery_token: String,
}

impl PersistedTimerRoute {
    /// Creates and validates an immutable route.
    ///
    /// # Errors
    ///
    /// Returns [`TimerContractError::InvalidRoute`] when the version or token is empty.
    pub fn try_new(
        engine_id: TimerEngineId,
        format_version: u16,
        normalization_policy_fingerprint: u64,
        generation: TimerGeneration,
        delivery_token: impl Into<String>,
    ) -> Result<Self, TimerContractError> {
        let delivery_token = delivery_token.into();
        if format_version == 0 || delivery_token.is_empty() {
            return Err(TimerContractError::InvalidRoute);
        }
        Ok(Self {
            engine_id,
            format_version,
            normalization_policy_fingerprint,
            generation,
            delivery_token,
        })
    }

    /// Returns the owning engine.
    pub const fn engine_id(&self) -> TimerEngineId {
        self.engine_id
    }

    /// Returns the persistent record format version.
    pub const fn format_version(&self) -> u16 {
        self.format_version
    }

    /// Returns the normalization policy fingerprint captured at admission.
    pub const fn normalization_policy_fingerprint(&self) -> u64 {
        self.normalization_policy_fingerprint
    }

    /// Returns the logical schedule generation.
    pub const fn generation(&self) -> TimerGeneration {
        self.generation
    }

    /// Returns the stable delivery idempotency token.
    pub fn delivery_token(&self) -> &str {
        &self.delivery_token
    }
}

/// Validation error for stable timer contracts.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum TimerContractError {
    /// The persisted engine id is not recognized.
    #[error("unknown timer engine id: {0}")]
    UnknownEngine(String),
    /// The payload locator cannot identify a non-empty CommitLog record.
    #[error("timer payload locator requires a non-negative offset and non-zero size")]
    InvalidPayloadLocator,
    /// The immutable route is incomplete.
    #[error("timer route requires a non-zero format version and non-empty delivery token")]
    InvalidRoute,
}
