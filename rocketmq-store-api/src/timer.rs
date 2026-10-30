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

use crate::StoreContractViolation;

/// Current route format written by the Java-compatible timer engine.
pub const JAVA_COMPAT_TIMER_FORMAT_VERSION: u16 = 1;

/// Current record format written by the extended timeline engine.
pub const EXTENDED_TIMELINE_FORMAT_VERSION: u16 = 1;

/// Current route format written by the native timer engine.
pub const NATIVE_TIMER_FORMAT_VERSION: u16 = 1;

/// Broker-to-store request for generation-aware Extended Timer cancellation.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct TimerRecallRequest {
    /// Original real topic encoded in the Recall handle.
    pub topic: String,
    /// Original unique message id encoded in the Recall handle.
    pub unique_key: String,
}

/// Broker-to-store registration of a native timer recall, made before its tombstone is
/// written so a delivery batch taken in between still sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimerRecallRegistration {
    /// The key the tombstone carries in `TIMER_DEL_UNIQKEY`.
    pub key: String,
    /// The generation the tombstone carries.
    pub generation: u64,
    /// The deadline the Recall handle names.
    pub deliver_ms: i64,
}

/// Stable outcome of a typed Timer cancellation attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimerRecallStatus {
    /// Pending/ready generation was atomically cancelled.
    Cancelled,
    /// It had already been cancelled.
    AlreadyCancelled,
    /// Delivery crossed the READY-to-DELIVERING linearization point.
    TooLate,
    /// Lookup does not exist on this owner.
    NotFound,
    /// Generation or state should be retried after materialization catches up.
    Retry,
    /// Corrupt/quarantined state requires repair.
    Quarantined,
    /// Backend does not own the Extended Timeline capability.
    Unsupported,
}

/// Configured timer storage mode.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TimerStoreMode {
    /// RocketMQ Java-compatible timer wheel and timer log.
    #[default]
    JavaCompat,
    /// Native long-horizon timeline. This mode must fail closed until the capability is installed.
    ExtendedTimeline,
    /// Tiered time-bucket log for delays of up to `timerNative.maxDelayDays`. Timers the
    /// Java-compatible engine admitted before the switch are still delivered by it.
    Native,
}

impl TimerStoreMode {
    /// Returns the stable configuration value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::JavaCompat => "java_compat",
            Self::ExtendedTimeline => "extended_timeline",
            Self::Native => "native",
        }
    }
}

/// On-disk format of the Java-compatible engine's TimerLog, TimerWheel and checkpoint.
///
/// The default is [`Java`](Self::Java), so a Rust Broker can replace a Java Broker in place
/// without losing its pending timers. A store that still holds `rust_v2` timers delivers them
/// from those files while new timers go to the configured format.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TimerFileFormat {
    /// Segmented, checksummed Rust files with A/B wheel pages.
    RustV2,
    /// The Apache RocketMQ 5.x `timerlog`, `timerwheel` and `timercheck` files, so the store can
    /// take over a Java Broker's pending timers and hand them back.
    #[default]
    Java,
}

impl TimerFileFormat {
    /// Returns the stable configuration value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RustV2 => "rust_v2",
            Self::Java => "java",
        }
    }
}

/// How new timer messages are admitted under one [`TimerStoreMode`]: the engine that owns them
/// and the limits that engine applies. Admission, recall handles and recall read this instead of
/// deciding per mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimerAdmissionRoute {
    /// Engine that owns the timer messages admitted now.
    pub engine: TimerEngineId,
    /// Format version stamped on each admitted message.
    pub format_version: u16,
    /// Longest delay the engine admits, in seconds.
    pub max_delay_sec: u64,
    /// Whether the engine admits new timer messages at all.
    pub admission_open: bool,
    /// Whether admission rejects a message whose wheel slot is congested.
    pub rejects_congested_slots: bool,
}

impl TimerAdmissionRoute {
    /// Returns the route of `mode`. The Java-compatible engine admits up to
    /// `java_compat_max_delay_sec` and checks wheel slot congestion; the Extended Timeline admits
    /// up to `extended_horizon_days` while `extended_admission_enable` is set; the native engine
    /// admits up to `native_max_delay_days`.
    pub const fn for_mode(
        mode: TimerStoreMode,
        java_compat_max_delay_sec: u64,
        extended_horizon_days: u16,
        extended_admission_enable: bool,
        native_max_delay_days: u16,
    ) -> Self {
        match mode {
            TimerStoreMode::JavaCompat => Self {
                engine: TimerEngineId::JavaCompat,
                format_version: JAVA_COMPAT_TIMER_FORMAT_VERSION,
                max_delay_sec: java_compat_max_delay_sec,
                admission_open: true,
                rejects_congested_slots: true,
            },
            TimerStoreMode::ExtendedTimeline => Self {
                engine: TimerEngineId::ExtendedTimeline,
                format_version: EXTENDED_TIMELINE_FORMAT_VERSION,
                max_delay_sec: (extended_horizon_days as u64).saturating_mul(86_400),
                admission_open: extended_admission_enable,
                rejects_congested_slots: false,
            },
            TimerStoreMode::Native => Self {
                engine: TimerEngineId::Native,
                format_version: NATIVE_TIMER_FORMAT_VERSION,
                max_delay_sec: (native_max_delay_days as u64).saturating_mul(86_400),
                admission_open: true,
                rejects_congested_slots: false,
            },
        }
    }

    /// Longest admitted delay in milliseconds, or `None` when it does not fit in `u64`.
    pub const fn max_delay_ms(&self) -> Option<u64> {
        self.max_delay_sec.checked_mul(1_000)
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
    /// Native tiered time-bucket log, encoded as `N`.
    #[serde(rename = "N")]
    Native,
}

impl TimerEngineId {
    /// Returns the persistent wire value used in internal message properties.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::JavaCompat => "F",
            Self::ExtendedTimeline => "R",
            Self::Native => "N",
        }
    }

    /// Parses a persistent engine identifier and rejects unknown owners.
    ///
    /// # Errors
    ///
    /// Returns [`StoreContractViolation::TimerUnknownEngine`] when `value` is not a known engine id.
    pub fn parse(value: &str) -> Result<Self, StoreContractViolation> {
        match value {
            "F" => Ok(Self::JavaCompat),
            "R" => Ok(Self::ExtendedTimeline),
            "N" => Ok(Self::Native),
            value => Err(StoreContractViolation::TimerUnknownEngine(value.to_owned())),
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
    /// Returns [`StoreContractViolation::TimerInvalidPayloadLocator`] for a negative offset or zero size.
    pub const fn try_new(commit_log_offset: i64, size: u32) -> Result<Self, StoreContractViolation> {
        if commit_log_offset < 0 || size == 0 {
            return Err(StoreContractViolation::TimerInvalidPayloadLocator);
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

/// Stable location of one complete message in the long-horizon payload store.
///
/// Unlike [`TimerPayloadLocator`], this locator does not depend on the ordinary CommitLog
/// retention window. Its partition is derived from the original UTC due day and delivery lane.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Hash, Serialize)]
pub struct TimerPayloadStoreLocator {
    due_day_utc: i32,
    lane: u16,
    segment_id: u64,
    offset: u64,
    length: u32,
    checksum: u32,
}

impl TimerPayloadStoreLocator {
    /// Creates a validated payload-store locator.
    ///
    /// # Errors
    ///
    /// Returns [`StoreContractViolation::TimerInvalidPayloadStoreLocator`] when `length` is zero.
    pub const fn try_new(
        due_day_utc: i32,
        lane: u16,
        segment_id: u64,
        offset: u64,
        length: u32,
        checksum: u32,
    ) -> Result<Self, StoreContractViolation> {
        if length == 0 {
            return Err(StoreContractViolation::TimerInvalidPayloadStoreLocator);
        }
        Ok(Self {
            due_day_utc,
            lane,
            segment_id,
            offset,
            length,
            checksum,
        })
    }

    /// Returns the UTC day number since the Unix epoch.
    pub const fn due_day_utc(self) -> i32 {
        self.due_day_utc
    }

    /// Returns the stable delivery lane.
    pub const fn lane(self) -> u16 {
        self.lane
    }

    /// Returns the partition-local segment id.
    pub const fn segment_id(self) -> u64 {
        self.segment_id
    }

    /// Returns the byte offset within the segment.
    pub const fn offset(self) -> u64 {
        self.offset
    }

    /// Returns the encoded record length.
    pub const fn length(self) -> u32 {
        self.length
    }

    /// Returns the CRC32C of the complete encoded payload record.
    pub const fn checksum(self) -> u32 {
        self.checksum
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
    /// Returns [`StoreContractViolation::TimerInvalidRoute`] when the version or token is empty.
    pub fn try_new(
        engine_id: TimerEngineId,
        format_version: u16,
        normalization_policy_fingerprint: u64,
        generation: TimerGeneration,
        delivery_token: impl Into<String>,
    ) -> Result<Self, StoreContractViolation> {
        let delivery_token = delivery_token.into();
        if format_version == 0 || delivery_token.is_empty() {
            return Err(StoreContractViolation::TimerInvalidRoute);
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
