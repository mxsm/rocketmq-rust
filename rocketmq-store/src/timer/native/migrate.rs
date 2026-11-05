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

//! Offline migration of the Extended Timeline's timers to the native engine.
//!
//! A store the Extended Timeline owns loads in no other mode, so its timers have no way to drain
//! into another engine. This moves the ones not yet delivered into the native engine, each one
//! copied with its message, and then puts the Extended Timeline's files aside, so that the store
//! loads under `timerStoreMode = native` and no engine delivers them a second time.
//!
//! A migration that stops partway resumes: an in-progress mark names the Extended directory's
//! new name, and the native files of an attempt that did not finish are written again.

use std::fs;
use std::path::Path;
use std::path::PathBuf;

use bytes::Bytes;
use cheetah_string::CheetahString;
use rocketmq_model::common::message::MessageConst;
use rocketmq_model::common::message::MessageTrait;
use rocketmq_protocol::common::message::message_decoder as MessageDecoder;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_store_api::StoreComponent;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreOperation;
use rocketmq_store_local::timer::native_format::NATIVE_TIMER_DIR;
use rocketmq_store_local::timer::payload_store::TimerPayloadStore;
use rocketmq_store_local::timer::payload_store::TimerPayloadStoreConfig;
use rocketmq_store_rocksdb::timer::codec::TimelineKeyV1;
use rocketmq_store_rocksdb::timer::state_index::TimelineState;
use rocketmq_store_rocksdb::timer::state_index::TimelineStateEntry;
use rocketmq_store_rocksdb::timer::timeline_index::RocksDbTimelineIndex;
use serde::Serialize;

use super::config::NativeTimerConfig;
use super::engine::NativeSourceKind;
use super::engine::NativeTimerEngine;
use super::engine::NativeTimerSource;
use crate::config::message_store_config::MessageStoreConfig;

/// Everything the Extended Timeline keeps, under the store root.
const EXTENDED_DIR: &str = "timer-extended";
/// The online move of the Timeline index into native runs, which this does not support.
const INDEX_MIGRATION_DIR: &str = "timer-extended/index-migration-v1";
/// The mark of the Extended Timeline owning the store.
const OWNER_MARKER: &str = "config/timer-store-owner.meta";
/// The mark of a migration under way; it holds the name the Extended directory is moved to.
const IN_PROGRESS_MARKER: &str = "config/timer-extended-migration.meta";
/// State records read at a time.
const PAGE_RECORDS: usize = 1024;

/// Whether a migration changes the store.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExtendedTimerMigrationMode {
    /// Count what would move, and change nothing.
    DryRun,
    /// Move the timers and put the Extended Timeline's files aside.
    Apply,
}

/// Why a migration does not start. Nothing is changed then.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase", tag = "kind")]
pub enum ExtendedTimerMigrationBlocker {
    /// Timers whose message the Extended Timeline has not copied yet. Running the Broker in
    /// `extended_timeline` mode until it has, then stopping it, removes them.
    PayloadHandoffPending { timers: u64 },
    /// Timers due later than `timerNative.maxDelayDays` admits; raise it to at least
    /// `required_days`.
    BeyondNativeMaxDelay { timers: u64, required_days: u64 },
}

/// What a migration found, and did.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExtendedTimerMigrationReport {
    /// Nothing was changed.
    pub dry_run: bool,
    /// Timers handed to the native engine, or that would be.
    pub migrated: u64,
    /// Of those, timers whose delivery was under way: the native engine may deliver them a
    /// second time.
    pub delivering: u64,
    /// Timers whose message was written, only their completion was not recorded; left out.
    pub committed: u64,
    /// Delivered, cancelled and quarantined timers; left out.
    pub finished: u64,
    /// Shadow records, which never deliver; left out.
    pub shadow: u64,
    /// What keeps the migration from starting.
    pub blockers: Vec<ExtendedTimerMigrationBlocker>,
    /// Where the Extended Timeline's files were moved, relative to the store root.
    pub backup: Option<String>,
}

/// Moves the Extended Timeline's timers in the store `config` describes to the native engine.
///
/// The Broker must be stopped, and the caller holds the store's lock. In
/// [`ExtendedTimerMigrationMode::Apply`], and only without blockers, the native engine indexes
/// every pending, ready or delivering timer with a copy of its message, and checkpoints them. The
/// `timer-extended` directory is then renamed, and the owner marker with it, so the store loads
/// under `timerStoreMode = native`, which also needs `timerWheelEnable = true`.
///
/// # Errors
///
/// Fails when the store is not owned by the Extended Timeline, when its Timeline index is moving
/// to native runs, when a record cannot be read, or when the native engine cannot be written. A
/// failed migration leaves the Extended Timeline in place and can be run again.
pub fn migrate_extended_timers_to_native(
    config: &MessageStoreConfig,
    mode: ExtendedTimerMigrationMode,
) -> Result<ExtendedTimerMigrationReport, StoreError> {
    let root = PathBuf::from(config.store_path_root_dir.as_str());
    if let Some(backup) = finish_interrupted(&root)? {
        return Ok(ExtendedTimerMigrationReport {
            dry_run: mode == ExtendedTimerMigrationMode::DryRun,
            backup: Some(backup),
            ..ExtendedTimerMigrationReport::default()
        });
    }
    if !root.join(OWNER_MARKER).exists() {
        return Err(migration_error("the Extended Timeline does not own this store"));
    }
    if root.join(INDEX_MIGRATION_DIR).exists() {
        return Err(migration_error(
            "the Extended Timeline index is moving to native runs, which this migration does not read",
        ));
    }
    let timeline = RocksDbTimelineIndex::open(&root)?
        .ok_or_else(|| migration_error("the Extended Timeline index cannot be opened"))?;
    let native_config = NativeTimerConfig::from_store(config);
    let now_ms = current_millis() as i64;

    let mut report = survey(&timeline, &native_config, now_ms)?;
    report.dry_run = mode == ExtendedTimerMigrationMode::DryRun;
    if report.dry_run || !report.blockers.is_empty() {
        close(timeline);
        return Ok(report);
    }

    let backup = format!("{EXTENDED_DIR}.migrated-{now_ms}");
    begin(&root, &backup)?;
    let moved = move_timers(config, &root, &timeline, native_config, now_ms);
    // Before the Extended directory moves: Windows does not rename one with open files.
    close(timeline);
    let moved = moved?;
    if moved != report.migrated {
        return Err(migration_error(
            "the Extended Timeline changed while it was migrated; is the Broker stopped?",
        ));
    }
    put_extended_aside(&root, &backup)?;
    report.backup = Some(backup);
    Ok(report)
}

fn close(timeline: RocksDbTimelineIndex) {
    timeline.close();
    drop(timeline);
}

/// Counts the timers by what becomes of them, and what would keep them from moving.
fn survey(
    timeline: &RocksDbTimelineIndex,
    native_config: &NativeTimerConfig,
    now_ms: i64,
) -> Result<ExtendedTimerMigrationReport, StoreError> {
    let mut report = ExtendedTimerMigrationReport::default();
    let mut source_only = 0;
    let mut beyond = 0;
    let mut latest_due_ms = i64::MIN;
    each_state(timeline, |entry| {
        match classify(entry) {
            Fate::Shadow => report.shadow += 1,
            Fate::Finished => report.finished += 1,
            Fate::Committed => report.committed += 1,
            Fate::SourceOnly => source_only += 1,
            Fate::Moves { delivering } => {
                report.migrated += 1;
                report.delivering += u64::from(delivering);
                let due_ms = entry.record.due_time_ms;
                if due_ms > now_ms.saturating_add(native_config.max_delay_ms) {
                    beyond += 1;
                    latest_due_ms = latest_due_ms.max(due_ms);
                }
            }
        }
        Ok(())
    })?;
    if source_only > 0 {
        report
            .blockers
            .push(ExtendedTimerMigrationBlocker::PayloadHandoffPending { timers: source_only });
    }
    if beyond > 0 {
        let required_days = latest_due_ms
            .saturating_sub(now_ms)
            .div_euclid(86_400_000)
            .saturating_add(1);
        report
            .blockers
            .push(ExtendedTimerMigrationBlocker::BeyondNativeMaxDelay {
                timers: beyond,
                required_days: u64::try_from(required_days).unwrap_or(u64::MAX),
            });
    }
    Ok(report)
}

/// Copies the moving timers into the native engine and checkpoints them, and returns how many.
fn move_timers(
    config: &MessageStoreConfig,
    root: &Path,
    timeline: &RocksDbTimelineIndex,
    native_config: NativeTimerConfig,
    now_ms: i64,
) -> Result<u64, StoreError> {
    let payloads = open_payloads(config, root)?;
    let (engine, _) = NativeTimerEngine::open(root, native_config, now_ms).map_err(native_error)?;
    let mut moved = 0u64;
    let mut sources = Vec::with_capacity(PAGE_RECORDS);
    each_state(timeline, |entry| {
        if !matches!(classify(entry), Fate::Moves { .. }) {
            return Ok(());
        }
        sources.push(source_of(timeline, &payloads, entry)?);
        if sources.len() == PAGE_RECORDS {
            moved += adopt(&engine, &sources, now_ms)?;
            sources.clear();
        }
        Ok(())
    })?;
    moved += adopt(&engine, &sources, now_ms)?;
    // The copies need nothing of the CommitLog.
    engine.checkpoint(i64::MAX, now_ms).map_err(native_error)?;
    Ok(moved)
}

fn adopt(engine: &NativeTimerEngine, sources: &[NativeTimerSource], now_ms: i64) -> Result<u64, StoreError> {
    if sources.is_empty() {
        return Ok(0);
    }
    let report = engine.adopt(sources, now_ms).map_err(native_error)?;
    if let Some((cq_offset, rejection)) = report.rejected.first() {
        return Err(migration_error(format!(
            "the native engine refused the timer of Timer CQ offset {cq_offset}: {rejection:?}"
        )));
    }
    Ok(u64::try_from(report.indexed).unwrap_or(u64::MAX))
}

/// The native source of a moving timer, read from its Timeline record and payload copy.
fn source_of(
    timeline: &RocksDbTimelineIndex,
    payloads: &TimerPayloadStore,
    entry: &TimelineStateEntry,
) -> Result<NativeTimerSource, StoreError> {
    let key = TimelineKeyV1 {
        due_time_ms: entry.record.due_time_ms,
        lane: entry.record.lane,
        timer_id: entry.timer_id,
        generation: entry.generation,
    };
    let record = timeline.get(key)?.ok_or_else(|| {
        migration_error(format!(
            "timer {:?} of generation {} has no Timeline record",
            entry.timer_id,
            entry.generation.get()
        ))
    })?;
    let payload = payloads.read(record.payload)?;
    let mut frame = Bytes::from(payload.frame);
    let message = MessageDecoder::decode(&mut frame.clone(), true, false, false, false, false)
        .ok_or_else(|| migration_error("a payload copy of the Extended Timeline does not decode"))?;
    let unique_key = message
        .property(&CheetahString::from_static_str(
            MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
        ))
        .map(|key| key.to_string())
        .unwrap_or_default();
    let size = i32::try_from(frame.len())
        .map_err(|_| migration_error("a payload copy of the Extended Timeline is too large"))?;
    Ok(NativeTimerSource {
        cq_offset: record.source_cq_offset.get(),
        commit_log_offset: record.source_physical_offset,
        size,
        due_ms: entry.record.due_time_ms,
        generation: entry.generation.get(),
        real_topic: payload.real_topic,
        real_queue_id: payload.real_queue_id,
        kind: NativeSourceKind::Timer { unique_key },
        frame: std::mem::take(&mut frame),
    })
}

/// What becomes of a timer of the Extended Timeline.
enum Fate {
    Shadow,
    /// Delivered, cancelled or quarantined.
    Finished,
    /// Written; only its completion was not recorded.
    Committed,
    /// Its message was not copied yet.
    SourceOnly,
    Moves {
        delivering: bool,
    },
}

fn classify(entry: &TimelineStateEntry) -> Fate {
    if entry.record.shadow_only {
        return Fate::Shadow;
    }
    match entry.record.state {
        TimelineState::SourceOnly => Fate::SourceOnly,
        TimelineState::Pending | TimelineState::Ready => Fate::Moves { delivering: false },
        TimelineState::Delivering => Fate::Moves { delivering: true },
        TimelineState::Committing => Fate::Committed,
        TimelineState::Delivered | TimelineState::Cancelled | TimelineState::Quarantined => Fate::Finished,
    }
}

fn each_state(
    timeline: &RocksDbTimelineIndex,
    mut visit: impl FnMut(&TimelineStateEntry) -> Result<(), StoreError>,
) -> Result<(), StoreError> {
    let states = timeline.state_index();
    let mut continuation = None;
    loop {
        let page = states.scan_after(continuation, PAGE_RECORDS)?;
        for entry in &page.entries {
            visit(entry)?;
        }
        match page.continuation {
            Some(next) => continuation = Some(next),
            None => return Ok(()),
        }
    }
}

fn open_payloads(config: &MessageStoreConfig, root: &Path) -> Result<TimerPayloadStore, StoreError> {
    let timer = &config.timer_store_config;
    let mut payload_config = TimerPayloadStoreConfig::for_store_root(root);
    payload_config.segment_bytes = timer.payload_segment_bytes;
    payload_config.max_open_handles = timer.payload_open_handles;
    payload_config.batch_bytes = timer.payload_batch_bytes;
    payload_config.max_record_bytes = timer.payload_record_bytes;
    payload_config.max_partition_live_bytes = timer.payload_partition_live_bytes;
    let payloads = TimerPayloadStore::new(payload_config)?
        .ok_or_else(|| migration_error("the Extended Timeline payload settings are inconsistent"))?;
    payloads.load()?;
    Ok(payloads)
}

/// Marks the migration under way, and clears the native files of one that did not finish.
fn begin(root: &Path, backup: &str) -> Result<(), StoreError> {
    let native_dir = root.join(NATIVE_TIMER_DIR);
    if native_dir.exists() {
        if !root.join(IN_PROGRESS_MARKER).exists() {
            return Err(migration_error(
                "the store already holds native timers; a migration needs an empty native engine",
            ));
        }
        fs::remove_dir_all(&native_dir).map_err(|error| io_error("clear an unfinished migration", error))?;
    }
    write_durably(&root.join(IN_PROGRESS_MARKER), backup)
}

/// Moves the Extended Timeline's files and owner marker aside, then the in-progress mark.
fn put_extended_aside(root: &Path, backup: &str) -> Result<(), StoreError> {
    fs::rename(root.join(EXTENDED_DIR), root.join(backup))
        .map_err(|error| io_error("move the Extended Timeline aside", error))?;
    fs::rename(root.join(OWNER_MARKER), root.join(format!("{OWNER_MARKER}.migrated")))
        .map_err(|error| io_error("move the Extended Timeline owner marker aside", error))?;
    fs::remove_file(root.join(IN_PROGRESS_MARKER)).map_err(|error| io_error("finish the migration", error))
}

/// Finishes a migration stopped after its timers moved, and returns where the Extended
/// Timeline went. A migration stopped earlier still finds the Extended directory, and runs again.
fn finish_interrupted(root: &Path) -> Result<Option<String>, StoreError> {
    let marker = root.join(IN_PROGRESS_MARKER);
    let Ok(backup) = fs::read_to_string(&marker) else {
        return Ok(None);
    };
    let backup = backup.trim().to_owned();
    if root.join(EXTENDED_DIR).exists() || !root.join(&backup).exists() {
        return Ok(None);
    }
    let owner = root.join(OWNER_MARKER);
    if owner.exists() {
        fs::rename(&owner, root.join(format!("{OWNER_MARKER}.migrated")))
            .map_err(|error| io_error("move the Extended Timeline owner marker aside", error))?;
    }
    fs::remove_file(&marker).map_err(|error| io_error("finish the migration", error))?;
    Ok(Some(backup))
}

fn write_durably(path: &Path, contents: &str) -> Result<(), StoreError> {
    let write = || -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let temp = path.with_extension("meta.tmp");
        let mut file = fs::File::create(&temp)?;
        std::io::Write::write_all(&mut file, contents.as_bytes())?;
        file.sync_all()?;
        fs::rename(&temp, path)
    };
    write().map_err(|error| io_error("mark the migration under way", error))
}

fn migration_error(detail: impl Into<String>) -> StoreError {
    StoreError::new(&rocketmq_error::STORAGE_WRITE_FAILED, StoreOperation::Admin)
        .in_component(StoreComponent::Store)
        .with_detail(detail.into())
}

fn io_error(what: &str, error: std::io::Error) -> StoreError {
    migration_error(format!("failed to {what}")).with_source(error)
}

fn native_error(error: impl std::error::Error + Send + Sync + 'static) -> StoreError {
    migration_error("the native timer engine cannot be written").with_source(error)
}
