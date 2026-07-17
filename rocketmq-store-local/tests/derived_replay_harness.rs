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

use std::cell::Cell;
use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::HashSet;
use std::error::Error as StdError;
use std::fmt;
use std::rc::Rc;

use bytes::Bytes;
use rocketmq_store_api::DerivedCheckpoint;
use rocketmq_store_api::DerivedCheckpointDecodeError;
use rocketmq_store_api::DerivedCursor;
use rocketmq_store_api::DerivedEngine;
use rocketmq_store_api::DerivedRecordId;
use rocketmq_store_api::LegacyDerivedCursorV0;
use rocketmq_store_api::DERIVED_CHECKPOINT_ENCODED_LEN;
use rocketmq_store_local::commit_log::record::CommitLogFrameSource;
use rocketmq_store_local::commit_log::record::MESSAGE_MAGIC_CODE;
use rocketmq_store_local::derived::replay_derived;
use rocketmq_store_local::derived::CheckpointPersistence;
use rocketmq_store_local::derived::DerivedCursorOwner;
use rocketmq_store_local::derived::DerivedCursorOwnerError;
use rocketmq_store_local::derived::DerivedReplayApply;
use rocketmq_store_local::derived::DerivedReplayError;
use rocketmq_store_local::derived::DerivedReplaySink;
use rocketmq_store_local::derived::DerivedReplayStop;

const SOURCE_EPOCH: u64 = 41;

#[derive(Clone)]
struct BytesSource {
    bytes: Bytes,
}

impl BytesSource {
    fn new(bytes: impl Into<Bytes>) -> Self {
        Self { bytes: bytes.into() }
    }
}

impl CommitLogFrameSource for BytesSource {
    fn source_len(&self) -> usize {
        self.bytes.len()
    }

    fn read(&self, offset: usize, len: usize) -> Option<Bytes> {
        let end = offset.checked_add(len)?;
        self.bytes.get(offset..end).map(Bytes::copy_from_slice)
    }
}

fn frame(size: usize, fill: u8) -> Bytes {
    assert!(size >= 8);
    let mut bytes = vec![fill; size];
    bytes[..4].copy_from_slice(&(size as i32).to_be_bytes());
    bytes[4..8].copy_from_slice(&MESSAGE_MAGIC_CODE.to_be_bytes());
    Bytes::from(bytes)
}

fn golden_log() -> Bytes {
    let first = frame(8, 0xA1);
    let second = frame(12, 0xB2);
    let third = frame(16, 0xC3);
    Bytes::from([first.as_ref(), second.as_ref(), third.as_ref()].concat())
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum PersistFailure {
    #[default]
    None,
    BeforeWriteOnce,
    AfterWriteOnce,
}

#[derive(Default)]
struct PersistenceState {
    checkpoints: HashMap<DerivedEngine, Vec<u8>>,
    persist_calls: usize,
}

#[derive(Clone, Default)]
struct MemoryPersistence {
    state: Rc<RefCell<PersistenceState>>,
    failure: Rc<Cell<PersistFailure>>,
}

impl MemoryPersistence {
    fn fail_once(&self, failure: PersistFailure) {
        self.failure.set(failure);
    }

    fn raw_checkpoint(&self, engine: DerivedEngine) -> Option<Vec<u8>> {
        self.state.borrow().checkpoints.get(&engine).cloned()
    }

    fn persist_calls(&self) -> usize {
        self.state.borrow().persist_calls
    }

    fn corrupt(&self, engine: DerivedEngine, byte: usize) {
        if let Some(checkpoint) = self.state.borrow_mut().checkpoints.get_mut(&engine) {
            checkpoint[byte] ^= 0x80;
        }
    }
}

impl CheckpointPersistence for MemoryPersistence {
    type Error = TestError;

    fn load(&self, engine: DerivedEngine) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(self.raw_checkpoint(engine))
    }

    fn persist(
        &mut self,
        engine: DerivedEngine,
        checkpoint: &[u8; DERIVED_CHECKPOINT_ENCODED_LEN],
    ) -> Result<(), Self::Error> {
        self.state.borrow_mut().persist_calls += 1;
        let failure = self.failure.replace(PersistFailure::None);
        if failure == PersistFailure::BeforeWriteOnce {
            return Err(TestError("injected failure before checkpoint write"));
        }
        self.state.borrow_mut().checkpoints.insert(engine, checkpoint.to_vec());
        if failure == PersistFailure::AfterWriteOnce {
            return Err(TestError("injected uncertain checkpoint result"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TestError(&'static str);

impl fmt::Display for TestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl StdError for TestError {}

#[derive(Default)]
struct IdempotentSink {
    visible: HashSet<DerivedRecordId>,
    attempts: usize,
    fail_after_apply_once: bool,
}

impl DerivedReplaySink for IdempotentSink {
    type Error = TestError;

    fn apply(&mut self, record: DerivedRecordId, _frame: &Bytes) -> Result<DerivedReplayApply, Self::Error> {
        self.attempts += 1;
        let inserted = self.visible.insert(record);
        if std::mem::take(&mut self.fail_after_apply_once) {
            return Err(TestError("injected crash after derived apply"));
        }
        Ok(if inserted {
            DerivedReplayApply::Applied
        } else {
            DerivedReplayApply::Duplicate
        })
    }
}

#[test]
fn golden_replay_commits_every_frame_contiguously_without_payload_metadata() {
    let persistence = MemoryPersistence::default();
    let mut owner = DerivedCursorOwner::open(DerivedEngine::Index, SOURCE_EPOCH, persistence.clone())
        .expect("owner opens at genesis");
    let mut sink = IdempotentSink::default();

    let report = replay_derived(BytesSource::new(golden_log()), &mut owner, &mut sink).expect("golden replay succeeds");

    assert_eq!(3, report.applied());
    assert_eq!(0, report.duplicates());
    assert_eq!(3, report.committed_records());
    assert_eq!(36, report.committed_offset());
    assert_eq!(DerivedReplayStop::EndOfSource, report.stop());
    assert_eq!(3, sink.visible.len());
    assert_eq!(3, persistence.persist_calls());

    let raw = persistence
        .raw_checkpoint(DerivedEngine::Index)
        .expect("checkpoint is durable");
    assert_eq!(DERIVED_CHECKPOINT_ENCODED_LEN, raw.len());
    assert_eq!(
        DerivedCursor::restore(SOURCE_EPOCH, 36),
        DerivedCheckpoint::decode(&raw, DerivedEngine::Index)
            .expect("checkpoint is valid")
            .cursor()
    );
    assert!(!golden_log()
        .windows(9)
        .any(|payload| raw.windows(payload.len()).any(|bytes| bytes == payload)));
}

#[test]
fn crash_after_apply_replays_the_same_key_idempotently_without_a_hole() {
    let persistence = MemoryPersistence::default();
    let mut owner = DerivedCursorOwner::open(DerivedEngine::Tiered, SOURCE_EPOCH, persistence.clone())
        .expect("owner opens at genesis");
    let mut sink = IdempotentSink {
        fail_after_apply_once: true,
        ..IdempotentSink::default()
    };

    let error = replay_derived(BytesSource::new(golden_log()), &mut owner, &mut sink)
        .expect_err("injected crash must stop replay");
    assert!(matches!(error, DerivedReplayError::Sink(TestError(_))));
    assert_eq!(0, owner.cursor().next_offset());
    assert_eq!(1, sink.visible.len());

    let persistence = owner.into_persistence();
    let mut restarted = DerivedCursorOwner::open(DerivedEngine::Tiered, SOURCE_EPOCH, persistence)
        .expect("restart reloads genesis cursor");
    let report = replay_derived(BytesSource::new(golden_log()), &mut restarted, &mut sink).expect("restart catches up");

    assert_eq!(2, report.applied());
    assert_eq!(1, report.duplicates());
    assert_eq!(3, report.committed_records());
    assert_eq!(36, restarted.cursor().next_offset());
    assert_eq!(3, sink.visible.len());
    assert_eq!(4, sink.attempts);
}

#[test]
fn uncertain_checkpoint_result_is_resolved_by_reload_without_regression() {
    let persistence = MemoryPersistence::default();
    persistence.fail_once(PersistFailure::AfterWriteOnce);
    let mut owner = DerivedCursorOwner::open(DerivedEngine::RocksDb, SOURCE_EPOCH, persistence.clone())
        .expect("owner opens at genesis");
    let mut sink = IdempotentSink::default();

    let error = replay_derived(BytesSource::new(golden_log()), &mut owner, &mut sink)
        .expect_err("uncertain persistence result must stop replay");
    assert!(matches!(
        error,
        DerivedReplayError::Owner(DerivedCursorOwnerError::Persistence(TestError(_)))
    ));
    assert_eq!(0, owner.cursor().next_offset());

    let mut restarted = DerivedCursorOwner::open(DerivedEngine::RocksDb, SOURCE_EPOCH, persistence)
        .expect("restart observes the durable checkpoint");
    assert_eq!(8, restarted.cursor().next_offset());
    let report =
        replay_derived(BytesSource::new(golden_log()), &mut restarted, &mut sink).expect("remaining frames replay");
    assert_eq!(2, report.applied());
    assert_eq!(0, report.duplicates());
    assert_eq!(36, report.committed_offset());
    assert_eq!(3, sink.visible.len());
}

#[test]
fn dirty_tail_stops_at_last_complete_frame_and_resumes_after_repair() {
    let complete = frame(8, 0x11);
    let repaired = frame(12, 0x22);
    let mut partial = repaired.to_vec();
    partial.truncate(7);
    let dirty = Bytes::from([complete.as_ref(), partial.as_slice()].concat());
    let clean = Bytes::from([complete.as_ref(), repaired.as_ref()].concat());
    let persistence = MemoryPersistence::default();
    let mut owner = DerivedCursorOwner::open(DerivedEngine::ConsumeQueue, SOURCE_EPOCH, persistence)
        .expect("owner opens at genesis");
    let mut sink = IdempotentSink::default();

    let dirty_report =
        replay_derived(BytesSource::new(dirty), &mut owner, &mut sink).expect("complete prefix is replayable");
    assert_eq!(DerivedReplayStop::DirtyTail, dirty_report.stop());
    assert_eq!(8, dirty_report.committed_offset());
    assert_eq!(1, sink.visible.len());

    let clean_report =
        replay_derived(BytesSource::new(clean), &mut owner, &mut sink).expect("repaired tail resumes from checkpoint");
    assert_eq!(DerivedReplayStop::EndOfSource, clean_report.stop());
    assert_eq!(1, clean_report.applied());
    assert_eq!(20, clean_report.committed_offset());
    assert_eq!(2, sink.visible.len());
}

#[test]
fn corrupted_checkpoint_fails_closed_without_replaying_from_zero() {
    let persistence = MemoryPersistence::default();
    let mut owner = DerivedCursorOwner::open(DerivedEngine::Compaction, SOURCE_EPOCH, persistence.clone())
        .expect("owner opens at genesis");
    let first = DerivedRecordId::try_new(SOURCE_EPOCH, 0, 8).expect("record is valid");
    owner.commit(first).expect("checkpoint commit succeeds");
    persistence.corrupt(DerivedEngine::Compaction, 20);

    let error = match DerivedCursorOwner::open(DerivedEngine::Compaction, SOURCE_EPOCH, persistence) {
        Ok(_) => panic!("corruption must fail readiness"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        DerivedCursorOwnerError::Checkpoint(DerivedCheckpointDecodeError::ChecksumMismatch)
    ));
}

#[test]
fn legacy_offset_is_upgraded_durably_before_owner_publication() {
    let persistence = MemoryPersistence::default();
    let legacy = LegacyDerivedCursorV0::new(20);
    let owner =
        DerivedCursorOwner::open_or_upgrade(DerivedEngine::Index, SOURCE_EPOCH, persistence.clone(), Some(legacy))
            .expect("legacy cursor upgrades");

    assert_eq!(DerivedCursor::restore(SOURCE_EPOCH, 20), owner.cursor());
    assert_eq!(1, persistence.persist_calls());
    let reopened = DerivedCursorOwner::open(DerivedEngine::Index, SOURCE_EPOCH, persistence)
        .expect("current checkpoint reopens without the legacy input");
    assert_eq!(owner.cursor(), reopened.cursor());
}

#[test]
fn each_engine_owns_an_isolated_checkpoint_namespace() {
    let persistence = MemoryPersistence::default();
    let mut index =
        DerivedCursorOwner::open(DerivedEngine::Index, SOURCE_EPOCH, persistence.clone()).expect("index owner opens");
    let mut tiered =
        DerivedCursorOwner::open(DerivedEngine::Tiered, SOURCE_EPOCH, persistence.clone()).expect("tiered owner opens");
    let first = DerivedRecordId::try_new(SOURCE_EPOCH, 0, 8).expect("record is valid");

    index.commit(first).expect("index checkpoint commits");
    assert!(persistence.raw_checkpoint(DerivedEngine::Index).is_some());
    assert!(persistence.raw_checkpoint(DerivedEngine::Tiered).is_none());

    tiered.commit(first).expect("tiered checkpoint commits independently");
    assert!(persistence.raw_checkpoint(DerivedEngine::Tiered).is_some());
}
