// Copyright 2023 The RocketMQ Rust Authors
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

use std::path::Path;
use std::sync::Arc;

use rocketmq_store_local::timer::checkpoint::TimerCheckpointRecord;
use rocketmq_store_local::timer::checkpoint::TimerCheckpointV2Record;
use rocketmq_store_local::timer::checkpoint::VersionedTimerCheckpointStore;
use rocketmq_store_local::timer::metrics::TimerStorageMetrics;
use rocketmq_store_local::timer::service::TimerLogRecord;
use rocketmq_store_local::timer::timer_log::TimerLog;
use rocketmq_store_local::timer::timer_wheel::TimerWheel;
use tempfile::tempdir;

#[test]
fn fixed_v1_fixture_migrates_without_changing_pending_records() {
    let directory = tempdir().unwrap();
    copy_fixture(directory.path());

    let timer_log = TimerLog::new(directory.path().join("timerlog"), 800);
    timer_log.load().unwrap();
    assert_eq!(timer_log.len().unwrap(), 80);
    let first = TimerLogRecord::decode(&timer_log.read_at(0, TimerLogRecord::SIZE).unwrap()).unwrap();
    let second = TimerLogRecord::decode(&timer_log.read_at(40, TimerLogRecord::SIZE).unwrap()).unwrap();
    assert_eq!((first.queue_offset, first.prev_pos), (7, -1));
    assert_eq!((second.queue_offset, second.prev_pos), (8, 0));

    let metrics = Arc::new(TimerStorageMetrics::default());
    let wheel = TimerWheel::with_page_size_and_metrics(directory.path().join("timerwheel"), 16, 1_000, 288, metrics);
    wheel.load_at_generation(0).unwrap();
    let slot = wheel.get_slot(5_000).unwrap();
    assert_eq!((slot.first_pos, slot.last_pos, slot.num, slot.magic), (0, 40, 2, 5));
    let wheel_generation = wheel.flush_generation().unwrap();

    let legacy_checkpoint =
        TimerCheckpointRecord::decode(&std::fs::read(directory.path().join("timercheck")).unwrap()).unwrap();
    assert_eq!(legacy_checkpoint.last_timer_log_flush_pos, 80);
    assert_eq!(legacy_checkpoint.last_timer_queue_offset, 9);
    let checkpoint_store = VersionedTimerCheckpointStore::new(directory.path().join("timercheck"));
    checkpoint_store.load_best(|_| Ok(())).unwrap();
    checkpoint_store
        .commit(TimerCheckpointV2Record {
            durable_queue_offset: legacy_checkpoint.last_timer_queue_offset,
            timer_log_durable_length: legacy_checkpoint.last_timer_log_flush_pos,
            dequeue_slot_ms: legacy_checkpoint.last_read_time_ms,
            wheel_generation,
            master_queue_offset: legacy_checkpoint.master_timer_queue_offset,
            data_version: legacy_checkpoint.version,
            ..TimerCheckpointV2Record::default()
        })
        .unwrap();
    wheel.commit_generation(wheel_generation).unwrap();

    let reloaded = TimerWheel::with_page_size_and_metrics(
        directory.path().join("timerwheel"),
        16,
        1_000,
        288,
        Arc::new(TimerStorageMetrics::default()),
    );
    reloaded.load_at_generation(wheel_generation).unwrap();
    assert_eq!(reloaded.get_slot(5_000).unwrap().num, 2);
}

#[test]
fn interrupted_temporary_migration_is_rebuilt_from_v1_fixture() {
    let directory = tempdir().unwrap();
    copy_fixture(directory.path());
    std::fs::create_dir_all(directory.path().join("timerlog/v2.migrating/log")).unwrap();
    std::fs::write(directory.path().join("timerlog/v2.migrating/log/partial"), b"partial").unwrap();

    let timer_log = TimerLog::new(directory.path().join("timerlog"), 800);
    timer_log.load().unwrap();
    assert_eq!(timer_log.len().unwrap(), 80);
    assert!(!directory.path().join("timerlog/v2.migrating").exists());
    assert!(directory.path().join("timerlog/v2/MIGRATION_COMMITTED").exists());
}

fn copy_fixture(target: &Path) {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("../rocketmq-store-local/tests/fixtures/timer/rust-v1");
    std::fs::create_dir_all(target.join("timerlog")).unwrap();
    std::fs::copy(
        fixture.join("timerlog/00000000000000000000"),
        target.join("timerlog/00000000000000000000"),
    )
    .unwrap();
    std::fs::copy(fixture.join("timerwheel"), target.join("timerwheel")).unwrap();
    std::fs::copy(fixture.join("timercheck"), target.join("timercheck")).unwrap();
}
