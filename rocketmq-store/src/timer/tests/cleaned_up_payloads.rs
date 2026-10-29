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

//! Timers and recalls whose CommitLog payload cleanup deleted are skipped, as in Apache
//! RocketMQ, with the default `timerSkipUnknownError = false`: nothing brings the payload back,
//! and stopping on it would hold back every later timer.

use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::Bytes;
use cheetah_string::CheetahString;
use rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_model::common::message::MessageConst;
use rocketmq_model::common::message::MessageTrait;
use tempfile::tempdir;
use tempfile::TempDir;

use super::support::delivered;
use super::support::delivered_keys;
use super::support::keys;
use super::support::open_store;
use super::support::process_until_idle;
use super::support::put_all;
use super::support::timer_message;
use super::support::with_property;
use super::support::ManualClock;
use super::support::REAL_TOPIC;
use crate::base::backend_ops::BackendOps;
use crate::queue::consume_queue_store::ConsumeQueueStoreTrait;
use crate::queue::local_file_consume_queue_store::ConsumeQueueStore;
use crate::timer::timer_message_store::build_delete_key;
use crate::timer::timer_message_store::TimerMessageStore;
use crate::timer::timer_message_store::TIMER_TOPIC;
use crate::LocalFileMessageStore;

/// A second boundary for the manual clock; every timer here is due at it.
const SECOND_MS: i64 = 1_700_000_000_000;
/// CommitLog files this small hold one test timer each.
const COMMIT_LOG_FILE_SIZE: usize = 2_048;
/// Bodies this large keep a second message out of a timer's CommitLog file.
const BODY_BYTES: usize = 1_500;
/// Timer CQ files of two entries each.
const CONSUME_QUEUE_FILE_SIZE: usize = 2 * 20;

struct CleanupStore {
    _root: TempDir,
    store: LocalFileMessageStore,
    timer_store: Arc<TimerMessageStore>,
    clock: Arc<ManualClock>,
}

impl CleanupStore {
    async fn open() -> Self {
        let root = tempdir().unwrap();
        let clock = Arc::new(ManualClock(AtomicI64::new(SECOND_MS - 700)));
        let (store, timer_store) = open_store(root.path(), |config| {
            config.mapped_file_size_commit_log = COMMIT_LOG_FILE_SIZE;
            config.mapped_file_size_consume_queue = CONSUME_QUEUE_FILE_SIZE;
        })
        .await;
        timer_store.set_clock(clock.clone());
        timer_store.set_should_running_dequeue(true);
        Self {
            _root: root,
            store,
            timer_store,
            clock,
        }
    }

    async fn put(&mut self, messages: Vec<MessageExtBrokerInner>) {
        put_all(&mut self.store, messages).await;
    }

    async fn process(&mut self) {
        process_until_idle(&self.timer_store).await;
        self.store.reput_once().await;
    }

    /// Deletes the oldest `files` CommitLog files, as cleanup under disk pressure does.
    fn delete_commit_log_files(&mut self, files: i32) {
        let deleted = self
            .store
            .get_commit_log_mut()
            .delete_expired_files_by_time(0, 0, 0, true, files);
        assert_eq!(deleted, files);
    }

    /// Lets the slot of every timer here end.
    fn pass_the_slot(&self) {
        self.clock.0.store(SECOND_MS + 1_100, Ordering::Release);
    }
}

/// Timer `m{index}`, due at `SECOND_MS`, alone in its CommitLog file.
fn timer(index: usize) -> MessageExtBrokerInner {
    let mut message = timer_message(SECOND_MS, &format!("m{index}"));
    message.set_body(Bytes::from(vec![b'x'; BODY_BYTES]));
    message
}

#[tokio::test]
async fn a_timer_whose_payload_cleanup_deleted_is_skipped_when_its_slot_drains() {
    let mut store = CleanupStore::open().await;
    store.put(vec![timer(0), timer(1), timer(2)]).await;
    store.process().await;

    // The first file starts at offset 0, the second does not.
    store.delete_commit_log_files(2);
    store.pass_the_slot();
    store.process().await;

    assert_eq!(delivered_keys(&store.store), keys(2..3));
    assert_eq!(delivered(&store.store).len(), 1, "nothing is delivered twice");
    assert_eq!(store.timer_store.quarantine_count(), 0);
    store.store.shutdown().await;
}

#[tokio::test]
async fn a_timer_source_whose_payload_cleanup_deleted_is_skipped() {
    let mut store = CleanupStore::open().await;
    store.put(vec![timer(0), timer(1), timer(2)]).await;

    // The first file starts at offset 0, the second does not.
    store.delete_commit_log_files(2);
    store.process().await;
    store.pass_the_slot();
    store.process().await;

    assert_eq!(delivered_keys(&store.store), keys(2..3));
    assert_eq!(delivered(&store.store).len(), 1, "nothing is delivered twice");
    assert_eq!(store.timer_store.quarantine_count(), 0);
    store.store.shutdown().await;
}

#[tokio::test]
async fn an_overdue_timer_source_whose_payload_cleanup_deleted_is_not_written() {
    let mut store = CleanupStore::open().await;
    store.process().await;
    // The read cursor and the visibility watermark pass the slot, so the Source stage writes
    // the timers below straight to their topic.
    store.pass_the_slot();
    store.process().await;
    store.put(vec![timer(0), timer(1), timer(2)]).await;

    // A read below the oldest CommitLog file left returns that file's bytes.
    store.delete_commit_log_files(2);
    store.process().await;

    assert_eq!(delivered_keys(&store.store), keys(2..3));
    assert_eq!(delivered(&store.store).len(), 1, "nothing is delivered twice");
    store.store.shutdown().await;
}

#[tokio::test]
async fn the_source_stage_moves_past_timer_cq_entries_cleanup_removed() {
    let mut store = CleanupStore::open().await;
    store.put(vec![timer(0), timer(1), timer(2)]).await;

    store.delete_commit_log_files(2);
    let min_commit_log_offset = store.store.get_min_phy_offset();
    let timer_queue = store
        .store
        .find_consume_queue(&CheetahString::from_static_str(TIMER_TOPIC), 0)
        .expect("the Timer CQ exists");
    let queue_store = store
        .store
        .get_queue_store()
        .downcast_ref::<ConsumeQueueStore>()
        .expect("a local file consume queue store");
    queue_store.delete_expired_file(&**timer_queue.read(), min_commit_log_offset);
    timer_queue.read().correct_min_offset(min_commit_log_offset);
    assert_eq!(timer_queue.read().get_min_offset_in_queue(), 2);

    store.process().await;
    store.pass_the_slot();
    store.process().await;

    assert_eq!(delivered_keys(&store.store), keys(2..3));
    assert_eq!(delivered(&store.store).len(), 1, "nothing is delivered twice");
    store.store.shutdown().await;
}

#[tokio::test]
async fn a_recall_whose_payload_cleanup_deleted_cancels_nothing() {
    let mut store = CleanupStore::open().await;
    let mut recall = with_property(
        timer_message(SECOND_MS, "m0-recall"),
        MessageConst::PROPERTY_TIMER_DEL_UNIQKEY,
        build_delete_key(REAL_TOPIC, "m0").as_str(),
    );
    recall.set_body(Bytes::from(vec![b'0'; BODY_BYTES]));
    store.put(vec![recall, timer(0)]).await;
    store.process().await;

    store.delete_commit_log_files(1);
    store.pass_the_slot();
    store.process().await;

    assert_eq!(delivered_keys(&store.store), keys(0..1));
    assert_eq!(delivered(&store.store).len(), 1, "nothing is delivered twice");
    assert_eq!(store.timer_store.quarantine_count(), 0);
    store.store.shutdown().await;
}
