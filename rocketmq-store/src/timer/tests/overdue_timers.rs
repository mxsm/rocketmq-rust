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

//! Timers already due when the Source stage reads them are written to their topics at once, as
//! in Apache RocketMQ, unless a recall may still be on its way to their slot.

use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::Bytes;
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
use crate::base::backend_ops::MessageStoreEngine;
use crate::timer::timer_message_store::build_delete_key;
use crate::timer::timer_message_store::TimerMessageStore;
use crate::LocalFileMessageStore;

/// A second boundary for the manual clock.
const SECOND_MS: i64 = 1_700_000_000_000;

struct LateTimerStore {
    _root: TempDir,
    store: LocalFileMessageStore,
    timer_store: Arc<TimerMessageStore>,
    clock: Arc<ManualClock>,
}

impl LateTimerStore {
    async fn open(start_ms: i64) -> Self {
        let root = tempdir().unwrap();
        let clock = Arc::new(ManualClock(AtomicI64::new(start_ms)));
        let (store, timer_store) = open_store(root.path(), |_| {}).await;
        timer_store.set_clock(clock.clone());
        timer_store.set_should_running_dequeue(true);
        Self {
            _root: root,
            store,
            timer_store,
            clock,
        }
    }

    fn set_time(&self, time_ms: i64) {
        self.clock.0.store(time_ms, Ordering::Release);
    }

    async fn process(&mut self) {
        process_until_idle(&self.timer_store).await;
        self.store.reput_once().await;
    }
}

/// A store at 300 ms into a second whose read cursor is past the slot of `m0`, delivered.
async fn store_past_the_read_slot() -> LateTimerStore {
    let mut store = LateTimerStore::open(SECOND_MS - 700).await;
    put_all(&mut store.store, vec![timer_message(SECOND_MS - 2_000, "m0")]).await;
    store.process().await;
    store.set_time(SECOND_MS + 300);
    store.process().await;
    assert_eq!(delivered_keys(&store.store), keys(0..1));
    store
}

#[tokio::test]
async fn an_overdue_timer_is_delivered_when_the_source_stage_reads_it() {
    let mut store = store_past_the_read_slot().await;

    // Indexed, it would wait in the read slot until this second ends.
    put_all(&mut store.store, vec![timer_message(SECOND_MS - 2_000, "m1")]).await;
    store.process().await;
    assert_eq!(delivered_keys(&store.store), keys(0..2));

    store.set_time(SECOND_MS + 2_300);
    store.process().await;
    assert_eq!(delivered(&store.store).len(), 2);
    store.store.shutdown().await;
}

#[tokio::test]
async fn the_pipeline_source_stage_delivers_an_overdue_timer() {
    let mut store = store_past_the_read_slot().await;

    put_all(&mut store.store, vec![timer_message(SECOND_MS - 2_000, "m1")]).await;
    let (processed, _) = store
        .timer_store
        .process_pipeline_enqueue_stage(16)
        .await
        .expect("the Source stage runs");
    store.store.reput_once().await;
    assert_eq!(processed, 1);
    assert_eq!(delivered_keys(&store.store), keys(0..2));
    store.store.shutdown().await;
}

#[tokio::test]
async fn a_recall_that_may_have_reached_the_timer_cq_in_time_still_cancels_an_overdue_timer() {
    // A Store that starts with a backlog: the read cursor starts at the current second, past
    // the slot of these due timers, but nothing shows yet when their recall became visible.
    let mut store = LateTimerStore::open(SECOND_MS + 1_100).await;
    let mut recall = with_property(
        timer_message(SECOND_MS, "m0-recall"),
        MessageConst::PROPERTY_TIMER_DEL_UNIQKEY,
        build_delete_key(REAL_TOPIC, "m0").as_str(),
    );
    recall.set_body(Bytes::from_static(b"0"));
    put_all(
        &mut store.store,
        vec![timer_message(SECOND_MS, "m0"), timer_message(SECOND_MS, "m1"), recall],
    )
    .await;
    store.process().await;

    store.set_time(SECOND_MS + 3_100);
    store.process().await;
    assert_eq!(delivered_keys(&store.store), keys(1..2));
    store.store.shutdown().await;
}
