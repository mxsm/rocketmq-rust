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

//! Timers that cannot be handled: by default they stop the timers behind them, and with
//! `timerSkipUnknownError` they are quarantined and skipped as in Apache RocketMQ.

use rocketmq_model::common::message::MessageConst;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_store_api::TimerEngineId;
use tempfile::tempdir;

use super::support::delivered_keys;
use super::support::open_store;
use super::support::process_until_idle;
use super::support::put_all;
use super::support::timer_message;
use super::support::with_property;
use crate::base::backend_ops::MessageStoreEngine;
use crate::MessageStoreConfig;

/// Delivers `m0`, `bad` and `m1`, all due, and returns the keys delivered and the number of
/// quarantined timers.
async fn deliver_around(
    bad: impl FnOnce(i64) -> rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner,
    skip_unknown_error: bool,
) -> (std::collections::HashSet<String>, usize) {
    let root = tempdir().unwrap();
    let (mut store, timer_store) = open_store(root.path(), |config: &mut MessageStoreConfig| {
        config.timer_skip_unknown_error = skip_unknown_error;
    })
    .await;
    let deliver_ms = current_millis() as i64 - 2_000;
    timer_store.set_should_running_dequeue(true);
    put_all(
        &mut store,
        vec![
            timer_message(deliver_ms, "m0"),
            bad(deliver_ms),
            timer_message(deliver_ms, "m1"),
        ],
    )
    .await;
    process_until_idle(&timer_store).await;
    store.reput_once().await;
    let outcome = (delivered_keys(&store), timer_store.quarantine_count());
    store.shutdown().await;
    outcome
}

fn key_set(keys: &[&str]) -> std::collections::HashSet<String> {
    keys.iter().map(|key| (*key).to_owned()).collect()
}

/// Claims the JavaCompat route but carries none of its envelope, so enqueue cannot accept it.
fn unroutable(deliver_ms: i64) -> rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner {
    with_property(
        timer_message(deliver_ms, "bad"),
        MessageConst::TIMER_ENGINE_TYPE,
        TimerEngineId::JavaCompat.as_str(),
    )
}

/// Enqueues normally but names a queue that cannot be parsed, so it cannot be delivered.
fn unconvertible(deliver_ms: i64) -> rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner {
    with_property(
        timer_message(deliver_ms, "bad"),
        MessageConst::PROPERTY_REAL_QUEUE_ID,
        "not-a-queue",
    )
}

#[tokio::test]
async fn an_unroutable_timer_source_stops_enqueue_by_default() {
    let (delivered, quarantined) = deliver_around(unroutable, false).await;
    // Enqueue stops at the bad record, and delivery never passes a record enqueue has not.
    assert!(!delivered.contains("m1"));
    assert_eq!(quarantined, 1);
}

#[tokio::test]
async fn skipping_unknown_errors_enqueues_past_an_unroutable_timer_source() {
    assert_eq!(deliver_around(unroutable, true).await, (key_set(&["m0", "m1"]), 1));
}

#[tokio::test]
async fn an_unconvertible_timer_stops_its_slot_by_default() {
    assert_eq!(deliver_around(unconvertible, false).await, (key_set(&["m0"]), 1));
}

#[tokio::test]
async fn skipping_unknown_errors_delivers_past_an_unconvertible_timer() {
    assert_eq!(deliver_around(unconvertible, true).await, (key_set(&["m0", "m1"]), 1));
}
