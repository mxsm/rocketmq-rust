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

//! Slot drain batches that are read and converted on several threads before delivery.

use std::collections::HashSet;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::Bytes;
use rocketmq_model::common::message::MessageConst;
use rocketmq_model::common::message::MessageTrait;
use rocketmq_runtime::common::time_utils::current_millis;
use tempfile::tempdir;

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

#[tokio::test]
async fn a_batch_prepared_on_several_threads_delivers_each_message_once() {
    let root = tempdir().unwrap();
    let (mut store, timer_store) = open_store(root.path(), |_| {}).await;
    let deliver_ms = current_millis() as i64 - 2_000;
    let mut messages = (0..40)
        .map(|index| timer_message(deliver_ms, &format!("m{index}")))
        .collect::<Vec<_>>();
    let mut recall = with_property(
        timer_message(deliver_ms, "m7-recall"),
        MessageConst::PROPERTY_TIMER_DEL_UNIQKEY,
        build_delete_key(REAL_TOPIC, "m7").as_str(),
    );
    recall.set_body(Bytes::from_static(b"0"));
    messages.push(recall);
    put_all(&mut store, messages).await;
    assert_eq!(timer_store.process_once().await, 41);

    timer_store.set_should_running_dequeue(true);
    process_until_idle(&timer_store).await;
    store.reput_once().await;

    let delivered = delivered(&store);
    let mut expected = keys(0..40);
    expected.remove("m7");
    assert_eq!(delivered.len(), expected.len());
    assert_eq!(
        delivered.iter().map(|(key, _)| key.clone()).collect::<HashSet<_>>(),
        expected
    );
    // These records carry no admission token, so each gets its own replay token.
    let tokens = delivered
        .iter()
        .map(|(_, token)| token.clone().expect("a delivered message carries a token"))
        .collect::<HashSet<_>>();
    assert_eq!(tokens.len(), delivered.len());
    store.shutdown().await;
}

#[tokio::test]
async fn a_batch_prepared_on_several_threads_stops_at_its_first_message_not_due() {
    let root = tempdir().unwrap();
    let start_ms = (current_millis() as i64 / 1_000) * 1_000;
    let clock = Arc::new(ManualClock(AtomicI64::new(start_ms)));
    let slot_ms = start_ms + 10_000;
    let (mut store, timer_store) = open_store(root.path(), |_| {}).await;
    timer_store.set_clock(clock.clone());
    // All 41 share one wheel slot, which opens once the clock passes `slot_ms + 2_000`. The
    // twenty-first message is due only later, and it sits in the second of three prepare chunks.
    put_all(
        &mut store,
        (0..41)
            .map(|index| {
                let original_deliver_ms = if index == 20 { slot_ms + 2_600 } else { slot_ms + 100 };
                with_property(
                    timer_message(slot_ms + 100, &format!("m{index}")),
                    MessageConst::PROPERTY_TIMER_ORIGINAL_DELIVER_MS,
                    original_deliver_ms.to_string(),
                )
            })
            .collect(),
    )
    .await;
    assert_eq!(timer_store.process_once().await, 41);

    clock.0.store(slot_ms + 2_200, Ordering::Release);
    timer_store.set_should_running_dequeue(true);
    process_until_idle(&timer_store).await;
    store.reput_once().await;
    assert_eq!(delivered_keys(&store), keys(0..20));

    clock.0.store(slot_ms + 3_000, Ordering::Release);
    process_until_idle(&timer_store).await;
    store.reput_once().await;
    assert_eq!(delivered_keys(&store), keys(0..41));
    store.shutdown().await;
}
