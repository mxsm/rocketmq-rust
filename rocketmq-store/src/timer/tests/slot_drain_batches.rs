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
use std::path::Path;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::Bytes;
use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_model::common::config::TopicConfig;
use rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_model::common::message::MessageConst;
use rocketmq_model::common::message::MessageTrait;
use rocketmq_protocol::common::message::message_decoder::message_properties_to_string;
use rocketmq_runtime::common::time_utils::current_millis;
use tempfile::tempdir;

use crate::base::backend_ops::BackendOps;
use crate::base::message_store_reader::MessageStoreReader;
use crate::timer::clock::TimerClock;
use crate::timer::timer_message_store::build_delete_key;
use crate::timer::timer_message_store::TimerMessageStore;
use crate::timer::timer_message_store::TIMER_TOPIC;
use crate::LocalFileMessageStore;
use crate::MessageStoreConfig;
use crate::StoreRuntimeConfig;

const REAL_TOPIC: &str = "slot_batch_topic";
/// One due batch of 64 messages, split three ways.
const BATCH_MESSAGES: usize = 64;
const PREPARE_THREADS: usize = 3;

struct ManualClock(AtomicI64);

impl TimerClock for ManualClock {
    fn wall_time_ms(&self) -> i64 {
        self.0.load(Ordering::Acquire)
    }

    fn monotonic_elapsed_ms(&self) -> u64 {
        self.wall_time_ms().max(0) as u64
    }
}

async fn open_store(root_dir: &Path) -> (LocalFileMessageStore, Arc<TimerMessageStore>) {
    let config = Arc::new(MessageStoreConfig {
        store_path_root_dir: CheetahString::from_string(root_dir.to_string_lossy().into_owned()),
        read_uncommitted: true,
        max_msgs_num_batch: BATCH_MESSAGES,
        timer_get_message_thread_num: PREPARE_THREADS,
        timer_put_message_thread_num: 1,
        timer_source_batch_messages: BATCH_MESSAGES,
        timer_due_batch_messages: BATCH_MESSAGES,
        ..MessageStoreConfig::default()
    });
    let topic_config_table = Arc::new(DashMap::new());
    for topic in [REAL_TOPIC, TIMER_TOPIC] {
        topic_config_table.insert(CheetahString::from_static_str(topic), Arc::new(TopicConfig::default()));
    }
    let mut store = LocalFileMessageStore::new(
        config,
        rocketmq_store_local::commit_log::append::micro_batch::MicroBatchPolicy::disabled(1)
            .expect("valid test policy"),
        Arc::new(StoreRuntimeConfig::default()),
        topic_config_table,
        None,
        false,
        crate::runtime::test_service_context("timer-slot-batch-test"),
    )
    .expect("create Timer test Store")
    .expect("test Timer Store configuration is valid");
    store
        .wire_owned_root_dependencies()
        .expect("Timer tests should wire owned Store capabilities");
    let timer_store = store
        .get_timer_message_store()
        .cloned()
        .expect("Timer should be enabled for Timer tests");
    assert!(store.init().await.is_ok());
    assert!(store.load().await);
    (store, timer_store)
}

fn timer_message(deliver_ms: i64, original_deliver_ms: Option<i64>, unique_key: &str) -> MessageExtBrokerInner {
    let mut message = MessageExtBrokerInner::default();
    message.set_topic(CheetahString::from_static_str(TIMER_TOPIC));
    message.message_ext_inner.queue_id = 0;
    message.set_body(Bytes::from_static(b"slot-batch-body"));
    let mut properties = vec![
        (MessageConst::PROPERTY_REAL_TOPIC, REAL_TOPIC.to_owned()),
        (MessageConst::PROPERTY_REAL_QUEUE_ID, "0".to_owned()),
        (MessageConst::PROPERTY_TIMER_OUT_MS, deliver_ms.to_string()),
        (
            MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
            unique_key.to_owned(),
        ),
    ];
    if let Some(original_deliver_ms) = original_deliver_ms {
        properties.push((
            MessageConst::PROPERTY_TIMER_ORIGINAL_DELIVER_MS,
            original_deliver_ms.to_string(),
        ));
    }
    for (key, value) in properties {
        message.put_property(CheetahString::from_static_str(key), CheetahString::from_string(value));
    }
    message.properties_string = message_properties_to_string(message.get_properties());
    message
}

async fn put_all(store: &mut LocalFileMessageStore, messages: Vec<MessageExtBrokerInner>) {
    for message in messages {
        assert!(store.put_message(message).await.is_ok());
    }
    store.reput_once().await;
}

/// Runs `process_once` until a pass does no work.
async fn process_until_idle(timer_store: &TimerMessageStore) {
    while timer_store.process_once().await > 0 {}
}

fn property(message: &rocketmq_model::common::message::message_ext::MessageExt, name: &'static str) -> Option<String> {
    message
        .property(&CheetahString::from_static_str(name))
        .map(|value| value.as_str().to_owned())
}

/// Unique keys and delivery tokens of the messages delivered so far, in queue order.
fn delivered(store: &LocalFileMessageStore) -> Vec<(String, Option<String>)> {
    let topic = CheetahString::from_static_str(REAL_TOPIC);
    (0..store.get_max_offset_in_queue(&topic, 0))
        .map(|queue_offset| {
            let commit_log_offset = store.get_commit_log_offset_in_queue(&topic, 0, queue_offset);
            let message = store
                .look_message_by_offset(commit_log_offset)
                .expect("a delivered message is readable");
            for timer_property in [
                MessageConst::PROPERTY_REAL_TOPIC,
                MessageConst::PROPERTY_REAL_QUEUE_ID,
                MessageConst::PROPERTY_TIMER_OUT_MS,
                MessageConst::PROPERTY_TIMER_ORIGINAL_DELIVER_MS,
            ] {
                assert_eq!(property(&message, timer_property), None);
            }
            (
                property(&message, MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
                    .expect("the unique key survives delivery"),
                property(&message, MessageConst::PROPERTY_TIMER_DELIVERY_TOKEN),
            )
        })
        .collect()
}

fn keys(range: std::ops::Range<usize>) -> HashSet<String> {
    range.map(|index| format!("m{index}")).collect()
}

#[tokio::test]
async fn a_batch_prepared_on_several_threads_delivers_each_message_once() {
    let root = tempdir().unwrap();
    let (mut store, timer_store) = open_store(root.path()).await;
    let deliver_ms = current_millis() as i64 - 2_000;
    let mut messages = (0..40)
        .map(|index| timer_message(deliver_ms, None, &format!("m{index}")))
        .collect::<Vec<_>>();
    let mut recall = timer_message(deliver_ms, None, "m7-recall");
    recall.set_body(Bytes::from_static(b"0"));
    recall.put_property(
        CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_DEL_UNIQKEY),
        build_delete_key(REAL_TOPIC, "m7"),
    );
    recall.properties_string = message_properties_to_string(recall.get_properties());
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
    let (mut store, timer_store) = open_store(root.path()).await;
    timer_store.set_clock(clock.clone());
    // All 41 share one wheel slot, which opens once the clock passes `slot_ms + 2_000`. The
    // twenty-first message is due only later, and it sits in the second of three prepare chunks.
    put_all(
        &mut store,
        (0..41)
            .map(|index| {
                let original_deliver_ms = if index == 20 { slot_ms + 2_600 } else { slot_ms + 100 };
                timer_message(slot_ms + 100, Some(original_deliver_ms), &format!("m{index}"))
            })
            .collect(),
    )
    .await;
    assert_eq!(timer_store.process_once().await, 41);

    clock.0.store(slot_ms + 2_200, Ordering::Release);
    timer_store.set_should_running_dequeue(true);
    process_until_idle(&timer_store).await;
    store.reput_once().await;
    assert_eq!(
        delivered(&store)
            .into_iter()
            .map(|(key, _)| key)
            .collect::<HashSet<_>>(),
        keys(0..20)
    );

    clock.0.store(slot_ms + 3_000, Ordering::Release);
    process_until_idle(&timer_store).await;
    store.reput_once().await;
    assert_eq!(
        delivered(&store)
            .into_iter()
            .map(|(key, _)| key)
            .collect::<HashSet<_>>(),
        keys(0..41)
    );
    store.shutdown().await;
}
