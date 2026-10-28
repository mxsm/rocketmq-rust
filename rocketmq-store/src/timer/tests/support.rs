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

//! A Store with its JavaCompat timer, timer messages, and the messages it delivered, for
//! delivery scenarios driven through `process_once`.

use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::Bytes;
use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_model::common::config::TopicConfig;
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_model::common::message::MessageConst;
use rocketmq_model::common::message::MessageTrait;
use rocketmq_protocol::common::message::message_decoder::message_properties_to_string;

use crate::base::backend_ops::BackendOps;
use crate::base::message_store_reader::MessageStoreReader;
use crate::timer::clock::TimerClock;
use crate::timer::timer_message_store::TimerMessageStore;
use crate::timer::timer_message_store::TIMER_TOPIC;
use crate::LocalFileMessageStore;
use crate::MessageStoreConfig;
use crate::StoreRuntimeConfig;

pub(super) const REAL_TOPIC: &str = "timer_test_topic";
/// One due batch of 64 messages, read and converted on three threads.
pub(super) const BATCH_MESSAGES: usize = 64;
pub(super) const PREPARE_THREADS: usize = 3;

pub(super) struct ManualClock(pub(super) AtomicI64);

impl TimerClock for ManualClock {
    fn wall_time_ms(&self) -> i64 {
        self.0.load(Ordering::Acquire)
    }

    fn monotonic_elapsed_ms(&self) -> u64 {
        self.wall_time_ms().max(0) as u64
    }
}

/// Opens a Store under `root_dir`; `configure` adjusts the timer settings of the test.
pub(super) async fn open_store(
    root_dir: &Path,
    configure: impl FnOnce(&mut MessageStoreConfig),
) -> (LocalFileMessageStore, Arc<TimerMessageStore>) {
    let mut config = MessageStoreConfig {
        store_path_root_dir: CheetahString::from_string(root_dir.to_string_lossy().into_owned()),
        read_uncommitted: true,
        max_msgs_num_batch: BATCH_MESSAGES,
        timer_get_message_thread_num: PREPARE_THREADS,
        timer_put_message_thread_num: 1,
        timer_source_batch_messages: BATCH_MESSAGES,
        timer_due_batch_messages: BATCH_MESSAGES,
        ..MessageStoreConfig::default()
    };
    configure(&mut config);
    let topic_config_table = Arc::new(DashMap::new());
    for topic in [REAL_TOPIC, TIMER_TOPIC] {
        topic_config_table.insert(CheetahString::from_static_str(topic), Arc::new(TopicConfig::default()));
    }
    let mut store = LocalFileMessageStore::new(
        Arc::new(config),
        rocketmq_store_local::commit_log::append::micro_batch::MicroBatchPolicy::disabled(1)
            .expect("valid test policy"),
        Arc::new(StoreRuntimeConfig::default()),
        topic_config_table,
        None,
        false,
        crate::runtime::test_service_context("timer-delivery-test"),
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

/// A timer message for queue 0 of `REAL_TOPIC`, identified by its unique key.
pub(super) fn timer_message(deliver_ms: i64, unique_key: &str) -> MessageExtBrokerInner {
    let mut message = MessageExtBrokerInner::default();
    message.set_topic(CheetahString::from_static_str(TIMER_TOPIC));
    message.message_ext_inner.queue_id = 0;
    message.set_body(Bytes::from_static(b"timer-test-body"));
    for (key, value) in [
        (MessageConst::PROPERTY_REAL_TOPIC, REAL_TOPIC.to_owned()),
        (MessageConst::PROPERTY_REAL_QUEUE_ID, "0".to_owned()),
        (MessageConst::PROPERTY_TIMER_OUT_MS, deliver_ms.to_string()),
        (
            MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
            unique_key.to_owned(),
        ),
    ] {
        message.put_property(CheetahString::from_static_str(key), CheetahString::from_string(value));
    }
    message.properties_string = message_properties_to_string(message.get_properties());
    message
}

/// Sets one property of a timer message.
pub(super) fn with_property(
    mut message: MessageExtBrokerInner,
    key: &'static str,
    value: impl Into<String>,
) -> MessageExtBrokerInner {
    message.put_property(
        CheetahString::from_static_str(key),
        CheetahString::from_string(value.into()),
    );
    message.properties_string = message_properties_to_string(message.get_properties());
    message
}

pub(super) async fn put_all(store: &mut LocalFileMessageStore, messages: Vec<MessageExtBrokerInner>) {
    for message in messages {
        assert!(store.put_message(message).await.is_ok());
    }
    store.reput_once().await;
}

/// Runs `process_once` until a pass does no work.
pub(super) async fn process_until_idle(timer_store: &TimerMessageStore) {
    while timer_store.process_once().await > 0 {}
}

fn property(message: &MessageExt, name: &'static str) -> Option<String> {
    message
        .property(&CheetahString::from_static_str(name))
        .map(|value| value.as_str().to_owned())
}

/// Unique keys and delivery tokens of the messages delivered so far, in queue order. Each
/// delivered message must have left its timer properties behind.
pub(super) fn delivered(store: &LocalFileMessageStore) -> Vec<(String, Option<String>)> {
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

/// The unique keys delivered so far.
pub(super) fn delivered_keys(store: &LocalFileMessageStore) -> HashSet<String> {
    delivered(store).into_iter().map(|(key, _)| key).collect()
}

/// The keys `m{index}` for each index in `range`.
pub(super) fn keys(range: std::ops::Range<usize>) -> HashSet<String> {
    range.map(|index| format!("m{index}")).collect()
}
