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

//! Integration tests for timer recovery semantics across LocalFileMessageStore restarts.
//!
//! These tests exercise the default store path where LocalFileMessageStore
//! auto-initializes the timer subsystem, instead of wiring TimerMessageStore
//! manually inside the timer module's unit tests.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::Bytes;
use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::MessageDecoder::message_properties_to_string;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use rocketmq_store::message_store::local_file_message_store::LocalFileMessageStore;
use rocketmq_store::store_path_config_helper::get_timer_check_path;
use rocketmq_store::timer::timer_checkpoint::TimerCheckpoint;
use rocketmq_store::timer::timer_message_store::TIMER_OUT_MS;
use rocketmq_store::timer::timer_message_store::TIMER_TOPIC;
use tempfile::TempDir;

fn timer_store_config(temp_dir: &TempDir) -> MessageStoreConfig {
    MessageStoreConfig {
        store_path_root_dir: temp_dir.path().to_string_lossy().to_string().into(),
        read_uncommitted: true,
        mapped_file_size_commit_log: 4096,
        mapped_file_size_consume_queue: 200,
        timer_precision_ms: 100,
        timer_roll_window_slot: 512,
        ..MessageStoreConfig::default()
    }
}

fn new_test_store(temp_dir: &TempDir) -> ArcMut<LocalFileMessageStore> {
    let real_topic = CheetahString::from_static_str("phase6-timer-real-topic");
    let broker_config = Arc::new(BrokerConfig::default());
    let topic_config_table: Arc<DashMap<CheetahString, ArcMut<TopicConfig>>> = Arc::new(DashMap::new());
    topic_config_table.insert(real_topic, ArcMut::new(TopicConfig::default()));
    topic_config_table.insert(
        CheetahString::from_static_str(TIMER_TOPIC),
        ArcMut::new(TopicConfig::default()),
    );

    let mut store = ArcMut::new(LocalFileMessageStore::new(
        Arc::new(timer_store_config(temp_dir)),
        broker_config,
        topic_config_table,
        None,
        false,
    ));
    let store_clone = store.clone();
    store.set_message_store_arc(store_clone);
    store
}

fn build_timer_message(real_topic: &CheetahString, deliver_ms: u64) -> MessageExtBrokerInner {
    let mut msg = MessageExtBrokerInner::default();
    msg.set_topic(CheetahString::from_static_str(TIMER_TOPIC));
    msg.message_ext_inner.queue_id = 0;
    msg.set_body(Bytes::from_static(b"phase6-timer-body"));
    msg.put_property(
        CheetahString::from_static_str(MessageConst::PROPERTY_REAL_TOPIC),
        real_topic.clone(),
    );
    msg.put_property(
        CheetahString::from_static_str(MessageConst::PROPERTY_REAL_QUEUE_ID),
        CheetahString::from_string("0".to_string()),
    );
    msg.put_property(
        CheetahString::from_static_str(TIMER_OUT_MS),
        CheetahString::from_string(deliver_ms.to_string()),
    );
    msg.properties_string = message_properties_to_string(msg.get_properties());
    msg
}

#[tokio::test]
async fn restart_with_auto_initialized_timer_store_redelivers_due_message() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let real_topic = CheetahString::from_static_str("phase6-timer-real-topic");

    let mut writer = new_test_store(&temp_dir);
    writer.init().await.expect("init writer");
    assert!(writer.load().await, "load writer");

    let timer_message_store = writer
        .get_timer_message_store()
        .cloned()
        .expect("timer message store should be auto-initialized");

    let put_result = writer
        .put_message(build_timer_message(
            &real_topic,
            current_millis().saturating_sub(2_000) as u64,
        ))
        .await;
    assert!(put_result.is_ok(), "put timer message");

    writer.reput_once().await;
    assert_eq!(timer_message_store.process_once().await, 1);
    writer.shutdown().await;
    drop(writer);

    let mut reloaded = new_test_store(&temp_dir);
    reloaded.init().await.expect("init reloaded store");
    assert!(reloaded.load().await, "load reloaded store");

    let reloaded_timer_message_store = reloaded
        .get_timer_message_store()
        .cloned()
        .expect("timer message store should be auto-initialized after restart");
    reloaded_timer_message_store.set_should_running_dequeue(true);

    let delivered = reloaded_timer_message_store.process_once().await;
    reloaded.reput_once().await;

    assert_eq!(delivered, 1);
    assert_eq!(reloaded.get_max_offset_in_queue(&real_topic, 0), 1);
    assert_eq!(
        reloaded_timer_message_store.curr_queue_offset.load(Ordering::Relaxed),
        1
    );
}

#[tokio::test]
async fn restart_revises_lagging_timer_checkpoint_in_default_store_path() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let real_topic = CheetahString::from_static_str("phase6-timer-real-topic");

    let mut writer = new_test_store(&temp_dir);
    writer.init().await.expect("init writer");
    assert!(writer.load().await, "load writer");

    let timer_message_store = writer
        .get_timer_message_store()
        .cloned()
        .expect("timer message store should be auto-initialized");

    let put_result = writer
        .put_message(build_timer_message(&real_topic, current_millis() as u64 + 60_000))
        .await;
    assert!(put_result.is_ok(), "put timer message");

    writer.reput_once().await;
    assert_eq!(timer_message_store.process_once().await, 1);
    writer.shutdown().await;
    drop(writer);

    let checkpoint = TimerCheckpoint::new(get_timer_check_path(temp_dir.path().to_string_lossy().as_ref()))
        .expect("open timer checkpoint");
    checkpoint.set_last_timer_queue_offset(99);
    checkpoint.set_master_timer_queue_offset(99);
    checkpoint.flush().expect("flush timer checkpoint");

    let mut reloaded = new_test_store(&temp_dir);
    reloaded.init().await.expect("init reloaded store");
    assert!(reloaded.load().await, "load reloaded store");

    let reloaded_timer_message_store = reloaded
        .get_timer_message_store()
        .cloned()
        .expect("timer message store should be auto-initialized after restart");

    assert_eq!(
        reloaded_timer_message_store.curr_queue_offset.load(Ordering::Relaxed),
        1
    );
}
