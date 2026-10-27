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

//! The JavaCompat engine on Apache RocketMQ 5.x timer files (`timerFileFormat = java`), and
//! switching a store between its file formats.

use std::path::Path;
use std::path::PathBuf;
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
use rocketmq_store_api::TimerFileFormat;
use rocketmq_store_local::timer::java_format::java_timer_wheel_len;
use rocketmq_store_local::timer::java_format::java_topic_hash;
use rocketmq_store_local::timer::java_format::JavaTimerLogRecord;
use tempfile::tempdir;

use crate::base::backend_ops::BackendOps;
use crate::base::message_store_reader::MessageStoreReader;
use crate::store_path_config_helper::get_timer_log_path;
use crate::store_path_config_helper::get_timer_wheel_path;
use crate::timer::clock::TimerClock;
use crate::timer::timer_message_store::build_delete_key;
use crate::timer::timer_message_store::TimerMessageStore;
use crate::timer::timer_message_store::DAY_SECS;
use crate::timer::timer_message_store::TIMER_TOPIC;
use crate::timer::timer_message_store::TIMER_WHEEL_TTL_DAY;
use crate::LocalFileMessageStore;
use crate::MessageStoreConfig;
use crate::StoreRuntimeConfig;

const REAL_TOPIC: &str = "java_format_topic";
/// Three units fit in a file, so a handful of messages crosses a file boundary.
const TIMER_LOG_FILE_SIZE: usize = 200;
/// A file size both formats accept: V2 segments hold whole 80-byte records and 4 KiB pages.
const SHARED_TIMER_LOG_FILE_SIZE: usize = 20_480;

fn config(root_dir: &Path, format: TimerFileFormat, batch_messages: usize) -> Arc<MessageStoreConfig> {
    let mapped_file_size_timer_log = match format {
        TimerFileFormat::Java => TIMER_LOG_FILE_SIZE,
        TimerFileFormat::RustV2 => SHARED_TIMER_LOG_FILE_SIZE,
    };
    config_with_file_size(root_dir, format, batch_messages, mapped_file_size_timer_log)
}

/// A store that switches formats keeps one TimerLog file size.
fn switching_config(root_dir: &Path, format: TimerFileFormat) -> Arc<MessageStoreConfig> {
    config_with_file_size(root_dir, format, 64, SHARED_TIMER_LOG_FILE_SIZE)
}

fn config_with_file_size(
    root_dir: &Path,
    format: TimerFileFormat,
    batch_messages: usize,
    mapped_file_size_timer_log: usize,
) -> Arc<MessageStoreConfig> {
    Arc::new(MessageStoreConfig {
        store_path_root_dir: CheetahString::from_string(root_dir.to_string_lossy().into_owned()),
        read_uncommitted: true,
        timer_file_format: format,
        mapped_file_size_timer_log,
        max_msgs_num_batch: batch_messages,
        timer_get_message_thread_num: 1,
        timer_put_message_thread_num: 1,
        timer_source_batch_messages: batch_messages,
        timer_due_batch_messages: batch_messages,
        ..MessageStoreConfig::default()
    })
}

async fn open_store(config: Arc<MessageStoreConfig>) -> Option<(LocalFileMessageStore, Arc<TimerMessageStore>)> {
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
        crate::runtime::test_service_context("timer-java-format-test"),
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
    if store.init().await.is_err() || !store.load().await {
        return None;
    }
    Some((store, timer_store))
}

async fn close_store(mut store: LocalFileMessageStore, timer_store: Arc<TimerMessageStore>) {
    drop(timer_store);
    store.shutdown().await;
}

fn timer_message(deliver_ms: u64, unique_key: &str) -> MessageExtBrokerInner {
    let mut message = MessageExtBrokerInner::default();
    message.set_topic(CheetahString::from_static_str(TIMER_TOPIC));
    message.message_ext_inner.queue_id = 0;
    message.set_body(Bytes::from_static(b"java-format-body"));
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

/// A recall of `unique_key` as Java writes it.
fn tombstone(deliver_ms: u64, unique_key: &str) -> MessageExtBrokerInner {
    let mut message = timer_message(deliver_ms, &format!("{unique_key}-recall"));
    message.set_body(Bytes::from_static(b"0"));
    message.put_property(
        CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_DEL_UNIQKEY),
        build_delete_key(REAL_TOPIC, unique_key),
    );
    message.properties_string = message_properties_to_string(message.get_properties());
    message
}

async fn put_all(store: &mut LocalFileMessageStore, messages: Vec<MessageExtBrokerInner>) {
    for message in messages {
        assert!(store.put_message(message).await.is_ok());
    }
    store.reput_once().await;
}

fn delivered(store: &LocalFileMessageStore) -> i64 {
    store.get_max_offset_in_queue(&CheetahString::from_static_str(REAL_TOPIC), 0)
}

fn timer_log_file(root_dir: &Path, start: u64) -> PathBuf {
    PathBuf::from(get_timer_log_path(&root_dir.to_string_lossy())).join(format!("{start:020}"))
}

#[tokio::test]
async fn java_format_writes_apache_timer_files_and_delivers_from_them() {
    let root = tempdir().unwrap();
    let (mut store, timer_store) = open_store(config(root.path(), TimerFileFormat::Java, 64))
        .await
        .expect("open a Java-format timer store");
    let deliver_ms = current_millis().saturating_sub(2_000);
    put_all(
        &mut store,
        (0..4)
            .map(|index| timer_message(deliver_ms, &format!("m{index}")))
            .collect(),
    )
    .await;
    assert_eq!(timer_store.process_once().await, 4);

    // Java's files and nothing of the Rust V2 layout.
    let root_dir = root.path().to_string_lossy().into_owned();
    assert!(!root.path().join("timer-v2").exists());
    assert!(!Path::new(&format!("{}.v2", get_timer_wheel_path(&root_dir))).exists());
    assert_eq!(
        std::fs::metadata(get_timer_wheel_path(&root_dir)).unwrap().len(),
        java_timer_wheel_len(TIMER_WHEEL_TTL_DAY as usize * DAY_SECS as usize) as u64
    );
    let first = std::fs::read(timer_log_file(root.path(), 0)).unwrap();
    assert_eq!(first.len(), TIMER_LOG_FILE_SIZE);
    let Ok(JavaTimerLogRecord::Unit(unit)) = JavaTimerLogRecord::decode(&first) else {
        panic!("the first TimerLog file starts with a Java unit");
    };
    assert_eq!(unit.prev_pos, -1);
    assert_eq!(unit.topic_hash, java_topic_hash(REAL_TOPIC));
    // The fourth unit opens the second file.
    assert_eq!(
        std::fs::metadata(timer_log_file(root.path(), TIMER_LOG_FILE_SIZE as u64))
            .unwrap()
            .len(),
        TIMER_LOG_FILE_SIZE as u64
    );

    timer_store.set_should_running_dequeue(true);
    assert_eq!(timer_store.process_once().await, 4);
    store.reput_once().await;
    assert_eq!(delivered(&store), 4);
    close_store(store, timer_store).await;
}

#[tokio::test]
async fn java_format_rebuilds_lost_wheel_slots_from_the_timer_log_on_restart() {
    let root = tempdir().unwrap();
    let config = config(root.path(), TimerFileFormat::Java, 64);
    let (mut store, timer_store) = open_store(Arc::clone(&config)).await.expect("open the store");
    let deliver_ms = current_millis().saturating_sub(2_000);
    put_all(
        &mut store,
        vec![timer_message(deliver_ms, "a"), timer_message(deliver_ms, "b")],
    )
    .await;
    assert_eq!(timer_store.process_once().await, 2);
    close_store(store, timer_store).await;

    // The wheel did not reach the disk; Java's recovery links the logged units back in.
    let wheel_path = get_timer_wheel_path(&root.path().to_string_lossy());
    let wheel_len = std::fs::metadata(&wheel_path).unwrap().len();
    std::fs::write(&wheel_path, vec![0u8; wheel_len as usize]).unwrap();

    let (mut store, timer_store) = open_store(config).await.expect("reopen the store");
    assert_eq!(timer_store.curr_queue_offset.load(Ordering::Relaxed), 2);
    timer_store.set_should_running_dequeue(true);
    assert_eq!(timer_store.process_once().await, 2);
    store.reput_once().await;
    assert_eq!(delivered(&store), 2);
    close_store(store, timer_store).await;
}

#[tokio::test]
async fn java_format_walks_a_slot_whose_count_a_tombstone_lowered() {
    let root = tempdir().unwrap();
    let (mut store, timer_store) = open_store(config(root.path(), TimerFileFormat::Java, 64))
        .await
        .expect("open the store");
    let deliver_ms = current_millis().saturating_sub(2_000);
    put_all(
        &mut store,
        vec![
            timer_message(deliver_ms, "recalled"),
            timer_message(deliver_ms, "kept"),
            tombstone(deliver_ms, "recalled"),
        ],
    )
    .await;
    assert_eq!(timer_store.process_once().await, 3);
    // Java counts two messages and a tombstone as one; the slot still holds three units.
    let around = (deliver_ms as i64 / 1_000) * 1_000;
    let counted = (around - 10_000..around + 10_000)
        .step_by(1_000)
        .find_map(|time_ms| timer_store.get_timer_wheel_slot(time_ms))
        .expect("the slot of the three units");
    assert_eq!(counted.num, 1);

    timer_store.set_should_running_dequeue(true);
    timer_store.process_once().await;
    store.reput_once().await;
    assert_eq!(delivered(&store), 1);
    close_store(store, timer_store).await;
}

#[tokio::test]
async fn java_format_restart_mid_slot_keeps_the_tombstone_it_processed() {
    let root = tempdir().unwrap();
    let config = config(root.path(), TimerFileFormat::Java, 1);
    let (mut store, timer_store) = open_store(Arc::clone(&config)).await.expect("open the store");
    let deliver_ms = current_millis().saturating_sub(2_000);
    put_all(
        &mut store,
        vec![tombstone(deliver_ms, "target"), timer_message(deliver_ms, "target")],
    )
    .await;
    timer_store.set_should_running_dequeue(true);
    // Two sources are indexed and the tombstone, first in the slot, is processed.
    assert_eq!(timer_store.process_once().await, 3);
    assert_eq!(delivered(&store), 0);
    close_store(store, timer_store).await;

    // The Java checkpoint has no drain cursor; the wheel slot now starts after the tombstone,
    // and the walk still reaches it through the chain.
    let (mut store, timer_store) = open_store(config).await.expect("reopen the store");
    timer_store.set_should_running_dequeue(true);
    assert_eq!(timer_store.process_once().await, 1);
    assert_eq!(timer_store.process_once().await, 0);
    store.reput_once().await;
    assert_eq!(delivered(&store), 0);
    close_store(store, timer_store).await;
}

fn registry_text(root_dir: &Path) -> String {
    std::fs::read_to_string(root_dir.join("timer").join("formats.json")).unwrap()
}

/// Whether any Rust V2 timer log segment holds a record.
fn rust_v2_log_has_records(root_dir: &Path) -> bool {
    let log_dir = PathBuf::from(get_timer_log_path(&root_dir.to_string_lossy()))
        .join("v2")
        .join("log");
    std::fs::read_dir(log_dir).is_ok_and(|entries| {
        entries.flatten().any(|entry| {
            entry.file_name().to_string_lossy().len() == 20 && entry.metadata().is_ok_and(|metadata| metadata.len() > 0)
        })
    })
}

struct ManualClock(AtomicI64);

impl TimerClock for ManualClock {
    fn wall_time_ms(&self) -> i64 {
        self.0.load(Ordering::Acquire)
    }

    fn monotonic_elapsed_ms(&self) -> u64 {
        self.wall_time_ms().max(0) as u64
    }
}

#[tokio::test]
async fn switching_to_java_drains_rust_v2_and_indexes_each_source_once() {
    let root = tempdir().unwrap();
    let deliver_ms = current_millis().saturating_sub(2_000);
    let (mut store, timer_store) = open_store(switching_config(root.path(), TimerFileFormat::RustV2))
        .await
        .expect("open a Rust V2 store");
    put_all(
        &mut store,
        vec![timer_message(deliver_ms, "v2-a"), timer_message(deliver_ms, "v2-b")],
    )
    .await;
    assert_eq!(timer_store.process_once().await, 2);
    close_store(store, timer_store).await;

    let (mut store, timer_store) = open_store(switching_config(root.path(), TimerFileFormat::Java))
        .await
        .expect("switch the store to the Java format");
    assert_eq!(timer_store.draining_file_format(), Some(TimerFileFormat::RustV2));
    // The Java format starts where Rust V2 stopped: only the new source is indexed.
    assert_eq!(timer_store.curr_queue_offset.load(Ordering::Relaxed), 2);
    assert_eq!(registry_text(root.path()), r#"{"active":"java","draining":"rust_v2"}"#);
    put_all(&mut store, vec![timer_message(deliver_ms, "java-c")]).await;
    assert_eq!(timer_store.process_once().await, 1);
    let first_java_file = std::fs::read(timer_log_file(root.path(), 0)).unwrap();
    assert!(matches!(
        JavaTimerLogRecord::decode(&first_java_file),
        Ok(JavaTimerLogRecord::Unit(_))
    ));

    timer_store.set_should_running_dequeue(true);
    assert_eq!(timer_store.process_once().await, 3);
    store.reput_once().await;
    assert_eq!(delivered(&store), 3);
    // The commit after the delivery retires the drained format.
    assert_eq!(timer_store.draining_file_format(), None);
    assert_eq!(registry_text(root.path()), r#"{"active":"java"}"#);
    close_store(store, timer_store).await;

    let (mut store, timer_store) = open_store(switching_config(root.path(), TimerFileFormat::Java))
        .await
        .expect("reopen the store");
    assert_eq!(timer_store.draining_file_format(), None);
    timer_store.set_should_running_dequeue(true);
    assert_eq!(timer_store.process_once().await, 0);
    store.reput_once().await;
    assert_eq!(delivered(&store), 3);
    close_store(store, timer_store).await;
}

#[tokio::test]
async fn a_java_store_switches_to_rust_v2_without_reading_its_files_as_v1() {
    let root = tempdir().unwrap();
    let deliver_ms = current_millis().saturating_sub(2_000);
    let (mut store, timer_store) = open_store(switching_config(root.path(), TimerFileFormat::Java))
        .await
        .expect("open a Java-format store");
    put_all(
        &mut store,
        vec![timer_message(deliver_ms, "java-a"), timer_message(deliver_ms, "java-b")],
    )
    .await;
    assert_eq!(timer_store.process_once().await, 2);
    close_store(store, timer_store).await;

    // A store without a registry, as a Java Broker leaves it, switches the same way.
    std::fs::remove_file(root.path().join("timer").join("formats.json")).unwrap();
    let (mut store, timer_store) = open_store(switching_config(root.path(), TimerFileFormat::RustV2))
        .await
        .expect("switch the store to Rust V2");
    assert_eq!(timer_store.draining_file_format(), Some(TimerFileFormat::Java));
    assert_eq!(timer_store.curr_queue_offset.load(Ordering::Relaxed), 2);
    // Java's first TimerLog file has the legacy V1 name; it was not migrated into V2.
    assert!(!rust_v2_log_has_records(root.path()));

    timer_store.set_should_running_dequeue(true);
    assert_eq!(timer_store.process_once().await, 2);
    store.reput_once().await;
    assert_eq!(delivered(&store), 2);
    assert_eq!(timer_store.draining_file_format(), None);
    assert_eq!(registry_text(root.path()), r#"{"active":"rust_v2"}"#);
    close_store(store, timer_store).await;
}

#[tokio::test]
async fn a_recall_after_the_switch_cancels_a_timer_the_draining_format_holds() {
    let root = tempdir().unwrap();
    let start_ms = (current_millis() as i64 / 1_000) * 1_000;
    let clock = Arc::new(ManualClock(AtomicI64::new(start_ms)));
    let deliver_ms = (start_ms + 10_000) as u64;
    let (mut store, timer_store) = open_store(switching_config(root.path(), TimerFileFormat::RustV2))
        .await
        .expect("open a Rust V2 store");
    timer_store.set_clock(clock.clone());
    put_all(
        &mut store,
        vec![timer_message(deliver_ms, "target"), timer_message(deliver_ms, "kept")],
    )
    .await;
    assert_eq!(timer_store.process_once().await, 2);
    close_store(store, timer_store).await;

    let (mut store, timer_store) = open_store(switching_config(root.path(), TimerFileFormat::Java))
        .await
        .expect("switch the store to the Java format");
    timer_store.set_clock(clock.clone());
    assert_eq!(timer_store.draining_file_format(), Some(TimerFileFormat::RustV2));
    put_all(&mut store, vec![tombstone(deliver_ms, "target")]).await;
    assert_eq!(timer_store.process_once().await, 1);

    clock.0.store(start_ms + 20_000, Ordering::Release);
    timer_store.set_should_running_dequeue(true);
    timer_store.process_once().await;
    store.reput_once().await;
    assert_eq!(delivered(&store), 1);
    close_store(store, timer_store).await;
}
