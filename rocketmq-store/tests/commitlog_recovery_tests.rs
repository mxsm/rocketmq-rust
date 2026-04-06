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

//! Integration tests for CommitLog recovery behavior.
//!
//! These tests target the Phase 6 alignment risks called out in the analysis:
//! dirty-tail truncation and stale ConsumeQueue cleanup when CommitLog files
//! disappear across restarts.

use std::collections::BTreeMap;
use std::fs;
use std::fs::OpenOptions;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_status_enum::GetMessageStatus;
use rocketmq_store::base::message_status_enum::PutMessageStatus;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::config::flush_disk_type::FlushDiskType;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use rocketmq_store::log_file::commit_log_recovery::RecoveryContext;
use rocketmq_store::log_file::commit_log_recovery::RecoveryStatistics;
use rocketmq_store::message_store::local_file_message_store::LocalFileMessageStore;
use rocketmq_store::store_path_config_helper::get_abort_file;
use rocketmq_store::store_path_config_helper::get_store_path_consume_queue;
use tempfile::TempDir;

fn new_test_store(temp_dir: &TempDir, mut message_store_config: MessageStoreConfig) -> ArcMut<LocalFileMessageStore> {
    message_store_config.store_path_root_dir = temp_dir.path().to_string_lossy().to_string().into();
    let broker_config = Arc::new(BrokerConfig::default());
    let topic_table: Arc<DashMap<CheetahString, ArcMut<TopicConfig>>> = Arc::new(DashMap::new());

    let mut store = ArcMut::new(LocalFileMessageStore::new(
        Arc::new(message_store_config),
        broker_config,
        topic_table,
        None,
        false,
    ));
    let store_clone = store.clone();
    store.set_message_store_arc(store_clone);
    store
}

fn phase6_store_config() -> MessageStoreConfig {
    MessageStoreConfig {
        flush_disk_type: FlushDiskType::AsyncFlush,
        mapped_file_size_commit_log: 4096,
        mapped_file_size_consume_queue: 200,
        ..MessageStoreConfig::default()
    }
}

fn build_test_message(topic: &CheetahString, queue_id: i32, body: &'static [u8]) -> MessageExtBrokerInner {
    let mut msg = MessageExtBrokerInner::default();
    msg.set_topic(topic.clone());
    msg.message_ext_inner.set_queue_id(queue_id);
    msg.set_body(Bytes::from_static(body));
    msg
}

fn first_commitlog_file(root: &Path) -> PathBuf {
    PathBuf::from(root).join("commitlog").join("00000000000000000000")
}

fn corrupt_commitlog_tail(commitlog_file: &Path, offset: i64, payload: &[u8]) {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(commitlog_file)
        .expect("open commitlog file");
    file.seek(SeekFrom::Start(offset as u64)).expect("seek commitlog tail");
    file.write_all(payload).expect("write dirty tail");
    file.sync_data().expect("sync dirty tail");
}

#[test]
fn test_recovery_context_creation() {
    let config = Arc::new(MessageStoreConfig::default());
    let delay_table = BTreeMap::new();

    let ctx = RecoveryContext::new(
        true,  // check_crc
        false, // check_dup_info
        config,
        16, // max_delay_level
        delay_table,
    );

    assert!(ctx.check_crc);
    assert!(!ctx.check_dup_info);
    assert_eq!(ctx.max_delay_level, 16);
}

#[test]
fn test_recovery_statistics_default() {
    let stats = RecoveryStatistics::default();

    assert_eq!(stats.files_processed, 0);
    assert_eq!(stats.messages_recovered, 0);
    assert_eq!(stats.bytes_processed, 0);
    assert_eq!(stats.invalid_messages, 0);
    assert_eq!(stats.recovery_time_ms, 0);
}

#[test]
fn test_recovery_statistics_updates() {
    let stats = RecoveryStatistics {
        files_processed: 10,
        messages_recovered: 1000,
        bytes_processed: 1024 * 1024,
        invalid_messages: 5,
        recovery_time_ms: 500,
    };

    assert_eq!(stats.files_processed, 10);
    assert_eq!(stats.messages_recovered, 1000);
    assert_eq!(stats.bytes_processed, 1024 * 1024);
    assert_eq!(stats.invalid_messages, 5);
    assert_eq!(stats.recovery_time_ms, 500);
}

#[test]
fn test_recovery_statistics_clone() {
    let stats = RecoveryStatistics {
        files_processed: 5,
        messages_recovered: 500,
        bytes_processed: 512 * 1024,
        invalid_messages: 2,
        recovery_time_ms: 250,
    };

    let cloned = stats.clone();

    assert_eq!(cloned.files_processed, stats.files_processed);
    assert_eq!(cloned.messages_recovered, stats.messages_recovered);
    assert_eq!(cloned.bytes_processed, stats.bytes_processed);
    assert_eq!(cloned.invalid_messages, stats.invalid_messages);
    assert_eq!(cloned.recovery_time_ms, stats.recovery_time_ms);
}

#[test]
fn test_module_compiles() {
    let _config = Arc::new(MessageStoreConfig::default());
    let _stats = RecoveryStatistics::default();
}

#[tokio::test]
async fn normal_recovery_truncates_dirty_tail_and_keeps_dispatched_message() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let topic = CheetahString::from_static_str("phase6-normal-recovery-topic");
    let group = CheetahString::from_static_str("phase6-normal-recovery-group");

    let mut writer = new_test_store(&temp_dir, phase6_store_config());
    writer.init().await.expect("init writer");

    let put_result = writer
        .put_message(build_test_message(&topic, 0, b"phase6-normal-body"))
        .await;
    assert_eq!(put_result.put_message_status(), PutMessageStatus::PutOk);

    let append_result = put_result.append_message_result().expect("append result should exist");
    let wrote_offset = append_result.wrote_offset;
    let valid_end = append_result.wrote_offset + append_result.wrote_bytes as i64;

    writer.reput_once().await;
    writer.shutdown().await;
    drop(writer);

    let commitlog_file = first_commitlog_file(temp_dir.path());
    corrupt_commitlog_tail(&commitlog_file, valid_end, &[0x13, 0x37, 0xC0, 0xDE]);

    let mut restarted = new_test_store(&temp_dir, phase6_store_config());
    restarted.init().await.expect("init restarted store");
    assert!(restarted.load().await, "load should succeed after normal recovery");

    assert_eq!(restarted.get_max_phy_offset(), valid_end);
    assert!(restarted.get_commit_log_data(valid_end).is_none());
    assert_eq!(restarted.get_commit_log_offset_in_queue(&topic, 0, 0), wrote_offset);

    let get_result = restarted
        .get_message(&group, &topic, 0, 0, 32, None)
        .await
        .expect("get message result");
    assert_eq!(get_result.status(), Some(GetMessageStatus::Found));
    assert_eq!(get_result.message_count(), 1);
    assert_eq!(get_result.message_queue_offset(), &vec![0]);
}

#[tokio::test]
async fn abnormal_recovery_truncates_dirty_tail_and_keeps_dispatched_message() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let topic = CheetahString::from_static_str("phase6-abnormal-recovery-topic");
    let group = CheetahString::from_static_str("phase6-abnormal-recovery-group");

    let mut writer = new_test_store(&temp_dir, phase6_store_config());
    writer.init().await.expect("init writer");

    let put_result = writer
        .put_message(build_test_message(&topic, 0, b"phase6-abnormal-body"))
        .await;
    assert_eq!(put_result.put_message_status(), PutMessageStatus::PutOk);

    let append_result = put_result.append_message_result().expect("append result should exist");
    let wrote_offset = append_result.wrote_offset;
    let valid_end = append_result.wrote_offset + append_result.wrote_bytes as i64;

    writer.reput_once().await;
    writer.shutdown().await;
    drop(writer);

    fs::write(
        get_abort_file(temp_dir.path().to_string_lossy().as_ref()),
        b"phase6-abort",
    )
    .expect("create abort file");
    let commitlog_file = first_commitlog_file(temp_dir.path());
    corrupt_commitlog_tail(&commitlog_file, valid_end, &[0xFA, 0x11, 0xED, 0x01, 0x02]);

    let mut restarted = new_test_store(&temp_dir, phase6_store_config());
    restarted.init().await.expect("init restarted store");
    assert!(restarted.load().await, "load should succeed after abnormal recovery");

    assert_eq!(restarted.get_max_phy_offset(), valid_end);
    assert!(restarted.get_commit_log_data(valid_end).is_none());
    assert_eq!(restarted.get_commit_log_offset_in_queue(&topic, 0, 0), wrote_offset);

    let get_result = restarted
        .get_message(&group, &topic, 0, 0, 32, None)
        .await
        .expect("get message result");
    assert_eq!(get_result.status(), Some(GetMessageStatus::Found));
    assert_eq!(get_result.message_count(), 1);
    assert_eq!(get_result.message_queue_offset(), &vec![0]);
}

#[tokio::test]
async fn load_clears_stale_consume_queue_when_commitlog_is_missing() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let topic = CheetahString::from_static_str("phase6-missing-commitlog-topic");

    let mut writer = new_test_store(&temp_dir, phase6_store_config());
    writer.init().await.expect("init writer");

    let put_result = writer
        .put_message(build_test_message(&topic, 0, b"phase6-stale-cq-body"))
        .await;
    assert_eq!(put_result.put_message_status(), PutMessageStatus::PutOk);

    writer.reput_once().await;
    writer.shutdown().await;
    drop(writer);

    let consume_queue_dir = PathBuf::from(get_store_path_consume_queue(temp_dir.path().to_string_lossy().as_ref()));
    let topic_queue_dir = consume_queue_dir.join(topic.as_str()).join("0");
    assert!(consume_queue_dir.exists(), "consume queue should exist before restart");
    assert!(
        topic_queue_dir.exists(),
        "topic consume queue should exist before restart"
    );

    fs::remove_dir_all(temp_dir.path().join("commitlog")).expect("remove commitlog directory");

    let mut restarted = new_test_store(&temp_dir, phase6_store_config());
    restarted.init().await.expect("init restarted store");
    assert!(
        restarted.load().await,
        "load should succeed and clean stale consume queue files"
    );

    assert_eq!(restarted.get_max_phy_offset(), 0);
    assert!(
        !topic_queue_dir.exists(),
        "stale topic consume queue should be removed when commitlog is missing"
    );
}
