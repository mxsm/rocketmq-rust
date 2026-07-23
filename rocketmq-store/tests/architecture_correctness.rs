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

//! Correctness-first runners used by the target-hardware performance gate.

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Write;
use std::path::Path;
use std::process::Command;
use std::process::Stdio;
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_store::base::message_status_enum::GetMessageStatus;
use rocketmq_store::base::message_status_enum::PutMessageStatus;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::config::flush_disk_type::FlushDiskType;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use rocketmq_store::message_store::local_file_message_store::LocalFileMessageStore;
use rocketmq_store::store_path_config_helper::get_abort_file;
use rocketmq_store::store_path_config_helper::get_store_checkpoint;
use rocketmq_store::store_path_config_helper::get_store_path_consume_queue;
use rocketmq_store::store_path_config_helper::get_store_path_index;
use tempfile::TempDir;

const CHILD_STORE_ROOT: &str = "ROCKETMQ_ARCHITECTURE_SYNC_CRASH_ROOT";
const CHILD_MESSAGE_COUNT: &str = "ROCKETMQ_ARCHITECTURE_SYNC_CRASH_COUNT";
const ACK_PREFIX: &str = "ARCHITECTURE_SYNC_FLUSH_ACK";
const SYNC_CRASH_MESSAGES: usize = 16;
const REPLAY_MESSAGES: usize = 32;
const MESSAGE_SIZE_BYTES: usize = 1024;

fn new_store(root: &Path, flush_disk_type: FlushDiskType) -> LocalFileMessageStore {
    let config = MessageStoreConfig {
        store_path_root_dir: root.to_string_lossy().to_string().into(),
        flush_disk_type,
        mapped_file_size_commit_log: 4 * 1024 * 1024,
        mapped_file_size_consume_queue: 20 * 1024,
        ha_listen_port: 0,
        timer_wheel_enable: false,
        ..MessageStoreConfig::default()
    };
    let mut store = LocalFileMessageStore::new(
        Arc::new(config),
        Arc::new(BrokerConfig::default()),
        Arc::new(DashMap::<CheetahString, Arc<TopicConfig>>::new()),
        None,
        false,
    );
    store
        .wire_owned_root_dependencies()
        .expect("architecture correctness store must wire owned dependencies");
    store
}

async fn start_store(store: &mut LocalFileMessageStore) {
    store.init().await.expect("initialize architecture correctness store");
    assert!(store.load().await, "load architecture correctness store");
    store.start().await.expect("start architecture correctness store");
}

fn message(topic: &CheetahString, sequence: usize) -> MessageExtBrokerInner {
    let mut message = MessageExtBrokerInner::default();
    message.set_topic(topic.clone());
    message.message_ext_inner.set_queue_id(0);
    message.set_body(expected_body(sequence));
    message
}

fn expected_body(sequence: usize) -> Bytes {
    Bytes::from(vec![(sequence & 0xff) as u8; MESSAGE_SIZE_BYTES])
}

#[tokio::test]
#[ignore = "subprocess helper; the parent test terminates it after durable acknowledgements"]
async fn sync_flush_crash_writer_helper() {
    let root = env::var_os(CHILD_STORE_ROOT).expect("child store root");
    let message_count = env::var(CHILD_MESSAGE_COUNT)
        .expect("child message count")
        .parse::<usize>()
        .expect("numeric child message count");
    let topic = CheetahString::from_static_str("ArchitectureSyncCrashRecovery");
    let mut store = new_store(Path::new(&root), FlushDiskType::SyncFlush);
    start_store(&mut store).await;

    for sequence in 0..message_count {
        let result = store.put_message(message(&topic, sequence)).await;
        assert_eq!(result.put_message_status(), PutMessageStatus::PutOk);
        let append = result.append_message_result().expect("SyncFlush append result");
        println!("{ACK_PREFIX} {sequence} {} {}", append.wrote_offset, append.wrote_bytes);
        std::io::stdout().flush().expect("flush acknowledgement line");
    }

    std::future::pending::<()>().await;
}

#[test]
fn sync_flush_crash_recovery() {
    let temp_dir = TempDir::new().expect("create SyncFlush crash directory");
    let executable = env::current_exe().expect("resolve architecture correctness test executable");
    let mut child = Command::new(executable)
        .args([
            "--exact",
            "sync_flush_crash_writer_helper",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD_STORE_ROOT, temp_dir.path())
        .env(CHILD_MESSAGE_COUNT, SYNC_CRASH_MESSAGES.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn SyncFlush writer subprocess");
    let stdout = child.stdout.take().expect("capture SyncFlush writer stdout");
    let (sender, receiver) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            if sender.send(line).is_err() {
                break;
            }
        }
    });

    let mut acknowledgements = BTreeMap::new();
    while acknowledgements.len() < SYNC_CRASH_MESSAGES {
        let line = match receiver.recv_timeout(Duration::from_secs(30)) {
            Ok(Ok(line)) => line,
            Ok(Err(error)) => {
                let _ = child.kill();
                panic!("read SyncFlush writer output: {error}");
            }
            Err(error) => {
                let _ = child.kill();
                panic!(
                    "SyncFlush writer stopped before all acknowledgements: {error}; observed {}",
                    acknowledgements.len()
                );
            }
        };
        let Some(marker) = line.find(ACK_PREFIX) else {
            continue;
        };
        let fields = line[marker + ACK_PREFIX.len()..].split_whitespace().collect::<Vec<_>>();
        assert_eq!(fields.len(), 3, "malformed acknowledgement line: {line}");
        let sequence = fields[0].parse::<usize>().expect("ack sequence");
        let offset = fields[1].parse::<i64>().expect("ack physical offset");
        let size = fields[2].parse::<i32>().expect("ack encoded size");
        assert!(acknowledgements.insert(sequence, (offset, size)).is_none());
    }

    child.kill().expect("force-kill acknowledged SyncFlush writer");
    let status = child.wait().expect("wait for killed SyncFlush writer");
    assert!(!status.success(), "writer must not complete a graceful shutdown");
    reader.join().expect("join SyncFlush stdout reader");
    assert!(
        Path::new(&get_abort_file(&temp_dir.path().to_string_lossy())).is_file(),
        "forced termination must leave the abnormal-exit marker"
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .expect("build SyncFlush recovery runtime");
    runtime.block_on(async {
        let topic = CheetahString::from_static_str("ArchitectureSyncCrashRecovery");
        let mut recovered = new_store(temp_dir.path(), FlushDiskType::SyncFlush);
        recovered.init().await.expect("initialize killed SyncFlush store");
        assert!(recovered.load().await, "recover killed SyncFlush store");
        assert_eq!(acknowledgements.len(), SYNC_CRASH_MESSAGES);
        for (sequence, (offset, size)) in acknowledgements {
            let recovered_message = recovered
                .look_message_by_offset(offset)
                .unwrap_or_else(|| panic!("acknowledged message {sequence} at {offset} was lost"));
            assert_eq!(recovered_message.topic(), &topic);
            assert_eq!(recovered_message.queue_offset, sequence as i64);
            assert_eq!(recovered_message.store_size, size);
            assert_eq!(recovered_message.body(), Some(expected_body(sequence)));
        }
        assert_eq!(recovered.get_max_offset_in_queue(&topic, 0), SYNC_CRASH_MESSAGES as i64);
        recovered.shutdown().await;
    });
}

#[tokio::test]
async fn derived_replay_no_holes() {
    let temp_dir = TempDir::new().expect("create derived replay directory");
    let topic = CheetahString::from_static_str("ArchitectureDerivedReplay");
    let group = CheetahString::from_static_str("ArchitectureDerivedReplayGroup");
    let mut writer = new_store(temp_dir.path(), FlushDiskType::SyncFlush);
    start_store(&mut writer).await;
    let mut physical_offsets = Vec::with_capacity(REPLAY_MESSAGES);
    for sequence in 0..REPLAY_MESSAGES {
        let result = writer.put_message(message(&topic, sequence)).await;
        assert_eq!(result.put_message_status(), PutMessageStatus::PutOk);
        physical_offsets.push(
            result
                .append_message_result()
                .expect("derived replay append result")
                .wrote_offset,
        );
    }
    writer.shutdown().await;
    drop(writer);

    let root = temp_dir.path().to_string_lossy();
    for path in [
        get_store_path_consume_queue(&root),
        get_store_path_index(&root),
        get_store_checkpoint(&root),
    ] {
        remove_path_if_exists(Path::new(&path));
    }
    fs::write(get_abort_file(&root), b"force abnormal CommitLog replay")
        .expect("write derived replay abnormal-exit marker");

    let mut recovered = new_store(temp_dir.path(), FlushDiskType::SyncFlush);
    recovered.init().await.expect("initialize derived replay store");
    assert!(recovered.load().await, "load derived replay store");
    for _ in 0..REPLAY_MESSAGES + 4 {
        if recovered.get_max_offset_in_queue(&topic, 0) == REPLAY_MESSAGES as i64 {
            break;
        }
        recovered.reput_once().await;
    }
    assert_eq!(recovered.get_min_offset_in_queue(&topic, 0), 0);
    assert_eq!(recovered.get_max_offset_in_queue(&topic, 0), REPLAY_MESSAGES as i64);

    for (sequence, physical_offset) in physical_offsets.into_iter().enumerate() {
        let result = recovered
            .get_message(&group, &topic, 0, sequence as i64, 1, None)
            .await
            .unwrap_or_else(|| panic!("missing replay result for queue offset {sequence}"));
        assert_eq!(result.status(), Some(GetMessageStatus::Found));
        assert_eq!(result.message_count(), 1);
        assert_eq!(result.next_begin_offset(), sequence as i64 + 1);
        let replayed = recovered
            .look_message_by_offset(physical_offset)
            .unwrap_or_else(|| panic!("missing authoritative CommitLog message {sequence}"));
        assert_eq!(replayed.queue_offset, sequence as i64);
        assert_eq!(replayed.body(), Some(expected_body(sequence)));
    }
    recovered.shutdown().await;
}

fn remove_path_if_exists(path: &Path) {
    if path.is_dir() {
        fs::remove_dir_all(path).unwrap_or_else(|error| panic!("remove {}: {error}", path.display()));
    } else if path.is_file() {
        fs::remove_file(path).unwrap_or_else(|error| panic!("remove {}: {error}", path.display()));
    }
}
