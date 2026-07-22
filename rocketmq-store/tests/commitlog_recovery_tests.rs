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
use rocketmq_common::common::broker::broker_config::BrokerConfig as RuntimeBrokerConfig;
use rocketmq_common::common::broker::broker_role::BrokerRole;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_status_enum::GetMessageStatus;
use rocketmq_store::base::message_status_enum::PutMessageStatus;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::base::store_enum::StoreType;
use rocketmq_store::config::flush_disk_type::FlushDiskType;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use rocketmq_store::log_file::commit_log_recovery::RecoveryContext;
use rocketmq_store::log_file::commit_log_recovery::RecoveryStatistics;
use rocketmq_store::message_store::local_file_message_store::LocalFileMessageStore;
use rocketmq_store::store_path_config_helper::get_abort_file;
use rocketmq_store::store_path_config_helper::get_store_path_consume_queue;
use rocketmq_store_local::commit_log::record::BLANK_MAGIC_CODE;
use tempfile::TempDir;

const RECOVERY_SEGMENT_SIZE: usize = 512;
static FIRST_RECOVERY_BODY: [u8; 120] = [0x41; 120];

fn new_test_store(temp_dir: &TempDir, mut message_store_config: MessageStoreConfig) -> ArcMut<LocalFileMessageStore> {
    message_store_config.store_path_root_dir = temp_dir.path().to_string_lossy().to_string().into();
    let broker_config = Arc::new(BrokerConfig::default());
    let topic_table: Arc<DashMap<CheetahString, Arc<TopicConfig>>> = Arc::new(DashMap::new());

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

thread_local! {
    static NEXT_BROKER_CONFIG: std::cell::RefCell<Option<RuntimeBrokerConfig>> = const {
        std::cell::RefCell::new(None)
    };
}

struct BrokerConfig;

impl BrokerConfig {
    fn default() -> RuntimeBrokerConfig {
        NEXT_BROKER_CONFIG.with(|slot| slot.borrow_mut().take().unwrap_or_default())
    }
}

fn stage_next_broker_config(config: RuntimeBrokerConfig) {
    NEXT_BROKER_CONFIG.with(|slot| {
        let previous = slot.borrow_mut().replace(config);
        assert!(previous.is_none(), "broker config staging slot must be empty");
    });
}

fn phase6_store_config() -> MessageStoreConfig {
    MessageStoreConfig {
        flush_disk_type: FlushDiskType::AsyncFlush,
        mapped_file_size_commit_log: 4096,
        mapped_file_size_consume_queue: 200,
        ha_listen_port: 0,
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

#[derive(Clone, Copy)]
enum NormalRecoveryRoute {
    Standard,
    Optimized,
}

#[derive(Clone, Copy)]
enum FirstSegmentShape {
    Blank,
    SourceEnded,
    Invalid,
    ValidThenBlank,
    ValidPair,
}

#[derive(Clone, Copy)]
enum AbnormalRecoveryGateMode {
    Duplication,
    Controller,
}

struct TwoSegmentFixture {
    temp_dir: TempDir,
    topic: CheetahString,
    first_end: i64,
    second_start: i64,
    second_end: i64,
}

#[derive(Debug, PartialEq, Eq)]
struct NormalRecoverySnapshot {
    max_phy_offset: i64,
    confirm_offset: u64,
    max_queue_offset: i64,
}

async fn two_segment_fixture(shape: FirstSegmentShape) -> TwoSegmentFixture {
    let source_dir = TempDir::new().expect("create recovery frame source directory");
    let temp_dir = TempDir::new().expect("create recovery fixture directory");
    let topic = CheetahString::from_static_str("m06-normal-recovery-state-topic");
    let mut config = phase6_store_config();
    config.mapped_file_size_commit_log = RECOVERY_SEGMENT_SIZE;
    let mut writer = new_test_store(&source_dir, config);
    writer.init().await.expect("init recovery fixture writer");

    let mut first_message = build_test_message(&topic, 0, &FIRST_RECOVERY_BODY);
    first_message.put_property(
        CheetahString::from_static_str(MessageConst::DUP_INFO),
        CheetahString::from_static_str("0_0"),
    );
    let first = writer.put_message(first_message).await;
    assert_eq!(first.put_message_status(), PutMessageStatus::PutOk);
    let first_append = first.append_message_result().expect("first append result");
    let first_end = first_append.wrote_offset + i64::from(first_append.wrote_bytes);

    writer.shutdown().await;
    drop(writer);

    let source_path = first_commitlog_file(source_dir.path());
    let first_bytes = fs::read(&source_path).expect("read source recovery segment");
    let commitlog_dir = temp_dir.path().join("commitlog");
    fs::create_dir_all(&commitlog_dir).expect("create target commitlog directory");
    let first_path = first_commitlog_file(temp_dir.path());
    let second_path = temp_dir.path().join("commitlog").join("00000000000000000512");
    let first_size = usize::try_from(first_append.wrote_bytes).expect("positive first frame size");
    let valid_frame = &first_bytes[..first_size];
    let second_start = 512;
    let second_end = second_start + i64::from(first_append.wrote_bytes);
    let mut second_segment = vec![0; RECOVERY_SEGMENT_SIZE];
    second_segment[..first_size].copy_from_slice(valid_frame);
    if matches!(shape, FirstSegmentShape::ValidPair) {
        second_segment[28..36].copy_from_slice(&second_start.to_be_bytes());
    }
    fs::write(&second_path, second_segment).expect("write second recovery segment");

    let mut replacement = vec![0; RECOVERY_SEGMENT_SIZE];
    match shape {
        FirstSegmentShape::Blank => {
            replacement[..4].copy_from_slice(&512_i32.to_be_bytes());
            replacement[4..8].copy_from_slice(&BLANK_MAGIC_CODE.to_be_bytes());
            fs::write(&first_path, replacement).expect("replace first segment with blank marker");
        }
        FirstSegmentShape::SourceEnded => {
            fs::write(&first_path, replacement).expect("replace first segment with empty source");
        }
        FirstSegmentShape::Invalid => {
            replacement[..4].copy_from_slice(&8_i32.to_be_bytes());
            replacement[4..8].copy_from_slice(&123_i32.to_be_bytes());
            fs::write(&first_path, replacement).expect("replace first segment with invalid record");
        }
        FirstSegmentShape::ValidThenBlank => {
            replacement[..first_size].copy_from_slice(valid_frame);
            let blank_size = RECOVERY_SEGMENT_SIZE - first_size;
            let blank_size = i32::try_from(blank_size).expect("blank size fits i32");
            replacement[first_size..first_size + 4].copy_from_slice(&blank_size.to_be_bytes());
            replacement[first_size + 4..first_size + 8].copy_from_slice(&BLANK_MAGIC_CODE.to_be_bytes());
            fs::write(&first_path, replacement).expect("write valid frame and blank marker");
            let replacement = vec![0; RECOVERY_SEGMENT_SIZE];
            fs::write(&second_path, replacement).expect("replace second segment with empty source");
        }
        FirstSegmentShape::ValidPair => {
            replacement[..first_size].copy_from_slice(valid_frame);
            let blank_size = RECOVERY_SEGMENT_SIZE - first_size;
            let blank_size = i32::try_from(blank_size).expect("blank size fits i32");
            replacement[first_size..first_size + 4].copy_from_slice(&blank_size.to_be_bytes());
            replacement[first_size + 4..first_size + 8].copy_from_slice(&BLANK_MAGIC_CODE.to_be_bytes());
            fs::write(&first_path, replacement).expect("write first valid frame and blank marker");
        }
    }

    TwoSegmentFixture {
        temp_dir,
        topic,
        first_end,
        second_start,
        second_end,
    }
}

async fn run_normal_recovery_with_max_cq(
    fixture: &TwoSegmentFixture,
    route: NormalRecoveryRoute,
    max_phy_offset_of_consume_queue: i64,
) -> (u64, NormalRecoverySnapshot) {
    let mut config = phase6_store_config();
    config.mapped_file_size_commit_log = RECOVERY_SEGMENT_SIZE;
    let mut restarted = new_test_store(&fixture.temp_dir, config);
    restarted.init().await.expect("init recovery fixture reader");
    assert!(restarted.get_commit_log_mut().load(), "load fixture commitlog");
    let initial_confirm = u64::try_from(restarted.get_commit_log().get_confirm_offset())
        .expect("normal recovery initial confirm must be non-negative");
    match route {
        NormalRecoveryRoute::Standard => {
            restarted
                .get_commit_log_mut()
                .recover_normally(max_phy_offset_of_consume_queue)
                .await;
        }
        NormalRecoveryRoute::Optimized => {
            restarted
                .get_commit_log_mut()
                .recover_normally_optimized(max_phy_offset_of_consume_queue)
                .await;
        }
    }
    let snapshot = NormalRecoverySnapshot {
        max_phy_offset: restarted.get_max_phy_offset(),
        confirm_offset: restarted.get_store_checkpoint().confirm_phy_offset(),
        max_queue_offset: restarted.get_max_offset_in_queue(&fixture.topic, 0),
    };
    (initial_confirm, snapshot)
}

async fn run_normal_recovery(fixture: &TwoSegmentFixture, route: NormalRecoveryRoute) -> (u64, NormalRecoverySnapshot) {
    run_normal_recovery_with_max_cq(fixture, route, 0).await
}

async fn run_abnormal_recovery_with_max_cq(
    fixture: &TwoSegmentFixture,
    route: NormalRecoveryRoute,
    max_phy_offset_of_consume_queue: i64,
) -> NormalRecoverySnapshot {
    let mut config = phase6_store_config();
    config.mapped_file_size_commit_log = RECOVERY_SEGMENT_SIZE;
    let mut restarted = new_test_store(&fixture.temp_dir, config);
    restarted.init().await.expect("init abnormal recovery fixture reader");
    assert!(restarted.get_commit_log_mut().load(), "load abnormal fixture commitlog");
    match route {
        NormalRecoveryRoute::Standard => {
            restarted
                .get_commit_log_mut()
                .recover_abnormally(max_phy_offset_of_consume_queue)
                .await;
        }
        NormalRecoveryRoute::Optimized => {
            restarted
                .get_commit_log_mut()
                .recover_abnormally_optimized(max_phy_offset_of_consume_queue)
                .await;
        }
    }
    NormalRecoverySnapshot {
        max_phy_offset: restarted.get_max_phy_offset(),
        confirm_offset: restarted.get_store_checkpoint().confirm_phy_offset(),
        max_queue_offset: restarted.get_max_offset_in_queue(&fixture.topic, 0),
    }
}

async fn run_gated_abnormal_recovery(
    fixture: &TwoSegmentFixture,
    route: NormalRecoveryRoute,
    gate_mode: AbnormalRecoveryGateMode,
) -> NormalRecoverySnapshot {
    let mut config = phase6_store_config();
    config.mapped_file_size_commit_log = RECOVERY_SEGMENT_SIZE;
    match gate_mode {
        AbnormalRecoveryGateMode::Duplication => config.duplication_enable = true,
        AbnormalRecoveryGateMode::Controller => {
            config.enable_controller_mode = true;
            config.broker_role = BrokerRole::Slave;
        }
    }
    let broker_config = match gate_mode {
        AbnormalRecoveryGateMode::Duplication => RuntimeBrokerConfig {
            duplication_enable: true,
            ..RuntimeBrokerConfig::default()
        },
        AbnormalRecoveryGateMode::Controller => RuntimeBrokerConfig {
            enable_controller_mode: true,
            ..RuntimeBrokerConfig::default()
        },
    };
    stage_next_broker_config(broker_config);
    let mut restarted = new_test_store(&fixture.temp_dir, config);
    restarted.init().await.expect("init gated abnormal recovery reader");
    assert!(restarted.get_commit_log_mut().load(), "load gated abnormal fixture");
    let gate_limit = match gate_mode {
        AbnormalRecoveryGateMode::Duplication => fixture.first_end,
        AbnormalRecoveryGateMode::Controller => fixture.first_end + 1,
    };
    restarted.get_commit_log_mut().set_confirm_offset(gate_limit);
    match route {
        NormalRecoveryRoute::Standard => {
            restarted.get_commit_log_mut().recover_abnormally(0).await;
        }
        NormalRecoveryRoute::Optimized => {
            restarted.get_commit_log_mut().recover_abnormally_optimized(0).await;
        }
    }
    NormalRecoverySnapshot {
        max_phy_offset: restarted.get_max_phy_offset(),
        confirm_offset: restarted.get_store_checkpoint().confirm_phy_offset(),
        max_queue_offset: restarted.get_max_offset_in_queue(&fixture.topic, 0),
    }
}

#[tokio::test]
async fn normal_recovery_blank_first_segment_rolls_to_valid_second_without_dispatch() {
    let standard_fixture = two_segment_fixture(FirstSegmentShape::Blank).await;
    let (_, standard) = run_normal_recovery(&standard_fixture, NormalRecoveryRoute::Standard).await;
    assert_eq!(
        standard,
        NormalRecoverySnapshot {
            max_phy_offset: standard_fixture.second_end,
            confirm_offset: u64::try_from(standard_fixture.second_start).expect("second start fits u64"),
            max_queue_offset: 0,
        }
    );

    let optimized_fixture = two_segment_fixture(FirstSegmentShape::Blank).await;
    let (_, optimized) = run_normal_recovery(&optimized_fixture, NormalRecoveryRoute::Optimized).await;
    assert_eq!(
        optimized,
        NormalRecoverySnapshot {
            max_phy_offset: optimized_fixture.second_end,
            confirm_offset: u64::try_from(optimized_fixture.second_end).expect("second end fits u64"),
            max_queue_offset: 0,
        }
    );
}

#[tokio::test]
async fn normal_recovery_source_end_and_invalid_control_cross_segment_progress() {
    for shape in [FirstSegmentShape::SourceEnded, FirstSegmentShape::Invalid] {
        let standard_fixture = two_segment_fixture(shape).await;
        let (initial_confirm, standard) = run_normal_recovery(&standard_fixture, NormalRecoveryRoute::Standard).await;
        assert_eq!(
            standard,
            NormalRecoverySnapshot {
                max_phy_offset: 0,
                confirm_offset: initial_confirm,
                max_queue_offset: 0,
            }
        );

        let optimized_fixture = two_segment_fixture(shape).await;
        let (_, optimized) = run_normal_recovery(&optimized_fixture, NormalRecoveryRoute::Optimized).await;
        assert_eq!(
            optimized,
            NormalRecoverySnapshot {
                max_phy_offset: optimized_fixture.second_end,
                confirm_offset: u64::try_from(optimized_fixture.second_end).expect("second end fits u64"),
                max_queue_offset: 0,
            }
        );
    }
}

#[tokio::test]
async fn normal_recovery_empty_later_segment_freezes_route_specific_dual_watermarks() {
    let standard_fixture = two_segment_fixture(FirstSegmentShape::ValidThenBlank).await;
    let (_, standard) = run_normal_recovery(&standard_fixture, NormalRecoveryRoute::Standard).await;
    assert_eq!(
        standard,
        NormalRecoverySnapshot {
            max_phy_offset: standard_fixture.second_start,
            confirm_offset: 0,
            max_queue_offset: 0,
        }
    );

    let optimized_fixture = two_segment_fixture(FirstSegmentShape::ValidThenBlank).await;
    let (_, optimized) = run_normal_recovery(&optimized_fixture, NormalRecoveryRoute::Optimized).await;
    assert_eq!(
        optimized,
        NormalRecoverySnapshot {
            max_phy_offset: optimized_fixture.first_end,
            confirm_offset: u64::try_from(optimized_fixture.first_end).expect("first end fits u64"),
            max_queue_offset: 0,
        }
    );
}

#[tokio::test]
async fn normal_recovery_negative_consume_queue_offset_preserves_route_watermarks() {
    for route in [NormalRecoveryRoute::Standard, NormalRecoveryRoute::Optimized] {
        let baseline_fixture = two_segment_fixture(FirstSegmentShape::Blank).await;
        let (baseline_initial, baseline) = run_normal_recovery(&baseline_fixture, route).await;

        let negative_fixture = two_segment_fixture(FirstSegmentShape::Blank).await;
        let (negative_initial, negative) = run_normal_recovery_with_max_cq(&negative_fixture, route, -1).await;

        assert_eq!(negative_initial, baseline_initial);
        assert_eq!(negative, baseline);
    }
}

#[tokio::test]
async fn abnormal_recovery_blank_hook_rolls_without_dispatching_a_blank_record() {
    let standard_fixture = two_segment_fixture(FirstSegmentShape::Blank).await;
    let standard = run_abnormal_recovery_with_max_cq(&standard_fixture, NormalRecoveryRoute::Standard, 0).await;
    assert_eq!(
        standard,
        NormalRecoverySnapshot {
            max_phy_offset: standard_fixture.second_end,
            confirm_offset: u64::try_from(standard_fixture.second_start).expect("second start fits u64"),
            max_queue_offset: 1,
        }
    );

    let optimized_fixture = two_segment_fixture(FirstSegmentShape::Blank).await;
    let optimized = run_abnormal_recovery_with_max_cq(&optimized_fixture, NormalRecoveryRoute::Optimized, 0).await;
    assert_eq!(
        optimized,
        NormalRecoverySnapshot {
            max_phy_offset: optimized_fixture.second_end,
            confirm_offset: u64::try_from(optimized_fixture.second_end).expect("second end fits u64"),
            max_queue_offset: 1,
        }
    );
}

#[tokio::test]
async fn abnormal_recovery_invalid_and_source_end_keep_route_specific_cross_segment_actions() {
    for shape in [FirstSegmentShape::SourceEnded, FirstSegmentShape::Invalid] {
        let standard_fixture = two_segment_fixture(shape).await;
        let standard = run_abnormal_recovery_with_max_cq(&standard_fixture, NormalRecoveryRoute::Standard, 0).await;
        assert_eq!(
            standard,
            NormalRecoverySnapshot {
                max_phy_offset: 0,
                confirm_offset: 0,
                max_queue_offset: 0,
            }
        );

        let optimized_fixture = two_segment_fixture(shape).await;
        let optimized = run_abnormal_recovery_with_max_cq(&optimized_fixture, NormalRecoveryRoute::Optimized, 0).await;
        assert_eq!(
            optimized,
            NormalRecoverySnapshot {
                max_phy_offset: optimized_fixture.second_end,
                confirm_offset: u64::try_from(optimized_fixture.second_end).expect("second end fits u64"),
                max_queue_offset: 1,
            }
        );
    }
}

#[tokio::test]
async fn abnormal_recovery_empty_later_segment_freezes_route_specific_three_watermarks() {
    let standard_fixture = two_segment_fixture(FirstSegmentShape::ValidThenBlank).await;
    let standard = run_abnormal_recovery_with_max_cq(&standard_fixture, NormalRecoveryRoute::Standard, 0).await;
    assert_eq!(
        standard,
        NormalRecoverySnapshot {
            max_phy_offset: standard_fixture.second_start,
            confirm_offset: 0,
            max_queue_offset: 1,
        }
    );

    let optimized_fixture = two_segment_fixture(FirstSegmentShape::ValidThenBlank).await;
    let optimized = run_abnormal_recovery_with_max_cq(&optimized_fixture, NormalRecoveryRoute::Optimized, 0).await;
    assert_eq!(
        optimized,
        NormalRecoverySnapshot {
            max_phy_offset: optimized_fixture.first_end,
            confirm_offset: u64::try_from(optimized_fixture.first_end).expect("first end fits u64"),
            max_queue_offset: 1,
        }
    );
}

#[tokio::test]
async fn abnormal_recovery_negative_consume_queue_offset_preserves_route_watermarks() {
    for route in [NormalRecoveryRoute::Standard, NormalRecoveryRoute::Optimized] {
        let baseline_fixture = two_segment_fixture(FirstSegmentShape::Blank).await;
        let baseline = run_abnormal_recovery_with_max_cq(&baseline_fixture, route, 0).await;
        let negative_fixture = two_segment_fixture(FirstSegmentShape::Blank).await;
        let negative = run_abnormal_recovery_with_max_cq(&negative_fixture, route, -1).await;
        assert_eq!(negative, baseline);
    }
}

#[tokio::test]
async fn abnormal_recovery_dup_gate_skips_second_dispatch_but_keeps_physical_truncate() {
    let standard_fixture = two_segment_fixture(FirstSegmentShape::ValidPair).await;
    let standard = run_gated_abnormal_recovery(
        &standard_fixture,
        NormalRecoveryRoute::Standard,
        AbnormalRecoveryGateMode::Duplication,
    )
    .await;
    assert_eq!(standard_fixture.second_start, RECOVERY_SEGMENT_SIZE as i64);
    assert_eq!(
        standard,
        NormalRecoverySnapshot {
            max_phy_offset: standard_fixture.second_end,
            confirm_offset: u64::try_from(standard_fixture.second_start).expect("second start fits u64"),
            max_queue_offset: 1,
        }
    );

    let optimized_fixture = two_segment_fixture(FirstSegmentShape::ValidPair).await;
    let optimized = run_gated_abnormal_recovery(
        &optimized_fixture,
        NormalRecoveryRoute::Optimized,
        AbnormalRecoveryGateMode::Duplication,
    )
    .await;
    assert_eq!(
        optimized,
        NormalRecoverySnapshot {
            max_phy_offset: optimized_fixture.second_end,
            confirm_offset: u64::try_from(optimized_fixture.second_end).expect("second end fits u64"),
            max_queue_offset: 1,
        }
    );
}

#[tokio::test]
async fn abnormal_recovery_controller_clamps_to_last_confirm_eligible_input_end() {
    for route in [NormalRecoveryRoute::Standard, NormalRecoveryRoute::Optimized] {
        let fixture = two_segment_fixture(FirstSegmentShape::ValidPair).await;
        let snapshot = run_gated_abnormal_recovery(&fixture, route, AbnormalRecoveryGateMode::Controller).await;
        assert_eq!(
            snapshot,
            NormalRecoverySnapshot {
                max_phy_offset: fixture.second_end,
                confirm_offset: u64::try_from(fixture.first_end).expect("first end fits u64"),
                max_queue_offset: 1,
            }
        );
    }
}

#[derive(Debug, PartialEq)]
struct StoreParitySnapshot {
    get_status: Option<GetMessageStatus>,
    get_message_count: i32,
    query_message_count: usize,
    max_offset_in_queue: i64,
    total_in_queue: i64,
    max_phy_offset: i64,
}

async fn collect_restart_snapshot(store_type: StoreType, topic: &CheetahString) -> StoreParitySnapshot {
    let temp_dir = TempDir::new().expect("create temp dir");
    let group = CheetahString::from_static_str("phase6-store-parity-group");
    let key = CheetahString::from_static_str("phase6-store-parity-key");
    let mut config = phase6_store_config();
    config.store_type = store_type;

    let mut writer = new_test_store(&temp_dir, config.clone());
    writer.init().await.expect("init writer");
    assert!(writer.load().await, "load writer");

    for body in [b"phase6-parity-first".as_slice(), b"phase6-parity-second".as_slice()] {
        let mut msg = build_test_message(topic, 0, body);
        msg.set_keys(key.clone());
        let put_result = writer.put_message(msg).await;
        assert_eq!(put_result.put_message_status(), PutMessageStatus::PutOk);
    }

    writer.reput_once().await;
    writer.shutdown().await;
    drop(writer);

    let mut reloaded = new_test_store(&temp_dir, config);
    reloaded.init().await.expect("init reloaded store");
    assert!(reloaded.load().await, "load reloaded store");

    let get_result = reloaded
        .get_message(&group, topic, 0, 0, 32, None)
        .await
        .expect("get parity messages");
    let query_result = reloaded
        .query_message(topic, &key, 32, 0, i64::MAX)
        .await
        .expect("query parity messages");

    StoreParitySnapshot {
        get_status: get_result.status(),
        get_message_count: get_result.message_count(),
        query_message_count: query_result.message_maped_list.len(),
        max_offset_in_queue: reloaded.get_max_offset_in_queue(topic, 0),
        total_in_queue: reloaded.get_message_total_in_queue(topic, 0),
        max_phy_offset: reloaded.get_max_phy_offset(),
    }
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

#[tokio::test]
async fn file_store_vs_rocksdb_behavior_parity_after_restart() {
    let topic = CheetahString::from_static_str("phase6-store-parity-topic");

    let local_snapshot = collect_restart_snapshot(StoreType::LocalFile, &topic).await;
    let rocksdb_snapshot = collect_restart_snapshot(StoreType::RocksDB, &topic).await;

    assert_eq!(local_snapshot, rocksdb_snapshot);
    assert_eq!(local_snapshot.get_status, Some(GetMessageStatus::Found));
    assert_eq!(local_snapshot.get_message_count, 2);
    assert_eq!(local_snapshot.query_message_count, 2);
}
