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

//! Checksummed snapshot restore tests.

use std::io::Cursor;

use openraft::storage::RaftStateMachine;
use openraft::RaftSnapshotBuilder;
use rocketmq_controller::config::ControllerConfig;
use rocketmq_controller::config::ControllerConfigReader;
use rocketmq_controller::openraft::StateMachine;
use rocketmq_controller::openraft::SNAPSHOT_MAX_BYTES;
use rocketmq_controller::typ::ControllerRequest;
use rocketmq_controller::typ::EntryPayload;
use rocketmq_controller::typ::LogEntry;
use rocketmq_controller::typ::LogId;
use rocketmq_controller::typ::SnapshotMeta;
use rocketmq_controller::typ::Vote;

fn state_machine() -> StateMachine {
    StateMachine::new(ControllerConfigReader::new(ControllerConfig::test_config()))
}

async fn apply_broker_id(state_machine: &mut StateMachine, broker_name: &str, broker_id: u64) {
    let entry = LogEntry {
        log_id: LogId {
            leader_id: Vote::new(1, 1).leader_id,
            index: 1,
        },
        payload: EntryPayload::Normal(ControllerRequest::ApplyBrokerId {
            cluster_name: "snapshot-cluster".to_string(),
            broker_name: broker_name.to_string(),
            broker_address: "127.0.0.1:10911".to_string(),
            applied_broker_id: broker_id,
            register_check_code: "check-code".to_string(),
        }),
    };
    let entries = futures::stream::iter([Ok::<_, std::io::Error>((entry, None))]);
    RaftStateMachine::apply(state_machine, entries)
        .await
        .expect("apply committed broker id");
}

fn next_broker_id(state_machine: &StateMachine, broker_name: &str) -> u64 {
    state_machine
        .read_view()
        .get_next_broker_id("snapshot-cluster", broker_name)
        .response()
        .and_then(|header| header.next_broker_id)
        .expect("next broker id")
}

#[tokio::test]
async fn checksummed_snapshot_round_trip_restores_controller_state() {
    let mut source = state_machine();
    apply_broker_id(&mut source, "broker-a", 1).await;
    let snapshot = source.build_snapshot().await.expect("build snapshot");

    let mut restored = state_machine();
    restored
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .expect("install snapshot");

    assert_eq!(next_broker_id(&restored, "broker-a"), 2);
}

#[tokio::test]
async fn corrupt_snapshot_is_rejected_without_mutating_live_state() {
    let mut source = state_machine();
    apply_broker_id(&mut source, "incoming", 1).await;
    let snapshot = source.build_snapshot().await.expect("build snapshot");
    let mut corrupted = snapshot.snapshot.into_inner();
    let byte = corrupted.last_mut().expect("non-empty snapshot");
    *byte ^= 0x01;

    let mut target = state_machine();
    apply_broker_id(&mut target, "existing", 1).await;
    let error = target
        .install_snapshot(&snapshot.meta, Cursor::new(corrupted))
        .await
        .expect_err("corrupt snapshot must fail");

    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert_eq!(next_broker_id(&target, "existing"), 2);
    assert_eq!(next_broker_id(&target, "incoming"), 1);
}

#[tokio::test]
async fn oversized_snapshot_is_rejected_before_install() {
    let mut target = state_machine();
    let error = target
        .install_snapshot(
            &SnapshotMeta {
                last_log_id: None,
                last_membership: Default::default(),
                snapshot_id: "oversized".to_string(),
            },
            Cursor::new(vec![0; SNAPSHOT_MAX_BYTES + 1]),
        )
        .await
        .expect_err("oversized snapshot must fail");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
}

#[tokio::test]
async fn mismatched_snapshot_metadata_is_rejected() {
    let mut source = state_machine();
    apply_broker_id(&mut source, "broker-a", 1).await;
    let snapshot = source.build_snapshot().await.expect("build snapshot");
    let mut mismatched_meta = snapshot.meta.clone();
    mismatched_meta.snapshot_id = "different-id".to_string();

    let mut target = state_machine();
    let error = target
        .install_snapshot(&mismatched_meta, snapshot.snapshot)
        .await
        .expect_err("metadata mismatch must fail");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
}
