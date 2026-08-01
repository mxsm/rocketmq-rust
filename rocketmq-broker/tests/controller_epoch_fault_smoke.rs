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

//! Broker-side consumer tests for the canonical Controller authority contract.

use rocketmq_store_api::decide_replication;
use rocketmq_store_api::AckPolicy;
use rocketmq_store_api::HaRejectReason;
use rocketmq_store_api::MasterEpoch;
use rocketmq_store_api::ReplicationDecision;
use rocketmq_store_api::ReplicationObservation;
use rocketmq_store_api::SyncStateSet;
use rocketmq_store_api::WriteAuthority;

fn authority(broker_id: i64, epoch: i32) -> WriteAuthority {
    WriteAuthority::try_new(broker_id, MasterEpoch::try_from(epoch).expect("positive epoch"))
        .expect("non-negative broker id")
}

fn evaluate(current: WriteAuthority, requested: WriteAuthority) -> ReplicationDecision {
    decide_replication(
        &ReplicationObservation::try_new(
            current,
            requested,
            AckPolicy::LocalDurable,
            32,
            32,
            Vec::new(),
            SyncStateSet::try_new([current.broker_id()]).expect("leader ISR"),
        )
        .expect("valid Broker authority observation"),
    )
}

#[test]
fn broker_rejects_the_previous_master_after_epoch_advance() {
    assert_eq!(
        evaluate(authority(2, 8), authority(1, 7)),
        ReplicationDecision::Reject(HaRejectReason::StaleAuthority)
    );
}

#[test]
fn broker_rejects_a_different_master_without_an_installed_epoch_advance() {
    assert_eq!(
        evaluate(authority(1, 8), authority(2, 8)),
        ReplicationDecision::Reject(HaRejectReason::AuthorityMismatch)
    );
}
