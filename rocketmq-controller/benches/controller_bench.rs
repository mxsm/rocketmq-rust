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

use std::hint::black_box;
use std::time::Duration;

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::Criterion;
use rocketmq_controller::BrokerIdentityInfoSnapshot;
use rocketmq_controller::BrokerLiveInfoSnapshot;
use rocketmq_controller::ConfirmOffsetAudit;
use rocketmq_controller::ControllerConfig;
use rocketmq_controller::ControllerConfigReader;
use rocketmq_controller::ControllerRequest;
use rocketmq_controller::FailoverMilestone;
use rocketmq_controller::FailoverTimeline;
use rocketmq_controller::PutOkMessageAudit;
use rocketmq_controller::ReplicasInfoManager;

fn heartbeat_fixture() -> (BrokerIdentityInfoSnapshot, BrokerLiveInfoSnapshot) {
    (
        BrokerIdentityInfoSnapshot::new("benchmark-cluster", "broker-a", Some(1)),
        BrokerLiveInfoSnapshot {
            cluster_name: "benchmark-cluster".to_string(),
            broker_name: "broker-a".to_string(),
            broker_addr: "127.0.0.1:10911".to_string(),
            broker_id: 1,
            last_update_timestamp: 1_000,
            heartbeat_timeout_millis: 10_000,
            epoch: 1,
            max_offset: 100,
            confirm_offset: 100,
            election_priority: Some(1),
        },
    )
}

fn controller_bench(criterion: &mut Criterion) {
    let (identity, heartbeat) = heartbeat_fixture();
    let manager = ReplicasInfoManager::new(ControllerConfigReader::new(ControllerConfig::test_config()));
    manager.on_broker_heartbeat(identity.clone(), heartbeat.clone());
    criterion.bench_function("controller/replicated_heartbeat_apply", |bencher| {
        bencher.iter(|| {
            manager.on_broker_heartbeat(black_box(identity.clone()), black_box(heartbeat.clone()));
        });
    });

    let request = ControllerRequest::BrokerHeartbeat {
        broker_identity: identity,
        broker_live_info: heartbeat,
    };
    criterion.bench_function("controller/raft_request_json_encode", |bencher| {
        bencher.iter(|| black_box(serde_json::to_vec(black_box(&request)).expect("serialize benchmark request")));
    });

    criterion.bench_function("controller/failover_evidence_record", |bencher| {
        bencher.iter(|| {
            let mut timeline = FailoverTimeline::new();
            timeline
                .record_elapsed(FailoverMilestone::ControllerLeaderElected, Duration::from_millis(900))
                .expect("record controller leader");
            timeline
                .record_elapsed(FailoverMilestone::BrokerMasterElected, Duration::from_millis(1_400))
                .expect("record broker master");
            timeline
                .record_elapsed(
                    FailoverMilestone::StoreWriteAuthorityGranted,
                    Duration::from_millis(1_700),
                )
                .expect("record write authority");
            timeline
                .record_elapsed(FailoverMilestone::RouteConverged, Duration::from_millis(2_200))
                .expect("record route convergence");
            timeline
                .record_elapsed(FailoverMilestone::ProducerRecovered, Duration::from_millis(2_400))
                .expect("record producer recovery");
            let mut messages = PutOkMessageAudit::with_limits(1, 1);
            messages.record_put_ok("message-a", 100).expect("record PutOk message");
            messages.observe_recovered("message-a", 100);
            let mut confirm = ConfirmOffsetAudit::with_sample_limit(1);
            confirm.observe(1, 100, 100);
            black_box((timeline.snapshot(), messages.report(), confirm.report()));
        });
    });
}

criterion_group!(benches, controller_bench);
criterion_main!(benches);
