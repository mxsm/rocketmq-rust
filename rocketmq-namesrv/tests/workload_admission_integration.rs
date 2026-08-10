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

use std::sync::Arc;

use rocketmq_namesrv::processor::workload_admission::NameServerWorkloadAdmission;
use rocketmq_namesrv::processor::workload_admission::WorkloadAdmissionClass;
use rocketmq_namesrv::processor::workload_admission::WorkloadAdmissionRejection;
use rocketmq_namesrv::NamesrvConfig;

#[tokio::test]
async fn broker_control_saturation_preserves_route_and_admin_reserves() {
    let config = NamesrvConfig {
        client_request_thread_pool_nums: 1,
        client_request_thread_pool_queue_capacity: 1,
        default_thread_pool_nums: 4,
        default_thread_pool_queue_capacity: 4,
        namesrv_workload_admission_timeout_millis: 10,
        ..NamesrvConfig::default()
    };
    let admission = Arc::new(NameServerWorkloadAdmission::from_namesrv_config(&config));
    let _broker_one = admission
        .acquire(WorkloadAdmissionClass::BrokerControl)
        .await
        .expect("first broker permit");
    let _broker_two = admission
        .acquire(WorkloadAdmissionClass::BrokerControl)
        .await
        .expect("second broker permit");
    let _broker_three = admission
        .acquire(WorkloadAdmissionClass::BrokerControl)
        .await
        .expect("third broker permit");

    let _route = admission
        .acquire(WorkloadAdmissionClass::RouteRead)
        .await
        .expect("route reserve must remain available");
    let _admin = admission
        .acquire(WorkloadAdmissionClass::Admin)
        .await
        .expect("admin reserve must remain available");

    let rejection = admission
        .acquire(WorkloadAdmissionClass::BrokerControl)
        .await
        .expect_err("saturated broker pool must time out");
    assert_eq!(rejection, WorkloadAdmissionRejection::TimedOut);
}

#[tokio::test]
async fn java_pool_names_define_real_rust_permit_and_queue_limits() {
    let config = NamesrvConfig {
        client_request_thread_pool_nums: 1,
        client_request_thread_pool_queue_capacity: 1,
        default_thread_pool_nums: 2,
        default_thread_pool_queue_capacity: 2,
        namesrv_workload_admission_timeout_millis: 50,
        ..NamesrvConfig::default()
    };
    let admission = Arc::new(NameServerWorkloadAdmission::from_namesrv_config(&config));
    let _active = admission
        .acquire(WorkloadAdmissionClass::RouteRead)
        .await
        .expect("active route");
    let waiting_admission = Arc::clone(&admission);
    let waiting = tokio::spawn(async move { waiting_admission.acquire(WorkloadAdmissionClass::RouteRead).await });
    tokio::task::yield_now().await;

    assert_eq!(admission.snapshot().route_inflight, 1);
    assert_eq!(admission.snapshot().route_waiting, 1);
    assert_eq!(
        admission
            .acquire(WorkloadAdmissionClass::RouteRead)
            .await
            .expect_err("configured route queue is full"),
        WorkloadAdmissionRejection::QueueFull
    );

    waiting.abort();
    let _ = waiting.await;
    assert_eq!(admission.snapshot().route_waiting, 0);
}
