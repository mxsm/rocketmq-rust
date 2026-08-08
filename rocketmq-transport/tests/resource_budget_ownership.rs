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

use std::net::IpAddr;
use std::net::Ipv4Addr;

use rocketmq_runtime::ProcessMemoryLimit;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_transport::api::v1::AdmissionClass;
use rocketmq_transport::api::v1::AdmissionController;
use rocketmq_transport::api::v1::AdmissionLimits;
use rocketmq_transport::api::v1::AdmissionResource;
use rocketmq_transport::api::v1::AdmissionScope;
use rocketmq_transport::api::v1::ResourceLimit;

#[test]
fn transport_controllers_share_the_injected_process_ceiling() {
    let memory_limit = ProcessMemoryLimit::configured(12).expect("configured memory limit");
    let owner = RuntimeOwner::new_with_memory_limit(RuntimeConfig::default(), memory_limit).expect("runtime owner");
    let process_budget = owner.root_context().component("transport").process_budget();
    let limits = AdmissionLimits {
        inflight: ResourceLimit { count: 4, bytes: 12 },
        per_ip: ResourceLimit { count: 4, bytes: 12 },
        per_tenant: ResourceLimit { count: 4, bytes: 12 },
        per_session: ResourceLimit { count: 4, bytes: 12 },
        control_reserve: ResourceLimit { count: 0, bytes: 0 },
        ..AdmissionLimits::default()
    };
    let first = AdmissionController::try_new_with_budget(limits, &process_budget).expect("first controller");
    let second = AdmissionController::try_new_with_budget(limits, &process_budget).expect("second controller");
    let scope = AdmissionScope::new(IpAddr::V4(Ipv4Addr::LOCALHOST));

    let permit = first
        .try_acquire(AdmissionResource::Inflight, scope, 8, AdmissionClass::Data)
        .expect("first controller reservation");
    assert!(second
        .try_acquire(AdmissionResource::Inflight, scope, 8, AdmissionClass::Data)
        .is_err());
    assert_eq!(process_budget.snapshot().current_bytes, 8);

    drop(permit);
    assert!(second
        .try_acquire(AdmissionResource::Inflight, scope, 8, AdmissionClass::Data)
        .is_ok());
}
