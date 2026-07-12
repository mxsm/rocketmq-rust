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
use std::str::FromStr;

use rocketmq_transport::admission::AdmissionClass;
use rocketmq_transport::admission::AdmissionController;
use rocketmq_transport::admission::AdmissionLimits;
use rocketmq_transport::admission::AdmissionResource;
use rocketmq_transport::admission::AdmissionScope;
use rocketmq_transport::admission::FullPolicy;
use rocketmq_transport::admission::ResourceLimit;

#[test]
fn global_ip_tenant_and_session_limits_release_as_one_permit() {
    let limits = AdmissionLimits {
        inflight: ResourceLimit { count: 2, bytes: 16 },
        per_ip: ResourceLimit { count: 1, bytes: 8 },
        per_tenant: ResourceLimit { count: 1, bytes: 8 },
        per_session: ResourceLimit { count: 1, bytes: 8 },
        ..AdmissionLimits::default()
    };
    let controller = AdmissionController::new(limits);
    let scope = AdmissionScope::new(IpAddr::from_str("127.0.0.1").unwrap())
        .with_tenant(7)
        .with_session(9);
    let permit = controller
        .try_acquire(AdmissionResource::Inflight, scope, 8, AdmissionClass::Data)
        .unwrap();
    let error = controller
        .try_acquire(AdmissionResource::Inflight, scope, 1, AdmissionClass::Data)
        .unwrap_err();
    assert_eq!(error.policy(), FullPolicy::Reject);
    assert_eq!(controller.snapshot().inflight.current_count, 1);
    assert_eq!(controller.snapshot().inflight.current_bytes, 8);

    drop(permit);
    assert_eq!(controller.snapshot().inflight.current_count, 0);
    assert_eq!(controller.snapshot().inflight.current_bytes, 0);
    assert!(controller
        .try_acquire(AdmissionResource::Inflight, scope, 8, AdmissionClass::Data)
        .is_ok());
}

#[test]
fn control_reserve_remains_bounded_and_available_during_data_overload() {
    let limits = AdmissionLimits {
        processors: ResourceLimit { count: 2, bytes: 8 },
        control_reserve: ResourceLimit { count: 1, bytes: 4 },
        per_ip: ResourceLimit { count: 2, bytes: 8 },
        per_tenant: ResourceLimit { count: 2, bytes: 8 },
        per_session: ResourceLimit { count: 2, bytes: 8 },
        ..AdmissionLimits::default()
    };
    let controller = AdmissionController::new(limits);
    let scope = AdmissionScope::new(IpAddr::from_str("127.0.0.2").unwrap());
    let data = controller
        .try_acquire(AdmissionResource::Processor, scope, 4, AdmissionClass::Data)
        .unwrap();
    assert!(controller
        .try_acquire(AdmissionResource::Processor, scope, 1, AdmissionClass::Data)
        .is_err());
    let control = controller
        .try_acquire(AdmissionResource::Processor, scope, 4, AdmissionClass::Control)
        .expect("reserved control capacity must remain available");
    assert!(controller
        .try_acquire(AdmissionResource::Processor, scope, 1, AdmissionClass::Control)
        .is_err());
    drop((data, control));
}

#[tokio::test]
async fn dropped_collector_never_blocks_or_unbounds_data_plane_admission() {
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    drop(receiver);
    let controller = AdmissionController::with_observer(AdmissionLimits::default(), sender);
    let scope = AdmissionScope::new(IpAddr::from_str("127.0.0.3").unwrap());
    for _ in 0..100 {
        let permit = controller
            .try_acquire(AdmissionResource::Queued, scope, 1, AdmissionClass::Data)
            .unwrap();
        drop(permit);
    }
    assert_eq!(controller.snapshot().queued.current_count, 0);
}

#[test]
fn released_session_scopes_are_reclaimed_before_the_key_limit_rejects_new_sessions() {
    let controller = AdmissionController::new(AdmissionLimits {
        max_scope_keys: 3,
        ..AdmissionLimits::default()
    });
    let ip = IpAddr::from_str("127.0.0.4").unwrap();

    for session in 1..=100 {
        let permit = controller
            .try_acquire(
                AdmissionResource::Connection,
                AdmissionScope::new(ip).with_session(session),
                0,
                AdmissionClass::Data,
            )
            .unwrap_or_else(|error| panic!("released session {session} should not exhaust scope keys: {error}"));
        drop(permit);
    }
}

#[test]
fn scope_reclamation_is_safe_when_release_and_next_acquire_cross_threads() {
    let controller = std::sync::Arc::new(AdmissionController::new(AdmissionLimits {
        max_scope_keys: 2,
        ..AdmissionLimits::default()
    }));
    let ip = IpAddr::from_str("127.0.0.5").unwrap();
    let first = controller
        .try_acquire(
            AdmissionResource::Connection,
            AdmissionScope::new(ip).with_session(1),
            0,
            AdmissionClass::Data,
        )
        .unwrap();
    let released = std::sync::Arc::new(std::sync::Barrier::new(2));
    let release_barrier = released.clone();
    let releasing = std::thread::spawn(move || {
        drop(first);
        release_barrier.wait();
    });
    released.wait();

    let second = controller.try_acquire(
        AdmissionResource::Connection,
        AdmissionScope::new(ip).with_session(2),
        0,
        AdmissionClass::Data,
    );
    releasing.join().unwrap();
    assert!(
        second.is_ok(),
        "a fully released scope must be reclaimable across threads"
    );
}
