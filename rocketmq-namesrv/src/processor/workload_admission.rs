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

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;

use crate::config::NamesrvConfig;
use crate::security::NameServerRequestClass;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkloadAdmissionClass {
    RouteRead,
    BrokerControl,
    Admin,
}

impl WorkloadAdmissionClass {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RouteRead => "route-read",
            Self::BrokerControl => "broker-control",
            Self::Admin => "admin",
        }
    }
}

impl From<NameServerRequestClass> for WorkloadAdmissionClass {
    fn from(value: NameServerRequestClass) -> Self {
        match value {
            NameServerRequestClass::RouteRead => Self::RouteRead,
            NameServerRequestClass::BrokerControl => Self::BrokerControl,
            NameServerRequestClass::AdminRead | NameServerRequestClass::AdminWrite => Self::Admin,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkloadAdmissionRejection {
    QueueFull,
    TimedOut,
}

impl WorkloadAdmissionRejection {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::QueueFull => "queue-full",
            Self::TimedOut => "timeout",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WorkloadAdmissionSnapshot {
    pub route_inflight: usize,
    pub route_waiting: usize,
    pub broker_inflight: usize,
    pub broker_waiting: usize,
    pub admin_inflight: usize,
    pub admin_waiting: usize,
}

#[derive(Debug)]
struct AdmissionPool {
    semaphore: Arc<Semaphore>,
    max_permits: usize,
    max_waiting: usize,
    waiting: AtomicUsize,
}

impl AdmissionPool {
    fn new(max_permits: usize, max_waiting: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_permits)),
            max_permits,
            max_waiting,
            waiting: AtomicUsize::new(0),
        }
    }

    fn inflight(&self) -> usize {
        self.max_permits.saturating_sub(self.semaphore.available_permits())
    }

    fn waiting(&self) -> usize {
        self.waiting.load(Ordering::Acquire)
    }

    fn reserve_waiter(&self) -> Result<WaitingGuard<'_>, WorkloadAdmissionRejection> {
        let reserved = self
            .waiting
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |waiting| {
                (waiting < self.max_waiting).then_some(waiting + 1)
            })
            .is_ok();
        if reserved {
            Ok(WaitingGuard { pool: self })
        } else {
            Err(WorkloadAdmissionRejection::QueueFull)
        }
    }
}

struct WaitingGuard<'a> {
    pool: &'a AdmissionPool,
}

impl Drop for WaitingGuard<'_> {
    fn drop(&mut self) {
        self.pool.waiting.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Debug)]
pub struct WorkloadAdmissionLease {
    _permit: OwnedSemaphorePermit,
    queued: bool,
}

impl WorkloadAdmissionLease {
    #[must_use]
    pub const fn was_queued(&self) -> bool {
        self.queued
    }
}

#[derive(Debug)]
pub struct NameServerWorkloadAdmission {
    route: AdmissionPool,
    broker: AdmissionPool,
    admin: AdmissionPool,
    queue_timeout: Duration,
}

impl NameServerWorkloadAdmission {
    #[must_use]
    pub fn from_namesrv_config(config: &NamesrvConfig) -> Self {
        let route_permits = positive_usize(config.client_request_thread_pool_nums);
        let default_permits = positive_usize(config.default_thread_pool_nums);
        let route_waiting = positive_usize(config.client_request_thread_pool_queue_capacity);
        let default_waiting = positive_usize(config.default_thread_pool_queue_capacity);
        let (broker_permits, admin_permits) = split_default_capacity(default_permits);
        let (broker_waiting, admin_waiting) = split_default_capacity(default_waiting);

        Self {
            route: AdmissionPool::new(route_permits, route_waiting),
            broker: AdmissionPool::new(broker_permits, broker_waiting),
            admin: AdmissionPool::new(admin_permits, admin_waiting),
            queue_timeout: Duration::from_millis(config.namesrv_workload_admission_timeout_millis),
        }
    }

    #[cfg(test)]
    fn with_limits(
        route: (usize, usize),
        broker: (usize, usize),
        admin: (usize, usize),
        queue_timeout: Duration,
    ) -> Self {
        Self {
            route: AdmissionPool::new(route.0, route.1),
            broker: AdmissionPool::new(broker.0, broker.1),
            admin: AdmissionPool::new(admin.0, admin.1),
            queue_timeout,
        }
    }

    pub async fn acquire(
        &self,
        class: WorkloadAdmissionClass,
    ) -> Result<WorkloadAdmissionLease, WorkloadAdmissionRejection> {
        let pool = self.pool(class);
        if let Ok(permit) = Arc::clone(&pool.semaphore).try_acquire_owned() {
            return Ok(WorkloadAdmissionLease {
                _permit: permit,
                queued: false,
            });
        }

        let waiting = pool.reserve_waiter()?;
        let acquired = tokio::time::timeout(self.queue_timeout, Arc::clone(&pool.semaphore).acquire_owned()).await;
        drop(waiting);
        match acquired {
            Ok(Ok(permit)) => Ok(WorkloadAdmissionLease {
                _permit: permit,
                queued: true,
            }),
            Ok(Err(_)) | Err(_) => Err(WorkloadAdmissionRejection::TimedOut),
        }
    }

    #[must_use]
    pub fn try_observe(&self, class: WorkloadAdmissionClass) -> Option<WorkloadAdmissionLease> {
        Arc::clone(&self.pool(class).semaphore)
            .try_acquire_owned()
            .ok()
            .map(|permit| WorkloadAdmissionLease {
                _permit: permit,
                queued: false,
            })
    }

    #[must_use]
    pub fn snapshot(&self) -> WorkloadAdmissionSnapshot {
        WorkloadAdmissionSnapshot {
            route_inflight: self.route.inflight(),
            route_waiting: self.route.waiting(),
            broker_inflight: self.broker.inflight(),
            broker_waiting: self.broker.waiting(),
            admin_inflight: self.admin.inflight(),
            admin_waiting: self.admin.waiting(),
        }
    }

    #[must_use]
    pub fn class_counts(&self, class: WorkloadAdmissionClass) -> (usize, usize) {
        let pool = self.pool(class);
        (pool.inflight(), pool.waiting())
    }

    fn pool(&self, class: WorkloadAdmissionClass) -> &AdmissionPool {
        match class {
            WorkloadAdmissionClass::RouteRead => &self.route,
            WorkloadAdmissionClass::BrokerControl => &self.broker,
            WorkloadAdmissionClass::Admin => &self.admin,
        }
    }
}

fn split_default_capacity(total: usize) -> (usize, usize) {
    let admin = (total / 4).max(1);
    (total - admin, admin)
}

fn positive_usize(value: i32) -> usize {
    usize::try_from(value).unwrap_or(1).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_classes_map_to_the_expected_admission_pool_and_label() {
        for (request_class, admission_class, label) in [
            (
                NameServerRequestClass::RouteRead,
                WorkloadAdmissionClass::RouteRead,
                "route-read",
            ),
            (
                NameServerRequestClass::BrokerControl,
                WorkloadAdmissionClass::BrokerControl,
                "broker-control",
            ),
            (
                NameServerRequestClass::AdminRead,
                WorkloadAdmissionClass::Admin,
                "admin",
            ),
            (
                NameServerRequestClass::AdminWrite,
                WorkloadAdmissionClass::Admin,
                "admin",
            ),
        ] {
            assert_eq!(WorkloadAdmissionClass::from(request_class), admission_class);
            assert_eq!(admission_class.as_str(), label);
        }
        assert_eq!(WorkloadAdmissionRejection::QueueFull.as_str(), "queue-full");
        assert_eq!(WorkloadAdmissionRejection::TimedOut.as_str(), "timeout");
    }

    #[test]
    fn capacity_helpers_clamp_inputs_and_reserve_the_admin_share() {
        for (input, expected) in [(i32::MIN, 1), (-1, 1), (0, 1), (1, 1), (8, 8)] {
            assert_eq!(positive_usize(input), expected);
        }
        for (total, expected) in [(1, (0, 1)), (3, (2, 1)), (4, (3, 1)), (5, (4, 1)), (8, (6, 2))] {
            assert_eq!(split_default_capacity(total), expected);
        }
    }

    #[test]
    fn nonblocking_observation_releases_each_class_permit_on_drop() {
        let admission = NameServerWorkloadAdmission::with_limits((1, 1), (1, 1), (1, 1), Duration::from_secs(10));
        for class in [
            WorkloadAdmissionClass::RouteRead,
            WorkloadAdmissionClass::BrokerControl,
            WorkloadAdmissionClass::Admin,
        ] {
            let lease = admission.try_observe(class).unwrap();
            assert!(!lease.was_queued());
            assert_eq!(admission.class_counts(class), (1, 0));
            assert!(admission.try_observe(class).is_none());
            drop(lease);
            assert_eq!(admission.snapshot(), WorkloadAdmissionSnapshot::default());
            assert!(admission.try_observe(class).is_some());
        }
    }

    #[tokio::test]
    async fn queued_acquires_update_only_their_class_and_mark_the_lease() {
        let admission = NameServerWorkloadAdmission::with_limits((1, 1), (1, 1), (1, 1), Duration::from_secs(10));
        let classes = [
            WorkloadAdmissionClass::RouteRead,
            WorkloadAdmissionClass::BrokerControl,
            WorkloadAdmissionClass::Admin,
        ];
        for (class, expected) in [
            (
                classes[0],
                WorkloadAdmissionSnapshot {
                    route_inflight: 1,
                    route_waiting: 1,
                    ..Default::default()
                },
            ),
            (
                classes[1],
                WorkloadAdmissionSnapshot {
                    broker_inflight: 1,
                    broker_waiting: 1,
                    ..Default::default()
                },
            ),
            (
                classes[2],
                WorkloadAdmissionSnapshot {
                    admin_inflight: 1,
                    admin_waiting: 1,
                    ..Default::default()
                },
            ),
        ] {
            let active = admission.acquire(class).await.unwrap();
            assert!(!active.was_queued());
            let mut waiting = std::pin::pin!(admission.acquire(class));
            assert!(futures::poll!(&mut waiting).is_pending());
            assert_eq!(admission.snapshot(), expected);
            for observed_class in classes {
                assert_eq!(
                    admission.class_counts(observed_class),
                    if observed_class == class { (1, 1) } else { (0, 0) }
                );
            }
            drop(active);
            let queued = waiting.await.unwrap();
            assert!(queued.was_queued());
            assert_eq!(admission.class_counts(class), (1, 0));
            drop(queued);
            assert_eq!(admission.snapshot(), WorkloadAdmissionSnapshot::default());
        }
    }

    #[test]
    fn config_sizes_the_pools_and_their_waiting_limits() {
        let default_config = NamesrvConfig::default();
        let default_admission = NameServerWorkloadAdmission::from_namesrv_config(&default_config);
        assert_eq!(
            default_admission.route.max_permits,
            default_config.client_request_thread_pool_nums as usize
        );
        assert_eq!(
            default_admission.broker.max_permits + default_admission.admin.max_permits,
            default_config.default_thread_pool_nums as usize
        );
        assert_eq!(default_admission.snapshot(), WorkloadAdmissionSnapshot::default());

        let config = NamesrvConfig {
            client_request_thread_pool_nums: 3,
            default_thread_pool_nums: 8,
            client_request_thread_pool_queue_capacity: 5,
            default_thread_pool_queue_capacity: 12,
            namesrv_workload_admission_timeout_millis: 17,
            ..Default::default()
        };
        let admission = NameServerWorkloadAdmission::from_namesrv_config(&config);
        for (class, permits, waiting) in [
            (WorkloadAdmissionClass::RouteRead, 3, 5),
            (WorkloadAdmissionClass::BrokerControl, 6, 9),
            (WorkloadAdmissionClass::Admin, 2, 3),
        ] {
            let pool = admission.pool(class);
            assert_eq!((pool.max_permits, pool.max_waiting), (permits, waiting));
            let leases: Vec<_> = (0..permits).map(|_| admission.try_observe(class).unwrap()).collect();
            assert!(admission.try_observe(class).is_none());
            drop(leases);
        }
        assert_eq!(admission.queue_timeout, Duration::from_millis(17));
    }

    #[tokio::test]
    async fn broker_saturation_does_not_consume_route_capacity() {
        let admission = NameServerWorkloadAdmission::with_limits((1, 1), (1, 1), (1, 1), Duration::from_millis(50));
        let _broker = admission
            .acquire(WorkloadAdmissionClass::BrokerControl)
            .await
            .expect("broker permit");

        let _route = admission
            .acquire(WorkloadAdmissionClass::RouteRead)
            .await
            .expect("route permit remains isolated");
        assert_eq!(admission.snapshot().broker_inflight, 1);
        assert_eq!(admission.snapshot().route_inflight, 1);
    }

    #[tokio::test]
    async fn full_queue_and_timeout_have_stable_rejections_without_leaks() {
        let admission = Arc::new(NameServerWorkloadAdmission::with_limits(
            (1, 1),
            (1, 1),
            (1, 1),
            Duration::from_millis(5),
        ));
        let _active = admission
            .acquire(WorkloadAdmissionClass::RouteRead)
            .await
            .expect("active route");
        let queued_admission = Arc::clone(&admission);
        let queued = tokio::spawn(async move { queued_admission.acquire(WorkloadAdmissionClass::RouteRead).await });
        tokio::task::yield_now().await;
        assert_eq!(admission.snapshot().route_waiting, 1);

        assert!(matches!(
            admission.acquire(WorkloadAdmissionClass::RouteRead).await,
            Err(WorkloadAdmissionRejection::QueueFull)
        ));
        assert!(matches!(
            queued.await.expect("queued task"),
            Err(WorkloadAdmissionRejection::TimedOut)
        ));
        assert_eq!(admission.snapshot().route_waiting, 0);
    }

    #[tokio::test]
    async fn cancelling_waiter_releases_waiting_slot() {
        let admission = Arc::new(NameServerWorkloadAdmission::with_limits(
            (1, 1),
            (1, 1),
            (1, 1),
            Duration::from_secs(30),
        ));
        let _active = admission
            .acquire(WorkloadAdmissionClass::Admin)
            .await
            .expect("active admin");
        let waiting_admission = Arc::clone(&admission);
        let waiting = tokio::spawn(async move { waiting_admission.acquire(WorkloadAdmissionClass::Admin).await });
        tokio::task::yield_now().await;
        assert_eq!(admission.snapshot().admin_waiting, 1);

        waiting.abort();
        let _ = waiting.await;
        assert_eq!(admission.snapshot().admin_waiting, 0);
    }
}
