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

use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering as AtomicOrdering;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use parking_lot::Mutex;
use parking_lot::RwLock;
use rocketmq_runtime::ScheduledTaskSnapshot;
use serde::Serialize;

use crate::latency::latency_fault_tolerance::LatencyFaultTolerance;
use crate::latency::resolver::Resolver;
use crate::latency::service_detector::ServiceDetector;
use crate::runtime::schedule_client_fixed_delay_task;
use crate::runtime::ClientScheduledTaskHandle;

const DETECTOR_TASK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const DETECTOR_SCAN_INITIAL_DELAY: Duration = Duration::from_secs(3);
const DETECTOR_SCAN_INTERVAL: Duration = Duration::from_secs(3);

pub struct LatencyFaultToleranceImpl<R, S> {
    fault_item_table: DashMap<CheetahString, FaultItem>,
    detect_timeout: AtomicU32,
    detect_interval: AtomicU32,
    start_detector_enable: AtomicBool,
    resolver: RwLock<Option<Arc<R>>>,
    service_detector: RwLock<Option<Arc<S>>>,
    detector_lifecycle: Mutex<DetectorLifecycle>,
}

#[derive(Default)]
struct DetectorLifecycle {
    task: Option<FaultDetectorTaskHandle>,
    stopping: bool,
}

struct DetectorStoppingReset<'a> {
    lifecycle: &'a Mutex<DetectorLifecycle>,
}

impl Drop for DetectorStoppingReset<'_> {
    fn drop(&mut self) {
        self.lifecycle.lock().stopping = false;
    }
}

struct FaultDetectorTaskHandle {
    handle: ClientScheduledTaskHandle,
}

impl FaultDetectorTaskHandle {
    fn is_finished(&self) -> bool {
        !self.handle.is_running()
    }

    fn abort(self) {
        let report = self.handle.shutdown_now();
        if !report.is_healthy() {
            tracing::warn!(
                report = %report.to_json(),
                "fault detector task shutdown_now report is unhealthy"
            );
        }
    }

    async fn shutdown(self, timeout: Duration) -> bool {
        let report = self.handle.shutdown(timeout).await;
        report.is_healthy()
    }

    fn task_count(&self) -> usize {
        self.handle.task_count()
    }

    fn schedule_snapshot(&self) -> Vec<ScheduledTaskSnapshot> {
        self.handle.schedule_snapshot()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct LatencyFaultDetectorLifecycleProbe {
    pub task_count_before_shutdown: usize,
    pub task_count_after_shutdown: usize,
    pub scheduled_runs: u64,
    pub scheduled_skips: u64,
    pub scheduled_overlaps: u64,
    pub scheduled_failures: u64,
    pub shutdown_elapsed_us: u128,
    pub healthy: bool,
}

impl<R, S> LatencyFaultToleranceImpl<R, S> {
    pub fn new() -> Self {
        Self {
            resolver: RwLock::new(None),
            service_detector: RwLock::new(None),
            fault_item_table: Default::default(),
            detect_timeout: AtomicU32::new(200),
            detect_interval: AtomicU32::new(2000),
            start_detector_enable: AtomicBool::new(false),
            detector_lifecycle: Mutex::new(DetectorLifecycle::default()),
        }
    }

    pub fn set_resolver(&self, resolver: R) {
        *self.resolver.write() = Some(Arc::new(resolver));
    }

    pub fn set_service_detector(&self, service_detector: S) {
        *self.service_detector.write() = Some(Arc::new(service_detector));
    }

    #[cfg(test)]
    pub(crate) fn detector_config_for_test(&self) -> (u32, u32) {
        (
            self.detect_interval.load(AtomicOrdering::Acquire),
            self.detect_timeout.load(AtomicOrdering::Acquire),
        )
    }

    pub async fn shutdown_detector(&self) -> bool {
        let handle = {
            let mut lifecycle = self.detector_lifecycle.lock();
            if lifecycle.stopping {
                return true;
            }
            lifecycle.stopping = true;
            lifecycle.task.take()
        };
        let _stopping_reset = DetectorStoppingReset {
            lifecycle: &self.detector_lifecycle,
        };
        match handle {
            Some(handle) => handle.shutdown(DETECTOR_TASK_SHUTDOWN_TIMEOUT).await,
            None => true,
        }
    }

    fn detector_task_count(&self) -> usize {
        self.detector_lifecycle
            .lock()
            .task
            .as_ref()
            .map(FaultDetectorTaskHandle::task_count)
            .unwrap_or_default()
    }

    fn detector_schedule_snapshot(&self) -> Vec<ScheduledTaskSnapshot> {
        self.detector_lifecycle
            .lock()
            .task
            .as_ref()
            .map(FaultDetectorTaskHandle::schedule_snapshot)
            .unwrap_or_default()
    }
}

impl<R, S> LatencyFaultTolerance<CheetahString> for LatencyFaultToleranceImpl<R, S>
where
    R: Resolver,
    S: ServiceDetector,
{
    async fn update_fault_item(
        &self,
        name: CheetahString,
        current_latency: u64,
        not_available_duration: u64,
        reachable: bool,
    ) {
        let entry = self
            .fault_item_table
            .entry(name.clone())
            .or_insert_with(|| FaultItem::new(name.clone()));

        entry.set_current_latency(current_latency);
        entry.update_not_available_duration(not_available_duration);
        entry.set_reachable(reachable);

        if !reachable {
            info!("{} is unreachable, it will not be used until it's reachable", name);
        }
    }

    #[inline]
    fn is_available(&self, name: &CheetahString) -> bool {
        self.fault_item_table.get(name).is_none_or(|item| item.is_available())
    }

    #[inline]
    fn is_reachable(&self, name: &CheetahString) -> bool {
        self.fault_item_table.get(name).is_none_or(|item| item.is_reachable())
    }

    async fn remove(&self, name: &CheetahString) {
        self.fault_item_table.remove(name);
    }

    async fn pick_one_at_least(&self) -> Option<CheetahString> {
        use smallvec::SmallVec;

        let mut reachable_names: SmallVec<[CheetahString; 4]> = SmallVec::new();

        for entry in self.fault_item_table.iter() {
            if entry.value().is_reachable() {
                reachable_names.push(entry.key().clone());
            }
        }

        if !reachable_names.is_empty() {
            use rand::seq::SliceRandom;
            reachable_names.shuffle(&mut rand::rng());
            return Some(reachable_names[0].clone());
        }
        None
    }

    fn start_detector(this: Arc<Self>) {
        let mut lifecycle = this.detector_lifecycle.lock();
        if lifecycle.stopping || lifecycle.task.as_ref().is_some_and(|handle| !handle.is_finished()) {
            return;
        }

        if let Some(handle) = spawn_detector_loop(this.clone(), DETECTOR_SCAN_INITIAL_DELAY, DETECTOR_SCAN_INTERVAL) {
            lifecycle.task = Some(handle);
        }
    }

    fn shutdown(&self) {
        let handle = {
            let mut lifecycle = self.detector_lifecycle.lock();
            if lifecycle.stopping {
                return;
            }
            lifecycle.stopping = true;
            lifecycle.task.take()
        };
        let _stopping_reset = DetectorStoppingReset {
            lifecycle: &self.detector_lifecycle,
        };
        if let Some(handle) = handle {
            handle.abort();
        }
    }

    async fn detect_by_one_round(&self) {
        let resolver = self.resolver.read().clone();
        let service_detector = self.service_detector.read().clone();
        let detect_interval = self.detect_interval.load(AtomicOrdering::Acquire);
        let detect_timeout = self.detect_timeout.load(AtomicOrdering::Acquire);
        let (Some(resolver), Some(service_detector)) = (resolver, service_detector) else {
            return;
        };
        let mut remove_set = HashSet::new();

        for entry in self.fault_item_table.iter() {
            let (name, fault_item) = (entry.key(), entry.value());
            if current_millis() as i64 - (fault_item.check_stamp.load(std::sync::atomic::Ordering::Relaxed) as i64) < 0
            {
                continue;
            }
            fault_item.check_stamp.store(
                current_millis() + detect_interval as u64,
                std::sync::atomic::Ordering::Release,
            );

            let broker_addr = match resolver.resolve(fault_item.name.as_ref()).await {
                Some(addr) => addr,
                None => {
                    remove_set.insert(name.clone());
                    continue;
                }
            };

            let service_ok = service_detector
                .detect(broker_addr.as_str(), detect_timeout as u64)
                .await;

            if service_ok && !fault_item.is_reachable() {
                info!("{} is reachable now, then it can be used.", name);
                fault_item.set_reachable(true);
            }
        }

        for name in remove_set {
            self.fault_item_table.remove(&name);
        }
    }

    fn set_detect_timeout(&self, detect_timeout: u32) {
        self.detect_timeout.store(detect_timeout, AtomicOrdering::Release);
    }

    fn set_detect_interval(&self, detect_interval: u32) {
        self.detect_interval.store(detect_interval, AtomicOrdering::Release);
    }

    fn set_start_detector_enable(&self, start_detector_enable: bool) {
        self.start_detector_enable
            .store(start_detector_enable, AtomicOrdering::Release);
    }

    fn is_start_detector_enable(&self) -> bool {
        self.start_detector_enable.load(AtomicOrdering::Acquire)
    }
}

use cheetah_string::CheetahString;
use rocketmq_common::TimeUtils::current_millis;
use std::cmp::Ordering;
use std::hash::Hash;
use tracing::error;
use tracing::info;

fn spawn_detector_loop<R, S>(
    this: Arc<LatencyFaultToleranceImpl<R, S>>,
    initial_delay: Duration,
    scan_interval: Duration,
) -> Option<FaultDetectorTaskHandle>
where
    R: Resolver,
    S: ServiceDetector,
{
    match schedule_client_fixed_delay_task(
        "rocketmq-client-fault-detector",
        initial_delay,
        scan_interval,
        DETECTOR_TASK_SHUTDOWN_TIMEOUT,
        move || {
            let detector = this.clone();
            async move {
                if detector
                    .start_detector_enable
                    .load(std::sync::atomic::Ordering::Relaxed)
                {
                    detector.detect_by_one_round().await;
                }
            }
        },
    ) {
        Ok(handle) => Some(FaultDetectorTaskHandle { handle }),
        Err(error) => {
            error!("Failed to spawn fault tolerance detector task: {}", error);
            None
        }
    }
}

#[doc(hidden)]
pub async fn run_latency_fault_detector_lifecycle_probe() -> LatencyFaultDetectorLifecycleProbe {
    let detector = Arc::new(LatencyFaultToleranceImpl::<ProbeResolver, ProbeServiceDetector>::new());
    detector.set_start_detector_enable(true);
    let handle = spawn_detector_loop(detector.clone(), Duration::ZERO, Duration::from_millis(1))
        .expect("latency fault detector probe task should start");
    detector.detector_lifecycle.lock().task = Some(handle);

    let mut snapshots = detector.detector_schedule_snapshot();
    for _ in 0..100 {
        if snapshots
            .iter()
            .any(|snapshot| snapshot.runs > 0 && snapshot.active_runs == 0)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
        snapshots = detector.detector_schedule_snapshot();
    }

    let scheduled_runs = snapshots.iter().map(|snapshot| snapshot.runs).sum();
    let scheduled_skips = snapshots.iter().map(|snapshot| snapshot.skips).sum();
    let scheduled_overlaps = snapshots.iter().map(|snapshot| snapshot.overlaps).sum();
    let scheduled_failures = snapshots.iter().map(|snapshot| snapshot.failures).sum();
    let task_count_before_shutdown = detector.detector_task_count();
    let shutdown_started_at = std::time::Instant::now();
    let shutdown_healthy = detector.shutdown_detector().await;
    let shutdown_elapsed_us = shutdown_started_at.elapsed().as_micros();
    let task_count_after_shutdown = detector.detector_task_count();
    let healthy = shutdown_healthy
        && scheduled_runs > 0
        && scheduled_overlaps == 0
        && scheduled_failures == 0
        && task_count_before_shutdown > 0
        && task_count_after_shutdown == 0;

    LatencyFaultDetectorLifecycleProbe {
        task_count_before_shutdown,
        task_count_after_shutdown,
        scheduled_runs,
        scheduled_skips,
        scheduled_overlaps,
        scheduled_failures,
        shutdown_elapsed_us,
        healthy,
    }
}

struct ProbeResolver;

impl Resolver for ProbeResolver {
    async fn resolve(&self, _name: &CheetahString) -> Option<CheetahString> {
        None
    }
}

struct ProbeServiceDetector;

impl ServiceDetector for ProbeServiceDetector {
    async fn detect(&self, _endpoint: &str, _timeout_millis: u64) -> bool {
        true
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct FaultItem {
    reachable_flag: std::sync::atomic::AtomicBool,
    _padding1: [u8; 7],
    start_timestamp: std::sync::atomic::AtomicU64,
    current_latency: std::sync::atomic::AtomicU64,

    check_stamp: std::sync::atomic::AtomicU64,
    name: CheetahString,
}

impl FaultItem {
    pub fn new(name: CheetahString) -> Self {
        FaultItem {
            reachable_flag: std::sync::atomic::AtomicBool::new(true),
            _padding1: [0; 7],
            start_timestamp: std::sync::atomic::AtomicU64::new(0),
            current_latency: std::sync::atomic::AtomicU64::new(0),
            check_stamp: std::sync::atomic::AtomicU64::new(0),
            name,
        }
    }

    pub fn update_not_available_duration(&self, not_available_duration: u64) {
        let now = current_millis();
        if not_available_duration > 0
            && now + not_available_duration > self.start_timestamp.load(std::sync::atomic::Ordering::Relaxed)
        {
            self.start_timestamp
                .store(now + not_available_duration, std::sync::atomic::Ordering::Release);
            info!("{} will be isolated for {} ms.", self.name, not_available_duration);
        }
    }

    pub fn set_reachable(&self, reachable_flag: bool) {
        self.reachable_flag
            .store(reachable_flag, std::sync::atomic::Ordering::Release);
    }

    pub fn set_check_stamp(&self, check_stamp: u64) {
        self.check_stamp
            .store(check_stamp, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn is_available(&self) -> bool {
        let now = current_millis();
        now >= self.start_timestamp.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn is_reachable(&self) -> bool {
        self.reachable_flag.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn set_current_latency(&self, latency: u64) {
        self.current_latency
            .store(latency, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn get_current_latency(&self) -> u64 {
        self.current_latency.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn get_start_timestamp(&self) -> u64 {
        self.start_timestamp.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn compare_to(&self, other: &Self) -> i32 {
        match self.cmp(other) {
            Ordering::Less => -1,
            Ordering::Equal => 0,
            Ordering::Greater => 1,
        }
    }
}

impl Eq for FaultItem {}

impl Ord for FaultItem {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.is_available() != other.is_available() {
            if self.is_available() {
                return Ordering::Less;
            }
            if other.is_available() {
                return Ordering::Greater;
            }
        }

        match self
            .current_latency
            .load(std::sync::atomic::Ordering::Relaxed)
            .cmp(&other.current_latency.load(std::sync::atomic::Ordering::Relaxed))
        {
            Ordering::Equal => (),
            ord => return ord,
        }

        match self
            .start_timestamp
            .load(std::sync::atomic::Ordering::Relaxed)
            .cmp(&other.start_timestamp.load(std::sync::atomic::Ordering::Relaxed))
        {
            Ordering::Equal => (),
            ord => return ord,
        }

        Ordering::Equal
    }
}

impl PartialEq<Self> for FaultItem {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.current_latency.load(std::sync::atomic::Ordering::Relaxed)
                == other.current_latency.load(std::sync::atomic::Ordering::Relaxed)
            && self.start_timestamp.load(std::sync::atomic::Ordering::Relaxed)
                == other.start_timestamp.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl PartialOrd for FaultItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Hash for FaultItem {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.current_latency
            .load(std::sync::atomic::Ordering::Relaxed)
            .hash(state);
        self.start_timestamp
            .load(std::sync::atomic::Ordering::Relaxed)
            .hash(state);
    }
}

impl std::fmt::Display for FaultItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "FaultItem{{ name='{}', current_latency={}, start_timestamp={}, reachable_flag={} }}",
            self.name,
            self.get_current_latency(),
            self.get_start_timestamp(),
            self.is_reachable()
        )
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicU64;
    use std::sync::Arc;

    use super::*;

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::Release);
        }
    }

    struct NoopResolver;

    impl Resolver for NoopResolver {
        async fn resolve(&self, _name: &CheetahString) -> Option<CheetahString> {
            None
        }
    }

    struct NoopServiceDetector;

    impl ServiceDetector for NoopServiceDetector {
        async fn detect(&self, _endpoint: &str, _timeout_millis: u64) -> bool {
            true
        }
    }

    struct StaticResolver;

    impl Resolver for StaticResolver {
        async fn resolve(&self, _name: &CheetahString) -> Option<CheetahString> {
            Some(CheetahString::from_static_str("127.0.0.1:10911"))
        }
    }

    struct RecordingServiceDetector {
        timeout_millis: Arc<AtomicU64>,
    }

    impl ServiceDetector for RecordingServiceDetector {
        async fn detect(&self, _endpoint: &str, timeout_millis: u64) -> bool {
            self.timeout_millis
                .store(timeout_millis, std::sync::atomic::Ordering::Release);
            true
        }
    }

    #[test]
    fn fault_item_compare_to_matches_java_comparable_shape() {
        let fast = FaultItem::new(CheetahString::from("fast"));
        let slow = FaultItem::new(CheetahString::from("slow"));
        fast.set_current_latency(10);
        slow.set_current_latency(20);

        assert_eq!(fast.compare_to(&slow), -1);
        assert_eq!(slow.compare_to(&fast), 1);
        assert_eq!(fast.compare_to(&fast), 0);
    }

    #[tokio::test]
    async fn shared_detector_dependency_and_config_snapshots_drive_one_round() {
        let detector = Arc::new(LatencyFaultToleranceImpl::<StaticResolver, RecordingServiceDetector>::new());
        let timeout_millis = Arc::new(AtomicU64::new(0));
        detector.set_resolver(StaticResolver);
        detector.set_service_detector(RecordingServiceDetector {
            timeout_millis: timeout_millis.clone(),
        });
        detector.set_detect_timeout(321);
        detector.set_detect_interval(12_345);
        let broker = CheetahString::from_static_str("broker-a");
        detector.update_fault_item(broker.clone(), 10, 0, false).await;

        detector.detect_by_one_round().await;

        assert!(detector.is_reachable(&broker));
        assert_eq!(timeout_millis.load(std::sync::atomic::Ordering::Acquire), 321);
        assert_eq!(detector.detector_config_for_test(), (12_345, 321));
    }

    #[test]
    fn start_detector_without_tokio_runtime_does_not_spawn_panic() {
        let detector = Arc::new(LatencyFaultToleranceImpl::<NoopResolver, NoopServiceDetector>::new());

        LatencyFaultTolerance::start_detector(detector.clone());
        detector.shutdown();

        std::thread::sleep(tokio::time::Duration::from_millis(20));
    }

    #[tokio::test]
    async fn sync_shutdown_aborts_and_drains_detector_task() {
        let detector = LatencyFaultToleranceImpl::<NoopResolver, NoopServiceDetector>::new();
        let started = Arc::new(AtomicBool::new(false));
        let dropped = Arc::new(AtomicBool::new(false));
        let started_in_task = started.clone();
        let dropped_in_task = dropped.clone();
        let handle = schedule_client_fixed_delay_task(
            "latency-fault-detector-sync-shutdown-test",
            Duration::ZERO,
            Duration::from_secs(60),
            Duration::from_millis(20),
            move || {
                let started = started_in_task.clone();
                let dropped = dropped_in_task.clone();
                async move {
                    let _drop_flag = DropFlag(dropped);
                    started.store(true, std::sync::atomic::Ordering::Release);
                    pending::<()>().await;
                }
            },
        )
        .expect("scheduled detector test task should start");
        detector.detector_lifecycle.lock().task = Some(FaultDetectorTaskHandle { handle });

        tokio::time::timeout(Duration::from_secs(1), async {
            while !started.load(std::sync::atomic::Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("detector task should start before shutdown");

        detector.shutdown();

        assert!(detector.detector_lifecycle.lock().task.is_none());
        tokio::time::timeout(Duration::from_secs(1), async {
            while !dropped.load(std::sync::atomic::Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("sync shutdown should abort the detector task");
    }

    #[tokio::test]
    async fn detector_task_shutdown_waits_for_worker_completion() {
        let completed = Arc::new(AtomicBool::new(false));
        let completed_in_task = completed.clone();
        let handle = FaultDetectorTaskHandle {
            handle: schedule_client_fixed_delay_task(
                "latency-fault-detector-clean-shutdown-test",
                Duration::ZERO,
                Duration::from_secs(60),
                Duration::from_secs(1),
                move || {
                    let completed = completed_in_task.clone();
                    async move {
                        completed.store(true, std::sync::atomic::Ordering::Release);
                    }
                },
            )
            .expect("scheduled detector test task should start"),
        };

        tokio::time::timeout(Duration::from_secs(1), async {
            while !completed.load(std::sync::atomic::Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("detector task should complete one run before shutdown");
        assert!(handle.shutdown(Duration::from_secs(1)).await);
        assert!(completed.load(std::sync::atomic::Ordering::Acquire));
    }

    #[tokio::test]
    async fn detector_task_shutdown_aborts_after_timeout() {
        let dropped = Arc::new(AtomicBool::new(false));
        let started = Arc::new(AtomicBool::new(false));
        let dropped_in_task = dropped.clone();
        let started_in_task = started.clone();
        let handle = FaultDetectorTaskHandle {
            handle: schedule_client_fixed_delay_task(
                "latency-fault-detector-timeout-shutdown-test",
                Duration::ZERO,
                Duration::from_secs(60),
                Duration::from_millis(20),
                move || {
                    let dropped = dropped_in_task.clone();
                    let started = started_in_task.clone();
                    async move {
                        let _drop_flag = DropFlag(dropped);
                        started.store(true, std::sync::atomic::Ordering::Release);
                        pending::<()>().await;
                    }
                },
            )
            .expect("scheduled detector test task should start"),
        };

        tokio::time::timeout(Duration::from_secs(1), async {
            while !started.load(std::sync::atomic::Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("detector task should start before timeout shutdown");
        assert!(!handle.shutdown(Duration::from_millis(20)).await);
        assert!(dropped.load(std::sync::atomic::Ordering::Acquire));
    }

    #[tokio::test]
    async fn shutdown_detector_waits_for_started_detector_task() {
        let detector = Arc::new(LatencyFaultToleranceImpl::<NoopResolver, NoopServiceDetector>::new());

        LatencyFaultTolerance::start_detector(detector.clone());

        assert!(detector.detector_lifecycle.lock().task.is_some());
        assert!(detector.shutdown_detector().await);
        assert!(detector.detector_lifecycle.lock().task.is_none());
    }

    #[tokio::test]
    async fn concurrent_detector_start_publishes_one_owned_task() {
        let detector = Arc::new(LatencyFaultToleranceImpl::<NoopResolver, NoopServiceDetector>::new());
        let starts = (0..8)
            .map(|_| {
                let detector = detector.clone();
                tokio::spawn(async move {
                    LatencyFaultTolerance::start_detector(detector);
                })
            })
            .collect::<Vec<_>>();

        for start in starts {
            start.await.expect("detector start task should finish");
        }

        assert_eq!(detector.detector_task_count(), 1);
        assert!(detector.shutdown_detector().await);
        assert_eq!(detector.detector_task_count(), 0);
    }

    #[tokio::test]
    async fn latency_fault_detector_lifecycle_probe_reports_clean_shutdown() {
        let probe = run_latency_fault_detector_lifecycle_probe().await;

        assert!(probe.healthy, "{probe:?}");
        assert_eq!(probe.task_count_after_shutdown, 0, "{probe:?}");
        assert_eq!(probe.scheduled_overlaps, 0, "{probe:?}");
        assert_eq!(probe.scheduled_failures, 0, "{probe:?}");
    }
}
