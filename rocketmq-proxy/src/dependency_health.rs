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
use std::time::Duration;

use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_runtime::{
    ChildServiceContext, DependencyReadiness, ServiceLifecycle, ShutdownDeadline, ShutdownReport, TaskGroup,
};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio::time::Instant;

use crate::error::{canonical, ProxyError, ProxyResult};
use crate::service::MetadataService;
use rocketmq_proxy_core::ProxyMode;

/// Restart-required policy for read-only backend health checks.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct DependencyHealthConfig {
    pub interval_ms: u64,
    pub timeout_ms: u64,
    pub stale_after_ms: u64,
    pub failure_threshold: u32,
    pub recovery_threshold: u32,
    pub jitter_percent: u8,
}

impl Default for DependencyHealthConfig {
    fn default() -> Self {
        Self {
            interval_ms: 5_000,
            timeout_ms: 3_000,
            stale_after_ms: 15_000,
            failure_threshold: 3,
            recovery_threshold: 2,
            jitter_percent: 10,
        }
    }
}

impl DependencyHealthConfig {
    pub(crate) fn validate(&self) -> ProxyResult<()> {
        let maximum_interval = self.interval_ms.checked_add(self.interval_ms / 2);
        if self.interval_ms == 0
            || self.timeout_ms == 0
            || self.failure_threshold == 0
            || self.recovery_threshold == 0
            || self.jitter_percent > 50
            || self.stale_after_ms < self.interval_ms
            || self.stale_after_ms < self.timeout_ms
            || maximum_interval
                .and_then(|millis| Instant::now().checked_add(Duration::from_millis(millis)))
                .is_none()
            || Instant::now()
                .checked_add(Duration::from_millis(self.stale_after_ms))
                .is_none()
        {
            return Err(ProxyError::from(canonical::configuration_invalid(
                "dependencyHealth",
                "invalid probe timing or thresholds",
            )));
        }
        Ok(())
    }

    fn interval(&self, sample: u64) -> Duration {
        let spread = (u128::from(self.interval_ms) * u128::from(self.jitter_percent) / 100) as u64;
        let adjustment = if spread == 0 { 0 } else { sample % (spread * 2 + 1) };
        Duration::from_millis(self.interval_ms - spread + adjustment)
    }
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum DependencyHealthState {
    Starting,
    Ready,
    Degraded,
    Stopped,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum DependencyHealthReason {
    NotChecked,
    MetadataUnavailable,
    TimedOut,
    Stale,
    Recovering,
    Shutdown,
}

/// Safe, low-cardinality health evidence. Timestamps are Unix milliseconds.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DependencyHealthSnapshot {
    /// Local checks Broker lifecycle and Store writes; Cluster checks fresh routes and Broker metadata.
    pub mode: ProxyMode,
    pub checked_at: Option<u64>,
    pub last_success_at: Option<u64>,
    pub consecutive_failures: u32,
    pub consecutive_successes: u32,
    pub reason: Option<DependencyHealthReason>,
    pub state: DependencyHealthState,
}

impl Default for DependencyHealthSnapshot {
    fn default() -> Self {
        Self {
            mode: ProxyMode::default(),
            checked_at: None,
            last_success_at: None,
            consecutive_failures: 0,
            consecutive_successes: 0,
            reason: Some(DependencyHealthReason::NotChecked),
            state: DependencyHealthState::Starting,
        }
    }
}

/// An observation handle that does not own or extend the monitor task lifetime.
#[derive(Clone)]
pub struct DependencyHealth {
    receiver: watch::Receiver<DependencyHealthSnapshot>,
}

impl DependencyHealth {
    pub fn snapshot(&self) -> DependencyHealthSnapshot {
        self.receiver.borrow().clone()
    }
}

struct HealthState {
    snapshot: DependencyHealthSnapshot,
    last_success: Option<Instant>,
}

impl HealthState {
    fn new(mode: ProxyMode) -> Self {
        Self {
            snapshot: DependencyHealthSnapshot {
                mode,
                ..Default::default()
            },
            last_success: None,
        }
    }

    fn record(&mut self, outcome: Result<(), DependencyHealthReason>, now: Instant, config: &DependencyHealthConfig) {
        let checked_at = current_millis();
        self.snapshot.checked_at = Some(checked_at);
        match outcome {
            Ok(()) => {
                self.last_success = Some(now);
                self.snapshot.last_success_at = Some(checked_at);
                self.snapshot.consecutive_failures = 0;
                self.snapshot.consecutive_successes = self.snapshot.consecutive_successes.saturating_add(1);
                if self.snapshot.state == DependencyHealthState::Starting
                    || self.snapshot.consecutive_successes >= config.recovery_threshold
                {
                    self.snapshot.state = DependencyHealthState::Ready;
                }
                self.snapshot.reason = if self.snapshot.state == DependencyHealthState::Degraded {
                    Some(DependencyHealthReason::Recovering)
                } else {
                    None
                };
            }
            Err(reason) => {
                self.snapshot.consecutive_successes = 0;
                self.snapshot.consecutive_failures = self.snapshot.consecutive_failures.saturating_add(1);
                self.snapshot.reason = Some(reason);
                if self.snapshot.state == DependencyHealthState::Starting
                    || self.snapshot.consecutive_failures >= config.failure_threshold
                {
                    self.snapshot.state = DependencyHealthState::Degraded;
                }
            }
        }
        self.expire(now, config);
    }

    fn expiry(&self, config: &DependencyHealthConfig) -> Option<Instant> {
        self.last_success
            .map(|last| last + Duration::from_millis(config.stale_after_ms))
    }

    fn expire(&mut self, now: Instant, config: &DependencyHealthConfig) {
        if self.expiry(config).is_some_and(|expiry| now >= expiry) {
            self.snapshot.state = DependencyHealthState::Degraded;
            self.snapshot.reason = Some(DependencyHealthReason::Stale);
            self.snapshot.consecutive_successes = 0;
        }
    }
}

pub(crate) struct DependencyHealthMonitor {
    config: DependencyHealthConfig,
    sender: watch::Sender<DependencyHealthSnapshot>,
    tasks: Option<TaskGroup>,
    lifecycle: Option<ServiceLifecycle>,
}

impl DependencyHealthMonitor {
    pub(crate) fn new(config: DependencyHealthConfig, mode: ProxyMode) -> Self {
        let (sender, _) = watch::channel(DependencyHealthSnapshot {
            mode,
            ..Default::default()
        });
        Self {
            config,
            sender,
            tasks: None,
            lifecycle: None,
        }
    }

    pub(crate) fn handle(&self) -> DependencyHealth {
        DependencyHealth {
            receiver: self.sender.subscribe(),
        }
    }

    pub(crate) async fn start(
        &mut self,
        source: Arc<dyn MetadataService>,
        context: &ChildServiceContext,
        lifecycle: Option<ServiceLifecycle>,
    ) -> ProxyResult<()> {
        self.config.validate()?;
        self.lifecycle = lifecycle;
        let starting = self.sender.borrow().clone();
        publish(&self.sender, self.lifecycle.as_ref(), &starting);
        let initial =
            tokio::time::timeout(Duration::from_millis(self.config.timeout_ms), source.readiness_check()).await;
        let mut state = HealthState::new(self.sender.borrow().mode);
        let failure_reason = if initial.is_err() {
            DependencyHealthReason::TimedOut
        } else {
            DependencyHealthReason::MetadataUnavailable
        };
        let initial = match initial {
            Ok(result) => result,
            Err(_) => Err(ProxyError::Transport {
                message: "Proxy backend readiness check timed out".into(),
            }),
        };
        state.record(
            initial.as_ref().map(|_| ()).map_err(|_| failure_reason),
            Instant::now(),
            &self.config,
        );
        publish(&self.sender, self.lifecycle.as_ref(), &state.snapshot);
        initial?;
        let tasks = context.component("dependency-health").task_group().clone();
        self.tasks = Some(tasks.clone());
        let cancellation = tasks.cancellation_token();
        let config = self.config.clone();
        let sender = self.sender.clone();
        let lifecycle = self.lifecycle.clone();
        tasks
            .spawn_service("proxy.dependency-health", async move {
                let mut next_probe = Instant::now() + config.interval(uuid::Uuid::new_v4().as_u128() as u64);
                loop {
                    let wake = if state.snapshot.state == DependencyHealthState::Ready {
                        state
                            .expiry(&config)
                            .map_or(next_probe, |expiry| expiry.min(next_probe))
                    } else {
                        next_probe
                    };
                    tokio::select! {
                        biased;
                        _ = cancellation.cancelled() => break,
                        _ = tokio::time::sleep_until(wake) => {}
                    }
                    let now = Instant::now();
                    state.expire(now, &config);
                    publish(&sender, lifecycle.as_ref(), &state.snapshot);
                    if now < next_probe {
                        continue;
                    }
                    let mut deadline = now + Duration::from_millis(config.timeout_ms);
                    if state.snapshot.state == DependencyHealthState::Ready {
                        if let Some(expiry) = state.expiry(&config) {
                            deadline = deadline.min(expiry);
                        }
                    }
                    let result = tokio::select! {
                        biased;
                        _ = cancellation.cancelled() => break,
                        result = tokio::time::timeout_at(deadline, source.readiness_check()) => match result {
                            Ok(Ok(())) => Ok(()),
                            Ok(Err(_)) => Err(DependencyHealthReason::MetadataUnavailable),
                            Err(_) => Err(DependencyHealthReason::TimedOut),
                        }
                    };
                    state.record(result, Instant::now(), &config);
                    publish(&sender, lifecycle.as_ref(), &state.snapshot);
                    let interval = config.interval(uuid::Uuid::new_v4().as_u128() as u64);
                    let candidate = now + interval;
                    next_probe = if candidate <= Instant::now() {
                        Instant::now() + interval
                    } else {
                        candidate
                    };
                }
            })
            .map_err(|error| ProxyError::from(canonical::transport_unavailable_with_source(error)))?;
        Ok(())
    }

    pub(crate) async fn shutdown_until(&mut self, deadline: ShutdownDeadline) -> Option<ShutdownReport> {
        if let Some(tasks) = &self.tasks {
            tasks.cancellation_token().cancel();
        }
        let mut snapshot = self.sender.borrow().clone();
        snapshot.state = DependencyHealthState::Stopped;
        snapshot.reason = Some(DependencyHealthReason::Shutdown);
        publish(&self.sender, self.lifecycle.as_ref(), &snapshot);
        let report = match &self.tasks {
            Some(tasks) => Some(tasks.shutdown_until(deadline).await),
            None => None,
        };
        publish(&self.sender, self.lifecycle.as_ref(), &snapshot);
        report
    }
}

fn publish(
    sender: &watch::Sender<DependencyHealthSnapshot>,
    lifecycle: Option<&ServiceLifecycle>,
    snapshot: &DependencyHealthSnapshot,
) {
    if let Some(lifecycle) = lifecycle {
        lifecycle.set_dependency_readiness(if snapshot.state == DependencyHealthState::Ready {
            DependencyReadiness::Ready
        } else {
            DependencyReadiness::Degraded
        });
    }
    sender.send_replace(snapshot.clone());
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_proxy_core::{
        ProxyContext, ProxyServiceFuture, ProxyTopicMessageType, ResourceIdentity, SubscriptionGroupMetadata,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn health_hysteresis_tolerates_a_blip_and_requires_two_successes_after_degradation() {
        let config = DependencyHealthConfig::default();
        let now = Instant::now();
        let mut state = HealthState::new(ProxyMode::Cluster);
        state.record(Ok(()), now, &config);
        state.record(Err(DependencyHealthReason::MetadataUnavailable), now, &config);
        assert_eq!(state.snapshot.state, DependencyHealthState::Ready);
        state.record(Ok(()), now, &config);
        assert_eq!(state.snapshot.consecutive_failures, 0);
        for _ in 0..3 {
            state.record(Err(DependencyHealthReason::MetadataUnavailable), now, &config);
        }
        assert_eq!(state.snapshot.state, DependencyHealthState::Degraded);
        state.record(Ok(()), now, &config);
        assert_eq!(state.snapshot.state, DependencyHealthState::Degraded);
        assert_eq!(state.snapshot.reason, Some(DependencyHealthReason::Recovering));
        state.record(Ok(()), now, &config);
        assert_eq!(state.snapshot.state, DependencyHealthState::Ready);
        state.expire(now + Duration::from_millis(config.stale_after_ms), &config);
        assert_eq!(state.snapshot.state, DependencyHealthState::Degraded);
        assert_eq!(state.snapshot.reason, Some(DependencyHealthReason::Stale));
    }

    #[test]
    fn invalid_health_policy_fails_before_scheduling_and_jitter_stays_bounded() {
        let config = DependencyHealthConfig::default();
        config.validate().unwrap();
        for sample in [0, 1, 999, u64::MAX] {
            assert!((Duration::from_millis(4_500)..=Duration::from_millis(5_500)).contains(&config.interval(sample)));
        }
        for invalid in [
            DependencyHealthConfig {
                interval_ms: 0,
                ..config.clone()
            },
            DependencyHealthConfig {
                timeout_ms: 20_000,
                ..config.clone()
            },
            DependencyHealthConfig {
                jitter_percent: 51,
                ..config.clone()
            },
            DependencyHealthConfig {
                failure_threshold: 0,
                ..config.clone()
            },
        ] {
            assert!(invalid.validate().is_err());
        }
    }

    struct Probe {
        calls: AtomicUsize,
        active: AtomicUsize,
        entered: tokio::sync::Notify,
        responses: tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<ProxyResult<()>>>,
    }

    struct ActiveProbe<'a>(&'a AtomicUsize);
    impl Drop for ActiveProbe<'_> {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    impl MetadataService for Probe {
        fn readiness_check(&self) -> ProxyServiceFuture<'_, ()> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                self.active.fetch_add(1, Ordering::SeqCst);
                let _active = ActiveProbe(&self.active);
                self.entered.notify_one();
                self.responses.lock().await.recv().await.unwrap()
            })
        }
        fn topic_message_type<'a>(
            &'a self,
            _: &'a ProxyContext,
            _: &'a ResourceIdentity,
        ) -> ProxyServiceFuture<'a, ProxyTopicMessageType> {
            panic!("health monitor must use only the backend readiness operation")
        }
        fn subscription_group<'a>(
            &'a self,
            _: &'a ProxyContext,
            _: &'a ResourceIdentity,
            _: &'a ResourceIdentity,
        ) -> ProxyServiceFuture<'a, Option<SubscriptionGroupMetadata>> {
            panic!("health monitor must not query user resources")
        }
    }

    fn probe() -> (Arc<Probe>, tokio::sync::mpsc::UnboundedSender<ProxyResult<()>>) {
        let (sender, responses) = tokio::sync::mpsc::unbounded_channel();
        (
            Arc::new(Probe {
                calls: AtomicUsize::new(0),
                active: AtomicUsize::new(0),
                entered: tokio::sync::Notify::new(),
                responses: tokio::sync::Mutex::new(responses),
            }),
            sender,
        )
    }

    #[tokio::test(start_paused = true)]
    async fn initial_readiness_failure_and_timeout_publish_no_ready_generation() {
        for timed_out in [false, true] {
            let runtime = rocketmq_runtime::RuntimeContext::from_current("health-startup-test");
            let context = runtime.service_context("proxy");
            let (source, responses) = probe();
            if !timed_out {
                responses
                    .send(Err(ProxyError::not_implemented("test metadata failure")))
                    .unwrap();
            }
            let mut monitor = DependencyHealthMonitor::new(Default::default(), ProxyMode::Local);
            let handle = monitor.handle();
            let error = monitor.start(source.clone(), &context, None).await.unwrap_err();
            if !timed_out {
                assert!(matches!(
                    error,
                    ProxyError::NotImplemented {
                        feature: "test metadata failure"
                    }
                ));
            }
            assert_eq!(source.active.load(Ordering::SeqCst), 0);
            assert_eq!(handle.snapshot().state, DependencyHealthState::Degraded);
            assert_eq!(
                handle.snapshot().reason,
                Some(if timed_out {
                    DependencyHealthReason::TimedOut
                } else {
                    DependencyHealthReason::MetadataUnavailable
                })
            );
            assert_eq!(context.task_group().component_count(), 0);
            assert!(monitor
                .shutdown_until(ShutdownDeadline::after(Duration::from_secs(1)))
                .await
                .is_none());
            assert_eq!(handle.snapshot().state, DependencyHealthState::Stopped);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_cancels_and_awaits_an_inflight_probe() {
        let runtime = rocketmq_runtime::RuntimeContext::from_current("health-owner-test");
        let context = runtime.service_context("proxy");
        let (source, responses) = probe();
        responses.send(Ok(())).unwrap();
        let mut monitor = DependencyHealthMonitor::new(
            DependencyHealthConfig {
                jitter_percent: 0,
                ..Default::default()
            },
            ProxyMode::Cluster,
        );
        let handle = monitor.handle();
        monitor.start(source.clone(), &context, None).await.unwrap();
        source.entered.notified().await;
        tokio::time::advance(Duration::from_secs(5)).await;
        source.entered.notified().await;
        assert_eq!(source.active.load(Ordering::SeqCst), 1);
        let report = monitor
            .shutdown_until(ShutdownDeadline::after(Duration::from_secs(1)))
            .await
            .unwrap();
        assert!(report.is_healthy(), "{}", report.to_json());
        assert_eq!(source.active.load(Ordering::SeqCst), 0);
        assert_eq!(handle.snapshot().state, DependencyHealthState::Stopped);
        tokio::time::advance(Duration::from_secs(60)).await;
        assert_eq!(source.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn stale_evidence_degrades_while_a_probe_is_waiting() {
        let runtime = rocketmq_runtime::RuntimeContext::from_current("health-stale-test");
        let context = runtime.service_context("proxy");
        let (source, responses) = probe();
        responses.send(Ok(())).unwrap();
        let mut monitor = DependencyHealthMonitor::new(
            DependencyHealthConfig {
                jitter_percent: 0,
                timeout_ms: 10_000,
                failure_threshold: 100,
                ..Default::default()
            },
            ProxyMode::Cluster,
        );
        let mut handle = monitor.handle();
        monitor.start(source.clone(), &context, None).await.unwrap();
        source.entered.notified().await;
        tokio::time::advance(Duration::from_secs(5)).await;
        source.entered.notified().await;
        tokio::time::advance(Duration::from_secs(10)).await;
        while handle.snapshot().state != DependencyHealthState::Degraded {
            handle.receiver.changed().await.unwrap();
        }
        assert_eq!(handle.snapshot().reason, Some(DependencyHealthReason::Stale));
        assert_eq!(handle.snapshot().consecutive_failures, 1);
        assert!(monitor
            .shutdown_until(ShutdownDeadline::after(Duration::from_secs(1)))
            .await
            .unwrap()
            .is_healthy());
    }
}
