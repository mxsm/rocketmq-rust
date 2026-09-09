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

use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use rocketmq_runtime::ShutdownReport;
use rocketmq_transport::api::ClientShutdownReport;
use serde::Serialize;
use tokio::sync::Notify;

/// Runtime lifecycle states for NameServer.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RuntimeState {
    Created = 0,
    Initialized = 1,
    Running = 2,
    ShuttingDown = 3,
    Stopped = 4,
}

impl RuntimeState {
    #[inline]
    pub(super) fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Created),
            1 => Some(Self::Initialized),
            2 => Some(Self::Running),
            3 => Some(Self::ShuttingDown),
            4 => Some(Self::Stopped),
            _ => None,
        }
    }

    #[inline]
    pub(super) fn name(&self) -> &'static str {
        match self {
            Self::Created => "Created",
            Self::Initialized => "Initialized",
            Self::Running => "Running",
            Self::ShuttingDown => "ShuttingDown",
            Self::Stopped => "Stopped",
        }
    }

    #[inline]
    pub(super) fn can_transition_to(&self, next: RuntimeState) -> bool {
        matches!(
            (self, next),
            (Self::Created, Self::Initialized)
                | (Self::Created, Self::ShuttingDown)
                | (Self::Created, Self::Stopped)
                | (Self::Initialized, Self::Running)
                | (Self::Initialized, Self::ShuttingDown)
                | (Self::Initialized, Self::Stopped)
                | (Self::Running, Self::ShuttingDown)
                | (Self::ShuttingDown, Self::Stopped)
        )
    }
}

impl std::fmt::Display for RuntimeState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[doc(hidden)]
#[derive(Debug, Clone, Default, Serialize)]
pub struct NameServerInFlightDrainReport {
    pub elapsed_ms: u64,
    pub timeout_ms: u64,
    pub completed: u64,
    pub remaining: usize,
    pub timed_out: bool,
}

impl NameServerInFlightDrainReport {
    #[doc(hidden)]
    pub fn is_healthy(&self) -> bool {
        !self.timed_out && self.remaining == 0
    }
}

#[doc(hidden)]
#[derive(Debug, Clone, Default, Serialize)]
pub struct NameServerShutdownReport {
    pub elapsed_ms: u64,
    pub deadline_expired: bool,
    pub shutdown_relay: Option<ShutdownReport>,
    pub in_flight: NameServerInFlightDrainReport,
    pub scheduled: Option<ShutdownReport>,
    pub embedded_controller_healthy: Option<bool>,
    pub route_unregistration: Option<ShutdownReport>,
    pub cluster_test_route_lookup_healthy: Option<bool>,
    pub server: Option<ShutdownReport>,
    pub remoting_server: Option<ShutdownReport>,
    pub remoting_client: Option<ClientShutdownReport>,
    pub auth_runtime_healthy: Option<bool>,
    pub metadata_io_healthy: Option<bool>,
    pub root: Option<ShutdownReport>,
}

impl NameServerShutdownReport {
    #[doc(hidden)]
    pub fn is_healthy(&self) -> bool {
        !self.deadline_expired
            && self.shutdown_relay.as_ref().is_none_or(ShutdownReport::is_healthy)
            && self.in_flight.is_healthy()
            && self.scheduled.as_ref().is_none_or(ShutdownReport::is_healthy)
            && self.embedded_controller_healthy.unwrap_or(true)
            && self
                .route_unregistration
                .as_ref()
                .is_none_or(ShutdownReport::is_healthy)
            && self.cluster_test_route_lookup_healthy.unwrap_or(true)
            && self.server.as_ref().is_none_or(ShutdownReport::is_healthy)
            && self.remoting_server.as_ref().is_none_or(ShutdownReport::is_healthy)
            && self
                .remoting_client
                .as_ref()
                .is_none_or(ClientShutdownReport::is_healthy)
            && self.auth_runtime_healthy.unwrap_or(true)
            && self.metadata_io_healthy.unwrap_or(true)
            && self.root.as_ref().is_none_or(ShutdownReport::is_healthy)
    }
}

#[derive(Debug, Default)]
pub(crate) struct InFlightRequestTracker {
    active: AtomicUsize,
    completed: AtomicU64,
    notify: Notify,
}

impl InFlightRequestTracker {
    pub(crate) fn enter(self: &Arc<Self>) -> InFlightRequestGuard {
        self.active.fetch_add(1, Ordering::AcqRel);
        InFlightRequestGuard {
            tracker: Arc::clone(self),
        }
    }

    pub(super) async fn drain(&self, timeout: Duration) -> NameServerInFlightDrainReport {
        let started_at = Instant::now();
        let timed_out = if self.active.load(Ordering::Acquire) == 0 {
            false
        } else {
            tokio::time::timeout(timeout, async {
                loop {
                    let notified = self.notify.notified();
                    if self.active.load(Ordering::Acquire) == 0 {
                        break;
                    }
                    notified.await;
                }
            })
            .await
            .is_err()
        };

        NameServerInFlightDrainReport {
            elapsed_ms: started_at.elapsed().as_millis() as u64,
            timeout_ms: timeout.as_millis() as u64,
            completed: self.completed.load(Ordering::Acquire),
            remaining: self.active.load(Ordering::Acquire),
            timed_out,
        }
    }
}

#[derive(Debug)]
pub(crate) struct InFlightRequestGuard {
    tracker: Arc<InFlightRequestTracker>,
}

impl Drop for InFlightRequestGuard {
    fn drop(&mut self) {
        let previous = self.tracker.active.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "in-flight request counter underflow");
        self.tracker.completed.fetch_add(1, Ordering::AcqRel);
        self.tracker.notify.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_states_have_stable_encodings_and_names() {
        for (value, state, name) in [
            (0, RuntimeState::Created, "Created"),
            (1, RuntimeState::Initialized, "Initialized"),
            (2, RuntimeState::Running, "Running"),
            (3, RuntimeState::ShuttingDown, "ShuttingDown"),
            (4, RuntimeState::Stopped, "Stopped"),
        ] {
            assert_eq!(RuntimeState::from_u8(value), Some(state));
            assert_eq!(state.name(), name);
            assert_eq!(state.to_string(), name);
        }
        for value in 5..=u8::MAX {
            assert_eq!(RuntimeState::from_u8(value), None);
        }
    }

    #[test]
    fn lifecycle_transitions_allow_startup_and_shutdown_but_never_restart() {
        use RuntimeState::*;
        let states = [Created, Initialized, Running, ShuttingDown, Stopped];
        let allowed = [
            [false, true, false, true, true],
            [false, false, true, true, true],
            [false, false, false, true, false],
            [false, false, false, false, true],
            [false, false, false, false, false],
        ];
        for (row, source) in states.iter().enumerate() {
            for (column, target) in states.iter().enumerate() {
                assert_eq!(
                    source.can_transition_to(*target),
                    allowed[row][column],
                    "{source} -> {target}"
                );
            }
        }
    }

    #[tokio::test]
    async fn dropping_the_last_request_wakes_drain_and_counts_completions() {
        let tracker = Arc::new(InFlightRequestTracker::default());
        assert_eq!(tracker.active.load(Ordering::Acquire), 0);
        let first = tracker.enter();
        let second = tracker.enter();
        assert_eq!(tracker.active.load(Ordering::Acquire), 2);
        let mut drain = std::pin::pin!(tracker.drain(Duration::from_secs(10)));
        assert!(futures::poll!(&mut drain).is_pending());

        drop(first);
        assert_eq!(tracker.active.load(Ordering::Acquire), 1);
        assert_eq!(tracker.completed.load(Ordering::Acquire), 1);
        assert!(futures::poll!(&mut drain).is_pending());
        drop(second);
        let std::task::Poll::Ready(report) = futures::poll!(&mut drain) else {
            panic!("the last guard must wake the pending drain");
        };
        assert!(report.is_healthy());
        assert_eq!(report.completed, 2);
        assert_eq!(tracker.active.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn idle_drain_is_immediate_and_preserves_completed_count() {
        let tracker = Arc::new(InFlightRequestTracker::default());
        drop(tracker.enter());
        let std::task::Poll::Ready(report) = futures::poll!(std::pin::pin!(tracker.drain(Duration::ZERO))) else {
            panic!("idle drain must not wait");
        };
        assert!(report.is_healthy());
        assert_eq!(report.completed, 1);
    }

    #[tokio::test]
    async fn drain_timeout_reports_the_remaining_request() {
        let tracker = Arc::new(InFlightRequestTracker::default());
        let guard = tracker.enter();
        let report = tracker.drain(Duration::ZERO).await;
        assert!(report.timed_out);
        assert_eq!(report.remaining, 1);
        assert_eq!(report.completed, 0);
        assert_eq!(report.timeout_ms, 0);
        drop(guard);
        assert!(tracker.drain(Duration::ZERO).await.is_healthy());
    }

    #[test]
    fn drain_health_requires_no_timeout_and_no_remaining_requests() {
        for (timed_out, remaining, healthy) in [(false, 0, true), (true, 0, false), (false, 1, false), (true, 1, false)]
        {
            let report = NameServerInFlightDrainReport {
                timed_out,
                remaining,
                ..Default::default()
            };
            assert_eq!(report.is_healthy(), healthy);
        }
    }

    #[test]
    fn shutdown_health_includes_deadline_drain_and_component_results() {
        assert!(NameServerShutdownReport::default().is_healthy());
        assert!(!NameServerShutdownReport {
            deadline_expired: true,
            ..Default::default()
        }
        .is_healthy());
        assert!(!NameServerShutdownReport {
            in_flight: NameServerInFlightDrainReport {
                remaining: 1,
                ..Default::default()
            },
            ..Default::default()
        }
        .is_healthy());

        for healthy in [true, false] {
            let mut nested = ShutdownReport::new("root", Duration::ZERO);
            nested.failed = usize::from(!healthy);
            for report in [
                NameServerShutdownReport {
                    root: Some(nested),
                    ..Default::default()
                },
                NameServerShutdownReport {
                    auth_runtime_healthy: Some(healthy),
                    ..Default::default()
                },
            ] {
                assert_eq!(report.is_healthy(), healthy);
            }
        }
    }
}
