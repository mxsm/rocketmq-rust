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
