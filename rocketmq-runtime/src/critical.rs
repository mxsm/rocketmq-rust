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

//! Durable failure records for tasks a component registered as critical.
//!
//! The record is the authority. It is written when the failure is observed and
//! stays until a handler takes it, so a full notification channel or a subscriber
//! that stopped reading cannot lose the fact that a critical task failed. The
//! monitor that handles records runs under an owner outside the group it
//! monitors, because a poisoned group can neither spawn nor run its own monitor.

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use parking_lot::Mutex;
use tokio::sync::mpsc;
use tokio::sync::Notify;

use crate::error::RuntimeResult;
use crate::task_group::TaskId;
use crate::task_group::TaskKind;
use crate::task_spawner::TaskSpawner;
use crate::RuntimeContractPolicy;
use crate::RuntimeContractViolation;

/// How a critical task stopped being healthy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CriticalFailureKind {
    /// The task panicked.
    Panicked,
    /// A service returned before its owner started shutting down.
    ExitedUnexpectedly,
    /// The task reported an unrecoverable error through the state's record API.
    Unrecoverable,
}

impl CriticalFailureKind {
    /// Returns the stable identifier used in bounded log fields.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Panicked => "panicked",
            Self::ExitedUnexpectedly => "exited-unexpectedly",
            Self::Unrecoverable => "unrecoverable",
        }
    }
}

/// A durable record of one critical task failure.
///
/// Deliberately bounded: it carries the failure kind, the task category, and a
/// sequence, and never the task's name, group name, or arguments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CriticalFailure {
    kind: CriticalFailureKind,
    task_kind: TaskKind,
    sequence: u64,
    observed_at: Instant,
}

impl CriticalFailure {
    /// Returns the bounded failure kind.
    #[must_use]
    pub const fn kind(self) -> CriticalFailureKind {
        self.kind
    }

    /// Returns the bounded category of the failed task.
    #[must_use]
    pub const fn task_kind(self) -> TaskKind {
        self.task_kind
    }

    /// Returns the process-lifetime sequence of this failure.
    #[must_use]
    pub const fn sequence(self) -> u64 {
        self.sequence
    }

    /// Returns the instant at which the failure was observed.
    #[must_use]
    pub const fn observed_at(self) -> Instant {
        self.observed_at
    }
}

/// The registration data a critical spawn carries into task execution.
#[derive(Debug, Clone)]
pub(crate) struct CriticalRegistration {
    pub(crate) failures: CriticalFailureState,
    pub(crate) task_kind: TaskKind,
    /// Whether returning before owner cancellation is itself a failure.
    pub(crate) expects_until_cancelled: bool,
}

#[derive(Debug)]
struct CriticalFailureStateInner {
    pending: Mutex<Option<CriticalFailure>>,
    subscribers: Mutex<Vec<mpsc::Sender<CriticalFailure>>>,
    sequence: AtomicU64,
    occurrences: AtomicU64,
    changed: Notify,
}

/// The owner-scoped authority for critical task failures.
///
/// One state is shared by the critical tasks of a component and by the monitor
/// that handles them. Clones share the same record.
#[derive(Debug, Clone)]
pub struct CriticalFailureState {
    inner: Arc<CriticalFailureStateInner>,
}

impl Default for CriticalFailureState {
    fn default() -> Self {
        Self::new()
    }
}

impl CriticalFailureState {
    /// Creates a state with no recorded failure.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(CriticalFailureStateInner {
                pending: Mutex::new(None),
                subscribers: Mutex::new(Vec::new()),
                sequence: AtomicU64::new(1),
                occurrences: AtomicU64::new(0),
                changed: Notify::new(),
            }),
        }
    }

    /// Records one failure and returns it.
    ///
    /// The first unhandled failure stays pending, because it is the evidence a
    /// handler takes with [`Self::handle`]. Later failures still advance the
    /// sequence and the occurrence count, so a handler can tell that more than
    /// one occurred, and they never replace the pending record.
    pub fn record(&self, kind: CriticalFailureKind, task_kind: TaskKind) -> CriticalFailure {
        let failure = CriticalFailure {
            kind,
            task_kind,
            sequence: self.inner.sequence.fetch_add(1, Ordering::AcqRel),
            observed_at: Instant::now(),
        };
        self.inner.occurrences.fetch_add(1, Ordering::AcqRel);
        {
            let mut pending = self.inner.pending.lock();
            if pending.is_none() {
                *pending = Some(failure);
            }
        }
        self.inner.changed.notify_waiters();
        self.notify_subscribers(failure);
        failure
    }

    fn notify_subscribers(&self, failure: CriticalFailure) {
        let mut subscribers = self.inner.subscribers.lock();
        // A full channel drops this notification and keeps the subscriber: the
        // pending record is what a handler must rely on.
        subscribers
            .retain(|subscriber| !matches!(subscriber.try_send(failure), Err(mpsc::error::TrySendError::Closed(_))));
    }

    /// Returns the pending failure without handling it.
    #[must_use]
    pub fn pending(&self) -> Option<CriticalFailure> {
        *self.inner.pending.lock()
    }

    /// Takes the pending failure, marking it handled.
    #[must_use]
    pub fn handle(&self) -> Option<CriticalFailure> {
        self.inner.pending.lock().take()
    }

    /// Returns how many failures were recorded in this process.
    #[must_use]
    pub fn occurrences(&self) -> u64 {
        self.inner.occurrences.load(Ordering::Acquire)
    }

    /// Waits until a failure is pending and returns it without handling it.
    pub async fn wait(&self) -> CriticalFailure {
        loop {
            let changed = self.inner.changed.notified();
            if let Some(failure) = self.pending() {
                return failure;
            }
            changed.await;
        }
    }

    /// Subscribes to bounded failure notifications.
    ///
    /// The subscription is a signal, not the record: when the channel is full the
    /// notification is dropped and [`Self::pending`] still reports the failure.
    ///
    /// # Errors
    ///
    /// Returns a contract violation when `capacity` is zero.
    pub fn subscribe(&self, capacity: usize) -> Result<CriticalFailureSubscription, RuntimeContractViolation> {
        if capacity == 0 {
            return Err(RuntimeContractViolation::InvalidConfiguration {
                policy: RuntimeContractPolicy::CriticalFailureSubscriptionCapacityPositive,
            });
        }
        let (sender, receiver) = mpsc::channel(capacity);
        self.inner.subscribers.lock().push(sender);
        Ok(CriticalFailureSubscription { receiver })
    }

    /// Spawns a monitor under `owner` that takes each pending failure and hands
    /// it to `on_failure`.
    ///
    /// `owner` must be an owner outside the group of the monitored tasks. A group
    /// that became poisoned cannot spawn work, so a monitor inside it could never
    /// run.
    ///
    /// # Errors
    ///
    /// Returns an error when `owner` is shutting down or closed.
    pub fn spawn_monitor<F>(
        &self,
        owner: &TaskSpawner,
        name: impl Into<Arc<str>>,
        on_failure: F,
    ) -> RuntimeResult<TaskId>
    where
        F: Fn(CriticalFailure) + Send + 'static,
    {
        let failures = self.clone();
        let cancellation = owner.cancellation_token();
        owner.spawn(name, TaskKind::Worker, async move {
            loop {
                tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => break,
                    _ = failures.wait() => {
                        // Take the current record atomically. Another handler
                        // may have consumed the notification's older record.
                        if let Some(failure) = failures.handle() {
                            on_failure(failure);
                        }
                    }
                }
            }
        })
    }
}

/// A bounded subscription to critical failure notifications.
#[derive(Debug)]
pub struct CriticalFailureSubscription {
    receiver: mpsc::Receiver<CriticalFailure>,
}

impl CriticalFailureSubscription {
    /// Waits for the next failure notification.
    ///
    /// Returns `None` once the state is dropped.
    pub async fn recv(&mut self) -> Option<CriticalFailure> {
        self.receiver.recv().await
    }

    /// Returns the next notification without waiting.
    #[must_use]
    pub fn try_recv(&mut self) -> Option<CriticalFailure> {
        self.receiver.try_recv().ok()
    }
}
