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

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use parking_lot::Mutex;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskKind;
use rocketmq_transport::TransportClient;
use tokio::sync::mpsc;
use tracing::info;
use tracing::warn;

use super::NotifyKey;
use super::NotifyState;
use super::NotifyTask;
use crate::error::ControllerError;
use crate::error::Result;

const DEFAULT_MAILBOX_CAPACITY: usize = 1_024;
const MAX_NOTIFY_ATTEMPTS: u32 = 3;
const NOTIFY_TIMEOUT_MILLIS: u64 = 3_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SubmitOutcome {
    Accepted,
    Coalesced,
    Replaced,
    Stale,
    Full,
    Inactive,
    Closed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NotifySnapshot {
    pub capacity: usize,
    pub queued_keys: usize,
    pub in_flight_keys: usize,
    pub retry_waiting_keys: usize,
    pub retained_keys: usize,
    pub generation: u64,
    pub accepted: u64,
    pub coalesced: u64,
    pub replaced: u64,
    pub stale: u64,
    pub rejected_full: u64,
    pub retries: u64,
    pub completed: u64,
    pub failed: u64,
    pub oldest_queue_wait: Duration,
    pub last_rpc_latency: Option<Duration>,
    pub closed: bool,
}

#[derive(Debug)]
struct QueuedTask {
    task: NotifyTask,
    queued_at: Instant,
}

#[derive(Debug)]
pub(super) struct Mailbox {
    capacity: usize,
    pending: HashMap<NotifyKey, QueuedTask>,
    in_flight: HashMap<NotifyKey, NotifyState>,
    retry_waiting: HashMap<NotifyKey, NotifyState>,
    notified: HashMap<NotifyKey, NotifyState>,
    generation: u64,
    started: bool,
    enabled: bool,
    closed: bool,
    accepted: u64,
    coalesced: u64,
    replaced: u64,
    stale: u64,
    rejected_full: u64,
    retries: u64,
    completed: u64,
    failed: u64,
    last_rpc_latency: Option<Duration>,
}

impl Mailbox {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            pending: HashMap::new(),
            in_flight: HashMap::new(),
            retry_waiting: HashMap::new(),
            notified: HashMap::new(),
            generation: 0,
            started: false,
            enabled: false,
            closed: false,
            accepted: 0,
            coalesced: 0,
            replaced: 0,
            stale: 0,
            rejected_full: 0,
            retries: 0,
            completed: 0,
            failed: 0,
            last_rpc_latency: None,
        }
    }

    fn retained_keys(&self) -> usize {
        let mut retained: HashSet<_> = self.in_flight.keys().cloned().collect();
        retained.extend(self.retry_waiting.keys().cloned());
        retained.extend(self.pending.keys().cloned());
        retained.len()
    }

    pub(super) fn submit(
        &mut self,
        mut task: NotifyTask,
        retry: bool,
        sender: &mpsc::Sender<NotifyKey>,
    ) -> SubmitOutcome {
        if retry {
            self.retry_waiting.remove(&task.key);
        }
        if !self.started || self.closed {
            return SubmitOutcome::Closed;
        }
        if !self.enabled {
            return SubmitOutcome::Inactive;
        }
        if retry {
            if task.generation != self.generation {
                self.stale += 1;
                return SubmitOutcome::Stale;
            }
        } else {
            task.generation = self.generation;
        }

        if let Some(previous) = self.notified.get(&task.key) {
            if previous == &task.state {
                self.coalesced += 1;
                return SubmitOutcome::Coalesced;
            }
            if previous.is_same_or_newer_than(&task.state) {
                self.stale += 1;
                return SubmitOutcome::Stale;
            }
        }

        if let Some(previous) = self.in_flight.get(&task.key) {
            if previous == &task.state {
                self.coalesced += 1;
                return SubmitOutcome::Coalesced;
            }
            if previous.is_same_or_newer_than(&task.state) {
                self.stale += 1;
                return SubmitOutcome::Stale;
            }
        }

        if let Some(previous) = self.retry_waiting.get(&task.key) {
            if previous == &task.state {
                self.coalesced += 1;
                return SubmitOutcome::Coalesced;
            }
            if previous.is_same_or_newer_than(&task.state) {
                self.stale += 1;
                return SubmitOutcome::Stale;
            }
        }

        if let Some(previous) = self.pending.get(&task.key) {
            if previous.task.state == task.state {
                self.coalesced += 1;
                return SubmitOutcome::Coalesced;
            }
            if previous.task.state.is_same_or_newer_than(&task.state) {
                self.stale += 1;
                return SubmitOutcome::Stale;
            }
            self.pending.insert(
                task.key.clone(),
                QueuedTask {
                    task,
                    queued_at: previous.queued_at,
                },
            );
            self.replaced += 1;
            return SubmitOutcome::Replaced;
        }

        let key_already_retained = self.in_flight.contains_key(&task.key) || self.retry_waiting.contains_key(&task.key);
        if !key_already_retained && self.retained_keys() >= self.capacity {
            self.rejected_full += 1;
            return SubmitOutcome::Full;
        }

        let key = task.key.clone();
        self.pending.insert(
            key.clone(),
            QueuedTask {
                task,
                queued_at: Instant::now(),
            },
        );
        match sender.try_send(key.clone()) {
            Ok(()) => {
                if retry {
                    self.retries += 1;
                } else {
                    self.accepted += 1;
                }
                SubmitOutcome::Accepted
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.pending.remove(&key);
                self.rejected_full += 1;
                SubmitOutcome::Full
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.pending.remove(&key);
                self.closed = true;
                SubmitOutcome::Closed
            }
        }
    }

    pub(super) fn take(&mut self, key: &NotifyKey) -> Option<NotifyTask> {
        let queued = self.pending.remove(key)?;
        if queued.task.generation != self.generation {
            self.stale += 1;
            return None;
        }
        self.in_flight.insert(key.clone(), queued.task.state.clone());
        Some(queued.task)
    }

    pub(super) fn finish(&mut self, task: &NotifyTask, success: bool, latency: Duration) -> bool {
        self.in_flight.remove(&task.key);
        self.last_rpc_latency = Some(latency);
        if task.generation != self.generation {
            self.stale += 1;
            return false;
        }
        if success {
            self.completed += 1;
            self.notified.insert(task.key.clone(), task.state.clone());
            return false;
        }

        self.failed += 1;
        let should_retry =
            task.attempt + 1 < MAX_NOTIFY_ATTEMPTS && !self.pending.contains_key(&task.key) && !self.closed;
        if should_retry {
            self.retry_waiting.insert(task.key.clone(), task.state.clone());
        }
        should_retry
    }

    fn abandon_retry(&mut self, key: &NotifyKey) {
        self.retry_waiting.remove(key);
    }

    pub(super) fn reset(&mut self) {
        self.enabled = false;
        self.generation = self.generation.wrapping_add(1);
        self.pending.clear();
        self.retry_waiting.clear();
        self.notified.clear();
    }

    fn close(&mut self) {
        self.closed = true;
        self.reset();
    }

    pub(super) fn snapshot(&self) -> NotifySnapshot {
        NotifySnapshot {
            capacity: self.capacity,
            queued_keys: self.pending.len(),
            in_flight_keys: self.in_flight.len(),
            retry_waiting_keys: self.retry_waiting.len(),
            retained_keys: self.retained_keys(),
            generation: self.generation,
            accepted: self.accepted,
            coalesced: self.coalesced,
            replaced: self.replaced,
            stale: self.stale,
            rejected_full: self.rejected_full,
            retries: self.retries,
            completed: self.completed,
            failed: self.failed,
            oldest_queue_wait: self
                .pending
                .values()
                .map(|queued| queued.queued_at.elapsed())
                .max()
                .unwrap_or(Duration::ZERO),
            last_rpc_latency: self.last_rpc_latency,
            closed: self.closed,
        }
    }

    #[cfg(test)]
    pub(super) fn notified_contains(&self, key: &NotifyKey) -> bool {
        self.notified.contains_key(key)
    }

    #[cfg(test)]
    pub(super) fn mark_started(&mut self) {
        self.started = true;
        self.enabled = true;
    }
}

#[derive(Clone)]
pub(crate) struct BrokerRoleNotifier {
    client: Arc<TransportClient>,
    sender: mpsc::Sender<NotifyKey>,
    receiver: Arc<Mutex<Option<mpsc::Receiver<NotifyKey>>>>,
    mailbox: Arc<Mutex<Mailbox>>,
    retry_base_delay: Duration,
}

impl BrokerRoleNotifier {
    pub(crate) fn new(client: Arc<TransportClient>, retry_base_delay: Duration) -> Self {
        let (sender, receiver) = mpsc::channel(DEFAULT_MAILBOX_CAPACITY);
        Self {
            client,
            sender,
            receiver: Arc::new(Mutex::new(Some(receiver))),
            mailbox: Arc::new(Mutex::new(Mailbox::new(DEFAULT_MAILBOX_CAPACITY))),
            retry_base_delay,
        }
    }

    pub(crate) fn start(&self, task_group: &TaskGroup) -> Result<()> {
        let mut receiver = self
            .receiver
            .lock()
            .take()
            .ok_or_else(|| ControllerError::runtime_error("Broker role notifier was already started"))?;
        {
            let mut mailbox = self.mailbox.lock();
            mailbox.started = true;
            mailbox.enabled = false;
            mailbox.closed = false;
        }

        let notifier = self.clone();
        let task_group = task_group.clone();
        let worker_group = task_group.clone();
        let shutdown_token = task_group.cancellation_token();
        task_group
            .spawn_service("controller.broker-role-notifier", async move {
                loop {
                    let key = tokio::select! {
                        _ = shutdown_token.cancelled() => break,
                        key = receiver.recv() => key,
                    };
                    let Some(key) = key else {
                        break;
                    };
                    let Some(task) = notifier.mailbox.lock().take(&key) else {
                        continue;
                    };
                    notifier.process(task, &worker_group).await;
                }
            })
            .map_err(|error| {
                ControllerError::runtime_error(format!("Failed to spawn broker role notifier task: {error}"))
            })?;
        Ok(())
    }

    pub(crate) fn submit(&self, task: NotifyTask) -> SubmitOutcome {
        self.mailbox.lock().submit(task, false, &self.sender)
    }

    fn submit_retry(&self, task: NotifyTask) -> SubmitOutcome {
        self.mailbox.lock().submit(task, true, &self.sender)
    }

    async fn process(&self, task: NotifyTask, task_group: &TaskGroup) {
        let started_at = Instant::now();
        let result = self
            .client
            .invoke_request(Some(&task.broker_addr), task.build_request(), NOTIFY_TIMEOUT_MILLIS)
            .await;
        let success = matches!(&result, Ok(response) if response.code() == ResponseCode::Success as i32);
        let should_retry = self.mailbox.lock().finish(&task, success, started_at.elapsed());

        match result {
            Ok(_response) if success => {
                info!(
                    target = %task.broker_addr,
                    broker_id = task.key.broker_id,
                    broker = %task.key.broker_name,
                    "Notified broker role change"
                );
            }
            Ok(response) => {
                warn!(
                    target = %task.broker_addr,
                    broker_id = task.key.broker_id,
                    broker = %task.key.broker_name,
                    code = response.code(),
                    remark = ?response.remark(),
                    "Broker role notify did not succeed"
                );
            }
            Err(error) => {
                warn!(
                    target = %task.broker_addr,
                    broker_id = task.key.broker_id,
                    broker = %task.key.broker_name,
                    %error,
                    "Failed to notify broker role change"
                );
            }
        }

        if should_retry {
            self.schedule_retry(task.retry(), task_group);
        }
    }

    fn schedule_retry(&self, task: NotifyTask, task_group: &TaskGroup) {
        let delay = self
            .retry_base_delay
            .saturating_mul(task.attempt)
            .min(Duration::from_secs(2));
        let task_key = task.key.clone();
        let notifier = self.clone();
        let shutdown_token = task_group.cancellation_token();
        if let Err(error) = task_group.spawn("controller.broker-role-notifier.retry", TaskKind::Worker, async move {
            tokio::select! {
                _ = shutdown_token.cancelled() => return,
                _ = tokio::time::sleep(delay) => {}
            }
            let outcome = notifier.submit_retry(task);
            if matches!(outcome, SubmitOutcome::Full | SubmitOutcome::Closed) {
                warn!(?outcome, "Broker role notify retry was not retained");
            }
        }) {
            self.mailbox.lock().abandon_retry(&task_key);
            warn!(?error, "Failed to spawn broker role notifier retry");
        }
    }

    pub(crate) fn reset(&self) {
        self.mailbox.lock().reset();
    }

    pub(crate) fn enable(&self) {
        let mut mailbox = self.mailbox.lock();
        if mailbox.started && !mailbox.closed {
            mailbox.enabled = true;
        }
    }

    pub(crate) fn close(&self) {
        self.mailbox.lock().close();
    }

    pub(crate) fn snapshot(&self) -> NotifySnapshot {
        self.mailbox.lock().snapshot()
    }
}
