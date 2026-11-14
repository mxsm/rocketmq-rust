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

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_model::common::message::message_queue::MessageQueue;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use super::LitePullTaskHandle;
use super::ASSIGNMENT_TASK_SHUTDOWN_TIMEOUT;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AssignmentRegistrySnapshot {
    pub entries: usize,
    pub owned_tasks: usize,
    pub closed: bool,
}

/// Owns every queue-scoped resource for one active LitePull assignment.
pub(super) struct AssignmentEntry {
    operation_lock: Arc<Mutex<()>>,
    task: Mutex<Option<OwnedLitePullTask>>,
    next_task_generation: AtomicU64,
    cancellation: CancellationToken,
    retired: AtomicBool,
}

struct OwnedLitePullTask {
    generation: u64,
    handle: LitePullTaskHandle,
}

impl AssignmentEntry {
    fn new() -> Self {
        Self {
            operation_lock: Arc::new(Mutex::new(())),
            task: Mutex::new(None),
            next_task_generation: AtomicU64::new(1),
            cancellation: CancellationToken::new(),
            retired: AtomicBool::new(false),
        }
    }

    pub(super) fn operation_lock(&self) -> Arc<Mutex<()>> {
        Arc::clone(&self.operation_lock)
    }

    pub(super) fn cancellation(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    pub(super) fn is_retired(&self) -> bool {
        self.retired.load(Ordering::Acquire)
    }

    fn retire(&self) {
        self.retired.store(true, Ordering::Release);
        self.cancellation.cancel();
    }

    pub(super) fn next_task_generation(&self) -> u64 {
        self.next_task_generation.fetch_add(1, Ordering::Relaxed)
    }

    async fn install_task(&self, generation: u64, handle: LitePullTaskHandle) -> Result<(), LitePullTaskHandle> {
        if self.is_retired() {
            return Err(handle);
        }
        let mut task = self.task.lock().await;
        if self.is_retired() || task.is_some() {
            return Err(handle);
        }
        *task = Some(OwnedLitePullTask { generation, handle });
        Ok(())
    }

    pub(super) async fn has_task(&self) -> bool {
        self.task.lock().await.is_some()
    }

    pub(super) async fn owns_task_generation(&self, generation: u64) -> bool {
        self.task
            .lock()
            .await
            .as_ref()
            .is_some_and(|task| task.generation == generation)
    }

    pub(super) async fn take_task(&self) -> Option<LitePullTaskHandle> {
        self.task.lock().await.take().map(|task| task.handle)
    }

    pub(super) async fn take_task_if_generation(&self, generation: u64) -> Option<LitePullTaskHandle> {
        let mut task = self.task.lock().await;
        if task.as_ref().is_some_and(|task| task.generation == generation) {
            return task.take().map(|task| task.handle);
        }
        None
    }

    pub(super) async fn clear_task_if_generation(&self, generation: u64) {
        drop(self.take_task_if_generation(generation).await);
    }

    async fn shutdown(&self, timeout: Duration) -> bool {
        self.retire();
        match self.take_task().await {
            Some(handle) => handle.wait(timeout).await,
            None => true,
        }
    }
}

/// Canonical lifecycle registry for LitePull queue assignments.
pub(super) struct AssignmentRegistry {
    entries: RwLock<HashMap<MessageQueue, Arc<AssignmentEntry>>>,
    closed: AtomicBool,
}

impl AssignmentRegistry {
    pub(super) fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            closed: AtomicBool::new(false),
        }
    }

    pub(super) async fn ensure(&self, message_queue: MessageQueue) -> Option<Arc<AssignmentEntry>> {
        if self.closed.load(Ordering::Acquire) {
            return None;
        }
        if let Some(entry) = self.entries.read().await.get(&message_queue) {
            return Some(Arc::clone(entry));
        }
        let mut entries = self.entries.write().await;
        if self.closed.load(Ordering::Acquire) {
            return None;
        }
        Some(
            entries
                .entry(message_queue)
                .or_insert_with(|| Arc::new(AssignmentEntry::new()))
                .clone(),
        )
    }

    pub(super) async fn get(&self, message_queue: &MessageQueue) -> Option<Arc<AssignmentEntry>> {
        self.entries.read().await.get(message_queue).cloned()
    }

    pub(super) async fn is_current(&self, message_queue: &MessageQueue, expected: &Arc<AssignmentEntry>) -> bool {
        self.entries
            .read()
            .await
            .get(message_queue)
            .is_some_and(|current| Arc::ptr_eq(current, expected))
            && !expected.is_retired()
    }

    pub(super) async fn install_task(
        &self,
        message_queue: &MessageQueue,
        expected: &Arc<AssignmentEntry>,
        generation: u64,
        handle: LitePullTaskHandle,
    ) -> Result<(), LitePullTaskHandle> {
        if !self.is_current(message_queue, expected).await {
            return Err(handle);
        }
        expected.install_task(generation, handle).await
    }

    pub(super) async fn remove(&self, message_queue: &MessageQueue) -> Option<Arc<AssignmentEntry>> {
        let removed = self.entries.write().await.remove(message_queue);
        if let Some(entry) = removed.as_ref() {
            entry.retire();
        }
        removed
    }

    pub(super) async fn remove_and_wait(&self, message_queue: &MessageQueue) -> bool {
        if let Some(entry) = self.remove(message_queue).await {
            return entry.shutdown(ASSIGNMENT_TASK_SHUTDOWN_TIMEOUT).await;
        }
        true
    }

    pub(super) async fn reconcile_topic(&self, topic: &str, assigned: &HashSet<MessageQueue>) -> Vec<MessageQueue> {
        let removed = {
            let mut entries = self.entries.write().await;
            let queues = entries
                .keys()
                .filter(|queue| queue.topic() == topic && !assigned.contains(*queue))
                .cloned()
                .collect::<Vec<_>>();
            queues
                .into_iter()
                .filter_map(|queue| entries.remove(&queue).map(|entry| (queue, entry)))
                .collect::<Vec<_>>()
        };
        for (_, entry) in &removed {
            entry.retire();
        }
        shutdown_together(removed, ASSIGNMENT_TASK_SHUTDOWN_TIMEOUT)
            .await
            .into_iter()
            .map(|(queue, _)| queue)
            .collect()
    }

    pub(super) async fn reconcile_all(&self, assigned: &HashSet<MessageQueue>) -> Vec<MessageQueue> {
        let removed = {
            let mut entries = self.entries.write().await;
            let queues = entries
                .keys()
                .filter(|queue| !assigned.contains(*queue))
                .cloned()
                .collect::<Vec<_>>();
            queues
                .into_iter()
                .filter_map(|queue| entries.remove(&queue).map(|entry| (queue, entry)))
                .collect::<Vec<_>>()
        };
        for (_, entry) in &removed {
            entry.retire();
        }
        shutdown_together(removed, ASSIGNMENT_TASK_SHUTDOWN_TIMEOUT)
            .await
            .into_iter()
            .map(|(queue, _)| queue)
            .collect()
    }

    pub(super) async fn remove_topic(&self, topic: &str) {
        let removed = {
            let mut entries = self.entries.write().await;
            let queues = entries
                .keys()
                .filter(|queue| queue.topic() == topic)
                .cloned()
                .collect::<Vec<_>>();
            queues
                .into_iter()
                .filter_map(|queue| entries.remove(&queue).map(|entry| (queue, entry)))
                .collect::<Vec<_>>()
        };
        for (_, entry) in &removed {
            entry.retire();
        }
        shutdown_together(removed, ASSIGNMENT_TASK_SHUTDOWN_TIMEOUT).await;
    }

    pub(super) async fn close_and_shutdown(&self, timeout: Duration) -> Vec<(MessageQueue, bool)> {
        self.closed.store(true, Ordering::Release);
        let removed = {
            let mut entries = self.entries.write().await;
            std::mem::take(&mut *entries)
        };
        for entry in removed.values() {
            entry.retire();
        }
        shutdown_together(removed, timeout).await
    }

    pub(super) async fn snapshot(&self) -> AssignmentRegistrySnapshot {
        let entries = self.entries.read().await.values().cloned().collect::<Vec<_>>();
        let mut owned_tasks = 0;
        for entry in &entries {
            owned_tasks += usize::from(entry.has_task().await);
        }
        AssignmentRegistrySnapshot {
            entries: entries.len(),
            owned_tasks,
            closed: self.closed.load(Ordering::Acquire),
        }
    }
}

/// Stops the tasks of retired entries under one shared deadline and reports whether each stopped
/// in time. A pull task may be waiting for a Broker that holds the pull for its whole suspend
/// time, so waiting queue by queue would stretch the wait to one timeout per queue.
async fn shutdown_together(
    entries: impl IntoIterator<Item = (MessageQueue, Arc<AssignmentEntry>)>,
    timeout: Duration,
) -> Vec<(MessageQueue, bool)> {
    futures::future::join_all(
        entries
            .into_iter()
            .map(|(queue, entry)| async move { (queue, entry.shutdown(timeout).await) }),
    )
    .await
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::super::spawn_lite_pull_task;
    use super::*;

    #[tokio::test]
    async fn closing_stops_every_queue_task_within_one_timeout() {
        // Tasks that ignore cancellation, like pulls a Broker holds for its whole suspend time.
        const TIMEOUT: Duration = Duration::from_millis(300);
        let registry = AssignmentRegistry::new();
        let context = crate::runtime::test_service_context("lite-pull-assignment-close-test");
        for queue_id in 0..4 {
            let queue = MessageQueue::from_parts("close-together-topic", "broker-a", queue_id);
            let entry = registry
                .ensure(queue.clone())
                .await
                .expect("an open registry admits queues");
            let handle = spawn_lite_pull_task(
                &context,
                "rocketmq-client-lite-pull-test",
                Arc::new(AtomicBool::new(false)),
                std::future::pending::<()>(),
            )
            .expect("the pull task spawns");
            let generation = entry.next_task_generation();
            assert!(registry.install_task(&queue, &entry, generation, handle).await.is_ok());
        }

        let started = Instant::now();
        let reports = registry.close_and_shutdown(TIMEOUT).await;
        let elapsed = started.elapsed();

        assert_eq!(reports.len(), 4);
        assert!(reports.iter().all(|(_, stopped)| !stopped), "every task timed out");
        // One timeout to wait plus the abort; queue by queue would take four timeouts.
        assert!(elapsed < TIMEOUT * 3, "closing took {elapsed:?}");
        assert_eq!(registry.snapshot().await.entries, 0);
    }
}
