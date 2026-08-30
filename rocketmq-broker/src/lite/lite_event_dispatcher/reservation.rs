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

use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use parking_lot::Mutex;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_runtime::ResourcePermit;

use super::event_retained_bytes;
use super::ClientEventState;
use super::LiteEventDispatcher;
use super::DEFAULT_CLIENT_EVENT_LIMIT;

#[derive(Debug)]
pub(super) struct LiteEventRecord {
    event: CheetahString,
    permit: Arc<ResourcePermit>,
    enqueued_at: Duration,
}

#[derive(Debug)]
pub(super) struct ReservedEventBatch {
    id: u64,
    group: CheetahString,
    records: VecDeque<LiteEventRecord>,
}

#[derive(Default)]
pub(super) struct LiteEventReservationMetrics {
    batches: AtomicUsize,
    events: AtomicUsize,
    retained_bytes: AtomicUsize,
}

/// Low-cardinality ownership snapshot for in-flight Lite event batches.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct LiteEventReservationSnapshot {
    pub(crate) batches: usize,
    pub(crate) events: usize,
    pub(crate) permits: usize,
    pub(crate) retained_bytes: usize,
}

struct LiteEventReservationObservation {
    metrics: Arc<LiteEventReservationMetrics>,
    events: usize,
    retained_bytes: usize,
}

impl LiteEventReservationObservation {
    fn new(metrics: Arc<LiteEventReservationMetrics>, records: &VecDeque<LiteEventRecord>) -> Self {
        let events = records.len();
        let retained_bytes = records.iter().fold(0usize, |total, record| {
            total.saturating_add(event_retained_bytes(&record.event))
        });
        metrics.batches.fetch_add(1, Ordering::AcqRel);
        metrics.events.fetch_add(events, Ordering::AcqRel);
        metrics.retained_bytes.fetch_add(retained_bytes, Ordering::AcqRel);
        Self {
            metrics,
            events,
            retained_bytes,
        }
    }
}

impl Drop for LiteEventReservationObservation {
    fn drop(&mut self) {
        self.metrics.batches.fetch_sub(1, Ordering::AcqRel);
        self.metrics.events.fetch_sub(self.events, Ordering::AcqRel);
        self.metrics
            .retained_bytes
            .fetch_sub(self.retained_bytes, Ordering::AcqRel);
    }
}

impl ClientEventState {
    fn reserve(&mut self, id: u64) -> bool {
        if self.reserved.is_some() || self.events.is_empty() {
            return false;
        }
        let records = self.drain_records();
        self.reserved = Some(ReservedEventBatch {
            id,
            group: self.group.clone(),
            records,
        });
        true
    }

    fn append_pending_to_reservation(&mut self, id: u64) {
        if self.events.is_empty() {
            return;
        }
        let records = self.drain_records();
        let reserved = self
            .reserved
            .as_mut()
            .filter(|reserved| reserved.id == id)
            .expect("active Lite event reservation remains installed");
        reserved.records.extend(records);
    }

    fn drain_records(&mut self) -> VecDeque<LiteEventRecord> {
        self.events
            .drain(..)
            .map(|event| {
                let permit = self
                    .permits
                    .remove(&event)
                    .expect("queued Lite event owns its resource permit");
                let enqueued_at = self
                    .enqueued_at
                    .remove(&event)
                    .expect("queued Lite event owns its enqueue timestamp");
                LiteEventRecord {
                    event,
                    permit,
                    enqueued_at,
                }
            })
            .collect()
    }

    fn commit(&mut self, id: u64) -> Option<ReservedEventBatch> {
        if self.reserved.as_ref().is_none_or(|reserved| reserved.id != id) {
            return None;
        }
        let reserved = self.reserved.take()?;
        for record in &reserved.records {
            self.committed_events.insert(record.event.clone());
        }
        Some(reserved)
    }

    fn rollback(&mut self, id: u64) -> bool {
        if self.reserved.as_ref().is_none_or(|reserved| reserved.id != id) {
            return false;
        }
        let reserved = self.reserved.take().expect("matching Lite event reservation exists");
        let mut restored = VecDeque::with_capacity(reserved.records.len().saturating_add(self.events.len()));
        for record in reserved.records {
            self.permits.insert(record.event.clone(), record.permit);
            self.enqueued_at.insert(record.event.clone(), record.enqueued_at);
            restored.push_back(record.event);
        }
        restored.append(&mut self.events);
        self.events = restored;
        true
    }
}

impl LiteEventDispatcher {
    pub(crate) fn reservation_snapshot(&self) -> LiteEventReservationSnapshot {
        let events = self.reservation_metrics.events.load(Ordering::Acquire);
        LiteEventReservationSnapshot {
            batches: self.reservation_metrics.batches.load(Ordering::Acquire),
            events,
            permits: events,
            retained_bytes: self.reservation_metrics.retained_bytes.load(Ordering::Acquire),
        }
    }

    /// Reserves the currently consumable batch without releasing its permits.
    ///
    /// An uncommitted reservation restores its records ahead of events that
    /// arrived while the reservation was in flight. Only one reservation may
    /// exist for a client, so compatibility drains cannot steal a batch.
    pub(crate) fn reserve_pending_events(&self, client_id: &CheetahString) -> Option<LiteEventBatchReservation> {
        let now = current_millis();
        self.scan(now);
        self.touch_client(client_id);
        let id = self.next_reservation_id.fetch_add(1, Ordering::AcqRel);
        if id == 0 {
            return None;
        }
        {
            let mut entry = self.client_events.get_mut(client_id)?;
            if entry.reserved.is_some() || !entry.reserve(id) {
                return None;
            }
        }
        if self.promote_deferred_events(client_id, now, true) > 0 {
            let mut entry = self
                .client_events
                .get_mut(client_id)
                .expect("reserved Lite event client remains installed");
            entry.append_pending_to_reservation(id);
        }
        let observation = {
            let entry = self
                .client_events
                .get(client_id)
                .expect("reserved Lite event client remains installed");
            let records = &entry
                .reserved
                .as_ref()
                .filter(|reserved| reserved.id == id)
                .expect("new Lite event reservation remains installed")
                .records;
            LiteEventReservationObservation::new(Arc::clone(&self.reservation_metrics), records)
        };
        Some(LiteEventBatchReservation {
            dispatcher: self.clone(),
            client_id: client_id.clone(),
            id,
            observation: Some(observation),
            armed: true,
        })
    }

    fn commit_reservation(&self, client_id: &CheetahString, id: u64) -> Option<ReservedEventBatch> {
        let mut entry = self.client_events.get_mut(client_id)?;
        entry.commit(id)
    }

    fn rollback_reservation(&self, client_id: &CheetahString, id: u64) {
        if let Some(mut entry) = self.client_events.get_mut(client_id) {
            entry.rollback(id);
        }
    }

    fn settle_committed_records(
        &self,
        client_id: &CheetahString,
        group: &CheetahString,
        mut records: VecDeque<LiteEventRecord>,
        requeue_events: &HashSet<CheetahString>,
        max_event_count: usize,
        _full_dispatch_delay_millis: u64,
    ) {
        if records.is_empty() {
            self.cleanup_client_state(client_id);
            return;
        }
        let mut inserted = 0usize;
        {
            let mut entry = self.client_events.entry(client_id.clone()).or_default();
            if entry.group.is_empty() {
                entry.group = group.clone();
            }
            entry.set_limit(max_event_count);
            while let Some(record) = records.pop_front() {
                entry.committed_events.remove(&record.event);
                let redispatched = entry.committed_redispatches.remove(&record.event);
                if !redispatched && !requeue_events.contains(&record.event) {
                    entry.event_set.remove(&record.event);
                    continue;
                }
                // The committed marker retained this event's logical slot and
                // permit. Converting it back to queued ownership is therefore
                // an in-place transfer, not a fresh capacity acquisition.
                entry.events.push_back(record.event.clone());
                entry.permits.insert(record.event.clone(), record.permit);
                entry.enqueued_at.insert(record.event, record.enqueued_at);
                inserted += 1;
            }
        }
        if inserted > 0 {
            self.notify_client(client_id);
        }
        self.cleanup_client_state(client_id);
    }
}

/// Affine ownership of a not-yet-consumed Lite event batch.
///
/// Dropping this value before `commit` restores the batch in its original
/// relative order and keeps the original resource permits alive.
#[must_use]
pub(crate) struct LiteEventBatchReservation {
    dispatcher: LiteEventDispatcher,
    client_id: CheetahString,
    id: u64,
    observation: Option<LiteEventReservationObservation>,
    armed: bool,
}

impl LiteEventBatchReservation {
    pub(crate) fn event_count(&self) -> usize {
        self.dispatcher
            .client_events
            .get(&self.client_id)
            .and_then(|entry| {
                entry
                    .reserved
                    .as_ref()
                    .filter(|reserved| reserved.id == self.id)
                    .map(|reserved| reserved.records.len())
            })
            .unwrap_or(0)
    }

    pub(crate) fn retained_bytes(&self) -> usize {
        self.observation
            .as_ref()
            .map_or(0, |observation| observation.retained_bytes)
    }

    pub(crate) fn commit(mut self) -> LiteEventBatch {
        let reserved = self
            .dispatcher
            .commit_reservation(&self.client_id, self.id)
            .expect("armed Lite event reservation remains installed until commit");
        self.armed = false;
        LiteEventBatch {
            dispatcher: self.dispatcher.clone(),
            client_id: self.client_id.clone(),
            group: reserved.group,
            records: reserved.records,
            observation: self.observation.take(),
            completed: false,
        }
    }

    /// Splits this affine reservation into an execution handle and a terminal owner.
    ///
    /// The execution handle may commit and stage a consume/requeue decision, but the
    /// terminal owner alone settles records and releases their permits. This lets a
    /// deferred response keep event ownership through canonical socket completion.
    pub(crate) fn into_terminal_ownership(self) -> (LiteEventBatchExecution, LiteEventBatchTerminal) {
        let state = Arc::new(Mutex::new(LiteEventBatchTerminalState::Reserved(self)));
        (
            LiteEventBatchExecution {
                state: Arc::clone(&state),
            },
            LiteEventBatchTerminal { state: Some(state) },
        )
    }
}

impl Drop for LiteEventBatchReservation {
    fn drop(&mut self) {
        if self.armed {
            self.dispatcher.rollback_reservation(&self.client_id, self.id);
        }
    }
}

/// A committed Lite event batch whose permits remain owned until completion.
#[must_use]
pub(crate) struct LiteEventBatch {
    dispatcher: LiteEventDispatcher,
    client_id: CheetahString,
    group: CheetahString,
    records: VecDeque<LiteEventRecord>,
    observation: Option<LiteEventReservationObservation>,
    completed: bool,
}

impl LiteEventBatch {
    pub(crate) fn event_names(&self) -> Vec<CheetahString> {
        self.records.iter().map(|record| record.event.clone()).collect()
    }

    pub(crate) fn event_count(&self) -> usize {
        self.records.len()
    }

    pub(crate) fn retained_bytes(&self) -> usize {
        self.observation
            .as_ref()
            .map_or(0, |observation| observation.retained_bytes)
    }

    /// Completes the batch, consuming unlisted records and transferring the
    /// original permits for listed records back to the dispatcher.
    pub(crate) fn complete(self, requeue_events: &HashSet<CheetahString>) {
        let max_event_count = self
            .dispatcher
            .client_events
            .get(&self.client_id)
            .map_or(DEFAULT_CLIENT_EVENT_LIMIT, |entry| entry.max_event_count);
        self.complete_with_limit(requeue_events, max_event_count, 0);
    }

    pub(crate) fn complete_with_limit(
        mut self,
        requeue_events: &HashSet<CheetahString>,
        max_event_count: usize,
        full_dispatch_delay_millis: u64,
    ) {
        let records = std::mem::take(&mut self.records);
        self.completed = true;
        self.dispatcher.settle_committed_records(
            &self.client_id,
            &self.group,
            records,
            requeue_events,
            max_event_count,
            full_dispatch_delay_millis,
        );
    }
}

impl Drop for LiteEventBatch {
    fn drop(&mut self) {
        if self.completed || self.records.is_empty() {
            return;
        }
        let records = std::mem::take(&mut self.records);
        let max_event_count = self
            .dispatcher
            .client_events
            .get(&self.client_id)
            .map_or(DEFAULT_CLIENT_EVENT_LIMIT, |entry| entry.max_event_count);
        let requeue_events = records.iter().map(|record| record.event.clone()).collect();
        self.dispatcher.settle_committed_records(
            &self.client_id,
            &self.group,
            records,
            &requeue_events,
            max_event_count,
            0,
        );
    }
}

struct LiteEventBatchCompletion {
    requeue_events: HashSet<CheetahString>,
    max_event_count: usize,
    full_dispatch_delay_millis: u64,
}

enum LiteEventBatchTerminalState {
    Reserved(LiteEventBatchReservation),
    Committed {
        batch: LiteEventBatch,
        completion: Option<LiteEventBatchCompletion>,
    },
    Settled,
}

/// Affine handler-side access to an event batch retained by a terminal owner.
#[must_use]
pub(crate) struct LiteEventBatchExecution {
    state: Arc<Mutex<LiteEventBatchTerminalState>>,
}

impl LiteEventBatchExecution {
    pub(crate) fn commit(self) -> LiteEventBatchCommit {
        let mut state = self.state.lock();
        let reserved = match std::mem::replace(&mut *state, LiteEventBatchTerminalState::Settled) {
            LiteEventBatchTerminalState::Reserved(reserved) => reserved,
            LiteEventBatchTerminalState::Committed { .. } | LiteEventBatchTerminalState::Settled => {
                panic!("Lite event terminal reservation commits exactly once")
            }
        };
        let batch = reserved.commit();
        *state = LiteEventBatchTerminalState::Committed {
            batch,
            completion: None,
        };
        drop(state);
        LiteEventBatchCommit { state: self.state }
    }
}

/// A committed event-batch view whose settlement is deferred to response terminal.
#[must_use]
pub(crate) struct LiteEventBatchCommit {
    state: Arc<Mutex<LiteEventBatchTerminalState>>,
}

impl LiteEventBatchCommit {
    pub(crate) fn event_names(&self) -> Vec<CheetahString> {
        let state = self.state.lock();
        match &*state {
            LiteEventBatchTerminalState::Committed { batch, .. } => batch.event_names(),
            LiteEventBatchTerminalState::Reserved(_) | LiteEventBatchTerminalState::Settled => Vec::new(),
        }
    }

    pub(crate) fn complete(self, requeue_events: &HashSet<CheetahString>) {
        let max_event_count = {
            let state = self.state.lock();
            match &*state {
                LiteEventBatchTerminalState::Committed { batch, .. } => batch
                    .dispatcher
                    .client_events
                    .get(&batch.client_id)
                    .map_or(DEFAULT_CLIENT_EVENT_LIMIT, |entry| entry.max_event_count),
                LiteEventBatchTerminalState::Reserved(_) | LiteEventBatchTerminalState::Settled => return,
            }
        };
        self.complete_with_limit(requeue_events, max_event_count, 0);
    }

    pub(crate) fn complete_with_limit(
        self,
        requeue_events: &HashSet<CheetahString>,
        max_event_count: usize,
        full_dispatch_delay_millis: u64,
    ) {
        let mut state = self.state.lock();
        let LiteEventBatchTerminalState::Committed { completion, .. } = &mut *state else {
            return;
        };
        debug_assert!(completion.is_none(), "Lite event batch completion is staged once");
        *completion = Some(LiteEventBatchCompletion {
            requeue_events: requeue_events.clone(),
            max_event_count,
            full_dispatch_delay_millis,
        });
    }
}

/// Terminal owner that settles a committed batch only when canonical response ownership ends.
#[must_use]
pub(crate) struct LiteEventBatchTerminal {
    state: Option<Arc<Mutex<LiteEventBatchTerminalState>>>,
}

impl Drop for LiteEventBatchTerminal {
    fn drop(&mut self) {
        let Some(state) = self.state.take() else {
            return;
        };
        let terminal = {
            let mut state = state.lock();
            std::mem::replace(&mut *state, LiteEventBatchTerminalState::Settled)
        };
        match terminal {
            LiteEventBatchTerminalState::Reserved(reservation) => drop(reservation),
            LiteEventBatchTerminalState::Committed {
                batch,
                completion: Some(completion),
            } => batch.complete_with_limit(
                &completion.requeue_events,
                completion.max_event_count,
                completion.full_dispatch_delay_millis,
            ),
            LiteEventBatchTerminalState::Committed {
                batch,
                completion: None,
            } => drop(batch),
            LiteEventBatchTerminalState::Settled => {}
        }
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/lite/lite_event_dispatcher/reservation.rs"]
mod tests;
