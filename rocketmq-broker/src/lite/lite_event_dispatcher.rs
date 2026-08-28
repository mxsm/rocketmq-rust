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

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use cheetah_string::CheetahString;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_runtime::BudgetConfigError;
use rocketmq_runtime::BudgetLimit;
use rocketmq_runtime::BudgetSnapshot;
use rocketmq_runtime::FullPolicy;
use rocketmq_runtime::RateLimit;
use rocketmq_runtime::ResourceBudget;
use rocketmq_runtime::ResourceBudgetTree;
use rocketmq_runtime::ResourcePermit;
use tokio::sync::Notify;

pub(crate) use self::reservation::LiteEventBatch;
pub(crate) use self::reservation::LiteEventBatchExecution;
pub(crate) use self::reservation::LiteEventBatchReservation;
pub(crate) use self::reservation::LiteEventBatchTerminal;
use self::reservation::LiteEventReservationMetrics;
pub(crate) use self::reservation::LiteEventReservationSnapshot;
use self::reservation::ReservedEventBatch;

mod reservation;

const DEFAULT_CLIENT_EVENT_LIMIT: usize = 10_000;
const DEFAULT_EVENT_MEMORY_BYTES: usize = 16 * 1024 * 1024;
const MAX_EVENT_AGE: Duration = Duration::from_secs(30);
const MAX_CLIENT_IDLE_AGE: Duration = Duration::from_secs(300);

#[derive(Debug)]
struct ClientAccessState {
    last_access_time: u64,
    last_touch: Duration,
    _permit: ResourcePermit,
}

#[derive(Debug)]
struct ClientEventState {
    group: CheetahString,
    events: VecDeque<CheetahString>,
    event_set: HashSet<CheetahString>,
    committed_events: HashSet<CheetahString>,
    committed_redispatches: HashSet<CheetahString>,
    permits: HashMap<CheetahString, Arc<ResourcePermit>>,
    enqueued_at: HashMap<CheetahString, Duration>,
    max_event_count: usize,
    reserved: Option<ReservedEventBatch>,
}

impl Default for ClientEventState {
    fn default() -> Self {
        Self {
            group: CheetahString::new(),
            events: VecDeque::new(),
            event_set: HashSet::new(),
            committed_events: HashSet::new(),
            committed_redispatches: HashSet::new(),
            permits: HashMap::new(),
            enqueued_at: HashMap::new(),
            max_event_count: DEFAULT_CLIENT_EVENT_LIMIT,
            reserved: None,
        }
    }
}

impl ClientEventState {
    fn set_limit(&mut self, max_event_count: usize) {
        self.max_event_count = normalize_limit(max_event_count);
    }

    fn offer(&mut self, event: CheetahString, permit: Arc<ResourcePermit>, enqueued_at: Duration) -> bool {
        if self.event_set.contains(&event) {
            return true;
        }
        if self.event_set.len() >= self.max_event_count {
            return false;
        }
        self.event_set.insert(event.clone());
        self.events.push_back(event.clone());
        self.permits.insert(event.clone(), permit);
        self.enqueued_at.insert(event, enqueued_at);
        true
    }

    fn pending_events(&self) -> Vec<CheetahString> {
        self.events.iter().cloned().collect()
    }

    fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    fn is_fully_empty(&self) -> bool {
        self.events.is_empty()
            && self.reserved.is_none()
            && self.committed_events.is_empty()
            && self.committed_redispatches.is_empty()
    }

    fn drop_stale(&mut self, now: Duration, max_age: Duration) -> usize {
        let stale = self
            .enqueued_at
            .iter()
            .filter(|(_, enqueued_at)| now.saturating_sub(**enqueued_at) >= max_age)
            .map(|(event, _)| event.clone())
            .collect::<HashSet<_>>();
        if stale.is_empty() {
            return 0;
        }
        self.events.retain(|event| !stale.contains(event));
        for event in &stale {
            self.event_set.remove(event);
            self.permits.remove(event);
            self.enqueued_at.remove(event);
        }
        stale.len()
    }
}

#[derive(Debug, Clone, Default)]
struct DeferredDispatchState {
    group: CheetahString,
    events: BTreeSet<CheetahString>,
    permits: HashMap<CheetahString, Arc<ResourcePermit>>,
    enqueued_at: HashMap<CheetahString, Duration>,
    due_time: u64,
    delay_millis: u64,
    max_event_count: usize,
}

impl DeferredDispatchState {
    fn drop_stale(&mut self, now: Duration, max_age: Duration) -> usize {
        let stale = self
            .enqueued_at
            .iter()
            .filter(|(_, enqueued_at)| now.saturating_sub(**enqueued_at) >= max_age)
            .map(|(event, _)| event.clone())
            .collect::<Vec<_>>();
        for event in &stale {
            self.events.remove(event);
            self.permits.remove(event);
            self.enqueued_at.remove(event);
        }
        stale.len()
    }
}

fn normalize_limit(max_event_count: usize) -> usize {
    if max_event_count == 0 {
        DEFAULT_CLIENT_EVENT_LIMIT
    } else {
        max_event_count
    }
}

fn client_access_retained_bytes(client_id: &CheetahString) -> usize {
    std::mem::size_of::<CheetahString>()
        .saturating_add(std::mem::size_of::<ClientAccessState>())
        .saturating_add(client_id.len())
}

fn event_retained_bytes(event: &CheetahString) -> usize {
    const STORED_EVENT_KEYS: usize = 4;

    std::mem::size_of::<CheetahString>()
        .saturating_mul(STORED_EVENT_KEYS)
        .saturating_add(std::mem::size_of::<Arc<ResourcePermit>>())
        .saturating_add(std::mem::size_of::<Duration>())
        .saturating_add(event.len())
}

#[derive(Clone)]
pub(crate) struct LiteEventDispatcher {
    client_events: Arc<DashMap<CheetahString, ClientEventState>>,
    deferred_dispatches: Arc<DashMap<CheetahString, DeferredDispatchState>>,
    client_access: Arc<DashMap<CheetahString, ClientAccessState>>,
    wakeup_notify: Arc<Mutex<Option<Arc<Notify>>>>,
    event_budget: ResourceBudget,
    client_access_budget: ResourceBudget,
    next_reservation_id: Arc<AtomicU64>,
    reservation_metrics: Arc<LiteEventReservationMetrics>,
}

impl Default for LiteEventDispatcher {
    fn default() -> Self {
        let tree = ResourceBudgetTree::new(
            "broker-lite-events",
            BudgetLimit::new(
                DEFAULT_CLIENT_EVENT_LIMIT.saturating_mul(2),
                DEFAULT_EVENT_MEMORY_BYTES,
                FullPolicy::Reject,
            ),
        )
        .expect("positive standalone Lite event limits are valid");
        Self::try_with_resource_budget(&tree.root(), DEFAULT_CLIENT_EVENT_LIMIT, DEFAULT_CLIENT_EVENT_LIMIT)
            .expect("standalone Lite event child budget fits its root")
    }
}

impl LiteEventDispatcher {
    pub(crate) fn try_with_resource_budget(
        parent_budget: &ResourceBudget,
        max_pending_event_count: usize,
        max_tracked_client_count: usize,
    ) -> Result<Self, BudgetConfigError> {
        let max_pending_event_count =
            normalize_limit(max_pending_event_count).min(parent_budget.limit().capacity.count);
        let max_tracked_client_count = max_tracked_client_count
            .max(1)
            .min(parent_budget.limit().capacity.count);
        // The parent already enforces the aggregate byte ceiling across event
        // and client-index children. Giving each child the parent's byte
        // ceiling avoids introducing an unrelated per-item size limit when
        // the configured count limit is intentionally small.
        let event_bytes = parent_budget.limit().capacity.bytes;
        let event_rate = u64::try_from(max_pending_event_count).unwrap_or(u64::MAX).max(1);
        let event_budget = parent_budget.child(
            "lite-events",
            BudgetLimit::new(max_pending_event_count, event_bytes, FullPolicy::DropStale)
                .with_rate(RateLimit::new(event_rate, event_rate))
                .with_max_age(MAX_EVENT_AGE),
        )?;
        let client_access_bytes = parent_budget.limit().capacity.bytes;
        let client_rate = u64::try_from(max_tracked_client_count).unwrap_or(u64::MAX).max(1);
        let client_access_budget = parent_budget.child(
            "lite-client-access",
            BudgetLimit::new(max_tracked_client_count, client_access_bytes, FullPolicy::DropStale)
                .with_rate(RateLimit::new(client_rate, client_rate))
                .with_max_age(MAX_CLIENT_IDLE_AGE),
        )?;
        Ok(Self {
            client_events: Arc::new(DashMap::new()),
            deferred_dispatches: Arc::new(DashMap::new()),
            client_access: Arc::new(DashMap::new()),
            wakeup_notify: Arc::new(Mutex::new(None)),
            event_budget,
            client_access_budget,
            next_reservation_id: Arc::new(AtomicU64::new(1)),
            reservation_metrics: Arc::new(LiteEventReservationMetrics::default()),
        })
    }

    pub(crate) fn budget_snapshot(&self) -> BudgetSnapshot {
        self.event_budget.snapshot()
    }

    pub(crate) fn client_access_budget_snapshot(&self) -> BudgetSnapshot {
        self.client_access_budget.snapshot()
    }

    pub(crate) fn set_wakeup_notify(&self, wakeup_notify: Arc<Notify>) {
        *self.wakeup_notify.lock().expect("lite wakeup notify lock poisoned") = Some(wakeup_notify);
    }

    pub(crate) fn clear_wakeup_notify(&self) {
        self.wakeup_notify
            .lock()
            .expect("lite wakeup notify lock poisoned")
            .take();
    }

    pub(crate) fn touch_client(&self, client_id: &CheetahString) -> bool {
        let last_access_time = current_millis();
        let last_touch = self.client_access_budget.monotonic_now();
        if let Some(mut state) = self.client_access.get_mut(client_id) {
            state.last_access_time = last_access_time;
            state.last_touch = last_touch;
            return true;
        }

        self.prune_stale_client_access(last_touch);
        let Ok(permit) = self
            .client_access_budget
            .try_acquire_data(client_access_retained_bytes(client_id))
        else {
            return false;
        };
        match self.client_access.entry(client_id.clone()) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().last_access_time = last_access_time;
                entry.get_mut().last_touch = last_touch;
            }
            Entry::Vacant(entry) => {
                entry.insert(ClientAccessState {
                    last_access_time,
                    last_touch,
                    _permit: permit,
                });
            }
        }
        true
    }

    pub(crate) fn get_client_last_access_time(&self, client_id: &CheetahString) -> u64 {
        self.client_access
            .get(client_id)
            .map(|entry| entry.last_access_time)
            .unwrap_or(0)
    }

    pub(crate) fn event_map_size(&self) -> usize {
        self.client_events.len()
    }

    pub(crate) fn do_full_dispatch(
        &self,
        client_id: &CheetahString,
        group: &CheetahString,
        lmq_names: &HashSet<CheetahString>,
    ) -> usize {
        self.do_full_dispatch_with_limit(client_id, group, lmq_names, DEFAULT_CLIENT_EVENT_LIMIT, 0)
    }

    pub(crate) fn do_full_dispatch_with_limit(
        &self,
        client_id: &CheetahString,
        group: &CheetahString,
        lmq_names: &HashSet<CheetahString>,
        max_event_count: usize,
        full_dispatch_delay_millis: u64,
    ) -> usize {
        let now = current_millis();
        let enqueued_at = self.event_budget.monotonic_now();
        self.scan(now);
        if !self.touch_client(client_id) {
            return 0;
        }
        if lmq_names.is_empty() {
            return 0;
        }

        let deferred_snapshot = self
            .deferred_dispatches
            .get(client_id)
            .map(|entry| entry.events.clone())
            .unwrap_or_default();
        let mut overflow = HashMap::new();
        let ordered_lmq_names = lmq_names.iter().cloned().collect::<BTreeSet<_>>();
        let inserted = {
            let mut entry = self.client_events.entry(client_id.clone()).or_default();
            entry.group = group.clone();
            entry.set_limit(max_event_count);
            let original_len = entry.events.len();
            for lmq_name in ordered_lmq_names {
                if entry.event_set.contains(&lmq_name) || deferred_snapshot.contains(&lmq_name) {
                    if entry.committed_events.contains(&lmq_name) {
                        entry.committed_redispatches.insert(lmq_name);
                    }
                    self.event_budget.record_coalesced(1);
                    continue;
                }
                let Ok(permit) = self.event_budget.try_acquire_data(event_retained_bytes(&lmq_name)) else {
                    continue;
                };
                let permit = Arc::new(permit);
                if !entry.offer(lmq_name.clone(), Arc::clone(&permit), enqueued_at) {
                    overflow.insert(lmq_name, permit);
                }
            }
            entry.events.len().saturating_sub(original_len)
        };

        if !overflow.is_empty() {
            self.schedule_deferred_dispatch(
                client_id,
                group,
                &overflow,
                now.saturating_add(full_dispatch_delay_millis),
                full_dispatch_delay_millis,
                max_event_count,
                enqueued_at,
            );
        }

        if inserted > 0 {
            self.notify_client(client_id);
        }
        inserted
    }

    pub(crate) fn do_full_dispatch_by_group(
        &self,
        group: &CheetahString,
        dispatch_map: &HashMap<CheetahString, HashSet<CheetahString>>,
    ) -> usize {
        self.do_full_dispatch_by_group_with_limit(group, dispatch_map, DEFAULT_CLIENT_EVENT_LIMIT, 0)
    }

    pub(crate) fn do_full_dispatch_by_group_with_limit(
        &self,
        group: &CheetahString,
        dispatch_map: &HashMap<CheetahString, HashSet<CheetahString>>,
        max_event_count: usize,
        full_dispatch_delay_millis: u64,
    ) -> usize {
        dispatch_map
            .iter()
            .map(|(client_id, lmq_names)| {
                self.do_full_dispatch_with_limit(
                    client_id,
                    group,
                    lmq_names,
                    max_event_count,
                    full_dispatch_delay_millis,
                )
            })
            .sum()
    }

    pub(crate) fn pending_events(&self, client_id: &CheetahString) -> Vec<CheetahString> {
        self.scan(current_millis());
        self.client_events
            .get(client_id)
            .map(|entry| entry.pending_events())
            .unwrap_or_default()
    }

    pub(crate) fn pending_client_ids(&self) -> Vec<CheetahString> {
        self.scan(current_millis());
        self.client_events
            .iter()
            .filter(|entry| !entry.is_empty())
            .map(|entry| entry.key().clone())
            .collect()
    }

    pub(crate) fn take_pending_events(&self, client_id: &CheetahString) -> Vec<CheetahString> {
        let Some(reservation) = self.reserve_pending_events(client_id) else {
            return Vec::new();
        };
        let batch = reservation.commit();
        let events = batch.event_names();
        batch.complete(&HashSet::new());
        events
    }

    fn scan(&self, now: u64) {
        self.drop_stale_events(self.event_budget.monotonic_now());
        self.prune_stale_client_access(self.client_access_budget.monotonic_now());
        let due_clients = self
            .deferred_dispatches
            .iter()
            .filter(|entry| entry.due_time <= now)
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();

        for client_id in due_clients {
            self.promote_deferred_events(&client_id, now, false);
            self.cleanup_client_state(&client_id);
        }
    }

    fn drop_stale_events(&self, now: Duration) {
        let max_age = self.event_budget.limit().max_age.unwrap_or(MAX_EVENT_AGE);
        let mut dropped = 0;
        for mut entry in self.client_events.iter_mut() {
            dropped += entry.drop_stale(now, max_age);
        }
        for mut entry in self.deferred_dispatches.iter_mut() {
            dropped += entry.drop_stale(now, max_age);
        }
        let empty_deferred = self
            .deferred_dispatches
            .iter()
            .filter(|entry| entry.events.is_empty())
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for client_id in empty_deferred {
            self.deferred_dispatches
                .remove_if(&client_id, |_, state| state.events.is_empty());
        }
        let empty_clients = self
            .client_events
            .iter()
            .filter(|entry| entry.is_fully_empty() && !self.deferred_dispatches.contains_key(entry.key()))
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for client_id in empty_clients {
            self.client_events
                .remove_if(&client_id, |_, state| state.is_fully_empty());
        }
        if dropped > 0 {
            self.event_budget.record_dropped(dropped);
        }
    }

    fn prune_stale_client_access(&self, now: Duration) {
        let max_age = self.client_access_budget.limit().max_age.unwrap_or(MAX_CLIENT_IDLE_AGE);
        let stale_clients = self
            .client_access
            .iter()
            .filter(|entry| now.saturating_sub(entry.last_touch) >= max_age)
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        if stale_clients.is_empty() {
            return;
        }
        let mut removed = 0;
        for client_id in stale_clients {
            if self
                .client_access
                .remove_if(&client_id, |_, state| now.saturating_sub(state.last_touch) >= max_age)
                .is_some()
            {
                removed += 1;
            }
        }
        self.client_access_budget.record_dropped(removed);
    }

    fn promote_deferred_events(&self, client_id: &CheetahString, now: u64, force: bool) -> usize {
        let Some(mut state) = self.deferred_dispatches.get_mut(client_id) else {
            return 0;
        };
        if !force && state.due_time > now {
            return 0;
        }

        let inserted = {
            let mut entry = self.client_events.entry(client_id.clone()).or_default();
            entry.group = state.group.clone();
            entry.set_limit(state.max_event_count);
            let mut promoted = 0;
            let candidates = state.events.iter().cloned().collect::<Vec<_>>();
            for lmq_name in candidates {
                if entry.event_set.contains(&lmq_name) {
                    if entry.committed_events.contains(&lmq_name) {
                        entry.committed_redispatches.insert(lmq_name.clone());
                    }
                    state.events.remove(&lmq_name);
                    state.permits.remove(&lmq_name);
                    state.enqueued_at.remove(&lmq_name);
                    self.event_budget.record_coalesced(1);
                    continue;
                }
                let permit = state
                    .permits
                    .get(&lmq_name)
                    .cloned()
                    .expect("deferred Lite event owns its resource permit");
                let enqueued_at = *state
                    .enqueued_at
                    .get(&lmq_name)
                    .expect("deferred Lite event owns its enqueue timestamp");
                if entry.offer(lmq_name.clone(), permit, enqueued_at) {
                    state.events.remove(&lmq_name);
                    state.permits.remove(&lmq_name);
                    state.enqueued_at.remove(&lmq_name);
                    promoted += 1;
                } else {
                    break;
                }
            }
            promoted
        };

        if state.events.is_empty() {
            drop(state);
            self.deferred_dispatches
                .remove_if(client_id, |_, current| current.events.is_empty());
        } else {
            state.due_time = now.saturating_add(state.delay_millis);
        }

        if inserted > 0 {
            self.notify_client(client_id);
        }
        inserted
    }

    fn schedule_deferred_dispatch(
        &self,
        client_id: &CheetahString,
        group: &CheetahString,
        lmq_names: &HashMap<CheetahString, Arc<ResourcePermit>>,
        due_time: u64,
        delay_millis: u64,
        max_event_count: usize,
        enqueued_at: Duration,
    ) {
        let mut entry = self.deferred_dispatches.entry(client_id.clone()).or_default();
        entry.group = group.clone();
        entry.delay_millis = delay_millis;
        entry.max_event_count = normalize_limit(max_event_count);
        entry.due_time = if entry.events.is_empty() {
            due_time
        } else {
            entry.due_time.min(due_time)
        };
        let available = entry.max_event_count.saturating_sub(entry.events.len());
        let mut retained = 0;
        let mut coalesced = 0;
        for (lmq_name, permit) in lmq_names {
            let pending_in_client = self.client_events.get_mut(client_id).is_some_and(|mut state| {
                let pending = state.event_set.contains(lmq_name);
                if pending && state.committed_events.contains(lmq_name) {
                    state.committed_redispatches.insert(lmq_name.clone());
                }
                pending
            });
            let already_pending = entry.events.contains(lmq_name) || pending_in_client;
            if already_pending {
                coalesced += 1;
                continue;
            }
            if retained >= available {
                continue;
            }
            if entry.events.insert(lmq_name.clone()) {
                entry.permits.insert(lmq_name.clone(), Arc::clone(permit));
                entry.enqueued_at.insert(lmq_name.clone(), enqueued_at);
                retained += 1;
            }
        }
        self.event_budget.record_coalesced(coalesced);
        self.event_budget
            .record_dropped(lmq_names.len().saturating_sub(retained).saturating_sub(coalesced));
    }

    fn cleanup_client_state(&self, client_id: &CheetahString) {
        let has_deferred = self
            .deferred_dispatches
            .get(client_id)
            .is_some_and(|entry| !entry.events.is_empty());
        let should_remove = self
            .client_events
            .get(client_id)
            .is_some_and(|entry| entry.is_fully_empty() && !has_deferred);
        if should_remove {
            self.client_events
                .remove_if(client_id, |_, state| state.is_fully_empty());
        }
    }

    fn notify_client(&self, _client_id: &CheetahString) {
        let wakeup_notify = self
            .wakeup_notify
            .lock()
            .expect("lite wakeup notify lock poisoned")
            .clone();
        if let Some(wakeup_notify) = wakeup_notify {
            wakeup_notify.notify_one();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;

    use super::*;
    use rocketmq_runtime::MonotonicClock;

    #[derive(Default)]
    struct ManualClock {
        millis: AtomicU64,
    }

    impl ManualClock {
        fn advance(&self, duration: Duration) {
            self.millis.fetch_add(
                duration.as_millis().try_into().expect("test duration fits u64"),
                Ordering::AcqRel,
            );
        }
    }

    impl MonotonicClock for ManualClock {
        fn now(&self) -> Duration {
            Duration::from_millis(self.millis.load(Ordering::Acquire))
        }
    }

    #[test]
    fn touch_client_updates_last_access_without_creating_event_state() {
        let dispatcher = LiteEventDispatcher::default();
        let client_id = CheetahString::from_static_str("client-a");

        dispatcher.touch_client(&client_id);

        assert!(dispatcher.get_client_last_access_time(&client_id) > 0);
        assert_eq!(dispatcher.event_map_size(), 0);
    }

    #[test]
    fn do_full_dispatch_deduplicates_events_per_client() {
        let dispatcher = LiteEventDispatcher::default();
        let client_id = CheetahString::from_static_str("client-a");
        let group = CheetahString::from_static_str("group-a");
        let lmq_names = HashSet::from([
            CheetahString::from_static_str("%LMQ%$parent$child-a"),
            CheetahString::from_static_str("%LMQ%$parent$child-b"),
        ]);

        let inserted = dispatcher.do_full_dispatch(&client_id, &group, &lmq_names);
        let inserted_again = dispatcher.do_full_dispatch(&client_id, &group, &lmq_names);

        assert_eq!(inserted, 2);
        assert_eq!(inserted_again, 0);
        assert_eq!(dispatcher.event_map_size(), 1);
        assert_eq!(dispatcher.pending_events(&client_id).len(), 2);
        assert!(dispatcher.get_client_last_access_time(&client_id) > 0);
    }

    #[test]
    fn take_pending_events_drains_client_event_state() {
        let dispatcher = LiteEventDispatcher::default();
        let client_id = CheetahString::from_static_str("client-a");
        let group = CheetahString::from_static_str("group-a");
        let lmq_names = HashSet::from([CheetahString::from_static_str("%LMQ%$parent$child-a")]);

        dispatcher.do_full_dispatch(&client_id, &group, &lmq_names);

        assert_eq!(
            dispatcher.take_pending_events(&client_id),
            vec![CheetahString::from_static_str("%LMQ%$parent$child-a")]
        );
        assert_eq!(dispatcher.event_map_size(), 0);
    }

    #[test]
    fn bounded_dispatch_defers_overflow_and_releases_it_on_consume() {
        let dispatcher = LiteEventDispatcher::default();
        let client_id = CheetahString::from_static_str("client-a");
        let group = CheetahString::from_static_str("group-a");
        let lmq_names = HashSet::from([
            CheetahString::from_static_str("%LMQ%$parent$child-a"),
            CheetahString::from_static_str("%LMQ%$parent$child-b"),
            CheetahString::from_static_str("%LMQ%$parent$child-c"),
        ]);

        let inserted = dispatcher.do_full_dispatch_with_limit(&client_id, &group, &lmq_names, 2, 10_000);

        assert_eq!(inserted, 2);
        assert_eq!(
            dispatcher.pending_events(&client_id),
            vec![
                CheetahString::from_static_str("%LMQ%$parent$child-a"),
                CheetahString::from_static_str("%LMQ%$parent$child-b"),
            ]
        );
        assert_eq!(
            dispatcher.take_pending_events(&client_id),
            vec![
                CheetahString::from_static_str("%LMQ%$parent$child-a"),
                CheetahString::from_static_str("%LMQ%$parent$child-b"),
                CheetahString::from_static_str("%LMQ%$parent$child-c"),
            ]
        );
        assert!(dispatcher.pending_events(&client_id).is_empty());
        assert_eq!(dispatcher.budget_snapshot().current_count, 0);
    }

    #[test]
    fn overload_retains_no_more_than_the_global_event_budget() {
        let tree = ResourceBudgetTree::new(
            "broker-lite-event-overload",
            BudgetLimit::new(8, 4096, FullPolicy::Reject),
        )
        .expect("root budget");
        let dispatcher = LiteEventDispatcher::try_with_resource_budget(&tree.root(), 4, 4).expect("event budget");
        let client_id = CheetahString::from_static_str("client-a");
        let group = CheetahString::from_static_str("group-a");
        let lmq_names = (0..8)
            .map(|index| CheetahString::from(format!("%LMQ%$parent$child-{index}")))
            .collect::<HashSet<_>>();

        let inserted = dispatcher.do_full_dispatch_with_limit(&client_id, &group, &lmq_names, 2, 10_000);

        assert_eq!(inserted, 2);
        assert_eq!(dispatcher.pending_events(&client_id).len(), 2);
        assert_eq!(dispatcher.budget_snapshot().current_count, 4);
        assert_eq!(dispatcher.budget_snapshot().rejected_count, 4);
    }

    #[test]
    fn stale_events_release_permits_and_increment_drop_metrics() {
        let clock = Arc::new(ManualClock::default());
        let tree = ResourceBudgetTree::with_clock(
            "broker-lite-event-age",
            BudgetLimit::new(4, 4096, FullPolicy::Reject),
            clock.clone(),
        )
        .expect("root budget");
        let dispatcher = LiteEventDispatcher::try_with_resource_budget(&tree.root(), 2, 2).expect("event budget");
        let client_id = CheetahString::from_static_str("client-a");
        let group = CheetahString::from_static_str("group-a");
        let lmq_names = HashSet::from([CheetahString::from_static_str("%LMQ%$parent$child-a")]);

        assert_eq!(
            dispatcher.do_full_dispatch_with_limit(&client_id, &group, &lmq_names, 1, 10_000),
            1
        );
        clock.advance(Duration::from_secs(31));

        assert!(dispatcher.pending_events(&client_id).is_empty());
        assert_eq!(dispatcher.budget_snapshot().current_count, 0);
        assert_eq!(dispatcher.budget_snapshot().dropped_count, 1);
    }

    #[test]
    fn overload_bounds_the_client_access_index() {
        let tree = ResourceBudgetTree::new(
            "broker-lite-client-access-overload",
            BudgetLimit::new(4, 4096, FullPolicy::Reject),
        )
        .expect("root budget");
        let dispatcher = LiteEventDispatcher::try_with_resource_budget(&tree.root(), 2, 2).expect("event budget");
        let clients = (0..4)
            .map(|index| CheetahString::from(format!("client-{index}")))
            .collect::<Vec<_>>();

        for client_id in &clients {
            let lmq_names = HashSet::from([CheetahString::from(format!("%LMQ%$parent${client_id}"))]);
            dispatcher.do_full_dispatch(client_id, &CheetahString::from_static_str("group-a"), &lmq_names);
        }

        assert!(dispatcher.get_client_last_access_time(&clients[0]) > 0);
        assert!(dispatcher.get_client_last_access_time(&clients[1]) > 0);
        assert_eq!(dispatcher.get_client_last_access_time(&clients[2]), 0);
        assert_eq!(dispatcher.get_client_last_access_time(&clients[3]), 0);
        assert_eq!(dispatcher.client_access_budget_snapshot().current_count, 2);
        assert_eq!(dispatcher.client_access_budget_snapshot().rejected_count, 2);
        assert_eq!(dispatcher.event_map_size(), 2);
    }

    #[test]
    fn client_access_capacity_is_independent_from_event_capacity() {
        let tree = ResourceBudgetTree::new(
            "broker-lite-client-access-capacity",
            BudgetLimit::new(8, 4096, FullPolicy::Reject),
        )
        .expect("root budget");
        let dispatcher = LiteEventDispatcher::try_with_resource_budget(&tree.root(), 2, 4).expect("event budget");
        let clients = (0..5)
            .map(|index| CheetahString::from(format!("client-{index}")))
            .collect::<Vec<_>>();

        for client_id in clients.iter().take(4) {
            assert!(dispatcher.touch_client(client_id));
        }
        assert!(!dispatcher.touch_client(&clients[4]));

        assert_eq!(dispatcher.client_access_budget_snapshot().current_count, 4);
        assert_eq!(dispatcher.client_access_budget_snapshot().rejected_count, 1);
    }

    #[tokio::test]
    async fn do_full_dispatch_notifies_registered_wakeup_signal() {
        let dispatcher = LiteEventDispatcher::default();
        let wakeup_notify = Arc::new(Notify::new());
        dispatcher.set_wakeup_notify(wakeup_notify.clone());
        let client_id = CheetahString::from_static_str("client-a");
        let group = CheetahString::from_static_str("group-a");
        let lmq_names = HashSet::from([CheetahString::from_static_str("%LMQ%$parent$child-a")]);

        dispatcher.do_full_dispatch(&client_id, &group, &lmq_names);

        tokio::time::timeout(Duration::from_secs(1), wakeup_notify.notified())
            .await
            .expect("Lite event should wake the long-poll scanner");
        assert_eq!(dispatcher.pending_client_ids(), vec![client_id]);
    }
}
