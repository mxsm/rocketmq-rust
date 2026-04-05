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
use std::sync::Arc;
use std::sync::Mutex;

use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_common::TimeUtils::current_millis;
use tokio::sync::mpsc::UnboundedSender;

#[derive(Debug, Clone)]
struct ClientEventState {
    group: CheetahString,
    events: VecDeque<CheetahString>,
    event_set: HashSet<CheetahString>,
    max_event_count: usize,
}

impl Default for ClientEventState {
    fn default() -> Self {
        Self {
            group: CheetahString::new(),
            events: VecDeque::new(),
            event_set: HashSet::new(),
            max_event_count: usize::MAX,
        }
    }
}

impl ClientEventState {
    fn set_limit(&mut self, max_event_count: usize) {
        self.max_event_count = normalize_limit(max_event_count);
    }

    fn offer(&mut self, event: CheetahString) -> bool {
        if self.event_set.contains(&event) {
            return true;
        }
        if self.events.len() >= self.max_event_count {
            return false;
        }
        self.event_set.insert(event.clone());
        self.events.push_back(event);
        true
    }

    fn drain(&mut self) -> Vec<CheetahString> {
        let drained = self.events.drain(..).collect::<Vec<_>>();
        self.event_set.clear();
        drained
    }

    fn pending_events(&self) -> Vec<CheetahString> {
        self.events.iter().cloned().collect()
    }

    fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

#[derive(Debug, Clone, Default)]
struct DeferredDispatchState {
    group: CheetahString,
    events: BTreeSet<CheetahString>,
    due_time: u64,
    delay_millis: u64,
    max_event_count: usize,
}

fn normalize_limit(max_event_count: usize) -> usize {
    if max_event_count == 0 {
        usize::MAX
    } else {
        max_event_count
    }
}

#[derive(Clone, Default)]
pub(crate) struct LiteEventDispatcher {
    client_events: Arc<DashMap<CheetahString, ClientEventState>>,
    deferred_dispatches: Arc<DashMap<CheetahString, DeferredDispatchState>>,
    client_last_access_time: Arc<DashMap<CheetahString, u64>>,
    wakeup_sender: Arc<Mutex<Option<UnboundedSender<CheetahString>>>>,
}

impl LiteEventDispatcher {
    pub(crate) fn set_wakeup_sender(&self, wakeup_sender: UnboundedSender<CheetahString>) {
        *self.wakeup_sender.lock().expect("lite wakeup sender lock poisoned") = Some(wakeup_sender);
    }

    pub(crate) fn touch_client(&self, client_id: &CheetahString) {
        self.client_last_access_time.insert(client_id.clone(), current_millis());
    }

    pub(crate) fn get_client_last_access_time(&self, client_id: &CheetahString) -> u64 {
        self.client_last_access_time
            .get(client_id)
            .map(|entry| *entry.value())
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
        self.do_full_dispatch_with_limit(client_id, group, lmq_names, usize::MAX, 0)
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
        self.scan(now);
        self.touch_client(client_id);
        if lmq_names.is_empty() {
            return 0;
        }

        let deferred_snapshot = self
            .deferred_dispatches
            .get(client_id)
            .map(|entry| entry.events.clone())
            .unwrap_or_default();
        let mut overflow = BTreeSet::new();
        let ordered_lmq_names = lmq_names.iter().cloned().collect::<BTreeSet<_>>();
        let inserted = {
            let mut entry = self.client_events.entry(client_id.clone()).or_default();
            entry.group = group.clone();
            entry.set_limit(max_event_count);
            let original_len = entry.events.len();
            for lmq_name in ordered_lmq_names {
                if entry.event_set.contains(&lmq_name) || deferred_snapshot.contains(&lmq_name) {
                    continue;
                }
                if !entry.offer(lmq_name.clone()) {
                    overflow.insert(lmq_name);
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
        self.do_full_dispatch_by_group_with_limit(group, dispatch_map, usize::MAX, 0)
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

    pub(crate) fn take_pending_events(&self, client_id: &CheetahString) -> Vec<CheetahString> {
        let now = current_millis();
        self.scan(now);
        self.touch_client(client_id);

        let mut drained = self.drain_client_events(client_id);
        if self.promote_deferred_events(client_id, now, true) > 0 {
            drained.extend(self.drain_client_events(client_id));
        }
        self.cleanup_client_state(client_id);
        drained
    }

    fn scan(&self, now: u64) {
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

    fn drain_client_events(&self, client_id: &CheetahString) -> Vec<CheetahString> {
        self.client_events
            .get_mut(client_id)
            .map(|mut entry| entry.drain())
            .unwrap_or_default()
    }

    fn promote_deferred_events(&self, client_id: &CheetahString, now: u64, force: bool) -> usize {
        let Some(snapshot) = self
            .deferred_dispatches
            .get(client_id)
            .map(|entry| entry.value().clone())
        else {
            return 0;
        };
        if !force && snapshot.due_time > now {
            return 0;
        }

        let mut state = snapshot;
        let inserted = {
            let mut entry = self.client_events.entry(client_id.clone()).or_default();
            entry.group = state.group.clone();
            entry.set_limit(state.max_event_count);
            let mut promoted = 0;
            let candidates = state.events.iter().cloned().collect::<Vec<_>>();
            for lmq_name in candidates {
                if entry.event_set.contains(&lmq_name) {
                    state.events.remove(&lmq_name);
                    continue;
                }
                if entry.offer(lmq_name.clone()) {
                    state.events.remove(&lmq_name);
                    promoted += 1;
                } else {
                    break;
                }
            }
            promoted
        };

        if state.events.is_empty() {
            self.deferred_dispatches.remove(client_id);
        } else {
            state.due_time = now.saturating_add(state.delay_millis);
            self.deferred_dispatches.insert(client_id.clone(), state);
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
        lmq_names: &BTreeSet<CheetahString>,
        due_time: u64,
        delay_millis: u64,
        max_event_count: usize,
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
        entry.events.extend(lmq_names.iter().cloned());
    }

    fn cleanup_client_state(&self, client_id: &CheetahString) {
        let has_deferred = self
            .deferred_dispatches
            .get(client_id)
            .is_some_and(|entry| !entry.events.is_empty());
        let should_remove = self
            .client_events
            .get(client_id)
            .is_some_and(|entry| entry.is_empty() && !has_deferred);
        if should_remove {
            self.client_events.remove(client_id);
        }
    }

    fn notify_client(&self, client_id: &CheetahString) {
        let sender = self
            .wakeup_sender
            .lock()
            .expect("lite wakeup sender lock poisoned")
            .clone();
        if let Some(sender) = sender {
            let _ = sender.send(client_id.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    }

    #[tokio::test]
    async fn do_full_dispatch_notifies_registered_wakeup_sender() {
        let dispatcher = LiteEventDispatcher::default();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        dispatcher.set_wakeup_sender(sender);
        let client_id = CheetahString::from_static_str("client-a");
        let group = CheetahString::from_static_str("group-a");
        let lmq_names = HashSet::from([CheetahString::from_static_str("%LMQ%$parent$child-a")]);

        dispatcher.do_full_dispatch(&client_id, &group, &lmq_names);

        assert_eq!(receiver.recv().await, Some(client_id));
    }
}
