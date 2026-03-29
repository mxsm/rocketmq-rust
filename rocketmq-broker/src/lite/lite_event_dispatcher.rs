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
use std::sync::Arc;
use std::sync::Mutex;

use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_common::TimeUtils::current_millis;
use tokio::sync::mpsc::UnboundedSender;

#[derive(Debug, Clone, Default)]
struct ClientEventState {
    group: CheetahString,
    events: BTreeSet<CheetahString>,
}

#[derive(Clone, Default)]
pub(crate) struct LiteEventDispatcher {
    client_events: Arc<DashMap<CheetahString, ClientEventState>>,
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
        self.touch_client(client_id);
        if lmq_names.is_empty() {
            return 0;
        }

        let inserted = {
            let mut entry = self.client_events.entry(client_id.clone()).or_default();
            entry.group = group.clone();
            let original_len = entry.events.len();
            entry.events.extend(lmq_names.iter().cloned());
            entry.events.len().saturating_sub(original_len)
        };
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
        dispatch_map
            .iter()
            .map(|(client_id, lmq_names)| self.do_full_dispatch(client_id, group, lmq_names))
            .sum()
    }

    pub(crate) fn pending_events(&self, client_id: &CheetahString) -> Vec<CheetahString> {
        self.client_events
            .get(client_id)
            .map(|entry| entry.events.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub(crate) fn take_pending_events(&self, client_id: &CheetahString) -> Vec<CheetahString> {
        self.client_events
            .remove(client_id)
            .map(|(_, entry)| entry.events.into_iter().collect())
            .unwrap_or_default()
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
