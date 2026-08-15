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
use std::sync::Arc;

use parking_lot::Mutex;

#[derive(Default)]
struct LmqQuotaState {
    committed: HashSet<String>,
    inflight: HashMap<String, usize>,
}

#[derive(Default)]
pub(super) struct LmqQuotaController {
    state: Mutex<LmqQuotaState>,
}

impl LmqQuotaController {
    pub(super) fn reserve(
        self: &Arc<Self>,
        queue_keys: &[String],
        existing_queue_keys: impl IntoIterator<Item = String>,
        max_lmq_count: usize,
    ) -> Option<LmqQuotaReservation> {
        let mut state = self.state.lock();
        state.committed.extend(existing_queue_keys);

        let unique_queue_keys: HashSet<&str> = queue_keys.iter().map(String::as_str).collect();
        let new_unique_count = unique_queue_keys
            .iter()
            .filter(|queue_key| !state.committed.contains(**queue_key) && !state.inflight.contains_key(**queue_key))
            .count();
        let occupied = state.committed.len()
            + state
                .inflight
                .keys()
                .filter(|key| !state.committed.contains(*key))
                .count();
        if new_unique_count > max_lmq_count.saturating_sub(occupied) {
            return None;
        }

        let reserved = unique_queue_keys
            .into_iter()
            .filter(|queue_key| !state.committed.contains(*queue_key))
            .map(str::to_owned)
            .collect::<Vec<_>>();
        for queue_key in &reserved {
            *state.inflight.entry(queue_key.clone()).or_default() += 1;
        }
        drop(state);

        Some(LmqQuotaReservation {
            controller: Arc::clone(self),
            queue_keys: reserved,
            committed: false,
        })
    }

    fn finish(&self, queue_keys: &[String], commit: bool) {
        let mut state = self.state.lock();
        for queue_key in queue_keys {
            if commit {
                state.committed.insert(queue_key.clone());
            }
            if let Some(count) = state.inflight.get_mut(queue_key) {
                *count -= 1;
                if *count == 0 {
                    state.inflight.remove(queue_key);
                }
            }
        }
    }
}

pub(super) struct LmqQuotaReservation {
    controller: Arc<LmqQuotaController>,
    queue_keys: Vec<String>,
    committed: bool,
}

impl LmqQuotaReservation {
    pub(super) fn commit(mut self) {
        self.controller.finish(&self.queue_keys, true);
        self.committed = true;
    }
}

impl Drop for LmqQuotaReservation {
    fn drop(&mut self) {
        if !self.committed {
            self.controller.finish(&self.queue_keys, false);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concurrent_unique_reservations_cannot_exceed_limit_and_drop_releases_capacity() {
        let controller = Arc::new(LmqQuotaController::default());
        let alpha = controller
            .reserve(&["%LMQ%alpha-0".to_owned()], Vec::new(), 1)
            .expect("first unique name fits");
        assert!(controller.reserve(&["%LMQ%beta-0".to_owned()], Vec::new(), 1).is_none());

        drop(alpha);
        assert!(controller.reserve(&["%LMQ%beta-0".to_owned()], Vec::new(), 1).is_some());
    }

    #[test]
    fn concurrent_same_name_counts_once_and_existing_name_is_allowed_at_zero_limit() {
        let controller = Arc::new(LmqQuotaController::default());
        let first = controller
            .reserve(&["%LMQ%alpha-0".to_owned()], Vec::new(), 1)
            .expect("first reservation");
        let duplicate = controller
            .reserve(&["%LMQ%alpha-0".to_owned()], Vec::new(), 1)
            .expect("same inflight name does not consume another quota slot");
        first.commit();
        drop(duplicate);

        assert!(controller
            .reserve(&["%LMQ%alpha-0".to_owned()], vec!["%LMQ%alpha-0".to_owned()], 0,)
            .is_some());
    }
}
