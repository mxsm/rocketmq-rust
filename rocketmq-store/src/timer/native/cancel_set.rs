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

use std::collections::BTreeMap;
use std::collections::HashMap;

use rocketmq_store_local::timer::native_format::native_key_hash;
use rocketmq_store_local::timer::native_format::NativeCancelRecord;
use rocketmq_store_local::timer::native_format::NativeIndexEntry;

/// A recall that may cancel a timer. Its key and generation are checked against the timer's
/// message once delivery has read it, as the Java-compatible engine checks a slot's recalls.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct NativeRecall {
    pub(crate) key: Vec<u8>,
    pub(crate) generation: u64,
}

/// Recalls by the hash of each unique key they may name.
///
/// A recall carries the deadline of the timer it cancels, but that timer may be indexed later
/// than its deadline: one that reaches the engine after its lane has delivered past its
/// deadline is moved up to the lane's watermark. So a recall is matched by key alone, and its
/// key and generation decide at delivery.
///
/// A recall registered ahead of its tombstone is held until the tombstone is indexed, or until
/// the registration is withdrawn because the tombstone could not be written.
#[derive(Default)]
pub(crate) struct CancelSet {
    by_key: HashMap<u64, Vec<NativeRecall>>,
    recalls: HashMap<NativeRecall, RecallHolds>,
    /// Indexed recalls by the time after which they can no longer cancel anything.
    by_expiry: BTreeMap<i64, Vec<NativeRecall>>,
}

struct RecallHolds {
    /// Its tombstone is indexed, so it stays until it expires.
    logged: bool,
    /// Registrations whose tombstones are still on their way.
    registered: usize,
    /// The time after which the timer it names is no longer indexed in the future.
    expires_ms: i64,
}

impl Default for RecallHolds {
    fn default() -> Self {
        Self {
            logged: false,
            registered: 0,
            expires_ms: i64::MIN,
        }
    }
}

impl CancelSet {
    /// Adds a recall whose tombstone is indexed. The timer it names was indexed before it, at
    /// a time no later than `expires_ms`, so once every timer due by then is delivered the
    /// recall can be dropped. Adding the same recall again keeps the later expiry.
    pub(crate) fn insert(&mut self, record: &NativeCancelRecord, expires_ms: i64) {
        let recall = NativeRecall {
            key: record.key.clone(),
            generation: record.generation,
        };
        let holds = self.add(recall.clone());
        holds.logged = true;
        if expires_ms > holds.expires_ms {
            holds.expires_ms = expires_ms;
            self.by_expiry.entry(expires_ms).or_default().push(recall);
        }
    }

    /// Drops the indexed recalls that expire before `through_ms`, unless a registration still
    /// holds one.
    pub(crate) fn expire(&mut self, through_ms: i64) {
        let live = self.by_expiry.split_off(&through_ms);
        let expired = std::mem::replace(&mut self.by_expiry, live);
        for recall in expired.into_values().flatten() {
            let Some(holds) = self.recalls.get(&recall) else {
                continue;
            };
            if holds.registered == 0 && holds.expires_ms < through_ms {
                self.remove(&recall);
            }
        }
    }

    /// Holds a recall registered ahead of its tombstone.
    pub(crate) fn hold(&mut self, recall: NativeRecall) {
        self.add(recall).registered += 1;
    }

    /// Withdraws a registration of `recall` whose tombstone could not be written. The recall
    /// stays while its tombstone is indexed or another registration holds it.
    pub(crate) fn release(&mut self, recall: &NativeRecall) {
        let Some(holds) = self.recalls.get_mut(recall) else {
            return;
        };
        holds.registered = holds.registered.saturating_sub(1);
        if holds.logged || holds.registered > 0 {
            return;
        }
        self.remove(recall);
    }

    fn remove(&mut self, recall: &NativeRecall) {
        self.recalls.remove(recall);
        for unique_key in named_unique_keys(&recall.key) {
            let hash = native_key_hash(unique_key);
            if let Some(recalls) = self.by_key.get_mut(&hash) {
                recalls.retain(|held| held != recall);
                if recalls.is_empty() {
                    self.by_key.remove(&hash);
                }
            }
        }
    }

    /// The recalls that may cancel the timer of `entry`.
    pub(crate) fn candidates(&self, entry: &NativeIndexEntry) -> Vec<NativeRecall> {
        self.by_key.get(&entry.key_hash).cloned().unwrap_or_default()
    }

    /// Recalls held.
    pub(crate) fn len(&self) -> usize {
        self.recalls.len()
    }

    fn add(&mut self, recall: NativeRecall) -> &mut RecallHolds {
        if !self.recalls.contains_key(&recall) {
            for unique_key in named_unique_keys(&recall.key) {
                self.by_key
                    .entry(native_key_hash(unique_key))
                    .or_default()
                    .push(recall.clone());
            }
        }
        self.recalls.entry(recall).or_default()
    }
}

/// The unique keys a recall key may name. A recall key is the unique key alone, as Java writes
/// it; `<real topic>_<unique key>`; or `T:<topic length>:<topic><unique key>`. A topic may hold
/// `_`, so every suffix after a `_` is a candidate; the full check at delivery rules out the
/// wrong ones.
fn named_unique_keys(recall_key: &[u8]) -> Vec<&[u8]> {
    let mut keys = vec![recall_key];
    if let Some(unique_key) = canonical_unique_key(recall_key) {
        keys.push(unique_key);
    }
    keys.extend(
        recall_key
            .iter()
            .enumerate()
            .filter(|(_, byte)| **byte == b'_')
            .map(|(at, _)| &recall_key[at + 1..])
            .filter(|suffix| !suffix.is_empty()),
    );
    keys.sort_unstable();
    keys.dedup();
    keys
}

/// The unique key of a `T:<topic length>:<topic><unique key>` recall key.
fn canonical_unique_key(recall_key: &[u8]) -> Option<&[u8]> {
    let rest = recall_key.strip_prefix(b"T:")?;
    let colon = rest.iter().position(|byte| *byte == b':')?;
    let topic_len: usize = std::str::from_utf8(&rest[..colon]).ok()?.parse().ok()?;
    rest.get(colon + 1..)?.get(topic_len..).filter(|key| !key.is_empty())
}

#[cfg(test)]
mod tests {
    use rocketmq_store_local::timer::native_format::NativeLocator;

    use super::*;
    use crate::timer::timer_message_store::build_canonical_delete_key;
    use crate::timer::timer_message_store::build_delete_key;

    const DUE_MS: i64 = 1_700_000_000_000;
    const UNIQUE_KEY: &str = "7F00000100002A9F0000ABCD";

    fn timer(unique_key: &str, due_ms: i64) -> NativeIndexEntry {
        NativeIndexEntry {
            due_ms,
            cq_offset: 1,
            locator: NativeLocator::CommitLog { offset: 0 },
            generation: 0,
            key_hash: native_key_hash(unique_key.as_bytes()),
            size: 10,
            lane: 0,
        }
    }

    fn recall(key: &str, due_ms: i64) -> NativeCancelRecord {
        NativeCancelRecord {
            due_ms,
            tombstone_cq_offset: 9,
            generation: 0,
            key: key.as_bytes().to_vec(),
        }
    }

    #[test]
    fn every_recall_key_form_finds_its_timer() {
        for key in [
            UNIQUE_KEY.to_owned(),
            build_delete_key("Topic_A", UNIQUE_KEY).to_string(),
            build_canonical_delete_key("Topic_A", UNIQUE_KEY, true).to_string(),
        ] {
            let mut cancels = CancelSet::default();
            cancels.insert(&recall(&key, DUE_MS), DUE_MS);
            let candidates = cancels.candidates(&timer(UNIQUE_KEY, DUE_MS + 1));
            assert_eq!(candidates.len(), 1, "{key}");
            assert_eq!(candidates[0].key, key.as_bytes());
        }
    }

    #[test]
    fn a_recall_reaches_its_timer_wherever_the_timer_was_indexed() {
        let mut cancels = CancelSet::default();
        cancels.insert(&recall(UNIQUE_KEY, DUE_MS), DUE_MS);
        // A timer moved up to its lane's watermark, into a later hour, is still found.
        assert_eq!(cancels.candidates(&timer(UNIQUE_KEY, DUE_MS + 3_600_000)).len(), 1);
        assert!(cancels.candidates(&timer("another-key", DUE_MS)).is_empty());
    }

    #[test]
    fn the_same_recall_twice_is_one_recall() {
        let mut cancels = CancelSet::default();
        let key = build_delete_key("TopicA", UNIQUE_KEY).to_string();
        cancels.insert(&recall(&key, DUE_MS), DUE_MS);
        cancels.insert(&recall(&key, DUE_MS), DUE_MS);
        assert_eq!(cancels.len(), 1);
        assert_eq!(cancels.candidates(&timer(UNIQUE_KEY, DUE_MS)).len(), 1);
    }

    #[test]
    fn a_registered_recall_leaves_once_withdrawn_unless_indexed_or_held_again() {
        let held = || NativeRecall {
            key: UNIQUE_KEY.as_bytes().to_vec(),
            generation: 0,
        };
        let mut cancels = CancelSet::default();
        cancels.hold(held());
        cancels.hold(held());
        cancels.release(&held());
        assert_eq!(cancels.candidates(&timer(UNIQUE_KEY, DUE_MS)).len(), 1);
        cancels.release(&held());
        assert_eq!(cancels.len(), 0);
        assert!(cancels.candidates(&timer(UNIQUE_KEY, DUE_MS)).is_empty());

        // Once its tombstone is indexed, withdrawing a registration leaves it in place.
        cancels.hold(held());
        cancels.insert(&recall(UNIQUE_KEY, DUE_MS), DUE_MS);
        cancels.release(&held());
        assert_eq!(cancels.candidates(&timer(UNIQUE_KEY, DUE_MS)).len(), 1);
    }

    #[test]
    fn an_indexed_recall_is_dropped_once_it_expires_unless_a_registration_holds_it() {
        let mut cancels = CancelSet::default();
        cancels.insert(&recall(UNIQUE_KEY, DUE_MS), DUE_MS);
        cancels.insert(&recall("held", DUE_MS), DUE_MS);
        cancels.insert(&recall("later", DUE_MS), DUE_MS);
        // Indexed again later, it expires later.
        cancels.insert(&recall("later", DUE_MS), DUE_MS + 10);
        cancels.hold(NativeRecall {
            key: b"held".to_vec(),
            generation: 0,
        });
        cancels.expire(DUE_MS);
        assert_eq!(cancels.len(), 3);

        cancels.expire(DUE_MS + 1);
        assert!(cancels.candidates(&timer(UNIQUE_KEY, DUE_MS)).is_empty());
        assert_eq!(cancels.candidates(&timer("held", DUE_MS)).len(), 1);
        assert_eq!(cancels.candidates(&timer("later", DUE_MS)).len(), 1);
        cancels.expire(DUE_MS + 11);
        assert!(cancels.candidates(&timer("later", DUE_MS)).is_empty());
        assert_eq!(cancels.len(), 1);
    }

    #[test]
    fn a_canonical_key_names_what_follows_its_topic() {
        assert_eq!(canonical_unique_key(b"T:7:Topic_Akey"), Some(&b"key"[..]));
        assert_eq!(canonical_unique_key(b"T:9:Topic_Akey"), Some(&b"y"[..]));
        assert_eq!(canonical_unique_key(b"T:20:short"), None);
        assert_eq!(canonical_unique_key(b"Topic_key"), None);
    }
}
