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

use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::sync::Arc;

use rocketmq_model::common::message::message_ext::MessageExt;

pub(crate) struct ProcessQueueMessageStore {
    inner: PendingMessageStore,
}

enum PendingMessageStore {
    BTree(BTreeMap<i64, Arc<MessageExt>>),
    Contiguous(ContiguousOffsetStore),
}

struct ContiguousOffsetStore {
    base_offset: i64,
    messages: VecDeque<Arc<MessageExt>>,
}

impl ProcessQueueMessageStore {
    pub(crate) fn new() -> Self {
        Self {
            inner: PendingMessageStore::Contiguous(ContiguousOffsetStore::empty()),
        }
    }

    pub(crate) fn len(&self) -> usize {
        match &self.inner {
            PendingMessageStore::BTree(messages) => messages.len(),
            PendingMessageStore::Contiguous(messages) => messages.len(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn reserve(&mut self, additional: usize) {
        if let PendingMessageStore::Contiguous(messages) = &mut self.inner {
            messages.reserve(additional);
        }
    }

    #[cfg(test)]
    pub(crate) fn storage_kind(&self) -> &'static str {
        match &self.inner {
            PendingMessageStore::BTree(_) => "btree",
            PendingMessageStore::Contiguous(_) => "contiguous",
        }
    }

    pub(crate) fn insert(&mut self, offset: i64, message: Arc<MessageExt>) -> Option<Arc<MessageExt>> {
        match &mut self.inner {
            PendingMessageStore::BTree(messages) => {
                if messages.is_empty() {
                    self.inner = PendingMessageStore::Contiguous(ContiguousOffsetStore::singleton(offset, message));
                    None
                } else if ContiguousOffsetStore::can_represent_insert_map(messages, offset) {
                    let mut contiguous = ContiguousOffsetStore::from_btree_map(std::mem::take(messages));
                    let previous = contiguous.insert_contiguous(offset, message);
                    self.inner = PendingMessageStore::Contiguous(contiguous);
                    previous
                } else {
                    messages.insert(offset, message)
                }
            }
            PendingMessageStore::Contiguous(messages) => {
                if messages.can_insert(offset) {
                    messages.insert_contiguous(offset, message)
                } else {
                    let mut fallback = messages.to_btree_map();
                    let previous = fallback.insert(offset, message);
                    self.inner = PendingMessageStore::BTree(fallback);
                    previous
                }
            }
        }
    }

    pub(crate) fn remove(&mut self, offset: &i64) -> Option<Arc<MessageExt>> {
        match &mut self.inner {
            PendingMessageStore::BTree(messages) => messages.remove(offset),
            PendingMessageStore::Contiguous(messages) => {
                if !messages.contains_key(*offset) {
                    None
                } else if messages.can_remove_without_hole(*offset) {
                    messages.remove_contiguous(*offset)
                } else {
                    let mut fallback = messages.to_btree_map();
                    let previous = fallback.remove(offset);
                    self.inner = PendingMessageStore::BTree(fallback);
                    previous
                }
            }
        }
    }

    pub(crate) fn pop_first(&mut self) -> Option<(i64, Arc<MessageExt>)> {
        match &mut self.inner {
            PendingMessageStore::BTree(messages) => messages.pop_first(),
            PendingMessageStore::Contiguous(messages) => messages.pop_first(),
        }
    }

    pub(crate) fn first(&self) -> Option<(i64, Arc<MessageExt>)> {
        match &self.inner {
            PendingMessageStore::BTree(messages) => messages
                .first_key_value()
                .map(|(offset, message)| (*offset, message.clone())),
            PendingMessageStore::Contiguous(messages) => messages.first(),
        }
    }

    pub(crate) fn first_offset(&self) -> Option<i64> {
        self.first().map(|(offset, _)| offset)
    }

    pub(crate) fn offset_span(&self) -> Option<(i64, i64)> {
        match &self.inner {
            PendingMessageStore::BTree(messages) => match (messages.first_key_value(), messages.last_key_value()) {
                (Some((min, _)), Some((max, _))) => Some((*min, *max)),
                _ => None,
            },
            PendingMessageStore::Contiguous(messages) => messages.offset_span(),
        }
    }

    pub(crate) fn contains_key(&self, offset: &i64) -> bool {
        match &self.inner {
            PendingMessageStore::BTree(messages) => messages.contains_key(offset),
            PendingMessageStore::Contiguous(messages) => messages.contains_key(*offset),
        }
    }

    pub(crate) fn append_from_btree_map(&mut self, messages: &mut BTreeMap<i64, Arc<MessageExt>>) {
        let moved = std::mem::take(messages);
        for (offset, message) in moved {
            self.insert(offset, message);
        }
    }

    pub(crate) fn clear(&mut self) {
        self.inner = PendingMessageStore::Contiguous(ContiguousOffsetStore::empty());
    }
}

impl ContiguousOffsetStore {
    fn empty() -> Self {
        Self {
            base_offset: 0,
            messages: VecDeque::new(),
        }
    }

    fn singleton(offset: i64, message: Arc<MessageExt>) -> Self {
        let mut messages = VecDeque::with_capacity(1);
        messages.push_back(message);
        Self {
            base_offset: offset,
            messages,
        }
    }

    fn from_btree_map(messages: BTreeMap<i64, Arc<MessageExt>>) -> Self {
        let base_offset = messages.first_key_value().map_or(0, |(offset, _)| *offset);
        Self {
            base_offset,
            messages: messages.into_values().collect(),
        }
    }

    fn len(&self) -> usize {
        self.messages.len()
    }

    fn reserve(&mut self, additional: usize) {
        self.messages.reserve(additional);
    }

    fn append_offset(&self) -> Option<i64> {
        self.last_offset()?.checked_add(1)
    }

    fn last_offset(&self) -> Option<i64> {
        if self.messages.is_empty() {
            None
        } else {
            self.base_offset.checked_add(self.messages.len() as i64 - 1)
        }
    }

    fn can_insert(&self, offset: i64) -> bool {
        if self.messages.is_empty() {
            return true;
        }
        self.contains_key(offset)
            || self.append_offset() == Some(offset)
            || self.base_offset.checked_sub(1) == Some(offset)
    }

    fn can_represent_insert_map(messages: &BTreeMap<i64, Arc<MessageExt>>, offset: i64) -> bool {
        if messages.is_empty() {
            return true;
        }
        let min = *messages.first_key_value().expect("non-empty map has first key").0;
        let max = *messages.last_key_value().expect("non-empty map has last key").0;
        let Some(width) = max.checked_sub(min).and_then(|delta| delta.checked_add(1)) else {
            return false;
        };
        if width != messages.len() as i64 {
            return false;
        }
        (min..=max).contains(&offset) || max.checked_add(1) == Some(offset) || min.checked_sub(1) == Some(offset)
    }

    fn insert_contiguous(&mut self, offset: i64, message: Arc<MessageExt>) -> Option<Arc<MessageExt>> {
        if self.messages.is_empty() {
            self.base_offset = offset;
            self.messages.push_back(message);
            return None;
        }

        if self.base_offset.checked_sub(1) == Some(offset) {
            self.base_offset = offset;
            self.messages.push_front(message);
            return None;
        }

        if self.append_offset() == Some(offset) {
            self.messages.push_back(message);
            return None;
        }

        let index = offset
            .checked_sub(self.base_offset)
            .and_then(|distance| usize::try_from(distance).ok())
            .expect("contiguous replacement offset is in range");
        let previous = self.messages[index].clone();
        self.messages[index] = message;
        Some(previous)
    }

    fn can_remove_without_hole(&self, offset: i64) -> bool {
        self.messages.is_empty() || offset == self.base_offset || Some(offset) == self.last_offset()
    }

    fn remove_contiguous(&mut self, offset: i64) -> Option<Arc<MessageExt>> {
        if !self.contains_key(offset) {
            return None;
        }

        if offset == self.base_offset {
            let removed = self.messages.pop_front();
            self.base_offset = self.base_offset.saturating_add(1);
            return removed;
        }

        if Some(offset) == self.last_offset() {
            return self.messages.pop_back();
        }

        None
    }

    fn pop_first(&mut self) -> Option<(i64, Arc<MessageExt>)> {
        let offset = self.base_offset;
        let message = self.messages.pop_front()?;
        self.base_offset = self.base_offset.saturating_add(1);
        Some((offset, message))
    }

    fn first(&self) -> Option<(i64, Arc<MessageExt>)> {
        self.messages.front().map(|message| (self.base_offset, message.clone()))
    }

    fn offset_span(&self) -> Option<(i64, i64)> {
        self.last_offset().map(|last| (self.base_offset, last))
    }

    fn contains_key(&self, offset: i64) -> bool {
        offset
            .checked_sub(self.base_offset)
            .and_then(|distance| usize::try_from(distance).ok())
            .is_some_and(|index| index < self.messages.len())
    }

    fn to_btree_map(&self) -> BTreeMap<i64, Arc<MessageExt>> {
        self.messages
            .iter()
            .enumerate()
            .map(|(index, message)| {
                (
                    self.base_offset
                        .checked_add(index as i64)
                        .expect("contiguous offset stays within i64 range"),
                    message.clone(),
                )
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg() -> Arc<MessageExt> {
        Arc::new(MessageExt::default())
    }

    #[test]
    fn insert_and_drain_both_at_i64_max_boundary_without_overflow() {
        let mut store = ProcessQueueMessageStore::new();
        store.insert(i64::MAX - 1, msg());
        store.insert(i64::MAX, msg());

        assert_eq!(store.len(), 2);
        assert!(store.contains_key(&(i64::MAX - 1)));
        assert!(store.contains_key(&i64::MAX));
        assert_eq!(store.offset_span(), Some((i64::MAX - 1, i64::MAX)));

        let (first_offset, _) = store.pop_first().expect("first entry at i64::MAX - 1");
        assert_eq!(first_offset, i64::MAX - 1);
        let (second_offset, _) = store.pop_first().expect("second entry at i64::MAX");
        assert_eq!(second_offset, i64::MAX);
        assert!(store.is_empty());
    }

    #[test]
    fn insert_in_reverse_order_at_i64_min_boundary_prepends_correctly() {
        let mut store = ProcessQueueMessageStore::new();
        store.insert(i64::MIN + 1, msg());
        store.insert(i64::MIN, msg());

        assert_eq!(store.len(), 2);
        assert_eq!(store.offset_span(), Some((i64::MIN, i64::MIN + 1)));

        let (front_offset, _) = store.first().expect("front entry at i64::MIN");
        assert_eq!(front_offset, i64::MIN);

        let (popped_first, _) = store.pop_first().expect("pop i64::MIN first");
        assert_eq!(popped_first, i64::MIN);
        let (popped_second, _) = store.pop_first().expect("pop i64::MIN + 1 second");
        assert_eq!(popped_second, i64::MIN + 1);
    }

    #[test]
    fn insert_both_integer_extremes_falls_back_to_sparse_storage_preserving_both_keys() {
        let mut store = ProcessQueueMessageStore::new();
        store.insert(i64::MIN, msg());
        store.insert(i64::MAX, msg());

        assert_eq!(store.storage_kind(), "btree");
        assert_eq!(store.len(), 2);
        assert!(store.contains_key(&i64::MIN));
        assert!(store.contains_key(&i64::MAX));
        assert_eq!(store.offset_span(), Some((i64::MIN, i64::MAX)));
    }

    #[test]
    fn ordinary_offset_after_clearing_extreme_offset_store_has_no_stale_base_offset() {
        let mut store = ProcessQueueMessageStore::new();
        store.insert(i64::MAX, msg());

        store.clear();
        store.insert(5, msg());

        assert_eq!(store.storage_kind(), "contiguous");
        assert_eq!(store.len(), 1);
        assert_eq!(store.offset_span(), Some((5, 5)));
        assert!(store.contains_key(&5));
        assert!(!store.contains_key(&i64::MAX));
    }

    #[test]
    fn ordinary_offset_after_draining_extreme_offset_store_has_no_stale_base_offset() {
        let mut store = ProcessQueueMessageStore::new();
        store.insert(i64::MAX, msg());
        store.pop_first();
        assert!(store.is_empty());

        store.insert(7, msg());

        assert_eq!(store.storage_kind(), "contiguous");
        assert_eq!(store.len(), 1);
        assert_eq!(store.offset_span(), Some((7, 7)));
        assert!(store.contains_key(&7));
        assert!(!store.contains_key(&i64::MAX));
    }
}
