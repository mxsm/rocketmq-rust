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

use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_rust::ArcMut;

pub(crate) struct ProcessQueueMessageStore {
    inner: PendingMessageStore,
}

enum PendingMessageStore {
    BTree(BTreeMap<i64, ArcMut<MessageExt>>),
    Contiguous(ContiguousOffsetStore),
}

struct ContiguousOffsetStore {
    base_offset: i64,
    messages: VecDeque<ArcMut<MessageExt>>,
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

    pub(crate) fn insert(&mut self, offset: i64, message: ArcMut<MessageExt>) -> Option<ArcMut<MessageExt>> {
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

    pub(crate) fn remove(&mut self, offset: &i64) -> Option<ArcMut<MessageExt>> {
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

    pub(crate) fn pop_first(&mut self) -> Option<(i64, ArcMut<MessageExt>)> {
        match &mut self.inner {
            PendingMessageStore::BTree(messages) => messages.pop_first(),
            PendingMessageStore::Contiguous(messages) => messages.pop_first(),
        }
    }

    pub(crate) fn first(&self) -> Option<(i64, ArcMut<MessageExt>)> {
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

    pub(crate) fn append_from_btree_map(&mut self, messages: &mut BTreeMap<i64, ArcMut<MessageExt>>) {
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

    fn singleton(offset: i64, message: ArcMut<MessageExt>) -> Self {
        let mut messages = VecDeque::with_capacity(1);
        messages.push_back(message);
        Self {
            base_offset: offset,
            messages,
        }
    }

    fn from_btree_map(messages: BTreeMap<i64, ArcMut<MessageExt>>) -> Self {
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

    fn can_represent_insert_map(messages: &BTreeMap<i64, ArcMut<MessageExt>>, offset: i64) -> bool {
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

    fn insert_contiguous(&mut self, offset: i64, message: ArcMut<MessageExt>) -> Option<ArcMut<MessageExt>> {
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

    fn remove_contiguous(&mut self, offset: i64) -> Option<ArcMut<MessageExt>> {
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

    fn pop_first(&mut self) -> Option<(i64, ArcMut<MessageExt>)> {
        let offset = self.base_offset;
        let message = self.messages.pop_front()?;
        self.base_offset = self.base_offset.saturating_add(1);
        Some((offset, message))
    }

    fn first(&self) -> Option<(i64, ArcMut<MessageExt>)> {
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

    fn to_btree_map(&self) -> BTreeMap<i64, ArcMut<MessageExt>> {
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
