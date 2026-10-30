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
use std::collections::VecDeque;
use std::fs::File;
use std::hash::Hash;

/// Files kept open at once, per kind of file.
const HANDLE_LIMIT: usize = 64;

/// Open file handles, the least recently used closed first.
pub(super) struct HandleCache<K> {
    files: HashMap<K, File>,
    order: VecDeque<K>,
}

impl<K: Copy + Eq + Hash> HandleCache<K> {
    pub(super) fn new() -> Self {
        Self {
            files: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    /// The handle of `key`, opened with `open` when it is not cached.
    pub(super) fn get_or_open(
        &mut self,
        key: K,
        open: impl FnOnce() -> std::io::Result<File>,
    ) -> std::io::Result<&File> {
        if !self.files.contains_key(&key) {
            self.insert(key, open()?);
        } else if self.order.back() != Some(&key) {
            self.order.retain(|cached| *cached != key);
            self.order.push_back(key);
        }
        self.files.get(&key).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "a native timer file was closed while in use",
            )
        })
    }

    pub(super) fn insert(&mut self, key: K, file: File) {
        if self.files.insert(key, file).is_none() {
            self.order.push_back(key);
        }
        while self.files.len() > HANDLE_LIMIT {
            let Some(evicted) = self.order.pop_front() else {
                break;
            };
            self.files.remove(&evicted);
        }
    }

    /// Closes the handle of `key`; an open handle keeps a file from being deleted on Windows.
    pub(super) fn remove(&mut self, key: &K) {
        if self.files.remove(key).is_some() {
            self.order.retain(|cached| cached != key);
        }
    }
}
