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

use std::collections::HashSet;

use cheetah_string::CheetahString;

/// Container for batched trace data ready for transmission.
///
/// Holds encoded trace data and associated message keys for dispatch to the trace backend.
#[derive(Debug, Clone, Default)]
pub struct TraceTransferBean {
    /// Encoded trace data
    pub trans_data: CheetahString,
    /// Message keys associated with this batch
    pub trans_key: HashSet<CheetahString>,
}

impl TraceTransferBean {
    /// Creates an empty trace transfer bean.
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a trace transfer bean with encoded data and keys.
    #[inline]
    pub fn with_data(trans_data: CheetahString, trans_key: HashSet<CheetahString>) -> Self {
        Self { trans_data, trans_key }
    }

    /// Sets the encoded trace data.
    #[inline]
    pub fn set_trans_data(&mut self, trans_data: CheetahString) {
        self.trans_data = trans_data;
    }

    /// Returns a reference to the encoded trace data.
    #[inline]
    pub fn trans_data(&self) -> &CheetahString {
        &self.trans_data
    }

    /// Sets the message keys.
    #[inline]
    pub fn set_trans_key(&mut self, trans_key: HashSet<CheetahString>) {
        self.trans_key = trans_key;
    }

    /// Returns a reference to the message keys.
    #[inline]
    pub fn trans_key(&self) -> &HashSet<CheetahString> {
        &self.trans_key
    }

    /// Returns a mutable reference to the message keys.
    #[inline]
    pub fn trans_key_mut(&mut self) -> &mut HashSet<CheetahString> {
        &mut self.trans_key
    }

    /// Inserts a key into the set.
    ///
    /// Returns `true` if the key was newly inserted, `false` if it already existed.
    #[inline]
    pub fn add_key(&mut self, key: CheetahString) -> bool {
        self.trans_key.insert(key)
    }

    /// Returns `true` if both trace data and keys are empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.trans_data.is_empty() && self.trans_key.is_empty()
    }

    /// Returns the number of keys.
    #[inline]
    pub fn key_count(&self) -> usize {
        self.trans_key.len()
    }
}
