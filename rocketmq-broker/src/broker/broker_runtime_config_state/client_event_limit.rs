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

use super::BrokerRuntimeConfigState;

/// Only the default Lite per-client count follows publication. Group overrides,
/// existing queued/reserved events and resource budgets retain their own rules.
#[derive(Clone)]
pub(crate) struct BrokerClientEventLimit(Source);

#[derive(Clone)]
enum Source {
    Runtime(BrokerRuntimeConfigState),
    Fixed(usize),
}

impl BrokerClientEventLimit {
    pub(crate) fn fixed(limit: usize) -> Self {
        Self(Source::Fixed(limit.max(1)))
    }

    pub(crate) fn from_runtime(source: BrokerRuntimeConfigState) -> Self {
        Self(Source::Runtime(source))
    }

    pub(crate) fn get(&self) -> usize {
        match &self.0 {
            Source::Runtime(source) => source.snapshot().broker().max_client_event_count.max(1) as usize,
            Source::Fixed(limit) => *limit,
        }
    }
}

impl std::fmt::Debug for BrokerClientEventLimit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BrokerClientEventLimit")
            .field("limit", &self.get())
            .finish()
    }
}
