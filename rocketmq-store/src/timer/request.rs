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

use rocketmq_store_api::PersistedTimerRoute;
use rocketmq_store_api::TimerId;
use rocketmq_store_api::TimerPayloadLocator;
use rocketmq_store_api::TimerSourceCqOffset;
use rocketmq_store_api::TimerTimelineCursor;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TimerSourceRecord {
    pub(crate) id: TimerId,
    pub(crate) source_offset: TimerSourceCqOffset,
    pub(crate) due_time_ms: i64,
    pub(crate) payload: TimerPayloadLocator,
    pub(crate) route: PersistedTimerRoute,
    pub(crate) estimated_bytes: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DueTimerRecord {
    pub(crate) source: TimerSourceRecord,
    pub(crate) cursor: TimerTimelineCursor,
    pub(crate) shard: u32,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct EngineBatchProgress {
    pub(crate) messages: usize,
    pub(crate) bytes: usize,
    pub(crate) continuation: Option<TimerTimelineCursor>,
    pub(crate) durable: bool,
}

impl EngineBatchProgress {
    pub(crate) const fn empty() -> Self {
        Self {
            messages: 0,
            bytes: 0,
            continuation: None,
            durable: true,
        }
    }
}
