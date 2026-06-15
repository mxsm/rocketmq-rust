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

pub use crate::semantic::metrics::MESSAGES_IN_TOTAL;
pub use crate::semantic::metrics::MESSAGES_OUT_TOTAL;
pub use crate::semantic::metrics::MESSAGE_SIZE;
pub use crate::semantic::metrics::THROUGHPUT_IN_TOTAL;
pub use crate::semantic::metrics::THROUGHPUT_OUT_TOTAL;

#[derive(Debug, Clone, Copy, Default)]
pub struct BrokerMetrics;

impl BrokerMetrics {
    pub fn noop() -> Self {
        Self
    }

    #[inline]
    pub fn record_messages_in(&self, _count: u64, _bytes: u64, _message_size: u64) {}

    #[inline]
    pub fn record_messages_out(&self, _count: u64, _bytes: u64) {}
}
