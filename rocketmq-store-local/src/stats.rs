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

pub mod stats_type;
mod store_stats;

pub use store_stats::StoreStatsState;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CallSnapshot {
    pub timestamp: u64,
    pub call_times_total: u64,
}

impl CallSnapshot {
    pub const fn new(timestamp: u64, call_times_total: u64) -> Self {
        Self {
            timestamp,
            call_times_total,
        }
    }

    pub fn get_tps(begin: &Self, end: &Self) -> f64 {
        let total = end.call_times_total as i64 - begin.call_times_total as i64;
        let duration = end.timestamp as i64 - begin.timestamp as i64;
        if duration == 0 {
            return 0.0;
        }
        total as f64 / duration as f64 * 1000.0
    }
}
