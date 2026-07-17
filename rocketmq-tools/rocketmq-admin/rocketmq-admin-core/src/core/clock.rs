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

//! Injectable wall clock used by deterministic admin planning.

use std::time::SystemTime;
use std::time::UNIX_EPOCH;

pub trait Clock: Send + Sync {
    fn now_millis(&self) -> u64;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_millis(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis().min(u64::MAX as u128) as u64)
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::Clock;
    use super::SystemClock;

    #[test]
    fn system_clock_reports_unix_epoch_millis() {
        assert!(SystemClock.now_millis() > 0);
    }
}
