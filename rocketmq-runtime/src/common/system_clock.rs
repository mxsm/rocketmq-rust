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

use std::time::SystemTime;
use std::time::UNIX_EPOCH;

pub struct SystemClock;

impl SystemClock {
    pub fn now() -> u128 {
        millis_since_epoch(SystemTime::now())
    }
}

fn millis_since_epoch(now: SystemTime) -> u128 {
    now.duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn millis_since_epoch_saturates_when_clock_is_before_epoch() {
        assert_eq!(millis_since_epoch(UNIX_EPOCH - Duration::from_millis(1)), 0);
    }
}
