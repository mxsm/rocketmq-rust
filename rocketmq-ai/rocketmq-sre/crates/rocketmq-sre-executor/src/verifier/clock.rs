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

use std::time::Duration;

use chrono::DateTime;
use chrono::Utc;

/// Independent UTC qualification and monotonic observation clocks.
///
/// Production uses the real system/Runtime clocks. Injection supports
/// deterministic isolated fault tests; no HTTP/config input can replace them.
pub trait VerificationClock: Send + Sync {
    fn utc_now(&self) -> DateTime<Utc>;
    fn monotonic_elapsed(&self) -> Duration;
}

pub(super) struct SystemVerificationClock {
    origin: tokio::time::Instant,
}

impl SystemVerificationClock {
    pub(super) fn new() -> Self {
        Self {
            origin: tokio::time::Instant::now(),
        }
    }
}

impl VerificationClock for SystemVerificationClock {
    fn utc_now(&self) -> DateTime<Utc> {
        Utc::now()
    }

    fn monotonic_elapsed(&self) -> Duration {
        self.origin.elapsed()
    }
}
