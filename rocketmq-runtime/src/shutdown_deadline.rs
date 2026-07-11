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

use std::time::Duration;
use std::time::Instant;

/// A single absolute shutdown deadline shared by nested lifecycle owners.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShutdownDeadline {
    at: Instant,
}

impl ShutdownDeadline {
    pub fn after(timeout: Duration) -> Self {
        Self {
            at: Instant::now() + timeout,
        }
    }

    pub fn at(at: Instant) -> Self {
        Self { at }
    }

    pub fn instant(self) -> Instant {
        self.at
    }

    pub fn remaining(self) -> Duration {
        self.at.saturating_duration_since(Instant::now())
    }

    pub fn is_expired(self) -> bool {
        self.remaining().is_zero()
    }
}
