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
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use crate::guard::GuardError;

const RATE_LIMIT_WINDOW: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Default)]
pub(crate) struct RateLimiter {
    calls: Arc<Mutex<HashMap<String, VecDeque<Instant>>>>,
}

impl RateLimiter {
    pub(crate) fn check(
        &self,
        principal_id: &str,
        cluster: Option<&str>,
        operation: &str,
        limit_per_minute: u32,
    ) -> Result<(), GuardError> {
        let key = format!("{principal_id}|{}|{operation}", cluster.unwrap_or("_"));
        if limit_per_minute == 0 {
            return Err(GuardError::RateLimited(format!(
                "{operation} is disabled by zero rate limit"
            )));
        }

        let Ok(mut calls) = self.calls.lock() else {
            return Err(GuardError::RateLimited("rate limiter state is unavailable".to_string()));
        };
        let now = Instant::now();
        let entries = calls.entry(key).or_default();

        while entries
            .front()
            .is_some_and(|first_call| now.duration_since(*first_call) >= RATE_LIMIT_WINDOW)
        {
            entries.pop_front();
        }

        if entries.len() >= limit_per_minute as usize {
            return Err(GuardError::RateLimited(format!(
                "{operation} exceeded {limit_per_minute} calls per minute for principal `{principal_id}`"
            )));
        }

        entries.push_back(now);
        Ok(())
    }
}
