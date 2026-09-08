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
use std::time::Instant;

use rocketmq_proxy_core::error::canonical;
use rocketmq_proxy_core::ProxyError;
use rocketmq_proxy_core::ProxyResult;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug)]
pub(crate) struct RequestControl {
    pub(crate) deadline_at: Option<Instant>,
    pub(crate) timeout_budget: Option<Duration>,
    pub(crate) cancellation: CancellationToken,
}

impl RequestControl {
    pub(crate) fn new(
        entry: Instant,
        deadline: Option<Instant>,
        timeout: Option<Duration>,
        parent: &CancellationToken,
    ) -> ProxyResult<Self> {
        let typed_deadline = timeout
            .map(|timeout| {
                entry.checked_add(timeout).ok_or_else(|| {
                    ProxyError::from(canonical::argument(
                        "local request timeout exceeds the supported clock range",
                    ))
                })
            })
            .transpose()?;
        let deadline_at = match (deadline, typed_deadline) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        Ok(Self {
            deadline_at,
            timeout_budget: deadline_at.map(|deadline| deadline.saturating_duration_since(entry)),
            cancellation: parent.child_token(),
        })
    }

    pub(crate) fn timeout_error(&self) -> ProxyError {
        canonical::timed_out(
            "local broker request",
            self.timeout_budget
                .unwrap_or_default()
                .as_millis()
                .min(u64::MAX as u128) as u64,
        )
        .into()
    }

    pub(crate) fn check(&self) -> ProxyResult<()> {
        if self.cancellation.is_cancelled() {
            return Err(ProxyError::Transport {
                message: "local broker request cancelled".to_owned(),
            });
        }
        if self.deadline_at.is_some_and(|deadline| Instant::now() >= deadline) {
            return Err(self.timeout_error());
        }
        Ok(())
    }

    pub(crate) fn remaining(&self, operation_limit: Duration) -> ProxyResult<Duration> {
        self.check()?;
        Ok(self.deadline_at.map_or(operation_limit, |deadline| {
            deadline.saturating_duration_since(Instant::now()).min(operation_limit)
        }))
    }

    pub(crate) async fn expired(&self) {
        match self.deadline_at {
            Some(deadline) => tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await,
            None => std::future::pending().await,
        }
    }
}
