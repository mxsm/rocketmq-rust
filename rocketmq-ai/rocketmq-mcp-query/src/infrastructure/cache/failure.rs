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

//! What the query cache does with a failed load.

use std::any::Any;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::MutexGuard;

use tokio::sync::Mutex;

use crate::tools::executor::ToolExecutionError;
use crate::tools::executor::ToolFailure;
use crate::tools::executor::ToolRejection;

/// How a failed load is answered to the other callers of its key.
pub(crate) trait CachedFailure: Clone + Send + Sync + 'static {
    /// Whether the callers that waited for the failed load get this failure too. Otherwise the
    /// next of them loads again.
    fn is_shared(&self) -> bool;

    /// Whether the calls of the next moments get this failure without a load of their own.
    fn is_kept(&self) -> bool;

    /// Whether a value that expired a short while ago may answer in place of this failure.
    fn allows_stale(&self) -> bool;
}

impl CachedFailure for ToolFailure {
    /// A cancellation is about the caller that loaded, so it is the one failure not shared.
    fn is_shared(&self) -> bool {
        !matches!(self, Self::Rejected(ToolRejection::Cancelled))
    }

    /// An unavailable source fails fast, so without this every call would reach it again. A
    /// timeout is not kept: the caller that loaded may have had only the rest of its budget.
    fn is_kept(&self) -> bool {
        matches!(self, Self::Operational(ToolExecutionError::SourceUnavailable(_)))
    }

    fn allows_stale(&self) -> bool {
        matches!(
            self,
            Self::Operational(ToolExecutionError::SourceUnavailable(_))
                | Self::Rejected(ToolRejection::TimedOut { .. })
        )
    }
}

/// The loads of one key.
///
/// A caller holds `turn` while it loads. The callers that wait for the turn learn here how a
/// load ended when it failed; a load that succeeded left its value in the cache.
#[derive(Default)]
pub(super) struct Flight {
    pub(super) turn: Mutex<()>,
    failed: StdMutex<FailedLoads>,
}

#[derive(Default)]
struct FailedLoads {
    count: u64,
    last: Option<Arc<dyn Any + Send + Sync>>,
}

impl Flight {
    /// The number of loads that failed so far. A caller reads it before it waits for the turn.
    pub(super) fn failed_loads(&self) -> u64 {
        self.failed().count
    }

    /// Leaves `failure` for the callers that wait for the turn.
    pub(super) fn fail<E: CachedFailure>(&self, failure: &E) {
        let mut failed = self.failed();
        failed.count = failed.count.saturating_add(1);
        failed.last = Some(Arc::new(failure.clone()));
    }

    /// The failure of a load that ended after the caller counted `seen` failed loads.
    pub(super) fn failure_since<E: CachedFailure>(&self, seen: u64) -> Option<E> {
        let failed = self.failed();
        if failed.count == seen {
            return None;
        }
        failed.last.as_ref()?.downcast_ref::<E>().cloned()
    }

    fn failed(&self) -> MutexGuard<'_, FailedLoads> {
        self.failed.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::executor::NotFoundEntity;

    #[test]
    fn tool_failures_are_shared_kept_and_replaced_by_stale_values_by_kind() {
        let policy = |failure: ToolFailure| (failure.is_shared(), failure.is_kept(), failure.allows_stale());

        assert_eq!(
            policy(ToolFailure::Operational(ToolExecutionError::SourceUnavailable(None))),
            (true, true, true)
        );
        assert_eq!(
            policy(ToolFailure::Rejected(ToolRejection::TimedOut { timeout_ms: 25_000 })),
            (true, false, true)
        );
        // An answer about the request, not about the source: every waiter asked the same thing.
        assert_eq!(
            policy(ToolFailure::not_found(NotFoundEntity::Topic)),
            (true, false, false)
        );
        assert_eq!(
            policy(ToolFailure::Operational(ToolExecutionError::Internal(None))),
            (true, false, false)
        );
        assert_eq!(
            policy(ToolFailure::Rejected(ToolRejection::Cancelled)),
            (false, false, false)
        );
    }

    #[test]
    fn a_flight_hands_a_failure_only_to_callers_that_joined_before_it() {
        let flight = Flight::default();
        let early = flight.failed_loads();
        assert_eq!(
            flight.failure_since::<ToolFailure>(early).map(|failure| failure.code()),
            None
        );

        flight.fail(&ToolFailure::Operational(ToolExecutionError::SourceUnavailable(None)));
        let late = flight.failed_loads();

        assert_eq!(
            flight.failure_since::<ToolFailure>(early).map(|failure| failure.code()),
            Some("source_unavailable")
        );
        assert_eq!(
            flight.failure_since::<ToolFailure>(late).map(|failure| failure.code()),
            None
        );
    }
}
