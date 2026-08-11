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

use std::future::Future;
use std::pin::Pin;

use crate::base::dispatch_request::DispatchRequest;

/// Execution boundary required by one Reput derived-state lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitLogDispatchExecution {
    /// The dispatcher is short, non-blocking work and may run on the coordinator task.
    Inline,
    /// The dispatcher performs synchronous storage I/O and must use the managed storage lane.
    Blocking,
    /// The dispatcher supplies an asynchronous, backpressure-aware implementation.
    Async,
}

pub trait CommitLogDispatcher: Send + Sync + 'static {
    fn dispatch(&self, dispatch_request: &mut DispatchRequest);

    /// Returns whether this dispatcher is independent of mutations made by sibling dispatchers.
    ///
    /// The ordered-lane coordinator only parallelizes a snapshot when every dispatcher opts in.
    /// External dispatchers keep serial semantics unless they explicitly establish this invariant.
    fn supports_parallel_dispatch(&self) -> bool {
        false
    }

    /// Returns the execution boundary used by the ordered-lane coordinator.
    fn dispatch_execution(&self) -> CommitLogDispatchExecution {
        CommitLogDispatchExecution::Inline
    }

    /// Dispatches one request while allowing bounded derived sinks to apply backpressure.
    fn dispatch_async<'a>(
        &'a self,
        dispatch_request: &'a mut DispatchRequest,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move { self.dispatch(dispatch_request) })
    }

    /// Dispatch a batch of requests. Default implementation calls dispatch for each request.
    /// Implementers can override this for batch optimizations.
    fn dispatch_batch(&self, dispatch_requests: &mut [DispatchRequest]) {
        for request in dispatch_requests.iter_mut() {
            self.dispatch(request);
        }
    }

    /// Dispatches a batch without letting a full derived-sink channel drop records.
    fn dispatch_batch_async<'a>(
        &'a self,
        dispatch_requests: &'a mut [DispatchRequest],
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move { self.dispatch_batch(dispatch_requests) })
    }

    /// Returns the highest persisted CommitLog offset this dispatcher has already processed.
    ///
    /// The returned offset is used as a recovery/reput lower bound. `None` means the dispatcher
    /// has no persisted progress to contribute.
    fn dispatch_progress_offset(&self, _commit_log_min_offset: i64) -> Option<i64> {
        None
    }
}
