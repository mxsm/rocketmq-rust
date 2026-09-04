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
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::base::dispatch_request::DispatchRequest;

const INDEX_ENABLED: u8 = 1;
const INDEX_INCOMPLETE: u8 = 1 << 1;

/// One coherent view of runtime message-index admission and completeness.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MessageIndexRuntimeSnapshot {
    pub enabled: bool,
    pub incomplete: bool,
}

/// Broker-owned source for the currently published message-index generation.
pub trait MessageIndexRuntimeSource: Send + Sync + 'static {
    fn snapshot(&self) -> MessageIndexRuntimeSnapshot;

    /// Runs one complete index-dispatch operation against one stable runtime state.
    ///
    /// The source keeps its transition read-side protection until `dispatch`
    /// returns, so a disable cannot publish while an admitted index write is
    /// still in flight.
    fn with_dispatch_admission(&self, dispatch: &mut dyn FnMut(bool));
}

/// Shared Store-side indirection used by every index dispatcher and query path.
#[derive(Clone)]
pub struct MessageIndexRuntimeHandle {
    fallback: Arc<AtomicU8>,
    source: Arc<RwLock<Option<Arc<dyn MessageIndexRuntimeSource>>>>,
}

impl MessageIndexRuntimeHandle {
    pub fn new(enabled: bool) -> Self {
        Self {
            fallback: Arc::new(AtomicU8::new(u8::from(enabled) * INDEX_ENABLED)),
            source: Arc::new(RwLock::new(None)),
        }
    }

    pub fn install(&self, source: Arc<dyn MessageIndexRuntimeSource>) {
        *self.source.write() = Some(source);
    }

    #[inline]
    pub fn snapshot(&self) -> MessageIndexRuntimeSnapshot {
        if let Some(source) = self.source.read().as_ref() {
            return source.snapshot();
        }
        decode_index_state(self.fallback.load(Ordering::Acquire))
    }

    #[inline]
    pub fn with_dispatch_admission(&self, dispatch: &mut dyn FnMut(bool)) {
        if let Some(source) = self.source.read().as_ref() {
            source.with_dispatch_admission(dispatch);
            return;
        }
        loop {
            let current = self.fallback.load(Ordering::Acquire);
            if current & INDEX_ENABLED != 0 {
                dispatch(true);
                return;
            }
            if current & INDEX_INCOMPLETE != 0
                || self
                    .fallback
                    .compare_exchange(current, current | INDEX_INCOMPLETE, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                dispatch(false);
                return;
            }
        }
    }

    pub fn set_fallback_enabled(&self, enabled: bool) -> bool {
        if self.source.read().is_some() {
            return false;
        }
        self.fallback
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(if enabled {
                    current | INDEX_ENABLED
                } else {
                    current & !INDEX_ENABLED
                })
            })
            .is_ok()
    }
}

fn decode_index_state(state: u8) -> MessageIndexRuntimeSnapshot {
    MessageIndexRuntimeSnapshot {
        enabled: state & INDEX_ENABLED != 0,
        incomplete: state & INDEX_INCOMPLETE != 0,
    }
}

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

    /// Advances derived-state coverage across a CommitLog BLANK record verified by the scanner.
    ///
    /// `blank_start_offset` is the first padding byte and `next_file_offset` is the mapped-file
    /// boundary selected by CommitLog. Implementations must not infer this transition from an
    /// arbitrary gap between ordinary dispatch requests.
    fn dispatch_commit_log_blank(&self, _blank_start_offset: i64, _next_file_offset: i64) {}

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

    fn dispatch_commit_log_blank_async<'a>(
        &'a self,
        blank_start_offset: i64,
        next_file_offset: i64,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move { self.dispatch_commit_log_blank(blank_start_offset, next_file_offset) })
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

    /// Installs the Broker-owned runtime state when this is an index dispatcher.
    fn install_message_index_runtime(&self, _source: Arc<dyn MessageIndexRuntimeSource>) -> bool {
        false
    }

    /// Returns the coherent index state when this is an index dispatcher.
    fn message_index_runtime_snapshot(&self) -> Option<MessageIndexRuntimeSnapshot> {
        None
    }

    /// Returns the durable exclusive CommitLog offset covered by this index.
    ///
    /// Unlike `dispatch_progress_offset`, this remains observable while runtime
    /// indexing is disabled so a no-gap re-enable can prove the armed baseline.
    fn message_index_safe_offset(&self) -> Option<i64> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::MessageIndexRuntimeHandle;

    #[test]
    fn fallback_index_runtime_marks_only_an_observed_disabled_gap() {
        let runtime = MessageIndexRuntimeHandle::new(false);
        assert!(!runtime.snapshot().enabled);
        assert!(!runtime.snapshot().incomplete);

        assert!(runtime.set_fallback_enabled(true));
        assert_eq!(
            runtime.snapshot(),
            super::MessageIndexRuntimeSnapshot {
                enabled: true,
                incomplete: false,
            }
        );

        assert!(runtime.set_fallback_enabled(false));
        let mut admitted = true;
        runtime.with_dispatch_admission(&mut |enabled| admitted = enabled);
        assert!(!admitted);
        assert!(runtime.snapshot().incomplete);
        assert!(runtime.set_fallback_enabled(true));
        assert!(runtime.snapshot().enabled);
        assert!(runtime.snapshot().incomplete);
    }
}
