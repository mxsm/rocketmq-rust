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

//! Backend-neutral ConsumeQueue roots and dispatch control flow.

use std::ops::Deref;
use std::ops::DerefMut;

/// Canonical owner of one ConsumeQueue adapter.
#[derive(Clone, Debug)]
pub struct ConsumeQueueRoot<A> {
    adapter: A,
}

impl<A> ConsumeQueueRoot<A> {
    pub fn new(adapter: A) -> Self {
        Self { adapter }
    }

    pub fn adapter(&self) -> &A {
        &self.adapter
    }

    pub fn adapter_mut(&mut self) -> &mut A {
        &mut self.adapter
    }

    pub fn into_adapter(self) -> A {
        self.adapter
    }
}

impl<A> Deref for ConsumeQueueRoot<A> {
    type Target = A;

    fn deref(&self) -> &Self::Target {
        self.adapter()
    }
}

impl<A> DerefMut for ConsumeQueueRoot<A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.adapter_mut()
    }
}

/// Canonical owner of the Store-facing ConsumeQueue composition adapter.
pub type ConsumeQueueStoreRoot<A> = ConsumeQueueRoot<A>;

/// Root that owns CommitLog-to-ConsumeQueue dispatch gating and progress projection.
#[derive(Clone, Debug)]
pub struct ConsumeQueueDispatchRoot<A> {
    adapter: A,
}

impl<A> ConsumeQueueDispatchRoot<A> {
    pub fn new(adapter: A) -> Self {
        Self { adapter }
    }

    pub fn adapter(&self) -> &A {
        &self.adapter
    }

    /// Dispatches only transaction states projected by Store as eligible.
    pub fn dispatch<Request, Apply>(&self, eligible: bool, request: &mut Request, apply: Apply) -> bool
    where
        Apply: FnOnce(&A, &mut Request),
    {
        if !eligible {
            return false;
        }
        apply(&self.adapter, request);
        true
    }

    /// Projects the global maximum physical offset into optional dispatcher progress.
    pub fn progress_offset<ReadOffset>(&self, read_offset: ReadOffset) -> Option<i64>
    where
        ReadOffset: FnOnce(&A) -> i64,
    {
        let offset = read_offset(&self.adapter);
        (offset >= 0).then_some(offset)
    }
}

/// ConsumeQueue format selected for dispatch validation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConsumeQueueDispatchMode {
    Single,
    Batch,
}

/// Neutral dispatch fields needed by the retry/validation owner.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConsumeQueueDispatchMetadata {
    pub message_base_offset: i64,
    pub batch_size: i16,
}

/// Final result of one ConsumeQueue dispatch drive.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConsumeQueueDispatchOutcome {
    Appended { attempts: usize },
    InvalidBatch,
    NotWriteable,
    Exhausted { attempts: usize },
}

/// Owns batch validation, writeability gating, and bounded append retries.
pub fn drive_consume_queue_dispatch<TryAppend>(
    mode: ConsumeQueueDispatchMode,
    metadata: ConsumeQueueDispatchMetadata,
    writeable: bool,
    max_attempts: usize,
    mut try_append: TryAppend,
) -> ConsumeQueueDispatchOutcome
where
    TryAppend: FnMut(usize) -> bool,
{
    if mode == ConsumeQueueDispatchMode::Batch && (metadata.message_base_offset < 0 || metadata.batch_size <= 0) {
        return ConsumeQueueDispatchOutcome::InvalidBatch;
    }
    if !writeable {
        return ConsumeQueueDispatchOutcome::NotWriteable;
    }
    for attempt in 0..max_attempts {
        if try_append(attempt) {
            return ConsumeQueueDispatchOutcome::Appended { attempts: attempt + 1 };
        }
    }
    ConsumeQueueDispatchOutcome::Exhausted { attempts: max_attempts }
}

/// Runs the lock-free lookup, out-of-lock construction, and atomic publication sequence.
pub fn find_or_create_consume_queue<T, Find, Create, Publish>(mut find: Find, create: Create, publish: Publish) -> T
where
    Find: FnMut() -> Option<T>,
    Create: FnOnce() -> T,
    Publish: FnOnce(T) -> T,
{
    if let Some(existing) = find() {
        return existing;
    }
    publish(create())
}

/// Clamps a backend query result to the current logical queue range.
pub fn clamp_consume_queue_offset(offset: i64, min_offset: i64, max_offset: i64) -> i64 {
    offset.max(min_offset).min(max_offset)
}
