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
use std::error::Error;
use std::fmt;
use std::mem::size_of;
use std::num::NonZeroUsize;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use cheetah_string::CheetahString;

use crate::long_polling::pending_arrival_latch::PendingArrivalValue;
use crate::long_polling::pop_deferred::index::PopArrivalView;
use crate::long_polling::pop_deferred::index::PopCriteriaIndex;
use crate::long_polling::pop_deferred::index::PopFanoutBatch;
use crate::long_polling::pop_deferred::index::PopFanoutCursor;

struct OwnedPopArrival {
    topic: CheetahString,
    queue_id: i32,
    tags_code: Option<i64>,
    message_store_time: i64,
    filter_bitmap: Option<Vec<u8>>,
    properties: Option<HashMap<CheetahString, CheetahString>>,
}

impl OwnedPopArrival {
    #[allow(
        clippy::too_many_arguments,
        reason = "the continuation retains the exact Store arrival callback metadata"
    )]
    fn try_new(
        topic: &CheetahString,
        queue_id: i32,
        tags_code: Option<i64>,
        message_store_time: i64,
        filter_bitmap: Option<&[u8]>,
        properties: Option<&HashMap<CheetahString, CheetahString>>,
    ) -> Result<Self, PopContinuationError> {
        Ok(Self {
            topic: topic.clone(),
            queue_id,
            tags_code,
            message_store_time,
            filter_bitmap: copy_bitmap(filter_bitmap)?,
            properties: copy_properties(properties)?,
        })
    }

    fn view<'a>(&'a self, consumer_group: &'a CheetahString) -> PopArrivalView<'a> {
        PopArrivalView::new(&self.topic, consumer_group, self.queue_id).with_filter_metadata(
            self.tags_code,
            self.message_store_time,
            self.filter_bitmap.as_deref(),
            self.properties.as_ref(),
        )
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct PopPendingArrivalKey {
    sequence: u64,
    topic: CheetahString,
    queue_id: i32,
}

impl PopPendingArrivalKey {
    pub(crate) fn new(sequence: u64, topic: CheetahString, queue_id: i32) -> Self {
        Self {
            sequence,
            topic,
            queue_id,
        }
    }
}

pub(crate) struct PopPendingArrival {
    arrival: OwnedPopArrival,
    cursor: PopFanoutCursor,
    retained_bytes: usize,
}

impl PopPendingArrival {
    #[allow(
        clippy::too_many_arguments,
        reason = "the pending replay retains the exact Store arrival callback metadata"
    )]
    pub(super) fn new(
        topic: &CheetahString,
        queue_id: i32,
        tags_code: Option<i64>,
        message_store_time: i64,
        filter_bitmap: Option<&[u8]>,
        properties: Option<&HashMap<CheetahString, CheetahString>>,
        cursor: PopFanoutCursor,
    ) -> Result<Self, PopContinuationError> {
        let retained_bytes = retained_arrival_bytes(topic, filter_bitmap, properties)?;
        let arrival = OwnedPopArrival::try_new(
            topic,
            queue_id,
            tags_code,
            message_store_time,
            filter_bitmap,
            properties,
        )?;
        Ok(Self {
            arrival,
            cursor,
            retained_bytes,
        })
    }

    pub(crate) fn view<'a>(&'a self, consumer_group: &'a CheetahString) -> PopArrivalView<'a> {
        self.arrival.view(consumer_group)
    }

    pub(super) fn next_batch(&mut self, index: &PopCriteriaIndex, limit: NonZeroUsize) -> PopFanoutBatch {
        index.consumer_group_batch(&self.arrival.topic, self.arrival.queue_id, &mut self.cursor, limit)
    }
}

impl PendingArrivalValue for PopPendingArrival {
    fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    fn rewind_from_start(&mut self) {
        self.cursor = PopFanoutCursor::new();
    }

    fn coalesce_refresh(&mut self) {
        self.rewind_from_start();
    }
}

#[must_use]
pub(crate) struct PopArrivalContinuation {
    pending: PopPendingArrival,
    _permit: PopContinuationPermit,
}

#[must_use]
pub(crate) enum PopContinuationOutcome {
    Admitted(PopArrivalContinuation),
    Rejected(PopContinuationRejection),
}

impl PopArrivalContinuation {
    #[allow(
        clippy::too_many_arguments,
        reason = "the continuation retains the exact Store arrival callback metadata"
    )]
    pub(super) fn try_admit(
        admission: &Arc<PopContinuationAdmission>,
        topic: &CheetahString,
        queue_id: i32,
        tags_code: Option<i64>,
        message_store_time: i64,
        filter_bitmap: Option<&[u8]>,
        properties: Option<&HashMap<CheetahString, CheetahString>>,
        cursor: PopFanoutCursor,
    ) -> Result<PopContinuationOutcome, PopContinuationError> {
        let pending = PopPendingArrival::new(
            topic,
            queue_id,
            tags_code,
            message_store_time,
            filter_bitmap,
            properties,
            cursor,
        )?;
        let retained_bytes = pending.retained_bytes();
        match admission.reserve(retained_bytes)? {
            PopContinuationReserveOutcome::Reserved(permit) => Ok(PopContinuationOutcome::Admitted(Self {
                pending,
                _permit: permit,
            })),
            PopContinuationReserveOutcome::Rejected(rejection) => Ok(PopContinuationOutcome::Rejected(rejection)),
        }
    }

    pub(crate) fn view<'a>(&'a self, consumer_group: &'a CheetahString) -> PopArrivalView<'a> {
        self.pending.view(consumer_group)
    }

    pub(super) fn next_batch(&mut self, index: &PopCriteriaIndex, limit: NonZeroUsize) -> PopFanoutBatch {
        self.pending.next_batch(index, limit)
    }
}

pub(super) struct PopContinuationAdmission {
    max_count: usize,
    max_bytes: usize,
    count: AtomicUsize,
    bytes: AtomicUsize,
    rejected: AtomicUsize,
}

impl PopContinuationAdmission {
    pub(super) const fn new(max_count: usize, max_bytes: usize) -> Self {
        Self {
            max_count,
            max_bytes,
            count: AtomicUsize::new(0),
            bytes: AtomicUsize::new(0),
            rejected: AtomicUsize::new(0),
        }
    }

    pub(super) fn reserve(
        self: &Arc<Self>,
        bytes: usize,
    ) -> Result<PopContinuationReserveOutcome, PopContinuationError> {
        let count = self.count.fetch_add(1, Ordering::AcqRel);
        if count >= self.max_count {
            self.reject_count();
            return Ok(PopContinuationReserveOutcome::Rejected(
                PopContinuationRejection::CountFull,
            ));
        }
        let mut current = self.bytes.load(Ordering::Acquire);
        loop {
            let Some(next) = current.checked_add(bytes) else {
                self.reject_count();
                return Err(PopContinuationError::SizeOverflow);
            };
            if next > self.max_bytes {
                self.reject_count();
                return Ok(PopContinuationReserveOutcome::Rejected(
                    PopContinuationRejection::BytesFull,
                ));
            }
            match self
                .bytes
                .compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
        Ok(PopContinuationReserveOutcome::Reserved(PopContinuationPermit {
            admission: Arc::clone(self),
            bytes,
        }))
    }

    fn reject_count(&self) {
        self.count.fetch_sub(1, Ordering::AcqRel);
        self.rejected.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn snapshot(&self) -> PopContinuationSnapshot {
        PopContinuationSnapshot {
            count: self.count.load(Ordering::Acquire),
            bytes: self.bytes.load(Ordering::Acquire),
            rejected: self.rejected.load(Ordering::Acquire),
        }
    }
}

pub(super) struct PopContinuationPermit {
    admission: Arc<PopContinuationAdmission>,
    bytes: usize,
}

pub(super) enum PopContinuationReserveOutcome {
    Reserved(PopContinuationPermit),
    Rejected(PopContinuationRejection),
}

impl Drop for PopContinuationPermit {
    fn drop(&mut self) {
        self.admission.count.fetch_sub(1, Ordering::AcqRel);
        self.admission.bytes.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

pub(super) struct PopContinuationSnapshot {
    pub(super) count: usize,
    pub(super) bytes: usize,
    pub(super) rejected: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PopContinuationRejection {
    CountFull,
    BytesFull,
}

#[derive(Debug)]
pub(crate) enum PopContinuationError {
    SizeOverflow,
    Allocation(std::collections::TryReserveError),
}

impl fmt::Display for PopContinuationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SizeOverflow => formatter.write_str("POP continuation retained size overflowed"),
            Self::Allocation(_) => formatter.write_str("POP continuation allocation failed"),
        }
    }
}

impl Error for PopContinuationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Allocation(source) => Some(source),
            Self::SizeOverflow => None,
        }
    }
}

fn retained_arrival_bytes(
    topic: &CheetahString,
    bitmap: Option<&[u8]>,
    properties: Option<&HashMap<CheetahString, CheetahString>>,
) -> Result<usize, PopContinuationError> {
    let mut bytes = size_of::<PopArrivalContinuation>()
        .checked_add(allocation_bound(topic.len())?)
        .ok_or(PopContinuationError::SizeOverflow)?;
    if let Some(bitmap) = bitmap {
        bytes = bytes
            .checked_add(allocation_bound(bitmap.len())?)
            .ok_or(PopContinuationError::SizeOverflow)?;
    }
    if let Some(properties) = properties {
        let buckets = properties
            .len()
            .checked_mul(size_of::<(CheetahString, CheetahString)>() + size_of::<u64>() + 1)
            .ok_or(PopContinuationError::SizeOverflow)?;
        bytes = bytes.checked_add(buckets).ok_or(PopContinuationError::SizeOverflow)?;
        for (key, value) in properties {
            bytes = bytes
                .checked_add(allocation_bound(key.len())?)
                .and_then(|total| total.checked_add(allocation_bound(value.len()).ok()?))
                .ok_or(PopContinuationError::SizeOverflow)?;
        }
    }
    Ok(bytes.max(1))
}

fn allocation_bound(len: usize) -> Result<usize, PopContinuationError> {
    len.checked_mul(2)
        .and_then(|bytes| bytes.checked_add(size_of::<usize>()))
        .ok_or(PopContinuationError::SizeOverflow)
}

fn copy_bitmap(bitmap: Option<&[u8]>) -> Result<Option<Vec<u8>>, PopContinuationError> {
    let Some(bitmap) = bitmap else {
        return Ok(None);
    };
    let mut owned = Vec::new();
    owned
        .try_reserve_exact(bitmap.len())
        .map_err(PopContinuationError::Allocation)?;
    owned.extend_from_slice(bitmap);
    Ok(Some(owned))
}

fn copy_properties(
    properties: Option<&HashMap<CheetahString, CheetahString>>,
) -> Result<Option<HashMap<CheetahString, CheetahString>>, PopContinuationError> {
    let Some(properties) = properties else {
        return Ok(None);
    };
    let mut owned = HashMap::new();
    owned
        .try_reserve(properties.len())
        .map_err(PopContinuationError::Allocation)?;
    owned.extend(properties.iter().map(|(key, value)| (key.clone(), value.clone())));
    Ok(Some(owned))
}
