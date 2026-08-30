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
use std::mem::size_of;
use std::num::NonZeroUsize;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_transport::api::DeferredWakeReason;

use crate::long_polling::pending_arrival_latch::PendingArrivalValue;
use crate::long_polling::pull_deferred::index::PullArrivalView;
use crate::long_polling::pull_deferred::index::PullCandidateBatch;
use crate::long_polling::pull_deferred::index::PullCriteriaIndex;
use crate::long_polling::pull_deferred::index::PullCriteriaKey;
use crate::long_polling::pull_deferred::index::PullScanCursor;

enum PullContinuationKind {
    Arrival(OwnedPullArrival),
    Forced,
}

struct OwnedPullArrival {
    topic: CheetahString,
    queue_id: i32,
    max_offset: i64,
    tags_code: Option<i64>,
    message_store_time: i64,
    filter_bitmap: Option<Vec<u8>>,
    properties: Option<HashMap<CheetahString, CheetahString>>,
    forced: bool,
}

impl OwnedPullArrival {
    fn retained_bytes(arrival: PullArrivalView<'_>) -> Result<usize, PullContinuationError> {
        retained_arrival_bytes(
            size_of::<PullArrivalContinuation>(),
            arrival.topic().len(),
            arrival.filter_bitmap(),
            arrival.properties(),
        )
    }

    fn try_from_view(arrival: PullArrivalView<'_>) -> Result<Self, PullContinuationError> {
        let filter_bitmap = copy_bitmap(arrival.filter_bitmap())?;
        let properties = copy_properties(arrival.properties())?;
        Ok(Self {
            topic: arrival.topic().clone(),
            queue_id: arrival.queue_id(),
            max_offset: arrival.max_offset(),
            tags_code: arrival.tags_code(),
            message_store_time: arrival.message_store_time(),
            filter_bitmap,
            properties,
            forced: arrival.is_forced(),
        })
    }

    fn view(&self) -> PullArrivalView<'_> {
        let arrival = PullArrivalView::new(&self.topic, self.queue_id, self.max_offset).with_filter_metadata(
            self.tags_code,
            self.message_store_time,
            self.filter_bitmap.as_deref(),
            self.properties.as_ref(),
        );
        if self.forced {
            arrival.forced()
        } else {
            arrival
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) enum PullPendingArrivalKey {
    Arrival(u64, PullCriteriaKey),
    Forced,
}

pub(crate) struct PullPendingArrival {
    kind: PullContinuationKind,
    cursor: PullScanCursor,
    reason: DeferredWakeReason,
    retained_bytes: usize,
}

impl PullPendingArrival {
    pub(super) fn arrival(arrival: PullArrivalView<'_>, cursor: PullScanCursor) -> Result<Self, PullContinuationError> {
        let retained_bytes = OwnedPullArrival::retained_bytes(arrival)?;
        let owned = OwnedPullArrival::try_from_view(arrival)?;
        Ok(Self {
            kind: PullContinuationKind::Arrival(owned),
            cursor,
            reason: DeferredWakeReason::MessageArrived,
            retained_bytes,
        })
    }

    pub(super) fn forced(cursor: PullScanCursor) -> Self {
        Self {
            kind: PullContinuationKind::Forced,
            cursor,
            reason: DeferredWakeReason::ForcedRefresh,
            retained_bytes: size_of::<Self>().max(1),
        }
    }

    pub(crate) const fn reason(&self) -> DeferredWakeReason {
        self.reason
    }

    pub(super) fn reserve_next(
        &mut self,
        index: &PullCriteriaIndex,
        scan_limit: NonZeroUsize,
        candidate_limit: NonZeroUsize,
    ) -> PullCandidateBatch {
        match &self.kind {
            PullContinuationKind::Arrival(owned) => {
                index.reserve_matching_batch(&owned.view(), &mut self.cursor, scan_limit, candidate_limit)
            }
            PullContinuationKind::Forced => index.reserve_forced_batch(&mut self.cursor, scan_limit, candidate_limit),
        }
    }
}

impl PendingArrivalValue for PullPendingArrival {
    fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    fn rewind_from_start(&mut self) {
        self.cursor = PullScanCursor::new();
    }

    fn coalesce_refresh(&mut self) {
        self.rewind_from_start();
    }
}

#[must_use]
pub(crate) struct PullArrivalContinuation {
    pending: PullPendingArrival,
    _permit: PullContinuationPermit,
}

impl PullArrivalContinuation {
    pub(super) fn arrival(
        admission: &Arc<PullContinuationAdmission>,
        arrival: PullArrivalView<'_>,
        cursor: PullScanCursor,
    ) -> Result<Self, PullContinuationError> {
        let pending = PullPendingArrival::arrival(arrival, cursor)?;
        let retained_bytes = pending.retained_bytes();
        let permit = admission.reserve(retained_bytes)?;
        Ok(Self {
            pending,
            _permit: permit,
        })
    }

    pub(super) fn forced(
        admission: &Arc<PullContinuationAdmission>,
        cursor: PullScanCursor,
    ) -> Result<Self, PullContinuationError> {
        let pending = PullPendingArrival::forced(cursor);
        let permit = admission.reserve(pending.retained_bytes())?;
        Ok(Self {
            pending,
            _permit: permit,
        })
    }

    pub(super) fn reserve_next(
        &mut self,
        index: &PullCriteriaIndex,
        scan_limit: NonZeroUsize,
        candidate_limit: NonZeroUsize,
    ) -> PullCandidateBatch {
        self.pending.reserve_next(index, scan_limit, candidate_limit)
    }
}

pub(super) struct PullContinuationAdmission {
    max_count: usize,
    max_bytes: usize,
    count: AtomicUsize,
    bytes: AtomicUsize,
    rejected: AtomicUsize,
}

impl PullContinuationAdmission {
    pub(super) const fn new(max_count: usize, max_bytes: usize) -> Self {
        Self {
            max_count,
            max_bytes,
            count: AtomicUsize::new(0),
            bytes: AtomicUsize::new(0),
            rejected: AtomicUsize::new(0),
        }
    }

    pub(super) fn reserve(self: &Arc<Self>, bytes: usize) -> Result<PullContinuationPermit, PullContinuationError> {
        let count = self.count.fetch_add(1, Ordering::AcqRel);
        if count >= self.max_count {
            self.count.fetch_sub(1, Ordering::AcqRel);
            self.rejected.fetch_add(1, Ordering::Relaxed);
            return Err(PullContinuationError::CountFull);
        }
        let mut current = self.bytes.load(Ordering::Acquire);
        loop {
            let Some(next) = current.checked_add(bytes) else {
                self.reject_count();
                return Err(PullContinuationError::SizeOverflow);
            };
            if next > self.max_bytes {
                self.reject_count();
                return Err(PullContinuationError::BytesFull);
            }
            match self
                .bytes
                .compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
        Ok(PullContinuationPermit {
            admission: Arc::clone(self),
            bytes,
        })
    }

    fn reject_count(&self) {
        self.count.fetch_sub(1, Ordering::AcqRel);
        self.rejected.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn snapshot(&self) -> PullContinuationSnapshot {
        PullContinuationSnapshot {
            count: self.count.load(Ordering::Acquire),
            bytes: self.bytes.load(Ordering::Acquire),
            rejected: self.rejected.load(Ordering::Acquire),
        }
    }
}

pub(super) struct PullContinuationPermit {
    admission: Arc<PullContinuationAdmission>,
    bytes: usize,
}

impl Drop for PullContinuationPermit {
    fn drop(&mut self) {
        self.admission.count.fetch_sub(1, Ordering::AcqRel);
        self.admission.bytes.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

pub(super) struct PullContinuationSnapshot {
    pub(super) count: usize,
    pub(super) bytes: usize,
    pub(super) rejected: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PullContinuationError {
    CountFull,
    BytesFull,
    SizeOverflow,
    Allocation,
}

fn retained_arrival_bytes(
    base: usize,
    topic_len: usize,
    bitmap: Option<&[u8]>,
    properties: Option<&HashMap<CheetahString, CheetahString>>,
) -> Result<usize, PullContinuationError> {
    let mut bytes = base
        .checked_add(allocation_bound(topic_len)?)
        .ok_or(PullContinuationError::SizeOverflow)?;
    if let Some(bitmap) = bitmap {
        bytes = bytes
            .checked_add(allocation_bound(bitmap.len())?)
            .ok_or(PullContinuationError::SizeOverflow)?;
    }
    if let Some(properties) = properties {
        let buckets = properties
            .len()
            .checked_mul(size_of::<(CheetahString, CheetahString)>() + size_of::<u64>() + 1)
            .ok_or(PullContinuationError::SizeOverflow)?;
        bytes = bytes.checked_add(buckets).ok_or(PullContinuationError::SizeOverflow)?;
        for (key, value) in properties {
            bytes = bytes
                .checked_add(allocation_bound(key.len())?)
                .and_then(|total| total.checked_add(allocation_bound(value.len()).ok()?))
                .ok_or(PullContinuationError::SizeOverflow)?;
        }
    }
    Ok(bytes.max(1))
}

fn allocation_bound(len: usize) -> Result<usize, PullContinuationError> {
    len.checked_mul(2)
        .and_then(|bytes| bytes.checked_add(size_of::<usize>()))
        .ok_or(PullContinuationError::SizeOverflow)
}

fn copy_bitmap(bitmap: Option<&[u8]>) -> Result<Option<Vec<u8>>, PullContinuationError> {
    let Some(bitmap) = bitmap else {
        return Ok(None);
    };
    let mut owned = Vec::new();
    owned
        .try_reserve_exact(bitmap.len())
        .map_err(|_| PullContinuationError::Allocation)?;
    owned.extend_from_slice(bitmap);
    Ok(Some(owned))
}

fn copy_properties(
    properties: Option<&HashMap<CheetahString, CheetahString>>,
) -> Result<Option<HashMap<CheetahString, CheetahString>>, PullContinuationError> {
    let Some(properties) = properties else {
        return Ok(None);
    };
    let mut owned = HashMap::new();
    owned
        .try_reserve(properties.len())
        .map_err(|_| PullContinuationError::Allocation)?;
    owned.extend(properties.iter().map(|(key, value)| (key.clone(), value.clone())));
    Ok(Some(owned))
}
