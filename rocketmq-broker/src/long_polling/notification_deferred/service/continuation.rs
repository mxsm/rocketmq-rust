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
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use cheetah_string::CheetahString;

use super::NotificationContinuationOperationalError;
use super::NotificationContinuationOutcome;
use super::NotificationContinuationRejection;
use crate::long_polling::notification_deferred::index::NotificationArrivalView;
use crate::long_polling::notification_deferred::index::NotificationScanCursor;
use crate::long_polling::pending_arrival_latch::PendingArrivalValue;

pub(super) struct OwnedNotificationArrival {
    topic: CheetahString,
    queue_id: i32,
    tags_code: Option<i64>,
    msg_store_time: i64,
    filter_bit_map: Option<Vec<u8>>,
    properties: Option<HashMap<CheetahString, CheetahString>>,
}

impl OwnedNotificationArrival {
    pub(super) fn retained_bytes(
        arrival: NotificationArrivalView<'_>,
        cursor: &NotificationScanCursor,
    ) -> Result<usize, NotificationContinuationOperationalError> {
        let mut bytes = size_of::<NotificationArrivalContinuation>()
            .checked_add(
                cursor
                    .try_retained_bytes()
                    .ok_or(NotificationContinuationOperationalError::SizeOverflow)?,
            )
            .and_then(|total| total.checked_add(string_allocation_bound(arrival.topic.len())?))
            .ok_or(NotificationContinuationOperationalError::SizeOverflow)?;
        if let Some(bitmap) = arrival.filter_bit_map {
            let bitmap_bytes =
                byte_allocation_bound(bitmap.len()).ok_or(NotificationContinuationOperationalError::SizeOverflow)?;
            bytes = bytes
                .checked_add(bitmap_bytes)
                .ok_or(NotificationContinuationOperationalError::SizeOverflow)?;
        }
        if let Some(properties) = arrival.properties {
            let bucket_count = properties
                .len()
                .checked_mul(2)
                .and_then(|count| count.checked_add(1))
                .ok_or(NotificationContinuationOperationalError::SizeOverflow)?;
            let bucket_bytes = bucket_count
                .checked_mul(
                    size_of::<(CheetahString, CheetahString)>()
                        .checked_add(size_of::<u64>())
                        .and_then(|bytes| bytes.checked_add(1))
                        .ok_or(NotificationContinuationOperationalError::SizeOverflow)?,
                )
                .ok_or(NotificationContinuationOperationalError::SizeOverflow)?;
            bytes = bytes
                .checked_add(bucket_bytes)
                .ok_or(NotificationContinuationOperationalError::SizeOverflow)?;
            for (key, value) in properties {
                let key_bytes =
                    string_allocation_bound(key.len()).ok_or(NotificationContinuationOperationalError::SizeOverflow)?;
                let value_bytes = string_allocation_bound(value.len())
                    .ok_or(NotificationContinuationOperationalError::SizeOverflow)?;
                bytes = bytes
                    .checked_add(key_bytes)
                    .and_then(|total| total.checked_add(value_bytes))
                    .ok_or(NotificationContinuationOperationalError::SizeOverflow)?;
            }
        }
        Ok(bytes.max(1))
    }

    pub(super) fn try_from_view(
        arrival: NotificationArrivalView<'_>,
    ) -> Result<Self, NotificationContinuationOperationalError> {
        let filter_bit_map = match arrival.filter_bit_map {
            Some(bitmap) => {
                let mut owned = Vec::new();
                owned
                    .try_reserve_exact(bitmap.len())
                    .map_err(NotificationContinuationOperationalError::Allocation)?;
                owned.extend_from_slice(bitmap);
                Some(owned)
            }
            None => None,
        };
        let properties = match arrival.properties {
            Some(properties) => {
                let mut owned = HashMap::new();
                owned
                    .try_reserve(properties.len())
                    .map_err(NotificationContinuationOperationalError::Allocation)?;
                owned.extend(properties.iter().map(|(key, value)| (key.clone(), value.clone())));
                Some(owned)
            }
            None => None,
        };
        Ok(Self {
            topic: arrival.topic.clone(),
            queue_id: arrival.queue_id,
            tags_code: arrival.tags_code,
            msg_store_time: arrival.msg_store_time,
            filter_bit_map,
            properties,
        })
    }

    pub(super) fn view(&self) -> NotificationArrivalView<'_> {
        NotificationArrivalView::new(&self.topic, self.queue_id).with_filter_metadata(
            self.tags_code,
            self.msg_store_time,
            self.filter_bit_map.as_deref(),
            self.properties.as_ref(),
        )
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct NotificationPendingArrivalKey {
    sequence: u64,
    topic: CheetahString,
    queue_id: i32,
}

impl NotificationPendingArrivalKey {
    pub(crate) fn new(sequence: u64, topic: CheetahString, queue_id: i32) -> Self {
        Self {
            sequence,
            topic,
            queue_id,
        }
    }
}

pub(crate) struct NotificationPendingArrival {
    pub(super) owned: OwnedNotificationArrival,
    pub(super) cursor: NotificationScanCursor,
    pub(super) remaining_conflicts: usize,
    max_conflicts: usize,
    retained_bytes: usize,
}

impl NotificationPendingArrival {
    pub(super) fn new(
        arrival: NotificationArrivalView<'_>,
        cursor: NotificationScanCursor,
        remaining_conflicts: usize,
        max_conflicts: usize,
    ) -> Result<NotificationContinuationOutcome<Self>, NotificationContinuationOperationalError> {
        let retained_bytes = OwnedNotificationArrival::retained_bytes(arrival, &cursor)?;
        let owned = OwnedNotificationArrival::try_from_view(arrival)?;
        Ok(NotificationContinuationOutcome::Continued(Self {
            owned,
            cursor,
            remaining_conflicts,
            max_conflicts,
            retained_bytes,
        }))
    }
}

impl PendingArrivalValue for NotificationPendingArrival {
    fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    fn rewind_from_start(&mut self) {
        self.cursor.restart_for_refresh();
        self.remaining_conflicts = self.max_conflicts;
    }

    fn coalesce_refresh(&mut self) {
        self.rewind_from_start();
    }
}

#[must_use]
pub(crate) struct NotificationArrivalContinuation {
    pub(super) owned: OwnedNotificationArrival,
    pub(super) cursor: NotificationScanCursor,
    pub(super) remaining_conflicts: usize,
    pub(super) _permit: ContinuationPermit,
}

fn byte_allocation_bound(len: usize) -> Option<usize> {
    len.checked_mul(2)?.checked_add(size_of::<usize>())
}

fn string_allocation_bound(len: usize) -> Option<usize> {
    byte_allocation_bound(len)
}

pub(super) struct ContinuationAdmission {
    max_count: usize,
    max_bytes: usize,
    count: AtomicUsize,
    bytes: AtomicUsize,
    rejected: AtomicUsize,
}

impl ContinuationAdmission {
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
    ) -> Result<NotificationContinuationOutcome<ContinuationPermit>, NotificationContinuationOperationalError> {
        let count = self.count.fetch_add(1, Ordering::AcqRel);
        if count >= self.max_count {
            self.count.fetch_sub(1, Ordering::AcqRel);
            self.rejected.fetch_add(1, Ordering::Relaxed);
            return Ok(NotificationContinuationOutcome::Rejected(
                NotificationContinuationRejection::CountFull,
            ));
        }
        let mut current = self.bytes.load(Ordering::Acquire);
        loop {
            let Some(next) = current.checked_add(bytes) else {
                self.count.fetch_sub(1, Ordering::AcqRel);
                self.rejected.fetch_add(1, Ordering::Relaxed);
                return Err(NotificationContinuationOperationalError::SizeOverflow);
            };
            if next > self.max_bytes {
                self.count.fetch_sub(1, Ordering::AcqRel);
                self.rejected.fetch_add(1, Ordering::Relaxed);
                return Ok(NotificationContinuationOutcome::Rejected(
                    NotificationContinuationRejection::BytesFull,
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
        Ok(NotificationContinuationOutcome::Continued(ContinuationPermit {
            admission: Arc::clone(self),
            bytes,
        }))
    }

    pub(super) fn snapshot(&self) -> ContinuationSnapshot {
        ContinuationSnapshot {
            count: self.count.load(Ordering::Acquire),
            bytes: self.bytes.load(Ordering::Acquire),
            rejected: self.rejected.load(Ordering::Acquire),
        }
    }
}

pub(super) struct ContinuationPermit {
    admission: Arc<ContinuationAdmission>,
    bytes: usize,
}

impl Drop for ContinuationPermit {
    fn drop(&mut self) {
        self.admission.count.fetch_sub(1, Ordering::AcqRel);
        self.admission.bytes.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

pub(super) struct ContinuationSnapshot {
    pub(super) count: usize,
    pub(super) bytes: usize,
    pub(super) rejected: usize,
}
