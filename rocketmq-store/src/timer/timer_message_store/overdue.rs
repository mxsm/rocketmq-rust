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

//! Timers that are already due when the Source stage reads them.
//!
//! Apache RocketMQ writes a timer whose deadline has passed straight to its topic instead of
//! indexing it. Once the read cursor has passed a timer's slot, indexing puts the timer in the
//! read slot, where it waits for that slot to end, up to one `timerPrecisionMs` late. The Source
//! stage writes such a timer straight away instead. A timer whose slot the cursor has not passed
//! still joins that slot, where the recalls already indexed for it cancel it; a rolled timer can
//! reach the Timer CQ after its recall. The write also waits until every Timer CQ entry visible
//! before the slot ended is indexed, as a slot drain does, so it cannot pass over a recall the
//! slot would have honoured. Recall tombstones are always indexed.

use std::sync::atomic::Ordering;

use futures_util::stream::FuturesOrdered;
use futures_util::StreamExt;
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_store_local::timer::service::is_due_not_before;

use super::is_delete_timer_message;
use super::original_deliver_time_ms;
use super::parse_deliver_time_ms;
use super::real_write_target;
use super::timer_generation;
use super::validate_timer_source_route;
use super::SlotEntryAction;
use super::SlotEntryDone;
use super::TimerMessageStore;
use super::TimerWriteTarget;
use super::SLOT_DELIVERY_PIPELINE_DEPTH;
use crate::timer::source::TimerSourceRead;

/// What a Source batch does with a timer it could deliver now.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum OverdueTimers {
    /// Indexes it like any other timer.
    Index,
    /// Hands it to the caller, which writes it to its topic.
    Deliver,
}

impl OverdueTimers {
    /// Overdue timers are written only under a delivery epoch.
    pub(super) fn with_epoch(delivery_epoch: Option<u64>) -> Self {
        if delivery_epoch.is_some() {
            Self::Deliver
        } else {
            Self::Index
        }
    }
}

/// One Source batch.
#[derive(Default)]
pub(super) struct SourceBatch {
    /// Sources indexed, or left to the Extended timeline.
    pub(super) processed: usize,
    /// The overdue timers that follow them, in Timer CQ order. The enqueue cursor stays in front
    /// of each one until it is written.
    pub(super) overdue: Vec<OverdueTimer>,
}

/// A Source frame, checked for an overdue timer.
pub(super) enum SourceFrame {
    /// A timer to write to its topic.
    Overdue(OverdueTimer),
    /// A frame to index.
    Index(TimerSourceRead),
}

/// A timer of a slot that could be drained now, converted for its topic.
pub(super) struct OverdueTimer {
    source_offset: i64,
    slot_time_ms: i64,
    original_deliver_ms: i64,
    deliver_time_ms: i64,
    born_timestamp: i64,
    message: MessageExtBrokerInner,
}

/// The time a Source batch takes overdue timers against.
#[derive(Clone, Copy)]
pub(super) struct OverdueBound {
    now_ms: i64,
    floor_now_ms: i64,
    /// Slots before this one are behind the read cursor, and they ended before the clock and
    /// before the source visibility watermark.
    passed_slot_bound_ms: i64,
    precision_ms: i64,
}

/// Where an overdue timer waits and goes.
struct OverdueSlot {
    slot_time_ms: i64,
    original_deliver_ms: i64,
    deliver_time_ms: i64,
    target: TimerWriteTarget,
}

/// The result of one overdue timer write.
struct OverdueWrite {
    source_offset: i64,
    deliver_time_ms: i64,
    born_timestamp: i64,
    done: Option<SlotEntryDone>,
}

impl TimerMessageStore {
    /// The bound of a Source batch read at `wall_time_ms` with the read cursor at
    /// `read_time_ms`, or `None` when the batch indexes its overdue timers.
    pub(super) fn overdue_bound(
        &self,
        overdue: OverdueTimers,
        wall_time_ms: i64,
        read_time_ms: i64,
    ) -> Option<OverdueBound> {
        if overdue == OverdueTimers::Index {
            return None;
        }
        let floor_now_ms = self.floor_time_ms(wall_time_ms);
        let visible_through_ms = self.source_visible_through_ms.load(Ordering::Acquire);
        Some(OverdueBound {
            now_ms: wall_time_ms,
            floor_now_ms,
            passed_slot_bound_ms: floor_now_ms
                .min(self.floor_time_ms(visible_through_ms))
                .min(read_time_ms),
            precision_ms: self.precision_ms(),
        })
    }

    /// Takes `frame` as an overdue timer, or gives it back to be indexed. A recall tombstone, a
    /// timer whose slot is not behind `bound` and one that cannot reach its real topic all go to
    /// the wheel.
    pub(super) fn check_overdue(&self, frame: TimerSourceRead, source_offset: i64, bound: OverdueBound) -> SourceFrame {
        let slot = match &frame {
            TimerSourceRead::Frame { message, .. } => self.overdue_slot(message, bound),
            _ => None,
        };
        let Some(slot) = slot else {
            return SourceFrame::Index(frame);
        };
        match frame {
            TimerSourceRead::Frame { message, .. } => {
                let generation = timer_generation(&message);
                let born_timestamp = message.born_timestamp();
                let mut converted = Self::timer_write(message, slot.target);
                Self::ensure_delivery_token(&mut converted, source_offset, generation);
                SourceFrame::Overdue(OverdueTimer {
                    source_offset,
                    slot_time_ms: slot.slot_time_ms,
                    original_deliver_ms: slot.original_deliver_ms,
                    deliver_time_ms: slot.deliver_time_ms,
                    born_timestamp,
                    message: converted,
                })
            }
            other => SourceFrame::Index(other),
        }
    }

    fn overdue_slot(&self, message: &MessageExt, bound: OverdueBound) -> Option<OverdueSlot> {
        if validate_timer_source_route(message).is_err() || is_delete_timer_message(message) {
            return None;
        }
        let deliver_time_ms = parse_deliver_time_ms(message)?;
        // A timer this late never rolls, so this is its own slot.
        let slot_time_ms = self.ceil_time_ms(deliver_time_ms);
        if slot_time_ms >= bound.passed_slot_bound_ms {
            return None;
        }
        let original_deliver_ms = original_deliver_time_ms(message, bound.precision_ms)?;
        if !is_due_not_before(original_deliver_ms, slot_time_ms, bound.floor_now_ms, bound.now_ms) {
            return None;
        }
        Some(OverdueSlot {
            slot_time_ms,
            original_deliver_ms,
            deliver_time_ms,
            target: real_write_target(message).ok()?,
        })
    }

    /// Writes the overdue timers of a Source batch to their topics, in Timer CQ order, and moves
    /// the enqueue cursor past each one written or dropped. Returns how many it moved past.
    ///
    /// Up to `SLOT_DELIVERY_PIPELINE_DEPTH` writes are in flight, each checked against the clock
    /// and the delivery epoch first, as in a slot drain. The first timer that must stay ends the
    /// run, and the next Source batch reads it again. Writes already in flight behind it may
    /// land; their timers are delivered again with the same token, which at-least-once delivery
    /// allows.
    pub(super) async fn deliver_overdue_timers(&self, overdue: Vec<OverdueTimer>, delivery_epoch: u64) -> usize {
        if overdue.is_empty() {
            return 0;
        }
        let mut pending = overdue.into_iter();
        let mut in_flight = FuturesOrdered::new();
        let mut stopped = false;
        let mut passed = 0usize;
        let mut dequeued = 0usize;
        loop {
            while !stopped && in_flight.len() < SLOT_DELIVERY_PIPELINE_DEPTH {
                let Some(timer) = pending.next() else {
                    break;
                };
                in_flight.push_back(self.write_overdue_timer(timer, delivery_epoch));
            }
            let Some(write) = in_flight.next().await else {
                break;
            };
            if stopped {
                continue;
            }
            let Some(done) = write.done else {
                stopped = true;
                continue;
            };
            // A role change may rewind the cursor while the write is in flight; the timer is then
            // read again from there.
            if self
                .curr_queue_offset
                .compare_exchange(
                    write.source_offset,
                    write.source_offset + 1,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err()
            {
                stopped = true;
                continue;
            }
            let real_topic = done.counted_topic.as_ref().map(|topic| topic.as_str());
            self.otel_timer_metrics.record_enqueue_total(real_topic);
            self.otel_store_metrics.record_delay_message_latency_from_timestamps(
                write.deliver_time_ms,
                write.born_timestamp,
                real_topic,
            );
            if let Some(topic) = real_topic {
                self.otel_timer_metrics.record_dequeue_total(topic);
            }
            if done.dequeued {
                dequeued += 1;
            }
            passed += 1;
        }
        let now_ms = current_millis() as i64;
        self.enqueue_tps_counter.record(passed, now_ms);
        self.dequeue_tps_counter.record(dequeued, now_ms);
        self.advance_source_visibility();
        if passed > 0 {
            self.pending_progress.store(true, Ordering::Release);
        }
        passed
    }

    async fn write_overdue_timer(&self, timer: OverdueTimer, delivery_epoch: u64) -> OverdueWrite {
        let OverdueTimer {
            source_offset,
            slot_time_ms,
            original_deliver_ms,
            deliver_time_ms,
            born_timestamp,
            message,
        } = timer;
        let done = self
            .run_slot_entry(
                SlotEntryAction::Deliver {
                    source_offset,
                    message,
                    original_deliver_ms,
                },
                slot_time_ms,
                delivery_epoch,
            )
            .await;
        OverdueWrite {
            source_offset,
            deliver_time_ms,
            born_timestamp,
            done,
        }
    }
}
