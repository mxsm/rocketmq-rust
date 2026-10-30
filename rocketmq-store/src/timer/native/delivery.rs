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

//! Delivery of the `native` engine's due timers, in three parts so the blocking reads stay off
//! the async runtime: [`NativeTimerEngine::prepare_due`] reads each due timer's message and
//! decides what to do with it, [`NativeDueBatch::write`] writes the messages to their real
//! topics, and [`NativeTimerEngine::settle`] moves the lane's watermark over the timers done.

use std::future::Future;

use bytes::Bytes;
use futures_util::future::ready;
use futures_util::future::Either;
use futures_util::stream::FuturesOrdered;
use futures_util::FutureExt;
use futures_util::StreamExt;
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_protocol::common::message::message_decoder as MessageDecoder;
use rocketmq_store_local::timer::native_format::NativeDueCursor;
use rocketmq_store_local::timer::native_format::NativeIndexEntry;
use rocketmq_store_local::timer::native_format::NativeLocator;

use super::engine::NativeDueTimer;
use super::engine::NativeEngineError;
use super::engine::NativeTimerEngine;
use crate::timer::timer_message_store::real_topic_write;
use crate::timer::timer_message_store::recall_cancels;

/// Reads the CommitLog record a due timer points at.
pub(crate) trait NativeCommitLogReader {
    fn read(&self, offset: i64, size: i32) -> NativeCommitLogRead;
}

#[allow(
    clippy::large_enum_variant,
    reason = "a read is moved out at once; boxing would add an allocation per due timer"
)]
pub(crate) enum NativeCommitLogRead {
    Message(MessageExt),
    /// CommitLog cleanup deleted the record.
    CleanedUp,
    /// The record is not there or does not decode.
    Unreadable,
}

/// Writes a due timer's message to its real topic.
pub(crate) trait NativeTimerWriter {
    /// Writes `message` for delivery epoch `epoch`, and says what the write means for its
    /// timer: a write for an epoch that is no longer current must stay.
    fn write(&self, message: MessageExtBrokerInner, epoch: u64) -> impl Future<Output = NativeWriteOutcome> + Send;
}

/// What a delivery write means for its timer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NativeWriteOutcome {
    /// The message reached its topic.
    Written,
    /// The timer leaves without its message, as `timerSkipUnknownError` or
    /// `timerEnableRetryUntilSuccess = false` allows.
    Dropped,
    /// The timer waits for a later attempt.
    Stays,
}

/// Why a due timer leaves without a write.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NativeSettled {
    /// A recall cancelled it.
    Recalled,
    /// CommitLog cleanup deleted its record.
    CleanedUp,
    /// Its record could not be read, and `timerSkipUnknownError` lets it go.
    Unreadable,
    /// Its message names no real topic or an invalid real queue, and `timerSkipUnknownError`
    /// lets it go.
    Unroutable,
}

/// What became of a due timer of a written batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NativeOutcome {
    Written,
    Dropped,
    Settled(NativeSettled),
    /// Back in the wheel for a later batch.
    Stays,
}

impl From<NativeWriteOutcome> for NativeOutcome {
    fn from(outcome: NativeWriteOutcome) -> Self {
        match outcome {
            NativeWriteOutcome::Written => Self::Written,
            NativeWriteOutcome::Dropped => Self::Dropped,
            NativeWriteOutcome::Stays => Self::Stays,
        }
    }
}

#[allow(
    clippy::large_enum_variant,
    reason = "a batch holds each due timer's write once; boxing would add an allocation per timer"
)]
enum NativeDelivery {
    Write {
        entry: NativeIndexEntry,
        message: MessageExtBrokerInner,
    },
    Settled {
        entry: NativeIndexEntry,
        reason: NativeSettled,
    },
}

impl NativeDelivery {
    fn entry(&self) -> &NativeIndexEntry {
        match self {
            Self::Write { entry, .. } | Self::Settled { entry, .. } => entry,
        }
    }
}

/// Due timers of one lane, their messages read, in the lane's order.
pub(crate) struct NativeDueBatch {
    lane: u16,
    deliveries: Vec<NativeDelivery>,
    /// The timer that stopped the batch: its record could not be read or routed, and
    /// `timerSkipUnknownError` is off. It waits in the wheel with the timers after it, and
    /// stops every batch of the lane until it can be delivered.
    pub(crate) blocked_at: Option<i64>,
}

/// The outcome of each timer of a written batch, in the lane's order.
pub(crate) struct NativeWrittenBatch {
    lane: u16,
    outcomes: Vec<(NativeIndexEntry, NativeOutcome)>,
}

/// What [`NativeTimerEngine::settle`] did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct NativeDeliveryReport {
    pub(crate) written: usize,
    pub(crate) dropped: usize,
    pub(crate) recalled: usize,
    pub(crate) cleaned_up: usize,
    /// Timers that left without a write because their record could not be read or routed.
    pub(crate) skipped: usize,
    /// Timers back in the wheel for a later batch.
    pub(crate) requeued: usize,
}

impl NativeTimerEngine {
    /// Takes up to `max` due timers of `lane` (see [`Self::poll_due`]) and reads each one's
    /// message, from the CommitLog through `reader` or from its copy. A recall that names the
    /// message cancels the timer; otherwise the message is converted into the write to its real
    /// topic, as the Java-compatible engine converts it. The reads are blocking.
    ///
    /// A timer whose CommitLog record cleanup deleted leaves without a write. One whose record
    /// cannot be read or routed leaves the same way when `skip_unknown_error` is set; otherwise
    /// it stops the batch and waits in the wheel with the timers after it.
    ///
    /// # Errors
    ///
    /// Returns a storage error when a bucket cannot be loaded; nothing is taken then.
    pub(crate) fn prepare_due(
        &self,
        lane: u16,
        now_ms: i64,
        max: usize,
        reader: &impl NativeCommitLogReader,
        skip_unknown_error: bool,
    ) -> Result<NativeDueBatch, NativeEngineError> {
        let mut due = self.poll_due(lane, now_ms, max)?.into_iter();
        let mut batch = NativeDueBatch {
            lane,
            deliveries: Vec::with_capacity(due.len()),
            blocked_at: None,
        };
        while let Some(timer) = due.next() {
            let delivery = self.delivery_of(timer, reader);
            if let NativeDelivery::Settled {
                entry,
                reason: NativeSettled::Unreadable | NativeSettled::Unroutable,
            } = &delivery
            {
                if !skip_unknown_error {
                    batch.blocked_at = Some(entry.cq_offset);
                    let blocked = *entry;
                    self.requeue(std::iter::once(blocked).chain(due.map(|timer| timer.entry)));
                    break;
                }
            }
            batch.deliveries.push(delivery);
        }
        Ok(batch)
    }

    /// Records the outcomes of a written batch. The lane's watermark moves over the timers done
    /// in front of the first that stays, and that timer goes back into the wheel with every
    /// timer after it, whatever their outcomes, so the lane keeps its order.
    pub(crate) fn settle(&self, batch: NativeWrittenBatch) -> NativeDeliveryReport {
        let mut report = NativeDeliveryReport::default();
        let mut through: Option<NativeDueCursor> = None;
        let mut outcomes = batch.outcomes.into_iter();
        let mut stays = None;
        for (entry, outcome) in outcomes.by_ref() {
            match outcome {
                NativeOutcome::Written => report.written += 1,
                NativeOutcome::Dropped => report.dropped += 1,
                NativeOutcome::Settled(NativeSettled::Recalled) => report.recalled += 1,
                NativeOutcome::Settled(NativeSettled::CleanedUp) => report.cleaned_up += 1,
                NativeOutcome::Settled(NativeSettled::Unreadable | NativeSettled::Unroutable) => report.skipped += 1,
                NativeOutcome::Stays => {
                    stays = Some(entry);
                    break;
                }
            }
            through = Some(entry.cursor());
        }
        if let Some(through) = through {
            self.advance_delivered(batch.lane, through);
        }
        if let Some(first) = stays {
            let requeued: Vec<_> = std::iter::once(first).chain(outcomes.map(|(entry, _)| entry)).collect();
            report.requeued = requeued.len();
            self.requeue(requeued);
        }
        report
    }

    fn delivery_of(&self, timer: NativeDueTimer, reader: &impl NativeCommitLogReader) -> NativeDelivery {
        let NativeDueTimer { entry, recalls } = timer;
        let message = match self.message_of(&entry, reader) {
            Ok(message) => message,
            Err(reason) => return NativeDelivery::Settled { entry, reason },
        };
        if recalls
            .iter()
            .any(|recall| recall_cancels(&message, &recall.key, recall.generation))
        {
            return NativeDelivery::Settled {
                entry,
                reason: NativeSettled::Recalled,
            };
        }
        match real_topic_write(message, entry.cq_offset) {
            Some(message) => NativeDelivery::Write { entry, message },
            None => NativeDelivery::Settled {
                entry,
                reason: NativeSettled::Unroutable,
            },
        }
    }

    /// The timer message of `entry`, or why it cannot be read.
    fn message_of(
        &self,
        entry: &NativeIndexEntry,
        reader: &impl NativeCommitLogReader,
    ) -> Result<MessageExt, NativeSettled> {
        match entry.locator {
            NativeLocator::CommitLog { offset } => match reader.read(offset, entry.size) {
                // A record of another Timer CQ offset means the entry points at the wrong one.
                NativeCommitLogRead::Message(message) if message.queue_offset() == entry.cq_offset => Ok(message),
                NativeCommitLogRead::Message(_) | NativeCommitLogRead::Unreadable => Err(NativeSettled::Unreadable),
                NativeCommitLogRead::CleanedUp => Err(NativeSettled::CleanedUp),
            },
            NativeLocator::Payload { .. } => {
                let record = self.read_payload(entry).map_err(|_| NativeSettled::Unreadable)?;
                if record.source_cq_offset.get() != entry.cq_offset {
                    return Err(NativeSettled::Unreadable);
                }
                let mut frame = Bytes::from(record.frame);
                MessageDecoder::decode(&mut frame, true, false, false, false, false).ok_or(NativeSettled::Unreadable)
            }
        }
    }
}

impl NativeDueBatch {
    pub(crate) fn len(&self) -> usize {
        self.deliveries.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.deliveries.is_empty()
    }

    /// Writes the batch's messages through `writer`, at most `max_in_flight` at a time so the
    /// CommitLog can merge them, and takes their outcomes in the lane's order. The first write
    /// that stays stops further writes. Writes already in flight behind it may still land;
    /// their timers stay too and are written again with the same delivery token, which
    /// at-least-once delivery allows.
    pub(crate) async fn write(
        self,
        writer: &impl NativeTimerWriter,
        epoch: u64,
        max_in_flight: usize,
    ) -> NativeWrittenBatch {
        let mut entries = Vec::with_capacity(self.deliveries.len());
        let mut outcomes = Vec::with_capacity(self.deliveries.len());
        let mut deliveries = self.deliveries.into_iter();
        let mut in_flight = FuturesOrdered::new();
        let mut stopped = false;
        loop {
            while !stopped && in_flight.len() < max_in_flight.max(1) {
                let Some(delivery) = deliveries.next() else {
                    break;
                };
                in_flight.push_back(match delivery {
                    NativeDelivery::Write { entry, message } => {
                        entries.push(entry);
                        Either::Left(writer.write(message, epoch).map(NativeOutcome::from))
                    }
                    NativeDelivery::Settled { entry, reason } => {
                        entries.push(entry);
                        Either::Right(ready(NativeOutcome::Settled(reason)))
                    }
                });
            }
            let Some(outcome) = in_flight.next().await else {
                break;
            };
            stopped |= outcome == NativeOutcome::Stays;
            outcomes.push(outcome);
        }
        for delivery in deliveries {
            entries.push(*delivery.entry());
            outcomes.push(NativeOutcome::Stays);
        }
        NativeWrittenBatch {
            lane: self.lane,
            outcomes: entries.into_iter().zip(outcomes).collect(),
        }
    }
}
