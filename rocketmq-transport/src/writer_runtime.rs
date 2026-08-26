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

use std::num::NonZeroUsize;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use rocketmq_error::RocketMQError;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::admission::AdmissionClass;
use crate::connection::record_transport_write;
use crate::connection::ConnectionFrameWriter;
use crate::connection::ConnectionState;
use crate::connection::SessionWriterDiagnostics;
use crate::connection::SessionWriterSnapshot;
use crate::deadline::RequestDeadline;
use crate::telemetry::TransportTelemetry;
use crate::write_result::WriterFailure;
use crate::write_strategy::OutboundPayload;
use crate::write_strategy::QueuedWrite;
use crate::write_strategy::WriterOperation;

/// Hard bounds for one writer micro-batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MicroBatchConfig {
    pub max_items: NonZeroUsize,
    pub max_bytes: NonZeroUsize,
    pub max_delay: Duration,
    pub max_iov: NonZeroUsize,
}

impl Default for MicroBatchConfig {
    fn default() -> Self {
        Self {
            max_items: NonZeroUsize::new(8).expect("non-zero batch item default"),
            max_bytes: NonZeroUsize::new(256 * 1024).expect("non-zero batch byte default"),
            // Ready backlog is still batched immediately. A non-zero sub-millisecond timer is
            // opt-in because its effective delay can be one scheduler tick (about 15.6 ms on
            // default Windows timer resolution), which is unacceptable for singleton RPCs.
            max_delay: Duration::ZERO,
            max_iov: NonZeroUsize::new(64).expect("non-zero iovec default"),
        }
    }
}

/// Independent queue limits and fairness policy for control and data writes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WriterQueueConfig {
    pub data_capacity: NonZeroUsize,
    pub control_capacity: NonZeroUsize,
    pub data_max_bytes: NonZeroUsize,
    pub control_max_bytes: NonZeroUsize,
    pub control_burst: NonZeroUsize,
    pub max_write_stall: Duration,
    pub batch: MicroBatchConfig,
}

impl Default for WriterQueueConfig {
    fn default() -> Self {
        Self {
            data_capacity: NonZeroUsize::new(960).expect("non-zero data queue default"),
            control_capacity: NonZeroUsize::new(64).expect("non-zero control queue default"),
            data_max_bytes: NonZeroUsize::new(16 * 1024 * 1024).expect("non-zero data byte default"),
            control_max_bytes: NonZeroUsize::new(1024 * 1024).expect("non-zero control byte default"),
            control_burst: NonZeroUsize::new(8).expect("non-zero control burst default"),
            max_write_stall: Duration::from_secs(30),
            batch: MicroBatchConfig::default(),
        }
    }
}

impl WriterQueueConfig {
    pub(crate) fn validate(self) -> Result<Self, RocketMQError> {
        if self.max_write_stall.is_zero() {
            return Err(RocketMQError::network_connection_failed(
                "transport-writer-policy",
                "maximum write stall must be greater than zero",
            ));
        }
        Ok(self)
    }

    pub(crate) fn total_capacity(self) -> usize {
        self.data_capacity.get().saturating_add(self.control_capacity.get())
    }
}

struct LaneEnvelope {
    write: Option<QueuedWrite>,
    bytes: usize,
    queued_items: Arc<AtomicUsize>,
    queued_bytes: Arc<AtomicUsize>,
}

impl LaneEnvelope {
    fn into_write(mut self) -> QueuedWrite {
        self.write.take().expect("lane envelope owns one queued write")
    }

    fn encoded_len(&self) -> usize {
        self.bytes
    }
}

impl Drop for LaneEnvelope {
    fn drop(&mut self) {
        self.queued_items.fetch_sub(1, Ordering::AcqRel);
        self.queued_bytes.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

#[derive(Clone)]
struct LaneSender {
    sender: mpsc::Sender<LaneEnvelope>,
    max_bytes: usize,
    queued_items: Arc<AtomicUsize>,
    queued_bytes: Arc<AtomicUsize>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WriterEnqueueError {
    Full,
    Closed,
}

impl LaneSender {
    fn try_send(&self, write: QueuedWrite) -> Result<(), WriterEnqueueError> {
        let bytes = write.encoded_len();
        if !try_reserve_bytes(&self.queued_bytes, self.max_bytes, bytes) {
            return Err(WriterEnqueueError::Full);
        }
        self.queued_items.fetch_add(1, Ordering::AcqRel);
        let envelope = LaneEnvelope {
            write: Some(write),
            bytes,
            queued_items: Arc::clone(&self.queued_items),
            queued_bytes: Arc::clone(&self.queued_bytes),
        };
        match self.sender.try_send(envelope) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => Err(WriterEnqueueError::Full),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(WriterEnqueueError::Closed),
        }
    }
}

fn try_reserve_bytes(counter: &AtomicUsize, limit: usize, bytes: usize) -> bool {
    let mut current = counter.load(Ordering::Acquire);
    loop {
        let Some(next) = current.checked_add(bytes) else {
            return false;
        };
        if next > limit {
            return false;
        }
        match counter.compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return true,
            Err(observed) => current = observed,
        }
    }
}

struct CloseRequest {
    completion: oneshot::Sender<rocketmq_error::RocketMQResult<()>>,
}

/// Cloneable capability for the two bounded writer lanes and independent close signal.
#[derive(Clone)]
pub(crate) struct WriterLanes {
    control: LaneSender,
    data: LaneSender,
    close: mpsc::Sender<CloseRequest>,
}

impl WriterLanes {
    pub(crate) fn enrich_snapshot(&self, snapshot: &mut SessionWriterSnapshot) {
        snapshot.control_capacity = self.control.sender.max_capacity();
        snapshot.control_queued_items = self.control.queued_items.load(Ordering::Acquire);
        snapshot.control_queued_bytes = self.control.queued_bytes.load(Ordering::Acquire);
        snapshot.data_capacity = self.data.sender.max_capacity();
        snapshot.data_queued_items = self.data.queued_items.load(Ordering::Acquire);
        snapshot.data_queued_bytes = self.data.queued_bytes.load(Ordering::Acquire);
    }

    pub(crate) fn try_send(&self, class: AdmissionClass, write: QueuedWrite) -> Result<(), WriterEnqueueError> {
        match class {
            AdmissionClass::Control => self.control.try_send(write),
            AdmissionClass::Data => self.data.try_send(write),
        }
    }

    pub(crate) async fn close(
        &self,
        completion: oneshot::Sender<rocketmq_error::RocketMQResult<()>>,
    ) -> Result<(), mpsc::error::SendError<()>> {
        self.close
            .send(CloseRequest { completion })
            .await
            .map_err(|_| mpsc::error::SendError(()))
    }
}

pub(crate) struct WriterReceivers {
    control: mpsc::Receiver<LaneEnvelope>,
    data: mpsc::Receiver<LaneEnvelope>,
    close: mpsc::Receiver<CloseRequest>,
    deferred: Option<LaneEnvelope>,
    control_taken: usize,
    config: WriterQueueConfig,
}

pub(crate) fn writer_lanes(config: WriterQueueConfig) -> (WriterLanes, WriterReceivers) {
    let control_items = Arc::new(AtomicUsize::new(0));
    let control_bytes = Arc::new(AtomicUsize::new(0));
    let data_items = Arc::new(AtomicUsize::new(0));
    let data_bytes = Arc::new(AtomicUsize::new(0));
    let (control_tx, control_rx) = mpsc::channel(config.control_capacity.get());
    let (data_tx, data_rx) = mpsc::channel(config.data_capacity.get());
    let (close_tx, close_rx) = mpsc::channel(1);
    (
        WriterLanes {
            control: LaneSender {
                sender: control_tx,
                max_bytes: config.control_max_bytes.get(),
                queued_items: control_items,
                queued_bytes: control_bytes,
            },
            data: LaneSender {
                sender: data_tx,
                max_bytes: config.data_max_bytes.get(),
                queued_items: data_items,
                queued_bytes: data_bytes,
            },
            close: close_tx,
        },
        WriterReceivers {
            control: control_rx,
            data: data_rx,
            close: close_rx,
            deferred: None,
            control_taken: 0,
            config,
        },
    )
}

// Boxing `LaneEnvelope` would add one allocation to every writer dequeue on the transport hot path.
#[allow(
    clippy::large_enum_variant,
    reason = "the single writer actor keeps only one event live at a time"
)]
enum WriterEvent {
    Write(LaneEnvelope),
    Close(CloseRequest),
    Closed,
}

impl WriterReceivers {
    fn take_ready(&mut self) -> Option<LaneEnvelope> {
        if let Some(envelope) = self.deferred.take() {
            return Some(envelope);
        }
        if self.control_taken >= self.config.control_burst.get() {
            if let Ok(next) = self.data.try_recv() {
                self.observe(AdmissionClass::Data);
                return Some(next);
            }
        }
        if let Ok(next) = self.control.try_recv() {
            self.observe(AdmissionClass::Control);
            return Some(next);
        }
        if let Ok(next) = self.data.try_recv() {
            self.observe(AdmissionClass::Data);
            return Some(next);
        }
        None
    }

    fn observe(&mut self, class: AdmissionClass) {
        match class {
            AdmissionClass::Control => self.control_taken = self.control_taken.saturating_add(1),
            AdmissionClass::Data => self.control_taken = 0,
        }
    }

    async fn recv(&mut self) -> WriterEvent {
        if let Ok(close) = self.close.try_recv() {
            return WriterEvent::Close(close);
        }
        if let Some(next) = self.take_ready() {
            return WriterEvent::Write(next);
        }
        if self.close.is_closed() {
            return tokio::select! {
                biased;
                next = self.control.recv() => match next {
                    Some(next) => {
                        self.observe(AdmissionClass::Control);
                        WriterEvent::Write(next)
                    }
                    None => match self.data.recv().await {
                        Some(next) => {
                            self.observe(AdmissionClass::Data);
                            WriterEvent::Write(next)
                        }
                        None => WriterEvent::Closed,
                    },
                },
                next = self.data.recv() => match next {
                    Some(next) => {
                        self.observe(AdmissionClass::Data);
                        WriterEvent::Write(next)
                    }
                    None => match self.control.recv().await {
                        Some(next) => {
                            self.observe(AdmissionClass::Control);
                            WriterEvent::Write(next)
                        }
                        None => WriterEvent::Closed,
                    },
                },
            };
        }
        tokio::select! {
            biased;
            close = self.close.recv() => close.map_or(WriterEvent::Closed, WriterEvent::Close),
            next = self.control.recv() => match next {
                Some(next) => {
                    self.observe(AdmissionClass::Control);
                    WriterEvent::Write(next)
                }
                None => match self.data.recv().await {
                    Some(next) => {
                        self.observe(AdmissionClass::Data);
                        WriterEvent::Write(next)
                    }
                    None => WriterEvent::Closed,
                },
            },
            next = self.data.recv() => match next {
                Some(next) => {
                    self.observe(AdmissionClass::Data);
                    WriterEvent::Write(next)
                }
                None => match self.control.recv().await {
                    Some(next) => {
                        self.observe(AdmissionClass::Control);
                        WriterEvent::Write(next)
                    }
                    None => WriterEvent::Closed,
                },
            },
        }
    }

    #[cfg(test)]
    pub(crate) async fn drop_next_write_for_test(&mut self) {
        match self.recv().await {
            WriterEvent::Write(envelope) => drop(envelope),
            WriterEvent::Close(_) | WriterEvent::Closed => panic!("expected queued writer envelope"),
        }
    }

    fn collect_ready(&mut self, batch: &mut Vec<LaneEnvelope>) {
        while batch.len() < self.config.batch.max_items.get() {
            let Some(next) = self.take_ready() else {
                break;
            };
            let batch_bytes = batch
                .iter()
                .fold(0usize, |total, item| total.saturating_add(item.encoded_len()));
            if batch_bytes.saturating_add(next.encoded_len()) > self.config.batch.max_bytes.get() {
                self.deferred = Some(next);
                break;
            }
            batch.push(next);
        }
    }

    async fn collect_batch(&mut self, batch: &mut Vec<LaneEnvelope>) -> Option<CloseRequest> {
        self.collect_ready(batch);
        if batch.len() >= self.config.batch.max_items.get() || self.config.batch.max_delay.is_zero() {
            return None;
        }
        let deadline_remaining = batch
            .iter()
            .filter_map(|envelope| envelope.write.as_ref().and_then(|write| write.deadline))
            .map(RequestDeadline::remaining)
            .min();
        let wait = deadline_remaining.map_or(self.config.batch.max_delay, |remaining| {
            remaining.min(self.config.batch.max_delay)
        });
        if wait.is_zero() {
            return None;
        }
        match tokio::time::timeout(wait, self.recv()).await {
            Ok(WriterEvent::Write(next)) => {
                let bytes = batch
                    .iter()
                    .fold(0usize, |total, item| total.saturating_add(item.encoded_len()));
                if bytes.saturating_add(next.encoded_len()) <= self.config.batch.max_bytes.get() {
                    batch.push(next);
                    self.collect_ready(batch);
                } else {
                    self.deferred = Some(next);
                }
                None
            }
            Ok(WriterEvent::Close(close)) => Some(close),
            Ok(WriterEvent::Closed) | Err(_) => None,
        }
    }

    fn is_drained(&self) -> bool {
        self.deferred.is_none() && self.control.is_empty() && self.data.is_empty()
    }

    fn close_business_lanes(&mut self) {
        self.control.close();
        self.data.close();
    }

    fn fail_remaining(&mut self, diagnostics: &SessionWriterDiagnostics, failure: &WriterFailure) {
        if let Some(envelope) = self.deferred.take() {
            fail_envelope(envelope, diagnostics, failure);
        }
        while let Ok(envelope) = self.control.try_recv() {
            fail_envelope(envelope, diagnostics, failure);
        }
        while let Ok(envelope) = self.data.try_recv() {
            fail_envelope(envelope, diagnostics, failure);
        }
        while let Ok(close) = self.close.try_recv() {
            let _ = close.completion.send(Err(RocketMQError::network_connection_failed(
                "transport-session-writer",
                "connection writer is poisoned by a previous frame failure",
            )));
        }
    }
}

fn fail_envelope(envelope: LaneEnvelope, diagnostics: &SessionWriterDiagnostics, failure: &WriterFailure) {
    let write = envelope.into_write();
    diagnostics.finish_not_started(write.enqueued_at, write.encoded_len(), false);
    let _ = write.completion.send(Err(failure.clone()));
}

/// Runs the sole socket writer owner until graceful close, cancellation, or a poisoned write.
pub(crate) async fn run_session_writer(
    mut frame_writer: ConnectionFrameWriter,
    mut receivers: WriterReceivers,
    diagnostics: Arc<SessionWriterDiagnostics>,
    state: tokio::sync::watch::Sender<ConnectionState>,
    reader_shutdown: tokio_util::sync::CancellationToken,
    telemetry: TransportTelemetry,
) {
    let mut closing: Option<CloseRequest> = None;
    loop {
        if closing.is_some() && receivers.is_drained() {
            let result = frame_writer.shutdown().await.map_err(Into::into);
            if let Some(close) = closing.take() {
                let _ = close.completion.send(result);
            }
            break;
        }
        let event = if closing.is_some() {
            receivers.take_ready().map_or(WriterEvent::Closed, WriterEvent::Write)
        } else {
            receivers.recv().await
        };
        match event {
            WriterEvent::Write(first) => {
                let mut batch = Vec::with_capacity(receivers.config.batch.max_items.get());
                batch.push(first);
                let close_during_batch = receivers.collect_batch(&mut batch).await;
                match write_batch(&mut frame_writer, &diagnostics, &telemetry, receivers.config, batch).await {
                    BatchWriteDisposition::Continue => {}
                    BatchWriteDisposition::Poisoned => {
                        let _ = state.send(ConnectionState::Degraded);
                        receivers.close_business_lanes();
                        let _ = frame_writer.shutdown().await;
                        let drained = WriterFailure::connection_failed(
                            crate::dispatch::WriteProgress::NotStarted,
                            "connection writer is poisoned by a previous frame failure",
                        );
                        receivers.fail_remaining(&diagnostics, &drained);
                        if let Some(close) = closing.take() {
                            let _ = close.completion.send(Err(RocketMQError::network_connection_failed(
                                "transport-session-writer",
                                "connection writer failed during retirement",
                            )));
                        }
                        let _ = state.send(ConnectionState::Closed);
                        reader_shutdown.cancel();
                        break;
                    }
                }
                if let Some(close) = close_during_batch {
                    receivers.close_business_lanes();
                    closing = Some(close);
                }
            }
            WriterEvent::Close(close) => {
                receivers.close_business_lanes();
                closing = Some(close);
            }
            WriterEvent::Closed => {
                if closing.is_some() {
                    continue;
                }
                break;
            }
        }
    }
}

enum BatchWriteDisposition {
    Continue,
    Poisoned,
}

async fn write_batch(
    frame_writer: &mut ConnectionFrameWriter,
    diagnostics: &SessionWriterDiagnostics,
    telemetry: &TransportTelemetry,
    config: WriterQueueConfig,
    batch: Vec<LaneEnvelope>,
) -> BatchWriteDisposition {
    let mut ready = Vec::with_capacity(batch.len());
    for envelope in batch {
        let write = envelope.into_write();
        if !write.progress.claim_for_writer() {
            diagnostics.finish_not_started(write.enqueued_at, write.encoded_len(), true);
            let _ = write
                .completion
                .send(Err(WriterFailure::deadline_exceeded_before_send()));
            continue;
        }
        if write.deadline.is_some_and(RequestDeadline::is_expired) {
            diagnostics.finish_not_started(write.enqueued_at, write.encoded_len(), true);
            let _ = write
                .completion
                .send(Err(WriterFailure::deadline_exceeded_before_send()));
            continue;
        }
        ready.push(write);
    }
    if ready.is_empty() {
        return BatchWriteDisposition::Continue;
    }
    let stall_deadline = RequestDeadline::after(config.max_write_stall);
    let write_deadline = ready
        .iter()
        .filter_map(|write| write.deadline)
        .fold(stall_deadline, |earliest, next| {
            if next.instant() < earliest.instant() {
                next
            } else {
                earliest
            }
        });
    let payloads = ready
        .iter()
        .map(|write| match &write.operation {
            WriterOperation::Send(payload) => payload,
        })
        .collect::<Vec<&OutboundPayload>>();
    let start_data = ready
        .iter()
        .map(|write| ActiveWriteStart {
            progress: &write.progress,
            enqueued_at: write.enqueued_at,
            encoded_len: write.encoded_len(),
        })
        .collect::<Vec<_>>();
    let mut started_at = vec![None; ready.len()];
    let mut write_started = false;
    let result = {
        let mut mark_started = || {
            if write_started {
                return;
            }
            write_started = true;
            for (start, started_at) in start_data.iter().zip(started_at.iter_mut()) {
                start.progress.start_write();
                *started_at = Some(diagnostics.start_write(start.enqueued_at, start.encoded_len));
            }
        };
        match write_deadline
            .timeout(frame_writer.write_transport_payloads_with_start(
                &payloads,
                config.batch.max_iov.get(),
                &mut mark_started,
            ))
            .await
        {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(if write_started {
                WriterFailure::from_io(crate::dispatch::WriteProgress::PossiblyPartial, error)
            } else {
                WriterFailure::from_io(crate::dispatch::WriteProgress::NotStarted, error)
            }),
            Err(_) => Err(if write_started {
                WriterFailure::write_timeout(write_deadline.budget_millis())
            } else {
                WriterFailure::deadline_exceeded_before_send()
            }),
        }
    };
    drop(start_data);
    let succeeded = result.is_ok();
    if succeeded {
        let written_bytes = ready
            .iter()
            .fold(0usize, |total, write| total.saturating_add(write.encoded_len()));
        telemetry.record_outbound_written_plaintext_bytes(written_bytes);
    }
    for (write, started_at) in ready.into_iter().zip(started_at) {
        let encoded_len = write.encoded_len();
        if let Some(started_at) = started_at {
            diagnostics.finish_write(started_at, succeeded, !succeeded && write_deadline.is_expired());
        } else {
            diagnostics.finish_not_started(
                write.enqueued_at,
                encoded_len,
                !succeeded && write_deadline.is_expired(),
            );
        }
        drop(write.permit);
        let completion = match &result {
            Ok(()) => {
                record_transport_write(encoded_len);
                Ok(())
            }
            Err(failure) => Err(failure.clone()),
        };
        let _ = write.completion.send(completion);
    }
    if result
        .as_ref()
        .err()
        .is_some_and(|failure| failure.progress() == crate::dispatch::WriteProgress::PossiblyPartial)
        || frame_writer.is_poisoned()
    {
        BatchWriteDisposition::Poisoned
    } else {
        BatchWriteDisposition::Continue
    }
}

struct ActiveWriteStart<'a> {
    progress: &'a crate::write_strategy::QueuedWriteProgress,
    enqueued_at: Option<Instant>,
    encoded_len: usize,
}

#[cfg(test)]
#[path = "writer_runtime/issue_9754_tests.rs"]
mod tests;

#[cfg(test)]
mod queue_regression_tests {
    use std::num::NonZeroUsize;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;
    use std::time::Instant;

    use bytes::Bytes;
    use tokio::sync::oneshot;

    use super::try_reserve_bytes;
    use super::writer_lanes;
    use super::LaneEnvelope;
    use super::MicroBatchConfig;
    use super::WriterEvent;
    use super::WriterQueueConfig;
    use crate::admission::AdmissionClass;
    use crate::admission::AdmissionController;
    use crate::admission::AdmissionLimits;
    use crate::admission::AdmissionResource;
    use crate::admission::AdmissionScope;
    use crate::write_strategy::OutboundPayload;
    use crate::write_strategy::QueuedWrite;
    use crate::write_strategy::QueuedWriteProgress;

    fn queued_write(target: &'static str, bytes: usize) -> QueuedWrite {
        let admission = AdmissionController::new(AdmissionLimits::default());
        let handle = admission
            .prepare_scope(AdmissionScope::new("127.0.0.1".parse().expect("loopback")))
            .expect("prepare scope");
        let permit = handle
            .try_acquire(AdmissionResource::Queued, bytes, AdmissionClass::Data)
            .expect("queue admission");
        let (completion, _result) = oneshot::channel();
        QueuedWrite::data(
            OutboundPayload::Contiguous(Bytes::from(vec![0x5a; bytes])),
            completion,
            permit,
            None,
            target.to_string(),
            Arc::new(QueuedWriteProgress::waiting()),
            Instant::now(),
        )
    }

    fn queue_config() -> WriterQueueConfig {
        WriterQueueConfig {
            data_capacity: NonZeroUsize::new(4).expect("non-zero"),
            control_capacity: NonZeroUsize::new(4).expect("non-zero"),
            data_max_bytes: NonZeroUsize::new(256).expect("non-zero"),
            control_max_bytes: NonZeroUsize::new(256).expect("non-zero"),
            control_burst: NonZeroUsize::new(2).expect("non-zero"),
            max_write_stall: Duration::from_secs(1),
            batch: MicroBatchConfig {
                max_items: NonZeroUsize::new(3).expect("non-zero"),
                max_bytes: NonZeroUsize::new(128).expect("non-zero"),
                max_delay: Duration::ZERO,
                max_iov: NonZeroUsize::new(8).expect("non-zero"),
            },
        }
    }

    #[test]
    fn byte_reservation_is_bounded() {
        let bytes = AtomicUsize::new(0);
        assert!(try_reserve_bytes(&bytes, 8, 8));
        assert!(!try_reserve_bytes(&bytes, 8, 1));
        assert_eq!(bytes.load(Ordering::Acquire), 8);
    }

    #[tokio::test]
    async fn independent_control_lane_and_close_remain_available_when_data_is_full() {
        let mut config = queue_config();
        config.data_capacity = NonZeroUsize::new(1).expect("non-zero");
        let (lanes, mut receivers) = writer_lanes(config);
        assert!(lanes.try_send(AdmissionClass::Data, queued_write("data-1", 64)).is_ok());
        assert!(lanes
            .try_send(AdmissionClass::Data, queued_write("data-2", 64))
            .is_err());
        assert!(lanes
            .try_send(AdmissionClass::Control, queued_write("control", 64))
            .is_ok());
        let (completion, _result) = oneshot::channel();
        lanes
            .close(completion)
            .await
            .expect("close signal has an independent lane");

        assert!(matches!(receivers.recv().await, WriterEvent::Close(_)));
    }

    #[test]
    fn weighted_fairness_advances_data_after_the_control_burst() {
        let (lanes, mut receivers) = writer_lanes(queue_config());
        for target in ["control-1", "control-2", "control-3"] {
            assert!(lanes
                .try_send(AdmissionClass::Control, queued_write(target, 16))
                .is_ok());
        }
        assert!(lanes.try_send(AdmissionClass::Data, queued_write("data", 16)).is_ok());

        let targets = (0..3)
            .map(|_| receivers.take_ready().expect("ready item").into_write().target)
            .collect::<Vec<_>>();
        assert_eq!(targets, ["control-1", "control-2", "data"]);
    }

    #[tokio::test]
    async fn closed_close_sender_does_not_drop_queued_business_work() {
        let (lanes, mut receivers) = writer_lanes(queue_config());
        assert!(lanes
            .try_send(AdmissionClass::Data, queued_write("queued-data", 16))
            .is_ok());
        drop(lanes);

        let WriterEvent::Write(envelope) = receivers.recv().await else {
            panic!("queued work must drain after all send handles are dropped");
        };
        assert_eq!(envelope.into_write().target, "queued-data");
    }

    #[tokio::test]
    async fn micro_batch_respects_item_and_byte_bounds_without_waiting_for_backlog() {
        let (lanes, mut receivers) = writer_lanes(queue_config());
        for target in ["first", "second", "third"] {
            assert!(lanes.try_send(AdmissionClass::Data, queued_write(target, 64)).is_ok());
        }
        let WriterEvent::Write(first) = receivers.recv().await else {
            panic!("first queued write");
        };
        let mut batch = vec![first];
        assert!(receivers.collect_batch(&mut batch).await.is_none());

        assert_eq!(batch.len(), 2);
        assert_eq!(batch.iter().map(LaneEnvelope::encoded_len).sum::<usize>(), 128);
        assert!(receivers.deferred.is_some());

        let mut item_config = queue_config();
        item_config.batch.max_items = NonZeroUsize::new(2).expect("non-zero");
        item_config.batch.max_bytes = NonZeroUsize::new(256).expect("non-zero");
        let (item_lanes, mut item_receivers) = writer_lanes(item_config);
        for target in ["item-first", "item-second", "item-third"] {
            assert!(item_lanes
                .try_send(AdmissionClass::Data, queued_write(target, 64))
                .is_ok());
        }
        let WriterEvent::Write(first) = item_receivers.recv().await else {
            panic!("first item-bounded write");
        };
        let mut item_batch = vec![first];
        assert!(item_receivers.collect_batch(&mut item_batch).await.is_none());
        assert_eq!(item_batch.len(), 2);
        assert_eq!(
            item_receivers
                .take_ready()
                .expect("third item remains queued")
                .into_write()
                .target,
            "item-third"
        );
    }
}
