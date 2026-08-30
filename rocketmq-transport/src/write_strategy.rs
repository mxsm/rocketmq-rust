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

use std::io;
use std::io::IoSlice;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use bytes::BytesMut;
use rocketmq_error::SerializationError;
use rocketmq_protocol::protocol::encoded_frame::EncodedFrame;
use rocketmq_protocol::protocol::encoded_frame::EncodedFrameHead;
use rocketmq_runtime::BlockingExecutor;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::sync::oneshot;

use crate::admission::AdmissionPermit;
use crate::backend::WriteBackend;
use crate::deadline::RequestDeadline;
use crate::file_region::FileRegion;
use crate::file_region::FileRegionSequence;
use crate::file_region::FileTransferMode;
use crate::file_region_writer::record_body_failure;
use crate::file_region_writer::record_fallback_unsupported;
use crate::file_region_writer::record_head_failure;
use crate::file_region_writer::record_portable_bytes;
use crate::file_region_writer::record_selection_failure;
#[cfg(all(target_os = "linux", feature = "linux-sendfile"))]
use crate::file_region_writer::record_sendfile_bytes;
use crate::file_region_writer::write_portable_file_region;

const QUEUED_WRITE_WAITING: u8 = 0;
const QUEUED_WRITE_CLAIMED: u8 = 1;
const QUEUED_WRITE_STARTED: u8 = 2;
const QUEUED_WRITE_CANCELLED_BY_DEADLINE: u8 = 3;
const QUEUED_WRITE_CANCELLED_BY_REQUEST: u8 = 4;
const QUEUED_WRITE_CANCELLED_BY_SESSION: u8 = 5;
const AUTO_SENDFILE_MIN_BYTES: u64 = 64 * 1024;

/// Test-only one-shot barrier placed immediately before the first write-progress
/// transition. It makes deadline/preflight races deterministic without adding a
/// production field or branch.
#[cfg(any(test, feature = "test-support"))]
#[derive(Clone)]
pub(crate) struct WritePreflightBarrier {
    checked: Arc<tokio::sync::Notify>,
    resume: Arc<tokio::sync::Notify>,
}

#[cfg(any(test, feature = "test-support"))]
impl WritePreflightBarrier {
    pub(crate) fn new(checked: Arc<tokio::sync::Notify>, resume: Arc<tokio::sync::Notify>) -> Self {
        Self { checked, resume }
    }

    async fn wait(&self) {
        self.checked.notify_one();
        self.resume.notified().await;
    }
}

pub(crate) struct QueuedWriteProgress(AtomicU8);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum QueuedWriteCancellation {
    Deadline,
    Request,
    SessionClosed,
}

impl QueuedWriteCancellation {
    const fn state(self) -> u8 {
        match self {
            Self::Deadline => QUEUED_WRITE_CANCELLED_BY_DEADLINE,
            Self::Request => QUEUED_WRITE_CANCELLED_BY_REQUEST,
            Self::SessionClosed => QUEUED_WRITE_CANCELLED_BY_SESSION,
        }
    }

    const fn from_state(state: u8) -> Option<Self> {
        match state {
            QUEUED_WRITE_CANCELLED_BY_DEADLINE => Some(Self::Deadline),
            QUEUED_WRITE_CANCELLED_BY_REQUEST => Some(Self::Request),
            QUEUED_WRITE_CANCELLED_BY_SESSION => Some(Self::SessionClosed),
            _ => None,
        }
    }
}

impl QueuedWriteProgress {
    pub(crate) fn waiting() -> Self {
        Self(AtomicU8::new(QUEUED_WRITE_WAITING))
    }

    /// Claims a queued envelope for the sole writer. A caller whose deadline
    /// wins `cancel_before_start` prevents this transition and therefore any
    /// later socket attempt for the envelope.
    pub(crate) fn claim_for_writer(&self) -> bool {
        self.0
            .compare_exchange(
                QUEUED_WRITE_WAITING,
                QUEUED_WRITE_CLAIMED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// Cancels a queued envelope before the writer owns it.
    pub(crate) fn cancel_before_start(&self) -> bool {
        self.cancel_before_start_with(QueuedWriteCancellation::Deadline)
    }

    pub(crate) fn cancel_before_start_with(&self, reason: QueuedWriteCancellation) -> bool {
        self.0
            .compare_exchange(
                QUEUED_WRITE_WAITING,
                reason.state(),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    pub(crate) fn cancellation_reason(&self) -> Option<QueuedWriteCancellation> {
        QueuedWriteCancellation::from_state(self.0.load(Ordering::Acquire))
    }

    /// Marks the precise boundary immediately before the first socket attempt.
    pub(crate) fn start_write(&self) {
        let _ = self.0.compare_exchange(
            QUEUED_WRITE_CLAIMED,
            QUEUED_WRITE_STARTED,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    pub(crate) fn write_started(&self) -> bool {
        self.0.load(Ordering::Acquire) == QUEUED_WRITE_STARTED
    }
}

pub(crate) struct QueuedWrite {
    pub(crate) operation: WriterOperation,
    pub(crate) completion: oneshot::Sender<crate::write_result::WriterResult>,
    pub(crate) permit: Option<AdmissionPermit>,
    pub(crate) deadline: Option<RequestDeadline>,
    #[allow(dead_code, reason = "queue-policy tests retain this opaque fixture identifier")]
    pub(crate) target: String,
    pub(crate) progress: Arc<QueuedWriteProgress>,
    pub(crate) enqueued_at: Option<Instant>,
    pub(crate) response_queue_wait_observation: ResponseQueueWaitObservation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResponseQueueWaitObservation {
    Ineligible,
    Response,
}

impl QueuedWrite {
    pub(crate) fn data(
        payload: OutboundPayload,
        completion: oneshot::Sender<crate::write_result::WriterResult>,
        permit: AdmissionPermit,
        deadline: Option<RequestDeadline>,
        target: String,
        progress: Arc<QueuedWriteProgress>,
        enqueued_at: Instant,
    ) -> Self {
        Self {
            operation: WriterOperation::Send(payload),
            completion,
            permit: Some(permit),
            deadline,
            target,
            progress,
            enqueued_at: Some(enqueued_at),
            response_queue_wait_observation: ResponseQueueWaitObservation::Ineligible,
        }
    }

    pub(crate) fn with_response_queue_wait_observation(mut self, observation: ResponseQueueWaitObservation) -> Self {
        self.response_queue_wait_observation = observation;
        self
    }

    pub(crate) fn encoded_len(&self) -> usize {
        match &self.operation {
            WriterOperation::Send(payload) => payload.encoded_len(),
        }
    }
}

pub(crate) enum WriterOperation {
    Send(OutboundPayload),
}

pub(crate) struct StructuredResponseFrame {
    head: EncodedFrameHead,
    body: PreparedStructuredResponseBody,
}

#[allow(
    dead_code,
    reason = "the later private response sink constructs this RSP-04 in-memory frame"
)]
enum StructuredResponseBody {
    Empty,
    Bytes(Bytes),
    Segments(Vec<Bytes>),
}

pub(crate) struct PreparedStructuredResponseBody {
    body: StructuredResponseBody,
    checked_len: usize,
}

impl PreparedStructuredResponseBody {
    pub(crate) fn empty() -> rocketmq_error::RocketMQResult<Self> {
        Ok(Self {
            body: StructuredResponseBody::Empty,
            checked_len: 0,
        })
    }

    pub(crate) fn bytes(body: Bytes) -> rocketmq_error::RocketMQResult<Self> {
        let checked_len = body.len();
        Ok(Self {
            body: StructuredResponseBody::Bytes(body),
            checked_len,
        })
    }

    pub(crate) fn segments(body: Vec<Bytes>) -> rocketmq_error::RocketMQResult<Self> {
        let checked_len = body.iter().try_fold(0_usize, |total, segment| {
            total.checked_add(segment.len()).ok_or_else(|| {
                SerializationError::encode_failed("structured-response-frame", "structured body length overflow")
            })
        })?;
        Ok(Self {
            body: StructuredResponseBody::Segments(body),
            checked_len,
        })
    }

    pub(crate) const fn body_len(&self) -> usize {
        self.checked_len
    }
}

impl StructuredResponseFrame {
    #[allow(
        dead_code,
        reason = "the later private response sink constructs structured frames through checked variants"
    )]
    pub(crate) fn new(
        head: EncodedFrameHead,
        body: PreparedStructuredResponseBody,
    ) -> rocketmq_error::RocketMQResult<Self> {
        let frame = Self { head, body };
        frame.validate_body_len()?;
        Ok(frame)
    }

    fn validate_body_len(&self) -> rocketmq_error::RocketMQResult<()> {
        let actual = self.body.checked_len;
        if actual != self.head.body_len() {
            return Err(SerializationError::encode_failed(
                "structured-response-frame",
                format!(
                    "structured body length {actual} does not match encoded frame head body length {}",
                    self.head.body_len()
                ),
            )
            .into());
        }
        Ok(())
    }

    fn encoded_len(&self) -> usize {
        self.head.encoded_len()
    }

    fn extend_segments<'a>(&'a self, segments: &mut Vec<&'a [u8]>) {
        segments.extend(self.head.segments());
        match &self.body.body {
            StructuredResponseBody::Empty => {}
            StructuredResponseBody::Bytes(bytes) => segments.push(bytes.as_ref()),
            StructuredResponseBody::Segments(body_segments) => {
                segments.extend(body_segments.iter().map(Bytes::as_ref));
            }
        }
    }

    fn copy_to(&self, destination: &mut BytesMut) {
        destination.reserve(self.encoded_len());
        for segment in self.head.segments() {
            destination.extend_from_slice(segment);
        }
        match &self.body.body {
            StructuredResponseBody::Empty => {}
            StructuredResponseBody::Bytes(bytes) => destination.extend_from_slice(bytes),
            StructuredResponseBody::Segments(segments) => {
                for segment in segments {
                    destination.extend_from_slice(segment);
                }
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn test_segments(&self) -> Vec<&[u8]> {
        let mut segments = Vec::new();
        self.extend_segments(&mut segments);
        segments
    }

    #[cfg(test)]
    pub(crate) fn test_body_segments(&self) -> Option<&[Bytes]> {
        match &self.body.body {
            StructuredResponseBody::Segments(segments) => Some(segments),
            _ => None,
        }
    }
}

pub(crate) enum OutboundPayload {
    Frame(EncodedFrame),
    #[allow(
        dead_code,
        reason = "the later private response sink enqueues the RSP-04 structured frame"
    )]
    StructuredFrame(StructuredResponseFrame),
    FileFrame {
        head: EncodedFrameHead,
        body: FileRegionSequence,
    },
    Batch {
        frames: Vec<EncodedFrame>,
        encoded_len: usize,
    },
    FrameSegments {
        segments: Vec<Bytes>,
        encoded_len: usize,
    },
    Contiguous(Bytes),
}

impl OutboundPayload {
    pub(crate) fn batch(frames: Vec<EncodedFrame>) -> rocketmq_error::RocketMQResult<Self> {
        let encoded_len = frames.iter().try_fold(0_usize, |total, frame| {
            total.checked_add(frame.encoded_len()).ok_or_else(|| {
                SerializationError::encode_failed("remoting-command-batch", "encoded batch length overflow")
            })
        })?;
        Ok(Self::Batch { frames, encoded_len })
    }

    pub(crate) fn encoded_len(&self) -> usize {
        match self {
            Self::Frame(frame) => frame.encoded_len(),
            Self::StructuredFrame(frame) => frame.encoded_len(),
            Self::FileFrame { head, .. } => head.encoded_len(),
            Self::Batch { encoded_len, .. } => *encoded_len,
            Self::FrameSegments { encoded_len, .. } => *encoded_len,
            Self::Contiguous(bytes) => bytes.len(),
        }
    }
}

/// Socket-write representation selected after TCP/TLS negotiation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(
    dead_code,
    reason = "explicit TLS comparison modes are exposed only by test_support and benchmark_support"
)]
pub enum FrameWriteMode {
    /// Preserve prefix, header, and body through to vectored plaintext writes.
    PlainVectored,
    /// Pass immutable frame segments to a TLS writer that supports vectored input.
    TlsVectored {
        /// Maximum encoded RocketMQ plaintext bytes accepted for one TLS record input.
        max_plaintext_frame_bytes: usize,
    },
    /// Coalesce one frame at a time for TLS, rejecting frames above the configured plaintext bound.
    TlsCoalesced {
        /// Maximum encoded RocketMQ plaintext bytes accepted for one TLS write.
        max_plaintext_frame_bytes: usize,
    },
    /// Coalesce small TLS plaintext and preserve vectored input above the measured crossover.
    TlsAuto {
        /// Maximum encoded RocketMQ plaintext bytes accepted for one TLS write.
        max_plaintext_frame_bytes: usize,
        /// Payloads at or below this size are coalesced before entering rustls.
        coalesce_below_bytes: usize,
    },
}

/// Single-owner frame writer with fail-closed poisoning after socket I/O failure.
///
/// `FrameWriter` never retains an `EncodedFrame`; the caller or owning writer actor keeps the
/// immutable frame alive until completion. TLS aggregation is private to this writer and therefore
/// cannot share mutable backing between queued frames.
pub struct FrameWriter<W> {
    io: W,
    mode: FrameWriteMode,
    tls_buffer: BytesMut,
    poisoned: bool,
    file_region_blocking: Option<BlockingExecutor>,
    file_transfer_mode: FileTransferMode,
    #[cfg(any(test, feature = "test-support"))]
    preflight_barrier: Option<WritePreflightBarrier>,
}

struct WriteCancellationGuard<'a> {
    poisoned: &'a mut bool,
    armed: bool,
}

impl<'a> WriteCancellationGuard<'a> {
    fn new(poisoned: &'a mut bool) -> Self {
        Self { poisoned, armed: true }
    }

    fn complete(&mut self) {
        self.armed = false;
    }
}

impl Drop for WriteCancellationGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            *self.poisoned = true;
        }
    }
}

#[allow(
    dead_code,
    reason = "direct writer controls are exposed only by test_support and benchmark_support"
)]
impl<W> FrameWriter<W>
where
    W: AsyncWrite + Unpin,
{
    /// Creates a writer for the negotiated transport mode.
    ///
    /// # Errors
    ///
    /// This constructor performs no I/O.
    pub fn new(io: W, mode: FrameWriteMode) -> io::Result<Self> {
        Ok(Self {
            io,
            mode,
            tls_buffer: BytesMut::new(),
            poisoned: false,
            file_region_blocking: None,
            file_transfer_mode: FileTransferMode::Auto,
            #[cfg(any(test, feature = "test-support"))]
            preflight_barrier: None,
        })
    }

    /// Creates a plaintext vectored writer.
    #[must_use]
    pub fn plaintext(io: W) -> Self {
        Self {
            io,
            mode: FrameWriteMode::PlainVectored,
            tls_buffer: BytesMut::new(),
            poisoned: false,
            file_region_blocking: None,
            file_transfer_mode: FileTransferMode::Auto,
            #[cfg(any(test, feature = "test-support"))]
            preflight_barrier: None,
        }
    }

    /// Returns the selected immutable write mode.
    #[inline]
    #[must_use]
    pub const fn mode(&self) -> FrameWriteMode {
        self.mode
    }

    /// Returns whether a prior socket write or flush failure poisoned this writer.
    #[inline]
    #[must_use]
    pub const fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// Returns the configured external-file transfer policy without performing I/O.
    #[inline]
    #[must_use]
    pub(crate) const fn file_transfer_mode(&self) -> FileTransferMode {
        self.file_transfer_mode
    }

    /// Returns ownership of the underlying I/O object.
    #[must_use]
    pub fn into_inner(self) -> W {
        self.io
    }

    /// Configures the injected blocking I/O lane and file-transfer selection for external bodies.
    #[must_use]
    pub fn with_file_region_io(mut self, blocking: BlockingExecutor, mode: FileTransferMode) -> Self {
        self.file_region_blocking = Some(blocking);
        self.file_transfer_mode = mode;
        self
    }

    pub(crate) fn configure_file_region_io(&mut self, blocking: BlockingExecutor, mode: FileTransferMode) {
        self.file_region_blocking = Some(blocking);
        self.file_transfer_mode = mode;
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn set_write_preflight_barrier(&mut self, barrier: WritePreflightBarrier) {
        self.preflight_barrier = Some(barrier);
    }

    #[cfg(any(test, feature = "test-support"))]
    async fn wait_write_preflight_barrier(&mut self) {
        if let Some(barrier) = self.preflight_barrier.take() {
            barrier.wait().await;
        }
    }

    /// Writes and flushes one immutable RocketMQ frame.
    ///
    /// # Errors
    ///
    /// Returns `BrokenPipe` without touching the socket after a prior failure. Plaintext writes
    /// propagate socket errors and `WriteZero`; TLS writes additionally reject frames larger than
    /// their configured coalescing bound.
    pub async fn write_frame(&mut self, frame: &EncodedFrame) -> io::Result<()> {
        self.ensure_healthy()?;
        if let Some(max_plaintext_frame_bytes) = tls_plaintext_bound(self.mode) {
            validate_tls_frame(frame, max_plaintext_frame_bytes)?;
        }
        let mode = self.mode;
        let mut cancellation = WriteCancellationGuard::new(&mut self.poisoned);
        match mode {
            FrameWriteMode::PlainVectored | FrameWriteMode::TlsVectored { .. } => {
                write_vectored_segments(&mut self.io, &frame.segments(), 64).await?;
            }
            FrameWriteMode::TlsAuto {
                coalesce_below_bytes, ..
            } if frame.encoded_len() > coalesce_below_bytes => {
                write_vectored_segments(&mut self.io, &frame.segments(), 64).await?;
            }
            FrameWriteMode::TlsCoalesced { .. } | FrameWriteMode::TlsAuto { .. } => {
                self.tls_buffer.clear();
                frame.copy_to(&mut self.tls_buffer);
                write_contiguous(&mut self.io, self.tls_buffer.as_ref()).await?;
                if self.tls_buffer.capacity() > 512 * 1024 {
                    self.tls_buffer = BytesMut::new();
                }
            }
        }
        self.io.flush().await?;
        cancellation.complete();
        Ok(())
    }

    /// Writes a bounded ordered payload batch and flushes exactly once.
    ///
    /// # Errors
    ///
    /// Returns the first validation, write, or flush error and poisons the writer
    /// when socket progress may have occurred.
    async fn write_payloads_with_start(
        &mut self,
        payloads: &[&OutboundPayload],
        max_iov: usize,
        on_start: &mut (dyn FnMut() + Send),
    ) -> io::Result<()> {
        self.ensure_healthy()?;
        if payloads.is_empty() {
            return Ok(());
        }
        let mode = self.mode;
        validate_structured_payloads(payloads)?;
        if let Some(max_plaintext_frame_bytes) = tls_plaintext_bound(mode) {
            validate_tls_payloads(payloads, max_plaintext_frame_bytes)?;
        }
        let payload_bytes = payloads
            .iter()
            .fold(0usize, |total, payload| total.saturating_add(payload.encoded_len()));

        // Resolve every deterministic strategy error before the write-progress
        // boundary. In particular, a file payload belongs to the file writer
        // and must not poison a healthy generic frame writer.
        let vectored_segments = match mode {
            FrameWriteMode::PlainVectored | FrameWriteMode::TlsVectored { .. } => Some(payload_segments(payloads)?),
            FrameWriteMode::TlsAuto {
                coalesce_below_bytes, ..
            } if payload_bytes > coalesce_below_bytes => Some(payload_segments(payloads)?),
            FrameWriteMode::TlsCoalesced { .. } | FrameWriteMode::TlsAuto { .. } => {
                if payloads
                    .iter()
                    .any(|payload| matches!(payload, OutboundPayload::FileFrame { .. }))
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "file-backed frames require the transport file writer",
                    ));
                }
                None
            }
        };

        // The callback is the canonical write-progress boundary. It runs after
        // validation/coalescing selection but immediately before the first
        // possible socket write or flush attempt.
        #[cfg(any(test, feature = "test-support"))]
        self.wait_write_preflight_barrier().await;
        on_start();
        let mut cancellation = WriteCancellationGuard::new(&mut self.poisoned);
        match mode {
            FrameWriteMode::PlainVectored | FrameWriteMode::TlsVectored { .. } => {
                let segments = vectored_segments.as_deref().expect("vectored preflight segments");
                write_vectored_segments(&mut self.io, segments, max_iov.max(1)).await?;
            }
            FrameWriteMode::TlsAuto {
                coalesce_below_bytes, ..
            } if payload_bytes > coalesce_below_bytes => {
                let segments = vectored_segments.as_deref().expect("vectored preflight segments");
                write_vectored_segments(&mut self.io, segments, max_iov.max(1)).await?;
            }
            FrameWriteMode::TlsCoalesced { .. } | FrameWriteMode::TlsAuto { .. } => {
                for payload in payloads {
                    write_coalesced_payload(&mut self.io, &mut self.tls_buffer, payload).await?;
                }
                if self.tls_buffer.capacity() > 512 * 1024 {
                    self.tls_buffer = BytesMut::new();
                }
            }
        }
        self.io.flush().await?;
        cancellation.complete();
        Ok(())
    }

    /// Writes and flushes caller-provided contiguous wire bytes.
    ///
    /// This compatibility path is intended for bytes that are already framed. New
    /// `RemotingCommand` writes should use [`Self::write_frame`].
    ///
    /// # Errors
    ///
    /// Returns the underlying socket or flush error and poisons the writer.
    pub async fn write_bytes(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.ensure_healthy()?;
        if let Some(max_plaintext_frame_bytes) = tls_plaintext_bound(self.mode) {
            if bytes.len() > max_plaintext_frame_bytes {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "contiguous payload exceeds TLS plaintext bound",
                ));
            }
        }
        let mut cancellation = WriteCancellationGuard::new(&mut self.poisoned);
        write_contiguous(&mut self.io, bytes).await?;
        self.io.flush().await?;
        cancellation.complete();
        Ok(())
    }

    /// Flushes buffered transport data.
    ///
    /// # Errors
    ///
    /// Returns and records the underlying flush failure.
    pub async fn flush(&mut self) -> io::Result<()> {
        self.ensure_healthy()?;
        let mut cancellation = WriteCancellationGuard::new(&mut self.poisoned);
        self.io.flush().await?;
        cancellation.complete();
        Ok(())
    }

    /// Actively shuts down the underlying write half.
    ///
    /// Shutdown remains available after poisoning so the owner can close a partially written
    /// byte stream without attempting another frame.
    pub async fn shutdown(&mut self) -> io::Result<()> {
        self.io.shutdown().await
    }

    fn ensure_healthy(&self) -> io::Result<()> {
        if self.poisoned {
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "frame writer is poisoned by a previous transport failure",
            ))
        } else {
            Ok(())
        }
    }
}

enum SelectedFileTransfer {
    Portable {
        blocking: BlockingExecutor,
        fallback: bool,
    },
    #[cfg(all(target_os = "linux", feature = "linux-sendfile"))]
    Sendfile,
}

impl FrameWriter<WriteBackend> {
    /// Writes transport payloads and calls `on_start` immediately before the
    /// first potentially observable socket operation.
    pub(crate) async fn write_transport_payloads_with_start(
        &mut self,
        payloads: &[&OutboundPayload],
        max_iov: usize,
        on_start: &mut (dyn FnMut() + Send),
    ) -> io::Result<()> {
        if !payloads
            .iter()
            .any(|payload| matches!(payload, OutboundPayload::FileFrame { .. }))
        {
            return self.write_payloads_with_start(payloads, max_iov, on_start).await;
        }

        let mut buffered = Vec::new();
        for payload in payloads {
            match payload {
                OutboundPayload::FileFrame { head, body } => {
                    if !buffered.is_empty() {
                        self.write_payloads_with_start(&buffered, max_iov, on_start).await?;
                        buffered.clear();
                    }
                    self.write_file_frame_with_start(head, body, max_iov, on_start).await?;
                }
                _ => buffered.push(*payload),
            }
        }
        if !buffered.is_empty() {
            self.write_payloads_with_start(&buffered, max_iov, on_start).await?;
        }
        Ok(())
    }

    async fn write_file_frame_with_start(
        &mut self,
        head: &EncodedFrameHead,
        body: &FileRegionSequence,
        max_iov: usize,
        on_start: &mut (dyn FnMut() + Send),
    ) -> io::Result<()> {
        self.ensure_healthy()?;
        let body_len = usize::try_from(body.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "file-region length exceeds usize"))?;
        if head.body_len() != body_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "encoded frame head body length does not match the leased file-region sequence",
            ));
        }
        if let Some(max_plaintext_frame_bytes) = tls_plaintext_bound(self.mode) {
            if head.encoded_len() > max_plaintext_frame_bytes {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "file-backed frame exceeds TLS plaintext bound",
                ));
            }
        }
        let mut selections = Vec::with_capacity(body.regions().len());
        for region in body.regions() {
            let native_eligible = matches!(self.mode, FrameWriteMode::PlainVectored)
                && matches!(&self.io, WriteBackend::Tcp(_))
                && native_file_region_eligible(region);
            let auto_native_eligible = native_eligible && region.len() >= AUTO_SENDFILE_MIN_BYTES;
            match select_file_transfer(
                self.file_transfer_mode,
                self.file_region_blocking.clone(),
                native_eligible,
                auto_native_eligible,
                region,
            )
            .await
            {
                Ok(selected) => selections.push(selected),
                Err(error) => {
                    record_selection_failure();
                    return Err(error);
                }
            }
        }
        #[cfg(any(test, feature = "test-support"))]
        self.wait_write_preflight_barrier().await;
        on_start();
        let mut cancellation = WriteCancellationGuard::new(&mut self.poisoned);
        if let Err(error) = write_vectored_segments(&mut self.io, &head.segments(), max_iov.max(1)).await {
            record_head_failure();
            return Err(error);
        }
        for (region, selected) in body.regions().iter().zip(selections) {
            let body_result = match selected {
                SelectedFileTransfer::Portable { blocking, fallback } => {
                    if fallback {
                        record_fallback_unsupported();
                    }
                    let written = write_portable_file_region(&mut self.io, region, &blocking).await;
                    if let Ok(written) = written {
                        record_portable_bytes(written);
                    }
                    written
                }
                #[cfg(all(target_os = "linux", feature = "linux-sendfile"))]
                SelectedFileTransfer::Sendfile => match &mut self.io {
                    WriteBackend::Tcp(writer) => {
                        let written = crate::linux::sendfile::send_file_region(writer, region).await;
                        if let Ok(written) = written {
                            record_sendfile_bytes(written);
                        }
                        written
                    }
                    WriteBackend::Compat(_) => unreachable!("native selection requires a plaintext TCP backend"),
                },
            };
            if let Err(error) = body_result {
                record_body_failure();
                return Err(error);
            }
        }
        if let Err(error) = self.io.flush().await {
            record_body_failure();
            return Err(error);
        }
        cancellation.complete();
        Ok(())
    }
}

async fn select_file_transfer(
    mode: FileTransferMode,
    blocking: Option<BlockingExecutor>,
    native_eligible: bool,
    auto_native_eligible: bool,
    body: &FileRegion,
) -> io::Result<SelectedFileTransfer> {
    #[cfg(not(all(target_os = "linux", feature = "linux-sendfile")))]
    let _ = body;
    match mode {
        FileTransferMode::Portable => portable_selection(blocking, false),
        FileTransferMode::Auto if auto_native_eligible => {
            #[cfg(all(target_os = "linux", feature = "linux-sendfile"))]
            {
                if probe_native_file_region(blocking.clone(), body).await? {
                    Ok(SelectedFileTransfer::Sendfile)
                } else {
                    portable_selection(blocking, true)
                }
            }
            #[cfg(not(all(target_os = "linux", feature = "linux-sendfile")))]
            {
                portable_selection(blocking, true)
            }
        }
        FileTransferMode::Auto if native_eligible => portable_selection(blocking, false),
        FileTransferMode::Auto => portable_selection(blocking, true),
        FileTransferMode::Sendfile if native_eligible => {
            #[cfg(all(target_os = "linux", feature = "linux-sendfile"))]
            {
                if probe_native_file_region(blocking, body).await? {
                    Ok(SelectedFileTransfer::Sendfile)
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        "the leased file does not support sendfile",
                    ))
                }
            }
            #[cfg(not(all(target_os = "linux", feature = "linux-sendfile")))]
            {
                Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "sendfile mode requires Linux and the linux-sendfile feature",
                ))
            }
        }
        FileTransferMode::Sendfile => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "sendfile mode requires an eligible regular file and plaintext TCP backend",
        )),
    }
}

fn portable_selection(blocking: Option<BlockingExecutor>, fallback: bool) -> io::Result<SelectedFileTransfer> {
    blocking
        .map(|blocking| SelectedFileTransfer::Portable { blocking, fallback })
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                "file-region writes require an injected storage BlockingExecutor",
            )
        })
}

#[cfg(all(target_os = "linux", feature = "linux-sendfile"))]
async fn probe_native_file_region(blocking: Option<BlockingExecutor>, body: &FileRegion) -> io::Result<bool> {
    let blocking = blocking.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::Unsupported,
            "sendfile preflight requires an injected storage BlockingExecutor",
        )
    })?;
    let region = body.clone();
    blocking
        .spawn_io("transport.file-region.sendfile-probe", move || {
            crate::linux::sendfile::probe_file_region(&region)
        })
        .await
        .map_err(preserve_preflight_task_error)?
}

#[cfg(any(all(target_os = "linux", feature = "linux-sendfile"), test))]
fn preserve_preflight_task_error(error: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::other(error)
}

fn native_file_region_eligible(body: &FileRegion) -> bool {
    #[cfg(all(target_os = "linux", feature = "linux-sendfile"))]
    {
        crate::linux::sendfile::is_eligible(body)
    }
    #[cfg(not(all(target_os = "linux", feature = "linux-sendfile")))]
    {
        let _ = body;
        false
    }
}

fn tls_plaintext_bound(mode: FrameWriteMode) -> Option<usize> {
    match mode {
        FrameWriteMode::PlainVectored => None,
        FrameWriteMode::TlsVectored {
            max_plaintext_frame_bytes,
        }
        | FrameWriteMode::TlsCoalesced {
            max_plaintext_frame_bytes,
        }
        | FrameWriteMode::TlsAuto {
            max_plaintext_frame_bytes,
            ..
        } => Some(max_plaintext_frame_bytes),
    }
}

fn validate_tls_payloads(payloads: &[&OutboundPayload], max_plaintext_frame_bytes: usize) -> io::Result<()> {
    for payload in payloads {
        match payload {
            OutboundPayload::Frame(frame) => validate_tls_frame(frame, max_plaintext_frame_bytes)?,
            OutboundPayload::StructuredFrame(frame) if frame.encoded_len() > max_plaintext_frame_bytes => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "structured response frame exceeds TLS plaintext bound",
                ));
            }
            OutboundPayload::StructuredFrame(_) => {}
            OutboundPayload::FileFrame { head, .. } if head.encoded_len() > max_plaintext_frame_bytes => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "file-backed frame exceeds TLS plaintext bound",
                ));
            }
            OutboundPayload::FileFrame { .. } => {}
            OutboundPayload::Batch { frames, .. } => {
                for frame in frames {
                    validate_tls_frame(frame, max_plaintext_frame_bytes)?;
                }
            }
            OutboundPayload::FrameSegments { encoded_len, .. } if *encoded_len > max_plaintext_frame_bytes => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "segmented frame exceeds TLS plaintext bound",
                ));
            }
            OutboundPayload::FrameSegments { .. } => {}
            OutboundPayload::Contiguous(bytes) if bytes.len() > max_plaintext_frame_bytes => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "contiguous payload exceeds TLS plaintext bound",
                ));
            }
            OutboundPayload::Contiguous(_) => {}
        }
    }
    Ok(())
}

fn validate_tls_frame(frame: &EncodedFrame, max_plaintext_frame_bytes: usize) -> io::Result<()> {
    if frame.encoded_len() > max_plaintext_frame_bytes {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "encoded frame is {} bytes, exceeding TLS plaintext coalescing limit {max_plaintext_frame_bytes}",
                frame.encoded_len()
            ),
        ))
    } else {
        Ok(())
    }
}

fn validate_structured_payloads(payloads: &[&OutboundPayload]) -> io::Result<()> {
    for payload in payloads {
        if let OutboundPayload::StructuredFrame(frame) = payload {
            frame
                .validate_body_len()
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        }
    }
    Ok(())
}

fn payload_segments<'a>(payloads: &'a [&'a OutboundPayload]) -> io::Result<Vec<&'a [u8]>> {
    let mut segments = Vec::new();
    for payload in payloads {
        match payload {
            OutboundPayload::Frame(frame) => segments.extend(frame.segments()),
            OutboundPayload::StructuredFrame(frame) => frame.extend_segments(&mut segments),
            OutboundPayload::FileFrame { .. } => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "file-backed frames require the transport file writer",
                ));
            }
            OutboundPayload::Batch { frames, .. } => {
                for frame in frames {
                    segments.extend(frame.segments());
                }
            }
            OutboundPayload::FrameSegments {
                segments: frame_segments,
                ..
            } => segments.extend(frame_segments.iter().map(Bytes::as_ref)),
            OutboundPayload::Contiguous(bytes) => segments.push(bytes.as_ref()),
        }
    }
    Ok(segments)
}

async fn write_coalesced_payload<W>(io: &mut W, buffer: &mut BytesMut, payload: &OutboundPayload) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    match payload {
        OutboundPayload::Frame(frame) => {
            buffer.clear();
            frame.copy_to(buffer);
            write_contiguous(io, buffer.as_ref()).await
        }
        OutboundPayload::StructuredFrame(frame) => {
            buffer.clear();
            frame.copy_to(buffer);
            write_contiguous(io, buffer.as_ref()).await
        }
        OutboundPayload::FileFrame { .. } => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "file-backed frames require the transport file writer",
        )),
        OutboundPayload::Batch { frames, .. } => {
            for frame in frames {
                buffer.clear();
                frame.copy_to(buffer);
                write_contiguous(io, buffer.as_ref()).await?;
            }
            Ok(())
        }
        OutboundPayload::FrameSegments { segments, .. } => {
            buffer.clear();
            for segment in segments {
                buffer.extend_from_slice(segment);
            }
            write_contiguous(io, buffer.as_ref()).await
        }
        OutboundPayload::Contiguous(bytes) => write_contiguous(io, bytes).await,
    }
}

async fn write_vectored_segments<W>(io: &mut W, segments: &[&[u8]], max_iov: usize) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut segment_index = 0;
    let mut segment_offset = 0;
    skip_empty_segments(segments, &mut segment_index, &mut segment_offset);
    while segment_index < segments.len() {
        let window_end = segment_index.saturating_add(max_iov).min(segments.len());
        let mut slices = Vec::with_capacity(window_end - segment_index);
        slices.push(IoSlice::new(&segments[segment_index][segment_offset..]));
        slices.extend(
            segments[segment_index + 1..window_end]
                .iter()
                .map(|segment| IoSlice::new(segment)),
        );
        let written = io.write_vectored(&slices).await?;
        if written == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "vectored frame write made no progress",
            ));
        }
        advance_segments(segments, &mut segment_index, &mut segment_offset, written)?;
    }
    Ok(())
}

async fn write_contiguous<W>(io: &mut W, mut bytes: &[u8]) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    while !bytes.is_empty() {
        let written = io.write(bytes).await?;
        if written == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "contiguous frame write made no progress",
            ));
        }
        if written > bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "AsyncWrite reported more bytes than supplied",
            ));
        }
        bytes = &bytes[written..];
    }
    Ok(())
}

fn skip_empty_segments(segments: &[&[u8]], segment_index: &mut usize, segment_offset: &mut usize) {
    while *segment_index < segments.len() && *segment_offset == segments[*segment_index].len() {
        *segment_index += 1;
        *segment_offset = 0;
    }
}

fn advance_segments(
    segments: &[&[u8]],
    segment_index: &mut usize,
    segment_offset: &mut usize,
    mut written: usize,
) -> io::Result<()> {
    while written > 0 && *segment_index < segments.len() {
        let remaining = segments[*segment_index].len() - *segment_offset;
        if written < remaining {
            *segment_offset += written;
            written = 0;
        } else {
            written -= remaining;
            *segment_index += 1;
            *segment_offset = 0;
            skip_empty_segments(segments, segment_index, segment_offset);
        }
    }
    if written == 0 {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "AsyncWrite reported more vectored bytes than supplied",
        ))
    }
}

#[cfg(test)]
#[path = "../tests/unit/write_strategy/brk02.rs"]
mod brk02_tests;

#[cfg(test)]
mod file_frame_tests {
    use std::error::Error;
    use std::fmt;
    use std::io::Write;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use rocketmq_protocol::protocol::encoded_frame::EncodedFrameHead;
    use rocketmq_protocol::protocol::RemotingCommand;

    use super::OutboundPayload;
    use crate::file_region::FileRegion;
    use crate::file_region::FileRegionLease;
    use crate::file_region::FileRegionSequence;

    #[derive(Debug)]
    struct PanicDisplay;

    impl fmt::Display for PanicDisplay {
        fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
            panic!("preflight source must not be formatted")
        }
    }

    impl Error for PanicDisplay {}

    struct DropLease {
        file: std::fs::File,
        drops: Arc<AtomicUsize>,
    }

    impl FileRegionLease for DropLease {
        fn file(&self) -> &std::fs::File {
            &self.file
        }
    }

    impl Drop for DropLease {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn queued_file_payload_counts_the_complete_wire_frame_and_retains_lease() {
        let mut file = tempfile::tempfile().expect("temporary file");
        file.write_all(&vec![0x33; 8192]).expect("write body");
        let drops = Arc::new(AtomicUsize::new(0));
        let lease = Arc::new(DropLease {
            file,
            drops: drops.clone(),
        });
        let region = FileRegion::try_new(lease.clone(), 0, 8192).expect("file region");
        let head = EncodedFrameHead::from_command_and_body_len(
            RemotingCommand::create_remoting_command(601),
            region.len() as usize,
        )
        .expect("frame head");
        let expected_wire_bytes = head.encoded_len();
        let payload = OutboundPayload::FileFrame {
            head,
            body: FileRegionSequence::single(region),
        };

        drop(lease);
        assert_eq!(drops.load(Ordering::SeqCst), 0, "queued payload must retain the lease");
        assert_eq!(payload.encoded_len(), expected_wire_bytes);

        drop(payload);
        assert_eq!(
            drops.load(Ordering::SeqCst),
            1,
            "lease should release with the queue item"
        );
    }

    #[test]
    fn preflight_task_error_preserves_the_typed_source_without_display() {
        let error = super::preserve_preflight_task_error(PanicDisplay);

        assert!(error.get_ref().is_some_and(|source| source.is::<PanicDisplay>()));
    }
}

#[cfg(test)]
mod structured_response_tests {
    use std::io;
    use std::io::IoSlice;
    use std::pin::Pin;
    use std::task::Context;
    use std::task::Poll;

    use bytes::Bytes;
    use rocketmq_protocol::protocol::encoded_frame::EncodedFrameHead;
    use rocketmq_protocol::protocol::RemotingCommand;
    use tokio::io::AsyncWrite;

    use super::FrameWriteMode;
    use super::FrameWriter;
    use super::OutboundPayload;
    use super::PreparedStructuredResponseBody;
    use super::StructuredResponseBody;
    use super::StructuredResponseFrame;

    #[derive(Default)]
    struct RecordingIo {
        output: Vec<u8>,
        vectored_widths: Vec<usize>,
        flushes: usize,
    }

    impl AsyncWrite for RecordingIo {
        fn poll_write(mut self: Pin<&mut Self>, _context: &mut Context<'_>, buffer: &[u8]) -> Poll<io::Result<usize>> {
            self.output.extend_from_slice(buffer);
            Poll::Ready(Ok(buffer.len()))
        }

        fn poll_write_vectored(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            buffers: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            self.vectored_widths.push(buffers.len());
            let mut written = 0;
            for buffer in buffers {
                self.output.extend_from_slice(buffer);
                written += buffer.len();
            }
            Poll::Ready(Ok(written))
        }

        fn is_write_vectored(&self) -> bool {
            true
        }

        fn poll_flush(mut self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.flushes += 1;
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    fn structured_segments() -> (OutboundPayload, Vec<u8>, usize) {
        let body = vec![
            Bytes::from_static(b"first"),
            Bytes::from_static(b"-second"),
            Bytes::from_static(b"-third"),
        ];
        let body_len = body.iter().map(Bytes::len).sum::<usize>();
        let owner = PreparedStructuredResponseBody::segments(body).expect("checked structured body");
        let head = EncodedFrameHead::from_command_and_body_len(
            RemotingCommand::create_response_command_with_code(901).set_opaque(37),
            body_len,
        )
        .expect("encoded response head");
        let encoded_len = head.encoded_len();
        let frame = StructuredResponseFrame::new(head, owner).expect("matching structured frame");
        let expected = frame
            .test_segments()
            .into_iter()
            .flat_map(|segment| segment.iter().copied())
            .collect();
        (OutboundPayload::StructuredFrame(frame), expected, encoded_len)
    }

    async fn write_payload(
        payload: &OutboundPayload,
        mode: FrameWriteMode,
        max_iov: usize,
    ) -> (io::Result<()>, RecordingIo, bool, bool) {
        let mut writer = FrameWriter::new(RecordingIo::default(), mode).expect("recording writer");
        let mut started = false;
        let result = writer
            .write_payloads_with_start(&[payload], max_iov, &mut || started = true)
            .await;
        let poisoned = writer.is_poisoned();
        (result, writer.into_inner(), started, poisoned)
    }

    #[tokio::test]
    async fn plaintext_preserves_segment_order_with_single_and_normal_iov_windows() {
        for (max_iov, expected_widths) in [(1, vec![1, 1, 1, 1, 1]), (3, vec![3, 2])] {
            let (payload, expected, encoded_len) = structured_segments();
            let (result, io, started, poisoned) = write_payload(&payload, FrameWriteMode::PlainVectored, max_iov).await;

            result.expect("plaintext structured write");
            assert!(started);
            assert!(!poisoned);
            assert_eq!(io.output, expected);
            assert_eq!(io.output.len(), encoded_len);
            assert_eq!(io.vectored_widths, expected_widths);
            assert_eq!(io.flushes, 1);
        }
    }

    #[tokio::test]
    async fn tls_vectored_coalesced_and_auto_modes_receive_identical_complete_plaintext() {
        let (baseline, expected, encoded_len) = structured_segments();
        let modes = [
            FrameWriteMode::TlsVectored {
                max_plaintext_frame_bytes: encoded_len,
            },
            FrameWriteMode::TlsCoalesced {
                max_plaintext_frame_bytes: encoded_len,
            },
            FrameWriteMode::TlsAuto {
                max_plaintext_frame_bytes: encoded_len,
                coalesce_below_bytes: encoded_len,
            },
            FrameWriteMode::TlsAuto {
                max_plaintext_frame_bytes: encoded_len,
                coalesce_below_bytes: 0,
            },
        ];

        for mode in modes {
            let (result, io, started, poisoned) = write_payload(&baseline, mode, 2).await;
            result.expect("bounded TLS structured write");
            assert!(started);
            assert!(!poisoned);
            assert_eq!(io.output, expected, "{mode:?}");
            assert_eq!(io.output.len(), encoded_len);
            assert_eq!(io.flushes, 1);
        }
    }

    #[tokio::test]
    async fn every_tls_mode_rejects_an_oversized_complete_frame_before_progress() {
        let (payload, _, encoded_len) = structured_segments();
        let modes = [
            FrameWriteMode::TlsVectored {
                max_plaintext_frame_bytes: encoded_len - 1,
            },
            FrameWriteMode::TlsCoalesced {
                max_plaintext_frame_bytes: encoded_len - 1,
            },
            FrameWriteMode::TlsAuto {
                max_plaintext_frame_bytes: encoded_len - 1,
                coalesce_below_bytes: encoded_len,
            },
        ];

        for mode in modes {
            let (result, io, started, poisoned) = write_payload(&payload, mode, 8).await;
            assert_eq!(
                result.expect_err("oversized TLS plaintext").kind(),
                io::ErrorKind::InvalidInput
            );
            assert!(!started, "{mode:?}");
            assert!(!poisoned, "{mode:?}");
            assert!(io.output.is_empty(), "{mode:?}");
            assert_eq!(io.flushes, 0, "{mode:?}");
        }
    }

    #[tokio::test]
    async fn a_mismatched_structured_head_is_rejected_before_progress() {
        let head =
            EncodedFrameHead::from_command_and_body_len(RemotingCommand::create_response_command_with_code(902), 7)
                .expect("encoded response head");
        let malformed = StructuredResponseFrame {
            head,
            body: PreparedStructuredResponseBody {
                body: StructuredResponseBody::Bytes(Bytes::from_static(b"six!!!")),
                checked_len: 6,
            },
        };
        let payload = OutboundPayload::StructuredFrame(malformed);

        let (result, io, started, poisoned) = write_payload(&payload, FrameWriteMode::PlainVectored, 8).await;
        assert_eq!(
            result.expect_err("mismatched body length").kind(),
            io::ErrorKind::InvalidInput
        );
        assert!(!started);
        assert!(!poisoned);
        assert!(io.output.is_empty());
        assert_eq!(io.flushes, 0);
    }

    #[tokio::test]
    async fn legacy_complete_frame_segments_keep_their_existing_write_shape() {
        let payload = OutboundPayload::FrameSegments {
            segments: vec![
                Bytes::from_static(b"legacy-prefix"),
                Bytes::from_static(b"legacy-header"),
                Bytes::from_static(b"legacy-body"),
            ],
            encoded_len: 37,
        };
        let (result, io, started, poisoned) = write_payload(&payload, FrameWriteMode::PlainVectored, 2).await;

        result.expect("legacy segmented payload");
        assert!(started);
        assert!(!poisoned);
        assert_eq!(io.output, b"legacy-prefixlegacy-headerlegacy-body");
        assert_eq!(io.vectored_widths, [2, 1]);
    }

    #[test]
    fn structured_encoded_len_counts_the_body_exactly_once() {
        let (payload, expected, encoded_len) = structured_segments();
        let announced_payload = i32::from_be_bytes(expected[..4].try_into().expect("frame prefix"));

        assert_eq!(payload.encoded_len(), encoded_len);
        assert_eq!(payload.encoded_len(), expected.len());
        assert_eq!(announced_payload as usize + 4, expected.len());
        assert!(matches!(payload, OutboundPayload::StructuredFrame(_)));
        assert!(!matches!(payload, OutboundPayload::FrameSegments { .. }));
    }
}
