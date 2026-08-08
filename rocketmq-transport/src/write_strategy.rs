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
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::sync::oneshot;

use crate::admission::AdmissionPermit;
use crate::deadline::RequestDeadline;

const QUEUED_WRITE_WAITING: u8 = 0;
const QUEUED_WRITE_STARTED: u8 = 1;

pub(crate) struct QueuedWriteProgress(AtomicU8);

impl QueuedWriteProgress {
    pub(crate) fn waiting() -> Self {
        Self(AtomicU8::new(QUEUED_WRITE_WAITING))
    }

    pub(crate) fn start_write(&self) {
        self.0.store(QUEUED_WRITE_STARTED, Ordering::Release);
    }

    pub(crate) fn write_started(&self) -> bool {
        self.0.load(Ordering::Acquire) == QUEUED_WRITE_STARTED
    }
}

pub(crate) struct QueuedWrite {
    pub(crate) operation: WriterOperation,
    pub(crate) completion: oneshot::Sender<rocketmq_error::RocketMQResult<()>>,
    pub(crate) permit: Option<AdmissionPermit>,
    pub(crate) deadline: Option<RequestDeadline>,
    pub(crate) target: String,
    pub(crate) progress: Option<Arc<QueuedWriteProgress>>,
    pub(crate) enqueued_at: Option<Instant>,
}

impl QueuedWrite {
    pub(crate) fn data(
        payload: OutboundPayload,
        completion: oneshot::Sender<rocketmq_error::RocketMQResult<()>>,
        permit: AdmissionPermit,
        deadline: Option<RequestDeadline>,
        target: String,
        progress: Option<Arc<QueuedWriteProgress>>,
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
        }
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

pub(crate) enum OutboundPayload {
    Frame(EncodedFrame),
    Batch {
        frames: Vec<EncodedFrame>,
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
            Self::Batch { encoded_len, .. } => *encoded_len,
            Self::Contiguous(bytes) => bytes.len(),
        }
    }

    pub(crate) async fn write_to<W>(&self, writer: &mut FrameWriter<W>) -> io::Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        writer.write_payloads(&[self], 64).await
    }
}

/// Socket-write representation selected after TCP/TLS negotiation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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

impl<W> FrameWriter<W>
where
    W: AsyncWrite + Unpin,
{
    /// Creates a writer for the negotiated transport mode.
    ///
    /// # Errors
    ///
    /// Returns `InvalidInput` when the TLS coalescing bound is zero.
    pub fn new(io: W, mode: FrameWriteMode) -> io::Result<Self> {
        if matches!(
            mode,
            FrameWriteMode::TlsCoalesced {
                max_plaintext_frame_bytes: 0
            } | FrameWriteMode::TlsVectored {
                max_plaintext_frame_bytes: 0
            } | FrameWriteMode::TlsAuto {
                max_plaintext_frame_bytes: 0,
                ..
            }
        ) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "TLS writing requires a non-zero frame-byte bound",
            ));
        }
        Ok(Self {
            io,
            mode,
            tls_buffer: BytesMut::new(),
            poisoned: false,
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

    /// Returns ownership of the underlying I/O object.
    #[must_use]
    pub fn into_inner(self) -> W {
        self.io
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
    pub(crate) async fn write_payloads(&mut self, payloads: &[&OutboundPayload], max_iov: usize) -> io::Result<()> {
        self.ensure_healthy()?;
        if payloads.is_empty() {
            return Ok(());
        }
        let mode = self.mode;
        if let Some(max_plaintext_frame_bytes) = tls_plaintext_bound(mode) {
            validate_tls_payloads(payloads, max_plaintext_frame_bytes)?;
        }
        let payload_bytes = payloads
            .iter()
            .fold(0usize, |total, payload| total.saturating_add(payload.encoded_len()));
        let mut cancellation = WriteCancellationGuard::new(&mut self.poisoned);
        match mode {
            FrameWriteMode::PlainVectored | FrameWriteMode::TlsVectored { .. } => {
                let segments = payload_segments(payloads);
                write_vectored_segments(&mut self.io, &segments, max_iov.max(1)).await?;
            }
            FrameWriteMode::TlsAuto {
                coalesce_below_bytes, ..
            } if payload_bytes > coalesce_below_bytes => {
                let segments = payload_segments(payloads);
                write_vectored_segments(&mut self.io, &segments, max_iov.max(1)).await?;
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
            OutboundPayload::Batch { frames, .. } => {
                for frame in frames {
                    validate_tls_frame(frame, max_plaintext_frame_bytes)?;
                }
            }
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

fn payload_segments<'a>(payloads: &'a [&'a OutboundPayload]) -> Vec<&'a [u8]> {
    let mut segments = Vec::new();
    for payload in payloads {
        match payload {
            OutboundPayload::Frame(frame) => segments.extend(frame.segments()),
            OutboundPayload::Batch { frames, .. } => {
                for frame in frames {
                    segments.extend(frame.segments());
                }
            }
            OutboundPayload::Contiguous(bytes) => segments.push(bytes.as_ref()),
        }
    }
    segments
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
        OutboundPayload::Batch { frames, .. } => {
            for frame in frames {
                buffer.clear();
                frame.copy_to(buffer);
                write_contiguous(io, buffer.as_ref()).await?;
            }
            Ok(())
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
