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
    pub(crate) queue_id: Option<u64>,
}

impl QueuedWrite {
    pub(crate) fn data(
        payload: OutboundPayload,
        completion: oneshot::Sender<rocketmq_error::RocketMQResult<()>>,
        permit: AdmissionPermit,
        deadline: Option<RequestDeadline>,
        target: String,
        progress: Option<Arc<QueuedWriteProgress>>,
        queue_id: u64,
    ) -> Self {
        Self {
            operation: WriterOperation::Send(payload),
            completion,
            permit: Some(permit),
            deadline,
            target,
            progress,
            queue_id: Some(queue_id),
        }
    }

    pub(crate) fn close(completion: oneshot::Sender<rocketmq_error::RocketMQResult<()>>) -> Self {
        Self {
            operation: WriterOperation::Close,
            completion,
            permit: None,
            deadline: None,
            target: String::new(),
            progress: None,
            queue_id: None,
        }
    }
}

pub(crate) enum WriterOperation {
    Send(OutboundPayload),
    Close,
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
        match self {
            Self::Frame(frame) => writer.write_frame(frame).await,
            Self::Batch { frames, .. } => {
                for frame in frames {
                    writer.write_frame(frame).await?;
                }
                Ok(())
            }
            Self::Contiguous(bytes) => writer.write_bytes(bytes).await,
        }
    }
}

/// Socket-write representation selected after TCP/TLS negotiation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrameWriteMode {
    /// Preserve prefix, header, and body through to vectored plaintext writes.
    PlainVectored,
    /// Coalesce one frame at a time for TLS, rejecting frames above the configured plaintext bound.
    TlsCoalesced {
        /// Maximum encoded RocketMQ plaintext bytes accepted for one TLS write.
        max_plaintext_frame_bytes: usize,
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
            }
        ) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "TLS coalescing requires a non-zero frame-byte bound",
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
        let mode = self.mode;
        if let FrameWriteMode::TlsCoalesced {
            max_plaintext_frame_bytes,
        } = mode
        {
            if frame.encoded_len() > max_plaintext_frame_bytes {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "encoded frame is {} bytes, exceeding TLS plaintext coalescing limit \
                         {max_plaintext_frame_bytes}",
                        frame.encoded_len()
                    ),
                ));
            }
        }
        let mut cancellation = WriteCancellationGuard::new(&mut self.poisoned);
        let result = match mode {
            FrameWriteMode::PlainVectored => write_vectored_frame(&mut self.io, frame).await,
            FrameWriteMode::TlsCoalesced { .. } => {
                self.tls_buffer.clear();
                frame.copy_to(&mut self.tls_buffer);
                let result = write_contiguous(&mut self.io, self.tls_buffer.as_ref()).await;
                self.tls_buffer.clear();
                result
            }
        };
        result?;
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

async fn write_vectored_frame<W>(io: &mut W, frame: &EncodedFrame) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let segments = frame.segments();
    let mut segment_index = 0;
    let mut segment_offset = 0;
    skip_empty_segments(&segments, &mut segment_index, &mut segment_offset);
    while segment_index < segments.len() {
        let current = &segments[segment_index][segment_offset..];
        let second = segments.get(segment_index + 1).copied().unwrap_or_default();
        let third = segments.get(segment_index + 2).copied().unwrap_or_default();
        let slices = [IoSlice::new(current), IoSlice::new(second), IoSlice::new(third)];
        let slice_count = segments.len() - segment_index;
        let written = io.write_vectored(&slices[..slice_count]).await?;
        if written == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "vectored frame write made no progress",
            ));
        }
        advance_segments(&segments, &mut segment_index, &mut segment_offset, written)?;
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
