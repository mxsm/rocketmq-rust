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

use std::io;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use rocketmq_runtime::BlockingExecutor;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;

use crate::file_region::FileRegion;
use crate::file_region_io::read_file_region_chunk;
use crate::file_region_io::FILE_REGION_READ_CHUNK_BYTES;

static PORTABLE_BYTES: AtomicU64 = AtomicU64::new(0);
static SENDFILE_BYTES: AtomicU64 = AtomicU64::new(0);
static FALLBACK_UNSUPPORTED: AtomicU64 = AtomicU64::new(0);
static SELECTION_FAILURES: AtomicU64 = AtomicU64::new(0);
static HEAD_FAILURES: AtomicU64 = AtomicU64::new(0);
static BODY_FAILURES: AtomicU64 = AtomicU64::new(0);

/// Monotonic process-wide file-transfer diagnostics with fixed-cardinality fields.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FileTransferSnapshot {
    /// Bytes written through bounded portable reads.
    pub portable_bytes: u64,
    /// Bytes written by Linux `sendfile`.
    pub sendfile_bytes: u64,
    /// Automatic fallbacks caused by an unsupported native path.
    pub fallback_unsupported: u64,
    /// Failures while selecting or preflighting a transfer backend.
    pub selection_failures: u64,
    /// Failures while writing a frame prefix/header.
    pub head_failures: u64,
    /// Failures while writing a file body.
    pub body_failures: u64,
}

/// Returns process-wide file-transfer counters without file paths or payload identifiers.
#[must_use]
pub fn file_transfer_snapshot() -> FileTransferSnapshot {
    FileTransferSnapshot {
        portable_bytes: PORTABLE_BYTES.load(Ordering::Relaxed),
        sendfile_bytes: SENDFILE_BYTES.load(Ordering::Relaxed),
        fallback_unsupported: FALLBACK_UNSUPPORTED.load(Ordering::Relaxed),
        selection_failures: SELECTION_FAILURES.load(Ordering::Relaxed),
        head_failures: HEAD_FAILURES.load(Ordering::Relaxed),
        body_failures: BODY_FAILURES.load(Ordering::Relaxed),
    }
}

pub(crate) fn record_portable_bytes(bytes: u64) {
    PORTABLE_BYTES.fetch_add(bytes, Ordering::Relaxed);
}

#[cfg(all(target_os = "linux", feature = "linux-sendfile"))]
pub(crate) fn record_sendfile_bytes(bytes: u64) {
    SENDFILE_BYTES.fetch_add(bytes, Ordering::Relaxed);
}

pub(crate) fn record_fallback_unsupported() {
    FALLBACK_UNSUPPORTED.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_selection_failure() {
    SELECTION_FAILURES.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_head_failure() {
    HEAD_FAILURES.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_body_failure() {
    BODY_FAILURES.fetch_add(1, Ordering::Relaxed);
}

pub(crate) async fn write_portable_file_region<W>(
    writer: &mut W,
    region: &FileRegion,
    blocking: &BlockingExecutor,
) -> io::Result<u64>
where
    W: AsyncWrite + Unpin,
{
    let mut sent = 0_u64;
    let initial_len = usize::try_from(region.len().min(FILE_REGION_READ_CHUNK_BYTES as u64))
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "file-region chunk length exceeds usize"))?;
    let mut buffer = vec![0_u8; initial_len];
    while sent < region.len() {
        let chunk_len = usize::try_from((region.len() - sent).min(FILE_REGION_READ_CHUNK_BYTES as u64))
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "file-region chunk length exceeds usize"))?;
        buffer.resize(chunk_len, 0);
        let region_offset = region.offset();
        let lease = region.lease().clone();
        let (returned_buffer, read) = blocking
            .spawn_io("transport.file-region.read", move || {
                let read = read_file_region_chunk(lease.file(), &mut buffer, region_offset, sent)?;
                Ok::<_, io::Error>((buffer, read))
            })
            .await
            .map_err(|error| io::Error::other(error.to_string()))??;
        buffer = returned_buffer;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "leased file region ended before its validated length",
            ));
        }
        writer.write_all(&buffer[..read]).await?;
        sent = sent
            .checked_add(read as u64)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "file-region progress overflow"))?;
    }
    Ok(sent)
}
