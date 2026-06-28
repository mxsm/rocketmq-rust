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

#[cfg(unix)]
use std::io;
#[cfg(unix)]
use std::os::fd::AsRawFd;
#[cfg(unix)]
use std::os::fd::RawFd;

#[cfg(unix)]
use tokio::io::AsyncWrite;
#[cfg(unix)]
use tokio::io::AsyncWriteExt;

#[cfg(unix)]
use crate::ha::transfer_engine::bytes::write_all_counted;
#[cfg(unix)]
use crate::ha::transfer_engine::vectored::VectoredTransferEngine;
#[cfg(unix)]
use crate::ha::transfer_engine::write_zero_error;
#[cfg(unix)]
use crate::ha::transfer_engine::TransferEngineKind;
#[cfg(unix)]
use crate::ha::transfer_engine::TransferStats;
#[cfg(unix)]
use crate::transfer::batch::TransferBatch;
#[cfg(unix)]
use crate::transfer::error::TransferError;
#[cfg(unix)]
use crate::transfer::error::TransferResult;
#[cfg(unix)]
use crate::transfer::segment::FileRange;

#[cfg(unix)]
pub trait SendfileOperation {
    fn sendfile(&mut self, out_fd: RawFd, in_fd: RawFd, offset: u64, len: usize) -> io::Result<usize>;
}

#[cfg(unix)]
#[derive(Debug, Default)]
pub struct LinuxSendfileOperation;

#[cfg(all(unix, target_os = "linux"))]
impl SendfileOperation for LinuxSendfileOperation {
    fn sendfile(&mut self, out_fd: RawFd, in_fd: RawFd, offset: u64, len: usize) -> io::Result<usize> {
        let mut raw_offset = libc::off_t::try_from(offset).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("sendfile offset exceeds libc::off_t: {offset}"),
            )
        })?;
        let written = unsafe { libc::sendfile(out_fd, in_fd, &mut raw_offset, len) };
        if written < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(written as usize)
        }
    }
}

#[cfg(all(unix, not(target_os = "linux")))]
impl SendfileOperation for LinuxSendfileOperation {
    fn sendfile(&mut self, _out_fd: RawFd, _in_fd: RawFd, _offset: u64, _len: usize) -> io::Result<usize> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "sendfile transfer is only enabled on Linux",
        ))
    }
}

#[cfg(unix)]
pub struct SendfileTransferEngine<W, O = LinuxSendfileOperation> {
    writer: W,
    operation: O,
}

#[cfg(unix)]
impl<W> SendfileTransferEngine<W, LinuxSendfileOperation> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            operation: LinuxSendfileOperation,
        }
    }
}

#[cfg(unix)]
impl<W, O> SendfileTransferEngine<W, O> {
    pub fn with_operation(writer: W, operation: O) -> Self {
        Self { writer, operation }
    }

    pub fn into_parts(self) -> (W, O) {
        (self.writer, self.operation)
    }
}

#[cfg(unix)]
impl<W, O> SendfileTransferEngine<W, O>
where
    W: AsyncWrite + AsRawFd + Unpin,
    O: SendfileOperation,
{
    pub async fn send_batch(&mut self, batch: &TransferBatch) -> TransferResult<TransferStats> {
        let Some(file_ranges) = batch_file_ranges(batch) else {
            let mut fallback = VectoredTransferEngine::new(&mut self.writer);
            let mut stats = fallback.send_batch(batch).await?;
            stats.fallback_bytes = stats.body_bytes;
            return Ok(stats);
        };

        let expected_header_calls = usize::from(!batch.frame_header.is_empty());
        let (header_written, header_calls) = write_all_counted(&mut self.writer, &batch.frame_header).await?;
        let mut bytes_written = header_written;
        let mut sendfile_bytes = 0;
        let mut sendfile_call_count = 0;
        let mut partial_write_count = header_calls.saturating_sub(expected_header_calls);
        let out_fd = self.writer.as_raw_fd();

        for range in file_ranges {
            let in_fd = range.file.as_raw_fd();
            let mut position = range.position;
            let mut remaining = range.len;
            while remaining > 0 {
                let written = match self.operation.sendfile(out_fd, in_fd, position, remaining) {
                    Ok(0) => return Err(write_zero_error()),
                    Ok(written) => written,
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    Err(error) => return Err(TransferError::Io(error)),
                };

                sendfile_call_count += 1;
                bytes_written += written;
                sendfile_bytes += written;
                if written < remaining {
                    partial_write_count += 1;
                }
                position += written as u64;
                remaining -= written;
            }
        }

        self.writer.flush().await?;

        Ok(TransferStats {
            engine: TransferEngineKind::Sendfile,
            bytes_written,
            body_bytes: batch.total_body_len,
            frame_count: 1,
            write_call_count: header_calls,
            sendfile_call_count,
            sendfile_bytes,
            fallback_bytes: 0,
            partial_write_count,
        })
    }
}

#[cfg(unix)]
fn batch_file_ranges(batch: &TransferBatch) -> Option<Vec<FileRange>> {
    let mut remaining = batch.total_body_len;
    if remaining == 0 {
        return Some(Vec::new());
    }

    let mut ranges = Vec::with_capacity(batch.segments.len());
    for segment in &batch.segments {
        let mut range = segment.as_file_range()?;
        let len = range.len.min(remaining);
        if len > 0 {
            range.len = len;
            ranges.push(range);
            remaining -= len;
        }
        if remaining == 0 {
            break;
        }
    }

    (remaining == 0).then_some(ranges)
}
