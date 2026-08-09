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
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use bytes::Bytes;
use cheetah_string::CheetahString;
#[cfg(unix)]
use rocketmq_store_local::ha::transfer_engine::sendfile::SendfileOperation;
#[cfg(unix)]
use rocketmq_store_local::ha::transfer_engine::sendfile::SendfileTransferEngine;
#[cfg(unix)]
use rocketmq_store_local::ha::transfer_engine::sendfile::SendfileWriteTarget;
use rocketmq_store_local::ha::transfer_engine::vectored::VectoredTransferEngine;
use rocketmq_store_local::ha::transfer_engine::TransferEngineKind;
use rocketmq_store_local::mapped_file::DefaultMappedFile;
use rocketmq_store_local::mapped_file::MappedFile;
use rocketmq_store_local::transfer::batch::TransferBatch;
use rocketmq_store_local::transfer::batch::TransferPlan;
use rocketmq_store_local::transfer::planner::TransferPlanInput;
use rocketmq_store_local::transfer::planner::TransferPlanner;
use rocketmq_store_local::transfer::segment::SegmentLease;
use rocketmq_store_local::transfer::segment::TransferCacheState;
use tokio::io::AsyncWrite;

#[test]
fn local_planner_owns_offset_resolution_flow_budget_and_file_boundary() {
    let plan = TransferPlanner::plan(
        TransferPlanInput {
            requested_offset: 900,
            next_transfer_offset: 900,
            max_commit_log_offset: 4096,
            configured_max_batch_bytes: 512,
            flow_control_available_bytes: 256,
            mapped_file_size: 1024,
            allow_cross_file_batch: false,
            heartbeat_due: false,
        },
        |offset, max_bytes, allow_cross_file| {
            assert_eq!(offset, 900);
            assert_eq!(max_bytes, 124);
            assert!(!allow_cross_file);
            Ok(vec![SegmentLease::from_bytes(
                offset,
                offset as u64,
                Bytes::from(vec![7; max_bytes]),
                TransferCacheState::Hot,
            )])
        },
    )
    .expect("plan transfer");

    let TransferPlan::Data(batch) = plan else {
        panic!("expected data batch");
    };
    assert_eq!(batch.next_offset, 1024);
    assert_eq!(batch.total_body_len, 124);
}

#[tokio::test]
async fn local_vectored_engine_preserves_frame_order_across_partial_writes() {
    let plan = TransferPlanner::plan(
        TransferPlanInput {
            requested_offset: 0,
            next_transfer_offset: 0,
            max_commit_log_offset: 8,
            configured_max_batch_bytes: 8,
            flow_control_available_bytes: 8,
            mapped_file_size: 1024,
            allow_cross_file_batch: false,
            heartbeat_due: false,
        },
        |_, _, _| {
            Ok(vec![SegmentLease::from_bytes(
                0,
                0,
                Bytes::from_static(b"abcdefgh"),
                TransferCacheState::Hot,
            )])
        },
    )
    .expect("plan transfer");
    let TransferPlan::Data(mut batch) = plan else {
        panic!("expected data batch");
    };
    batch.frame_header = Bytes::from_static(b"header");

    let mut engine = VectoredTransferEngine::new(ChunkedWriter::new(3));
    let stats = engine.send_batch(&batch).await.expect("send partial frame");
    let writer = engine.into_inner();
    assert_eq!(writer.bytes, b"headerabcdefgh");
    assert_eq!(stats.engine, TransferEngineKind::Vectored);
    assert!(stats.partial_write_count > 0);
    assert!(writer.vectored_calls > 1);
}

#[tokio::test]
async fn owning_file_range_and_bytes_fallback_emit_identical_frames() {
    let directory = tempfile::tempdir().expect("temporary mapped-file directory");
    let path = directory.path().join("00000000000000000000");
    let mapped_file = Arc::new(
        DefaultMappedFile::try_new(CheetahString::from(path.to_string_lossy().into_owned()), 64).expect("mapped file"),
    );
    let payload = b"fallback-parity";
    assert!(mapped_file.append_message_bytes(payload));
    let mut selected = mapped_file
        .select_mapped_buffer(0, payload.len() as i32)
        .expect("selected payload");
    assert!(selected.try_attach_mapped_file(Arc::clone(&mapped_file)));
    let owning = SegmentLease::from_selection(selected).expect("owning file range");
    assert!(owning.as_file_range().is_some());

    let mut file_range_batch = TransferBatch::data(0, vec![owning]);
    file_range_batch.frame_header = Bytes::from_static(b"frame:");
    let mut copied_batch = TransferBatch::data(
        0,
        vec![SegmentLease::from_bytes(
            0,
            0,
            Bytes::copy_from_slice(payload),
            TransferCacheState::Hot,
        )],
    );
    copied_batch.frame_header = Bytes::from_static(b"frame:");

    let mut file_range_fallback = VectoredTransferEngine::new(ChunkedWriter::new(4));
    let file_range_stats = file_range_fallback
        .send_batch(&file_range_batch)
        .await
        .expect("file-range fallback");
    let mut copied = VectoredTransferEngine::new(ChunkedWriter::new(4));
    let copied_stats = copied.send_batch(&copied_batch).await.expect("copied fallback");

    assert_eq!(file_range_fallback.into_inner().bytes, copied.into_inner().bytes);
    assert_eq!(file_range_stats.bytes_written, copied_stats.bytes_written);
    assert_eq!(file_range_stats.body_bytes, payload.len());
}

#[cfg(unix)]
#[tokio::test]
async fn sendfile_and_owning_bytes_fallback_emit_identical_frames() {
    let mut input = tempfile::tempfile().expect("temporary input file");
    std::io::Write::write_all(&mut input, b"sendfile-parity").expect("write input");
    let file_segment = SegmentLease::try_from_file_range(0, 0, 0, 15, Arc::new(input), TransferCacheState::Cold)
        .expect("checked file range");
    let mut file_batch = TransferBatch::data(0, vec![file_segment]);
    file_batch.frame_header = Bytes::from_static(b"frame:");
    let operation = RecordingSendfile::new(Bytes::from_static(b"sendfile-parity"), 3);
    let mut sendfile = SendfileTransferEngine::with_operation(ChunkedWriter::new(4), operation);

    let sendfile_stats = sendfile.send_batch(&file_batch).await.expect("sendfile frame");
    let (writer, operation) = sendfile.into_parts();
    let mut sendfile_frame = writer.bytes;
    sendfile_frame.extend_from_slice(&operation.emitted);

    let mut fallback_batch = TransferBatch::data(
        0,
        vec![SegmentLease::from_bytes(
            0,
            0,
            Bytes::from_static(b"sendfile-parity"),
            TransferCacheState::Cold,
        )],
    );
    fallback_batch.frame_header = Bytes::from_static(b"frame:");
    let mut fallback =
        SendfileTransferEngine::with_operation(ChunkedWriter::new(4), RecordingSendfile::new(Bytes::new(), 3));
    let fallback_stats = fallback
        .send_batch(&fallback_batch)
        .await
        .expect("owning bytes fallback");
    let (fallback_writer, _) = fallback.into_parts();

    assert_eq!(sendfile_frame, fallback_writer.bytes);
    assert_eq!(sendfile_stats.sendfile_bytes, 15);
    assert!(sendfile_stats.partial_write_count > 0);
    assert_eq!(fallback_stats.fallback_bytes, 15);
}

struct ChunkedWriter {
    max_write: usize,
    bytes: Vec<u8>,
    vectored_calls: usize,
}

impl ChunkedWriter {
    fn new(max_write: usize) -> Self {
        Self {
            max_write,
            bytes: Vec::new(),
            vectored_calls: 0,
        }
    }
}

impl AsyncWrite for ChunkedWriter {
    fn poll_write(mut self: Pin<&mut Self>, _context: &mut Context<'_>, buffer: &[u8]) -> Poll<io::Result<usize>> {
        let length = buffer.len().min(self.max_write);
        self.bytes.extend_from_slice(&buffer[..length]);
        Poll::Ready(Ok(length))
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        buffers: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.vectored_calls += 1;
        let mut remaining = self.max_write;
        let mut written = 0;
        for buffer in buffers {
            let length = buffer.len().min(remaining);
            self.bytes.extend_from_slice(&buffer[..length]);
            written += length;
            remaining -= length;
            if remaining == 0 {
                break;
            }
        }
        Poll::Ready(Ok(written))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[cfg(unix)]
impl SendfileWriteTarget for ChunkedWriter {
    fn sendfile_out_fd(&self) -> std::os::fd::RawFd {
        -1
    }
}

#[cfg(unix)]
struct RecordingSendfile {
    source: Bytes,
    emitted: Vec<u8>,
    max_write: usize,
}

#[cfg(unix)]
impl RecordingSendfile {
    fn new(source: Bytes, max_write: usize) -> Self {
        Self {
            source,
            emitted: Vec::new(),
            max_write,
        }
    }
}

#[cfg(unix)]
impl SendfileOperation for RecordingSendfile {
    fn sendfile(
        &mut self,
        _out_fd: std::os::fd::RawFd,
        _in_fd: std::os::fd::RawFd,
        offset: u64,
        len: usize,
    ) -> io::Result<usize> {
        let start = usize::try_from(offset).map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "offset"))?;
        let written = len.min(self.max_write);
        let end = start
            .checked_add(written)
            .filter(|end| *end <= self.source.len())
            .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "source"))?;
        self.emitted.extend_from_slice(&self.source[start..end]);
        Ok(written)
    }
}
