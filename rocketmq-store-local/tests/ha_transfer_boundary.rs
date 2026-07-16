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
use std::task::Context;
use std::task::Poll;

use bytes::Bytes;
use rocketmq_store_local::ha::transfer_engine::vectored::VectoredTransferEngine;
use rocketmq_store_local::ha::transfer_engine::TransferEngineKind;
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
