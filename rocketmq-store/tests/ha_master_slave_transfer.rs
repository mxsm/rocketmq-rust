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

use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use rocketmq_store::ha::transfer_engine::bytes::BytesTransferEngine;
use rocketmq_store::ha::transfer_engine::vectored::VectoredTransferEngine;
use rocketmq_store::ha::transfer_engine::TransferEngineKind;
use rocketmq_store::ha::transfer_engine::TransferStats;
use rocketmq_store::transfer::batch::TransferBatch;
use rocketmq_store::transfer::batch::TransferPlan;
use rocketmq_store::transfer::planner::TransferPlanInput;
use rocketmq_store::transfer::planner::TransferPlanner;
use rocketmq_store::transfer::segment::SegmentLease;
use rocketmq_store::transfer::segment::TransferCacheState;
use tokio::io::AsyncWrite;

#[tokio::test]
async fn single_master_slave_transfer_round_trips_with_bytes_and_vectored_engines() {
    let mut batch = master_plans_single_slave_batch();
    batch.frame_header = transfer_header(batch.start_offset, batch.total_body_len);
    let bytes_result = send_with_bytes_engine(&batch).await;
    let vectored_result = send_with_vectored_engine(&batch).await;

    assert_eq!(bytes_result.slave_frame.body, b"single-master-single-slave");
    assert_eq!(vectored_result.slave_frame.body, bytes_result.slave_frame.body);
    assert_eq!(bytes_result.slave_frame.ack_offset, batch.next_offset);
    assert_eq!(vectored_result.slave_frame.ack_offset, batch.next_offset);
    assert_eq!(
        bytes_result.slave_frame.ack_offset,
        vectored_result.slave_frame.ack_offset
    );
    assert_eq!(bytes_result.written, vectored_result.written);

    assert_eq!(bytes_result.stats.engine, TransferEngineKind::Bytes);
    assert_eq!(vectored_result.stats.engine, TransferEngineKind::Vectored);
    assert_eq!(bytes_result.stats.bytes_written, vectored_result.stats.bytes_written);
    assert_eq!(bytes_result.stats.body_bytes, vectored_result.stats.body_bytes);
    assert_eq!(bytes_result.writer.write_calls, 2);
    assert_eq!(bytes_result.writer.vectored_calls, 0);
    assert_eq!(vectored_result.writer.write_calls, 0);
    assert_eq!(vectored_result.writer.vectored_calls, 1);
}

fn master_plans_single_slave_batch() -> TransferBatch {
    let input = TransferPlanInput {
        requested_offset: 4096,
        next_transfer_offset: 4096,
        max_commit_log_offset: 4122,
        configured_max_batch_bytes: 1024,
        flow_control_available_bytes: 1024,
        mapped_file_size: 1024 * 1024,
        allow_cross_file_batch: false,
        heartbeat_due: false,
    };

    let plan = TransferPlanner::plan(input, |offset, max_bytes, allow_cross_file| {
        assert_eq!(offset, 4096);
        assert_eq!(max_bytes, 26);
        assert!(!allow_cross_file);
        Ok(vec![
            SegmentLease::from_bytes(
                offset,
                offset as u64,
                Bytes::from_static(b"single-master-"),
                TransferCacheState::Hot,
            ),
            SegmentLease::from_bytes(
                offset + 14,
                (offset + 14) as u64,
                Bytes::from_static(b"single-slave"),
                TransferCacheState::Hot,
            ),
        ])
    })
    .expect("master should plan one slave transfer batch");

    match plan {
        TransferPlan::Data(batch) => batch,
        TransferPlan::Heartbeat { .. } | TransferPlan::NoData => panic!("expected data transfer plan"),
    }
}

async fn send_with_bytes_engine(batch: &TransferBatch) -> TransferResultSnapshot {
    let mut engine = BytesTransferEngine::new(RecordingWriter::default());
    let stats = engine.send_batch(batch).await.expect("bytes HA transfer");
    let writer = engine.into_inner();
    let slave_frame = SlaveFrame::decode(&writer.written);

    TransferResultSnapshot {
        written: writer.written.clone(),
        writer,
        stats,
        slave_frame,
    }
}

async fn send_with_vectored_engine(batch: &TransferBatch) -> TransferResultSnapshot {
    let mut engine = VectoredTransferEngine::new(RecordingWriter::default());
    let stats = engine.send_batch(batch).await.expect("vectored HA transfer");
    let writer = engine.into_inner();
    let slave_frame = SlaveFrame::decode(&writer.written);

    TransferResultSnapshot {
        written: writer.written.clone(),
        writer,
        stats,
        slave_frame,
    }
}

fn transfer_header(offset: i64, body_size: usize) -> Bytes {
    let mut header = BytesMut::with_capacity(12);
    header.put_i64(offset);
    header.put_i32(i32::try_from(body_size).expect("body size fits i32"));
    header.freeze()
}

struct TransferResultSnapshot {
    written: Vec<u8>,
    writer: RecordingWriter,
    stats: TransferStats,
    slave_frame: SlaveFrame,
}

struct SlaveFrame {
    body: Vec<u8>,
    ack_offset: i64,
}

impl SlaveFrame {
    fn decode(frame: &[u8]) -> Self {
        assert!(frame.len() >= 12, "HA frame must include the 12-byte header");
        let offset = i64::from_be_bytes(frame[0..8].try_into().expect("offset header"));
        let body_len = i32::from_be_bytes(frame[8..12].try_into().expect("body size header"));
        assert!(body_len >= 0, "HA body size must be non-negative");
        let body_len = body_len as usize;
        let body = frame[12..].to_vec();

        assert_eq!(body.len(), body_len);
        Self {
            body,
            ack_offset: offset + body_len as i64,
        }
    }
}

#[derive(Default)]
struct RecordingWriter {
    written: Vec<u8>,
    write_calls: usize,
    vectored_calls: usize,
}

impl AsyncWrite for RecordingWriter {
    fn poll_write(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        self.write_calls += 1;
        self.written.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.vectored_calls += 1;
        let mut written = 0;
        for buf in bufs {
            self.written.extend_from_slice(buf);
            written += buf.len();
        }
        Poll::Ready(Ok(written))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
