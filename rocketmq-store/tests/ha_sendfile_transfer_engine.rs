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

#![cfg(unix)]

use std::collections::VecDeque;
use std::fs::File;
use std::io;
use std::io::IoSlice;
use std::io::Write;
use std::os::fd::AsRawFd;
use std::os::fd::RawFd;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use rocketmq_store::ha::transfer_engine::sendfile::SendfileOperation;
use rocketmq_store::ha::transfer_engine::sendfile::SendfileTransferEngine;
use rocketmq_store::ha::transfer_engine::sendfile::SendfileWriteTarget;
use rocketmq_store::ha::transfer_engine::vectored::VectoredTransferEngine;
use rocketmq_store::ha::transfer_engine::HaTransferEngine;
use rocketmq_store::ha::transfer_engine::TransferEngineKind;
use rocketmq_store::transfer::batch::TransferBatch;
use rocketmq_store::transfer::batch::TransferKind;
use rocketmq_store::transfer::error::TransferError;
use rocketmq_store::transfer::segment::SegmentLease;
use rocketmq_store::transfer::segment::TransferCacheState;
use tempfile::NamedTempFile;
use tokio::io::AsyncWrite;

#[test]
fn segment_lease_from_file_range_exposes_file_position_and_len() {
    let file = Arc::new(temp_file_with_bytes(b"0123456789abcdef"));
    let lease = SegmentLease::from_file_range(4096, 4096, 5, 7, file.clone(), TransferCacheState::Hot);

    let range = lease.as_file_range().expect("file range");

    assert_eq!(range.file.as_raw_fd(), file.as_raw_fd());
    assert_eq!(range.position, 5);
    assert_eq!(range.len, 7);
    assert_eq!(lease.len(), 7);
}

#[test]
fn ha_transfer_engine_from_sendfile_selection_keeps_sendfile_kind() {
    let engine = HaTransferEngine::from_selection(RecordingWriter::default(), TransferEngineKind::Sendfile);

    assert_eq!(engine.kind(), TransferEngineKind::Sendfile);
}

#[tokio::test]
async fn sendfile_transfer_engine_writes_header_then_sendfile_ranges() {
    let file = Arc::new(temp_file_with_bytes(b"0123456789abcdef"));
    let header = transfer_header(4096, 10);
    let batch = TransferBatch {
        frame_header: header.clone(),
        segments: vec![SegmentLease::from_file_range(
            4096,
            4096,
            3,
            10,
            file,
            TransferCacheState::Hot,
        )],
        total_body_len: 10,
        start_offset: 4096,
        next_offset: 4106,
        kind: TransferKind::Data,
    };
    let mut engine = SendfileTransferEngine::with_operation(RecordingWriter::default(), RecordingSendfile::new(4));

    let stats = engine.send_batch(&batch).await.expect("sendfile transfer");

    let (writer, operation) = engine.into_parts();
    assert_eq!(writer.written, header);
    assert_eq!(operation.calls.len(), 3);
    assert_eq!(operation.calls[0].offset, 3);
    assert_eq!(operation.calls[0].len, 10);
    assert_eq!(operation.calls[1].offset, 7);
    assert_eq!(operation.calls[1].len, 6);
    assert_eq!(operation.calls[2].offset, 11);
    assert_eq!(operation.calls[2].len, 2);
    assert_eq!(stats.engine, TransferEngineKind::Sendfile);
    assert_eq!(stats.bytes_written, header.len() + 10);
    assert_eq!(stats.body_bytes, 10);
    assert_eq!(stats.write_call_count, 1);
    assert_eq!(stats.sendfile_call_count, 3);
    assert_eq!(stats.sendfile_bytes, 10);
    assert_eq!(stats.fallback_bytes, 0);
    assert_eq!(stats.partial_write_count, 2);
}

#[tokio::test]
async fn sendfile_transfer_engine_falls_back_to_vectored_for_byte_segments() {
    let header = transfer_header(8192, 4);
    let body = Bytes::from_static(b"body");
    let batch = TransferBatch {
        frame_header: header.clone(),
        segments: vec![SegmentLease::from_bytes(8192, 0, body.clone(), TransferCacheState::Hot)],
        total_body_len: body.len(),
        start_offset: 8192,
        next_offset: 8196,
        kind: TransferKind::Data,
    };
    let mut engine =
        SendfileTransferEngine::with_operation(RecordingWriter::default(), RecordingSendfile::new(usize::MAX));

    let stats = engine.send_batch(&batch).await.expect("fallback transfer");

    let (writer, operation) = engine.into_parts();
    let mut expected = header.to_vec();
    expected.extend_from_slice(&body);
    assert_eq!(writer.written, expected);
    assert!(operation.calls.is_empty());
    assert_eq!(stats.engine, TransferEngineKind::Vectored);
    assert_eq!(stats.sendfile_bytes, 0);
    assert_eq!(stats.fallback_bytes, body.len());
}

#[tokio::test]
async fn sendfile_transfer_engine_records_syscall_proxy_against_vectored_baseline() {
    let body = Bytes::from(vec![7; 64 * 1024]);
    let header = transfer_header(16384, body.len());
    let vectored_batch = TransferBatch {
        frame_header: header.clone(),
        segments: vec![SegmentLease::from_bytes(
            16384,
            0,
            body.clone(),
            TransferCacheState::Hot,
        )],
        total_body_len: body.len(),
        start_offset: 16384,
        next_offset: 16384 + body.len() as i64,
        kind: TransferKind::Data,
    };
    let mut vectored_engine = VectoredTransferEngine::new(RecordingWriter::default());

    let vectored_stats = vectored_engine
        .send_batch(&vectored_batch)
        .await
        .expect("vectored baseline transfer");

    let file = Arc::new(temp_file_with_bytes(&body));
    let sendfile_batch = TransferBatch {
        frame_header: header,
        segments: vec![SegmentLease::from_file_range(
            16384,
            16384,
            0,
            body.len(),
            file,
            TransferCacheState::Hot,
        )],
        total_body_len: body.len(),
        start_offset: 16384,
        next_offset: 16384 + body.len() as i64,
        kind: TransferKind::Data,
    };
    let mut sendfile_engine =
        SendfileTransferEngine::with_operation(RecordingWriter::default(), RecordingSendfile::new(usize::MAX));

    let sendfile_stats = sendfile_engine
        .send_batch(&sendfile_batch)
        .await
        .expect("sendfile transfer");

    assert_eq!(vectored_stats.bytes_written, sendfile_stats.bytes_written);
    assert_eq!(vectored_stats.write_call_count, 1);
    assert_eq!(vectored_stats.sendfile_call_count, 0);
    assert_eq!(sendfile_stats.write_call_count, 1);
    assert_eq!(sendfile_stats.sendfile_call_count, 1);
    assert_eq!(sendfile_stats.sendfile_bytes, body.len());
    assert_eq!(sendfile_stats.fallback_bytes, 0);
}

#[tokio::test]
async fn sendfile_transfer_engine_retries_interrupted_and_would_block_without_advancing_offset() {
    let file = Arc::new(temp_file_with_bytes(b"abcdef"));
    let header = transfer_header(32768, 6);
    let batch = TransferBatch {
        frame_header: header.clone(),
        segments: vec![SegmentLease::from_file_range(
            32768,
            32768,
            0,
            6,
            file,
            TransferCacheState::Hot,
        )],
        total_body_len: 6,
        start_offset: 32768,
        next_offset: 32774,
        kind: TransferKind::Data,
    };
    let mut engine = SendfileTransferEngine::with_operation(
        RecordingWriter::default(),
        ScriptedSendfile::new(vec![
            SendfileOutcome::Error(io::ErrorKind::Interrupted),
            SendfileOutcome::Error(io::ErrorKind::WouldBlock),
            SendfileOutcome::Write(2),
            SendfileOutcome::Write(4),
        ]),
    );

    let stats = engine.send_batch(&batch).await.expect("sendfile transfer retries");

    let (writer, operation) = engine.into_parts();
    assert_eq!(writer.written, header);
    assert_eq!(operation.calls.len(), 4);
    assert_eq!(operation.calls[0].offset, 0);
    assert_eq!(operation.calls[1].offset, 0);
    assert_eq!(operation.calls[2].offset, 0);
    assert_eq!(operation.calls[3].offset, 2);
    assert_eq!(operation.calls[0].len, 6);
    assert_eq!(operation.calls[1].len, 6);
    assert_eq!(operation.calls[2].len, 6);
    assert_eq!(operation.calls[3].len, 4);
    assert_eq!(stats.engine, TransferEngineKind::Sendfile);
    assert_eq!(stats.bytes_written, header.len() + 6);
    assert_eq!(stats.sendfile_call_count, 2);
    assert_eq!(stats.sendfile_bytes, 6);
    assert_eq!(stats.partial_write_count, 1);
}

#[tokio::test]
async fn sendfile_transfer_engine_reports_write_zero_when_connection_closes() {
    let file = Arc::new(temp_file_with_bytes(b"closed"));
    let header = transfer_header(65536, 6);
    let batch = TransferBatch {
        frame_header: header,
        segments: vec![SegmentLease::from_file_range(
            65536,
            65536,
            0,
            6,
            file,
            TransferCacheState::Hot,
        )],
        total_body_len: 6,
        start_offset: 65536,
        next_offset: 65542,
        kind: TransferKind::Data,
    };
    let mut engine = SendfileTransferEngine::with_operation(
        RecordingWriter::default(),
        ScriptedSendfile::new(vec![SendfileOutcome::Zero]),
    );

    let error = engine
        .send_batch(&batch)
        .await
        .expect_err("zero-byte sendfile should report connection close");

    match error {
        TransferError::Io(error) => assert_eq!(error.kind(), io::ErrorKind::WriteZero),
        other => panic!("expected WriteZero I/O error, got {other:?}"),
    }
}

fn temp_file_with_bytes(bytes: &[u8]) -> File {
    let mut temp = NamedTempFile::new().expect("temp file");
    temp.write_all(bytes).expect("write temp file");
    temp.reopen().expect("reopen temp file")
}

fn transfer_header(offset: i64, body_size: usize) -> Bytes {
    let mut header = BytesMut::with_capacity(12);
    header.put_i64(offset);
    header.put_i32(i32::try_from(body_size).expect("body size fits i32"));
    header.freeze()
}

#[derive(Default)]
struct RecordingWriter {
    written: Vec<u8>,
    vectored_calls: usize,
}

impl AsRawFd for RecordingWriter {
    fn as_raw_fd(&self) -> RawFd {
        77
    }
}

impl SendfileWriteTarget for RecordingWriter {
    fn sendfile_out_fd(&self) -> RawFd {
        self.as_raw_fd()
    }
}

impl AsyncWrite for RecordingWriter {
    fn poll_write(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        self.written.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.vectored_calls += 1;
        let written = bufs.iter().map(|buf| buf.len()).sum::<usize>();
        for buf in bufs {
            self.written.extend_from_slice(buf);
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

#[derive(Debug)]
struct SendfileCall {
    offset: u64,
    len: usize,
}

struct RecordingSendfile {
    max_chunk: usize,
    calls: Vec<SendfileCall>,
}

impl RecordingSendfile {
    fn new(max_chunk: usize) -> Self {
        Self {
            max_chunk: max_chunk.max(1),
            calls: Vec::new(),
        }
    }
}

impl SendfileOperation for RecordingSendfile {
    fn sendfile(&mut self, _out_fd: RawFd, _in_fd: RawFd, offset: u64, len: usize) -> io::Result<usize> {
        self.calls.push(SendfileCall { offset, len });
        Ok(len.min(self.max_chunk))
    }
}

enum SendfileOutcome {
    Write(usize),
    Error(io::ErrorKind),
    Zero,
}

struct ScriptedSendfile {
    outcomes: VecDeque<SendfileOutcome>,
    calls: Vec<SendfileCall>,
}

impl ScriptedSendfile {
    fn new(outcomes: Vec<SendfileOutcome>) -> Self {
        Self {
            outcomes: VecDeque::from(outcomes),
            calls: Vec::new(),
        }
    }
}

impl SendfileOperation for ScriptedSendfile {
    fn sendfile(&mut self, _out_fd: RawFd, _in_fd: RawFd, offset: u64, len: usize) -> io::Result<usize> {
        self.calls.push(SendfileCall { offset, len });
        match self.outcomes.pop_front().unwrap_or(SendfileOutcome::Write(len)) {
            SendfileOutcome::Write(written) => Ok(written.min(len)),
            SendfileOutcome::Error(kind) => Err(io::Error::new(kind, "scripted sendfile error")),
            SendfileOutcome::Zero => Ok(0),
        }
    }
}
