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
use rocketmq_store::ha::transfer_engine::select_transfer_engine;
use rocketmq_store::ha::transfer_engine::select_transfer_engine_with_availability;
use rocketmq_store::ha::transfer_engine::vectored::VectoredTransferEngine;
use rocketmq_store::ha::transfer_engine::TransferEngineAvailability;
use rocketmq_store::ha::transfer_engine::TransferEngineKind;
use rocketmq_store::ha::transfer_engine::TransferEnginePreference;
use rocketmq_store::transfer::batch::TransferBatch;
use rocketmq_store::transfer::batch::TransferKind;
use rocketmq_store::transfer::segment::SegmentLease;
use rocketmq_store::transfer::segment::TransferCacheState;
use tokio::io::AsyncWrite;

#[tokio::test]
async fn bytes_transfer_engine_matches_existing_ha_frame_bytes() {
    let header = transfer_header(128, 4);
    let body = Bytes::from_static(b"data");
    let batch = transfer_batch(header.clone(), body.clone());
    let mut engine = BytesTransferEngine::new(ChunkedWriter::new(usize::MAX));

    let stats = engine.send_batch(&batch).await.expect("bytes transfer");

    let writer = engine.into_inner();
    let mut expected = header.to_vec();
    expected.extend_from_slice(&body);
    assert_eq!(writer.written, expected);
    assert_eq!(stats.engine, TransferEngineKind::Bytes);
    assert_eq!(stats.bytes_written, expected.len());
    assert_eq!(stats.body_bytes, body.len());
    assert_eq!(stats.frame_count, 1);
    assert_eq!(stats.write_call_count, 2);
    assert_eq!(stats.partial_write_count, 0);
    assert_eq!(writer.write_calls, 2);
}

#[tokio::test]
async fn vectored_transfer_engine_handles_partial_writes_without_reordering_frame() {
    let header = transfer_header(256, 8);
    let body = Bytes::from_static(b"abcdefgh");
    let batch = transfer_batch(header.clone(), body.clone());
    let mut engine = VectoredTransferEngine::new(ChunkedWriter::new(5));

    let stats = engine.send_batch(&batch).await.expect("vectored transfer");

    let writer = engine.into_inner();
    let mut expected = header.to_vec();
    expected.extend_from_slice(&body);
    assert_eq!(writer.written, expected);
    assert!(writer.vectored_calls > 1);
    assert_eq!(stats.engine, TransferEngineKind::Vectored);
    assert_eq!(stats.bytes_written, expected.len());
    assert_eq!(stats.body_bytes, body.len());
    assert!(stats.partial_write_count > 0);
}

#[tokio::test]
async fn vectored_transfer_engine_reduces_write_calls_for_complete_frame_writes() {
    let body = Bytes::from(vec![7; 64 * 1024]);
    let header = transfer_header(512, body.len());
    let batch = transfer_batch(header, body);

    let mut bytes_engine = BytesTransferEngine::new(ChunkedWriter::new(usize::MAX));
    let bytes_stats = bytes_engine.send_batch(&batch).await.expect("bytes transfer");
    let bytes_writer = bytes_engine.into_inner();

    let mut vectored_engine = VectoredTransferEngine::new(ChunkedWriter::new(usize::MAX));
    let vectored_stats = vectored_engine.send_batch(&batch).await.expect("vectored transfer");
    let vectored_writer = vectored_engine.into_inner();

    assert_eq!(bytes_stats.write_call_count, 2);
    assert_eq!(bytes_writer.write_calls, 2);
    assert_eq!(bytes_writer.vectored_calls, 0);
    assert_eq!(vectored_stats.write_call_count, 1);
    assert_eq!(vectored_writer.write_calls, 0);
    assert_eq!(vectored_writer.vectored_calls, 1);
    assert_eq!(bytes_writer.written, vectored_writer.written);
}

#[tokio::test]
async fn vectored_transfer_engine_falls_back_to_bytes_when_first_vectored_write_fails() {
    let header = transfer_header(768, 6);
    let body = Bytes::from_static(b"rocket");
    let batch = transfer_batch(header.clone(), body.clone());
    let mut engine = VectoredTransferEngine::new(FailingVectoredWriter::fail_before_write());

    let stats = engine.send_batch(&batch).await.expect("fallback transfer");

    let writer = engine.into_inner();
    let mut expected = header.to_vec();
    expected.extend_from_slice(&body);
    assert_eq!(writer.written, expected);
    assert_eq!(stats.engine, TransferEngineKind::Bytes);
    assert_eq!(stats.bytes_written, expected.len());
    assert_eq!(stats.body_bytes, body.len());
    assert_eq!(stats.fallback_bytes, body.len());
    assert_eq!(writer.vectored_calls, 1);
    assert_eq!(writer.write_calls, 2);
}

#[tokio::test]
async fn vectored_transfer_engine_does_not_fallback_after_partial_write() {
    let header = transfer_header(1024, 6);
    let body = Bytes::from_static(b"rocket");
    let batch = transfer_batch(header.clone(), body.clone());
    let mut engine = VectoredTransferEngine::new(FailingVectoredWriter::fail_after_partial_write(5));

    let error = engine
        .send_batch(&batch)
        .await
        .expect_err("partial frame write should remain non-retriable");

    let writer = engine.into_inner();
    let mut expected = header.to_vec();
    expected.extend_from_slice(&body);
    assert_eq!(writer.written, expected[..5]);
    assert_eq!(writer.write_calls, 0);
    assert_eq!(writer.vectored_calls, 2);
    assert!(error.to_string().contains("vectored write failed after partial frame"));
}

#[test]
fn transfer_engine_selection_falls_back_to_bytes_when_vectored_is_unavailable() {
    let selection = select_transfer_engine(TransferEnginePreference::Vectored, false);

    assert_eq!(selection.engine, TransferEngineKind::Bytes);
    assert!(selection.fallback_reason.is_some());
}

#[test]
fn sendfile_transfer_selection_uses_sendfile_when_available() {
    let selection = select_transfer_engine_with_availability(
        TransferEnginePreference::Sendfile,
        TransferEngineAvailability {
            vectored_write_available: true,
            sendfile_available: true,
            io_uring_available: false,
        },
    );

    assert_eq!(selection.engine, TransferEngineKind::Sendfile);
    assert_eq!(selection.fallback_reason, None);
}

#[test]
fn sendfile_transfer_selection_falls_back_to_vectored_then_bytes() {
    let vectored_selection = select_transfer_engine_with_availability(
        TransferEnginePreference::Sendfile,
        TransferEngineAvailability {
            vectored_write_available: true,
            sendfile_available: false,
            io_uring_available: false,
        },
    );
    let bytes_selection = select_transfer_engine_with_availability(
        TransferEnginePreference::Sendfile,
        TransferEngineAvailability {
            vectored_write_available: false,
            sendfile_available: false,
            io_uring_available: false,
        },
    );

    assert_eq!(vectored_selection.engine, TransferEngineKind::Vectored);
    assert_eq!(vectored_selection.fallback_reason, Some("sendfile unavailable"));
    assert_eq!(bytes_selection.engine, TransferEngineKind::Bytes);
    assert_eq!(
        bytes_selection.fallback_reason,
        Some("sendfile and vectored write unavailable")
    );
}

#[test]
fn sendfile_availability_gate_falls_back_for_non_linux_tls_or_missing_capability() {
    for blocked_gate in ["non-linux", "tls-transport", "missing-capability"] {
        let selection = select_transfer_engine_with_availability(
            TransferEnginePreference::Sendfile,
            TransferEngineAvailability {
                vectored_write_available: true,
                sendfile_available: false,
                io_uring_available: false,
            },
        );
        let auto_selection = select_transfer_engine_with_availability(
            TransferEnginePreference::Auto,
            TransferEngineAvailability {
                vectored_write_available: true,
                sendfile_available: false,
                io_uring_available: false,
            },
        );

        assert_eq!(selection.engine, TransferEngineKind::Vectored, "{blocked_gate}");
        assert_eq!(selection.fallback_reason, Some("sendfile unavailable"));
        assert_eq!(auto_selection.engine, TransferEngineKind::Vectored, "{blocked_gate}");
        assert_eq!(auto_selection.fallback_reason, None);
    }
}

#[test]
fn auto_transfer_selection_prefers_sendfile_then_vectored_then_bytes() {
    let sendfile_selection = select_transfer_engine_with_availability(
        TransferEnginePreference::Auto,
        TransferEngineAvailability {
            vectored_write_available: true,
            sendfile_available: true,
            io_uring_available: false,
        },
    );
    let vectored_selection = select_transfer_engine_with_availability(
        TransferEnginePreference::Auto,
        TransferEngineAvailability {
            vectored_write_available: true,
            sendfile_available: false,
            io_uring_available: false,
        },
    );
    let bytes_selection = select_transfer_engine_with_availability(
        TransferEnginePreference::Auto,
        TransferEngineAvailability {
            vectored_write_available: false,
            sendfile_available: false,
            io_uring_available: false,
        },
    );

    assert_eq!(sendfile_selection.engine, TransferEngineKind::Sendfile);
    assert_eq!(sendfile_selection.fallback_reason, None);
    assert_eq!(vectored_selection.engine, TransferEngineKind::Vectored);
    assert_eq!(vectored_selection.fallback_reason, None);
    assert_eq!(bytes_selection.engine, TransferEngineKind::Bytes);
    assert_eq!(bytes_selection.fallback_reason, Some("vectored write unavailable"));
}

#[test]
fn io_uring_transfer_selection_uses_io_uring_when_available() {
    let selection = select_transfer_engine_with_availability(
        TransferEnginePreference::IoUring,
        TransferEngineAvailability {
            vectored_write_available: true,
            sendfile_available: true,
            io_uring_available: true,
        },
    );

    assert_eq!(selection.engine, TransferEngineKind::IoUring);
    assert_eq!(selection.fallback_reason, None);
}

#[test]
fn io_uring_transfer_selection_falls_back_to_vectored_when_unavailable() {
    let selection = select_transfer_engine_with_availability(
        TransferEnginePreference::IoUring,
        TransferEngineAvailability {
            vectored_write_available: true,
            sendfile_available: true,
            io_uring_available: false,
        },
    );

    assert_eq!(selection.engine, TransferEngineKind::Vectored);
    assert_eq!(selection.fallback_reason, Some("io_uring unavailable"));
}

#[test]
fn io_uring_transfer_selection_falls_back_to_bytes_when_vectored_is_unavailable() {
    let selection = select_transfer_engine_with_availability(
        TransferEnginePreference::IoUring,
        TransferEngineAvailability {
            vectored_write_available: false,
            sendfile_available: false,
            io_uring_available: false,
        },
    );

    assert_eq!(selection.engine, TransferEngineKind::Bytes);
    assert_eq!(
        selection.fallback_reason,
        Some("io_uring and vectored write unavailable")
    );
}

fn transfer_header(offset: i64, body_size: usize) -> Bytes {
    let mut header = BytesMut::with_capacity(12);
    header.put_i64(offset);
    header.put_i32(i32::try_from(body_size).expect("body size fits i32"));
    header.freeze()
}

fn transfer_batch(header: Bytes, body: Bytes) -> TransferBatch {
    let total_body_len = body.len();
    TransferBatch {
        frame_header: header,
        segments: vec![SegmentLease::from_bytes(128, 128, body, TransferCacheState::Hot)],
        total_body_len,
        start_offset: 128,
        next_offset: 128 + total_body_len as i64,
        kind: TransferKind::Data,
    }
}

struct ChunkedWriter {
    max_write: usize,
    written: Vec<u8>,
    write_calls: usize,
    vectored_calls: usize,
}

impl ChunkedWriter {
    fn new(max_write: usize) -> Self {
        Self {
            max_write: max_write.max(1),
            written: Vec::new(),
            write_calls: 0,
            vectored_calls: 0,
        }
    }
}

impl AsyncWrite for ChunkedWriter {
    fn poll_write(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        self.write_calls += 1;
        let len = buf.len().min(self.max_write);
        self.written.extend_from_slice(&buf[..len]);
        Poll::Ready(Ok(len))
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.vectored_calls += 1;
        let mut remaining = self.max_write;
        let mut written = 0;
        for buf in bufs {
            if remaining == 0 {
                break;
            }
            let len = buf.len().min(remaining);
            self.written.extend_from_slice(&buf[..len]);
            remaining -= len;
            written += len;
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

struct FailingVectoredWriter {
    written: Vec<u8>,
    write_calls: usize,
    vectored_calls: usize,
    mode: FailingVectoredMode,
}

enum FailingVectoredMode {
    BeforeWrite,
    AfterPartialWrite { first_write_len: usize },
}

impl FailingVectoredWriter {
    fn fail_before_write() -> Self {
        Self {
            written: Vec::new(),
            write_calls: 0,
            vectored_calls: 0,
            mode: FailingVectoredMode::BeforeWrite,
        }
    }

    fn fail_after_partial_write(first_write_len: usize) -> Self {
        Self {
            written: Vec::new(),
            write_calls: 0,
            vectored_calls: 0,
            mode: FailingVectoredMode::AfterPartialWrite { first_write_len },
        }
    }

    fn write_vectored_slices(&mut self, bufs: &[IoSlice<'_>], limit: usize) -> usize {
        let mut remaining = limit;
        let mut written = 0;
        for buf in bufs {
            if remaining == 0 {
                break;
            }
            let len = buf.len().min(remaining);
            self.written.extend_from_slice(&buf[..len]);
            remaining -= len;
            written += len;
        }
        written
    }
}

impl AsyncWrite for FailingVectoredWriter {
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
        match self.mode {
            FailingVectoredMode::BeforeWrite if self.vectored_calls == 1 => {
                return Poll::Ready(Err(io::Error::other("vectored write failed before frame bytes")));
            }
            FailingVectoredMode::AfterPartialWrite { first_write_len } if self.vectored_calls == 1 => {
                let written = self.write_vectored_slices(bufs, first_write_len);
                return Poll::Ready(Ok(written));
            }
            FailingVectoredMode::AfterPartialWrite { .. } if self.vectored_calls == 2 => {
                return Poll::Ready(Err(io::Error::other("vectored write failed after partial frame")));
            }
            _ => {}
        }

        let written = self.write_vectored_slices(bufs, usize::MAX);
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
