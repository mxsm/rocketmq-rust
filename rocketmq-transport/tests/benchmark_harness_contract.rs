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
use std::io::IoSlice;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;

use rocketmq_protocol::protocol::encoded_frame::EncodedFrame;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_transport::Connection;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;

#[derive(Default)]
struct Record {
    bytes: Vec<u8>,
    writes: usize,
    flushes: usize,
}

struct RecordingDuplex {
    record: Arc<Mutex<Record>>,
}

impl RecordingDuplex {
    fn new() -> (Self, Arc<Mutex<Record>>) {
        let record = Arc::new(Mutex::new(Record::default()));
        (Self { record: record.clone() }, record)
    }
}

impl AsyncRead for RecordingDuplex {
    fn poll_read(self: Pin<&mut Self>, _context: &mut Context<'_>, _buffer: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        Poll::Pending
    }
}

impl AsyncWrite for RecordingDuplex {
    fn poll_write(self: Pin<&mut Self>, _context: &mut Context<'_>, bytes: &[u8]) -> Poll<io::Result<usize>> {
        let mut record = self.record.lock().expect("record lock");
        record.bytes.extend_from_slice(bytes);
        record.writes += 1;
        Poll::Ready(Ok(bytes.len()))
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        buffers: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let mut record = self.record.lock().expect("record lock");
        let mut bytes = 0;
        for buffer in buffers {
            record.bytes.extend_from_slice(buffer);
            bytes += buffer.len();
        }
        record.writes += 1;
        Poll::Ready(Ok(bytes))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.record.lock().expect("record lock").flushes += 1;
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

fn commands() -> Vec<RemotingCommand> {
    vec![
        RemotingCommand::create_remoting_command(10_100)
            .set_opaque(1)
            .set_body(vec![0x11; 128]),
        RemotingCommand::create_remoting_command(10_101)
            .set_opaque(2)
            .set_body(vec![0x22; 4 * 1024]),
    ]
}

#[tokio::test]
async fn counting_harness_observes_exact_frame_bytes_and_batch_flush_contract() {
    let commands = commands();
    let expected = commands
        .iter()
        .cloned()
        .flat_map(|command| {
            EncodedFrame::from_command(command)
                .expect("encode expected frame")
                .into_bytes()
        })
        .collect::<Vec<_>>();

    let (batch_io, batch_record) = RecordingDuplex::new();
    let mut batch = Connection::new_with_plaintext_stream(batch_io);
    batch.send_batch(commands.clone()).await.expect("batch write");
    {
        let batch_record = batch_record.lock().expect("batch record lock");
        assert_eq!(batch_record.bytes, expected);
        assert_eq!(batch_record.flushes, 1);
        assert!(batch_record.writes >= 1);
    }

    let (sequential_io, sequential_record) = RecordingDuplex::new();
    let mut sequential = Connection::new_with_plaintext_stream(sequential_io);
    for command in commands {
        sequential.send_command(command).await.expect("sequential write");
    }
    let sequential_record = sequential_record.lock().expect("sequential record lock");
    assert_eq!(sequential_record.bytes, expected);
    assert_eq!(sequential_record.flushes, 2);
}
