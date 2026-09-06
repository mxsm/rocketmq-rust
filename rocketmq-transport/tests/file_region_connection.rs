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

#![cfg(feature = "test-support")]

use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_error::RocketMQError;
use rocketmq_protocol::protocol::RemotingCommand;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_transport::api::file_transfer_snapshot;
use rocketmq_transport::api::ConnectionState;
use rocketmq_transport::api::FileRegion;
use rocketmq_transport::api::FileRegionSequence;
use rocketmq_transport::api::FileTransferMode;
use rocketmq_transport::api::RequestDeadline;
use rocketmq_transport::test_support::Connection;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;

fn runtime_owner() -> RuntimeOwner {
    RuntimeOwner::new().expect("test runtime owner")
}

fn region_with_prefix(prefix_len: usize, body: &[u8]) -> (Arc<std::fs::File>, FileRegion) {
    let mut file = tempfile::tempfile().expect("temporary file");
    file.write_all(&vec![0x55; prefix_len]).expect("write prefix");
    file.write_all(body).expect("write body");
    file.flush().expect("flush temporary file");
    let file = Arc::new(file);
    let region = FileRegion::try_new(file.clone(), prefix_len as u64, body.len() as u64).expect("valid file region");
    (file, region)
}

async fn tcp_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let (client, accepted) = tokio::join!(TcpStream::connect(address), listener.accept());
    (client.expect("connect loopback"), accepted.expect("accept loopback").0)
}

#[test]
fn portable_file_region_round_trips_with_nonzero_offset() {
    let owner = runtime_owner();
    let blocking = owner.root_context().component("file-region-test").storage_io().clone();
    let sizes = [1_usize, 4 * 1024, 1024 * 1024];
    let expected_bytes = sizes.iter().sum::<usize>() as u64;
    let before = file_transfer_snapshot();

    owner.block_on(async move {
        for (case, size) in sizes.into_iter().enumerate() {
            let body: Vec<u8> = (0..size).map(|index| (index % 251) as u8).collect();
            let (_file, region) = region_with_prefix(137 + case, &body);
            let (client, accepted) = tcp_pair().await;
            let mut sender = Connection::new(client).with_file_region_io(blocking.clone(), FileTransferMode::Portable);
            let mut receiver = Connection::new(accepted);
            let command = RemotingCommand::create_remoting_command(321).set_opaque(88 + case as i32);
            let (sent, received) = tokio::join!(
                sender.send_file_region_command(command, region, RequestDeadline::after(Duration::from_secs(5))),
                receiver.receive_command()
            );

            sent.expect("portable file frame should send");
            let received = received
                .expect("peer should receive a frame")
                .expect("frame should decode");
            assert_eq!(received.code(), 321);
            assert_eq!(received.opaque(), 88 + case as i32);
            assert_eq!(received.body().expect("file body").as_ref(), body.as_slice());
        }
    });

    let after = file_transfer_snapshot();
    assert!(after.portable_bytes.saturating_sub(before.portable_bytes) >= expected_bytes);
}

#[test]
fn portable_file_region_sequence_round_trips_as_one_frame() {
    let owner = runtime_owner();
    let blocking = owner
        .root_context()
        .component("file-region-sequence")
        .storage_io()
        .clone();
    let (_first_file, first) = region_with_prefix(11, b"ordered-");
    let (_second_file, second) = region_with_prefix(7, b"regions");
    let body = FileRegionSequence::try_new(vec![first, second]).expect("valid region sequence");

    owner.block_on(async move {
        let (client, accepted) = tcp_pair().await;
        let mut sender = Connection::new(client).with_file_region_io(blocking, FileTransferMode::Portable);
        let mut receiver = Connection::new(accepted);
        let (sent, received) = tokio::join!(
            sender.send_file_regions_command(
                RemotingCommand::create_remoting_command(328),
                body,
                RequestDeadline::after(Duration::from_secs(5)),
            ),
            receiver.receive_command()
        );

        sent.expect("portable region sequence should send");
        let received = received.expect("peer frame").expect("decoded peer frame");
        assert_eq!(received.code(), 328);
        assert_eq!(received.body().expect("sequence body").as_ref(), b"ordered-regions");
    });
}

#[test]
fn expired_deadline_writes_no_frame_bytes() {
    let owner = runtime_owner();
    let blocking = owner
        .root_context()
        .component("file-region-deadline")
        .storage_io()
        .clone();
    let (_file, region) = region_with_prefix(0, b"deadline-body");

    owner.block_on(async move {
        let (client, mut accepted) = tcp_pair().await;
        let mut sender = Connection::new(client).with_file_region_io(blocking, FileTransferMode::Portable);
        let error = sender
            .send_file_region_command(
                RemotingCommand::create_remoting_command(322),
                region,
                RequestDeadline::after(Duration::ZERO),
            )
            .await
            .expect_err("expired deadline must fail before the socket write");
        assert!(error.to_string().contains("deadline"));

        let mut byte = [0_u8; 1];
        assert!(
            tokio::time::timeout(Duration::from_millis(50), accepted.read_exact(&mut byte))
                .await
                .is_err()
        );
        assert_eq!(sender.state(), ConnectionState::Healthy);
    });
}

#[test]
fn tls_compatible_stream_forces_portable_path() {
    let owner = runtime_owner();
    let blocking = owner.root_context().component("file-region-tls").storage_io().clone();
    let body = vec![0xA5; 96 * 1024];
    let body_len = body.len() as u64;
    let (_file, region) = region_with_prefix(31, &body);
    let before = file_transfer_snapshot();

    owner.block_on(async move {
        let (client, server) = tokio::io::duplex(256 * 1024);
        let mut sender = Connection::new_with_tls_stream(client).with_file_region_io(blocking, FileTransferMode::Auto);
        let mut receiver = Connection::new_with_plaintext_stream(server);
        let (sent, received) = tokio::join!(
            sender.send_file_region_command(
                RemotingCommand::create_remoting_command(323),
                region,
                RequestDeadline::after(Duration::from_secs(5)),
            ),
            receiver.receive_command()
        );

        sent.expect("TLS-compatible stream should use portable reads");
        let received = received.expect("peer frame").expect("decoded peer frame");
        assert_eq!(received.body().expect("file body").as_ref(), body.as_slice());
    });

    let after = file_transfer_snapshot();
    assert!(after.portable_bytes.saturating_sub(before.portable_bytes) >= body_len);
    assert!(after.fallback_unsupported > before.fallback_unsupported);
}

#[test]
fn explicit_sendfile_on_tls_fails_before_writing_the_head() {
    let owner = runtime_owner();
    let blocking = owner
        .root_context()
        .component("file-region-tls-reject")
        .storage_io()
        .clone();
    let (_file, region) = region_with_prefix(0, b"must-not-bypass-tls");
    let before = file_transfer_snapshot();

    owner.block_on(async move {
        let (client, server) = tokio::io::duplex(1024);
        let mut sender =
            Connection::new_with_tls_stream(client).with_file_region_io(blocking, FileTransferMode::Sendfile);
        let error = sender
            .send_file_region_command(
                RemotingCommand::create_remoting_command(324),
                region,
                RequestDeadline::after(Duration::from_secs(1)),
            )
            .await
            .expect_err("sendfile must never bypass a TLS stream");
        let RocketMQError::Shared(source) = error else {
            panic!("sendfile preflight must use the canonical Shared carrier")
        };
        assert_eq!(source.code(), rocketmq_error::TRANSPORT_CONNECTION_FAILED.code());
        assert!(std::error::Error::source(source.as_ref()).is_some());
        assert_eq!(sender.state(), ConnectionState::Healthy);

        let mut receiver = Connection::new_with_plaintext_stream(server);
        let (sent, received) = tokio::join!(
            sender.send_command(RemotingCommand::create_remoting_command(325)),
            receiver.receive_command()
        );
        sent.expect("preflight sendfile rejection must leave the writer usable");
        assert_eq!(
            received
                .expect("peer response")
                .expect("decoded follow-up command")
                .code(),
            325
        );
    });

    assert!(file_transfer_snapshot().selection_failures > before.selection_failures);
}

#[test]
fn truncated_region_poison_closes_the_connection_after_partial_frame() {
    let owner = runtime_owner();
    let blocking = owner
        .root_context()
        .component("file-region-truncate")
        .storage_io()
        .clone();
    let body = vec![0x42; 160 * 1024];
    let (file, region) = region_with_prefix(0, &body);
    file.set_len(80 * 1024).expect("simulate a violated storage lease");

    owner.block_on(async move {
        let (client, mut accepted) = tcp_pair().await;
        let mut sender = Connection::new(client).with_file_region_io(blocking, FileTransferMode::Portable);
        let mut wire = Vec::new();
        let (sent, read) = tokio::join!(
            sender.send_file_region_command(
                RemotingCommand::create_remoting_command(325),
                region,
                RequestDeadline::after(Duration::from_secs(5)),
            ),
            accepted.read_to_end(&mut wire)
        );

        assert!(sent.is_err());
        read.expect("peer should observe orderly write shutdown");
        assert_eq!(sender.state(), ConnectionState::Closed);
        assert!(
            wire.len() > 80 * 1024,
            "frame head and the available body prefix should be visible"
        );
        assert!(
            wire.len() < body.len() + 1024,
            "the declared missing suffix must not be fabricated"
        );
    });
}

#[test]
fn cancellation_after_head_poison_closes_the_connection() {
    let owner = runtime_owner();
    let blocking = owner
        .root_context()
        .component("file-region-cancel")
        .storage_io()
        .clone();
    let body = vec![0x19; 4 * 1024 * 1024];
    let (_file, region) = region_with_prefix(0, &body);

    owner.block_on(async move {
        let (client, mut accepted) = tcp_pair().await;
        socket2::SockRef::from(&client)
            .set_send_buffer_size(4096)
            .expect("reduce send buffer");
        let mut sender = Connection::new(client).with_file_region_io(blocking, FileTransferMode::Portable);
        let error = sender
            .send_file_region_command(
                RemotingCommand::create_remoting_command(326),
                region,
                RequestDeadline::after(Duration::from_millis(20)),
            )
            .await
            .expect_err("blocked file transfer should reach its immutable deadline");
        assert!(error.to_string().contains("timeout"));
        assert_eq!(sender.state(), ConnectionState::Closed);

        let mut wire = Vec::new();
        accepted.read_to_end(&mut wire).await.expect("read partial wire frame");
        assert!(
            !wire.is_empty(),
            "frame head should have been emitted before cancellation"
        );
        assert!(
            wire.len() < body.len(),
            "cancelled transfer must not report full progress"
        );

        let retry = sender
            .send_command(RemotingCommand::create_remoting_command(327))
            .await
            .expect_err("a partial frame connection must never be reused");
        assert!(retry.to_string().contains("closed"));
    });
}
