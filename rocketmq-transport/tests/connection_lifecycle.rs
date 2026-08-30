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

#![cfg(feature = "test-support")]

use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use bytes::BytesMut;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_transport::api::ConnectionState;
use rocketmq_transport::api::FileRegion;
use rocketmq_transport::api::FrameLimits;
use rocketmq_transport::api::RequestDeadline;
use rocketmq_transport::test_support::Connection;
use tokio::io::AsyncReadExt;
use tokio_util::codec::Encoder;

#[tokio::test]
async fn loopback_connection_preserves_wire_identity_and_half_close_state() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (client, (server, _)) = tokio::try_join!(tokio::net::TcpStream::connect(address), listener.accept()).unwrap();
    let mut client = Connection::new(client);
    let mut server = Connection::new(server);

    client
        .send_command(RemotingCommand::create_remoting_command(105).set_opaque(77))
        .await
        .unwrap();
    let request = server.receive_command().await.unwrap().unwrap();
    assert_eq!(request.code(), 105);
    assert_eq!(request.opaque(), 77);

    client.shutdown().await.unwrap();
    assert_eq!(client.state(), ConnectionState::Closed);
    assert!(server.receive_command().await.is_none());
}

#[tokio::test]
async fn owner_injected_legacy_limits_accept_a_large_fragmented_frame() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut connection = Connection::new_with_limits(socket, FrameLimits::legacy_compatibility());
        connection
            .send_command(RemotingCommand::create_remoting_command(105).set_body(vec![3_u8; 5 * 1024 * 1024]))
            .await
            .unwrap();
        connection.shutdown().await.unwrap();
    });
    let socket = tokio::net::TcpStream::connect(address).await.unwrap();
    let mut connection = Connection::new_with_limits(socket, FrameLimits::legacy_compatibility());

    let command = connection
        .receive_command()
        .await
        .expect("large frame")
        .expect("valid legacy frame");
    assert_eq!(command.body().unwrap().len(), 5 * 1024 * 1024);
    server.await.unwrap();
}

#[tokio::test]
async fn outbound_connection_rejects_frames_that_exceed_its_inbound_limits() {
    let limits = FrameLimits {
        max_frame_bytes: 1024,
        max_header_bytes: 512,
        max_body_bytes: 16,
        initial_read_bytes: 8,
    };
    let (stream, mut peer) = tokio::io::duplex(4096);
    let mut connection = Connection::new_with_plaintext_stream_and_limits(stream, limits);

    let result = connection
        .send_command(RemotingCommand::create_remoting_command(105).set_body(vec![0_u8; 17]))
        .await;

    assert!(result.is_err());
    let mut byte = [0_u8; 1];
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), peer.read(&mut byte))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn raw_and_segmented_output_obey_the_owner_frame_limits_atomically() {
    let limits = FrameLimits {
        max_frame_bytes: 256,
        max_header_bytes: 192,
        max_body_bytes: 32,
        initial_read_bytes: 8,
    };
    let (stream, mut peer) = tokio::io::duplex(1024);
    let mut connection = Connection::new_with_plaintext_stream_and_limits(stream, limits);

    assert!(connection.send_bytes(Bytes::from(vec![0_u8; 129])).await.is_err());
    let mut wire = BytesMut::new();
    rocketmq_transport::test_support::RemotingCommandCodec::with_limits(limits)
        .encode(
            RemotingCommand::create_remoting_command(105).set_body(vec![7_u8; 33]),
            &mut wire,
        )
        .expect_err("fixture is over the body limit");

    let unrestricted = FrameLimits {
        max_frame_bytes: 1024,
        max_header_bytes: 512,
        max_body_bytes: 512,
        initial_read_bytes: 8,
    };
    rocketmq_transport::test_support::RemotingCommandCodec::with_limits(unrestricted)
        .encode(
            RemotingCommand::create_remoting_command(105).set_body(vec![7_u8; 33]),
            &mut wire,
        )
        .unwrap();
    let body_offset = 8 + (u32::from_be_bytes(wire[4..8].try_into().unwrap()) & 0x00ff_ffff) as usize;
    let body = wire.split_off(body_offset).freeze();
    let head = wire.freeze();
    assert!(connection.send_frame_segments(vec![head, body]).await.is_err());

    let mut byte = [0_u8; 1];
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), peer.read(&mut byte))
            .await
            .is_err()
    );

    let mut exact_wire = BytesMut::new();
    rocketmq_transport::test_support::RemotingCommandCodec::with_limits(unrestricted)
        .encode(
            RemotingCommand::create_remoting_command(105).set_body(vec![7_u8; 32]),
            &mut exact_wire,
        )
        .unwrap();
    let expected_len = exact_wire.len();
    let body_offset = 8 + (u32::from_be_bytes(exact_wire[4..8].try_into().unwrap()) & 0x00ff_ffff) as usize;
    let body = exact_wire.split_off(body_offset).freeze();
    connection
        .send_frame_segments(vec![exact_wire.freeze(), body])
        .await
        .unwrap();
    let mut received = vec![0_u8; expected_len];
    peer.read_exact(&mut received).await.unwrap();
}

#[tokio::test]
async fn direct_tls_segmented_output_obeys_the_owner_frame_limits() {
    let limits = FrameLimits {
        max_frame_bytes: 256,
        max_header_bytes: 192,
        max_body_bytes: 32,
        initial_read_bytes: 8,
    };
    let unrestricted = FrameLimits {
        max_frame_bytes: 1024,
        max_header_bytes: 512,
        max_body_bytes: 512,
        initial_read_bytes: 8,
    };
    let (stream, mut peer) = tokio::io::duplex(1024);
    let mut connection = Connection::new_with_tls_stream_and_limits(stream, limits);
    let mut wire = BytesMut::new();
    rocketmq_transport::test_support::RemotingCommandCodec::with_limits(unrestricted)
        .encode(
            RemotingCommand::create_remoting_command(105).set_body(vec![7_u8; 33]),
            &mut wire,
        )
        .unwrap();
    let body_offset = 8 + (u32::from_be_bytes(wire[4..8].try_into().unwrap()) & 0x00ff_ffff) as usize;
    let body = wire.split_off(body_offset).freeze();
    assert!(connection.send_frame_segments(vec![wire.freeze(), body]).await.is_err());

    let mut byte = [0_u8; 1];
    assert!(tokio::time::timeout(Duration::from_millis(20), peer.read(&mut byte))
        .await
        .is_err());
}

#[tokio::test]
async fn batch_and_file_region_output_reject_over_limit_bodies_before_writing() {
    let limits = FrameLimits {
        max_frame_bytes: 256,
        max_header_bytes: 192,
        max_body_bytes: 32,
        initial_read_bytes: 8,
    };
    let (stream, mut peer) = tokio::io::duplex(1024);
    let mut connection = Connection::new_with_plaintext_stream_and_limits(stream, limits);

    assert!(connection
        .send_batch(vec![
            RemotingCommand::create_remoting_command(105),
            RemotingCommand::create_remoting_command(106).set_body(vec![0_u8; 33]),
        ])
        .await
        .is_err());

    let mut file = tempfile::tempfile().unwrap();
    file.write_all(&[7_u8; 33]).unwrap();
    file.flush().unwrap();
    let region = FileRegion::try_new(Arc::new(file), 0, 33).unwrap();
    assert!(connection
        .send_file_region_command(
            RemotingCommand::create_remoting_command(107),
            region,
            RequestDeadline::after(Duration::from_secs(1)),
        )
        .await
        .is_err());

    let mut byte = [0_u8; 1];
    assert!(tokio::time::timeout(Duration::from_millis(20), peer.read(&mut byte))
        .await
        .is_err());
}
