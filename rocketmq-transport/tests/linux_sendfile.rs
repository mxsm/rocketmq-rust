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

#![cfg(all(target_os = "linux", feature = "linux-sendfile", feature = "test-support"))]

use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_protocol::protocol::RemotingCommand;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_transport::api::file_transfer_snapshot;
use rocketmq_transport::api::ConnectionState;
use rocketmq_transport::api::FileRegion;
use rocketmq_transport::api::FileTransferMode;
use rocketmq_transport::api::RequestDeadline;
use rocketmq_transport::test_support::Connection;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;

fn runtime_owner() -> RuntimeOwner {
    RuntimeOwner::new().expect("test runtime owner")
}

fn file_region(prefix_len: usize, body: &[u8]) -> (Arc<std::fs::File>, FileRegion) {
    let mut file = tempfile::tempfile().expect("temporary file");
    file.write_all(&vec![0x31; prefix_len]).expect("write prefix");
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
fn sendfile_round_trips_under_socket_backpressure() {
    let owner = runtime_owner();
    let blocking = owner
        .root_context()
        .component("linux-sendfile-test")
        .storage_io()
        .clone();
    let body: Vec<u8> = (0..4 * 1024 * 1024).map(|index| (index % 239) as u8).collect();
    let body_len = body.len() as u64;
    let (_file, region) = file_region(4093, &body);
    let before = file_transfer_snapshot();

    owner.block_on(async move {
        let (client, accepted) = tcp_pair().await;
        socket2::SockRef::from(&client)
            .set_send_buffer_size(4096)
            .expect("reduce send buffer to exercise partial sendfile calls");
        let mut sender = Connection::new(client).with_file_region_io(blocking, FileTransferMode::Auto);
        let mut receiver = Connection::new(accepted);
        let send = sender.send_file_region_command(
            RemotingCommand::create_remoting_command(401),
            region,
            RequestDeadline::after(Duration::from_secs(15)),
        );
        let receive = async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            receiver.receive_command().await
        };
        let (sent, received) = tokio::join!(send, receive);

        sent.expect("native sendfile frame should send");
        let received = received.expect("peer frame").expect("decoded peer frame");
        assert_eq!(received.code(), 401);
        assert_eq!(received.body().expect("file body").as_ref(), body.as_slice());
    });

    let after = file_transfer_snapshot();
    assert!(after.sendfile_bytes.saturating_sub(before.sendfile_bytes) >= body_len);
    assert_eq!(after.portable_bytes, before.portable_bytes);
}

#[test]
fn auto_keeps_small_file_regions_on_the_portable_path() {
    let owner = runtime_owner();
    let blocking = owner
        .root_context()
        .component("linux-sendfile-small")
        .storage_io()
        .clone();
    let body = vec![0x2A; 4 * 1024];
    let (_file, region) = file_region(17, &body);
    let before = file_transfer_snapshot();

    owner.block_on(async move {
        let (client, accepted) = tcp_pair().await;
        let mut sender = Connection::new(client).with_file_region_io(blocking, FileTransferMode::Auto);
        let mut receiver = Connection::new(accepted);
        let (sent, received) = tokio::join!(
            sender.send_file_region_command(
                RemotingCommand::create_remoting_command(403),
                region,
                RequestDeadline::after(Duration::from_secs(5)),
            ),
            receiver.receive_command()
        );

        sent.expect("small Auto transfer should use portable I/O");
        let received = received.expect("peer frame").expect("decoded peer frame");
        assert_eq!(received.body().expect("file body").as_ref(), body.as_slice());
    });

    let after = file_transfer_snapshot();
    assert!(after.portable_bytes.saturating_sub(before.portable_bytes) >= 4 * 1024);
    assert_eq!(after.sendfile_bytes, before.sendfile_bytes);
}

#[test]
fn sendfile_eof_poison_closes_the_connection() {
    let owner = runtime_owner();
    let blocking = owner
        .root_context()
        .component("linux-sendfile-eof")
        .storage_io()
        .clone();
    let body = vec![0x77; 192 * 1024];
    let (file, region) = file_region(0, &body);
    file.set_len(96 * 1024).expect("simulate a violated storage lease");

    owner.block_on(async move {
        let (client, mut accepted) = tcp_pair().await;
        let mut sender = Connection::new(client).with_file_region_io(blocking, FileTransferMode::Sendfile);
        let mut wire = Vec::new();
        let (sent, read) = tokio::join!(
            sender.send_file_region_command(
                RemotingCommand::create_remoting_command(402),
                region,
                RequestDeadline::after(Duration::from_secs(5)),
            ),
            accepted.read_to_end(&mut wire)
        );

        assert!(sent.is_err());
        read.expect("peer should observe write shutdown");
        assert_eq!(sender.state(), ConnectionState::Closed);
        assert!(wire.len() > 96 * 1024);
        assert!(wire.len() < body.len() + 1024);
    });
}

#[test]
fn sendfile_cancellation_after_head_closes_the_connection() {
    let owner = runtime_owner();
    let blocking = owner
        .root_context()
        .component("linux-sendfile-cancel")
        .storage_io()
        .clone();
    let body = vec![0x6B; 4 * 1024 * 1024];
    let (_file, region) = file_region(0, &body);

    owner.block_on(async move {
        let (client, mut accepted) = tcp_pair().await;
        socket2::SockRef::from(&client)
            .set_send_buffer_size(4096)
            .expect("reduce send buffer");
        let mut sender = Connection::new(client).with_file_region_io(blocking, FileTransferMode::Sendfile);
        let error = sender
            .send_file_region_command(
                RemotingCommand::create_remoting_command(404),
                region,
                RequestDeadline::after(Duration::from_millis(20)),
            )
            .await
            .expect_err("blocked sendfile should reach its deadline");
        assert!(error.to_string().contains("timeout"));
        assert_eq!(sender.state(), ConnectionState::Closed);

        let mut wire = Vec::new();
        accepted
            .read_to_end(&mut wire)
            .await
            .expect("read partial native frame");
        assert!(!wire.is_empty());
        assert!(wire.len() < body.len());
    });
}
