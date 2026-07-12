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

use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_transport::codec::remoting_command_codec::FrameLimits;
use rocketmq_transport::connection::Connection;
use rocketmq_transport::connection::ConnectionState;

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
