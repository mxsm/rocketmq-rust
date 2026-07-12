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
