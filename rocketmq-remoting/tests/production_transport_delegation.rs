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

#[test]
fn production_server_delegates_accept_and_handshake_to_transport() {
    let source = include_str!("../src/remoting_server/rocketmq_tokio_server.rs");
    let production = source.split("#[cfg(test)]").next().expect("production source");

    assert!(production.contains("rocketmq_transport::server::TransportListener"));
    assert!(production.contains("TransportListener::new"));
    assert!(!production.contains("listener.accept().await"));
    assert!(!production.contains("mpsc::unbounded_channel"));
    assert!(!production.contains("receive_command()"));
    assert!(!production.contains("send_command("));
}

#[test]
fn production_client_delegates_socket_and_tls_connect_to_transport() {
    let source = include_str!("../src/clients/client.rs");
    let production = source.split("#[cfg(test)]").next().expect("production source");

    assert!(production.contains("rocketmq_transport::client::connect_with_config"));
    assert!(production.contains("rocketmq_transport::server::run_connected_session"));
    assert!(!production.contains("TcpStream::connect"));
    assert!(!production.contains("connect_tls_stream"));
    assert!(!production.contains("receive_command()"));
}
