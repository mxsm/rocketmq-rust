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

use std::time::Duration;

use rocketmq_transport::api::v1::FrameLimits;
use rocketmq_transport::api::v1::RequestDeadline;
use rocketmq_transport::api::v1::SocketOptions;
use rocketmq_transport::api::v1::TlsConfig;
use rocketmq_transport::api::v1::TransportTelemetry;
use rocketmq_transport::test_support::connect_with_config_options_and_telemetry;
use tokio::net::TcpListener;

#[tokio::test]
async fn client_socket_policy_is_applied_before_transport_negotiation() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
    let address = listener.local_addr().expect("listener address");
    let accepted = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept client");
        drop(stream);
    });

    let connected = connect_with_config_options_and_telemetry(
        &address.to_string(),
        &TlsConfig::default(),
        FrameLimits::default(),
        SocketOptions {
            tcp_nodelay: true,
            ..SocketOptions::default()
        },
        RequestDeadline::after(Duration::from_secs(1)),
        TransportTelemetry::noop(),
    )
    .await
    .expect("connect with socket options");

    assert!(connected.socket_nodelay());
    accepted.await.expect("accept task");
    drop(connected);
}
