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

use rocketmq_transport::api::v1::ConnectTarget;
use rocketmq_transport::api::v1::FrameLimits;
use rocketmq_transport::api::v1::RequestDeadline;
use rocketmq_transport::api::v1::SocketOptions;
use rocketmq_transport::api::v1::TlsConfig;
use rocketmq_transport::api::v1::TransportTelemetry;
use rocketmq_transport::test_support::connect_target_with_config_options_and_telemetry;
use rocketmq_transport::test_support::connect_with_config_and_telemetry;

#[tokio::test]
async fn resolved_target_dials_the_socket_without_losing_logical_authority() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let socket_addr = listener.local_addr().unwrap();
    let target = ConnectTarget::new(socket_addr, "namesrv.default.svc.cluster.local:9876").unwrap();
    let accepted = tokio::spawn(async move { listener.accept().await.unwrap().1 });

    let connected = connect_target_with_config_options_and_telemetry(
        &target,
        &TlsConfig::default(),
        FrameLimits::legacy_compatibility(),
        SocketOptions::default(),
        RequestDeadline::after(Duration::from_secs(1)),
        TransportTelemetry::noop(),
    )
    .await
    .unwrap();

    assert_eq!(connected.remote_addr(), socket_addr);
    assert_eq!(target.authority(), "namesrv.default.svc.cluster.local:9876");
    assert_eq!(target.tls_server_name(), "namesrv.default.svc.cluster.local");
    assert_eq!(accepted.await.unwrap().ip(), connected.local_addr().ip());
}

#[tokio::test]
async fn legacy_address_wrapper_retains_the_existing_dialing_contract() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let socket_addr = listener.local_addr().unwrap();
    let accepted = tokio::spawn(async move { listener.accept().await.unwrap().1 });

    let connected = connect_with_config_and_telemetry(
        &socket_addr.to_string(),
        &TlsConfig::default(),
        FrameLimits::legacy_compatibility(),
        RequestDeadline::after(Duration::from_secs(1)),
        TransportTelemetry::noop(),
    )
    .await
    .unwrap();

    assert_eq!(connected.remote_addr(), socket_addr);
    assert_eq!(accepted.await.unwrap().ip(), connected.local_addr().ip());
}

#[test]
fn one_socket_with_two_authorities_has_two_connection_identities() {
    let socket_addr = "10.0.0.7:9876".parse().unwrap();
    let first = ConnectTarget::new(socket_addr, "namesrv-a.default.svc:9876").unwrap();
    let second = ConnectTarget::new(socket_addr, "namesrv-b.default.svc:9876").unwrap();

    assert_ne!(first.identity(), second.identity());
    assert_eq!(first.socket_addr(), second.socket_addr());
    assert_ne!(first.tls_server_name(), second.tls_server_name());
}
