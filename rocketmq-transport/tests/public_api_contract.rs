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

use std::time::Duration;

use rocketmq_transport::api::AdmissionLimits;
use rocketmq_transport::api::CachedConnectionState;
use rocketmq_transport::api::DefaultRequestProcessor;
use rocketmq_transport::api::FileTransferMode;
use rocketmq_transport::api::FrameLimits;
use rocketmq_transport::api::OneShotTransportClient;
use rocketmq_transport::api::RemotingClient;
use rocketmq_transport::api::RemotingDeserializable;
use rocketmq_transport::api::RemotingSerializable;
use rocketmq_transport::api::RequestDeadline;
use rocketmq_transport::api::RequestProcessor;
use rocketmq_transport::api::ServerConfig;
use rocketmq_transport::api::TransportClient;
use rocketmq_transport::api::TransportClientConfig;
use rocketmq_transport::api::TransportError;
use rocketmq_transport::api::TransportServer;

fn assert_serialization_contract<T: RemotingSerializable + RemotingDeserializable>() {}

fn assert_processor_contract<T: RequestProcessor>() {}

fn assert_transport_error_contract<T: Clone + std::fmt::Debug + std::error::Error>() {}

#[test]
fn lib_rs_keeps_the_curated_public_boundary() {
    let public_modules = include_str!("../src/lib.rs")
        .lines()
        .filter_map(|line| line.strip_prefix("pub mod "))
        .filter_map(|line| line.split([' ', ';', '{']).next())
        .collect::<Vec<_>>();

    assert_eq!(public_modules, ["benchmark_support", "prelude", "test_support", "api"]);
}

#[test]
fn api_reexports_capabilities_and_dtos() {
    let _ = AdmissionLimits::default();
    let _ = FrameLimits::default();
    let _ = ServerConfig::default();
    let _ = ServerConfig {
        listen_port: 10911,
        bind_address: "127.0.0.1".to_owned(),
        tls_config: Default::default(),
        file_transfer_mode: FileTransferMode::Auto,
    };
    let _ = TransportClientConfig {
        connect: Default::default(),
        maintenance: Default::default(),
        tls: Default::default(),
        #[cfg(feature = "socks")]
        socks_proxy: Default::default(),
    };
    let _: Option<OneShotTransportClient> = None;
    let _: Option<TransportClient<DefaultRequestProcessor>> = None;
    let _: Option<RemotingClient<DefaultRequestProcessor>> = None;
    let _: Option<TransportServer<DefaultRequestProcessor>> = None;
    assert_transport_error_contract::<TransportError>();
    let _: CachedConnectionState = CachedConnectionState::Absent;
    let _ = CachedConnectionState::Healthy;
    let _ = CachedConnectionState::UnhealthyRetired;
    let _ = RequestDeadline::after(Duration::from_millis(1));
    assert_serialization_contract::<String>();
    assert_processor_contract::<DefaultRequestProcessor>();
}
