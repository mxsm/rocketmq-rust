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

use rocketmq_transport::api::v1::AdmissionLimits;
use rocketmq_transport::api::v1::FrameLimits;
use rocketmq_transport::api::v1::OneShotTransportClient;
use rocketmq_transport::api::v1::ServerConfig;

#[test]
fn transport_consumers_use_only_versioned_capabilities_and_dtos() {
    let source = include_str!("../src/lib.rs");
    assert!(
        source.lines().all(|line| {
            let line = line.trim_start();
            !line.starts_with("pub use ") || line == "pub use crate::public_api::*;"
        }),
        "crate-root re-exports bypass the versioned `api::v1` compatibility boundary"
    );
    assert!(source.contains("pub mod api {"));
    assert!(source.contains("pub mod v1 {"));
    assert!(source.contains("pub use crate::public_api::*;"));

    for module in [
        "admission",
        "base",
        "buffer",
        "client",
        "clients",
        "codec",
        "common",
        "config",
        "config_support",
        "connection",
        "connection_context",
        "deadline",
        "discovery",
        "error_helpers",
        "error_response",
        "local",
        "net",
        "remoting",
        "remoting_server",
        "request_processor",
        "rpc",
        "runtime",
        "security",
        "server",
        "smart_encode_buffer",
        "tls",
    ] {
        assert!(
            !source.contains(&format!("pub mod {module};")),
            "`rocketmq-transport` implementation module `{module}` must remain private"
        );
    }

    let _ = AdmissionLimits::default();
    let _ = FrameLimits::default();
    let _ = ServerConfig::default();
    let _: Option<OneShotTransportClient> = None;
}
