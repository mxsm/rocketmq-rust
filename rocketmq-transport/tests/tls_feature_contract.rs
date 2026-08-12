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

use rocketmq_transport::api::v1::TlsConfig;
use rocketmq_transport::api::v1::TlsMode;
use rocketmq_transport::api::v1::TlsServerRuntime;

#[test]
fn resolved_nameserver_preserves_fqdn_for_tls_server_identity() {
    let target = rocketmq_transport::api::v1::ConnectTarget::new(
        "10.0.0.7:9876".parse().unwrap(),
        "namesrv.default.svc.cluster.local:9876",
    )
    .unwrap();

    assert_eq!(target.tls_server_name(), "namesrv.default.svc.cluster.local");
    assert_ne!(target.tls_server_name(), target.socket_addr().ip().to_string());
}

#[tokio::test]
async fn tls_runtime_preserves_default_permissive_mode() {
    let context = rocketmq_runtime::RuntimeContext::from_current("tls-feature-contract");
    let service = context.service_context("tls");
    let runtime = TlsServerRuntime::initialize_with_service_context(TlsConfig::default(), &service)
        .await
        .expect("TLS runtime should initialize");
    assert_eq!(runtime.mode(), TlsMode::Permissive);
    runtime.shutdown();
}

#[cfg(not(feature = "tls"))]
#[test]
fn tls_off_reports_a_typed_disabled_error() {
    let error = rocketmq_transport::test_support::tls_disabled_error();
    assert!(error.to_string().contains("compiled without the tls feature"));
}
