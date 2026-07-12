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

use rocketmq_transport::admission::AdmissionController;
use rocketmq_transport::admission::AdmissionLimits;
use rocketmq_transport::config::ServerConfig;
use rocketmq_transport::config::TlsClientAuth;
use rocketmq_transport::config::TlsClientConfig;
use rocketmq_transport::config::TlsConfig;
use rocketmq_transport::config::TlsMode;
use rocketmq_transport::config::TlsServerConfig;

#[test]
fn transport_defaults_preserve_tls_and_bounded_admission_contract() {
    let server = ServerConfig::default();
    assert_eq!(server.listen_port, 10_911);
    assert_eq!(server.tls_config.server.mode, TlsMode::Permissive);

    let limits = AdmissionLimits::default();
    assert!(limits.connections.count > 0);
    assert!(limits.connections.bytes > 0);
    assert!(limits.handshakes.count > 0);
    assert!(limits.inflight.count > 0);
    assert!(limits.inflight.bytes > 0);
    assert!(limits.queued.count > 0);
    assert!(limits.queued.bytes > 0);
    assert!(limits.processors.count > 0);

    let controller = AdmissionController::new(limits);
    let snapshot = controller.snapshot();
    assert_eq!(snapshot.connections.current_count, 0);
    assert_eq!(snapshot.inflight.current_bytes, 0);
}

#[test]
fn tls_config_preserves_legacy_serde_defaults_and_redaction() {
    let config: TlsConfig = serde_json::from_str(
        r#"{
            "enable": true,
            "testModeEnable": true,
            "server": {
                "mode": "enforcing",
                "needClientAuth": "require",
                "keyPassword": "server-secret"
            },
            "client": {
                "authServer": true,
                "keyPassword": "client-secret"
            }
        }"#,
    )
    .expect("legacy camelCase TLS envelope should deserialize");

    assert!(config.enable);
    assert!(config.test_mode_enable);
    assert_eq!(config.server.mode, TlsMode::Enforcing);
    assert_eq!(config.server.need_client_auth, TlsClientAuth::Require);
    assert!(config.client.auth_server);
    assert!(!format!("{config:?}").contains("server-secret"));
    assert!(!format!("{config:?}").contains("client-secret"));

    let default = TlsConfig::default();
    assert_eq!(default.config_file, "/etc/rocketmq/tls.properties");
    assert_eq!(default.server, TlsServerConfig::default());
    assert_eq!(default.client, TlsClientConfig::default());
    assert!(default.client.auth_server);
}
