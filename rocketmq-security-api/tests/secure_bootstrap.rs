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

use std::fs;
use std::net::SocketAddr;

use rocketmq_security_api::SecurityBootstrapConfig;
use rocketmq_security_api::SecurityBootstrapError;
use rocketmq_security_api::SecurityBootstrapMaterial;
use rocketmq_security_api::SecurityBootstrapProfile;
use rocketmq_security_api::MOUNTED_FILES_SECRET_PROVIDER;
use tempfile::TempDir;

struct SecurityFiles {
    _directory: TempDir,
    trust_anchor: std::path::PathBuf,
    certificate: std::path::PathBuf,
    private_key: std::path::PathBuf,
    admin_identity: std::path::PathBuf,
    request_policy: std::path::PathBuf,
}

impl SecurityFiles {
    fn create() -> Self {
        let directory = TempDir::new().expect("create security test directory");
        let trust_anchor = directory.path().join("ca.crt");
        let certificate = directory.path().join("tls.crt");
        let private_key = directory.path().join("tls.key");
        let admin_identity = directory.path().join("admin.identity");
        let request_policy = directory.path().join("request-policy.json");
        for path in [
            &trust_anchor,
            &certificate,
            &private_key,
            &admin_identity,
            &request_policy,
        ] {
            fs::write(path, b"test material").expect("write security test material");
        }
        Self {
            _directory: directory,
            trust_anchor,
            certificate,
            private_key,
            admin_identity,
            request_policy,
        }
    }

    fn secure_config(&self) -> SecurityBootstrapConfig {
        SecurityBootstrapConfig::new(SecurityBootstrapProfile::SecureEnforced)
            .with_trust_anchor(&self.trust_anchor)
            .with_tls_identity(&self.certificate, &self.private_key)
            .with_secret_provider(MOUNTED_FILES_SECRET_PROVIDER)
            .with_admin_identity(&self.admin_identity)
            .with_request_policy(&self.request_policy)
    }
}

#[test]
fn development_insecure_bootstrap_accepts_only_loopback_listeners() {
    let config = SecurityBootstrapConfig::new(SecurityBootstrapProfile::DevelopmentInsecureLoopback);
    let listeners = [
        SocketAddr::from(([127, 0, 0, 1], 10911)),
        "[::1]:8088".parse().expect("IPv6 loopback address"),
    ];
    let validated = config
        .validate(&listeners)
        .expect("loopback listeners should be accepted");
    assert_eq!(
        validated.profile(),
        SecurityBootstrapProfile::DevelopmentInsecureLoopback
    );
    assert_eq!(validated.listener_count(), 2);

    assert_eq!(
        config
            .validate(&[SocketAddr::from(([0, 0, 0, 0], 10911))])
            .expect_err("wildcard listener must fail closed"),
        SecurityBootstrapError::DevelopmentListenerNotLoopback
    );
}

#[test]
fn secure_bootstrap_requires_every_material_and_supported_provider() {
    let missing = SecurityBootstrapConfig::new(SecurityBootstrapProfile::SecureEnforced)
        .validate(&[SocketAddr::from(([0, 0, 0, 0], 10911))])
        .expect_err("missing secure material must fail closed");
    assert_eq!(
        missing,
        SecurityBootstrapError::MissingMaterial(SecurityBootstrapMaterial::TrustAnchor)
    );

    let files = SecurityFiles::create();
    let unsupported = files
        .secure_config()
        .with_secret_provider("untrusted-provider-value")
        .validate(&[SocketAddr::from(([0, 0, 0, 0], 10911))])
        .expect_err("unknown provider must fail closed");
    assert_eq!(unsupported, SecurityBootstrapError::UnsupportedSecretProvider);
    assert!(!unsupported.to_string().contains("untrusted-provider-value"));

    let validated = files
        .secure_config()
        .validate(&[SocketAddr::from(([0, 0, 0, 0], 10911))])
        .expect("complete secure bootstrap should pass");
    assert_eq!(validated.profile(), SecurityBootstrapProfile::SecureEnforced);
}

#[test]
fn bootstrap_debug_and_errors_do_not_expose_configured_paths() {
    let files = SecurityFiles::create();
    let config = files.secure_config();
    let debug = format!("{config:?}");
    for path in [
        &files.trust_anchor,
        &files.certificate,
        &files.private_key,
        &files.admin_identity,
        &files.request_policy,
    ] {
        assert!(!debug.contains(path.to_string_lossy().as_ref()));
    }
    assert!(!debug.contains(MOUNTED_FILES_SECRET_PROVIDER));

    fs::remove_file(&files.private_key).expect("remove private key fixture");
    let error = config
        .validate(&[])
        .expect_err("unavailable private key must fail closed");
    assert_eq!(
        error,
        SecurityBootstrapError::MaterialUnavailable(SecurityBootstrapMaterial::TlsPrivateKey)
    );
    assert!(!error.to_string().contains("tls.key"));

    fs::write(&files.private_key, []).expect("create empty private key fixture");
    assert_eq!(
        config.validate(&[]).expect_err("empty private key must fail closed"),
        SecurityBootstrapError::MaterialEmpty(SecurityBootstrapMaterial::TlsPrivateKey)
    );
}
