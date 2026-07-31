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
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use rocketmq_sre_model_gateway::ExternalSecretClient;
use rocketmq_sre_model_gateway::ExternalSecretManagerProvider;
use rocketmq_sre_model_gateway::ExternalSecretValue;
use rocketmq_sre_model_gateway::ProviderError;
use rocketmq_sre_model_gateway::ProviderErrorCode;
use rocketmq_sre_model_gateway::SecretProvider;
use rocketmq_sre_model_gateway::SecretReference;
use rocketmq_sre_model_gateway::VaultAgentFileSecretClient;

struct FixtureRoot {
    path: PathBuf,
}

impl FixtureRoot {
    fn new(name: &str) -> Self {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "rocketmq-sre-vault-agent-{name}-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("create fixture root");
        Self { path }
    }

    fn write(&self, relative: &str, value: &[u8]) {
        let path = self.path.join(relative);
        fs::create_dir_all(path.parent().expect("fixture file has parent")).expect("create fixture directory");
        fs::write(path, value).expect("write fixture file");
    }
}

impl Drop for FixtureRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

#[test]
fn sidecar_watch_refresh_rotates_a_cached_secret_without_leaking_values() {
    let root = FixtureRoot::new("rotation");
    root.write("team-a/models/deepseek", b"first-secret\n");
    root.write("team-a/models/deepseek.version", b"v1\n");
    let client = Arc::new(
        VaultAgentFileSecretClient::new(&root.path)
            .expect("Vault Agent client")
            .with_required_version_sidecar(".version")
            .expect("valid sidecar suffix"),
    );
    let provider = ExternalSecretManagerProvider::new(client, "team-a/models", Duration::from_secs(3600));
    let reference = SecretReference::external("team-a/models/deepseek").expect("external reference");

    let first = provider.resolve(&reference).expect("first secret");
    assert_eq!(first.expose_to_transport(), "first-secret");
    assert_eq!(first.version_fingerprint(), "version:vault-agent:sidecar:v1");

    root.write("team-a/models/deepseek", b"second-secret\n");
    root.write("team-a/models/deepseek.version", b"v2\n");
    assert_eq!(
        provider
            .resolve(&reference)
            .expect("cached secret")
            .expose_to_transport(),
        "first-secret"
    );

    assert_eq!(
        provider.on_watch_event(&reference).expect("watch refresh"),
        "version:vault-agent:sidecar:v2"
    );
    let rotated = provider.resolve(&reference).expect("rotated secret");
    assert_eq!(rotated.expose_to_transport(), "second-secret");
    assert!(!format!("{rotated:?}").contains("second-secret"));
}

#[test]
fn locator_and_namespace_boundaries_fail_closed_with_redacted_errors() {
    let root = FixtureRoot::new("authorization");
    root.write("team-a/models/openai", b"do-not-leak");
    let client = Arc::new(VaultAgentFileSecretClient::new(&root.path).expect("Vault Agent client"));
    let provider = ExternalSecretManagerProvider::new(client.clone(), "team-a/models", Duration::from_secs(60));
    let foreign = SecretReference::external("team-a/models-foreign/openai").expect("foreign reference");

    let namespace_error = provider.resolve(&foreign).expect_err("foreign namespace");
    assert_eq!(namespace_error.code, ProviderErrorCode::SecretAccessDenied);
    assert!(!format!("{namespace_error:?}").contains("models-foreign"));

    for invalid in [
        "../outside",
        "team-a/../outside",
        "/absolute/path",
        r"team-a\models\openai",
        "team-a//models/openai",
    ] {
        let error = expect_read_error(client.read_secret(invalid), "unsafe locator");
        assert_eq!(error.code, ProviderErrorCode::SecretAccessDenied);
        let rendered = format!("{error:?}");
        assert!(!rendered.contains(invalid));
        assert!(!rendered.contains("do-not-leak"));
    }

    let client_debug = format!("{client:?}");
    assert!(!client_debug.contains(&root.path.display().to_string()));
    assert!(!client_debug.contains("do-not-leak"));
}

#[test]
fn directories_and_oversized_files_are_rejected_before_secret_material_is_returned() {
    let root = FixtureRoot::new("bounds");
    root.write("team-a/models/oversized", b"123456789");
    fs::create_dir_all(root.path.join("team-a/models/directory")).expect("create directory fixture");
    let client = VaultAgentFileSecretClient::new(&root.path)
        .expect("Vault Agent client")
        .with_max_secret_bytes(8)
        .expect("bounded client");

    let oversized = expect_read_error(client.read_secret("team-a/models/oversized"), "oversized secret");
    assert_eq!(oversized.code, ProviderErrorCode::OutputTooLarge);
    assert!(!format!("{oversized:?}").contains("123456789"));

    let directory = expect_read_error(
        client.read_secret("team-a/models/directory"),
        "directory is not a secret",
    );
    assert_eq!(directory.code, ProviderErrorCode::SecretAccessDenied);
}

#[test]
fn metadata_fingerprint_never_depends_on_or_exposes_secret_content() {
    let root = FixtureRoot::new("metadata");
    root.write("team-a/models/kimi", b"metadata-secret");
    let client = VaultAgentFileSecretClient::new(&root.path).expect("Vault Agent client");

    let value = client.read_secret("team-a/models/kimi").expect("rendered secret");

    assert!(value.version.starts_with("vault-agent:metadata:"));
    assert!(!value.version.contains("metadata-secret"));
}

#[test]
fn final_and_intermediate_symbolic_links_are_rejected() {
    let root = FixtureRoot::new("symlink");
    root.write("real/final-secret", b"final-link-secret");
    fs::create_dir_all(root.path.join("real/intermediate")).expect("create intermediate target");
    fs::write(root.path.join("real/intermediate/secret"), b"intermediate-link-secret")
        .expect("write intermediate target");
    fs::create_dir_all(root.path.join("team-a/models")).expect("create final link parent");

    if let Err(error) = create_file_symlink(
        &root.path.join("real/final-secret"),
        &root.path.join("team-a/models/final-link"),
    ) {
        if symlink_creation_is_unavailable(&error) {
            return;
        }
        panic!("create file symlink: {error}");
    }
    if let Err(error) = create_dir_symlink(
        &root.path.join("real/intermediate"),
        &root.path.join("intermediate-link"),
    ) {
        if symlink_creation_is_unavailable(&error) {
            return;
        }
        panic!("create directory symlink: {error}");
    }

    let client = VaultAgentFileSecretClient::new(&root.path).expect("Vault Agent client");
    let final_error = expect_read_error(client.read_secret("team-a/models/final-link"), "final symlink");
    assert_eq!(final_error.code, ProviderErrorCode::SecretAccessDenied);
    let intermediate_error = expect_read_error(client.read_secret("intermediate-link/secret"), "intermediate symlink");
    assert_eq!(intermediate_error.code, ProviderErrorCode::SecretAccessDenied);
}

#[cfg(unix)]
fn create_file_symlink(target: &Path, link: &Path) -> io::Result<()> {
    std::os::unix::fs::symlink(target, link)
}

#[cfg(windows)]
fn create_file_symlink(target: &Path, link: &Path) -> io::Result<()> {
    std::os::windows::fs::symlink_file(target, link)
}

#[cfg(unix)]
fn create_dir_symlink(target: &Path, link: &Path) -> io::Result<()> {
    std::os::unix::fs::symlink(target, link)
}

#[cfg(windows)]
fn create_dir_symlink(target: &Path, link: &Path) -> io::Result<()> {
    std::os::windows::fs::symlink_dir(target, link)
}

fn expect_read_error(result: Result<ExternalSecretValue, ProviderError>, context: &str) -> ProviderError {
    match result {
        Ok(_) => panic!("{context} unexpectedly returned secret material"),
        Err(error) => error,
    }
}

fn symlink_creation_is_unavailable(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::PermissionDenied || cfg!(windows) && error.raw_os_error() == Some(1314)
}
