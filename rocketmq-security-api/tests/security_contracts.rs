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

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::SystemTime;

use cheetah_string::CheetahString;
use rocketmq_security_api::evaluate_request;
use rocketmq_security_api::validate_security_config;
use rocketmq_security_api::Action;
use rocketmq_security_api::AuthenticatedRequestContext;
use rocketmq_security_api::Decision;
use rocketmq_security_api::PeerInfo;
use rocketmq_security_api::RequestContext;
use rocketmq_security_api::RequestPolicy;
use rocketmq_security_api::Resource;
use rocketmq_security_api::Secret;
use rocketmq_security_api::SecurityConfigView;
use rocketmq_security_api::SecurityReadinessFailure;
use rocketmq_security_api::SecurityRequestView;

static TEST_DIRECTORY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new(name: &str) -> Self {
        let sequence = TEST_DIRECTORY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "rocketmq-security-api-{name}-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path).expect("test directory should be created");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).expect("test directory should be removed");
    }
}

#[test]
fn request_view_borrows_payload_and_redacts_sensitive_debug_data() {
    let mut fields = HashMap::new();
    fields.insert(CheetahString::from("token"), CheetahString::from("credential-value"));
    let body = b"secret-body".to_vec();
    let peer = PeerInfo::new("127.0.0.1:10911".parse().expect("valid address"), true);
    let view = SecurityRequestView::new(10, 1, &fields, Some(&body), Some(&peer));

    assert!(std::ptr::eq(view.fields(), &fields));
    assert!(std::ptr::eq(
        view.body().expect("body should be borrowed"),
        body.as_slice()
    ));
    let debug = format!("{view:?}");
    assert!(!debug.contains("credential-value"));
    assert!(!debug.contains("secret-body"));
    assert!(debug.contains("body_len"));
}

struct AllowAuthenticated;

impl RequestPolicy for AllowAuthenticated {
    fn evaluate_authenticated(&self, _context: AuthenticatedRequestContext<'_>) -> Decision {
        Decision::Allow
    }
}

#[test]
fn policy_gate_fails_closed_when_identity_is_missing() {
    let fields = HashMap::new();
    let view = SecurityRequestView::new(10, 1, &fields, None, None);
    let resource = Resource::topic("TopicA");
    let context = RequestContext::new(view, None, resource, Action::Publish);

    assert!(matches!(
        evaluate_request(&AllowAuthenticated, &context),
        Decision::Deny { .. }
    ));
}

#[test]
fn secrets_never_expose_values_through_debug() {
    let secret = Secret::new(String::from("credential-value"));
    assert_eq!(format!("{secret:?}"), "[REDACTED]");
    assert_eq!(secret.expose_secret(), "credential-value");
}

#[test]
fn secure_profile_dry_run_rejects_unknown_missing_expired_and_downgrade_inputs() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
    let unknown = SecurityConfigView::new("mystery");
    assert!(validate_security_config(unknown, now)
        .failures()
        .contains(&SecurityReadinessFailure::UnknownProfile));

    let missing = SecurityConfigView::new("secure");
    let missing_report = validate_security_config(missing, now);
    assert!(missing_report
        .failures()
        .contains(&SecurityReadinessFailure::MissingTrustAnchor));
    assert!(missing_report
        .failures()
        .contains(&SecurityReadinessFailure::MissingSecretFile));
    assert!(missing_report
        .failures()
        .contains(&SecurityReadinessFailure::MissingBootstrapExpiry));

    let expired = SecurityConfigView::new("secure")
        .with_trust_anchor(Path::new("ca.pem"))
        .with_secret_file(Path::new("secret.key"))
        .with_bootstrap_expiry(now - Duration::from_secs(1))
        .with_insecure_downgrade(true);
    let expired_report = validate_security_config(expired, now);
    assert!(expired_report
        .failures()
        .contains(&SecurityReadinessFailure::ExpiredBootstrap));
    assert!(expired_report
        .failures()
        .contains(&SecurityReadinessFailure::InsecureDowngrade));
}

#[test]
fn secure_profile_dry_run_rejects_files_that_cannot_be_opened() {
    let directory = TestDirectory::new("missing-files");
    let trust_anchor = directory.path().join("missing-ca.pem");
    let secret_file = directory.path().join("missing-secret.key");
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
    let view = SecurityConfigView::new("secure")
        .with_trust_anchor(&trust_anchor)
        .with_secret_file(&secret_file)
        .with_bootstrap_expiry(now + Duration::from_secs(1));

    let report = validate_security_config(view, now);

    assert!(report
        .failures()
        .contains(&SecurityReadinessFailure::TrustAnchorUnavailable));
    assert!(report
        .failures()
        .contains(&SecurityReadinessFailure::SecretFileUnavailable));
}

#[test]
fn secure_profile_dry_run_rejects_paths_that_are_not_regular_files() {
    let directory = TestDirectory::new("non-files");
    let trust_anchor = directory.path().join("trust-anchor-directory");
    let secret_file = directory.path().join("secret-directory");
    fs::create_dir(&trust_anchor).expect("trust anchor directory should be created");
    fs::create_dir(&secret_file).expect("secret directory should be created");
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
    let view = SecurityConfigView::new("secure")
        .with_trust_anchor(&trust_anchor)
        .with_secret_file(&secret_file)
        .with_bootstrap_expiry(now + Duration::from_secs(1));

    let report = validate_security_config(view, now);

    assert!(report
        .failures()
        .contains(&SecurityReadinessFailure::TrustAnchorNotRegularFile));
    assert!(report
        .failures()
        .contains(&SecurityReadinessFailure::SecretFileNotRegularFile));
}

#[cfg(unix)]
#[test]
fn secure_profile_dry_run_requires_owner_only_secret_permissions() {
    use std::os::unix::fs::PermissionsExt;

    let directory = TestDirectory::new("unix-permissions");
    let trust_anchor = directory.path().join("ca.pem");
    let secret_file = directory.path().join("secret.key");
    fs::write(&trust_anchor, b"not inspected by readiness").expect("trust anchor should be created");
    fs::write(&secret_file, b"not inspected by readiness").expect("secret should be created");
    fs::set_permissions(&secret_file, fs::Permissions::from_mode(0o640)).expect("secret permissions should be set");
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
    let insecure = SecurityConfigView::new("secure")
        .with_trust_anchor(&trust_anchor)
        .with_secret_file(&secret_file)
        .with_bootstrap_expiry(now + Duration::from_secs(1));

    let insecure_report = validate_security_config(insecure, now);

    assert!(insecure_report
        .failures()
        .contains(&SecurityReadinessFailure::InsecureSecretFilePermissions));

    fs::set_permissions(&secret_file, fs::Permissions::from_mode(0o600))
        .expect("secret permissions should be restricted");
    let secure = SecurityConfigView::new("secure")
        .with_trust_anchor(&trust_anchor)
        .with_secret_file(&secret_file)
        .with_bootstrap_expiry(now + Duration::from_secs(1));

    assert!(validate_security_config(secure, now).is_ready());
}

#[cfg(windows)]
#[test]
fn secure_profile_dry_run_fails_closed_without_windows_acl_inspection() {
    let directory = TestDirectory::new("windows-permissions");
    let trust_anchor = directory.path().join("ca.pem");
    let secret_file = directory.path().join("secret.key");
    fs::write(&trust_anchor, b"not inspected by readiness").expect("trust anchor should be created");
    fs::write(&secret_file, b"not inspected by readiness").expect("secret should be created");
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
    let view = SecurityConfigView::new("secure")
        .with_trust_anchor(&trust_anchor)
        .with_secret_file(&secret_file)
        .with_bootstrap_expiry(now + Duration::from_secs(1));

    let report = validate_security_config(view, now);

    assert!(report
        .failures()
        .contains(&SecurityReadinessFailure::InsecureSecretFilePermissions));
}
