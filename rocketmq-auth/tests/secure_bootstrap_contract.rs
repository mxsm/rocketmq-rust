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

#[cfg(unix)]
use std::sync::atomic::AtomicUsize;
#[cfg(unix)]
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::SystemTime;

#[cfg(unix)]
use rocketmq_auth::BootstrapAdminIdentity;
#[cfg(unix)]
use rocketmq_auth::BootstrapAdminProvisioner;
#[cfg(unix)]
use rocketmq_auth::BootstrapAdminProvisioningError;
#[cfg(unix)]
use rocketmq_auth::BootstrapEnrollmentRequest;
use rocketmq_auth::BootstrapError;
use rocketmq_auth::BootstrapGrant;
#[cfg(unix)]
use rocketmq_auth::BootstrapStatus;
#[cfg(unix)]
use rocketmq_auth::BootstrapTransportContext;
use rocketmq_auth::OneTimeBootstrap;
use rocketmq_security_api::SecretMaterial;

const CLUSTER: &str = "secure-cluster";
const LISTENER: &str = "127.0.0.1:19876";
const PROOF: [u8; 32] = [0x41; 32];

fn material(value: &[u8]) -> SecretMaterial {
    SecretMaterial::new(value.to_vec()).unwrap()
}

fn grant(expires_at: SystemTime) -> BootstrapGrant {
    BootstrapGrant::new(CLUSTER, LISTENER, expires_at, material(&PROOF)).unwrap()
}

#[cfg(unix)]
fn secure_tempdir() -> tempfile::TempDir {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().unwrap();
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
    directory
}

#[cfg(unix)]
fn identity() -> BootstrapAdminIdentity {
    BootstrapAdminIdentity::new("cluster-admin", [0x52; 32]).unwrap()
}

#[cfg(unix)]
fn request(cluster: &str, listener: &str, verified_tls: bool, proof: &[u8]) -> BootstrapEnrollmentRequest {
    let transport = BootstrapTransportContext::new(cluster, listener).unwrap();
    let transport = if verified_tls {
        transport.with_verified_tls()
    } else {
        transport
    };
    BootstrapEnrollmentRequest::new(transport, material(proof), identity())
}

#[cfg(unix)]
struct RecordingProvisioner {
    calls: AtomicUsize,
    result: Result<(), BootstrapAdminProvisioningError>,
}

#[cfg(unix)]
impl RecordingProvisioner {
    fn successful() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            result: Ok(()),
        }
    }

    fn failing() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            result: Err(BootstrapAdminProvisioningError::Unavailable),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

#[cfg(unix)]
impl BootstrapAdminProvisioner for RecordingProvisioner {
    fn create_first_admin(&self, _identity: &BootstrapAdminIdentity) -> Result<(), BootstrapAdminProvisioningError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.result
    }
}

#[cfg(windows)]
#[test]
fn bootstrap_persistence_fails_closed_without_acl_verification() {
    let directory = tempfile::tempdir().unwrap();
    let error = OneTimeBootstrap::open(
        grant(SystemTime::now() + Duration::from_secs(60)),
        directory.path().join("bootstrap.json"),
    )
    .unwrap_err();
    assert_eq!(error, BootstrapError::UnsupportedPlatform);
}

#[cfg(unix)]
#[test]
fn successful_bootstrap_consumes_once_and_survives_restart_without_proof_bytes() {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    let directory = secure_tempdir();
    let state_path = directory.path().join("bootstrap.json");
    let expires_at = SystemTime::now() + Duration::from_secs(60);
    let bootstrap = OneTimeBootstrap::open(grant(expires_at), &state_path).unwrap();
    let provisioner = RecordingProvisioner::successful();
    assert!(bootstrap.is_open(SystemTime::now()).unwrap());

    let result = bootstrap
        .enroll(
            request(CLUSTER, LISTENER, true, &PROOF),
            SystemTime::now(),
            &provisioner,
        )
        .unwrap();
    assert_eq!(result.principal_id(), "cluster-admin");
    assert_eq!(result.status(), BootstrapStatus::Consumed);
    assert_eq!(provisioner.calls(), 1);
    assert!(!bootstrap.is_open(SystemTime::now()).unwrap());
    assert_eq!(
        bootstrap
            .enroll(
                request(CLUSTER, LISTENER, true, &PROOF),
                SystemTime::now(),
                &provisioner,
            )
            .unwrap_err(),
        BootstrapError::AlreadyConsumed
    );
    assert_eq!(provisioner.calls(), 1);

    let claim_path = directory.path().join("bootstrap.json.claim");
    let persisted = [fs::read(&state_path).unwrap(), fs::read(&claim_path).unwrap()].concat();
    assert!(!persisted.windows(PROOF.len()).any(|window| window == PROOF));
    assert_eq!(fs::metadata(&state_path).unwrap().permissions().mode() & 0o077, 0);
    assert_eq!(fs::metadata(&claim_path).unwrap().permissions().mode() & 0o077, 0);

    let restarted = OneTimeBootstrap::open(grant(expires_at), state_path).unwrap();
    assert_eq!(restarted.status().unwrap(), BootstrapStatus::Consumed);
    assert_eq!(
        restarted
            .enroll(
                request(CLUSTER, LISTENER, true, &PROOF),
                SystemTime::now(),
                &provisioner,
            )
            .unwrap_err(),
        BootstrapError::AlreadyConsumed
    );
    assert_eq!(provisioner.calls(), 1);
}

#[cfg(unix)]
#[test]
fn invalid_transport_binding_and_proof_do_not_claim_the_grant() {
    let directory = secure_tempdir();
    let state_path = directory.path().join("bootstrap.json");
    let bootstrap = OneTimeBootstrap::open(grant(SystemTime::now() + Duration::from_secs(60)), state_path).unwrap();
    let provisioner = RecordingProvisioner::successful();

    let cases = [
        (request(CLUSTER, LISTENER, false, &PROOF), BootstrapError::TlsRequired),
        (
            request("other-cluster", LISTENER, true, &PROOF),
            BootstrapError::BindingMismatch,
        ),
        (
            request(CLUSTER, "127.0.0.1:29876", true, &PROOF),
            BootstrapError::BindingMismatch,
        ),
        (
            request(CLUSTER, LISTENER, true, &[0x42; 32]),
            BootstrapError::InvalidProof,
        ),
    ];
    for (request, expected) in cases {
        assert_eq!(
            bootstrap.enroll(request, SystemTime::now(), &provisioner).unwrap_err(),
            expected
        );
        assert_eq!(bootstrap.status().unwrap(), BootstrapStatus::Available);
    }
    assert_eq!(provisioner.calls(), 0);

    bootstrap
        .enroll(
            request(CLUSTER, LISTENER, true, &PROOF),
            SystemTime::now(),
            &provisioner,
        )
        .unwrap();
    assert_eq!(provisioner.calls(), 1);
}

#[cfg(unix)]
#[test]
fn expired_grant_never_invokes_the_provisioner() {
    let directory = secure_tempdir();
    let bootstrap = OneTimeBootstrap::open(
        grant(SystemTime::now() - Duration::from_secs(1)),
        directory.path().join("bootstrap.json"),
    )
    .unwrap();
    let provisioner = RecordingProvisioner::successful();
    assert!(!bootstrap.is_open(SystemTime::now()).unwrap());
    assert_eq!(
        bootstrap
            .enroll(
                request(CLUSTER, LISTENER, true, &PROOF),
                SystemTime::now(),
                &provisioner,
            )
            .unwrap_err(),
        BootstrapError::Expired
    );
    assert_eq!(provisioner.calls(), 0);
}

#[cfg(unix)]
#[test]
fn provisioning_failure_leaves_a_persistent_claim_and_never_retries() {
    let directory = secure_tempdir();
    let state_path = directory.path().join("bootstrap.json");
    let expires_at = SystemTime::now() + Duration::from_secs(60);
    let bootstrap = OneTimeBootstrap::open(grant(expires_at), &state_path).unwrap();
    let provisioner = RecordingProvisioner::failing();
    assert_eq!(
        bootstrap
            .enroll(
                request(CLUSTER, LISTENER, true, &PROOF),
                SystemTime::now(),
                &provisioner,
            )
            .unwrap_err(),
        BootstrapError::AdminProvisioningFailed
    );
    assert_eq!(bootstrap.status().unwrap(), BootstrapStatus::Claimed);
    assert_eq!(provisioner.calls(), 1);
    assert_eq!(
        bootstrap
            .enroll(
                request(CLUSTER, LISTENER, true, &PROOF),
                SystemTime::now(),
                &provisioner,
            )
            .unwrap_err(),
        BootstrapError::AlreadyClaimed
    );
    assert_eq!(provisioner.calls(), 1);

    let restarted = OneTimeBootstrap::open(grant(expires_at), state_path).unwrap();
    assert_eq!(restarted.status().unwrap(), BootstrapStatus::Claimed);
}

#[cfg(unix)]
#[test]
fn concurrent_coordinators_share_one_atomic_claim() {
    use std::sync::Arc;
    use std::sync::Barrier;

    let directory = secure_tempdir();
    let state_path = directory.path().join("bootstrap.json");
    let expires_at = SystemTime::now() + Duration::from_secs(60);
    let first = Arc::new(OneTimeBootstrap::open(grant(expires_at), &state_path).unwrap());
    let second = Arc::new(OneTimeBootstrap::open(grant(expires_at), &state_path).unwrap());
    let provisioner = Arc::new(RecordingProvisioner::successful());
    let barrier = Arc::new(Barrier::new(2));

    let handles = [first, second].map(|bootstrap| {
        let provisioner = provisioner.clone();
        let barrier = barrier.clone();
        std::thread::spawn(move || {
            barrier.wait();
            bootstrap.enroll(
                request(CLUSTER, LISTENER, true, &PROOF),
                SystemTime::now(),
                provisioner.as_ref(),
            )
        })
    });
    let results = handles.map(|handle| handle.join().unwrap());
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(BootstrapError::AlreadyClaimed)))
            .count(),
        1
    );
    assert_eq!(provisioner.calls(), 1);
}

#[cfg(unix)]
#[test]
fn broad_state_directory_permissions_are_rejected() {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().unwrap();
    let insecure = directory.path().join("insecure");
    fs::create_dir(&insecure).unwrap();
    fs::set_permissions(&insecure, fs::Permissions::from_mode(0o755)).unwrap();
    assert_eq!(
        OneTimeBootstrap::open(
            grant(SystemTime::now() + Duration::from_secs(60)),
            insecure.join("bootstrap.json"),
        )
        .unwrap_err(),
        BootstrapError::InsecurePermissions
    );
}

#[cfg(unix)]
#[test]
fn malformed_persisted_state_fails_closed() {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    let directory = secure_tempdir();
    let state_path = directory.path().join("bootstrap.json");
    let expires_at = SystemTime::now() + Duration::from_secs(60);
    drop(OneTimeBootstrap::open(grant(expires_at), &state_path).unwrap());

    let mut state: serde_json::Value = serde_json::from_slice(&fs::read(&state_path).unwrap()).unwrap();
    state["status"] = serde_json::Value::String("claimed".to_owned());
    fs::set_permissions(&state_path, fs::Permissions::from_mode(0o600)).unwrap();
    fs::write(&state_path, serde_json::to_vec(&state).unwrap()).unwrap();
    fs::set_permissions(&state_path, fs::Permissions::from_mode(0o400)).unwrap();

    assert_eq!(
        OneTimeBootstrap::open(grant(expires_at), state_path).unwrap_err(),
        BootstrapError::StateUnavailable
    );
}
