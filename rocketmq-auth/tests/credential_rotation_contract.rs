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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::Mutex;
use std::time::Duration;
use std::time::SystemTime;

use rocketmq_auth::BreakGlassReason;
use rocketmq_auth::CredentialAuditAction;
use rocketmq_auth::CredentialAuditEvent;
use rocketmq_auth::CredentialAuditOutcome;
use rocketmq_auth::CredentialAuditSink;
use rocketmq_auth::CredentialAuditSinkError;
use rocketmq_auth::CredentialBundleParseError;
use rocketmq_auth::CredentialBundleParser;
use rocketmq_auth::CredentialId;
use rocketmq_auth::CredentialRotationError;
use rocketmq_auth::CredentialRotationManager;
use rocketmq_auth::CredentialVerificationSource;
use rocketmq_auth::ValidatedCredential;
use rocketmq_security_api::SecretAccess;
use rocketmq_security_api::SecretMaterial;
use rocketmq_security_api::SecretName;
use rocketmq_security_api::SecretPersistence;
use rocketmq_security_api::SecretProvider;
use rocketmq_security_api::SecretProviderCapabilities;
use rocketmq_security_api::SecretProviderError;
use rocketmq_security_api::SecretProviderId;
use rocketmq_security_api::SecretVersion;
use rocketmq_security_api::SecretVersioning;
use rocketmq_security_api::VersionedSecret;

const ACTIVE_PROOF: [u8; 32] = [0x11; 32];
const CANDIDATE_PROOF: [u8; 32] = [0x22; 32];
const BREAK_GLASS_PROOF: [u8; 32] = [0x99; 32];

fn material(value: &[u8]) -> SecretMaterial {
    SecretMaterial::new(value.to_vec()).unwrap()
}

fn credential(seed: u8, now: SystemTime) -> ValidatedCredential {
    ValidatedCredential::new(
        CredentialId::new(format!("kid-{seed:02x}")).unwrap(),
        [seed; 32],
        now - Duration::from_secs(60),
        now + Duration::from_secs(3600),
        material(&[seed; 32]),
        Some(SecretVersion::new(seed as u64)),
    )
    .unwrap()
}

#[derive(Default)]
struct RecordingAudit {
    events: Mutex<Vec<CredentialAuditEvent>>,
    fail: AtomicBool,
}

impl RecordingAudit {
    fn events(&self) -> Vec<CredentialAuditEvent> {
        self.events.lock().unwrap().clone()
    }

    fn set_fail(&self, value: bool) {
        self.fail.store(value, Ordering::SeqCst);
    }
}

impl CredentialAuditSink for RecordingAudit {
    fn record(&self, event: &CredentialAuditEvent) -> Result<(), CredentialAuditSinkError> {
        if self.fail.load(Ordering::SeqCst) {
            return Err(CredentialAuditSinkError::Unavailable);
        }
        self.events.lock().unwrap().push(event.clone());
        Ok(())
    }
}

fn manager(now: SystemTime, audit: Arc<RecordingAudit>) -> CredentialRotationManager {
    CredentialRotationManager::new(
        credential(0x11, now),
        Some(credential(0x99, now)),
        Duration::from_secs(300),
        now,
        audit,
    )
    .unwrap()
}

#[test]
fn overlap_switch_and_finalize_revoke_the_old_credential() {
    let now = SystemTime::now();
    let audit = Arc::new(RecordingAudit::default());
    let manager = manager(now, audit.clone());
    let active_id = CredentialId::new("kid-11").unwrap();
    let candidate_id = CredentialId::new("kid-22").unwrap();

    let initial = manager.verify(&active_id, material(&ACTIVE_PROOF), now).unwrap();
    assert_eq!(initial.source(), CredentialVerificationSource::Active);
    assert_eq!(initial.generation(), 1);

    let overlap_until = now + Duration::from_secs(30);
    assert_eq!(manager.start_rotation(credential(0x22, now), overlap_until, now), Ok(2));
    assert_eq!(
        manager
            .verify(&candidate_id, material(&CANDIDATE_PROOF), now)
            .unwrap()
            .source(),
        CredentialVerificationSource::Active
    );
    assert_eq!(
        manager
            .verify(&active_id, material(&ACTIVE_PROOF), now)
            .unwrap()
            .source(),
        CredentialVerificationSource::Retiring
    );
    assert_eq!(
        manager.finalize_rotation(now + Duration::from_secs(29)),
        Err(CredentialRotationError::OverlapStillActive)
    );
    assert_eq!(manager.finalize_rotation(overlap_until), Ok(3));
    assert_eq!(
        manager.verify(&active_id, material(&ACTIVE_PROOF), overlap_until),
        Err(CredentialRotationError::CredentialRevoked)
    );
    assert_eq!(manager.snapshot().active().id(), &candidate_id);
    assert_eq!(manager.snapshot().revoked(), &[active_id]);

    let events = audit.events();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].action(), CredentialAuditAction::RotationStarted);
    assert_eq!(events[1].action(), CredentialAuditAction::RetiringCredentialRevoked);
    assert!(events
        .iter()
        .all(|event| event.outcome() == CredentialAuditOutcome::Authorized));
}

#[test]
fn rollback_restores_only_unrevoked_last_known_good() {
    let now = SystemTime::now();
    let audit = Arc::new(RecordingAudit::default());
    let manager = manager(now, audit.clone());
    let active_id = CredentialId::new("kid-11").unwrap();
    let candidate_id = CredentialId::new("kid-22").unwrap();

    manager
        .start_rotation(credential(0x22, now), now + Duration::from_secs(30), now)
        .unwrap();
    assert_eq!(manager.rollback(now + Duration::from_secs(5)), Ok(3));
    assert_eq!(manager.snapshot().active().id(), &active_id);
    assert_eq!(manager.snapshot().revoked(), std::slice::from_ref(&candidate_id));
    assert_eq!(
        manager.verify(&candidate_id, material(&CANDIDATE_PROOF), now),
        Err(CredentialRotationError::CredentialRevoked)
    );
    assert_eq!(
        manager.start_rotation(credential(0x22, now), now + Duration::from_secs(30), now),
        Err(CredentialRotationError::InvalidCredential)
    );
    assert_eq!(
        audit.events().last().unwrap().action(),
        CredentialAuditAction::RotationRolledBack
    );
}

#[test]
fn break_glass_is_bounded_disabled_by_default_and_audit_first() {
    let now = SystemTime::now();
    let audit = Arc::new(RecordingAudit::default());
    let manager = manager(now, audit.clone());
    let break_glass_id = CredentialId::new("kid-99").unwrap();

    assert_eq!(
        manager.verify(&break_glass_id, material(&BREAK_GLASS_PROOF), now),
        Err(CredentialRotationError::BreakGlassDisabled)
    );
    assert_eq!(
        manager.enable_break_glass(
            BreakGlassReason::IdentityProviderOutage,
            now + Duration::from_secs(301),
            now,
        ),
        Err(CredentialRotationError::InvalidBreakGlassWindow)
    );

    audit.set_fail(true);
    assert_eq!(
        manager.enable_break_glass(
            BreakGlassReason::IdentityProviderOutage,
            now + Duration::from_secs(60),
            now,
        ),
        Err(CredentialRotationError::AuditUnavailable)
    );
    assert_eq!(manager.snapshot().generation(), 1);

    audit.set_fail(false);
    assert_eq!(
        manager.enable_break_glass(
            BreakGlassReason::IdentityProviderOutage,
            now + Duration::from_secs(60),
            now,
        ),
        Ok(2)
    );
    assert_eq!(
        manager
            .verify(&break_glass_id, material(&BREAK_GLASS_PROOF), now)
            .unwrap()
            .source(),
        CredentialVerificationSource::BreakGlass
    );
    assert_eq!(
        manager.verify(
            &break_glass_id,
            material(&BREAK_GLASS_PROOF),
            now + Duration::from_secs(60),
        ),
        Err(CredentialRotationError::CredentialExpired)
    );
    assert_eq!(manager.disable_break_glass(), Ok(3));
    assert_eq!(
        manager.verify(&break_glass_id, material(&BREAK_GLASS_PROOF), now),
        Err(CredentialRotationError::BreakGlassDisabled)
    );
    assert_eq!(
        audit
            .events()
            .iter()
            .map(CredentialAuditEvent::action)
            .collect::<Vec<_>>(),
        vec![
            CredentialAuditAction::BreakGlassEnabled,
            CredentialAuditAction::BreakGlassDisabled
        ]
    );
}

struct StaticProvider {
    id: SecretProviderId,
    bundle: Vec<u8>,
    version: SecretVersion,
    fail: bool,
}

impl SecretProvider for StaticProvider {
    fn id(&self) -> &SecretProviderId {
        &self.id
    }

    fn capabilities(&self) -> SecretProviderCapabilities {
        SecretProviderCapabilities::new(
            SecretAccess::ReadOnly,
            SecretPersistence::ExternalService,
            SecretVersioning::Optimistic,
        )
    }

    fn read(&self, _name: &SecretName) -> Result<VersionedSecret, SecretProviderError> {
        if self.fail {
            return Err(SecretProviderError::Unavailable);
        }
        Ok(VersionedSecret::new(material(&self.bundle), Some(self.version)))
    }

    fn write(
        &self,
        _name: &SecretName,
        _material: SecretMaterial,
        _expected_version: Option<SecretVersion>,
    ) -> Result<SecretVersion, SecretProviderError> {
        Err(SecretProviderError::ReadOnly)
    }
}

struct TestBundleParser {
    now: SystemTime,
    force_version_mismatch: bool,
}

impl CredentialBundleParser for TestBundleParser {
    fn parse(
        &self,
        material: SecretMaterial,
        provider_version: Option<SecretVersion>,
    ) -> Result<ValidatedCredential, CredentialBundleParseError> {
        let seed = *material
            .expose_secret()
            .first()
            .ok_or(CredentialBundleParseError::InvalidBundle)?;
        if seed == 0 || material.len() < 32 {
            return Err(CredentialBundleParseError::InvalidBundle);
        }
        let version = if self.force_version_mismatch {
            Some(SecretVersion::new(999))
        } else {
            provider_version
        };
        ValidatedCredential::new(
            CredentialId::new(format!("kid-{seed:02x}")).map_err(|_| CredentialBundleParseError::InvalidBundle)?,
            [seed; 32],
            self.now - Duration::from_secs(60),
            self.now + Duration::from_secs(3600),
            material,
            version,
        )
        .map_err(|_| CredentialBundleParseError::InvalidBundle)
    }
}

#[test]
fn provider_reload_is_single_bundle_and_invalid_candidates_preserve_snapshot() {
    let now = SystemTime::now();
    let audit = Arc::new(RecordingAudit::default());
    let manager = manager(now, audit.clone());
    let name = SecretName::new("cluster-credential-bundle").unwrap();
    let invalid = StaticProvider {
        id: SecretProviderId::new("test-provider").unwrap(),
        bundle: vec![0; 32],
        version: SecretVersion::new(2),
        fail: false,
    };
    let parser = TestBundleParser {
        now,
        force_version_mismatch: false,
    };
    assert_eq!(
        manager.reload_from_provider(&invalid, &name, &parser, now + Duration::from_secs(30), now),
        Err(CredentialRotationError::InvalidBundle)
    );
    assert_eq!(manager.snapshot().generation(), 1);
    assert_eq!(
        audit.events().last().unwrap().action(),
        CredentialAuditAction::ReloadRejected
    );

    let unavailable = StaticProvider {
        id: SecretProviderId::new("test-provider").unwrap(),
        bundle: Vec::new(),
        version: SecretVersion::new(2),
        fail: true,
    };
    assert_eq!(
        manager.reload_from_provider(&unavailable, &name, &parser, now + Duration::from_secs(30), now,),
        Err(CredentialRotationError::ProviderUnavailable)
    );
    assert_eq!(manager.snapshot().generation(), 1);

    let valid = StaticProvider {
        id: SecretProviderId::new("test-provider").unwrap(),
        bundle: CANDIDATE_PROOF.to_vec(),
        version: SecretVersion::new(2),
        fail: false,
    };
    let mismatched_parser = TestBundleParser {
        now,
        force_version_mismatch: true,
    };
    assert_eq!(
        manager.reload_from_provider(&valid, &name, &mismatched_parser, now + Duration::from_secs(30), now,),
        Err(CredentialRotationError::InvalidBundle)
    );
    assert_eq!(manager.snapshot().generation(), 1);

    assert_eq!(
        manager.reload_from_provider(&valid, &name, &parser, now + Duration::from_secs(30), now),
        Ok(2)
    );
    assert_eq!(manager.snapshot().active().id().as_str(), "kid-22");
    assert_eq!(
        manager.snapshot().active().provider_version(),
        Some(SecretVersion::new(2))
    );
}

#[test]
fn concurrent_readers_observe_only_complete_old_or_new_snapshots() {
    let now = SystemTime::now();
    let audit = Arc::new(RecordingAudit::default());
    let manager = Arc::new(manager(now, audit));
    let barrier = Arc::new(Barrier::new(9));
    let handles = (0..8)
        .map(|index| {
            let manager = manager.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let (id, proof) = if index % 2 == 0 {
                    (CredentialId::new("kid-11").unwrap(), ACTIVE_PROOF)
                } else {
                    (CredentialId::new("kid-22").unwrap(), CANDIDATE_PROOF)
                };
                barrier.wait();
                (0..500)
                    .map(|_| manager.verify(&id, material(&proof), now))
                    .collect::<Vec<_>>()
            })
        })
        .collect::<Vec<_>>();
    barrier.wait();
    manager
        .start_rotation(credential(0x22, now), now + Duration::from_secs(30), now)
        .unwrap();

    for result in handles.into_iter().flat_map(|handle| handle.join().unwrap()) {
        match result {
            Ok(verification) => assert!(matches!(
                verification.source(),
                CredentialVerificationSource::Active | CredentialVerificationSource::Retiring
            )),
            Err(error) => assert_eq!(error, CredentialRotationError::CredentialNotAccepted),
        }
    }
}

#[test]
fn debug_and_errors_never_expose_proof_material() {
    let now = SystemTime::now();
    let proof = [0x5a; 32];
    let validated = ValidatedCredential::new(
        CredentialId::new("redacted-kid").unwrap(),
        [0x44; 32],
        now - Duration::from_secs(1),
        now + Duration::from_secs(60),
        material(&proof),
        None,
    )
    .unwrap();
    let debug = format!("{validated:?}");
    assert!(debug.contains("[REDACTED]"));
    assert!(!debug.contains(&hex::encode(proof)));
    assert!(!debug.contains(&hex::encode([0x44; 32])));
    assert!(!CredentialRotationError::InvalidProof.to_string().contains("5a"));

    let audit = Arc::new(RecordingAudit::default());
    let manager = manager(now, audit.clone());
    manager
        .start_rotation(credential(0x22, now), now + Duration::from_secs(30), now)
        .unwrap();
    let event_debug = format!("{:?}", audit.events().last().unwrap());
    assert!(event_debug.contains("[REDACTED]"));
    assert!(!event_debug.contains("kid-11"));
    assert!(!event_debug.contains("kid-22"));
    let verification_debug = format!(
        "{:?}",
        manager
            .verify(&CredentialId::new("kid-22").unwrap(), material(&CANDIDATE_PROOF), now,)
            .unwrap()
    );
    assert!(verification_debug.contains("[REDACTED]"));
    assert!(!verification_debug.contains("kid-22"));
}
