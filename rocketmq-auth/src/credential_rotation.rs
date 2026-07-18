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

//! Audit-first credential rotation with immutable request-path snapshots.

mod model;

pub use model::BreakGlassStatus;
pub use model::CredentialRotationSnapshot;
pub use model::CredentialVerification;
pub use model::CredentialVerificationSource;
pub use model::RetiringCredentialSnapshot;

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use arc_swap::ArcSwap;
use cheetah_string::CheetahString;
use rocketmq_security_api::SecretMaterial;
use rocketmq_security_api::SecretName;
use rocketmq_security_api::SecretProvider;
use rocketmq_security_api::SecretVersion;
use sha2::Digest;
use sha2::Sha256;
use subtle::ConstantTimeEq;
use thiserror::Error;

const MINIMUM_CREDENTIAL_PROOF_BYTES: usize = 32;

/// Stable, non-secret key identifier used for routing verification requests.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CredentialId(CheetahString);

impl CredentialId {
    /// Creates an identifier containing only ASCII letters, digits, `-`, `_`, or `.`.
    ///
    /// # Errors
    ///
    /// Returns [`CredentialRotationError::InvalidCredential`] for an empty, hidden, oversized,
    /// or unsupported identifier.
    pub fn new(value: impl Into<CheetahString>) -> Result<Self, CredentialRotationError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 128
            || value.starts_with('.')
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        {
            return Err(CredentialRotationError::InvalidCredential);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for CredentialId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("CredentialId").field(&self.0).finish()
    }
}

/// Public metadata for a fully validated credential. It never contains proof or key material.
#[derive(Clone, PartialEq, Eq)]
pub struct CredentialDescriptor {
    id: CredentialId,
    certificate_sha256: [u8; 32],
    valid_from_unix_seconds: u64,
    expires_at_unix_seconds: u64,
    provider_version: Option<SecretVersion>,
}

impl fmt::Debug for CredentialDescriptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CredentialDescriptor")
            .field("id", &self.id)
            .field("certificate_sha256", &"[REDACTED]")
            .field("valid_from_unix_seconds", &self.valid_from_unix_seconds)
            .field("expires_at_unix_seconds", &self.expires_at_unix_seconds)
            .field("provider_version", &self.provider_version)
            .finish()
    }
}

impl CredentialDescriptor {
    pub fn id(&self) -> &CredentialId {
        &self.id
    }

    pub const fn certificate_sha256(&self) -> &[u8; 32] {
        &self.certificate_sha256
    }

    pub const fn valid_from_unix_seconds(&self) -> u64 {
        self.valid_from_unix_seconds
    }

    pub const fn expires_at_unix_seconds(&self) -> u64 {
        self.expires_at_unix_seconds
    }

    pub const fn provider_version(&self) -> Option<SecretVersion> {
        self.provider_version
    }
}

/// Credential whose certificate/key pair and proof have already been parsed and validated.
pub struct ValidatedCredential {
    descriptor: CredentialDescriptor,
    proof_digest: [u8; 32],
}

impl ValidatedCredential {
    /// Creates a validated snapshot and discards the raw proof after hashing it.
    ///
    /// Parsers must validate the certificate/key relationship before calling this constructor.
    ///
    /// # Errors
    ///
    /// Returns [`CredentialRotationError::InvalidCredential`] for invalid validity bounds, an
    /// all-zero certificate fingerprint, or proof material shorter than 32 bytes.
    pub fn new(
        id: CredentialId,
        certificate_sha256: [u8; 32],
        valid_from: SystemTime,
        expires_at: SystemTime,
        proof: SecretMaterial,
        provider_version: Option<SecretVersion>,
    ) -> Result<Self, CredentialRotationError> {
        let valid_from_unix_seconds = unix_seconds(valid_from)?;
        let expires_at_unix_seconds = unix_seconds(expires_at)?;
        if valid_from_unix_seconds >= expires_at_unix_seconds
            || certificate_sha256.iter().all(|byte| *byte == 0)
            || proof.len() < MINIMUM_CREDENTIAL_PROOF_BYTES
        {
            return Err(CredentialRotationError::InvalidCredential);
        }
        Ok(Self {
            descriptor: CredentialDescriptor {
                id,
                certificate_sha256,
                valid_from_unix_seconds,
                expires_at_unix_seconds,
                provider_version,
            },
            proof_digest: Sha256::digest(proof.expose_secret()).into(),
        })
    }

    pub fn descriptor(&self) -> &CredentialDescriptor {
        &self.descriptor
    }

    fn is_valid_at(&self, now: u64) -> bool {
        self.descriptor.valid_from_unix_seconds <= now && now < self.descriptor.expires_at_unix_seconds
    }

    fn verifies(&self, proof: &SecretMaterial) -> bool {
        let presented: [u8; 32] = Sha256::digest(proof.expose_secret()).into();
        self.proof_digest.ct_eq(&presented).unwrap_u8() == 1
    }
}

impl fmt::Debug for ValidatedCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ValidatedCredential")
            .field("descriptor", &self.descriptor)
            .field("proof_digest", &"[REDACTED]")
            .finish()
    }
}

/// Opaque parse failure. Certificate, key, and proof details are never included.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum CredentialBundleParseError {
    #[error("credential bundle is invalid")]
    InvalidBundle,
}

/// Parser injected by the owning protocol/TLS adapter.
pub trait CredentialBundleParser: Send + Sync {
    /// Parses and validates a single atomic provider bundle.
    ///
    /// The implementation must validate the entire certificate/key/proof relationship before
    /// returning. The returned provider version must equal `provider_version`.
    fn parse(
        &self,
        material: SecretMaterial,
        provider_version: Option<SecretVersion>,
    ) -> Result<ValidatedCredential, CredentialBundleParseError>;
}

/// Typed reason required before break-glass can be enabled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BreakGlassReason {
    IdentityProviderOutage,
    CertificateRecovery,
    AdministrativeRecovery,
}

/// Stable actions sent to the injected audit sink before state publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialAuditAction {
    RotationStarted,
    RetiringCredentialRevoked,
    RotationRolledBack,
    ReloadRejected,
    BreakGlassEnabled,
    BreakGlassDisabled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialAuditOutcome {
    Authorized,
    Rejected,
}

/// Non-sensitive audit event. IDs and fingerprints are metadata, never credential material.
#[derive(Clone, PartialEq, Eq)]
pub struct CredentialAuditEvent {
    action: CredentialAuditAction,
    outcome: CredentialAuditOutcome,
    generation: u64,
    credential_id: Option<CredentialId>,
    related_credential_id: Option<CredentialId>,
    break_glass_reason: Option<BreakGlassReason>,
}

impl fmt::Debug for CredentialAuditEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CredentialAuditEvent")
            .field("action", &self.action)
            .field("outcome", &self.outcome)
            .field("generation", &self.generation)
            .field("credential_id", &self.credential_id.as_ref().map(|_| "[REDACTED]"))
            .field(
                "related_credential_id",
                &self.related_credential_id.as_ref().map(|_| "[REDACTED]"),
            )
            .field("break_glass_reason", &self.break_glass_reason)
            .finish()
    }
}

impl CredentialAuditEvent {
    pub const fn action(&self) -> CredentialAuditAction {
        self.action
    }

    pub const fn outcome(&self) -> CredentialAuditOutcome {
        self.outcome
    }

    pub const fn generation(&self) -> u64 {
        self.generation
    }

    pub fn credential_id(&self) -> Option<&CredentialId> {
        self.credential_id.as_ref()
    }

    pub fn related_credential_id(&self) -> Option<&CredentialId> {
        self.related_credential_id.as_ref()
    }

    pub const fn break_glass_reason(&self) -> Option<BreakGlassReason> {
        self.break_glass_reason
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum CredentialAuditSinkError {
    #[error("credential audit sink is unavailable")]
    Unavailable,
}

/// Synchronous audit-first boundary. PR-M11-06 owns asynchronous buffering and shutdown drain.
pub trait CredentialAuditSink: Send + Sync {
    fn record(&self, event: &CredentialAuditEvent) -> Result<(), CredentialAuditSinkError>;
}

/// Redacted rotation failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum CredentialRotationError {
    #[error("credential configuration is invalid")]
    InvalidCredential,
    #[error("credential is not currently valid")]
    CredentialExpired,
    #[error("credential rotation is already in progress")]
    RotationInProgress,
    #[error("credential rotation is not in progress")]
    NoRotationInProgress,
    #[error("credential overlap window is still active")]
    OverlapStillActive,
    #[error("credential has been revoked")]
    CredentialRevoked,
    #[error("credential is not accepted")]
    CredentialNotAccepted,
    #[error("credential proof is invalid")]
    InvalidProof,
    #[error("credential bundle is invalid")]
    InvalidBundle,
    #[error("credential provider is unavailable")]
    ProviderUnavailable,
    #[error("credential audit is unavailable")]
    AuditUnavailable,
    #[error("credential generation is exhausted")]
    GenerationExhausted,
    #[error("break-glass credential is not configured")]
    BreakGlassNotConfigured,
    #[error("break-glass credential is already enabled")]
    BreakGlassAlreadyEnabled,
    #[error("break-glass credential is disabled")]
    BreakGlassDisabled,
    #[error("break-glass activation window is invalid")]
    InvalidBreakGlassWindow,
}

struct RetiringCredential {
    credential: Arc<ValidatedCredential>,
    accept_until_unix_seconds: u64,
}

struct BreakGlassActivation {
    expires_at_unix_seconds: u64,
    reason: BreakGlassReason,
}

struct BreakGlassCredential {
    credential: Arc<ValidatedCredential>,
    activation: Option<BreakGlassActivation>,
}

struct RotationState {
    generation: u64,
    active: Arc<ValidatedCredential>,
    retiring: Option<RetiringCredential>,
    revoked: BTreeSet<CredentialId>,
    break_glass: Option<BreakGlassCredential>,
}

/// Concurrent credential owner. Readers load one immutable snapshot; writers serialize and
/// publish only after validation and audit succeed.
pub struct CredentialRotationManager {
    state: ArcSwap<RotationState>,
    writer: Mutex<()>,
    audit: Arc<dyn CredentialAuditSink>,
    maximum_break_glass_duration: Duration,
}

impl CredentialRotationManager {
    /// Creates generation 1 with break-glass disabled by default.
    ///
    /// # Errors
    ///
    /// Returns a redacted error if the active credential is not currently valid or the optional
    /// break-glass credential conflicts with it.
    pub fn new(
        active: ValidatedCredential,
        break_glass: Option<ValidatedCredential>,
        maximum_break_glass_duration: Duration,
        now: SystemTime,
        audit: Arc<dyn CredentialAuditSink>,
    ) -> Result<Self, CredentialRotationError> {
        let now = unix_seconds(now)?;
        if !active.is_valid_at(now) || maximum_break_glass_duration.is_zero() {
            return Err(CredentialRotationError::InvalidCredential);
        }
        if break_glass
            .as_ref()
            .is_some_and(|credential| credential.descriptor.id == active.descriptor.id || !credential.is_valid_at(now))
        {
            return Err(CredentialRotationError::InvalidCredential);
        }
        Ok(Self {
            state: ArcSwap::from_pointee(RotationState {
                generation: 1,
                active: Arc::new(active),
                retiring: None,
                revoked: BTreeSet::new(),
                break_glass: break_glass.map(|credential| BreakGlassCredential {
                    credential: Arc::new(credential),
                    activation: None,
                }),
            }),
            writer: Mutex::new(()),
            audit,
            maximum_break_glass_duration,
        })
    }

    pub fn snapshot(&self) -> CredentialRotationSnapshot {
        let state = self.state.load();
        CredentialRotationSnapshot {
            generation: state.generation,
            active: state.active.descriptor.clone(),
            retiring: state.retiring.as_ref().map(|retiring| RetiringCredentialSnapshot {
                credential: retiring.credential.descriptor.clone(),
                accept_until_unix_seconds: retiring.accept_until_unix_seconds,
            }),
            revoked: state.revoked.iter().cloned().collect(),
            break_glass: match state.break_glass.as_ref() {
                None => BreakGlassStatus::NotConfigured,
                Some(break_glass) => match break_glass.activation.as_ref() {
                    None => BreakGlassStatus::Disabled {
                        credential: break_glass.credential.descriptor.clone(),
                    },
                    Some(activation) => BreakGlassStatus::Enabled {
                        credential: break_glass.credential.descriptor.clone(),
                        expires_at_unix_seconds: activation.expires_at_unix_seconds,
                        reason: activation.reason,
                    },
                },
            },
        }
    }

    /// Loads a single atomic provider bundle, parses it completely, then starts rotation.
    ///
    /// # Errors
    ///
    /// Provider and parser failures leave the current snapshot unchanged and emit a rejected
    /// audit event. Audit failure also leaves the snapshot unchanged.
    pub fn reload_from_provider(
        &self,
        provider: &dyn SecretProvider,
        name: &SecretName,
        parser: &dyn CredentialBundleParser,
        overlap_until: SystemTime,
        now: SystemTime,
    ) -> Result<u64, CredentialRotationError> {
        let loaded = match provider.read(name) {
            Ok(loaded) => loaded,
            Err(_) => {
                self.audit_rejected_reload()?;
                return Err(CredentialRotationError::ProviderUnavailable);
            }
        };
        let provider_version = loaded.version();
        let candidate = match parser.parse(loaded.into_material(), provider_version) {
            Ok(candidate) if candidate.descriptor.provider_version == provider_version => candidate,
            Ok(_) | Err(_) => {
                self.audit_rejected_reload()?;
                return Err(CredentialRotationError::InvalidBundle);
            }
        };
        self.start_rotation(candidate, overlap_until, now)
    }

    /// Publishes a fully validated candidate as active while retaining the previous active
    /// credential for the bounded overlap window.
    pub fn start_rotation(
        &self,
        candidate: ValidatedCredential,
        overlap_until: SystemTime,
        now: SystemTime,
    ) -> Result<u64, CredentialRotationError> {
        let _writer = self
            .writer
            .lock()
            .map_err(|_| CredentialRotationError::InvalidCredential)?;
        let state = self.state.load_full();
        if state.retiring.is_some() {
            return Err(CredentialRotationError::RotationInProgress);
        }
        let now = unix_seconds(now)?;
        let overlap_until = unix_seconds(overlap_until)?;
        if !candidate.is_valid_at(now)
            || overlap_until <= now
            || overlap_until > candidate.descriptor.expires_at_unix_seconds
            || overlap_until > state.active.descriptor.expires_at_unix_seconds
            || candidate.descriptor.id == state.active.descriptor.id
            || state.revoked.contains(&candidate.descriptor.id)
            || state
                .break_glass
                .as_ref()
                .is_some_and(|break_glass| break_glass.credential.descriptor.id == candidate.descriptor.id)
        {
            return Err(CredentialRotationError::InvalidCredential);
        }
        let generation = next_generation(state.generation)?;
        self.record_audit(CredentialAuditEvent {
            action: CredentialAuditAction::RotationStarted,
            outcome: CredentialAuditOutcome::Authorized,
            generation,
            credential_id: Some(candidate.descriptor.id.clone()),
            related_credential_id: Some(state.active.descriptor.id.clone()),
            break_glass_reason: None,
        })?;
        self.state.store(Arc::new(RotationState {
            generation,
            active: Arc::new(candidate),
            retiring: Some(RetiringCredential {
                credential: state.active.clone(),
                accept_until_unix_seconds: overlap_until,
            }),
            revoked: state.revoked.clone(),
            break_glass: clone_break_glass(&state.break_glass),
        }));
        Ok(generation)
    }

    /// Revokes the retiring credential after the overlap window.
    pub fn finalize_rotation(&self, now: SystemTime) -> Result<u64, CredentialRotationError> {
        let _writer = self
            .writer
            .lock()
            .map_err(|_| CredentialRotationError::InvalidCredential)?;
        let state = self.state.load_full();
        let retiring = state
            .retiring
            .as_ref()
            .ok_or(CredentialRotationError::NoRotationInProgress)?;
        if unix_seconds(now)? < retiring.accept_until_unix_seconds {
            return Err(CredentialRotationError::OverlapStillActive);
        }
        let generation = next_generation(state.generation)?;
        self.record_audit(CredentialAuditEvent {
            action: CredentialAuditAction::RetiringCredentialRevoked,
            outcome: CredentialAuditOutcome::Authorized,
            generation,
            credential_id: Some(retiring.credential.descriptor.id.clone()),
            related_credential_id: Some(state.active.descriptor.id.clone()),
            break_glass_reason: None,
        })?;
        let mut revoked = state.revoked.clone();
        revoked.insert(retiring.credential.descriptor.id.clone());
        self.state.store(Arc::new(RotationState {
            generation,
            active: state.active.clone(),
            retiring: None,
            revoked,
            break_glass: clone_break_glass(&state.break_glass),
        }));
        Ok(generation)
    }

    /// Rolls back to the unrevoked last-known-good credential and revokes the failed candidate.
    pub fn rollback(&self, now: SystemTime) -> Result<u64, CredentialRotationError> {
        let _writer = self
            .writer
            .lock()
            .map_err(|_| CredentialRotationError::InvalidCredential)?;
        let state = self.state.load_full();
        let retiring = state
            .retiring
            .as_ref()
            .ok_or(CredentialRotationError::NoRotationInProgress)?;
        let now = unix_seconds(now)?;
        if !retiring.credential.is_valid_at(now) || state.revoked.contains(&retiring.credential.descriptor.id) {
            return Err(CredentialRotationError::CredentialRevoked);
        }
        let generation = next_generation(state.generation)?;
        self.record_audit(CredentialAuditEvent {
            action: CredentialAuditAction::RotationRolledBack,
            outcome: CredentialAuditOutcome::Authorized,
            generation,
            credential_id: Some(retiring.credential.descriptor.id.clone()),
            related_credential_id: Some(state.active.descriptor.id.clone()),
            break_glass_reason: None,
        })?;
        let mut revoked = state.revoked.clone();
        revoked.insert(state.active.descriptor.id.clone());
        self.state.store(Arc::new(RotationState {
            generation,
            active: retiring.credential.clone(),
            retiring: None,
            revoked,
            break_glass: clone_break_glass(&state.break_glass),
        }));
        Ok(generation)
    }

    pub fn enable_break_glass(
        &self,
        reason: BreakGlassReason,
        expires_at: SystemTime,
        now: SystemTime,
    ) -> Result<u64, CredentialRotationError> {
        let _writer = self
            .writer
            .lock()
            .map_err(|_| CredentialRotationError::InvalidCredential)?;
        let state = self.state.load_full();
        let break_glass = state
            .break_glass
            .as_ref()
            .ok_or(CredentialRotationError::BreakGlassNotConfigured)?;
        if break_glass.activation.is_some() {
            return Err(CredentialRotationError::BreakGlassAlreadyEnabled);
        }
        let now = unix_seconds(now)?;
        let expires_at = unix_seconds(expires_at)?;
        let maximum = now.saturating_add(self.maximum_break_glass_duration.as_secs());
        if expires_at <= now
            || expires_at > maximum
            || expires_at > break_glass.credential.descriptor.expires_at_unix_seconds
        {
            return Err(CredentialRotationError::InvalidBreakGlassWindow);
        }
        let generation = next_generation(state.generation)?;
        self.record_audit(CredentialAuditEvent {
            action: CredentialAuditAction::BreakGlassEnabled,
            outcome: CredentialAuditOutcome::Authorized,
            generation,
            credential_id: Some(break_glass.credential.descriptor.id.clone()),
            related_credential_id: None,
            break_glass_reason: Some(reason),
        })?;
        self.state.store(Arc::new(RotationState {
            generation,
            active: state.active.clone(),
            retiring: clone_retiring(&state.retiring),
            revoked: state.revoked.clone(),
            break_glass: Some(BreakGlassCredential {
                credential: break_glass.credential.clone(),
                activation: Some(BreakGlassActivation {
                    expires_at_unix_seconds: expires_at,
                    reason,
                }),
            }),
        }));
        Ok(generation)
    }

    pub fn disable_break_glass(&self) -> Result<u64, CredentialRotationError> {
        let _writer = self
            .writer
            .lock()
            .map_err(|_| CredentialRotationError::InvalidCredential)?;
        let state = self.state.load_full();
        let break_glass = state
            .break_glass
            .as_ref()
            .ok_or(CredentialRotationError::BreakGlassNotConfigured)?;
        let activation = break_glass
            .activation
            .as_ref()
            .ok_or(CredentialRotationError::BreakGlassDisabled)?;
        let generation = next_generation(state.generation)?;
        self.record_audit(CredentialAuditEvent {
            action: CredentialAuditAction::BreakGlassDisabled,
            outcome: CredentialAuditOutcome::Authorized,
            generation,
            credential_id: Some(break_glass.credential.descriptor.id.clone()),
            related_credential_id: None,
            break_glass_reason: Some(activation.reason),
        })?;
        self.state.store(Arc::new(RotationState {
            generation,
            active: state.active.clone(),
            retiring: clone_retiring(&state.retiring),
            revoked: state.revoked.clone(),
            break_glass: Some(BreakGlassCredential {
                credential: break_glass.credential.clone(),
                activation: None,
            }),
        }));
        Ok(generation)
    }

    /// Verifies proof against one immutable snapshot.
    pub fn verify(
        &self,
        credential_id: &CredentialId,
        proof: SecretMaterial,
        now: SystemTime,
    ) -> Result<CredentialVerification, CredentialRotationError> {
        let now = unix_seconds(now)?;
        let state = self.state.load();
        if state.revoked.contains(credential_id) {
            return Err(CredentialRotationError::CredentialRevoked);
        }
        let (credential, source) = if state.active.descriptor.id == *credential_id {
            (state.active.as_ref(), CredentialVerificationSource::Active)
        } else if let Some(retiring) = state.retiring.as_ref().filter(|retiring| {
            retiring.credential.descriptor.id == *credential_id && now < retiring.accept_until_unix_seconds
        }) {
            (retiring.credential.as_ref(), CredentialVerificationSource::Retiring)
        } else if let Some(break_glass) = state
            .break_glass
            .as_ref()
            .filter(|break_glass| break_glass.credential.descriptor.id == *credential_id)
        {
            let activation = break_glass
                .activation
                .as_ref()
                .ok_or(CredentialRotationError::BreakGlassDisabled)?;
            if now >= activation.expires_at_unix_seconds {
                return Err(CredentialRotationError::CredentialExpired);
            }
            (
                break_glass.credential.as_ref(),
                CredentialVerificationSource::BreakGlass,
            )
        } else {
            return Err(CredentialRotationError::CredentialNotAccepted);
        };
        if !credential.is_valid_at(now) {
            return Err(CredentialRotationError::CredentialExpired);
        }
        if !credential.verifies(&proof) {
            return Err(CredentialRotationError::InvalidProof);
        }
        Ok(CredentialVerification {
            credential_id: credential_id.clone(),
            generation: state.generation,
            source,
        })
    }

    fn audit_rejected_reload(&self) -> Result<(), CredentialRotationError> {
        let state = self.state.load();
        self.record_audit(CredentialAuditEvent {
            action: CredentialAuditAction::ReloadRejected,
            outcome: CredentialAuditOutcome::Rejected,
            generation: state.generation,
            credential_id: None,
            related_credential_id: Some(state.active.descriptor.id.clone()),
            break_glass_reason: None,
        })
    }

    fn record_audit(&self, event: CredentialAuditEvent) -> Result<(), CredentialRotationError> {
        self.audit
            .record(&event)
            .map_err(|_| CredentialRotationError::AuditUnavailable)
    }
}

impl fmt::Debug for CredentialRotationManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CredentialRotationManager")
            .field("snapshot", &self.snapshot())
            .field("audit", &"[INJECTED]")
            .finish()
    }
}

fn clone_retiring(value: &Option<RetiringCredential>) -> Option<RetiringCredential> {
    value.as_ref().map(|retiring| RetiringCredential {
        credential: retiring.credential.clone(),
        accept_until_unix_seconds: retiring.accept_until_unix_seconds,
    })
}

fn clone_break_glass(value: &Option<BreakGlassCredential>) -> Option<BreakGlassCredential> {
    value.as_ref().map(|break_glass| BreakGlassCredential {
        credential: break_glass.credential.clone(),
        activation: break_glass.activation.as_ref().map(|activation| BreakGlassActivation {
            expires_at_unix_seconds: activation.expires_at_unix_seconds,
            reason: activation.reason,
        }),
    })
}

fn next_generation(current: u64) -> Result<u64, CredentialRotationError> {
    current
        .checked_add(1)
        .ok_or(CredentialRotationError::GenerationExhausted)
}

fn unix_seconds(time: SystemTime) -> Result<u64, CredentialRotationError> {
    time.duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| CredentialRotationError::InvalidCredential)
}
