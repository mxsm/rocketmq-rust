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

//! Fail-closed one-time administrator bootstrap over a verified TLS listener.

mod state_file;

use std::fmt;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use cheetah_string::CheetahString;
use rocketmq_security_api::SecretMaterial;
use sha2::Digest;
use sha2::Sha256;
use subtle::ConstantTimeEq;
use thiserror::Error;

use state_file::BootstrapStateRecord;
use state_file::BootstrapStateStore;

const MINIMUM_BOOTSTRAP_PROOF_BYTES: usize = 32;

/// Grant metadata and a one-way proof digest. Raw proof material is discarded at construction.
#[derive(Clone)]
pub struct BootstrapGrant {
    cluster_id: CheetahString,
    listener: CheetahString,
    expires_at_unix_seconds: u64,
    proof_digest: [u8; 32],
}

impl BootstrapGrant {
    /// Creates a cluster/listener/expiry-bound grant from high-entropy proof material.
    ///
    /// # Errors
    ///
    /// Returns [`BootstrapError::InvalidConfiguration`] for invalid bindings, timestamps, or
    /// proof material shorter than 32 bytes.
    pub fn new(
        cluster_id: impl Into<CheetahString>,
        listener: impl Into<CheetahString>,
        expires_at: SystemTime,
        proof: SecretMaterial,
    ) -> Result<Self, BootstrapError> {
        let cluster_id = cluster_id.into();
        let listener = listener.into();
        validate_binding(&cluster_id)?;
        validate_binding(&listener)?;
        if proof.len() < MINIMUM_BOOTSTRAP_PROOF_BYTES {
            return Err(BootstrapError::InvalidConfiguration);
        }
        let expires_at_unix_seconds = unix_seconds(expires_at)?;
        let proof_digest = Sha256::digest(proof.expose_secret()).into();
        Ok(Self {
            cluster_id,
            listener,
            expires_at_unix_seconds,
            proof_digest,
        })
    }

    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    pub fn listener(&self) -> &str {
        &self.listener
    }

    pub const fn expires_at_unix_seconds(&self) -> u64 {
        self.expires_at_unix_seconds
    }

    fn grant_id(&self) -> String {
        hex::encode(self.proof_digest)
    }
}

impl fmt::Debug for BootstrapGrant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BootstrapGrant")
            .field("cluster_id", &self.cluster_id)
            .field("listener", &self.listener)
            .field("expires_at_unix_seconds", &self.expires_at_unix_seconds)
            .field("proof_digest", &"[REDACTED]")
            .finish()
    }
}

/// Non-secret first-administrator mTLS identity selected by the enrollment request.
#[derive(Clone, PartialEq, Eq)]
pub struct BootstrapAdminIdentity {
    principal_id: CheetahString,
    certificate_sha256: [u8; 32],
}

impl BootstrapAdminIdentity {
    /// Validates a principal and certificate fingerprint.
    ///
    /// # Errors
    ///
    /// Returns [`BootstrapError::InvalidConfiguration`] for an invalid principal or all-zero
    /// certificate fingerprint.
    pub fn new(principal_id: impl Into<CheetahString>, certificate_sha256: [u8; 32]) -> Result<Self, BootstrapError> {
        let principal_id = principal_id.into();
        validate_binding(&principal_id)?;
        if certificate_sha256.iter().all(|byte| *byte == 0) {
            return Err(BootstrapError::InvalidConfiguration);
        }
        Ok(Self {
            principal_id,
            certificate_sha256,
        })
    }

    pub fn principal_id(&self) -> &str {
        &self.principal_id
    }

    pub const fn certificate_sha256(&self) -> &[u8; 32] {
        &self.certificate_sha256
    }
}

impl fmt::Debug for BootstrapAdminIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BootstrapAdminIdentity")
            .field("principal_id", &self.principal_id)
            .field("certificate_sha256", &hex::encode(self.certificate_sha256))
            .finish()
    }
}

/// Authenticated listener metadata supplied by the transport boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapTransportContext {
    cluster_id: CheetahString,
    listener: CheetahString,
    verified_tls: bool,
}

impl BootstrapTransportContext {
    /// Creates an unverified context. The transport must explicitly attest TLS verification.
    ///
    /// # Errors
    ///
    /// Returns [`BootstrapError::InvalidConfiguration`] for invalid bindings.
    pub fn new(
        cluster_id: impl Into<CheetahString>,
        listener: impl Into<CheetahString>,
    ) -> Result<Self, BootstrapError> {
        let cluster_id = cluster_id.into();
        let listener = listener.into();
        validate_binding(&cluster_id)?;
        validate_binding(&listener)?;
        Ok(Self {
            cluster_id,
            listener,
            verified_tls: false,
        })
    }

    pub fn with_verified_tls(mut self) -> Self {
        self.verified_tls = true;
        self
    }
}

/// Enrollment input. Proof material is zeroized when this value leaves scope.
pub struct BootstrapEnrollmentRequest {
    transport: BootstrapTransportContext,
    proof: SecretMaterial,
    identity: BootstrapAdminIdentity,
}

impl BootstrapEnrollmentRequest {
    pub fn new(transport: BootstrapTransportContext, proof: SecretMaterial, identity: BootstrapAdminIdentity) -> Self {
        Self {
            transport,
            proof,
            identity,
        }
    }
}

impl fmt::Debug for BootstrapEnrollmentRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BootstrapEnrollmentRequest")
            .field("transport", &self.transport)
            .field("proof", &"[REDACTED]")
            .field("identity", &self.identity)
            .finish()
    }
}

/// Opaque provisioning failure returned by the injected first-admin store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum BootstrapAdminProvisioningError {
    #[error("administrator identity was rejected")]
    Rejected,
    #[error("administrator identity store is unavailable")]
    Unavailable,
}

/// Injection boundary that must atomically reject creation when an administrator already exists.
pub trait BootstrapAdminProvisioner: Send + Sync {
    fn create_first_admin(&self, identity: &BootstrapAdminIdentity) -> Result<(), BootstrapAdminProvisioningError>;
}

/// Persisted lifecycle of a one-time bootstrap grant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootstrapStatus {
    Available,
    Claimed,
    Consumed,
}

/// Successful enrollment result without credential or proof material.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapEnrollmentResult {
    principal_id: CheetahString,
    status: BootstrapStatus,
}

impl BootstrapEnrollmentResult {
    pub fn principal_id(&self) -> &str {
        &self.principal_id
    }

    pub const fn status(&self) -> BootstrapStatus {
        self.status
    }
}

/// Stable, redacted bootstrap failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum BootstrapError {
    #[error("bootstrap configuration is invalid")]
    InvalidConfiguration,
    #[error("bootstrap state is unavailable")]
    StateUnavailable,
    #[error("bootstrap state permissions are not owner-only")]
    InsecurePermissions,
    #[error("bootstrap persistence is unsupported on this platform")]
    UnsupportedPlatform,
    #[error("bootstrap proof is invalid")]
    InvalidProof,
    #[error("bootstrap grant has expired")]
    Expired,
    #[error("bootstrap request binding does not match the grant")]
    BindingMismatch,
    #[error("bootstrap requires a verified TLS listener")]
    TlsRequired,
    #[error("bootstrap grant has already been claimed")]
    AlreadyClaimed,
    #[error("bootstrap grant has already been consumed")]
    AlreadyConsumed,
    #[error("first administrator provisioning failed")]
    AdminProvisioningFailed,
}

/// Persistent, fail-closed one-time bootstrap coordinator.
pub struct OneTimeBootstrap {
    grant: BootstrapGrant,
    store: BootstrapStateStore,
    state: Mutex<BootstrapStateRecord>,
}

impl OneTimeBootstrap {
    /// Opens or creates owner-only state. Windows fails closed until ACL verification is available.
    ///
    /// # Errors
    ///
    /// Returns a redacted [`BootstrapError`] when state cannot be validated or persisted safely.
    pub fn open(grant: BootstrapGrant, state_path: impl Into<PathBuf>) -> Result<Self, BootstrapError> {
        let initial = BootstrapStateRecord::available(&grant);
        let (store, state) = BootstrapStateStore::open(state_path.into(), initial)?;
        Ok(Self {
            grant,
            store,
            state: Mutex::new(state),
        })
    }

    pub fn status(&self) -> Result<BootstrapStatus, BootstrapError> {
        self.state
            .lock()
            .map(|state| state.status())
            .map_err(|_| BootstrapError::StateUnavailable)
    }

    pub fn is_open(&self, now: SystemTime) -> Result<bool, BootstrapError> {
        let now = unix_seconds(now)?;
        Ok(self.status()? == BootstrapStatus::Available
            && now < self.grant.expires_at_unix_seconds
            && !self.store.claim_exists()?)
    }

    /// Claims the grant before invoking the administrator store and never reopens it after a
    /// provisioning or final-persistence failure.
    ///
    /// # Errors
    ///
    /// Returns a redacted [`BootstrapError`] for transport, proof, expiry, replay, persistence, or
    /// provisioning failures.
    pub fn enroll(
        &self,
        request: BootstrapEnrollmentRequest,
        now: SystemTime,
        provisioner: &dyn BootstrapAdminProvisioner,
    ) -> Result<BootstrapEnrollmentResult, BootstrapError> {
        let mut state = self.state.lock().map_err(|_| BootstrapError::StateUnavailable)?;
        match state.status() {
            BootstrapStatus::Claimed => return Err(BootstrapError::AlreadyClaimed),
            BootstrapStatus::Consumed => return Err(BootstrapError::AlreadyConsumed),
            BootstrapStatus::Available => {}
        }
        let now = unix_seconds(now)?;
        if now >= self.grant.expires_at_unix_seconds {
            return Err(BootstrapError::Expired);
        }
        if !request.transport.verified_tls {
            return Err(BootstrapError::TlsRequired);
        }
        if request.transport.cluster_id != self.grant.cluster_id || request.transport.listener != self.grant.listener {
            return Err(BootstrapError::BindingMismatch);
        }
        let presented_digest: [u8; 32] = Sha256::digest(request.proof.expose_secret()).into();
        if self.grant.proof_digest.ct_eq(&presented_digest).unwrap_u8() != 1 {
            return Err(BootstrapError::InvalidProof);
        }

        let claimed = state.claimed(&request.identity);
        self.store.claim(&claimed)?;
        *state = claimed.clone();
        drop(state);
        provisioner
            .create_first_admin(&request.identity)
            .map_err(|_| BootstrapError::AdminProvisioningFailed)?;

        let consumed = claimed.consumed(&request.identity);
        self.store.consume(&consumed)?;
        let mut state = self.state.lock().map_err(|_| BootstrapError::StateUnavailable)?;
        *state = consumed;
        Ok(BootstrapEnrollmentResult {
            principal_id: request.identity.principal_id,
            status: BootstrapStatus::Consumed,
        })
    }
}

impl fmt::Debug for OneTimeBootstrap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OneTimeBootstrap")
            .field("grant", &self.grant)
            .field("state_path", &"[REDACTED]")
            .field("status", &self.status().ok())
            .finish()
    }
}

fn validate_binding(value: &str) -> Result<(), BootstrapError> {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed.len() > 256 || trimmed.chars().any(char::is_control) {
        return Err(BootstrapError::InvalidConfiguration);
    }
    Ok(())
}

fn unix_seconds(time: SystemTime) -> Result<u64, BootstrapError> {
    time.duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| BootstrapError::InvalidConfiguration)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn material(bytes: &[u8]) -> SecretMaterial {
        SecretMaterial::new(bytes.to_vec()).unwrap()
    }

    #[test]
    fn grant_rejects_short_proof_and_redacts_digest() {
        let expires = SystemTime::now() + Duration::from_secs(60);
        assert_eq!(
            BootstrapGrant::new("cluster", "127.0.0.1:9876", expires, material(b"short")).unwrap_err(),
            BootstrapError::InvalidConfiguration
        );
        let grant = BootstrapGrant::new("cluster", "127.0.0.1:9876", expires, material(&[7; 32])).unwrap();
        let debug = format!("{grant:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains(&hex::encode([7; 32])));
    }
}
