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

use super::BreakGlassReason;
use super::CredentialDescriptor;
use super::CredentialId;

/// Request-path source that accepted a proof.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialVerificationSource {
    /// The active credential accepted the proof.
    Active,
    /// The retiring credential accepted the proof during its overlap window.
    Retiring,
    /// The enabled emergency credential accepted the proof.
    BreakGlass,
}

/// Accepted proof, identifying the credential, rotation generation, and source.
#[derive(Clone, PartialEq, Eq)]
pub struct CredentialVerification {
    pub(super) credential_id: CredentialId,
    pub(super) generation: u64,
    pub(super) source: CredentialVerificationSource,
}

impl std::fmt::Debug for CredentialVerification {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CredentialVerification")
            .field("credential_id", &"[REDACTED]")
            .field("generation", &self.generation)
            .field("source", &self.source)
            .finish()
    }
}

impl CredentialVerification {
    /// Returns the identifier of the credential that accepted the proof.
    pub fn credential_id(&self) -> &CredentialId {
        &self.credential_id
    }

    /// Returns the rotation generation used to verify the proof.
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Returns the credential source that accepted the proof.
    pub const fn source(&self) -> CredentialVerificationSource {
        self.source
    }
}

/// Previous active credential retained until the overlap window ends.
#[derive(Clone, PartialEq, Eq)]
pub struct RetiringCredentialSnapshot {
    pub(super) credential: CredentialDescriptor,
    pub(super) accept_until_unix_seconds: u64,
}

impl std::fmt::Debug for RetiringCredentialSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetiringCredentialSnapshot")
            .field("credential", &"[REDACTED]")
            .field("accept_until_unix_seconds", &self.accept_until_unix_seconds)
            .finish()
    }
}

impl RetiringCredentialSnapshot {
    /// Returns the retiring credential's descriptor without its secret material.
    pub fn credential(&self) -> &CredentialDescriptor {
        &self.credential
    }

    /// Returns the exclusive end of the overlap window, in Unix seconds.
    pub const fn accept_until_unix_seconds(&self) -> u64 {
        self.accept_until_unix_seconds
    }
}

/// Activation state of the emergency credential in a rotation snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BreakGlassStatus {
    /// No emergency credential is configured.
    NotConfigured,
    /// An emergency credential is configured but has no active enablement.
    Disabled {
        /// Descriptor of the configured emergency credential.
        credential: CredentialDescriptor,
    },
    /// An emergency credential has an enablement with an expiry and audit reason.
    Enabled {
        /// Descriptor of the enabled emergency credential.
        credential: CredentialDescriptor,
        /// Exclusive enablement expiry, in Unix seconds.
        expires_at_unix_seconds: u64,
        /// Recorded reason for enabling emergency access.
        reason: BreakGlassReason,
    },
}

/// Immutable view of one credential-rotation generation without secret material.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialRotationSnapshot {
    pub(super) generation: u64,
    pub(super) active: CredentialDescriptor,
    pub(super) retiring: Option<RetiringCredentialSnapshot>,
    pub(super) revoked: Vec<CredentialId>,
    pub(super) break_glass: BreakGlassStatus,
}

impl CredentialRotationSnapshot {
    /// Returns the generation shared by all state in this snapshot.
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Returns the active credential's descriptor.
    pub fn active(&self) -> &CredentialDescriptor {
        &self.active
    }

    /// Returns the previous active credential and overlap deadline, if retained.
    pub fn retiring(&self) -> Option<&RetiringCredentialSnapshot> {
        self.retiring.as_ref()
    }

    /// Returns credential identifiers denied verification in this generation.
    pub fn revoked(&self) -> &[CredentialId] {
        &self.revoked
    }

    /// Returns the configured emergency credential's activation state.
    pub const fn break_glass(&self) -> &BreakGlassStatus {
        &self.break_glass
    }
}
