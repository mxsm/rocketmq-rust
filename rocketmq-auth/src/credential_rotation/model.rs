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
    Active,
    Retiring,
    BreakGlass,
}

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
    pub fn credential_id(&self) -> &CredentialId {
        &self.credential_id
    }

    pub const fn generation(&self) -> u64 {
        self.generation
    }

    pub const fn source(&self) -> CredentialVerificationSource {
        self.source
    }
}

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
    pub fn credential(&self) -> &CredentialDescriptor {
        &self.credential
    }

    pub const fn accept_until_unix_seconds(&self) -> u64 {
        self.accept_until_unix_seconds
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BreakGlassStatus {
    NotConfigured,
    Disabled {
        credential: CredentialDescriptor,
    },
    Enabled {
        credential: CredentialDescriptor,
        expires_at_unix_seconds: u64,
        reason: BreakGlassReason,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialRotationSnapshot {
    pub(super) generation: u64,
    pub(super) active: CredentialDescriptor,
    pub(super) retiring: Option<RetiringCredentialSnapshot>,
    pub(super) revoked: Vec<CredentialId>,
    pub(super) break_glass: BreakGlassStatus,
}

impl CredentialRotationSnapshot {
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    pub fn active(&self) -> &CredentialDescriptor {
        &self.active
    }

    pub fn retiring(&self) -> Option<&RetiringCredentialSnapshot> {
        self.retiring.as_ref()
    }

    pub fn revoked(&self) -> &[CredentialId] {
        &self.revoked
    }

    pub const fn break_glass(&self) -> &BreakGlassStatus {
        &self.break_glass
    }
}
