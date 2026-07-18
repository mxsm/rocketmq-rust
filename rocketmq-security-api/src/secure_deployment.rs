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

//! Pure profile resolution and readiness contracts for secure deployments.

use std::fs::File;
use std::path::Path;
use std::str::FromStr;
use std::time::SystemTime;

use thiserror::Error;

use crate::DeploymentProfile;

/// Whether configuration belongs to a newly created or already deployed installation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeploymentOrigin {
    New,
    Existing,
}

/// Profile input before the compatibility-preserving default is resolved.
#[derive(Debug, Clone, Copy)]
pub struct SecurityProfileSelection<'a> {
    origin: DeploymentOrigin,
    configured_profile: Option<&'a str>,
}

impl<'a> SecurityProfileSelection<'a> {
    pub const fn new(origin: DeploymentOrigin) -> Self {
        Self {
            origin,
            configured_profile: None,
        }
    }

    pub const fn with_configured_profile(mut self, profile: &'a str) -> Self {
        self.configured_profile = Some(profile);
        self
    }
}

/// Migration action produced alongside the effective profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecurityMigrationStatus {
    NotRequired,
    CompatibilityProfileMustBePersisted,
    MigrationToSecurePending,
}

/// Effective profile and an explicit compatibility migration report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SecurityProfileResolution {
    profile: DeploymentProfile,
    migration_status: SecurityMigrationStatus,
}

impl SecurityProfileResolution {
    pub const fn profile(self) -> DeploymentProfile {
        self.profile
    }

    pub const fn migration_status(self) -> SecurityMigrationStatus {
        self.migration_status
    }
}

/// Invalid configured profile. Unknown values never downgrade to compatibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum SecurityProfileSelectionError {
    #[error("configured security profile is unknown")]
    UnknownProfile,
}

/// Resolves a profile without silently changing an identified existing deployment.
///
/// # Errors
///
/// Returns [`SecurityProfileSelectionError::UnknownProfile`] for every non-empty unknown value.
pub fn resolve_security_profile(
    selection: SecurityProfileSelection<'_>,
) -> Result<SecurityProfileResolution, SecurityProfileSelectionError> {
    let configured = selection
        .configured_profile
        .map(str::trim)
        .filter(|profile| !profile.is_empty());
    let profile = match configured {
        Some(profile) => {
            DeploymentProfile::from_str(profile).map_err(|()| SecurityProfileSelectionError::UnknownProfile)?
        }
        None if selection.origin == DeploymentOrigin::New => DeploymentProfile::Secure,
        None => DeploymentProfile::Compatibility,
    };
    let migration_status = match (selection.origin, configured, profile) {
        (DeploymentOrigin::Existing, None, DeploymentProfile::Compatibility) => {
            SecurityMigrationStatus::CompatibilityProfileMustBePersisted
        }
        (_, _, DeploymentProfile::Compatibility | DeploymentProfile::Development) => {
            SecurityMigrationStatus::MigrationToSecurePending
        }
        _ => SecurityMigrationStatus::NotRequired,
    };
    Ok(SecurityProfileResolution {
        profile,
        migration_status,
    })
}

/// Readiness inputs for the one-time-token bootstrap path.
#[derive(Debug, Clone, Copy)]
pub struct BootstrapReadinessView {
    expires_at: SystemTime,
    material_available: bool,
    verified_tls_listener: bool,
}

impl BootstrapReadinessView {
    pub const fn new(expires_at: SystemTime) -> Self {
        Self {
            expires_at,
            material_available: false,
            verified_tls_listener: false,
        }
    }

    pub const fn with_available_material(mut self) -> Self {
        self.material_available = true;
        self
    }

    pub const fn with_verified_tls_listener(mut self) -> Self {
        self.verified_tls_listener = true;
        self
    }
}

/// Complete pure input used before a service binds its data or management listeners.
#[derive(Debug, Clone, Copy)]
pub struct DeploymentSecurityConfigView<'a> {
    profile: SecurityProfileSelection<'a>,
    trust_anchor: Option<&'a Path>,
    secret_provider_registered: bool,
    provisioned_admin_identity: bool,
    one_time_bootstrap: Option<BootstrapReadinessView>,
    insecure_downgrade: bool,
}

impl<'a> DeploymentSecurityConfigView<'a> {
    pub const fn new(profile: SecurityProfileSelection<'a>) -> Self {
        Self {
            profile,
            trust_anchor: None,
            secret_provider_registered: false,
            provisioned_admin_identity: false,
            one_time_bootstrap: None,
            insecure_downgrade: false,
        }
    }

    pub const fn with_trust_anchor(mut self, path: &'a Path) -> Self {
        self.trust_anchor = Some(path);
        self
    }

    pub const fn with_registered_secret_provider(mut self) -> Self {
        self.secret_provider_registered = true;
        self
    }

    pub const fn with_provisioned_admin_identity(mut self) -> Self {
        self.provisioned_admin_identity = true;
        self
    }

    pub const fn with_one_time_bootstrap(mut self, bootstrap: BootstrapReadinessView) -> Self {
        self.one_time_bootstrap = Some(bootstrap);
        self
    }

    pub const fn with_insecure_downgrade(mut self) -> Self {
        self.insecure_downgrade = true;
        self
    }
}

/// Stable reasons a resolved deployment is not safe to serve traffic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DeploymentSecurityFailure {
    MissingTrustAnchor,
    TrustAnchorUnavailable,
    TrustAnchorNotRegularFile,
    MissingSecretProvider,
    MissingIdentityBootstrap,
    MultipleIdentityBootstrapSources,
    MissingBootstrapMaterial,
    ExpiredBootstrap,
    BootstrapListenerNotTls,
    InsecureDowngrade,
}

/// Resolved profile, migration report, and fail-closed readiness failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeploymentSecurityReport {
    resolution: SecurityProfileResolution,
    failures: Vec<DeploymentSecurityFailure>,
}

impl DeploymentSecurityReport {
    pub fn is_ready(&self) -> bool {
        self.failures.is_empty()
    }

    pub const fn resolution(&self) -> SecurityProfileResolution {
        self.resolution
    }

    pub fn failures(&self) -> &[DeploymentSecurityFailure] {
        &self.failures
    }
}

/// Resolves the effective profile and validates the secure identity bootstrap prerequisites.
///
/// # Errors
///
/// Returns [`SecurityProfileSelectionError::UnknownProfile`] instead of choosing a fallback.
pub fn validate_deployment_security(
    view: DeploymentSecurityConfigView<'_>,
    now: SystemTime,
) -> Result<DeploymentSecurityReport, SecurityProfileSelectionError> {
    let resolution = resolve_security_profile(view.profile)?;
    let mut failures = Vec::new();
    if resolution.profile == DeploymentProfile::Secure {
        inspect_trust_anchor(view.trust_anchor, &mut failures);
        if !view.secret_provider_registered {
            failures.push(DeploymentSecurityFailure::MissingSecretProvider);
        }
        match (view.provisioned_admin_identity, view.one_time_bootstrap) {
            (false, None) => failures.push(DeploymentSecurityFailure::MissingIdentityBootstrap),
            (true, Some(_)) => failures.push(DeploymentSecurityFailure::MultipleIdentityBootstrapSources),
            _ => {}
        }
        if let Some(bootstrap) = view.one_time_bootstrap {
            if !bootstrap.material_available {
                failures.push(DeploymentSecurityFailure::MissingBootstrapMaterial);
            }
            if bootstrap.expires_at <= now {
                failures.push(DeploymentSecurityFailure::ExpiredBootstrap);
            }
            if !bootstrap.verified_tls_listener {
                failures.push(DeploymentSecurityFailure::BootstrapListenerNotTls);
            }
        }
        if view.insecure_downgrade {
            failures.push(DeploymentSecurityFailure::InsecureDowngrade);
        }
    }
    Ok(DeploymentSecurityReport { resolution, failures })
}

fn inspect_trust_anchor(path: Option<&Path>, failures: &mut Vec<DeploymentSecurityFailure>) {
    let Some(path) = path else {
        failures.push(DeploymentSecurityFailure::MissingTrustAnchor);
        return;
    };
    let Ok(path_metadata) = path.metadata() else {
        failures.push(DeploymentSecurityFailure::TrustAnchorUnavailable);
        return;
    };
    if !path_metadata.is_file() {
        failures.push(DeploymentSecurityFailure::TrustAnchorNotRegularFile);
        return;
    }
    let Ok(file) = File::open(path) else {
        failures.push(DeploymentSecurityFailure::TrustAnchorUnavailable);
        return;
    };
    match file.metadata() {
        Ok(metadata) if metadata.is_file() => {}
        Ok(_) => failures.push(DeploymentSecurityFailure::TrustAnchorNotRegularFile),
        Err(_) => failures.push(DeploymentSecurityFailure::TrustAnchorUnavailable),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tempfile::NamedTempFile;

    use super::*;

    #[test]
    fn new_defaults_secure_while_existing_defaults_compatibility_with_report() {
        let new = resolve_security_profile(SecurityProfileSelection::new(DeploymentOrigin::New)).unwrap();
        assert_eq!(new.profile(), DeploymentProfile::Secure);
        assert_eq!(new.migration_status(), SecurityMigrationStatus::NotRequired);

        let existing = resolve_security_profile(SecurityProfileSelection::new(DeploymentOrigin::Existing)).unwrap();
        assert_eq!(existing.profile(), DeploymentProfile::Compatibility);
        assert_eq!(
            existing.migration_status(),
            SecurityMigrationStatus::CompatibilityProfileMustBePersisted
        );
    }

    #[test]
    fn unknown_profile_never_downgrades() {
        let selection = SecurityProfileSelection::new(DeploymentOrigin::Existing).with_configured_profile("unknown");
        assert_eq!(
            resolve_security_profile(selection).unwrap_err(),
            SecurityProfileSelectionError::UnknownProfile
        );
    }

    #[test]
    fn secure_provisioned_identity_requires_trust_and_provider() {
        let now = SystemTime::now();
        let missing = validate_deployment_security(
            DeploymentSecurityConfigView::new(SecurityProfileSelection::new(DeploymentOrigin::New))
                .with_provisioned_admin_identity(),
            now,
        )
        .unwrap();
        assert_eq!(
            missing.failures(),
            &[
                DeploymentSecurityFailure::MissingTrustAnchor,
                DeploymentSecurityFailure::MissingSecretProvider,
            ]
        );

        let trust_anchor = NamedTempFile::new().unwrap();
        let ready = validate_deployment_security(
            DeploymentSecurityConfigView::new(SecurityProfileSelection::new(DeploymentOrigin::New))
                .with_trust_anchor(trust_anchor.path())
                .with_registered_secret_provider()
                .with_provisioned_admin_identity(),
            now,
        )
        .unwrap();
        assert!(ready.is_ready());
    }

    #[test]
    fn token_bootstrap_requires_one_source_material_expiry_and_tls() {
        let now = SystemTime::now();
        let trust_anchor = NamedTempFile::new().unwrap();
        let invalid = validate_deployment_security(
            DeploymentSecurityConfigView::new(SecurityProfileSelection::new(DeploymentOrigin::New))
                .with_trust_anchor(trust_anchor.path())
                .with_registered_secret_provider()
                .with_provisioned_admin_identity()
                .with_one_time_bootstrap(BootstrapReadinessView::new(now - Duration::from_secs(1))),
            now,
        )
        .unwrap();
        assert_eq!(
            invalid.failures(),
            &[
                DeploymentSecurityFailure::MultipleIdentityBootstrapSources,
                DeploymentSecurityFailure::MissingBootstrapMaterial,
                DeploymentSecurityFailure::ExpiredBootstrap,
                DeploymentSecurityFailure::BootstrapListenerNotTls,
            ]
        );

        let ready = validate_deployment_security(
            DeploymentSecurityConfigView::new(SecurityProfileSelection::new(DeploymentOrigin::New))
                .with_trust_anchor(trust_anchor.path())
                .with_registered_secret_provider()
                .with_one_time_bootstrap(
                    BootstrapReadinessView::new(now + Duration::from_secs(60))
                        .with_available_material()
                        .with_verified_tls_listener(),
                ),
            now,
        )
        .unwrap();
        assert!(ready.is_ready());
    }

    #[test]
    fn compatibility_is_ready_without_secure_material_but_reports_migration() {
        let report = validate_deployment_security(
            DeploymentSecurityConfigView::new(SecurityProfileSelection::new(DeploymentOrigin::Existing)),
            SystemTime::now(),
        )
        .unwrap();
        assert!(report.is_ready());
        assert_eq!(
            report.resolution().migration_status(),
            SecurityMigrationStatus::CompatibilityProfileMustBePersisted
        );
    }
}
