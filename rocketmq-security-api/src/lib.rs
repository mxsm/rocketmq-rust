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

//! Runtime-neutral security contracts.

use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::io;
use std::io::Read;
use std::net::SocketAddr;
use std::path::Path;
use std::str::FromStr;
use std::time::SystemTime;

use cheetah_string::CheetahString;
use thiserror::Error;

/// Read-only peer metadata made available to security policies.
#[derive(Clone, PartialEq, Eq)]
pub struct PeerInfo {
    address: SocketAddr,
    tls: bool,
    certificate_subject: Option<CheetahString>,
}

impl PeerInfo {
    pub fn new(address: SocketAddr, tls: bool) -> Self {
        Self {
            address,
            tls,
            certificate_subject: None,
        }
    }

    pub fn with_certificate_subject(mut self, subject: impl Into<CheetahString>) -> Self {
        self.certificate_subject = Some(subject.into());
        self
    }

    pub fn address(&self) -> SocketAddr {
        self.address
    }

    pub fn is_tls(&self) -> bool {
        self.tls
    }

    pub fn certificate_subject(&self) -> Option<&str> {
        self.certificate_subject.as_deref()
    }
}

impl fmt::Debug for PeerInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PeerInfo")
            .field("address", &self.address)
            .field("tls", &self.tls)
            .field(
                "certificate_subject",
                &self.certificate_subject.as_ref().map(|_| "[REDACTED]"),
            )
            .finish()
    }
}

/// Minimal borrowed projection of an inbound or outbound request.
#[derive(Clone, Copy)]
pub struct SecurityRequestView<'a> {
    code: i32,
    version: i32,
    fields: &'a HashMap<CheetahString, CheetahString>,
    body: Option<&'a [u8]>,
    peer: Option<&'a PeerInfo>,
}

impl<'a> SecurityRequestView<'a> {
    pub fn new(
        code: i32,
        version: i32,
        fields: &'a HashMap<CheetahString, CheetahString>,
        body: Option<&'a [u8]>,
        peer: Option<&'a PeerInfo>,
    ) -> Self {
        Self {
            code,
            version,
            fields,
            body,
            peer,
        }
    }

    pub fn code(&self) -> i32 {
        self.code
    }

    pub fn version(&self) -> i32 {
        self.version
    }

    pub fn fields(&self) -> &'a HashMap<CheetahString, CheetahString> {
        self.fields
    }

    pub fn body(&self) -> Option<&'a [u8]> {
        self.body
    }

    pub fn peer(&self) -> Option<&'a PeerInfo> {
        self.peer
    }
}

impl fmt::Debug for SecurityRequestView<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SecurityRequestView")
            .field("code", &self.code)
            .field("version", &self.version)
            .field("field_count", &self.fields.len())
            .field("body_len", &self.body.map(<[u8]>::len))
            .field("peer", &self.peer)
            .finish()
    }
}

/// Authenticated identity without credential material.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Principal {
    id: CheetahString,
}

impl Principal {
    pub fn new(id: impl Into<CheetahString>) -> Self {
        Self { id: id.into() }
    }

    pub fn id(&self) -> &str {
        &self.id
    }
}

/// Protected resource category.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResourceKind {
    Topic,
    ConsumerGroup,
    Cluster,
    Broker,
    Other,
}

/// Resource selected for a policy decision.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Resource {
    kind: ResourceKind,
    name: CheetahString,
}

impl Resource {
    pub fn new(kind: ResourceKind, name: impl Into<CheetahString>) -> Self {
        Self {
            kind,
            name: name.into(),
        }
    }

    pub fn topic(name: impl Into<CheetahString>) -> Self {
        Self::new(ResourceKind::Topic, name)
    }

    pub fn kind(&self) -> ResourceKind {
        self.kind
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Security-relevant operation requested on a resource.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Action {
    Publish,
    Subscribe,
    Create,
    Update,
    Delete,
    Describe,
    Manage,
}

/// Final authorization decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    Allow,
    Deny { reason: CheetahString },
}

impl Decision {
    pub fn deny(reason: impl Into<CheetahString>) -> Self {
        Self::Deny { reason: reason.into() }
    }
}

/// Complete security input before authentication has been required.
pub struct RequestContext<'a> {
    request: SecurityRequestView<'a>,
    principal: Option<&'a Principal>,
    resource: Resource,
    action: Action,
}

impl<'a> RequestContext<'a> {
    pub fn new(
        request: SecurityRequestView<'a>,
        principal: Option<&'a Principal>,
        resource: Resource,
        action: Action,
    ) -> Self {
        Self {
            request,
            principal,
            resource,
            action,
        }
    }

    pub fn principal(&self) -> Option<&Principal> {
        self.principal
    }
}

/// Policy input for which the principal invariant has been established.
#[derive(Clone, Copy)]
pub struct AuthenticatedRequestContext<'a> {
    request: SecurityRequestView<'a>,
    principal: &'a Principal,
    resource: &'a Resource,
    action: Action,
}

impl<'a> AuthenticatedRequestContext<'a> {
    pub fn request(&self) -> SecurityRequestView<'a> {
        self.request
    }

    pub fn principal(&self) -> &'a Principal {
        self.principal
    }

    pub fn resource(&self) -> &'a Resource {
        self.resource
    }

    pub fn action(&self) -> Action {
        self.action
    }
}

/// Authorization policy that can only evaluate authenticated requests.
pub trait RequestPolicy: Send + Sync {
    fn evaluate_authenticated(&self, context: AuthenticatedRequestContext<'_>) -> Decision;
}

/// Applies the mandatory fail-closed identity gate before invoking a policy.
pub fn evaluate_request(policy: &dyn RequestPolicy, context: &RequestContext<'_>) -> Decision {
    let Some(principal) = context.principal else {
        return Decision::deny("missing authenticated principal");
    };
    policy.evaluate_authenticated(AuthenticatedRequestContext {
        request: context.request,
        principal,
        resource: &context.resource,
        action: context.action,
    })
}

/// Error returned by an outbound signing adapter.
#[derive(Debug, Error)]
pub enum SigningError {
    #[error("signing credentials are unavailable")]
    CredentialsUnavailable,
    #[error("request signing failed: {0}")]
    Failed(CheetahString),
}

/// Redacted signature fields produced by an outbound signer.
pub struct Signature {
    fields: Vec<(CheetahString, Secret<CheetahString>)>,
}

impl Signature {
    pub fn new(fields: Vec<(CheetahString, Secret<CheetahString>)>) -> Self {
        Self { fields }
    }

    pub fn fields(&self) -> &[(CheetahString, Secret<CheetahString>)] {
        &self.fields
    }
}

impl fmt::Debug for Signature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Signature")
            .field("field_count", &self.fields.len())
            .finish()
    }
}

/// Contract implemented by transport-specific outbound signers in `rocketmq-auth`.
pub trait OutboundSigner: Send + Sync {
    fn sign(&self, request: SecurityRequestView<'_>) -> Result<Signature, SigningError>;
}

/// Sensitive value whose debug representation is always redacted.
pub struct Secret<T>(T);

impl<T> Secret<T> {
    pub fn new(value: T) -> Self {
        Self(value)
    }

    pub fn expose_secret(&self) -> &T {
        &self.0
    }

    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> fmt::Debug for Secret<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[REDACTED]")
    }
}

/// Deployment security profile selected by configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeploymentProfile {
    Development,
    Compatibility,
    Secure,
}

impl FromStr for DeploymentProfile {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "development" => Ok(Self::Development),
            "compatibility" => Ok(Self::Compatibility),
            "secure" => Ok(Self::Secure),
            _ => Err(()),
        }
    }
}

/// Borrowed configuration input for a side-effect-free readiness check.
#[derive(Debug, Clone, Copy)]
pub struct SecurityConfigView<'a> {
    profile: &'a str,
    trust_anchor: Option<&'a Path>,
    secret_file: Option<&'a Path>,
    bootstrap_expires_at: Option<SystemTime>,
    insecure_downgrade: bool,
}

impl<'a> SecurityConfigView<'a> {
    pub fn new(profile: &'a str) -> Self {
        Self {
            profile,
            trust_anchor: None,
            secret_file: None,
            bootstrap_expires_at: None,
            insecure_downgrade: false,
        }
    }

    pub fn with_trust_anchor(mut self, path: &'a Path) -> Self {
        self.trust_anchor = Some(path);
        self
    }

    pub fn with_secret_file(mut self, path: &'a Path) -> Self {
        self.secret_file = Some(path);
        self
    }

    pub fn with_bootstrap_expiry(mut self, expires_at: SystemTime) -> Self {
        self.bootstrap_expires_at = Some(expires_at);
        self
    }

    pub fn with_insecure_downgrade(mut self, enabled: bool) -> Self {
        self.insecure_downgrade = enabled;
        self
    }
}

/// Stable reasons why a profile is not ready.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SecurityReadinessFailure {
    UnknownProfile,
    MissingTrustAnchor,
    TrustAnchorUnavailable,
    TrustAnchorNotRegularFile,
    MissingSecretFile,
    SecretFileUnavailable,
    SecretFileNotRegularFile,
    InsecureSecretFilePermissions,
    MissingBootstrapExpiry,
    ExpiredBootstrap,
    InsecureDowngrade,
}

/// Result of a side-effect-free security configuration dry run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecurityReadinessReport {
    profile: Option<DeploymentProfile>,
    failures: Vec<SecurityReadinessFailure>,
}

impl SecurityReadinessReport {
    pub fn is_ready(&self) -> bool {
        self.failures.is_empty()
    }

    pub fn profile(&self) -> Option<DeploymentProfile> {
        self.profile
    }

    pub fn failures(&self) -> &[SecurityReadinessFailure] {
        &self.failures
    }
}

/// Validates a configuration snapshot without reading file contents or writing external state.
pub fn validate_security_config(view: SecurityConfigView<'_>, now: SystemTime) -> SecurityReadinessReport {
    let Ok(profile) = DeploymentProfile::from_str(view.profile) else {
        return SecurityReadinessReport {
            profile: None,
            failures: vec![SecurityReadinessFailure::UnknownProfile],
        };
    };
    let mut failures = Vec::new();
    if profile == DeploymentProfile::Secure {
        match view.trust_anchor {
            None => failures.push(SecurityReadinessFailure::MissingTrustAnchor),
            Some(path) => inspect_readable_file(
                path,
                SecurityReadinessFailure::TrustAnchorUnavailable,
                SecurityReadinessFailure::TrustAnchorNotRegularFile,
                false,
                &mut failures,
            ),
        }
        match view.secret_file {
            None => failures.push(SecurityReadinessFailure::MissingSecretFile),
            Some(path) => inspect_readable_file(
                path,
                SecurityReadinessFailure::SecretFileUnavailable,
                SecurityReadinessFailure::SecretFileNotRegularFile,
                true,
                &mut failures,
            ),
        }
        match view.bootstrap_expires_at {
            None => failures.push(SecurityReadinessFailure::MissingBootstrapExpiry),
            Some(expiry) if expiry <= now => failures.push(SecurityReadinessFailure::ExpiredBootstrap),
            Some(_) => {}
        }
        if view.insecure_downgrade {
            failures.push(SecurityReadinessFailure::InsecureDowngrade);
        }
    }
    SecurityReadinessReport {
        profile: Some(profile),
        failures,
    }
}

fn inspect_readable_file(
    path: &Path,
    unavailable: SecurityReadinessFailure,
    not_regular_file: SecurityReadinessFailure,
    require_owner_only_permissions: bool,
    failures: &mut Vec<SecurityReadinessFailure>,
) {
    let Ok(path_metadata) = path.metadata() else {
        failures.push(unavailable);
        return;
    };
    if !path_metadata.is_file() {
        failures.push(not_regular_file);
        return;
    }
    let Ok(file) = File::open(path) else {
        failures.push(unavailable);
        return;
    };
    let Ok(metadata) = file.metadata() else {
        failures.push(unavailable);
        return;
    };
    if !metadata.is_file() {
        failures.push(not_regular_file);
        return;
    }
    if require_owner_only_permissions && !platform_secret_file_permissions(&metadata).is_owner_only() {
        failures.push(SecurityReadinessFailure::InsecureSecretFilePermissions);
    }
}

/// Portable projection of secret-file permissions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SecretFilePermissions {
    pub group_access: bool,
    pub other_access: bool,
}

impl SecretFilePermissions {
    pub fn is_owner_only(self) -> bool {
        !self.group_access && !self.other_access
    }
}

/// Errors produced while opening and reading a secret file through one handle.
#[derive(Debug, Error)]
pub enum SecretFileError {
    #[error("failed to open secret file: {0}")]
    Open(#[source] io::Error),
    #[error("failed to inspect secret file permissions: {0}")]
    Metadata(#[source] io::Error),
    #[error("secret file permissions allow group or other access")]
    InsecurePermissions,
    #[error("failed to read secret file: {0}")]
    Read(#[source] io::Error),
}

/// Opens, permission-checks and reads a secret using the same file handle.
pub fn read_secret_file(path: &Path) -> Result<Secret<Vec<u8>>, SecretFileError> {
    let mut file = File::open(path).map_err(SecretFileError::Open)?;
    let metadata = file.metadata().map_err(SecretFileError::Metadata)?;
    if !platform_secret_file_permissions(&metadata).is_owner_only() {
        return Err(SecretFileError::InsecurePermissions);
    }
    let mut value = Vec::new();
    file.read_to_end(&mut value).map_err(SecretFileError::Read)?;
    Ok(Secret::new(value))
}

#[cfg(unix)]
fn platform_secret_file_permissions(metadata: &std::fs::Metadata) -> SecretFilePermissions {
    use std::os::unix::fs::PermissionsExt;

    let mode = metadata.permissions().mode();
    SecretFilePermissions {
        group_access: mode & 0o070 != 0,
        other_access: mode & 0o007 != 0,
    }
}

#[cfg(windows)]
fn platform_secret_file_permissions(_metadata: &std::fs::Metadata) -> SecretFilePermissions {
    // Windows ACL inspection is supplied by the deployment adapter. The portable contract stays
    // fail-closed until that adapter provides an owner-only projection.
    SecretFilePermissions {
        group_access: true,
        other_access: true,
    }
}
