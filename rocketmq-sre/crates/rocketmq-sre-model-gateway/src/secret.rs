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

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

use crate::error::ProviderError;
use crate::error::ProviderErrorCode;

const MAX_DEV_SECRET_BYTES: u64 = 64 * 1024;

/// Supported secret-reference ownership and lookup schemes.
#[derive(Clone, Copy, Debug, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecretReferenceKind {
    Environment,
    File,
    External,
    Adapter,
}

impl SecretReferenceKind {
    const fn scheme(self) -> &'static str {
        match self {
            Self::Environment => "env",
            Self::File => "file",
            Self::External => "external",
            Self::Adapter => "adapter",
        }
    }
}

/// A reference to a credential, never the credential value itself.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct SecretReference {
    kind: SecretReferenceKind,
    locator: String,
}

impl SecretReference {
    /// Parses a fail-closed secret reference.
    ///
    /// Accepted forms are `env://NAME`, `file://path`, `external://namespace/key`,
    /// and `adapter://profile-key`.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderErrorCode::ProfileInvalid`] for an unknown scheme,
    /// an empty locator, or an embedded control character.
    pub fn parse(value: &str) -> Result<Self, ProviderError> {
        let (scheme, locator) = value.split_once("://").ok_or_else(|| {
            ProviderError::new(
                ProviderErrorCode::ProfileInvalid,
                "credential_ref must use an approved reference scheme",
            )
        })?;
        if locator.is_empty() || locator.chars().any(char::is_control) {
            return Err(ProviderError::new(
                ProviderErrorCode::ProfileInvalid,
                "credential_ref locator is invalid",
            ));
        }
        let kind = match scheme {
            "env" => SecretReferenceKind::Environment,
            "file" => SecretReferenceKind::File,
            "external" => SecretReferenceKind::External,
            "adapter" => SecretReferenceKind::Adapter,
            _ => {
                return Err(ProviderError::new(
                    ProviderErrorCode::ProfileInvalid,
                    "credential_ref scheme is not allowed",
                ));
            }
        };
        Ok(Self {
            kind,
            locator: locator.to_owned(),
        })
    }

    /// Creates an external secret-manager reference.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderErrorCode::ProfileInvalid`] when the locator is empty.
    pub fn external(locator: impl Into<String>) -> Result<Self, ProviderError> {
        let locator = locator.into();
        Self::parse(&format!("external://{locator}"))
    }

    /// Returns the reference kind.
    #[must_use]
    pub const fn kind(&self) -> SecretReferenceKind {
        self.kind
    }

    /// Returns the non-secret locator.
    #[must_use]
    pub fn locator(&self) -> &str {
        &self.locator
    }

    /// Returns a reference URI. It never contains credential material.
    #[must_use]
    pub fn as_reference_uri(&self) -> String {
        format!("{}://{}", self.kind.scheme(), self.locator)
    }
}

impl Debug for SecretReference {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SecretReference")
            .field("kind", &self.kind)
            .field("locator", &"[REFERENCE REDACTED]")
            .finish()
    }
}

/// Resolved credential material. Debug output is always redacted.
#[derive(Clone)]
pub struct SecretMaterial {
    value: Arc<str>,
    version_fingerprint: String,
    expires_at_unix_ms: Option<u64>,
}

impl SecretMaterial {
    /// Creates resolved secret material at a transport boundary.
    #[must_use]
    pub fn new(
        value: impl Into<String>,
        version_fingerprint: impl Into<String>,
        expires_at_unix_ms: Option<u64>,
    ) -> Self {
        Self {
            value: Arc::from(value.into()),
            version_fingerprint: version_fingerprint.into(),
            expires_at_unix_ms,
        }
    }

    /// Exposes the credential only to the injected network transport.
    ///
    /// Callers must not log, serialize, persist, or include the returned value
    /// in an error. Provider profiles and invocation records retain only the
    /// credential reference and version fingerprint.
    #[must_use]
    pub fn expose_to_transport(&self) -> &str {
        &self.value
    }

    /// Returns a non-secret credential version fingerprint.
    #[must_use]
    pub fn version_fingerprint(&self) -> &str {
        &self.version_fingerprint
    }

    /// Returns the provider-reported expiry, when available.
    #[must_use]
    pub const fn expires_at_unix_ms(&self) -> Option<u64> {
        self.expires_at_unix_ms
    }
}

impl Debug for SecretMaterial {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SecretMaterial")
            .field("value", &"[SECRET REDACTED]")
            .field("version_fingerprint", &self.version_fingerprint)
            .field("expires_at_unix_ms", &self.expires_at_unix_ms)
            .finish()
    }
}

/// Secret lookup boundary used by built-in provider adapters.
pub trait SecretProvider: Send + Sync {
    /// Resolves the current credential version.
    ///
    /// # Errors
    ///
    /// Returns a redacted [`ProviderError`] when lookup is disabled, denied,
    /// unavailable, or the reference kind is not owned by this provider.
    fn resolve(&self, reference: &SecretReference) -> Result<SecretMaterial, ProviderError>;

    /// Forces a refresh after rotation without restarting the gateway.
    ///
    /// # Errors
    ///
    /// Returns a redacted [`ProviderError`] under the same conditions as
    /// [`SecretProvider::resolve`].
    fn refresh(&self, reference: &SecretReference) -> Result<SecretMaterial, ProviderError>;
}

/// Explicitly enabled development-only environment/file adapter.
pub struct DevSecretProvider {
    enabled: bool,
    allowed_env_prefix: String,
    allowed_file_root: Option<PathBuf>,
}

impl DevSecretProvider {
    /// Creates the development provider. Callers must opt in with `enabled`.
    #[must_use]
    pub fn new(enabled: bool, allowed_env_prefix: impl Into<String>, allowed_file_root: Option<PathBuf>) -> Self {
        Self {
            enabled,
            allowed_env_prefix: allowed_env_prefix.into(),
            allowed_file_root,
        }
    }

    fn resolve_file(&self, locator: &str) -> Result<SecretMaterial, ProviderError> {
        let configured_root = self.allowed_file_root.as_ref().ok_or_else(|| {
            ProviderError::new(
                ProviderErrorCode::SecretAccessDenied,
                "file secret access is not configured",
            )
        })?;
        let root = configured_root.canonicalize().map_err(|_| {
            ProviderError::new(
                ProviderErrorCode::SecretUnavailable,
                "configured secret root is unavailable",
            )
        })?;
        let candidate = PathBuf::from(locator).canonicalize().map_err(|_| {
            ProviderError::new(
                ProviderErrorCode::SecretUnavailable,
                "referenced secret file is unavailable",
            )
        })?;
        if !candidate.starts_with(&root) {
            return Err(ProviderError::new(
                ProviderErrorCode::SecretAccessDenied,
                "secret file is outside the configured root",
            ));
        }
        let metadata = fs::metadata(&candidate).map_err(|_| {
            ProviderError::new(
                ProviderErrorCode::SecretUnavailable,
                "referenced secret file metadata is unavailable",
            )
        })?;
        if metadata.len() > MAX_DEV_SECRET_BYTES {
            return Err(ProviderError::new(
                ProviderErrorCode::OutputTooLarge,
                "referenced secret file exceeds the development limit",
            ));
        }
        let value = fs::read_to_string(&candidate).map_err(|_| {
            ProviderError::new(
                ProviderErrorCode::SecretUnavailable,
                "referenced secret file cannot be read",
            )
        })?;
        let modified = metadata
            .modified()
            .ok()
            .and_then(|value| value.duration_since(UNIX_EPOCH).ok())
            .map_or(0, |duration| duration.as_millis() as u64);
        Ok(SecretMaterial::new(
            value.trim_end_matches(['\r', '\n']).to_owned(),
            format!("file:{modified}:{}", metadata.len()),
            None,
        ))
    }

    fn resolve_env(&self, locator: &str) -> Result<SecretMaterial, ProviderError> {
        if !locator.starts_with(&self.allowed_env_prefix) {
            return Err(ProviderError::new(
                ProviderErrorCode::SecretAccessDenied,
                "environment secret is outside the configured prefix",
            ));
        }
        let value = std::env::var(locator).map_err(|_| {
            ProviderError::new(
                ProviderErrorCode::SecretUnavailable,
                "referenced environment secret is unavailable",
            )
        })?;
        Ok(SecretMaterial::new(value, format!("env:{locator}"), None))
    }
}

impl SecretProvider for DevSecretProvider {
    fn resolve(&self, reference: &SecretReference) -> Result<SecretMaterial, ProviderError> {
        if !self.enabled {
            return Err(ProviderError::new(
                ProviderErrorCode::SecretAccessDenied,
                "development secret adapter is disabled",
            ));
        }
        match reference.kind {
            SecretReferenceKind::Environment => self.resolve_env(reference.locator()),
            SecretReferenceKind::File => self.resolve_file(reference.locator()),
            SecretReferenceKind::External | SecretReferenceKind::Adapter => Err(ProviderError::new(
                ProviderErrorCode::SecretAccessDenied,
                "secret reference is not owned by the development adapter",
            )),
        }
    }

    fn refresh(&self, reference: &SecretReference) -> Result<SecretMaterial, ProviderError> {
        self.resolve(reference)
    }
}

/// Value returned by an organization-specific secret-manager client.
pub struct ExternalSecretValue {
    pub value: String,
    pub version: String,
    pub expires_at_unix_ms: Option<u64>,
}

/// Narrow production secret-manager interface.
///
/// A deployment can implement this trait for Vault, KMS, or its platform
/// Secret Manager without importing that vendor SDK into the model gateway.
pub trait ExternalSecretClient: Send + Sync {
    /// Reads one secret in the adapter's own allowed namespace.
    ///
    /// # Errors
    ///
    /// Returns a redacted gateway error. Implementations must not include
    /// secret values in error messages.
    fn read_secret(&self, locator: &str) -> Result<ExternalSecretValue, ProviderError>;
}

struct CachedSecret {
    material: SecretMaterial,
    refresh_after: Instant,
}

/// Production adapter around an injected external secret-manager client.
pub struct ExternalSecretManagerProvider {
    client: Arc<dyn ExternalSecretClient>,
    allowed_namespace: String,
    ttl: Duration,
    cache: Mutex<BTreeMap<String, CachedSecret>>,
}

impl ExternalSecretManagerProvider {
    /// Creates a namespace-constrained secret-manager adapter.
    #[must_use]
    pub fn new(client: Arc<dyn ExternalSecretClient>, allowed_namespace: impl Into<String>, ttl: Duration) -> Self {
        Self {
            client,
            allowed_namespace: allowed_namespace.into(),
            ttl,
            cache: Mutex::new(BTreeMap::new()),
        }
    }

    /// Applies a secret-manager watch notification and returns only the new
    /// non-secret version fingerprint.
    ///
    /// # Errors
    ///
    /// Returns a redacted access or availability error if the watched
    /// reference is outside this adapter's namespace or cannot be refreshed.
    pub fn on_watch_event(&self, reference: &SecretReference) -> Result<String, ProviderError> {
        self.refresh(reference)
            .map(|material| material.version_fingerprint().to_owned())
    }

    fn ensure_owned<'a>(&self, reference: &'a SecretReference) -> Result<&'a str, ProviderError> {
        let locator = reference.locator();
        let namespace_owned = if self.allowed_namespace.ends_with('/') {
            locator.starts_with(&self.allowed_namespace)
        } else {
            locator == self.allowed_namespace
                || locator
                    .strip_prefix(&self.allowed_namespace)
                    .is_some_and(|remainder| remainder.starts_with('/'))
        };
        if reference.kind != SecretReferenceKind::External || self.allowed_namespace.is_empty() || !namespace_owned {
            return Err(ProviderError::new(
                ProviderErrorCode::SecretAccessDenied,
                "secret reference is outside the adapter namespace",
            ));
        }
        Ok(locator)
    }

    fn read_and_cache(&self, locator: &str) -> Result<SecretMaterial, ProviderError> {
        let value = self.client.read_secret(locator)?;
        let material = SecretMaterial::new(
            value.value,
            format!("version:{}", value.version),
            value.expires_at_unix_ms,
        );
        let mut cache = self
            .cache
            .lock()
            .map_err(|_| ProviderError::new(ProviderErrorCode::SecretUnavailable, "secret cache is unavailable"))?;
        cache.insert(
            locator.to_owned(),
            CachedSecret {
                material: material.clone(),
                refresh_after: Instant::now() + self.ttl,
            },
        );
        Ok(material)
    }
}

impl SecretProvider for ExternalSecretManagerProvider {
    fn resolve(&self, reference: &SecretReference) -> Result<SecretMaterial, ProviderError> {
        let locator = self.ensure_owned(reference)?;
        {
            let cache = self
                .cache
                .lock()
                .map_err(|_| ProviderError::new(ProviderErrorCode::SecretUnavailable, "secret cache is unavailable"))?;
            if let Some(cached) = cache.get(locator)
                && Instant::now() < cached.refresh_after
            {
                return Ok(cached.material.clone());
            }
        }
        self.read_and_cache(locator)
    }

    fn refresh(&self, reference: &SecretReference) -> Result<SecretMaterial, ProviderError> {
        let locator = self.ensure_owned(reference)?;
        self.read_and_cache(locator)
    }
}

/// Returns the current Unix time in milliseconds for secret expiry fixtures.
#[must_use]
pub fn current_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct RotatingClient {
        current: Mutex<(&'static str, &'static str)>,
    }

    impl ExternalSecretClient for RotatingClient {
        fn read_secret(&self, _locator: &str) -> Result<ExternalSecretValue, ProviderError> {
            let current = self.current.lock().expect("rotation lock");
            Ok(ExternalSecretValue {
                value: current.0.to_owned(),
                version: current.1.to_owned(),
                expires_at_unix_ms: None,
            })
        }
    }

    #[test]
    fn external_adapter_refreshes_rotated_versions_without_exposing_values() {
        let client = Arc::new(RotatingClient {
            current: Mutex::new(("first-secret", "v1")),
        });
        let provider = ExternalSecretManagerProvider::new(client.clone(), "team-a/models/", Duration::from_secs(60));
        let reference = SecretReference::external("team-a/models/openai").expect("valid reference");
        let first = provider.resolve(&reference).expect("first secret");
        assert_eq!(first.version_fingerprint(), "version:v1");
        assert!(!format!("{first:?}").contains("first-secret"));

        *client.current.lock().expect("rotation lock") = ("second-secret", "v2");
        let watched_version = provider.on_watch_event(&reference).expect("watch-triggered refresh");
        assert_eq!(watched_version, "version:v2");
        let refreshed = provider.resolve(&reference).expect("rotated secret");
        assert_eq!(refreshed.version_fingerprint(), "version:v2");
        assert_eq!(refreshed.expose_to_transport(), "second-secret");
        assert!(!format!("{refreshed:?}").contains("second-secret"));
    }

    #[test]
    fn external_adapter_rejects_cross_namespace_and_adapter_owned_references() {
        let client = Arc::new(RotatingClient {
            current: Mutex::new(("secret", "v1")),
        });
        let provider = ExternalSecretManagerProvider::new(client, "team-a/models/", Duration::ZERO);
        let foreign = SecretReference::external("team-b/models/openai").expect("valid reference syntax");
        let adapter_owned = SecretReference::parse("adapter://private-provider").expect("adapter reference");

        assert_eq!(
            provider.resolve(&foreign).expect_err("cross namespace").code,
            ProviderErrorCode::SecretAccessDenied
        );
        assert_eq!(
            provider.resolve(&adapter_owned).expect_err("adapter ownership").code,
            ProviderErrorCode::SecretAccessDenied
        );
    }

    #[test]
    fn external_adapter_enforces_a_namespace_segment_boundary() {
        let client = Arc::new(RotatingClient {
            current: Mutex::new(("secret", "v1")),
        });
        let provider = ExternalSecretManagerProvider::new(client, "team-a/models", Duration::ZERO);
        let valid = SecretReference::external("team-a/models/openai").expect("valid reference");
        let lookalike = SecretReference::external("team-a/models-foreign/openai").expect("valid reference syntax");

        assert_eq!(
            provider.resolve(&valid).expect("owned namespace").expose_to_transport(),
            "secret"
        );
        assert_eq!(
            provider.resolve(&lookalike).expect_err("namespace lookalike").code,
            ProviderErrorCode::SecretAccessDenied
        );
    }

    #[test]
    fn development_adapter_is_deny_by_default() {
        let provider = DevSecretProvider::new(false, "ROCKETMQ_SRE_MODEL_", None);
        let reference = SecretReference::parse("env://ROCKETMQ_SRE_MODEL_TEST").expect("reference");
        assert_eq!(
            provider.resolve(&reference).expect_err("disabled").code,
            ProviderErrorCode::SecretAccessDenied
        );
    }

    #[test]
    fn development_file_adapter_enforces_root_and_reads_a_bounded_secret() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("rocketmq-sre-model-secret-{}-{unique}", std::process::id()));
        fs::create_dir(&root).expect("create secret fixture root");
        let path = root.join("provider.key");
        fs::write(&path, "fixture-secret\n").expect("write secret fixture");
        let provider = DevSecretProvider::new(true, "ROCKETMQ_SRE_MODEL_", Some(root.clone()));
        let reference = SecretReference::parse(&format!("file://{}", path.display())).expect("file reference");

        let material = provider.resolve(&reference).expect("file secret");

        assert_eq!(material.expose_to_transport(), "fixture-secret");
        assert!(material.version_fingerprint().starts_with("file:"));
        fs::remove_file(&path).expect("remove fixture file");
        fs::remove_dir(&root).expect("remove fixture root");
    }
}
