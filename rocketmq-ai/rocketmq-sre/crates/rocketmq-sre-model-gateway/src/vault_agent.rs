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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::fs;
use std::io::Read;
use std::path::Path;
use std::path::PathBuf;
use std::time::UNIX_EPOCH;

use cap_std::ambient_authority;
use cap_std::fs::Dir;
use cap_std::fs::Metadata;

use crate::error::ProviderError;
use crate::error::ProviderErrorCode;
use crate::secret::ExternalSecretClient;
use crate::secret::ExternalSecretValue;

const DEFAULT_MAX_SECRET_BYTES: u64 = 64 * 1024;
const HARD_MAX_SECRET_BYTES: u64 = 1024 * 1024;
const MAX_LOCATOR_BYTES: usize = 1024;
const MAX_LOCATOR_SEGMENTS: usize = 32;
const MAX_VERSION_BYTES: u64 = 256;
const MAX_VERSION_SUFFIX_BYTES: usize = 32;

/// Source used to identify a rendered Vault secret revision.
///
/// Neither option derives a fingerprint from the secret value. Metadata mode
/// uses only the rendered file's modification time and length. Sidecar mode
/// requires Vault Agent or the CSI provider to render a separate, explicitly
/// non-secret version file next to the secret.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VaultAgentVersionSource {
    /// Use stable, non-content file metadata.
    FileMetadata,
    /// Read a required non-secret sidecar appended to the secret filename.
    RequiredSidecar { suffix: String },
}

/// Production file adapter for secrets rendered by Vault Agent or a Vault CSI
/// provider.
///
/// The client pins a canonical directory capability at construction time.
/// Every locator is a conservative relative path, and every path component is
/// checked for symbolic links before and after opening. Deployments must mount
/// the rendered root read-only for the gateway workload; only Vault Agent or
/// the CSI provider should have write permission.
pub struct VaultAgentFileSecretClient {
    root: Dir,
    max_secret_bytes: u64,
    version_source: VaultAgentVersionSource,
}

impl VaultAgentFileSecretClient {
    /// Opens and pins a canonical Vault Agent/CSI render root.
    ///
    /// The default secret limit is 64 KiB and the default version source is
    /// non-content file metadata.
    ///
    /// # Errors
    ///
    /// Returns a redacted error if the configured root is unavailable, is not
    /// a directory, or is itself a symbolic link.
    pub fn new(root: impl AsRef<Path>) -> Result<Self, ProviderError> {
        let configured_root = root.as_ref();
        let configured_metadata = fs::symlink_metadata(configured_root).map_err(|_| {
            ProviderError::new(
                ProviderErrorCode::SecretUnavailable,
                "Vault Agent secret root is unavailable",
            )
        })?;
        if configured_metadata.file_type().is_symlink() {
            return Err(ProviderError::new(
                ProviderErrorCode::SecretAccessDenied,
                "Vault Agent secret root must not be a symbolic link",
            ));
        }
        if !configured_metadata.is_dir() {
            return Err(ProviderError::new(
                ProviderErrorCode::SecretAccessDenied,
                "Vault Agent secret root must be a directory",
            ));
        }

        let canonical_root = configured_root.canonicalize().map_err(|_| {
            ProviderError::new(
                ProviderErrorCode::SecretUnavailable,
                "Vault Agent secret root cannot be canonicalized",
            )
        })?;
        let root = Dir::open_ambient_dir(canonical_root, ambient_authority()).map_err(|_| {
            ProviderError::new(
                ProviderErrorCode::SecretUnavailable,
                "Vault Agent secret root cannot be opened",
            )
        })?;

        Ok(Self {
            root,
            max_secret_bytes: DEFAULT_MAX_SECRET_BYTES,
            version_source: VaultAgentVersionSource::FileMetadata,
        })
    }

    /// Sets the per-secret read limit.
    ///
    /// The value must be between one byte and the hard 1 MiB ceiling.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderErrorCode::ProfileInvalid`] for zero or an
    /// unreasonably large limit.
    pub fn with_max_secret_bytes(mut self, max_secret_bytes: u64) -> Result<Self, ProviderError> {
        if !(1..=HARD_MAX_SECRET_BYTES).contains(&max_secret_bytes) {
            return Err(ProviderError::new(
                ProviderErrorCode::ProfileInvalid,
                "Vault Agent secret byte limit is invalid",
            ));
        }
        self.max_secret_bytes = max_secret_bytes;
        Ok(self)
    }

    /// Requires a non-secret version sidecar such as `.version`.
    ///
    /// If the credential locator is `models/deepseek`, the example suffix
    /// resolves `models/deepseek.version`. Vault Agent must render both files
    /// atomically or notify the provider only after both files are current.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderErrorCode::ProfileInvalid`] unless the suffix starts
    /// with `.` and contains only ASCII letters, digits, `.`, `_`, or `-`.
    pub fn with_required_version_sidecar(mut self, suffix: impl Into<String>) -> Result<Self, ProviderError> {
        let suffix = suffix.into();
        if !valid_version_suffix(&suffix) {
            return Err(ProviderError::new(
                ProviderErrorCode::ProfileInvalid,
                "Vault Agent version sidecar suffix is invalid",
            ));
        }
        self.version_source = VaultAgentVersionSource::RequiredSidecar { suffix };
        Ok(self)
    }

    fn read_rendered_file(
        &self,
        relative_path: &Path,
        max_bytes: u64,
        kind: RenderedFileKind,
    ) -> Result<(String, Metadata), ProviderError> {
        self.ensure_regular_file_without_symlinks(relative_path, kind)?;

        let mut file = self.root.open(relative_path).map_err(|_| kind.unavailable())?;
        let metadata = file.metadata().map_err(|_| kind.unavailable())?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(kind.access_denied());
        }
        if metadata.len() > max_bytes {
            return Err(kind.output_too_large());
        }

        let capacity = usize::try_from(metadata.len().min(max_bytes))
            .map_err(|_| kind.output_too_large())?
            .saturating_add(1);
        let mut bytes = Vec::with_capacity(capacity);
        file.by_ref()
            .take(max_bytes.saturating_add(1))
            .read_to_end(&mut bytes)
            .map_err(|_| kind.unavailable())?;
        if u64::try_from(bytes.len()).map_or(true, |length| length > max_bytes) {
            return Err(kind.output_too_large());
        }

        self.ensure_regular_file_without_symlinks(relative_path, kind)?;
        let value = String::from_utf8(bytes).map_err(|_| kind.unavailable())?;
        Ok((value, metadata))
    }

    fn ensure_regular_file_without_symlinks(
        &self,
        relative_path: &Path,
        kind: RenderedFileKind,
    ) -> Result<(), ProviderError> {
        let components = relative_path.components().collect::<Vec<_>>();
        let mut current = PathBuf::new();
        for (index, component) in components.iter().enumerate() {
            current.push(component.as_os_str());
            let metadata = self.root.symlink_metadata(&current).map_err(|_| kind.unavailable())?;
            if metadata.file_type().is_symlink() {
                return Err(kind.access_denied());
            }
            let final_component = index + 1 == components.len();
            if (final_component && !metadata.is_file()) || (!final_component && !metadata.is_dir()) {
                return Err(kind.access_denied());
            }
        }
        Ok(())
    }

    fn metadata_version(metadata: &Metadata) -> Result<String, ProviderError> {
        let modified = metadata
            .modified()
            .map_err(|_| version_unavailable())?
            .into_std()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| version_unavailable())?;
        Ok(format!(
            "vault-agent:metadata:{}:{}:{}",
            modified.as_secs(),
            modified.subsec_nanos(),
            metadata.len()
        ))
    }

    fn sidecar_version(&self, secret_path: &Path, suffix: &str) -> Result<String, ProviderError> {
        let file_name = secret_path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(version_unavailable)?;
        let mut sidecar_path = secret_path.to_path_buf();
        sidecar_path.set_file_name(format!("{file_name}{suffix}"));
        let (version, _) = self.read_rendered_file(&sidecar_path, MAX_VERSION_BYTES, RenderedFileKind::Version)?;
        let version = version.trim();
        if version.is_empty()
            || !version
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':'))
        {
            return Err(version_unavailable());
        }
        Ok(format!("vault-agent:sidecar:{version}"))
    }
}

impl Debug for VaultAgentFileSecretClient {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VaultAgentFileSecretClient")
            .field("root", &"[PATH REDACTED]")
            .field("max_secret_bytes", &self.max_secret_bytes)
            .field("version_source", &self.version_source)
            .finish()
    }
}

impl ExternalSecretClient for VaultAgentFileSecretClient {
    fn read_secret(&self, locator: &str) -> Result<ExternalSecretValue, ProviderError> {
        let relative_path = validate_locator(locator)?;
        let (value, metadata) =
            self.read_rendered_file(&relative_path, self.max_secret_bytes, RenderedFileKind::Secret)?;
        let value = value.trim_end_matches(['\r', '\n']).to_owned();
        if value.is_empty() {
            return Err(ProviderError::new(
                ProviderErrorCode::SecretUnavailable,
                "Vault Agent rendered secret is empty",
            ));
        }
        let version = match &self.version_source {
            VaultAgentVersionSource::FileMetadata => Self::metadata_version(&metadata)?,
            VaultAgentVersionSource::RequiredSidecar { suffix } => self.sidecar_version(&relative_path, suffix)?,
        };
        Ok(ExternalSecretValue {
            value,
            version,
            expires_at_unix_ms: None,
        })
    }
}

#[derive(Clone, Copy)]
enum RenderedFileKind {
    Secret,
    Version,
}

impl RenderedFileKind {
    fn unavailable(self) -> ProviderError {
        let message = match self {
            Self::Secret => "Vault Agent rendered secret is unavailable",
            Self::Version => "Vault Agent version sidecar is unavailable",
        };
        ProviderError::new(ProviderErrorCode::SecretUnavailable, message)
    }

    fn access_denied(self) -> ProviderError {
        let message = match self {
            Self::Secret => "Vault Agent rendered secret path is not an allowed regular file",
            Self::Version => "Vault Agent version sidecar path is not an allowed regular file",
        };
        ProviderError::new(ProviderErrorCode::SecretAccessDenied, message)
    }

    fn output_too_large(self) -> ProviderError {
        let message = match self {
            Self::Secret => "Vault Agent rendered secret exceeds the configured limit",
            Self::Version => "Vault Agent version sidecar exceeds the configured limit",
        };
        ProviderError::new(ProviderErrorCode::OutputTooLarge, message)
    }
}

fn validate_locator(locator: &str) -> Result<PathBuf, ProviderError> {
    if locator.is_empty()
        || locator.len() > MAX_LOCATOR_BYTES
        || locator.starts_with('/')
        || locator.ends_with('/')
        || locator.contains('\\')
        || locator.chars().any(char::is_control)
    {
        return Err(invalid_locator());
    }

    let mut relative_path = PathBuf::new();
    let mut segment_count = 0;
    for segment in locator.split('/') {
        segment_count += 1;
        if segment_count > MAX_LOCATOR_SEGMENTS
            || segment.is_empty()
            || matches!(segment, "." | "..")
            || !segment
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        {
            return Err(invalid_locator());
        }
        relative_path.push(segment);
    }
    Ok(relative_path)
}

fn valid_version_suffix(suffix: &str) -> bool {
    suffix.starts_with('.')
        && (2..=MAX_VERSION_SUFFIX_BYTES).contains(&suffix.len())
        && suffix
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn invalid_locator() -> ProviderError {
    ProviderError::new(
        ProviderErrorCode::SecretAccessDenied,
        "Vault Agent secret locator must be a safe relative path",
    )
}

fn version_unavailable() -> ProviderError {
    ProviderError::new(
        ProviderErrorCode::SecretUnavailable,
        "Vault Agent secret version is unavailable",
    )
}
