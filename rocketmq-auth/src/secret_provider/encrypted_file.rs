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

use std::fmt;
#[cfg(unix)]
use std::fs;
#[cfg(unix)]
use std::fs::File;
#[cfg(unix)]
use std::io::Read;
#[cfg(unix)]
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
#[cfg(unix)]
use std::sync::Mutex;

#[cfg(any(unix, test))]
use aes_gcm::aead::Aead;
#[cfg(any(unix, test))]
use aes_gcm::aead::Generate;
#[cfg(any(unix, test))]
use aes_gcm::aead::KeyInit;
#[cfg(any(unix, test))]
use aes_gcm::aead::Nonce;
#[cfg(any(unix, test))]
use aes_gcm::aead::Payload;
#[cfg(any(unix, test))]
use aes_gcm::Aes256Gcm;
use rocketmq_security_api::SecretAccess;
use rocketmq_security_api::SecretMaterial;
use rocketmq_security_api::SecretName;
use rocketmq_security_api::SecretPersistence;
use rocketmq_security_api::SecretProvider;
use rocketmq_security_api::SecretProviderCapabilities;
use rocketmq_security_api::SecretProviderId;
use rocketmq_security_api::SecretVersion;
use rocketmq_security_api::SecretVersioning;
#[cfg(unix)]
use rocketmq_security_api::SecurityContractViolation;
use rocketmq_security_api::SecurityOperation;
use rocketmq_security_api::SecurityProviderError;
use rocketmq_security_api::SecurityProviderFailure;
use rocketmq_security_api::VersionedSecret;

use crate::AuthFailureKind;
use crate::AuthOperation;
use crate::AuthServiceError;
use crate::AuthServiceResult;

#[cfg(any(unix, test))]
const ENVELOPE_MAGIC: &[u8; 8] = b"RMQSEC01";
#[cfg(any(unix, test))]
const NONCE_LENGTH: usize = 12;
#[cfg(any(unix, test))]
const LENGTH_FIELD: usize = 8;
#[cfg(any(unix, test))]
const ENVELOPE_PREFIX_LENGTH: usize = ENVELOPE_MAGIC.len() + NONCE_LENGTH + LENGTH_FIELD;
#[cfg(any(unix, test))]
const MAX_SECRET_LENGTH: usize = 1024 * 1024;
#[cfg(any(unix, test))]
const AUTH_TAG_LENGTH: usize = 16;

/// AES-256-GCM local development adapter with owner-only, immutable version files.
pub struct EncryptedFileSecretProvider {
    id: SecretProviderId,
    #[cfg(unix)]
    root: PathBuf,
    #[cfg(unix)]
    key: SecretMaterial,
    #[cfg(unix)]
    write_lock: Mutex<()>,
}

impl EncryptedFileSecretProvider {
    /// Opens or creates an owner-only provider root. Windows remains fail-closed until an ACL
    /// verifier can prove the same owner-only contract.
    ///
    /// # Errors
    ///
    /// Returns a redacted provider error for invalid keys, permissions, paths, or unsupported ACLs.
    pub fn new(id: SecretProviderId, root: impl Into<PathBuf>, key: SecretMaterial) -> AuthServiceResult<Self> {
        if key.len() != 32 {
            return Err(AuthServiceError::new(
                AuthOperation::LoadSecret,
                AuthFailureKind::InvalidInput,
            ));
        }
        let root = root.into();
        prepare_root(&root).map_err(auth_provider_error)?;
        Ok(Self {
            id,
            #[cfg(unix)]
            root,
            #[cfg(unix)]
            key,
            #[cfg(unix)]
            write_lock: Mutex::new(()),
        })
    }

    #[cfg(unix)]
    fn read_version(
        &self,
        name: &SecretName,
        version: SecretVersion,
    ) -> Result<VersionedSecret, SecurityProviderError> {
        let path = version_path(&self.root.join(name.as_str()), version);
        let envelope = read_restricted_file(&path)?;
        let plaintext = decrypt_envelope(&self.key, name, version, &envelope)?;
        let material = SecretMaterial::new(plaintext).map_err(|source| {
            SecurityProviderError::caused_by(
                SecurityProviderFailure::InvalidData,
                SecurityOperation::ReadSecret,
                source,
            )
        })?;
        Ok(VersionedSecret::new(material, Some(version)))
    }

    #[cfg(unix)]
    fn write_version(
        &self,
        name: &SecretName,
        material: &SecretMaterial,
        expected_version: Option<SecretVersion>,
    ) -> Result<SecretVersion, SecurityProviderError> {
        if material.len() > MAX_SECRET_LENGTH {
            return Err(provider_error(
                SecurityProviderFailure::InvalidData,
                SecurityOperation::WriteSecret,
            ));
        }
        let directory = self.root.join(name.as_str());
        prepare_secret_directory(&directory)?;
        let current = latest_version(&directory)?;
        let next = match (current, expected_version) {
            (None, None) => SecretVersion::new(1),
            (Some(current), Some(expected)) if current == expected => SecretVersion::new(
                current
                    .get()
                    .checked_add(1)
                    .ok_or_else(|| provider_error(SecurityProviderFailure::Conflict, SecurityOperation::WriteSecret))?,
            ),
            _ => {
                return Err(provider_error(
                    SecurityProviderFailure::Conflict,
                    SecurityOperation::WriteSecret,
                ));
            }
        };
        let envelope = encrypt_envelope(&self.key, name, next, material.expose_secret())?;
        publish_version(&directory, next, &envelope)?;
        Ok(next)
    }
}

impl fmt::Debug for EncryptedFileSecretProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EncryptedFileSecretProvider")
            .field("id", &self.id)
            .field("root", &"[REDACTED]")
            .field("key", &"[REDACTED]")
            .finish()
    }
}

impl SecretProvider for EncryptedFileSecretProvider {
    fn id(&self) -> &SecretProviderId {
        &self.id
    }

    fn capabilities(&self) -> SecretProviderCapabilities {
        SecretProviderCapabilities::new(
            SecretAccess::ReadWrite,
            SecretPersistence::EncryptedLocalStorage,
            SecretVersioning::Optimistic,
        )
    }

    fn read(&self, name: &SecretName) -> Result<VersionedSecret, SecurityProviderError> {
        #[cfg(windows)]
        {
            let _ = name;
            Err(provider_error(
                SecurityProviderFailure::Unsupported,
                SecurityOperation::ReadSecret,
            ))
        }
        #[cfg(unix)]
        {
            let directory = self.root.join(name.as_str());
            check_restricted_directory(&directory)?;
            let version = latest_version(&directory)?
                .ok_or_else(|| provider_error(SecurityProviderFailure::NotFound, SecurityOperation::ReadSecret))?;
            self.read_version(name, version)
        }
    }

    fn write(
        &self,
        name: &SecretName,
        material: SecretMaterial,
        expected_version: Option<SecretVersion>,
    ) -> Result<SecretVersion, SecurityProviderError> {
        #[cfg(windows)]
        {
            let _ = (name, material, expected_version);
            Err(provider_error(
                SecurityProviderFailure::Unsupported,
                SecurityOperation::WriteSecret,
            ))
        }
        #[cfg(unix)]
        {
            let _guard = self
                .write_lock
                .lock()
                .map_err(|_| provider_error(SecurityProviderFailure::Unavailable, SecurityOperation::WriteSecret))?;
            self.write_version(name, &material, expected_version)
        }
    }
}

#[cfg(windows)]
fn prepare_root(_root: &Path) -> Result<(), SecurityProviderError> {
    Err(provider_error(
        SecurityProviderFailure::Unsupported,
        SecurityOperation::InspectSecret,
    ))
}

#[cfg(unix)]
fn prepare_root(root: &Path) -> Result<(), SecurityProviderError> {
    use std::os::unix::fs::DirBuilderExt;

    if !root.exists() {
        let mut builder = fs::DirBuilder::new();
        builder.recursive(true).mode(0o700);
        builder.create(root).map_err(|source| {
            SecurityProviderError::caused_by(
                SecurityProviderFailure::Unavailable,
                SecurityOperation::InspectSecret,
                source,
            )
        })?;
    }
    check_restricted_directory(root)
}

#[cfg(unix)]
fn prepare_secret_directory(directory: &Path) -> Result<(), SecurityProviderError> {
    use std::os::unix::fs::DirBuilderExt;

    if !directory.exists() {
        let mut builder = fs::DirBuilder::new();
        builder.mode(0o700);
        match builder.create(directory) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(source) => {
                return Err(SecurityProviderError::caused_by(
                    SecurityProviderFailure::Unavailable,
                    SecurityOperation::WriteSecret,
                    source,
                ));
            }
        }
    }
    check_restricted_directory(directory)
}

#[cfg(unix)]
fn check_restricted_directory(path: &Path) -> Result<(), SecurityProviderError> {
    use std::os::unix::fs::PermissionsExt;

    let metadata = fs::symlink_metadata(path).map_err(|source| {
        SecurityProviderError::caused_by(
            SecurityProviderFailure::NotFound,
            SecurityOperation::InspectSecret,
            source,
        )
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(SecurityProviderError::contract(
            SecurityOperation::InspectSecret,
            SecurityContractViolation::SecretStoragePermissionsInsecure,
        ));
    }
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(SecurityProviderError::contract(
            SecurityOperation::InspectSecret,
            SecurityContractViolation::SecretStoragePermissionsInsecure,
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn latest_version(directory: &Path) -> Result<Option<SecretVersion>, SecurityProviderError> {
    let entries = fs::read_dir(directory).map_err(|source| {
        SecurityProviderError::caused_by(
            SecurityProviderFailure::NotFound,
            SecurityOperation::InspectSecret,
            source,
        )
    })?;
    let mut latest = None;
    for entry in entries {
        let entry = entry.map_err(|source| {
            SecurityProviderError::caused_by(
                SecurityProviderFailure::Unavailable,
                SecurityOperation::InspectSecret,
                source,
            )
        })?;
        let file_name = entry.file_name();
        let Some(file_name) = file_name.to_str() else {
            return Err(provider_error(
                SecurityProviderFailure::InvalidData,
                SecurityOperation::InspectSecret,
            ));
        };
        let Some(raw_version) = file_name.strip_suffix(".secret") else {
            if file_name.ends_with(".tmp") {
                continue;
            }
            return Err(provider_error(
                SecurityProviderFailure::InvalidData,
                SecurityOperation::InspectSecret,
            ));
        };
        if raw_version.len() != 20 || !raw_version.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(provider_error(
                SecurityProviderFailure::InvalidData,
                SecurityOperation::InspectSecret,
            ));
        }
        let version = raw_version.parse::<u64>().map_err(|source| {
            SecurityProviderError::caused_by(
                SecurityProviderFailure::InvalidData,
                SecurityOperation::InspectSecret,
                source,
            )
        })?;
        if version == 0 {
            return Err(provider_error(
                SecurityProviderFailure::InvalidData,
                SecurityOperation::InspectSecret,
            ));
        }
        let metadata = entry.metadata().map_err(|source| {
            SecurityProviderError::caused_by(
                SecurityProviderFailure::Unavailable,
                SecurityOperation::InspectSecret,
                source,
            )
        })?;
        if !metadata.is_file() {
            return Err(provider_error(
                SecurityProviderFailure::InvalidData,
                SecurityOperation::InspectSecret,
            ));
        }
        let version = SecretVersion::new(version);
        latest = Some(latest.map_or(version, |current: SecretVersion| current.max(version)));
    }
    Ok(latest)
}

#[cfg(unix)]
fn version_path(directory: &Path, version: SecretVersion) -> PathBuf {
    directory.join(format!("{:020}.secret", version.get()))
}

#[cfg(unix)]
fn read_restricted_file(path: &Path) -> Result<Vec<u8>, SecurityProviderError> {
    use std::os::unix::fs::PermissionsExt;

    let link_metadata = fs::symlink_metadata(path).map_err(|source| {
        SecurityProviderError::caused_by(SecurityProviderFailure::NotFound, SecurityOperation::ReadSecret, source)
    })?;
    if link_metadata.file_type().is_symlink() {
        return Err(SecurityProviderError::contract(
            SecurityOperation::ReadSecret,
            SecurityContractViolation::SecretStoragePermissionsInsecure,
        ));
    }
    let mut file = File::open(path).map_err(|source| {
        SecurityProviderError::caused_by(
            SecurityProviderFailure::Unavailable,
            SecurityOperation::ReadSecret,
            source,
        )
    })?;
    let metadata = file.metadata().map_err(|source| {
        SecurityProviderError::caused_by(
            SecurityProviderFailure::Unavailable,
            SecurityOperation::ReadSecret,
            source,
        )
    })?;
    if !metadata.is_file() || metadata.permissions().mode() & 0o077 != 0 {
        return Err(SecurityProviderError::contract(
            SecurityOperation::ReadSecret,
            SecurityContractViolation::SecretStoragePermissionsInsecure,
        ));
    }
    let maximum = ENVELOPE_PREFIX_LENGTH + MAX_SECRET_LENGTH + AUTH_TAG_LENGTH;
    if metadata.len() > maximum as u64 {
        return Err(provider_error(
            SecurityProviderFailure::InvalidData,
            SecurityOperation::ReadSecret,
        ));
    }
    let mut envelope = Vec::with_capacity(metadata.len() as usize);
    (&mut file)
        .take(maximum as u64 + 1)
        .read_to_end(&mut envelope)
        .map_err(|source| {
            SecurityProviderError::caused_by(
                SecurityProviderFailure::Unavailable,
                SecurityOperation::ReadSecret,
                source,
            )
        })?;
    if envelope.len() > maximum {
        return Err(provider_error(
            SecurityProviderFailure::InvalidData,
            SecurityOperation::ReadSecret,
        ));
    }
    Ok(envelope)
}

#[cfg(any(unix, test))]
fn associated_data(name: &SecretName, version: SecretVersion) -> Vec<u8> {
    let mut data = Vec::with_capacity(32 + name.as_str().len());
    data.extend_from_slice(b"rocketmq-secret-envelope-v1\0");
    data.extend_from_slice(name.as_str().as_bytes());
    data.push(0);
    data.extend_from_slice(&version.get().to_be_bytes());
    data
}

#[cfg(any(unix, test))]
fn encrypt_envelope(
    key: &SecretMaterial,
    name: &SecretName,
    version: SecretVersion,
    plaintext: &[u8],
) -> Result<Vec<u8>, SecurityProviderError> {
    let cipher = Aes256Gcm::new_from_slice(key.expose_secret()).map_err(|source| {
        SecurityProviderError::caused_by(
            SecurityProviderFailure::InvalidData,
            SecurityOperation::EncryptSecret,
            source,
        )
    })?;
    let nonce = Nonce::<Aes256Gcm>::generate();
    let aad = associated_data(name, version);
    let ciphertext = cipher
        .encrypt(
            &nonce,
            Payload {
                msg: plaintext,
                aad: &aad,
            },
        )
        .map_err(|source| {
            SecurityProviderError::caused_by(
                SecurityProviderFailure::OperationFailed,
                SecurityOperation::EncryptSecret,
                source,
            )
        })?;
    let mut envelope = Vec::with_capacity(ENVELOPE_PREFIX_LENGTH + ciphertext.len());
    envelope.extend_from_slice(ENVELOPE_MAGIC);
    envelope.extend_from_slice(&nonce);
    envelope.extend_from_slice(&(ciphertext.len() as u64).to_be_bytes());
    envelope.extend_from_slice(&ciphertext);
    Ok(envelope)
}

#[cfg(any(unix, test))]
fn decrypt_envelope(
    key: &SecretMaterial,
    name: &SecretName,
    version: SecretVersion,
    envelope: &[u8],
) -> Result<Vec<u8>, SecurityProviderError> {
    if envelope.len() < ENVELOPE_PREFIX_LENGTH + AUTH_TAG_LENGTH || &envelope[..ENVELOPE_MAGIC.len()] != ENVELOPE_MAGIC
    {
        return Err(provider_error(
            SecurityProviderFailure::InvalidData,
            SecurityOperation::DecryptSecret,
        ));
    }
    let nonce_start = ENVELOPE_MAGIC.len();
    let nonce_end = nonce_start + NONCE_LENGTH;
    let length_end = nonce_end + LENGTH_FIELD;
    let nonce = Nonce::<Aes256Gcm>::try_from(&envelope[nonce_start..nonce_end])
        .map_err(|_| provider_error(SecurityProviderFailure::InvalidData, SecurityOperation::DecryptSecret))?;
    let encoded_length = u64::from_be_bytes(
        envelope[nonce_end..length_end]
            .try_into()
            .map_err(|_| provider_error(SecurityProviderFailure::InvalidData, SecurityOperation::DecryptSecret))?,
    );
    let ciphertext = &envelope[length_end..];
    if encoded_length != ciphertext.len() as u64 || ciphertext.len() > MAX_SECRET_LENGTH + AUTH_TAG_LENGTH {
        return Err(provider_error(
            SecurityProviderFailure::InvalidData,
            SecurityOperation::DecryptSecret,
        ));
    }
    let cipher = Aes256Gcm::new_from_slice(key.expose_secret()).map_err(|source| {
        SecurityProviderError::caused_by(
            SecurityProviderFailure::InvalidData,
            SecurityOperation::DecryptSecret,
            source,
        )
    })?;
    let aad = associated_data(name, version);
    cipher
        .decrypt(
            &nonce,
            Payload {
                msg: ciphertext,
                aad: &aad,
            },
        )
        .map_err(|source| {
            SecurityProviderError::caused_by(
                SecurityProviderFailure::InvalidData,
                SecurityOperation::DecryptSecret,
                source,
            )
        })
}

#[cfg(unix)]
fn publish_version(directory: &Path, version: SecretVersion, envelope: &[u8]) -> Result<(), SecurityProviderError> {
    use std::os::unix::fs::OpenOptionsExt;
    use std::os::unix::fs::PermissionsExt;

    let nonce_start = ENVELOPE_MAGIC.len();
    let nonce_end = nonce_start + NONCE_LENGTH;
    let temporary_path = directory.join(format!(
        ".{:020}-{}.tmp",
        version.get(),
        hex::encode(&envelope[nonce_start..nonce_end])
    ));
    let final_path = version_path(directory, version);
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true).mode(0o600);
    let mut file = options.open(&temporary_path).map_err(|source| {
        SecurityProviderError::caused_by(
            SecurityProviderFailure::Unavailable,
            SecurityOperation::WriteSecret,
            source,
        )
    })?;
    let write_result = (|| {
        file.write_all(envelope).map_err(|source| {
            SecurityProviderError::caused_by(
                SecurityProviderFailure::Unavailable,
                SecurityOperation::WriteSecret,
                source,
            )
        })?;
        file.sync_all().map_err(|source| {
            SecurityProviderError::caused_by(
                SecurityProviderFailure::Unavailable,
                SecurityOperation::SynchronizeSecret,
                source,
            )
        })?;
        file.set_permissions(fs::Permissions::from_mode(0o400))
            .map_err(|source| {
                SecurityProviderError::caused_by(
                    SecurityProviderFailure::Unavailable,
                    SecurityOperation::WriteSecret,
                    source,
                )
            })?;
        fs::hard_link(&temporary_path, &final_path).map_err(|source| {
            if source.kind() == std::io::ErrorKind::AlreadyExists {
                SecurityProviderError::caused_by(
                    SecurityProviderFailure::Conflict,
                    SecurityOperation::WriteSecret,
                    source,
                )
            } else {
                SecurityProviderError::caused_by(
                    SecurityProviderFailure::Unavailable,
                    SecurityOperation::WriteSecret,
                    source,
                )
            }
        })?;
        sync_directory(directory)
    })();
    drop(file);
    let _ = fs::remove_file(&temporary_path);
    write_result
}

#[cfg(unix)]
fn sync_directory(directory: &Path) -> Result<(), SecurityProviderError> {
    File::open(directory)
        .and_then(|file| file.sync_all())
        .map_err(|source| {
            SecurityProviderError::caused_by(
                SecurityProviderFailure::Unavailable,
                SecurityOperation::SynchronizeSecret,
                source,
            )
        })
}

fn provider_error(kind: SecurityProviderFailure, operation: SecurityOperation) -> SecurityProviderError {
    SecurityProviderError::new(kind, operation)
}

fn auth_provider_error(source: SecurityProviderError) -> AuthServiceError {
    AuthServiceError::provider(AuthOperation::LoadSecret, source)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    fn material(bytes: &[u8]) -> SecretMaterial {
        SecretMaterial::new(bytes.to_vec()).unwrap()
    }

    #[test]
    fn envelope_authenticates_name_version_and_ciphertext() {
        let key = material(&[9; 32]);
        let name = SecretName::new("broker-admin-key").unwrap();
        let version = SecretVersion::new(7);
        let mut envelope = encrypt_envelope(&key, &name, version, b"not-written-in-clear").unwrap();
        assert!(!envelope.windows(20).any(|window| window == b"not-written-in-clear"));
        assert_eq!(
            decrypt_envelope(&key, &name, version, &envelope).unwrap(),
            b"not-written-in-clear"
        );

        envelope[ENVELOPE_PREFIX_LENGTH] ^= 1;
        assert_eq!(
            decrypt_envelope(&key, &name, version, &envelope).unwrap_err().kind(),
            SecurityProviderFailure::InvalidData
        );
        let other_name = SecretName::new("other-key").unwrap();
        assert_eq!(
            decrypt_envelope(&key, &other_name, version, &envelope)
                .unwrap_err()
                .kind(),
            SecurityProviderFailure::InvalidData
        );
    }

    #[test]
    fn cipher_failures_retain_typed_sources_without_rendering_them() {
        let name = SecretName::new("broker-admin-key").unwrap();
        let version = SecretVersion::new(7);
        let invalid_key = material(&[9; 31]);
        let key_error = encrypt_envelope(&invalid_key, &name, version, b"secret")
            .expect_err("an invalid AES-256 key length must fail");
        assert_eq!(key_error.kind(), SecurityProviderFailure::InvalidData);
        assert_eq!(key_error.operation(), SecurityOperation::EncryptSecret);
        assert!(Error::source(&key_error)
            .and_then(|source| source.downcast_ref::<aes_gcm::aead::common::InvalidLength>())
            .is_some());

        let key = material(&[9; 32]);
        let mut envelope = encrypt_envelope(&key, &name, version, b"secret").unwrap();
        envelope[ENVELOPE_PREFIX_LENGTH] ^= 1;
        let decrypt_error = decrypt_envelope(&key, &name, version, &envelope)
            .expect_err("modified ciphertext must fail authentication");
        assert_eq!(decrypt_error.kind(), SecurityProviderFailure::InvalidData);
        assert_eq!(decrypt_error.operation(), SecurityOperation::DecryptSecret);
        assert!(Error::source(&decrypt_error)
            .and_then(|source| source.downcast_ref::<aes_gcm::Error>())
            .is_some());

        for error in [&key_error, &decrypt_error] {
            assert!(!error.to_string().contains("InvalidLength"));
            assert!(!format!("{error:?}").contains("aead::Error"));
        }
    }
}
