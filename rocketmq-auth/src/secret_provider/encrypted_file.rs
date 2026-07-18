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
use std::sync::Mutex;

use aes_gcm::aead::Aead;
use aes_gcm::aead::Generate;
use aes_gcm::aead::KeyInit;
use aes_gcm::aead::Nonce;
use aes_gcm::aead::Payload;
use aes_gcm::Aes256Gcm;
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

const ENVELOPE_MAGIC: &[u8; 8] = b"RMQSEC01";
const NONCE_LENGTH: usize = 12;
const LENGTH_FIELD: usize = 8;
const ENVELOPE_PREFIX_LENGTH: usize = ENVELOPE_MAGIC.len() + NONCE_LENGTH + LENGTH_FIELD;
const MAX_SECRET_LENGTH: usize = 1024 * 1024;
const AUTH_TAG_LENGTH: usize = 16;

/// AES-256-GCM local development adapter with owner-only, immutable version files.
pub struct EncryptedFileSecretProvider {
    id: SecretProviderId,
    root: PathBuf,
    key: SecretMaterial,
    write_lock: Mutex<()>,
}

impl EncryptedFileSecretProvider {
    /// Opens or creates an owner-only provider root. Windows remains fail-closed until an ACL
    /// verifier can prove the same owner-only contract.
    ///
    /// # Errors
    ///
    /// Returns a redacted provider error for invalid keys, permissions, paths, or unsupported ACLs.
    pub fn new(
        id: SecretProviderId,
        root: impl Into<PathBuf>,
        key: SecretMaterial,
    ) -> Result<Self, SecretProviderError> {
        if key.len() != 32 {
            return Err(SecretProviderError::InvalidMaterial);
        }
        let root = root.into();
        prepare_root(&root)?;
        Ok(Self {
            id,
            root,
            key,
            write_lock: Mutex::new(()),
        })
    }

    #[cfg(unix)]
    fn read_version(&self, name: &SecretName, version: SecretVersion) -> Result<VersionedSecret, SecretProviderError> {
        let path = version_path(&self.root.join(name.as_str()), version);
        let envelope = read_restricted_file(&path)?;
        let plaintext = decrypt_envelope(&self.key, name, version, &envelope)?;
        let material = SecretMaterial::new(plaintext).map_err(|_| SecretProviderError::InvalidEnvelope)?;
        Ok(VersionedSecret::new(material, Some(version)))
    }

    #[cfg(unix)]
    fn write_version(
        &self,
        name: &SecretName,
        material: &SecretMaterial,
        expected_version: Option<SecretVersion>,
    ) -> Result<SecretVersion, SecretProviderError> {
        if material.len() > MAX_SECRET_LENGTH {
            return Err(SecretProviderError::InvalidMaterial);
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
                    .ok_or(SecretProviderError::VersionConflict)?,
            ),
            _ => return Err(SecretProviderError::VersionConflict),
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

    fn read(&self, name: &SecretName) -> Result<VersionedSecret, SecretProviderError> {
        #[cfg(windows)]
        {
            let _ = name;
            Err(SecretProviderError::UnsupportedPlatform)
        }
        #[cfg(unix)]
        {
            let directory = self.root.join(name.as_str());
            check_restricted_directory(&directory)?;
            let version = latest_version(&directory)?.ok_or(SecretProviderError::NotFound)?;
            self.read_version(name, version)
        }
    }

    fn write(
        &self,
        name: &SecretName,
        material: SecretMaterial,
        expected_version: Option<SecretVersion>,
    ) -> Result<SecretVersion, SecretProviderError> {
        #[cfg(windows)]
        {
            let _ = (name, material, expected_version);
            Err(SecretProviderError::UnsupportedPlatform)
        }
        #[cfg(unix)]
        {
            let _guard = self.write_lock.lock().map_err(|_| SecretProviderError::Unavailable)?;
            self.write_version(name, &material, expected_version)
        }
    }
}

#[cfg(windows)]
fn prepare_root(_root: &Path) -> Result<(), SecretProviderError> {
    Err(SecretProviderError::UnsupportedPlatform)
}

#[cfg(unix)]
fn prepare_root(root: &Path) -> Result<(), SecretProviderError> {
    use std::os::unix::fs::DirBuilderExt;

    if !root.exists() {
        let mut builder = fs::DirBuilder::new();
        builder.recursive(true).mode(0o700);
        builder.create(root).map_err(|_| SecretProviderError::Unavailable)?;
    }
    check_restricted_directory(root)
}

#[cfg(unix)]
fn prepare_secret_directory(directory: &Path) -> Result<(), SecretProviderError> {
    use std::os::unix::fs::DirBuilderExt;

    if !directory.exists() {
        let mut builder = fs::DirBuilder::new();
        builder.mode(0o700);
        match builder.create(directory) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(_) => return Err(SecretProviderError::Unavailable),
        }
    }
    check_restricted_directory(directory)
}

#[cfg(unix)]
fn check_restricted_directory(path: &Path) -> Result<(), SecretProviderError> {
    use std::os::unix::fs::PermissionsExt;

    let metadata = fs::symlink_metadata(path).map_err(|_| SecretProviderError::NotFound)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(SecretProviderError::InsecurePermissions);
    }
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(SecretProviderError::InsecurePermissions);
    }
    Ok(())
}

#[cfg(unix)]
fn latest_version(directory: &Path) -> Result<Option<SecretVersion>, SecretProviderError> {
    let entries = fs::read_dir(directory).map_err(|_| SecretProviderError::NotFound)?;
    let mut latest = None;
    for entry in entries {
        let entry = entry.map_err(|_| SecretProviderError::Unavailable)?;
        let file_name = entry.file_name();
        let Some(file_name) = file_name.to_str() else {
            return Err(SecretProviderError::InvalidEnvelope);
        };
        let Some(raw_version) = file_name.strip_suffix(".secret") else {
            if file_name.ends_with(".tmp") {
                continue;
            }
            return Err(SecretProviderError::InvalidEnvelope);
        };
        if raw_version.len() != 20 || !raw_version.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(SecretProviderError::InvalidEnvelope);
        }
        let version = raw_version
            .parse::<u64>()
            .map_err(|_| SecretProviderError::InvalidEnvelope)?;
        if version == 0 {
            return Err(SecretProviderError::InvalidEnvelope);
        }
        let metadata = entry.metadata().map_err(|_| SecretProviderError::Unavailable)?;
        if !metadata.is_file() {
            return Err(SecretProviderError::InvalidEnvelope);
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
fn read_restricted_file(path: &Path) -> Result<Vec<u8>, SecretProviderError> {
    use std::os::unix::fs::PermissionsExt;

    let link_metadata = fs::symlink_metadata(path).map_err(|_| SecretProviderError::NotFound)?;
    if link_metadata.file_type().is_symlink() {
        return Err(SecretProviderError::InsecurePermissions);
    }
    let mut file = File::open(path).map_err(|_| SecretProviderError::Unavailable)?;
    let metadata = file.metadata().map_err(|_| SecretProviderError::Unavailable)?;
    if !metadata.is_file() || metadata.permissions().mode() & 0o077 != 0 {
        return Err(SecretProviderError::InsecurePermissions);
    }
    let maximum = ENVELOPE_PREFIX_LENGTH + MAX_SECRET_LENGTH + AUTH_TAG_LENGTH;
    if metadata.len() > maximum as u64 {
        return Err(SecretProviderError::InvalidEnvelope);
    }
    let mut envelope = Vec::with_capacity(metadata.len() as usize);
    (&mut file)
        .take(maximum as u64 + 1)
        .read_to_end(&mut envelope)
        .map_err(|_| SecretProviderError::Unavailable)?;
    if envelope.len() > maximum {
        return Err(SecretProviderError::InvalidEnvelope);
    }
    Ok(envelope)
}

fn associated_data(name: &SecretName, version: SecretVersion) -> Vec<u8> {
    let mut data = Vec::with_capacity(32 + name.as_str().len());
    data.extend_from_slice(b"rocketmq-secret-envelope-v1\0");
    data.extend_from_slice(name.as_str().as_bytes());
    data.push(0);
    data.extend_from_slice(&version.get().to_be_bytes());
    data
}

fn encrypt_envelope(
    key: &SecretMaterial,
    name: &SecretName,
    version: SecretVersion,
    plaintext: &[u8],
) -> Result<Vec<u8>, SecretProviderError> {
    let cipher = Aes256Gcm::new_from_slice(key.expose_secret()).map_err(|_| SecretProviderError::InvalidMaterial)?;
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
        .map_err(|_| SecretProviderError::Unavailable)?;
    let mut envelope = Vec::with_capacity(ENVELOPE_PREFIX_LENGTH + ciphertext.len());
    envelope.extend_from_slice(ENVELOPE_MAGIC);
    envelope.extend_from_slice(&nonce);
    envelope.extend_from_slice(&(ciphertext.len() as u64).to_be_bytes());
    envelope.extend_from_slice(&ciphertext);
    Ok(envelope)
}

fn decrypt_envelope(
    key: &SecretMaterial,
    name: &SecretName,
    version: SecretVersion,
    envelope: &[u8],
) -> Result<Vec<u8>, SecretProviderError> {
    if envelope.len() < ENVELOPE_PREFIX_LENGTH + AUTH_TAG_LENGTH || &envelope[..ENVELOPE_MAGIC.len()] != ENVELOPE_MAGIC
    {
        return Err(SecretProviderError::InvalidEnvelope);
    }
    let nonce_start = ENVELOPE_MAGIC.len();
    let nonce_end = nonce_start + NONCE_LENGTH;
    let length_end = nonce_end + LENGTH_FIELD;
    let nonce = Nonce::<Aes256Gcm>::try_from(&envelope[nonce_start..nonce_end])
        .map_err(|_| SecretProviderError::InvalidEnvelope)?;
    let encoded_length = u64::from_be_bytes(
        envelope[nonce_end..length_end]
            .try_into()
            .map_err(|_| SecretProviderError::InvalidEnvelope)?,
    );
    let ciphertext = &envelope[length_end..];
    if encoded_length != ciphertext.len() as u64 || ciphertext.len() > MAX_SECRET_LENGTH + AUTH_TAG_LENGTH {
        return Err(SecretProviderError::InvalidEnvelope);
    }
    let cipher = Aes256Gcm::new_from_slice(key.expose_secret()).map_err(|_| SecretProviderError::InvalidMaterial)?;
    let aad = associated_data(name, version);
    cipher
        .decrypt(
            &nonce,
            Payload {
                msg: ciphertext,
                aad: &aad,
            },
        )
        .map_err(|_| SecretProviderError::InvalidEnvelope)
}

#[cfg(unix)]
fn publish_version(directory: &Path, version: SecretVersion, envelope: &[u8]) -> Result<(), SecretProviderError> {
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
    let mut file = options
        .open(&temporary_path)
        .map_err(|_| SecretProviderError::Unavailable)?;
    let write_result = (|| {
        file.write_all(envelope).map_err(|_| SecretProviderError::Unavailable)?;
        file.sync_all().map_err(|_| SecretProviderError::Unavailable)?;
        file.set_permissions(fs::Permissions::from_mode(0o400))
            .map_err(|_| SecretProviderError::Unavailable)?;
        fs::hard_link(&temporary_path, &final_path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                SecretProviderError::VersionConflict
            } else {
                SecretProviderError::Unavailable
            }
        })?;
        sync_directory(directory)
    })();
    drop(file);
    let _ = fs::remove_file(&temporary_path);
    write_result
}

#[cfg(unix)]
fn sync_directory(directory: &Path) -> Result<(), SecretProviderError> {
    File::open(directory)
        .and_then(|file| file.sync_all())
        .map_err(|_| SecretProviderError::Unavailable)
}

#[cfg(test)]
mod tests {
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
            decrypt_envelope(&key, &name, version, &envelope).unwrap_err(),
            SecretProviderError::InvalidEnvelope
        );
        let other_name = SecretName::new("other-key").unwrap();
        assert_eq!(
            decrypt_envelope(&key, &other_name, version, &envelope).unwrap_err(),
            SecretProviderError::InvalidEnvelope
        );
    }
}
