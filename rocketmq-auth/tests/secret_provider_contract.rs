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

use std::sync::Arc;

use rocketmq_auth::EncryptedFileSecretProvider;
use rocketmq_auth::EnvironmentSecretProvider;
use rocketmq_auth::SecretProviderRegistry;
use rocketmq_security_api::SecretAccess;
use rocketmq_security_api::SecretMaterial;
use rocketmq_security_api::SecretName;
use rocketmq_security_api::SecretProvider;
use rocketmq_security_api::SecretProviderError;
use rocketmq_security_api::SecretProviderId;
#[cfg(unix)]
use rocketmq_security_api::SecretVersion;

fn provider_id(value: &str) -> SecretProviderId {
    SecretProviderId::new(value).unwrap()
}

fn secret_name(value: &str) -> SecretName {
    SecretName::new(value).unwrap()
}

fn material(value: &[u8]) -> SecretMaterial {
    SecretMaterial::new(value.to_vec()).unwrap()
}

#[test]
fn registry_requires_exact_explicit_provider() {
    let id = provider_id("environment-local");
    let provider = Arc::new(
        EnvironmentSecretProvider::new(id.clone(), [(secret_name("process-path"), "PATH".to_owned())]).unwrap(),
    );
    let mut registry = SecretProviderRegistry::new();

    assert_eq!(
        registry.provider(&id).err(),
        Some(SecretProviderError::ProviderNotRegistered)
    );
    registry.register(provider.clone()).unwrap();
    assert_eq!(registry.len(), 1);
    assert_eq!(registry.provider(&id).unwrap().id(), &id);
    assert_eq!(
        registry.register(provider).unwrap_err(),
        SecretProviderError::DuplicateProvider
    );
}

#[test]
fn environment_adapter_uses_only_mapped_names_and_is_read_only() {
    let mapped_name = secret_name("process-path");
    let provider = EnvironmentSecretProvider::new(
        provider_id("environment-local"),
        [(mapped_name.clone(), "PATH".to_owned())],
    )
    .unwrap();

    assert_eq!(provider.capabilities().access(), SecretAccess::ReadOnly);
    let value = provider.read(&mapped_name).unwrap();
    assert!(!value.material().is_empty());
    assert_eq!(value.version(), None);
    assert_eq!(
        provider.read(&secret_name("unmapped-name")).unwrap_err(),
        SecretProviderError::NotFound
    );
    assert_eq!(
        provider
            .write(&mapped_name, material(b"must-be-dropped"), None)
            .unwrap_err(),
        SecretProviderError::ReadOnly
    );
    let debug = format!("{provider:?}");
    assert!(!debug.contains("PATH"));
}

#[cfg(windows)]
#[test]
fn encrypted_file_adapter_fails_closed_without_acl_verification() {
    let directory = tempfile::tempdir().unwrap();
    let error = EncryptedFileSecretProvider::new(provider_id("encrypted-local"), directory.path(), material(&[3; 32]))
        .unwrap_err();
    assert_eq!(error, SecretProviderError::UnsupportedPlatform);
}

#[cfg(unix)]
#[test]
fn encrypted_file_adapter_enforces_permissions_encryption_atomicity_and_versions() {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("secrets");
    let provider = EncryptedFileSecretProvider::new(provider_id("encrypted-local"), &root, material(&[3; 32])).unwrap();
    let name = secret_name("broker-admin-key");
    let first_plaintext = b"first-plaintext-must-not-appear";
    let first_version = provider.write(&name, material(first_plaintext), None).unwrap();
    assert_eq!(first_version, SecretVersion::new(1));
    let first = provider.read(&name).unwrap();
    assert_eq!(first.material().expose_secret(), first_plaintext);
    assert_eq!(first.version(), Some(first_version));

    let second_plaintext = b"second-plaintext-must-not-appear";
    let second_version = provider
        .write(&name, material(second_plaintext), Some(first_version))
        .unwrap();
    assert_eq!(second_version, SecretVersion::new(2));
    assert_eq!(
        provider
            .write(&name, material(b"stale-update"), Some(first_version))
            .unwrap_err(),
        SecretProviderError::VersionConflict
    );

    let secret_directory = root.join(name.as_str());
    let mut versions = fs::read_dir(&secret_directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    versions.sort();
    assert_eq!(versions.len(), 2);
    for path in &versions {
        let bytes = fs::read(path).unwrap();
        assert!(!bytes
            .windows(first_plaintext.len())
            .any(|window| window == first_plaintext));
        assert!(!bytes
            .windows(second_plaintext.len())
            .any(|window| window == second_plaintext));
        assert_eq!(fs::metadata(path).unwrap().permissions().mode() & 0o077, 0);
    }
    assert_eq!(
        provider.read(&name).unwrap().material().expose_secret(),
        second_plaintext
    );

    let latest = versions.last().unwrap();
    fs::set_permissions(latest, fs::Permissions::from_mode(0o600)).unwrap();
    let mut tampered = fs::read(latest).unwrap();
    *tampered.last_mut().unwrap() ^= 1;
    fs::write(latest, tampered).unwrap();
    fs::set_permissions(latest, fs::Permissions::from_mode(0o400)).unwrap();
    assert_eq!(provider.read(&name).unwrap_err(), SecretProviderError::InvalidEnvelope);
}

#[cfg(unix)]
#[test]
fn encrypted_file_adapter_rejects_broad_root_permissions() {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("insecure");
    fs::create_dir(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o755)).unwrap();
    assert_eq!(
        EncryptedFileSecretProvider::new(provider_id("encrypted-local"), root, material(&[4; 32])).unwrap_err(),
        SecretProviderError::InsecurePermissions
    );
}
