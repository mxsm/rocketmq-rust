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

use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;
use std::hash::Hash;
use std::hash::Hasher;
use std::path::Path;
use std::path::PathBuf;

use cheetah_string::CheetahString;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use tokio::fs;

use crate::acl::validator::validate_acl_config;
use crate::migration::alc::acl_config::AclConfig;
use crate::migration::alc::plain_access_config::PlainAccessConfig;
use crate::migration::alc::plain_access_data::DataVersion;
use crate::migration::alc::plain_access_data::PlainAccessData;
use crate::runtime_bridge::AuthBlockingExecutor;

#[derive(Clone, Debug)]
pub struct FileAclConfigLoader {
    roots: Vec<PathBuf>,
    blocking: AuthBlockingExecutor,
}

#[derive(Clone, Debug)]
pub struct FileAclConfigStore {
    file: PathBuf,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AclConfigFingerprint(u64);

impl FileAclConfigLoader {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            roots: vec![root.into()],
            blocking: AuthBlockingExecutor::default(),
        }
    }

    pub fn from_roots<I, P>(roots: I) -> Self
    where
        I: IntoIterator<Item = P>,
        P: Into<PathBuf>,
    {
        Self {
            roots: roots.into_iter().map(Into::into).collect(),
            blocking: AuthBlockingExecutor::default(),
        }
    }

    pub async fn load(&self) -> RocketMQResult<AclConfig> {
        let files = self.discover_files().await?;
        let (config, _) = load_files(files).await?;
        validate_acl_config(&config)?;
        Ok(config)
    }

    pub async fn load_with_fingerprint(&self) -> RocketMQResult<(AclConfig, AclConfigFingerprint)> {
        let files = self.discover_files().await?;
        let (config, fingerprint) = load_files(files).await?;
        validate_acl_config(&config)?;
        Ok((config, fingerprint))
    }

    pub async fn discover_files(&self) -> RocketMQResult<Vec<PathBuf>> {
        let roots = self.roots.clone();
        self.blocking
            .spawn_io("auth.acl.discover_files", move || discover_acl_files(&roots))
            .await
            .map_err(|error| RocketMQError::storage_read_failed("auth.acl.discover_files", error.to_string()))?
    }
}

impl FileAclConfigStore {
    pub fn new(file: impl Into<PathBuf>) -> Self {
        Self { file: file.into() }
    }

    pub async fn update_global_white_remote_addresses<I, S>(&self, addresses: I) -> RocketMQResult<()>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let addresses: Vec<_> = addresses
            .into_iter()
            .map(|address| address.as_ref().trim().to_owned())
            .filter(|address| !address.is_empty())
            .map(CheetahString::from)
            .collect();
        if addresses.is_empty() {
            return Err(RocketMQError::illegal_argument("The globalWhiteAddrs is blank"));
        }

        let mut data = self.load_plain_access_data().await?;
        data.set_global_white_remote_addresses(addresses);
        bump_data_version(&mut data);
        self.write_plain_access_data(&data).await
    }

    async fn load_plain_access_data(&self) -> RocketMQResult<PlainAccessData> {
        let bytes = match fs::read(&self.file).await {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(PlainAccessData::default()),
            Err(error) => {
                return Err(RocketMQError::StorageReadFailed {
                    path: self.file.display().to_string(),
                    reason: error.to_string(),
                });
            }
        };

        if bytes.iter().all(u8::is_ascii_whitespace) {
            return Ok(PlainAccessData::default());
        }

        serde_yaml::from_slice(&bytes)
            .map_err(|error| RocketMQError::deserialization_failed("YAML", format!("{}: {error}", self.file.display())))
    }

    async fn write_plain_access_data(&self, data: &PlainAccessData) -> RocketMQResult<()> {
        let content = serde_yaml::to_string(data).map_err(|error| {
            RocketMQError::Serialization(rocketmq_error::SerializationError::encode_failed(
                "YAML",
                error.to_string(),
            ))
        })?;

        if let Some(parent) = self.file.parent() {
            fs::create_dir_all(parent).await.map_err(|error| {
                RocketMQError::storage_write_failed(parent.display().to_string(), error.to_string())
            })?;
        }

        if fs::metadata(&self.file).await.is_ok() {
            let backup = backup_file_path(&self.file);
            fs::copy(&self.file, &backup).await.map_err(|error| {
                RocketMQError::storage_write_failed(backup.display().to_string(), error.to_string())
            })?;
        }

        let temp_file = temp_file_path(&self.file);
        fs::write(&temp_file, content)
            .await
            .map_err(|error| RocketMQError::storage_write_failed(temp_file.display().to_string(), error.to_string()))?;

        match fs::rename(&temp_file, &self.file).await {
            Ok(()) => Ok(()),
            Err(rename_error) => {
                fs::copy(&temp_file, &self.file).await.map_err(|error| {
                    RocketMQError::storage_write_failed(
                        self.file.display().to_string(),
                        format!("{error}; rename failed first: {rename_error}"),
                    )
                })?;
                let _ = fs::remove_file(&temp_file).await;
                Ok(())
            }
        }
    }
}

async fn load_files(files: Vec<PathBuf>) -> RocketMQResult<(AclConfig, AclConfigFingerprint)> {
    let mut accounts = Vec::new();
    let mut global_white_addrs = Vec::new();
    let mut seen_access_keys = HashSet::new();
    let mut hasher = DefaultHasher::new();

    for file in files {
        file.hash(&mut hasher);
        let bytes = fs::read(&file)
            .await
            .map_err(|error| RocketMQError::StorageReadFailed {
                path: file.display().to_string(),
                reason: error.to_string(),
            })?;
        bytes.hash(&mut hasher);
        if bytes.iter().all(u8::is_ascii_whitespace) {
            continue;
        }

        let data: PlainAccessData = serde_yaml::from_slice(&bytes)
            .map_err(|error| RocketMQError::deserialization_failed("YAML", format!("{}: {error}", file.display())))?;

        global_white_addrs.extend(data.global_white_remote_addresses().iter().cloned());
        extend_accounts(&mut accounts, &mut seen_access_keys, data.accounts());
    }

    let mut config = AclConfig::new();
    if !global_white_addrs.is_empty() {
        config.set_global_white_addrs(global_white_addrs);
    }
    if !accounts.is_empty() {
        config.set_plain_access_configs(accounts);
    }
    Ok((config, AclConfigFingerprint(hasher.finish())))
}

fn extend_accounts(
    accounts: &mut Vec<PlainAccessConfig>,
    seen_access_keys: &mut HashSet<String>,
    incoming: &[PlainAccessConfig],
) {
    for account in incoming {
        let Some(access_key) = account.access_key() else {
            continue;
        };
        if seen_access_keys.insert(access_key.to_string()) {
            accounts.push(account.clone());
        }
    }
}

fn discover_acl_files(roots: &[PathBuf]) -> RocketMQResult<Vec<PathBuf>> {
    let mut files = Vec::new();
    for root in roots {
        collect_acl_files(root, &mut files)?;
    }
    Ok(files)
}

fn collect_acl_files(path: &Path, files: &mut Vec<PathBuf>) -> RocketMQResult<()> {
    let metadata = std::fs::metadata(path).map_err(|error| RocketMQError::StorageReadFailed {
        path: path.display().to_string(),
        reason: error.to_string(),
    })?;

    if metadata.is_file() {
        if is_acl_yaml(path) {
            files.push(path.to_path_buf());
        }
        return Ok(());
    }

    if metadata.is_dir() {
        let mut entries = Vec::new();
        for entry in std::fs::read_dir(path).map_err(|error| RocketMQError::StorageReadFailed {
            path: path.display().to_string(),
            reason: error.to_string(),
        })? {
            let entry = entry.map_err(|error| RocketMQError::StorageReadFailed {
                path: path.display().to_string(),
                reason: error.to_string(),
            })?;
            entries.push(entry.path());
        }
        entries.sort();
        for entry in entries {
            collect_acl_files(&entry, files)?;
        }
    }

    Ok(())
}

fn is_acl_yaml(path: &Path) -> bool {
    let Some(extension) = path.extension().and_then(|value| value.to_str()) else {
        return false;
    };
    if !extension.eq_ignore_ascii_case("yml") && !extension.eq_ignore_ascii_case("yaml") {
        return false;
    }
    path.file_name()
        .and_then(|value| value.to_str())
        .is_none_or(|file_name| !file_name.eq_ignore_ascii_case("tools.yml"))
}

fn backup_file_path(path: &Path) -> PathBuf {
    let mut backup = path.as_os_str().to_os_string();
    backup.push(".bak");
    PathBuf::from(backup)
}

fn temp_file_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("plain_acl.yml");
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    path.with_file_name(format!(".{file_name}.{}.tmp", nanos))
}

fn bump_data_version(data: &mut PlainAccessData) {
    let counter = data
        .latest_version()
        .map_or(1, |version| version.counter.saturating_add(1));
    data.set_data_version(vec![DataVersion {
        timestamp: current_time_millis(),
        counter,
    }]);
}

fn current_time_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use cheetah_string::CheetahString;
    use tempfile::TempDir;

    use super::*;

    #[tokio::test]
    async fn discovers_yaml_files_recursively_and_skips_tools_file() {
        let temp = TempDir::new().unwrap();
        let root = temp.path();
        let nested = root.join("nested");
        fs::create_dir_all(&nested).unwrap();
        fs::write(root.join("a.yml"), "").unwrap();
        fs::write(root.join("tools.yml"), "").unwrap();
        fs::write(nested.join("b.yaml"), "").unwrap();
        fs::write(nested.join("ignored.txt"), "").unwrap();

        let files = FileAclConfigLoader::new(root).discover_files().await.unwrap();

        assert_eq!(files.len(), 2);
        assert!(files.iter().any(|path| path.ends_with("a.yml")));
        assert!(files.iter().any(|path| path.ends_with("b.yaml")));
        assert!(!files.iter().any(|path| path.ends_with("tools.yml")));
    }

    #[tokio::test]
    async fn loads_java_acl_files_and_keeps_first_duplicate_access_key() {
        let temp = TempDir::new().unwrap();
        let first = temp.path().join("first.yml");
        let second = temp.path().join("second.yml");
        fs::write(
            &first,
            r#"
globalWhiteRemoteAddresses:
  - 10.0.0.1
accounts:
  - accessKey: ak
    secretKey: first-secret
    admin: true
"#,
        )
        .unwrap();
        fs::write(
            &second,
            r#"
globalWhiteRemoteAddresses:
  - 10.0.0.2
accounts:
  - accessKey: ak
    secretKey: second-secret
  - accessKey: bk
    secretKey: bk-secret
"#,
        )
        .unwrap();

        let config = FileAclConfigLoader::from_roots([second, first]).load().await.unwrap();

        let white_addrs = config.global_white_addrs().unwrap();
        assert_eq!(
            white_addrs,
            &[CheetahString::from("10.0.0.2"), CheetahString::from("10.0.0.1")]
        );

        let accounts = config.plain_access_configs().unwrap();
        assert_eq!(accounts.len(), 2);
        assert_eq!(accounts[0].access_key().unwrap().as_str(), "ak");
        assert_eq!(accounts[0].secret_key().unwrap().as_str(), "second-secret");
        assert_eq!(accounts[1].access_key().unwrap().as_str(), "bk");
    }

    #[tokio::test]
    async fn empty_files_load_as_empty_config() {
        let temp = TempDir::new().unwrap();
        let file = temp.path().join("plain_acl.yml");
        fs::write(&file, " \n\t").unwrap();

        let config = FileAclConfigLoader::new(file).load().await.unwrap();

        assert!(config.global_white_addrs().is_none());
        assert!(config.plain_access_configs().is_none());
    }

    #[tokio::test]
    async fn fingerprint_changes_when_acl_file_content_changes() {
        let temp = TempDir::new().unwrap();
        let file = temp.path().join("plain_acl.yml");
        fs::write(
            &file,
            r#"
accounts:
  - accessKey: ak
    secretKey: first
"#,
        )
        .unwrap();

        let loader = FileAclConfigLoader::new(&file);
        let (_, first_fingerprint) = loader.load_with_fingerprint().await.unwrap();
        let (_, same_fingerprint) = loader.load_with_fingerprint().await.unwrap();
        assert_eq!(first_fingerprint, same_fingerprint);

        fs::write(
            &file,
            r#"
accounts:
  - accessKey: ak
    secretKey: second
"#,
        )
        .unwrap();

        let (_, updated_fingerprint) = loader.load_with_fingerprint().await.unwrap();
        assert_ne!(first_fingerprint, updated_fingerprint);
    }

    #[tokio::test]
    async fn rejects_acl_file_with_missing_secret_key() {
        let temp = TempDir::new().unwrap();
        let file = temp.path().join("plain_acl.yml");
        fs::write(
            &file,
            r#"
accounts:
  - accessKey: ak
"#,
        )
        .unwrap();

        let error = FileAclConfigLoader::new(file).load().await.unwrap_err();

        assert!(error.to_string().contains("secretKey must not be blank"));
        assert!(!error.to_string().contains("secret="));
    }

    #[tokio::test]
    async fn store_updates_global_white_remote_addresses_and_preserves_accounts() {
        let temp = TempDir::new().unwrap();
        let file = temp.path().join("plain_acl.yml");
        fs::write(
            &file,
            r#"
globalWhiteRemoteAddresses:
  - 10.10.*.*
accounts:
  - accessKey: ak
    secretKey: sk
    defaultTopicPerm: PUB
"#,
        )
        .unwrap();

        FileAclConfigStore::new(&file)
            .update_global_white_remote_addresses(["172.16.*.*", "192.168.0.*"])
            .await
            .unwrap();

        let content = fs::read_to_string(&file).unwrap();
        let data: PlainAccessData = serde_yaml::from_str(&content).unwrap();
        assert_eq!(
            data.global_white_remote_addresses(),
            &[
                CheetahString::from_static_str("172.16.*.*"),
                CheetahString::from_static_str("192.168.0.*")
            ]
        );
        assert_eq!(data.accounts().len(), 1);
        assert_eq!(data.accounts()[0].access_key().unwrap().as_str(), "ak");
        assert_eq!(data.accounts()[0].secret_key().unwrap().as_str(), "sk");
        assert!(content.contains("globalWhiteRemoteAddresses"));
        assert!(content.contains("accessKey"));
    }

    #[tokio::test]
    async fn store_creates_missing_acl_file_for_global_white_remote_addresses() {
        let temp = TempDir::new().unwrap();
        let file = temp.path().join("conf").join("plain_acl.yml");

        FileAclConfigStore::new(&file)
            .update_global_white_remote_addresses(["10.10.*.*"])
            .await
            .unwrap();

        let data: PlainAccessData = serde_yaml::from_str(&fs::read_to_string(&file).unwrap()).unwrap();
        assert_eq!(
            data.global_white_remote_addresses(),
            &[CheetahString::from_static_str("10.10.*.*")]
        );
        assert!(data.accounts().is_empty());
    }

    #[tokio::test]
    async fn store_bumps_data_version_when_global_white_remote_addresses_change() {
        let temp = TempDir::new().unwrap();
        let file = temp.path().join("plain_acl.yml");
        fs::write(
            &file,
            r#"
globalWhiteRemoteAddresses:
  - 10.10.*.*
dataVersion:
  - timestamp: 100
    counter: 7
accounts:
  - accessKey: ak
    secretKey: sk
"#,
        )
        .unwrap();

        FileAclConfigStore::new(&file)
            .update_global_white_remote_addresses(["172.16.*.*"])
            .await
            .unwrap();

        let data: PlainAccessData = serde_yaml::from_str(&fs::read_to_string(&file).unwrap()).unwrap();
        let version = data.latest_version().unwrap();
        assert_eq!(version.counter, 8);
        assert!(version.timestamp > 100);
    }

    #[tokio::test]
    async fn store_creates_data_version_when_missing() {
        let temp = TempDir::new().unwrap();
        let file = temp.path().join("plain_acl.yml");

        FileAclConfigStore::new(&file)
            .update_global_white_remote_addresses(["10.10.*.*"])
            .await
            .unwrap();

        let data: PlainAccessData = serde_yaml::from_str(&fs::read_to_string(&file).unwrap()).unwrap();
        let version = data.latest_version().unwrap();
        assert_eq!(version.counter, 1);
        assert!(version.timestamp > 0);
    }
}
