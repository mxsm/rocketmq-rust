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

use std::collections::HashSet;
use std::path::Path;
use std::path::PathBuf;

use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use tokio::fs;

use crate::acl::validator::validate_acl_config;
use crate::migration::alc::acl_config::AclConfig;
use crate::migration::alc::plain_access_config::PlainAccessConfig;
use crate::migration::alc::plain_access_data::PlainAccessData;

#[derive(Clone, Debug)]
pub struct FileAclConfigLoader {
    roots: Vec<PathBuf>,
}

impl FileAclConfigLoader {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            roots: vec![root.into()],
        }
    }

    pub fn from_roots<I, P>(roots: I) -> Self
    where
        I: IntoIterator<Item = P>,
        P: Into<PathBuf>,
    {
        Self {
            roots: roots.into_iter().map(Into::into).collect(),
        }
    }

    pub async fn load(&self) -> RocketMQResult<AclConfig> {
        let files = self.discover_files().await?;
        let config = load_files(files).await?;
        validate_acl_config(&config)?;
        Ok(config)
    }

    pub async fn discover_files(&self) -> RocketMQResult<Vec<PathBuf>> {
        let roots = self.roots.clone();
        tokio::task::spawn_blocking(move || discover_acl_files(&roots))
            .await
            .map_err(|error| RocketMQError::Internal(format!("acl file discovery task failed: {error}")))?
    }
}

async fn load_files(files: Vec<PathBuf>) -> RocketMQResult<AclConfig> {
    let mut accounts = Vec::new();
    let mut global_white_addrs = Vec::new();
    let mut seen_access_keys = HashSet::new();

    for file in files {
        let bytes = fs::read(&file)
            .await
            .map_err(|error| RocketMQError::StorageReadFailed {
                path: file.display().to_string(),
                reason: error.to_string(),
            })?;
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
    Ok(config)
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
}
