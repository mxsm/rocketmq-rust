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
use std::fs;
use std::fs::File;
use std::io::Read;
use std::path::Path;

use cheetah_string::CheetahString;
use rocketmq_common::common::mix_all::ACL_CONF_TOOLS_FILE;
use rocketmq_common::utils::env_utils::EnvUtils;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use serde_yaml;

use crate::migration::alc::acl_config::AclConfig;
use crate::migration::alc::plain_access_config::PlainAccessConfig;
use crate::migration::alc::plain_access_data::PlainAccessData;

pub struct PlainPermissionManager {
    pub file_home: String,
    pub default_acl_dir: String,
    pub default_acl_file: String,
    pub file_list: Vec<String>,
}

impl PlainPermissionManager {
    pub fn new() -> Self {
        let file_home = EnvUtils::get_rocketmq_home();

        let default_acl_dir = Path::new(&file_home)
            .join("conf")
            .join("acl")
            .to_string_lossy()
            .to_string();

        let acl_file_prop =
            std::env::var("rocketmq.acl.plain.file").unwrap_or_else(|_| "conf/plain_acl.yml".to_string());

        let default_acl_file = Path::new(&file_home).join(acl_file_prop).to_string_lossy().to_string();

        let mut manager = Self {
            file_home,
            default_acl_dir,
            default_acl_file,
            file_list: Vec::new(),
        };

        // Try to load; if errors occur, log and continue with empty file_list
        if let Err(e) = manager.load() {
            tracing::error!("Failed to load ACL manager: {:#?}", e);
        }

        manager
    }

    pub fn get_all_acl_files<P: AsRef<Path>>(&self, path: P) -> Vec<String> {
        let path = path.as_ref();
        if !path.exists() {
            tracing::info!("The default acl dir {} is not exist", path.to_string_lossy());
            return Vec::new();
        }

        let mut all_acl_files = Vec::new();

        if let Ok(read_dir) = fs::read_dir(path) {
            for entry in read_dir.flatten() {
                let file_path = entry.path();
                let file_name = file_path.to_string_lossy().to_string();

                // Compare against tools file
                let tools_file = Path::new(&self.file_home)
                    .join(ACL_CONF_TOOLS_FILE.trim_start_matches('/'))
                    .to_string_lossy()
                    .to_string();

                if file_name == tools_file {
                    continue;
                } else if file_name.ends_with(".yml") || file_name.ends_with(".yaml") {
                    all_acl_files.push(file_name);
                } else if file_path.is_dir() {
                    let mut nested = self.get_all_acl_files(file_path);
                    all_acl_files.append(&mut nested);
                }
            }
        }

        all_acl_files
    }

    pub fn load(&mut self) -> Result<(), RocketMQError> {
        if self.file_home.is_empty() {
            return Ok(());
        }

        self.assure_acl_config_files_exist()?;

        self.file_list = self.get_all_acl_files(&self.default_acl_dir);

        if Path::new(&self.default_acl_file).exists() && !self.file_list.contains(&self.default_acl_file) {
            self.file_list.push(self.default_acl_file.clone());
        }

        Ok(())
    }

    fn assure_acl_config_files_exist(&self) -> RocketMQResult<()> {
        let default_acl_path = Path::new(&self.default_acl_file);
        if !default_acl_path.exists() {
            // Create file if it doesn't exist
            match File::create(default_acl_path) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => { /* maybe created by other threads */ }
                Err(e) => {
                    tracing::error!("Error in creating {}: {:?}", self.default_acl_file, e);
                    return Err(e.into());
                }
            }
        }
        Ok(())
    }

    /// Reads ACL YAML files and returns aggregated `AclConfig` (deduplicating access keys)
    pub fn get_all_acl_config(&self) -> RocketMQResult<AclConfig> {
        let mut acl_config = AclConfig::new();
        let mut configs: Vec<PlainAccessConfig> = Vec::new();
        let mut white_addrs: Vec<CheetahString> = Vec::new();
        let mut access_key_sets: HashSet<String> = HashSet::new();

        for path in &self.file_list {
            // Open file (return IO error if fail)
            let mut f = File::open(path).map_err(|e| {
                tracing::error!("ACL file {} cannot be opened: {:?}", path, e);
                RocketMQError::IO(e)
            })?;

            let mut content = String::new();
            f.read_to_string(&mut content).map_err(|e| {
                tracing::error!("Failed to read ACL file {}: {:?}", path, e);
                RocketMQError::IO(e)
            })?;

            let plain_acl_conf_data: PlainAccessData = serde_yaml::from_str(&content).map_err(|e| {
                tracing::error!("Failed to parse YAML {}: {:?}", path, e);
                // map to unified serialization error
                rocketmq_error::RocketMQError::Serialization(rocketmq_error::SerializationError::decode_failed(
                    "YAML",
                    e.to_string(),
                ))
            })?;

            let global_white_addrs = plain_acl_conf_data.global_white_remote_addresses();
            if !global_white_addrs.is_empty() {
                white_addrs.extend(global_white_addrs.iter().cloned());
            }

            let plain_access_configs = plain_acl_conf_data.accounts();
            if !plain_access_configs.is_empty() {
                for access_config in plain_access_configs {
                    if let Some(key_cs) = access_config.access_key() {
                        let key = key_cs.as_str().to_string();
                        if !access_key_sets.contains(&key) {
                            access_key_sets.insert(key);
                            // clone the config and push
                            configs.push(access_config.clone());
                        }
                    }
                }
            }
        }

        if !configs.is_empty() {
            acl_config.set_plain_access_configs(configs);
        }

        if !white_addrs.is_empty() {
            acl_config.set_global_white_addrs(white_addrs);
        }

        Ok(acl_config)
    }
}

impl Default for PlainPermissionManager {
    fn default() -> Self {
        Self::new()
    }
}
#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::TempDir;

    use super::*;

    #[test]
    fn test_get_all_acl_files_and_load() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();

        // Create expected directory structure
        let conf_acl = root.join("conf").join("acl");
        fs::create_dir_all(&conf_acl).unwrap();

        // tools file that should be excluded
        let tools = root.join("conf").join("tools.yml");
        fs::write(&tools, "").unwrap();

        // acl files
        let f1 = conf_acl.join("a.yml");
        fs::write(
            &f1,
            "
global_white_remote_addresses: [\"1.2.3.4\"]
accounts:
  - access_key: \"ak\"
    secret_key: \"sk\"
    white_remote_address: \"1.2.3.4\"
    admin: true
    default_topic_perm: \"DENY\"
    default_group_perm: \"DENY\"
    topic_perms: [\"t1\"]
    group_perms: [\"g1\"]
",
        )
        .unwrap();

        // nested dir
        let nested = conf_acl.join("nested");
        fs::create_dir_all(&nested).unwrap();
        let f2 = nested.join("b.yaml");
        fs::write(&f2, "accounts: []\n").unwrap();

        let mut mgr = PlainPermissionManager::new();
        mgr.file_home = root.to_string_lossy().to_string();
        mgr.default_acl_dir = conf_acl.to_string_lossy().to_string();
        mgr.default_acl_file = root.join("conf").join("plain_acl.yml").to_string_lossy().to_string();

        // test discovery
        let files = mgr.get_all_acl_files(&mgr.default_acl_dir);
        assert!(files.iter().any(|p| p.ends_with("a.yml")));
        assert!(files
            .iter()
            .any(|p| p.ends_with("nested\\b.yaml") || p.ends_with("nested/b.yaml")));
        assert!(!files.iter().any(|p| p.ends_with("tools.yml")));

        // load and parse
        mgr.file_list = files;
        let acl = mgr.get_all_acl_config().unwrap();
        let plain = acl.plain_access_configs().unwrap();
        assert_eq!(plain.len(), 1);
        assert_eq!(plain[0].access_key().unwrap().as_str(), "ak");
        let whites = acl.global_white_addrs().unwrap();
        assert_eq!(whites.len(), 1);
        assert_eq!(whites[0].as_str(), "1.2.3.4");
    }

    #[test]
    fn test_get_all_acl_config_parse_error() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();

        let conf_acl = root.join("conf").join("acl");
        fs::create_dir_all(&conf_acl).unwrap();

        let f1 = conf_acl.join("bad.yml");
        // invalid YAML (unterminated sequence)
        fs::write(&f1, "accounts: [").unwrap();

        let mut mgr = PlainPermissionManager::new();
        mgr.file_home = root.to_string_lossy().to_string();
        mgr.default_acl_dir = conf_acl.to_string_lossy().to_string();
        mgr.file_list = mgr.get_all_acl_files(&mgr.default_acl_dir);

        let res = mgr.get_all_acl_config();
        assert!(res.is_err());
        match res.err().unwrap() {
            rocketmq_error::RocketMQError::Serialization(_) => {}
            other => panic!("expected serialization error, got {other:?}"),
        }
    }
}
