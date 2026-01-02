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

use cheetah_string::CheetahString;

use crate::migration::alc::plain_access_config::PlainAccessConfig;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AclConfig {
    pub global_white_addrs: Option<Vec<CheetahString>>,
    pub plain_access_configs: Option<Vec<PlainAccessConfig>>,
}

impl AclConfig {
    pub fn new() -> Self {
        Self::default()
    }

    // globalWhiteAddrs
    pub fn global_white_addrs(&self) -> Option<&[CheetahString]> {
        self.global_white_addrs.as_deref()
    }

    pub fn set_global_white_addrs(&mut self, addrs: Vec<CheetahString>) {
        self.global_white_addrs = Some(addrs);
    }

    // plainAccessConfigs
    pub fn plain_access_configs(&self) -> Option<&[PlainAccessConfig]> {
        self.plain_access_configs.as_deref()
    }

    pub fn set_plain_access_configs(&mut self, configs: Vec<PlainAccessConfig>) {
        self.plain_access_configs = Some(configs);
    }
}

impl fmt::Display for AclConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "AclConfig{{ global_white_addrs={:?}, plain_access_configs={:?} }}",
            self.global_white_addrs, self.plain_access_configs
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acl_config_default_and_new() {
        let config = AclConfig::default();
        assert!(config.global_white_addrs().is_none());
        assert!(config.plain_access_configs().is_none());

        let config = AclConfig::new();
        assert!(config.global_white_addrs().is_none());
        assert!(config.plain_access_configs().is_none());
    }

    #[test]
    fn acl_config_setters_and_getters() {
        let mut config = AclConfig::new();

        let addrs = vec![CheetahString::from("127.0.0.1")];
        config.set_global_white_addrs(addrs.clone());
        assert_eq!(config.global_white_addrs(), Some(addrs.as_slice()));

        let plain_configs = vec![PlainAccessConfig::default()];
        config.set_plain_access_configs(plain_configs.clone());
        assert_eq!(config.plain_access_configs(), Some(plain_configs.as_slice()));
    }

    #[test]
    fn acl_config_display() {
        let config = AclConfig::new();
        let display = format!("{}", config);
        assert!(display.contains("AclConfig"));
        assert!(display.contains("global_white_addrs=None"));
        assert!(display.contains("plain_access_configs=None"));
    }
}
