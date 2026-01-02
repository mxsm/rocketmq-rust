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

use cheetah_string::CheetahString;

#[derive(Clone, Debug)]
pub struct AuthConfig {
    pub config_name: CheetahString,
    pub cluster_name: CheetahString,
    pub auth_config_path: CheetahString,

    pub authentication_enabled: bool,
    pub authentication_provider: CheetahString,
    pub authentication_metadata_provider: CheetahString,
    pub authentication_strategy: CheetahString,
    pub authentication_whitelist: CheetahString,
    pub init_authentication_user: CheetahString,
    pub inner_client_authentication_credentials: CheetahString,

    pub authorization_enabled: bool,
    pub authorization_provider: CheetahString,
    pub authorization_metadata_provider: CheetahString,
    pub authorization_strategy: CheetahString,
    pub authorization_whitelist: CheetahString,

    pub migrate_auth_from_v1_enabled: bool,

    pub user_cache_max_num: u32,
    pub user_cache_expired_second: u32,
    pub user_cache_refresh_second: u32,

    pub acl_cache_max_num: u32,
    pub acl_cache_expired_second: u32,
    pub acl_cache_refresh_second: u32,

    pub stateful_authentication_cache_max_num: u32,
    pub stateful_authentication_cache_expired_second: u32,
    pub stateful_authorization_cache_max_num: u32,
    pub stateful_authorization_cache_expired_second: u32,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            config_name: CheetahString::new(),
            cluster_name: CheetahString::new(),
            auth_config_path: CheetahString::new(),

            authentication_enabled: false,
            authentication_provider: CheetahString::new(),
            authentication_metadata_provider: CheetahString::new(),
            authentication_strategy: CheetahString::new(),
            authentication_whitelist: CheetahString::new(),
            init_authentication_user: CheetahString::new(),
            inner_client_authentication_credentials: CheetahString::new(),

            authorization_enabled: false,
            authorization_provider: CheetahString::new(),
            authorization_metadata_provider: CheetahString::new(),
            authorization_strategy: CheetahString::new(),
            authorization_whitelist: CheetahString::new(),

            migrate_auth_from_v1_enabled: false,

            user_cache_max_num: 1000,
            user_cache_expired_second: 600,
            user_cache_refresh_second: 60,

            acl_cache_max_num: 1000,
            acl_cache_expired_second: 600,
            acl_cache_refresh_second: 60,

            stateful_authentication_cache_max_num: 10000,
            stateful_authentication_cache_expired_second: 60,
            stateful_authorization_cache_max_num: 10000,
            stateful_authorization_cache_expired_second: 60,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_config_default_values() {
        let config = AuthConfig::default();

        // String fields
        assert!(config.config_name.is_empty());
        assert!(config.cluster_name.is_empty());
        assert!(config.auth_config_path.is_empty());

        assert!(config.authentication_provider.is_empty());
        assert!(config.authentication_metadata_provider.is_empty());
        assert!(config.authentication_strategy.is_empty());
        assert!(config.authentication_whitelist.is_empty());
        assert!(config.init_authentication_user.is_empty());
        assert!(config.inner_client_authentication_credentials.is_empty());

        assert!(config.authorization_provider.is_empty());
        assert!(config.authorization_metadata_provider.is_empty());
        assert!(config.authorization_strategy.is_empty());
        assert!(config.authorization_whitelist.is_empty());

        // Boolean fields
        assert!(!config.authentication_enabled);
        assert!(!config.authorization_enabled);
        assert!(!config.migrate_auth_from_v1_enabled);

        // Cache defaults
        assert_eq!(config.user_cache_max_num, 1000);
        assert_eq!(config.user_cache_expired_second, 600);
        assert_eq!(config.user_cache_refresh_second, 60);

        assert_eq!(config.acl_cache_max_num, 1000);
        assert_eq!(config.acl_cache_expired_second, 600);
        assert_eq!(config.acl_cache_refresh_second, 60);

        assert_eq!(config.stateful_authentication_cache_max_num, 10000);
        assert_eq!(config.stateful_authentication_cache_expired_second, 60);
        assert_eq!(config.stateful_authorization_cache_max_num, 10000);
        assert_eq!(config.stateful_authorization_cache_expired_second, 60);
    }
}
