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
use serde::Deserialize;
use serde::Serialize;

use crate::authentication::acl_signer::SignatureAlgorithm;

#[derive(Clone, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct AuthConfig {
    pub config_name: CheetahString,
    pub cluster_name: CheetahString,
    pub auth_config_path: CheetahString,
    pub acl_file: CheetahString,
    pub acl_file_watch_enabled: bool,
    pub acl_file_watch_interval_millis: u64,

    pub authentication_enabled: bool,
    pub authentication_provider: CheetahString,
    pub authentication_metadata_provider: CheetahString,
    pub authentication_strategy: CheetahString,
    pub authentication_whitelist: CheetahString,
    pub init_authentication_user: CheetahString,
    pub inner_client_authentication_credentials: CheetahString,
    pub signature_algorithm: SignatureAlgorithm,
    /// Request replay-protection window in milliseconds.
    ///
    /// `0` keeps Java-compatible behavior and disables timestamp expiry checks.
    pub request_timestamp_expired_millis: u64,

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
    pub stateful_authorization_cache_negative_enable: bool,
}

impl fmt::Debug for AuthConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthConfig")
            .field("config_name", &self.config_name)
            .field("cluster_name", &self.cluster_name)
            .field("auth_config_path", &self.auth_config_path)
            .field("acl_file", &self.acl_file)
            .field("acl_file_watch_enabled", &self.acl_file_watch_enabled)
            .field("acl_file_watch_interval_millis", &self.acl_file_watch_interval_millis)
            .field("authentication_enabled", &self.authentication_enabled)
            .field("authentication_provider", &self.authentication_provider)
            .field(
                "authentication_metadata_provider",
                &self.authentication_metadata_provider,
            )
            .field("authentication_strategy", &self.authentication_strategy)
            .field("authentication_whitelist", &self.authentication_whitelist)
            .field(
                "init_authentication_user",
                &redacted_if_present(&self.init_authentication_user),
            )
            .field(
                "inner_client_authentication_credentials",
                &redacted_if_present(&self.inner_client_authentication_credentials),
            )
            .field("signature_algorithm", &self.signature_algorithm)
            .field(
                "request_timestamp_expired_millis",
                &self.request_timestamp_expired_millis,
            )
            .field("authorization_enabled", &self.authorization_enabled)
            .field("authorization_provider", &self.authorization_provider)
            .field("authorization_metadata_provider", &self.authorization_metadata_provider)
            .field("authorization_strategy", &self.authorization_strategy)
            .field("authorization_whitelist", &self.authorization_whitelist)
            .field("migrate_auth_from_v1_enabled", &self.migrate_auth_from_v1_enabled)
            .field("user_cache_max_num", &self.user_cache_max_num)
            .field("user_cache_expired_second", &self.user_cache_expired_second)
            .field("user_cache_refresh_second", &self.user_cache_refresh_second)
            .field("acl_cache_max_num", &self.acl_cache_max_num)
            .field("acl_cache_expired_second", &self.acl_cache_expired_second)
            .field("acl_cache_refresh_second", &self.acl_cache_refresh_second)
            .field(
                "stateful_authentication_cache_max_num",
                &self.stateful_authentication_cache_max_num,
            )
            .field(
                "stateful_authentication_cache_expired_second",
                &self.stateful_authentication_cache_expired_second,
            )
            .field(
                "stateful_authorization_cache_max_num",
                &self.stateful_authorization_cache_max_num,
            )
            .field(
                "stateful_authorization_cache_expired_second",
                &self.stateful_authorization_cache_expired_second,
            )
            .field(
                "stateful_authorization_cache_negative_enable",
                &self.stateful_authorization_cache_negative_enable,
            )
            .finish()
    }
}

fn redacted_if_present(value: &CheetahString) -> Option<&'static str> {
    if value.is_empty() {
        None
    } else {
        Some("<redacted>")
    }
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            config_name: CheetahString::new(),
            cluster_name: CheetahString::new(),
            auth_config_path: CheetahString::new(),
            acl_file: CheetahString::new(),
            acl_file_watch_enabled: false,
            acl_file_watch_interval_millis: 5_000,

            authentication_enabled: false,
            authentication_provider: CheetahString::new(),
            authentication_metadata_provider: CheetahString::new(),
            authentication_strategy: CheetahString::new(),
            authentication_whitelist: CheetahString::new(),
            init_authentication_user: CheetahString::new(),
            inner_client_authentication_credentials: CheetahString::new(),
            signature_algorithm: SignatureAlgorithm::default(),
            request_timestamp_expired_millis: 0,

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
            stateful_authorization_cache_negative_enable: false,
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
        assert!(config.acl_file.is_empty());

        assert!(config.authentication_provider.is_empty());
        assert!(config.authentication_metadata_provider.is_empty());
        assert!(config.authentication_strategy.is_empty());
        assert!(config.authentication_whitelist.is_empty());
        assert!(config.init_authentication_user.is_empty());
        assert!(config.inner_client_authentication_credentials.is_empty());
        assert_eq!(config.signature_algorithm, SignatureAlgorithm::HmacSha1);
        assert_eq!(config.request_timestamp_expired_millis, 0);

        assert!(config.authorization_provider.is_empty());
        assert!(config.authorization_metadata_provider.is_empty());
        assert!(config.authorization_strategy.is_empty());
        assert!(config.authorization_whitelist.is_empty());

        // Boolean fields
        assert!(!config.authentication_enabled);
        assert!(!config.authorization_enabled);
        assert!(!config.acl_file_watch_enabled);
        assert!(!config.migrate_auth_from_v1_enabled);
        assert_eq!(config.acl_file_watch_interval_millis, 5_000);

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
        assert!(!config.stateful_authorization_cache_negative_enable);
    }

    #[test]
    fn auth_config_deserializes_acl_file_fields_from_camel_case() {
        let config: AuthConfig = serde_yaml::from_str(
            r#"
aclFile: conf/plain_acl.yml
aclFileWatchEnabled: true
aclFileWatchIntervalMillis: 250
signatureAlgorithm: HmacSHA256
requestTimestampExpiredMillis: 300000
statefulAuthorizationCacheNegativeEnable: true
"#,
        )
        .unwrap();

        assert_eq!(config.acl_file.as_str(), "conf/plain_acl.yml");
        assert!(config.acl_file_watch_enabled);
        assert_eq!(config.acl_file_watch_interval_millis, 250);
        assert_eq!(config.signature_algorithm, SignatureAlgorithm::HmacSha256);
        assert_eq!(config.request_timestamp_expired_millis, 300_000);
        assert!(config.stateful_authorization_cache_negative_enable);
    }

    #[test]
    fn auth_config_debug_redacts_embedded_credentials() {
        let config = AuthConfig {
            init_authentication_user: CheetahString::from("admin:init-secret"),
            inner_client_authentication_credentials: CheetahString::from(
                r#"{"accessKey":"inner","secretKey":"inner-secret"}"#,
            ),
            ..AuthConfig::default()
        };

        let debug = format!("{config:?}");

        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("init-secret"));
        assert!(!debug.contains("inner-secret"));
    }
}
