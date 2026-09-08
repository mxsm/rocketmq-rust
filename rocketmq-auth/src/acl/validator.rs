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

use crate::migration::alc::acl_config::AclConfig;
use crate::AuthFailureKind;
use crate::AuthOperation;
use crate::AuthServiceError;
use crate::AuthServiceResult;

pub fn validate_acl_config(config: &AclConfig) -> AuthServiceResult<()> {
    let Some(accounts) = config.plain_access_configs() else {
        return Ok(());
    };

    let mut seen_access_keys = HashSet::new();
    for account in accounts {
        let access_key = account
            .access_key()
            .map(|value| value.as_str().trim())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| invalid_acl_config("accessKey must not be blank", "<missing>"))?;

        if !seen_access_keys.insert(access_key.to_owned()) {
            return Err(invalid_acl_config("duplicate accessKey", access_key));
        }

        account
            .secret_key()
            .map(|value| value.as_str().trim())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| invalid_acl_config("secretKey must not be blank", access_key))?;
    }

    Ok(())
}

fn invalid_acl_config(_reason: &'static str, _account: &str) -> AuthServiceError {
    AuthServiceError::new(AuthOperation::Initialize, AuthFailureKind::InvalidConfiguration)
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::*;
    use crate::migration::alc::plain_access_config::PlainAccessConfig;

    #[test]
    fn validates_empty_config() {
        validate_acl_config(&AclConfig::new()).unwrap();
    }

    #[test]
    fn rejects_missing_access_key() {
        let mut account = PlainAccessConfig::new();
        account.set_secret_key(CheetahString::from("sk"));
        let mut config = AclConfig::new();
        config.set_plain_access_configs(vec![account]);

        let error = validate_acl_config(&config).unwrap_err();

        assert_eq!(error.kind(), AuthFailureKind::InvalidConfiguration);
    }

    #[test]
    fn rejects_missing_secret_key_without_leaking_secret() {
        let mut account = PlainAccessConfig::new();
        account.set_access_key(CheetahString::from("ak"));
        let mut config = AclConfig::new();
        config.set_plain_access_configs(vec![account]);

        let error = validate_acl_config(&config).unwrap_err();

        assert_eq!(error.kind(), AuthFailureKind::InvalidConfiguration);
        assert!(!error.to_string().contains("secret="));
    }

    #[test]
    fn rejects_duplicate_access_keys() {
        let mut first = PlainAccessConfig::new();
        first.set_access_key(CheetahString::from("ak"));
        first.set_secret_key(CheetahString::from("sk1"));
        let mut second = PlainAccessConfig::new();
        second.set_access_key(CheetahString::from("ak"));
        second.set_secret_key(CheetahString::from("sk2"));
        let mut config = AclConfig::new();
        config.set_plain_access_configs(vec![first, second]);

        let error = validate_acl_config(&config).unwrap_err();

        assert_eq!(error.kind(), AuthFailureKind::InvalidConfiguration);
    }
}
