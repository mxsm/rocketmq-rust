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
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;

use crate::config::validate_namesrv_property;
use crate::config::ConfigMutability;
use crate::config::NamesrvConfigKey;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ClassifiedConfigUpdate {
    pub(crate) key: CheetahString,
    pub(crate) value: CheetahString,
    pub(crate) mutability: ConfigMutability,
}

pub(crate) fn classify_runtime_updates(
    updates: impl IntoIterator<Item = (CheetahString, CheetahString)>,
) -> RocketMQResult<Vec<ClassifiedConfigUpdate>> {
    updates
        .into_iter()
        .map(|(key, value)| {
            let mutability = classify_runtime_update(&key, &value)?;
            if mutability == ConfigMutability::Unsupported {
                return Err(RocketMQError::nameserver_config_invalid(format!(
                    "configuration key '{key}' cannot be changed remotely"
                )));
            }
            Ok(ClassifiedConfigUpdate { key, value, mutability })
        })
        .collect()
}

fn classify_runtime_update(key: &str, value: &str) -> RocketMQResult<ConfigMutability> {
    if let Some(namesrv_key) = NamesrvConfigKey::from_java_name(key) {
        validate_namesrv_property(namesrv_key, value)?;
        return Ok(namesrv_key.mutability());
    }

    match key {
        "listenPort" => {
            parse_bounded_u64(key, value, 1, u16::MAX as u64)?;
            Ok(ConfigMutability::RestartRequired)
        }
        "bindAddress" => {
            if value.trim().is_empty() {
                return Err(invalid_value(key, "must not be empty"));
            }
            Ok(ConfigMutability::RestartRequired)
        }
        "connectTimeoutMillis" => {
            parse_bounded_u64(key, value, 1, 3_600_000)?;
            Ok(ConfigMutability::RestartRequired)
        }
        "channelNotActiveInterval" => {
            parse_bounded_u64(key, value, 0, 86_400_000)?;
            Ok(ConfigMutability::RestartRequired)
        }
        key if is_tls_config_key(key) => {
            if value.trim().is_empty() && key != "tls.ciphers" && key != "tls.protocols" {
                return Err(invalid_value(key, "must not be empty"));
            }
            Ok(ConfigMutability::RestartRequired)
        }
        _ => Err(RocketMQError::nameserver_config_invalid(format!(
            "unknown configuration key '{key}'"
        ))),
    }
}

fn parse_bounded_u64(key: &str, value: &str, minimum: u64, maximum: u64) -> RocketMQResult<u64> {
    let parsed = value
        .parse::<u64>()
        .map_err(|_| invalid_value(key, "expected a non-negative integer"))?;
    if !(minimum..=maximum).contains(&parsed) {
        return Err(invalid_value(key, &format!("must be between {minimum} and {maximum}")));
    }
    Ok(parsed)
}

fn invalid_value(key: &str, reason: &str) -> RocketMQError {
    RocketMQError::nameserver_config_invalid(format!("invalid value for '{key}': {reason}"))
}

fn is_tls_config_key(key: &str) -> bool {
    matches!(
        key,
        "tls.enable"
            | "tls.test.mode.enable"
            | "tls.config.file"
            | "tls.server.mode"
            | "tls.server.need.client.auth"
            | "tls.server.keyPath"
            | "tls.server.keyPassword"
            | "tls.server.certPath"
            | "tls.server.authClient"
            | "tls.server.trustCertPath"
            | "tls.client.keyPath"
            | "tls.client.keyPassword"
            | "tls.client.certPath"
            | "tls.client.authServer"
            | "tls.client.trustCertPath"
            | "tls.ciphers"
            | "tls.protocols"
    )
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::classify_runtime_updates;
    use super::ConfigMutability;

    #[test]
    fn classifies_mixed_live_and_restart_required_updates() {
        let classified = classify_runtime_updates([
            (
                CheetahString::from_static_str("enableTopicList"),
                CheetahString::from_static_str("false"),
            ),
            (
                CheetahString::from_static_str("listenPort"),
                CheetahString::from_static_str("19876"),
            ),
        ])
        .expect("valid updates should classify");

        assert_eq!(classified[0].mutability, ConfigMutability::Live);
        assert_eq!(classified[1].mutability, ConfigMutability::RestartRequired);
    }

    #[test]
    fn rejects_unknown_and_out_of_domain_updates() {
        for (key, value) in [
            ("unknownNameServerKey", "1"),
            ("unRegisterBrokerQueueCapacity", "0"),
            ("unRegisterBrokerQueueCapacity", "-1"),
            ("defaultThreadPoolNums", "0"),
            ("defaultThreadPoolQueueCapacity", "10000001"),
            ("scanNotActiveBrokerInterval", "0"),
            ("connectTimeoutMillis", "-1"),
        ] {
            let result = classify_runtime_updates([(CheetahString::from(key), CheetahString::from(value))]);
            assert!(result.is_err(), "{key}={value} must be rejected");
        }
    }
}
