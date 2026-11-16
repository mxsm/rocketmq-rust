// Copyright 2026 The RocketMQ Rust Authors
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

use super::error::{BrokerConfigError, ConfigSection};
use rocketmq_transport::api::TlsConfig;
use serde::Deserialize;

pub(super) fn deserialize<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<TlsConfig, D::Error> {
    TlsConfig::deserialize(deserializer)
        .map_err(|_| serde::de::Error::custom("invalid Broker client TLS configuration"))
}

pub(super) fn validate(config: &TlsConfig) -> Result<(), BrokerConfigError> {
    if !config.enable {
        return Ok(());
    }
    #[cfg(not(feature = "tls"))]
    {
        Err(invalid("TLS feature is required"))
    }
    #[cfg(feature = "tls")]
    {
        if !config.config_file.is_empty() || config.test_mode_enable || !config.client.auth_server {
            return Err(invalid("direct verified TLS configuration is required"));
        }
        match (&config.client.cert_path, &config.client.key_path) {
            (Some(cert), Some(key)) => {
                readable(cert)?;
                readable(key)?;
            }
            (None, None) if config.client.key_password.is_none() => {}
            _ => return Err(invalid("client certificate and key must be configured together")),
        }
        if let Some(path) = &config.client.trust_cert_path {
            readable(path)?;
        }
        Ok(())
    }
}

fn invalid(reason: &'static str) -> BrokerConfigError {
    BrokerConfigError::invalid(ConfigSection::Security, "broker.brokerClientTlsConfig", reason)
}

#[cfg(feature = "tls")]
fn readable(path: &str) -> Result<(), BrokerConfigError> {
    if path.trim().is_empty()
        || !std::fs::File::open(path)
            .and_then(|file| file.metadata())
            .is_ok_and(|metadata| metadata.is_file())
    {
        return Err(invalid("TLS material must be a readable regular file"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        broker_config::BrokerConfig,
        transaction::ConfigUpdateTransaction,
        validated::{ConfigGeneration, ValidatedBrokerConfig},
    };
    use rocketmq_store::MessageStoreConfig;

    #[test]
    fn outbound_tls_is_default_off_and_private_from_debug_remote_properties_and_cas() {
        let mut broker = BrokerConfig::default();
        assert!(!broker.outbound_transport_config().tls.enable);
        broker.broker_client_tls_config.client.trust_cert_path = Some("PRIVATE_CA_CANARY".to_owned());
        broker.broker_client_tls_config.client.key_path = Some("PRIVATE_KEY_CANARY".to_owned());
        broker.broker_client_tls_config.client.key_password = Some("PRIVATE_PASSWORD_CANARY".to_owned());
        assert!(!format!("{broker:?}").contains("CANARY"));
        let properties = broker.get_properties();
        assert!(!properties
            .keys()
            .any(|key| key.as_str().starts_with("brokerClientTlsConfig")));
        assert!(!properties.values().any(|value| value.as_str().contains("CANARY")));
        let error = serde_json::from_str::<BrokerConfig>(r#"{"brokerClientTlsConfig":{"enable":"PRIVATE_CANARY"}}"#)
            .unwrap_err();
        assert!(!error.to_string().contains("CANARY"));
        let current = ValidatedBrokerConfig::try_from_parts(broker, MessageStoreConfig::default()).unwrap();
        for key in [
            "brokerClientTlsConfig",
            "brokerClientTlsConfig.enable",
            "brokerClientTlsConfig.client.keyPath",
        ] {
            let patch = [(key.into(), "PRIVATE_PATCH_CANARY".into())].into_iter().collect();
            assert!(ConfigUpdateTransaction::from_broker_patch(ConfigGeneration::INITIAL, &current, &patch).is_err());
        }
    }

    #[test]
    fn outbound_tls_reaches_the_canonical_transport_without_copying_server_settings() {
        let mut broker = BrokerConfig::default();
        broker.broker_client_tls_config.enable = true;
        broker.broker_client_tls_config.config_file.clear();
        broker.broker_client_tls_config.client.trust_cert_path = Some("/private/ca.pem".to_owned());
        broker.broker_client_tls_config.client.cert_path = Some("/private/client.pem".to_owned());
        broker.broker_client_tls_config.client.key_path = Some("/private/client.key".to_owned());
        assert_eq!(broker.outbound_transport_config().tls, broker.broker_client_tls_config);
        assert!(!broker.broker_server_config.tls_config.enable);
        let parsed: BrokerConfig = serde_json::from_value(serde_json::to_value(&broker).unwrap()).unwrap();
        assert_eq!(parsed.broker_client_tls_config, broker.broker_client_tls_config);
    }

    #[cfg(not(feature = "tls"))]
    #[test]
    fn enabled_outbound_tls_requires_its_actual_feature() {
        let config = TlsConfig {
            enable: true,
            config_file: String::new(),
            ..Default::default()
        };
        assert!(validate(&config).is_err());
    }

    #[cfg(feature = "tls")]
    #[test]
    fn outbound_tls_rejects_properties_verification_bypass_and_partial_material() {
        let mut config = TlsConfig {
            enable: true,
            config_file: String::new(),
            ..Default::default()
        };
        validate(&config).unwrap();
        config.config_file = "/private/tls.properties".to_owned();
        assert!(validate(&config).is_err());
        config.config_file.clear();
        config.test_mode_enable = true;
        assert!(validate(&config).is_err());
        config.test_mode_enable = false;
        config.client.auth_server = false;
        assert!(validate(&config).is_err());
        config.client.auth_server = true;
        config.client.key_path = Some("PRIVATE_MISSING_CANARY".to_owned());
        let error = validate(&config).unwrap_err();
        assert!(!format!("{error:?}").contains("CANARY"));
        config.client.key_path = None;
        config.client.trust_cert_path = Some("PRIVATE_MISSING_CANARY".to_owned());
        assert!(validate(&config).is_err());
    }
}
