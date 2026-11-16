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

use rocketmq_proxy_core::error::canonical;
use rocketmq_proxy_core::ProxyResult;
use rocketmq_transport::api::TlsConfig;
use serde::Deserialize;

pub(super) fn deserialize<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<TlsConfig, D::Error> {
    TlsConfig::deserialize(deserializer).map_err(|_| serde::de::Error::custom("invalid backend TLS configuration"))
}

pub(super) fn validate(config: &TlsConfig) -> ProxyResult<()> {
    if !config.enable {
        return Ok(());
    }
    #[cfg(not(feature = "tls"))]
    {
        Err(canonical::configuration_invalid("cluster.tlsConfig", "TLS feature is required").into())
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

#[cfg(feature = "tls")]
fn invalid(reason: &'static str) -> rocketmq_proxy_core::ProxyError {
    canonical::configuration_invalid("cluster.tlsConfig", reason).into()
}

#[cfg(feature = "tls")]
fn readable(path: &str) -> ProxyResult<()> {
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

    #[test]
    fn backend_tls_defaults_are_compatible_and_private_paths_are_not_debugged() {
        let mut config = crate::ClusterConfig::default();
        assert!(!config.tls_config.enable);
        config.validate_tls().unwrap();
        config.tls_config.client.trust_cert_path = Some("PRIVATE_CA_CANARY".to_owned());
        config.tls_config.client.key_path = Some("PRIVATE_KEY_CANARY".to_owned());
        config.tls_config.client.key_password = Some("PRIVATE_PASSWORD_CANARY".to_owned());
        let rendered = format!("{config:?}");
        assert!(!rendered.contains("CANARY"));
        let parsed: crate::ClusterConfig = serde_json::from_str("{}").unwrap();
        assert!(!parsed.tls_config.enable);
        let error =
            serde_json::from_str::<crate::ClusterConfig>(r#"{"tlsConfig":{"enable":"PRIVATE_CANARY"}}"#).unwrap_err();
        assert!(!error.to_string().contains("CANARY"));
    }

    #[cfg(not(feature = "tls"))]
    #[test]
    fn enabled_backend_tls_requires_its_actual_feature() {
        let config = TlsConfig {
            enable: true,
            config_file: String::new(),
            ..Default::default()
        };
        assert!(validate(&config).is_err());
    }

    #[cfg(feature = "tls")]
    #[test]
    fn backend_tls_rejects_implicit_properties_unverified_or_partial_material() {
        let mut config = TlsConfig {
            enable: true,
            config_file: String::new(),
            ..Default::default()
        };
        validate(&config).unwrap(); // Existing native-root semantics remain available.
        config.config_file = "/private/tls.properties".to_owned();
        assert!(validate(&config).is_err());
        config.config_file.clear();
        config.test_mode_enable = true;
        assert!(validate(&config).is_err());
        config.test_mode_enable = false;
        config.client.auth_server = false;
        assert!(validate(&config).is_err());
        config.client.auth_server = true;
        config.client.cert_path = Some("PRIVATE_MISSING_CANARY".to_owned());
        let error = validate(&config).unwrap_err();
        assert!(!format!("{error:?}").contains("CANARY"));
        config.client.cert_path = None;
        config.client.trust_cert_path = Some("PRIVATE_MISSING_CANARY".to_owned());
        assert!(validate(&config).is_err());
    }
}
