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

use crate::error::canonical;
use crate::{ProxyError, ProxyResult};
use rocketmq_transport::api::TlsConfig;
use serde::Deserialize;

pub(super) fn deserialize<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<TlsConfig, D::Error> {
    TlsConfig::deserialize(deserializer).map_err(|_| serde::de::Error::custom("invalid Remoting TLS configuration"))
}

pub(super) fn validate(config: &TlsConfig) -> ProxyResult<()> {
    if !config.enable {
        return Ok(());
    }
    #[cfg(not(feature = "tls"))]
    {
        Err(invalid("TLS feature is required"))
    }
    #[cfg(feature = "tls")]
    {
        use rocketmq_transport::api::{TlsClientAuth, TlsMode};
        if !config.config_file.is_empty() || config.test_mode_enable || config.server.mode != TlsMode::Enforcing {
            return Err(invalid("direct enforcing TLS configuration is required"));
        }
        let (Some(cert), Some(key)) = (&config.server.cert_path, &config.server.key_path) else {
            return Err(invalid("server certificate and key are required"));
        };
        readable(cert)?;
        readable(key)?;
        if config.server.need_client_auth != TlsClientAuth::None {
            if !config.server.auth_client {
                return Err(invalid("client certificate verification is required"));
            }
            let Some(ca) = &config.server.trust_cert_path else {
                return Err(invalid("client trust material is required"));
            };
            readable(ca)?;
        }
        Ok(())
    }
}

fn invalid(reason: &'static str) -> ProxyError {
    canonical::configuration_invalid("remoting.tlsConfig", reason).into()
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
    fn remoting_tls_defaults_and_debug_do_not_expose_private_material() {
        let mut config = crate::RemotingConfig::default();
        config.validate().unwrap();
        assert!(!config.tls_config.enable);
        config.tls_config.server.key_path = Some("PRIVATE_KEY_CANARY".to_owned());
        config.tls_config.server.cert_path = Some("PRIVATE_CERT_CANARY".to_owned());
        config.tls_config.server.trust_cert_path = Some("PRIVATE_CA_CANARY".to_owned());
        assert!(!format!("{config:?}").contains("CANARY"));
        let parsed: crate::ProxyConfig = serde_json::from_str("{}").unwrap();
        assert!(!parsed.remoting.tls_config.enable);
        let error =
            serde_json::from_str::<crate::ProxyConfig>(r#"{"remoting":{"tlsConfig":{"enable":"PRIVATE_CANARY"}}}"#)
                .unwrap_err();
        assert!(!error.to_string().contains("CANARY"));
    }

    #[cfg(not(feature = "tls"))]
    #[test]
    fn enabled_remoting_tls_requires_its_actual_feature() {
        let config = TlsConfig {
            enable: true,
            config_file: String::new(),
            ..Default::default()
        };
        assert!(validate(&config).is_err());
    }

    #[cfg(feature = "tls")]
    #[test]
    fn remoting_tls_requires_direct_enforcing_complete_identity_and_client_ca() {
        use rocketmq_transport::api::{TlsClientAuth, TlsMode};
        let directory = tempfile::tempdir().unwrap();
        let material = directory.path().join("material.pem");
        std::fs::write(&material, b"validation only; actual TLS parser owns PEM checks").unwrap();
        let mut config = TlsConfig {
            enable: true,
            config_file: String::new(),
            ..Default::default()
        };
        config.server.cert_path = Some(material.to_string_lossy().into_owned());
        config.server.key_path = config.server.cert_path.clone();
        assert!(validate(&config).is_err()); // Permissive is not a new public TLS mode.
        config.server.mode = TlsMode::Enforcing;
        validate(&config).unwrap();
        config.server.need_client_auth = TlsClientAuth::Require;
        assert!(validate(&config).is_err());
        config.server.auth_client = true;
        config.server.trust_cert_path = config.server.cert_path.clone();
        validate(&config).unwrap();
        config.config_file = "/private/tls.properties".to_owned();
        assert!(validate(&config).is_err());
        config.config_file.clear();
        config.test_mode_enable = true;
        assert!(validate(&config).is_err());
        config.test_mode_enable = false;
        config.server.key_path = None;
        let error = validate(&config).unwrap_err();
        assert!(!format!("{error:?}").contains(&material.to_string_lossy().to_string()));
    }
}
