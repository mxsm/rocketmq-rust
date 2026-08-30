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

use std::path::PathBuf;
use std::time::Duration;

use rocketmq_sre_model_gateway::ProviderProfile;
use rocketmq_sre_model_gateway::SecretReferenceKind;

use crate::ControlPlaneError;

const DEFAULT_MODEL_TIMEOUT_SECONDS: u64 = 20;
const DEFAULT_MAX_REQUEST_BYTES: usize = 256 * 1024;
const DEFAULT_MAX_RESPONSE_BYTES: usize = 256 * 1024;
const DEFAULT_MAX_FALLBACKS: usize = 1;
const DEFAULT_SECRET_CACHE_TTL_SECONDS: u64 = 30;
const DEFAULT_SECRET_MAX_BYTES: u64 = 64 * 1024;

/// Explicit model credential ownership selected at process startup.
#[derive(Clone)]
pub(super) enum ModelSecretProviderConfig {
    None,
    Development {
        env_prefix: String,
        file_root: Option<PathBuf>,
    },
    VaultAgentFile {
        root: PathBuf,
        namespace: String,
        cache_ttl: Duration,
        max_secret_bytes: u64,
        version_sidecar_suffix: Option<String>,
    },
}

impl std::fmt::Debug for ModelSecretProviderConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => formatter.write_str("None"),
            Self::Development { .. } => formatter.write_str("Development([CONFIG REDACTED])"),
            Self::VaultAgentFile {
                cache_ttl,
                max_secret_bytes,
                version_sidecar_suffix,
                ..
            } => formatter
                .debug_struct("VaultAgentFile")
                .field("root", &"[PATH REDACTED]")
                .field("namespace", &"[REFERENCE REDACTED]")
                .field("cache_ttl", cache_ttl)
                .field("max_secret_bytes", max_secret_bytes)
                .field("version_sidecar_configured", &version_sidecar_suffix.is_some())
                .finish(),
        }
    }
}

/// Fail-closed runtime configuration for the model-assisted diagnosis path.
#[derive(Clone, Debug)]
pub(super) struct ModelRuntimeConfig {
    pub(super) enabled: bool,
    pub(super) profiles: Vec<ProviderProfile>,
    pub(super) max_fallbacks: usize,
    pub(super) request_timeout: Duration,
    pub(super) max_request_bytes: usize,
    pub(super) max_response_bytes: usize,
    pub(super) allow_insecure_non_loopback_http: bool,
    pub(super) secret_provider: ModelSecretProviderConfig,
}

impl ModelRuntimeConfig {
    pub(super) fn disabled() -> Self {
        Self {
            enabled: false,
            profiles: Vec::new(),
            max_fallbacks: DEFAULT_MAX_FALLBACKS,
            request_timeout: Duration::from_secs(DEFAULT_MODEL_TIMEOUT_SECONDS),
            max_request_bytes: DEFAULT_MAX_REQUEST_BYTES,
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
            allow_insecure_non_loopback_http: false,
            secret_provider: ModelSecretProviderConfig::None,
        }
    }

    /// Loads reference-only profile configuration.
    ///
    /// `ROCKETMQ_SRE_MODEL_PROFILES_JSON` is a JSON array of
    /// `ProviderProfile`. It can contain only secret references, never
    /// credential material. For a single OpenAI-compatible local or mock
    /// endpoint, `ROCKETMQ_SRE_MODEL_LOCAL_ENDPOINT` and
    /// `ROCKETMQ_SRE_MODEL_LOCAL_NAME` provide a smaller bootstrap surface.
    pub(super) fn from_env(dev_auth_enabled: bool) -> Result<Self, ControlPlaneError> {
        let enabled = parse_env("ROCKETMQ_SRE_MODEL_ENABLED", false)?;
        if !enabled {
            return Ok(Self::disabled());
        }

        let mut profiles = match optional_env("ROCKETMQ_SRE_MODEL_PROFILES_JSON") {
            Some(value) => serde_json::from_str::<Vec<ProviderProfile>>(&value).map_err(|_| {
                ControlPlaneError::configuration(
                    "ROCKETMQ_SRE_MODEL_PROFILES_JSON must contain valid reference-only provider profiles",
                )
            })?,
            None => Vec::new(),
        };
        if let Some(endpoint) = optional_env("ROCKETMQ_SRE_MODEL_LOCAL_ENDPOINT") {
            profiles.push(local_profile(
                endpoint,
                optional_env("ROCKETMQ_SRE_MODEL_LOCAL_NAME").unwrap_or_else(|| "served-model".to_owned()),
            )?);
        }
        if profiles.is_empty() {
            return Err(ControlPlaneError::configuration(
                "model calls are enabled but no provider profile is configured",
            ));
        }
        for profile in &profiles {
            profile.validate().map_err(|error| {
                ControlPlaneError::configuration(format!(
                    "configured model profile `{}` is invalid: {:?}",
                    profile.id, error.code
                ))
            })?;
        }
        let mut ids = std::collections::BTreeSet::new();
        if profiles.iter().any(|profile| !ids.insert(profile.id.clone())) {
            return Err(ControlPlaneError::configuration(
                "configured model profile identifiers must be unique",
            ));
        }

        let timeout_seconds = parse_env("ROCKETMQ_SRE_MODEL_TIMEOUT_SECONDS", DEFAULT_MODEL_TIMEOUT_SECONDS)?;
        let max_request_bytes = parse_env("ROCKETMQ_SRE_MODEL_MAX_REQUEST_BYTES", DEFAULT_MAX_REQUEST_BYTES)?;
        let max_response_bytes = parse_env("ROCKETMQ_SRE_MODEL_MAX_RESPONSE_BYTES", DEFAULT_MAX_RESPONSE_BYTES)?;
        if timeout_seconds == 0 || max_request_bytes == 0 || max_response_bytes == 0 {
            return Err(ControlPlaneError::configuration(
                "model time and body limits must be greater than zero",
            ));
        }
        let max_fallbacks = parse_env("ROCKETMQ_SRE_MODEL_MAX_FALLBACKS", DEFAULT_MAX_FALLBACKS)?;
        if max_fallbacks > 3 {
            return Err(ControlPlaneError::configuration(
                "ROCKETMQ_SRE_MODEL_MAX_FALLBACKS must not exceed 3",
            ));
        }
        let dev_secrets_requested = parse_env("ROCKETMQ_SRE_MODEL_DEV_SECRETS", false)?;
        if dev_secrets_requested && !dev_auth_enabled {
            return Err(ControlPlaneError::configuration(
                "development model secret adapters require ROCKETMQ_SRE_DEV_AUTH=true",
            ));
        }
        let allow_insecure_non_loopback_http = parse_env("ROCKETMQ_SRE_MODEL_ALLOW_INSECURE_HTTP", false)?;
        if allow_insecure_non_loopback_http && !dev_auth_enabled {
            return Err(ControlPlaneError::configuration(
                "plaintext non-loopback model endpoints require ROCKETMQ_SRE_DEV_AUTH=true",
            ));
        }
        let secret_provider = secret_provider_from_env(dev_auth_enabled, dev_secrets_requested, &profiles)?;

        Ok(Self {
            enabled: true,
            profiles,
            max_fallbacks,
            request_timeout: Duration::from_secs(timeout_seconds),
            max_request_bytes,
            max_response_bytes,
            allow_insecure_non_loopback_http,
            secret_provider,
        })
    }
}

fn secret_provider_from_env(
    dev_auth_enabled: bool,
    dev_secrets_requested: bool,
    profiles: &[ProviderProfile],
) -> Result<ModelSecretProviderConfig, ControlPlaneError> {
    let provider = optional_env("ROCKETMQ_SRE_MODEL_SECRET_PROVIDER");
    let provider = provider
        .as_deref()
        .unwrap_or(if dev_secrets_requested { "dev" } else { "none" });
    let config = match provider {
        "none" => ModelSecretProviderConfig::None,
        "dev" => {
            if !dev_auth_enabled || !dev_secrets_requested {
                return Err(ControlPlaneError::configuration(
                    "the development model secret provider requires explicit ROCKETMQ_SRE_DEV_AUTH=true and \
                     ROCKETMQ_SRE_MODEL_DEV_SECRETS=true",
                ));
            }
            ModelSecretProviderConfig::Development {
                env_prefix: optional_env("ROCKETMQ_SRE_MODEL_SECRET_ENV_PREFIX")
                    .unwrap_or_else(|| "ROCKETMQ_SRE_MODEL_".to_owned()),
                file_root: optional_env("ROCKETMQ_SRE_MODEL_SECRET_FILE_ROOT").map(PathBuf::from),
            }
        }
        "vault_agent_file" => {
            let root = required_env("ROCKETMQ_SRE_MODEL_VAULT_AGENT_ROOT")?.into();
            let namespace = required_env("ROCKETMQ_SRE_MODEL_SECRET_NAMESPACE")?;
            let cache_ttl_seconds = parse_env(
                "ROCKETMQ_SRE_MODEL_SECRET_CACHE_TTL_SECONDS",
                DEFAULT_SECRET_CACHE_TTL_SECONDS,
            )?;
            let max_secret_bytes = parse_env("ROCKETMQ_SRE_MODEL_SECRET_MAX_BYTES", DEFAULT_SECRET_MAX_BYTES)?;
            if cache_ttl_seconds == 0 || max_secret_bytes == 0 {
                return Err(ControlPlaneError::configuration(
                    "Vault Agent model secret cache TTL and byte limit must be greater than zero",
                ));
            }
            ModelSecretProviderConfig::VaultAgentFile {
                root,
                namespace,
                cache_ttl: Duration::from_secs(cache_ttl_seconds),
                max_secret_bytes,
                version_sidecar_suffix: optional_env("ROCKETMQ_SRE_MODEL_SECRET_VERSION_SUFFIX"),
            }
        }
        _ => {
            return Err(ControlPlaneError::configuration(
                "ROCKETMQ_SRE_MODEL_SECRET_PROVIDER must be one of none, dev, or vault_agent_file",
            ));
        }
    };
    validate_secret_provider_ownership(&config, profiles)?;
    Ok(config)
}

fn validate_secret_provider_ownership(
    config: &ModelSecretProviderConfig,
    profiles: &[ProviderProfile],
) -> Result<(), ControlPlaneError> {
    for profile in profiles {
        let Some(reference) = profile.credential_ref.as_ref() else {
            continue;
        };
        let owned = match config {
            ModelSecretProviderConfig::None => false,
            ModelSecretProviderConfig::Development { .. } => matches!(
                reference.kind(),
                SecretReferenceKind::Environment | SecretReferenceKind::File
            ),
            ModelSecretProviderConfig::VaultAgentFile { namespace, .. } => {
                reference.kind() == SecretReferenceKind::External && namespace_owns(namespace, reference.locator())
            }
        };
        if !owned {
            return Err(ControlPlaneError::configuration(
                "a model credential reference is not owned by the configured secret provider",
            ));
        }
    }
    Ok(())
}

fn namespace_owns(namespace: &str, locator: &str) -> bool {
    if namespace.is_empty() {
        return false;
    }
    if namespace.ends_with('/') {
        locator.starts_with(namespace)
    } else {
        locator == namespace
            || locator
                .strip_prefix(namespace)
                .is_some_and(|remainder| remainder.starts_with('/'))
    }
}

fn local_profile(endpoint: String, model: String) -> Result<ProviderProfile, ControlPlaneError> {
    let mut profile = rocketmq_sre_model_gateway::builtin_provider_profiles()
        .into_iter()
        .find(|profile| profile.id == "vllm")
        .ok_or_else(|| ControlPlaneError::configuration("the local provider fixture is unavailable"))?;
    profile.id = "local-openai-compatible".to_owned();
    profile.endpoint = endpoint;
    profile.model = model;
    profile.model_revision = "configured".to_owned();
    profile.endpoint_instance = "local-openai-compatible:private".to_owned();
    profile.priority = 1;
    profile.validate().map_err(|error| {
        ControlPlaneError::configuration(format!("local model profile is invalid: {:?}", error.code))
    })?;
    Ok(profile)
}

fn optional_env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.trim().is_empty())
}

fn required_env(name: &str) -> Result<String, ControlPlaneError> {
    optional_env(name).ok_or_else(|| ControlPlaneError::configuration(format!("{name} must be configured")))
}

fn parse_env<T>(name: &str, default: T) -> Result<T, ControlPlaneError>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|error| ControlPlaneError::configuration(format!("{name} is invalid: {error}"))),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(ControlPlaneError::configuration(format!(
            "{name} cannot be read: {error}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_configuration_has_no_profiles_or_secret_adapter() {
        let config = ModelRuntimeConfig::disabled();
        assert!(!config.enabled);
        assert!(config.profiles.is_empty());
        assert!(matches!(config.secret_provider, ModelSecretProviderConfig::None));
    }

    #[test]
    fn local_profile_is_reference_free_and_supports_private_data() {
        let profile =
            local_profile("http://127.0.0.1:11434/v1".to_owned(), "qwen-test".to_owned()).expect("local profile");
        assert!(profile.credential_ref.is_none());
        assert!(
            profile
                .allowed_data_classes
                .contains(&rocketmq_sre_model_gateway::DataClass::Restricted)
        );
    }

    #[test]
    fn production_vault_provider_owns_only_external_namespace_references() {
        let mut owned = rocketmq_sre_model_gateway::builtin_provider_profiles()
            .into_iter()
            .find(|profile| profile.id == "deepseek")
            .expect("deepseek fixture");
        let mut foreign = owned.clone();
        foreign.credential_ref = Some(
            rocketmq_sre_model_gateway::SecretReference::external("another-team/models/deepseek")
                .expect("external reference"),
        );
        let config = ModelSecretProviderConfig::VaultAgentFile {
            root: PathBuf::from("/redacted"),
            namespace: "rocketmq-sre/models".to_owned(),
            cache_ttl: Duration::from_secs(30),
            max_secret_bytes: DEFAULT_SECRET_MAX_BYTES,
            version_sidecar_suffix: None,
        };

        validate_secret_provider_ownership(&config, std::slice::from_ref(&owned)).expect("owned external reference");
        assert!(validate_secret_provider_ownership(&config, &[foreign]).is_err());

        owned.credential_ref = Some(
            rocketmq_sre_model_gateway::SecretReference::parse("env://ROCKETMQ_SRE_MODEL_DEEPSEEK")
                .expect("environment reference"),
        );
        assert!(validate_secret_provider_ownership(&config, &[owned]).is_err());
    }

    #[test]
    fn secret_provider_debug_redacts_paths_and_namespaces() {
        let config = ModelSecretProviderConfig::VaultAgentFile {
            root: PathBuf::from("/run/private/model-secrets"),
            namespace: "rocketmq-sre/models".to_owned(),
            cache_ttl: Duration::from_secs(30),
            max_secret_bytes: DEFAULT_SECRET_MAX_BYTES,
            version_sidecar_suffix: Some(".version".to_owned()),
        };
        let debug = format!("{config:?}");

        assert!(!debug.contains("/run/private/model-secrets"));
        assert!(!debug.contains("rocketmq-sre/models"));
        assert!(debug.contains("[PATH REDACTED]"));
        assert!(debug.contains("[REFERENCE REDACTED]"));
    }
}
