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

//! Canonical local configuration, profiles, Monitor migration, and commit records.

use std::{io, path::Path};

use rocketmq_dashboard_common::{
    ConnectionScope, CredentialSourceKind, EnvironmentId, OperationContext, TransportSettings,
};
use serde::{Deserialize, Serialize};

use super::{
    config_store::{AuthConfig, CONFIG_SCHEMA_VERSION, ConfigStoreFailure, DesktopConfig, FoundationFlags},
    monitor_store::MonitorRule,
};

/// A named environment whose identity survives transport and endpoint edits.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EnvironmentProfile {
    pub id: EnvironmentId,
    pub name: String,
    pub nameservers: Vec<String>,
    pub current_nameserver: Option<String>,
    pub proxies: Vec<String>,
    pub current_proxy: Option<String>,
    pub scope: ConnectionScope,
    pub transport: TransportSettings,
    pub credential_source: CredentialSourceKind,
}

impl EnvironmentProfile {
    pub fn from_config(id: EnvironmentId, name: String, config: &DesktopConfig) -> Self {
        Self {
            id,
            name,
            nameservers: config.nameservers.clone(),
            current_nameserver: config.current_nameserver.clone(),
            proxies: config.proxies.clone(),
            current_proxy: config.current_proxy.clone(),
            scope: config.scope,
            transport: config.transport,
            credential_source: config.auth.credential_source,
        }
    }

    fn project(&self, settings: &DesktopSettings) -> DesktopConfig {
        DesktopConfig {
            schema_version: CONFIG_SCHEMA_VERSION,
            revision: settings.revision,
            nameservers: self.nameservers.clone(),
            current_nameserver: self.current_nameserver.clone(),
            transport: self.transport,
            proxies: self.proxies.clone(),
            current_proxy: self.current_proxy.clone(),
            scope: self.scope,
            auth: AuthConfig {
                enabled: settings.authentication_enabled,
                credential_source: self.credential_source,
            },
            foundations: settings.foundations,
        }
    }
}

/// Safe profile catalog exposed to the application without persistence internals.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnvironmentCatalog {
    pub active_id: EnvironmentId,
    pub profiles: Vec<EnvironmentProfile>,
    pub unassigned_monitor_rules: usize,
}

pub enum EnvironmentChange {
    Create { name: String },
    Select { id: EnvironmentId },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct DesktopSettings {
    pub revision: u64,
    pub authentication_enabled: bool,
    pub foundations: FoundationFlags,
}

/// Old enabled rules remain incomplete until their environment and thresholds are supplied.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MonitorMigrationState {
    NeedsEnvironmentAndThresholds,
    NeedsThresholds,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct StoredMonitorRule {
    pub environment_id: Option<EnvironmentId>,
    pub rule: MonitorRule,
    pub migration_state: MonitorMigrationState,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct MonitorDocument {
    pub schema_version: u32,
    pub rules: Vec<StoredMonitorRule>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LocalOperationKind {
    ConfigurationSaved,
    MonitorSaved,
    MonitorDeleted,
}

/// Committed with local state in the same file replacement; this is the P11 writer seam.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct LocalOperationRecord {
    pub context: OperationContext,
    pub kind: LocalOperationKind,
    pub committed_at_epoch_ms: u64,
}

/// One replacement commits configuration, Monitor rules, and their operation records together.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct DesktopDocument {
    pub schema_version: u32,
    pub settings: DesktopSettings,
    pub active_environment_id: EnvironmentId,
    pub profiles: Vec<EnvironmentProfile>,
    pub monitors: MonitorDocument,
    pub local_operations: Vec<LocalOperationRecord>,
}

impl DesktopDocument {
    pub fn from_legacy(config: DesktopConfig, monitors: Vec<MonitorRule>) -> Result<Self, ConfigStoreFailure> {
        let id = new_environment_id()?;
        let profile = EnvironmentProfile::from_config(id.clone(), "Default".into(), &config);
        Ok(Self {
            schema_version: CONFIG_SCHEMA_VERSION,
            settings: DesktopSettings {
                revision: config.revision,
                authentication_enabled: config.auth.enabled,
                foundations: config.foundations,
            },
            active_environment_id: id,
            profiles: vec![profile],
            monitors: MonitorDocument {
                schema_version: 1,
                rules: monitors
                    .into_iter()
                    .map(|rule| StoredMonitorRule {
                        environment_id: None,
                        rule,
                        migration_state: MonitorMigrationState::NeedsEnvironmentAndThresholds,
                    })
                    .collect(),
            },
            local_operations: Vec::new(),
        })
    }

    pub fn config(&self) -> Result<DesktopConfig, ConfigStoreFailure> {
        self.active_profile()?.project(&self.settings).normalize()
    }

    pub fn catalog(&self) -> EnvironmentCatalog {
        EnvironmentCatalog {
            active_id: self.active_environment_id.clone(),
            profiles: self.profiles.clone(),
            unassigned_monitor_rules: self
                .monitors
                .rules
                .iter()
                .filter(|rule| rule.environment_id.is_none())
                .count(),
        }
    }

    fn active_profile(&self) -> Result<&EnvironmentProfile, ConfigStoreFailure> {
        self.profiles
            .iter()
            .find(|profile| profile.id == self.active_environment_id)
            .ok_or_else(|| ConfigStoreFailure::validation(EnvironmentValidation::MissingProfile))
    }

    pub fn apply_config(&mut self, config: &DesktopConfig) -> Result<(), ConfigStoreFailure> {
        let active = self.active_profile()?;
        let profile = EnvironmentProfile::from_config(active.id.clone(), active.name.clone(), config);
        if let Some(active) = self
            .profiles
            .iter_mut()
            .find(|profile| profile.id == self.active_environment_id)
        {
            *active = profile;
        }
        self.settings.revision = config.revision;
        self.settings.authentication_enabled = config.auth.enabled;
        self.settings.foundations = config.foundations;
        Ok(())
    }

    pub fn validate(&self) -> Result<(), ConfigStoreFailure> {
        if self.schema_version != CONFIG_SCHEMA_VERSION {
            return Err(ConfigStoreFailure::unsupported_schema(
                self.schema_version,
                CONFIG_SCHEMA_VERSION,
            ));
        }
        if self.monitors.schema_version != 1 {
            return Err(ConfigStoreFailure::unsupported_schema(self.monitors.schema_version, 1));
        }
        let mut ids = std::collections::BTreeSet::new();
        for profile in &self.profiles {
            if profile.name.trim().is_empty() || !ids.insert(&profile.id) {
                return Err(ConfigStoreFailure::validation(EnvironmentValidation::InvalidProfile));
            }
            profile.project(&self.settings).normalize()?;
        }
        self.active_profile()?;
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub(super) enum EnvironmentValidation {
    #[error("the selected environment profile does not exist")]
    MissingProfile,
    #[error("environment profiles need distinct identities and a nonempty name")]
    InvalidProfile,
}

pub(super) fn new_environment_id() -> Result<EnvironmentId, ConfigStoreFailure> {
    EnvironmentId::parse(format!("env-{:032x}", rand::random::<u128>())).map_err(ConfigStoreFailure::validation)
}

/// Decodes all sources before producing a candidate; legacy Monitor data is never attributed.
pub(super) fn load_document(path: &Path) -> Result<(DesktopDocument, bool), ConfigStoreFailure> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let monitors = load_legacy_monitors(&path.with_file_name("monitors.json"))?;
            return DesktopDocument::from_legacy(DesktopConfig::default(), monitors).map(|document| (document, true));
        }
        Err(source) => return Err(ConfigStoreFailure::io("read configuration", path.to_path_buf(), source)),
    };
    let value: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|source| ConfigStoreFailure::invalid_document(path.to_path_buf(), source))?;
    let version = value
        .get("schemaVersion")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(1);
    match version {
        1 => {
            let mut config: DesktopConfig = serde_json::from_value(value)
                .map_err(|source| ConfigStoreFailure::invalid_document(path.to_path_buf(), source))?;
            config.schema_version = CONFIG_SCHEMA_VERSION;
            let config = config.normalize()?;
            let monitors = load_legacy_monitors(&path.with_file_name("monitors.json"))?;
            DesktopDocument::from_legacy(config, monitors).map(|document| (document, true))
        }
        version if version == u64::from(CONFIG_SCHEMA_VERSION) => {
            let document: DesktopDocument = serde_json::from_value(value)
                .map_err(|source| ConfigStoreFailure::invalid_document(path.to_path_buf(), source))?;
            document.validate()?;
            Ok((document, false))
        }
        version => Err(ConfigStoreFailure::unsupported_schema(
            u32::try_from(version).unwrap_or(u32::MAX),
            CONFIG_SCHEMA_VERSION,
        )),
    }
}

fn load_legacy_monitors(path: &Path) -> Result<Vec<MonitorRule>, ConfigStoreFailure> {
    match std::fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map_err(|source| ConfigStoreFailure::invalid_document(path.to_path_buf(), source)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(source) => Err(ConfigStoreFailure::io(
            "read legacy monitors",
            path.to_path_buf(),
            source,
        )),
    }
}

#[cfg(test)]
#[path = "environment_store_tests.rs"]
mod tests;
