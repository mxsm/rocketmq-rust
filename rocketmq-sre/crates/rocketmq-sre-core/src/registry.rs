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

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;

use rocketmq_sre_contracts::Deprecation;
use rocketmq_sre_contracts::Descriptor;
use rocketmq_sre_contracts::DescriptorKind;
use rocketmq_sre_contracts::DescriptorStatus;
use rocketmq_sre_contracts::DescriptorVersion;

type RegistryKey = (DescriptorKind, String);

#[derive(Debug)]
struct RegistryEntry {
    active: DescriptorVersion,
    versions: BTreeMap<DescriptorVersion, Descriptor>,
}

/// Extension registry with explicit activation and rollback.
#[derive(Debug, Default)]
pub struct DescriptorRegistry {
    supported_schema_majors: BTreeMap<String, u16>,
    capabilities: BTreeSet<String>,
    entries: BTreeMap<RegistryKey, RegistryEntry>,
}

/// Registry validation and lifecycle errors.
#[derive(Debug, PartialEq)]
pub enum RegistryError {
    AlreadyExists {
        kind: DescriptorKind,
        id: String,
    },
    NotFound {
        kind: DescriptorKind,
        id: String,
    },
    InvalidVersion {
        version: String,
    },
    VersionConflict {
        active: DescriptorVersion,
        candidate: DescriptorVersion,
    },
    CapabilityMismatch {
        capability: String,
    },
    UnsupportedSchema,
}

impl fmt::Display for RegistryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyExists { kind, id } => {
                write!(formatter, "descriptor `{kind:?}/{id}` already exists")
            }
            Self::NotFound { kind, id } => write!(formatter, "descriptor `{kind:?}/{id}` was not found"),
            Self::InvalidVersion { version } => write!(formatter, "descriptor version `{version}` is invalid"),
            Self::VersionConflict { active, candidate } => {
                write!(
                    formatter,
                    "descriptor upgrade `{candidate}` must be newer than active `{active}`"
                )
            }
            Self::CapabilityMismatch { capability } => {
                write!(formatter, "descriptor requires unsupported capability `{capability}`")
            }
            Self::UnsupportedSchema => formatter.write_str("descriptor supports no known schema family and major"),
        }
    }
}

impl Error for RegistryError {}

impl DescriptorRegistry {
    /// Creates a fail-closed registry for explicit schemas and capabilities.
    #[must_use]
    pub fn new(
        supported_schema_majors: impl IntoIterator<Item = (impl Into<String>, u16)>,
        capabilities: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            supported_schema_majors: supported_schema_majors
                .into_iter()
                .map(|(family, major)| (family.into(), major))
                .collect(),
            capabilities: capabilities.into_iter().map(Into::into).collect(),
            entries: BTreeMap::new(),
        }
    }

    /// Registers the first version of an extension and activates it.
    ///
    /// # Errors
    ///
    /// Rejects duplicate IDs, invalid semantic versions, unknown schemas, and
    /// missing required capabilities.
    pub fn register(&mut self, descriptor: Descriptor) -> Result<(), RegistryError> {
        self.validate(&descriptor)?;
        let key = (descriptor.kind(), descriptor.id().to_owned());
        if self.entries.contains_key(&key) {
            return Err(RegistryError::AlreadyExists { kind: key.0, id: key.1 });
        }
        let version = parse_version(descriptor.version())?;
        self.entries.insert(
            key,
            RegistryEntry {
                active: version.clone(),
                versions: BTreeMap::from([(version, descriptor)]),
            },
        );
        Ok(())
    }

    /// Adds and activates a strictly newer descriptor version.
    ///
    /// # Errors
    ///
    /// Returns a validation, lookup, or version ordering error.
    pub fn upgrade(&mut self, descriptor: Descriptor) -> Result<(), RegistryError> {
        self.validate(&descriptor)?;
        let key = (descriptor.kind(), descriptor.id().to_owned());
        let candidate = parse_version(descriptor.version())?;
        let entry = self.entries.get_mut(&key).ok_or_else(|| RegistryError::NotFound {
            kind: key.0,
            id: key.1.clone(),
        })?;
        if candidate <= entry.active {
            return Err(RegistryError::VersionConflict {
                active: entry.active.clone(),
                candidate,
            });
        }
        entry.versions.insert(candidate.clone(), descriptor);
        entry.active = candidate;
        Ok(())
    }

    /// Selects a previously registered version without deleting history.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::NotFound`] if either the descriptor or target
    /// version does not exist.
    pub fn rollback(&mut self, kind: DescriptorKind, id: &str, version: &str) -> Result<(), RegistryError> {
        let key = (kind, id.to_owned());
        let target = parse_version(version)?;
        let entry = self.entries.get_mut(&key).ok_or_else(|| RegistryError::NotFound {
            kind,
            id: id.to_owned(),
        })?;
        if !entry.versions.contains_key(&target) {
            return Err(RegistryError::NotFound {
                kind,
                id: format!("{id}@{version}"),
            });
        }
        entry.active = target;
        Ok(())
    }

    /// Disables the active version without removing it.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::NotFound`] for an unknown descriptor.
    pub fn disable(&mut self, kind: DescriptorKind, id: &str) -> Result<(), RegistryError> {
        self.active_mut(kind, id)?.set_status(DescriptorStatus::Disabled);
        Ok(())
    }

    /// Marks the active version deprecated with explicit migration metadata.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::NotFound`] for an unknown descriptor.
    pub fn deprecate(&mut self, kind: DescriptorKind, id: &str, deprecation: Deprecation) -> Result<(), RegistryError> {
        let descriptor = self.active_mut(kind, id)?;
        descriptor.set_status(DescriptorStatus::Deprecated);
        descriptor.set_deprecation(deprecation);
        Ok(())
    }

    /// Returns the active descriptor.
    #[must_use]
    pub fn get(&self, kind: DescriptorKind, id: &str) -> Option<&Descriptor> {
        let entry = self.entries.get(&(kind, id.to_owned()))?;
        entry.versions.get(&entry.active)
    }

    fn active_mut(&mut self, kind: DescriptorKind, id: &str) -> Result<&mut Descriptor, RegistryError> {
        let entry = self
            .entries
            .get_mut(&(kind, id.to_owned()))
            .ok_or_else(|| RegistryError::NotFound {
                kind,
                id: id.to_owned(),
            })?;
        entry
            .versions
            .get_mut(&entry.active)
            .ok_or_else(|| RegistryError::NotFound {
                kind,
                id: id.to_owned(),
            })
    }

    fn validate(&self, descriptor: &Descriptor) -> Result<(), RegistryError> {
        if let Some(capability) = descriptor
            .required_capabilities()
            .iter()
            .find(|capability| !self.capabilities.contains(*capability))
        {
            return Err(RegistryError::CapabilityMismatch {
                capability: capability.clone(),
            });
        }
        let supports_known_schema = descriptor.supported_versions().iter().any(|version| {
            self.supported_schema_majors
                .get(&version.family)
                .is_some_and(|major| *major == version.major)
                && version
                    .required_features
                    .iter()
                    .all(|feature| self.capabilities.contains(feature))
        });
        if !supports_known_schema {
            return Err(RegistryError::UnsupportedSchema);
        }
        parse_version(descriptor.version())?;
        Ok(())
    }
}

fn parse_version(version: &str) -> Result<DescriptorVersion, RegistryError> {
    DescriptorVersion::parse(version).map_err(|_| RegistryError::InvalidVersion {
        version: version.to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use rocketmq_sre_contracts::DescriptorStatus;
    use rocketmq_sre_contracts::ProviderDescriptor;
    use rocketmq_sre_contracts::SchemaVersion;
    use serde_json::json;

    use super::*;

    fn provider(version: &str) -> Descriptor {
        Descriptor::Provider(ProviderDescriptor {
            id: "deepseek".to_owned(),
            version: version.to_owned(),
            owner: "rocketmq-sre".to_owned(),
            supported_versions: vec![SchemaVersion::new("rocketmq-sre.provider", 1, 0)],
            required_capabilities: BTreeSet::new(),
            config_schema: json!({"type": "object"}),
            status: DescriptorStatus::Active,
            deprecation: None,
            protocols: BTreeSet::from(["openai-compatible".to_owned()]),
            supports_streaming: true,
            supports_tools: true,
            supports_structured_output: true,
            supports_embeddings: false,
        })
    }

    #[test]
    fn upgrades_and_rolls_back_without_losing_versions() {
        let mut registry = DescriptorRegistry::new([("rocketmq-sre.provider", 1)], std::iter::empty::<String>());
        registry
            .register(provider("1.0.0"))
            .expect("first version should register");
        registry
            .upgrade(provider("1.1.0"))
            .expect("newer version should upgrade");
        assert_eq!(
            registry
                .get(DescriptorKind::Provider, "deepseek")
                .expect("active descriptor should exist")
                .version(),
            "1.1.0"
        );

        registry
            .rollback(DescriptorKind::Provider, "deepseek", "1.0.0")
            .expect("known version should roll back");
        assert_eq!(
            registry
                .get(DescriptorKind::Provider, "deepseek")
                .expect("rolled back descriptor should exist")
                .version(),
            "1.0.0"
        );
    }

    #[test]
    fn rejects_unknown_schema_major_and_missing_capability() {
        let registry = DescriptorRegistry::new([("rocketmq-sre.provider", 2)], std::iter::empty::<String>());
        assert_eq!(
            registry.validate(&provider("1.0.0")),
            Err(RegistryError::UnsupportedSchema)
        );

        let mut requires_capability = provider("1.0.0");
        if let Descriptor::Provider(provider) = &mut requires_capability {
            provider.required_capabilities.insert("model.tools".to_owned());
        }
        let registry = DescriptorRegistry::new([("rocketmq-sre.provider", 1)], std::iter::empty::<String>());
        assert_eq!(
            registry.validate(&requires_capability),
            Err(RegistryError::CapabilityMismatch {
                capability: "model.tools".to_owned()
            })
        );
    }
}
