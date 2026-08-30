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

use super::DIAGNOSTIC_OUTPUT_SCHEMA_FAMILY;
use super::DIAGNOSTIC_OUTPUT_SCHEMA_MAJOR;
use super::DiagnosticPack;
use super::PackVersion;

#[derive(Debug)]
struct PackVersions {
    active: PackVersion,
    versions: BTreeMap<PackVersion, Box<dyn DiagnosticPack>>,
}

/// Registry of versioned diagnostic packs with explicit activation.
#[derive(Debug, Default)]
pub struct DiagnosticPackRegistry {
    packs: BTreeMap<String, PackVersions>,
}

/// Diagnostic pack registration and lookup failures.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DiagnosticRegistryError {
    DuplicateVersion { id: String, version: PackVersion },
    NotFound { id: String },
    VersionNotFound { id: String, version: PackVersion },
    InvalidDescriptor { id: String, reason: String },
}

impl fmt::Display for DiagnosticRegistryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateVersion { id, version } => {
                write!(formatter, "diagnostic pack `{id}` version `{version}` already exists")
            }
            Self::NotFound { id } => write!(formatter, "diagnostic pack `{id}` was not found"),
            Self::VersionNotFound { id, version } => {
                write!(formatter, "diagnostic pack `{id}` version `{version}` was not found")
            }
            Self::InvalidDescriptor { id, reason } => {
                write!(formatter, "diagnostic pack `{id}` is invalid: {reason}")
            }
        }
    }
}

impl Error for DiagnosticRegistryError {}

impl DiagnosticPackRegistry {
    /// Registers a pack version and activates it when it is the newest version.
    ///
    /// # Errors
    ///
    /// Rejects duplicate versions and incomplete or ambiguous descriptors.
    pub fn register<P>(&mut self, pack: P) -> Result<(), DiagnosticRegistryError>
    where
        P: DiagnosticPack + 'static,
    {
        self.register_boxed(Box::new(pack))
    }

    /// Registers a boxed pack implementation.
    ///
    /// # Errors
    ///
    /// Rejects duplicate versions and incomplete or ambiguous descriptors.
    pub fn register_boxed(&mut self, pack: Box<dyn DiagnosticPack>) -> Result<(), DiagnosticRegistryError> {
        validate_descriptor(pack.as_ref())?;
        let id = pack.id().to_owned();
        let version = pack.version();
        let entry = self.packs.entry(id.clone()).or_insert_with(|| PackVersions {
            active: version,
            versions: BTreeMap::new(),
        });
        if entry.versions.contains_key(&version) {
            return Err(DiagnosticRegistryError::DuplicateVersion { id, version });
        }
        entry.versions.insert(version, pack);
        if version > entry.active {
            entry.active = version;
        }
        Ok(())
    }

    /// Activates a previously registered pack version.
    ///
    /// # Errors
    ///
    /// Returns a lookup error for an unknown pack or version.
    pub fn activate(&mut self, id: &str, version: PackVersion) -> Result<(), DiagnosticRegistryError> {
        let entry = self
            .packs
            .get_mut(id)
            .ok_or_else(|| DiagnosticRegistryError::NotFound { id: id.to_owned() })?;
        if !entry.versions.contains_key(&version) {
            return Err(DiagnosticRegistryError::VersionNotFound {
                id: id.to_owned(),
                version,
            });
        }
        entry.active = version;
        Ok(())
    }

    /// Returns the active version of a base pack ID.
    #[must_use]
    pub fn active(&self, id: &str) -> Option<&dyn DiagnosticPack> {
        let entry = self.packs.get(id)?;
        entry.versions.get(&entry.active).map(Box::as_ref)
    }

    /// Returns an exact semantic pack version.
    #[must_use]
    pub fn get(&self, id: &str, version: PackVersion) -> Option<&dyn DiagnosticPack> {
        self.packs.get(id)?.versions.get(&version).map(Box::as_ref)
    }

    /// Resolves a base ID or major-qualified ID such as `consumer-lag.v2`.
    #[must_use]
    pub fn resolve(&self, reference: &str) -> Option<&dyn DiagnosticPack> {
        if let Some(pack) = self.active(reference) {
            return Some(pack);
        }
        let (id, major) = reference.rsplit_once(".v")?;
        let major = major.parse::<u16>().ok()?;
        let entry = self.packs.get(id)?;
        if entry.active.major == major {
            return entry.versions.get(&entry.active).map(Box::as_ref);
        }
        entry
            .versions
            .iter()
            .rev()
            .find_map(|(version, pack)| (version.major == major).then_some(pack.as_ref()))
    }

    /// Returns all active packs in stable base-ID order.
    pub fn active_packs(&self) -> impl Iterator<Item = &dyn DiagnosticPack> {
        self.packs
            .values()
            .filter_map(|entry| entry.versions.get(&entry.active).map(Box::as_ref))
    }

    /// Number of distinct pack IDs.
    #[must_use]
    pub fn len(&self) -> usize {
        self.packs.len()
    }

    /// Whether no diagnostic pack is registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.packs.is_empty()
    }
}

fn validate_descriptor(pack: &dyn DiagnosticPack) -> Result<(), DiagnosticRegistryError> {
    let id = pack.id();
    if id.is_empty() || id.ends_with(".v1") || id.rsplit_once(".v").is_some() {
        return Err(invalid(id, "base ID must be non-empty and omit the `.vN` suffix"));
    }
    if pack.version().major == 0 {
        return Err(invalid(id, "major version must be greater than zero"));
    }
    if pack.applicable_components().is_empty() {
        return Err(invalid(id, "at least one applicable component is required"));
    }
    if pack.required_evidence().is_empty() {
        return Err(invalid(id, "at least one required evidence descriptor is required"));
    }
    if pack.rule_codes().is_empty() {
        return Err(invalid(id, "at least one rule code is required"));
    }
    let output_schema = pack.output_schema();
    if output_schema.family != DIAGNOSTIC_OUTPUT_SCHEMA_FAMILY
        || output_schema.major != DIAGNOSTIC_OUTPUT_SCHEMA_MAJOR
        || !output_schema.required_features.is_empty()
    {
        return Err(invalid(
            id,
            "output schema family, major, or required features are unsupported",
        ));
    }

    let mut keys = BTreeSet::new();
    for requirement in pack.required_evidence().iter().chain(pack.optional_evidence()) {
        if requirement.key.is_empty()
            || requirement.source.is_empty()
            || requirement.resource_prefix.is_empty()
            || requirement.purpose.is_empty()
        {
            return Err(invalid(id, "evidence descriptors must not contain empty fields"));
        }
        if matches!(requirement.source, "mcp" | "rocketmq_mcp") {
            return Err(invalid(
                id,
                "MCP evidence descriptors must use the canonical `rocketmq-mcp` source ID",
            ));
        }
        if !keys.insert(requirement.key) {
            return Err(invalid(id, "evidence descriptor keys must be unique"));
        }
    }

    let mut codes = BTreeSet::new();
    if pack
        .rule_codes()
        .iter()
        .any(|code| code.is_empty() || !codes.insert(*code))
    {
        return Err(invalid(id, "rule codes must be non-empty and unique"));
    }

    Ok(())
}

fn invalid(id: &str, reason: &str) -> DiagnosticRegistryError {
    DiagnosticRegistryError::InvalidDescriptor {
        id: id.to_owned(),
        reason: reason.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::super::DiagnosticContext;
    use super::super::DiagnosticError;
    use super::super::EvidenceRequirement;
    use super::super::FollowUpQuery;
    use super::super::RuleMatch;
    use super::*;

    const REQUIRED: &[EvidenceRequirement] = &[EvidenceRequirement {
        key: "test",
        source: "test",
        resource_prefix: "test/",
        purpose: "Registry version test",
    }];
    const MCP_ALIAS_REQUIRED: &[EvidenceRequirement] = &[EvidenceRequirement {
        key: "test",
        source: "mcp",
        resource_prefix: "test/",
        purpose: "Registry canonical source test",
    }];

    #[derive(Debug)]
    struct TestPack(PackVersion);
    #[derive(Debug)]
    struct McpAliasPack;

    impl DiagnosticPack for TestPack {
        fn id(&self) -> &'static str {
            "test-pack"
        }

        fn version(&self) -> PackVersion {
            self.0
        }

        fn applicable_components(&self) -> &'static [&'static str] {
            &["test"]
        }

        fn required_evidence(&self) -> &'static [EvidenceRequirement] {
            REQUIRED
        }

        fn optional_evidence(&self) -> &'static [EvidenceRequirement] {
            &[]
        }

        fn rule_codes(&self) -> &'static [&'static str] {
            &["TEST"]
        }

        fn follow_up_queries(&self) -> &'static [FollowUpQuery] {
            &[]
        }

        fn evaluate(&self, _context: &DiagnosticContext<'_>) -> Result<Vec<RuleMatch>, DiagnosticError> {
            Ok(Vec::new())
        }
    }

    impl DiagnosticPack for McpAliasPack {
        fn id(&self) -> &'static str {
            "mcp-alias-pack"
        }

        fn version(&self) -> PackVersion {
            PackVersion::new(1, 0, 0)
        }

        fn applicable_components(&self) -> &'static [&'static str] {
            &["test"]
        }

        fn required_evidence(&self) -> &'static [EvidenceRequirement] {
            MCP_ALIAS_REQUIRED
        }

        fn optional_evidence(&self) -> &'static [EvidenceRequirement] {
            &[]
        }

        fn rule_codes(&self) -> &'static [&'static str] {
            &["TEST"]
        }

        fn follow_up_queries(&self) -> &'static [FollowUpQuery] {
            &[]
        }

        fn evaluate(&self, _context: &DiagnosticContext<'_>) -> Result<Vec<RuleMatch>, DiagnosticError> {
            Ok(Vec::new())
        }
    }

    #[test]
    fn registers_versions_and_honors_explicit_activation_for_qualified_ids() {
        let initial = PackVersion::new(1, 0, 0);
        let upgrade = PackVersion::new(1, 1, 0);
        let mut registry = DiagnosticPackRegistry::default();
        registry
            .register(TestPack(initial))
            .expect("initial version should register");
        registry.register(TestPack(upgrade)).expect("upgrade should register");

        assert_eq!(
            registry
                .resolve("test-pack.v1")
                .expect("qualified pack should resolve")
                .version(),
            upgrade
        );

        registry
            .activate("test-pack", initial)
            .expect("known version should activate");
        assert_eq!(
            registry
                .resolve("test-pack.v1")
                .expect("qualified pack should honor activation")
                .version(),
            initial
        );
        assert_eq!(
            registry.register(TestPack(initial)),
            Err(DiagnosticRegistryError::DuplicateVersion {
                id: "test-pack".to_owned(),
                version: initial,
            })
        );
    }

    #[test]
    fn rejects_noncanonical_mcp_source_aliases() {
        let mut registry = DiagnosticPackRegistry::default();
        assert!(matches!(
            registry.register(McpAliasPack),
            Err(DiagnosticRegistryError::InvalidDescriptor { reason, .. })
                if reason.contains("canonical `rocketmq-mcp`")
        ));
    }
}
