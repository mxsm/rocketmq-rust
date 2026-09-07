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

use std::collections::BTreeSet;
use std::fs;
use std::path::Component;
use std::path::Path;
use std::path::PathBuf;

use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::TenantId;
use serde::Deserialize;
use serde::Serialize;

use super::ShadowEvalFailure;

/// Version of the Phase 01 offline shadow manifest.
pub const SHADOW_MANIFEST_SCHEMA: &str = "rocketmq-sre.shadow-eval.v1";

/// The complete Phase 01 Wave A surface.
pub const WAVE_A_PACKS: [&str; 8] = [
    "broker-health.v1",
    "cluster-topology.v1",
    "consumer-lag.v2",
    "consumer-runtime.v1",
    "deployment-drift.v1",
    "message-path.v1",
    "producer-connectivity.v1",
    "telemetry-pipeline.v1",
];

/// Normal, fault, and missing-evidence cases required for every pack.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScenarioClass {
    Normal,
    Fault,
    Missing,
}

/// One fixture execution within a Wave A scenario.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ScenarioCase {
    pub class: ScenarioClass,
    pub fixture: PathBuf,
    pub expected_status: String,
}

/// One Wave A diagnostic scenario and its three evidence states.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ScenarioDefinition {
    pub id: String,
    pub pack: String,
    pub description: String,
    pub cases: Vec<ScenarioCase>,
}

/// Explicitly immutable read-only shadow policy.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ShadowPolicy {
    pub mutation_supported: bool,
    pub executor_connected: bool,
    pub connector_identity: String,
    pub model_visible_tools: BTreeSet<String>,
}

impl ShadowPolicy {
    /// Validates that the manifest cannot enable a cluster mutation path.
    ///
    /// # Errors
    ///
    /// Returns an opaque evaluation failure for mutation, Executor, or unknown
    /// model tool exposure.
    pub fn validate(&self) -> Result<crate::EvalOutcome<()>, crate::EvalError> {
        match self.validate_inner() {
            Ok(()) => Ok(crate::EvalOutcome::Completed(())),
            Err(failure) => failure.into_boundary(),
        }
    }

    pub(super) fn validate_inner(&self) -> Result<(), ShadowEvalFailure> {
        if self.mutation_supported {
            return Err(ShadowEvalFailure::UnsafePolicy(
                "mutation_supported must remain false".to_owned(),
            ));
        }
        if self.executor_connected {
            return Err(ShadowEvalFailure::UnsafePolicy(
                "executor_connected must remain false".to_owned(),
            ));
        }
        if self.connector_identity != "read_only" {
            return Err(ShadowEvalFailure::UnsafePolicy(
                "connector_identity must be read_only".to_owned(),
            ));
        }

        let allowed = BTreeSet::from([
            "query_evidence".to_owned(),
            "read_runtime".to_owned(),
            "read_topology".to_owned(),
            "search_knowledge".to_owned(),
        ]);
        if let Some(tool) = self.model_visible_tools.difference(&allowed).next() {
            return Err(ShadowEvalFailure::UnsafePolicy(format!(
                "tool `{tool}` is outside the Phase 01 read-only surface"
            )));
        }
        Ok(())
    }
}

/// Versioned suite of offline Wave A scenarios.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ShadowManifest {
    pub schema_version: String,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub policy: ShadowPolicy,
    pub scenarios: Vec<ScenarioDefinition>,
}

impl ShadowManifest {
    /// Validates exact Wave A and normal/fault/missing coverage.
    ///
    /// # Errors
    ///
    /// Fails closed on schema drift, duplicate packs, incomplete cases, or
    /// unsafe fixture paths.
    pub fn validate(&self) -> Result<crate::EvalOutcome<()>, crate::EvalError> {
        match self.validate_inner() {
            Ok(()) => Ok(crate::EvalOutcome::Completed(())),
            Err(failure) => failure.into_boundary(),
        }
    }

    pub(super) fn validate_inner(&self) -> Result<(), ShadowEvalFailure> {
        if self.schema_version != SHADOW_MANIFEST_SCHEMA {
            return Err(ShadowEvalFailure::InvalidManifest(format!(
                "schema `{}` is unsupported; expected `{SHADOW_MANIFEST_SCHEMA}`",
                self.schema_version
            )));
        }
        self.policy.validate_inner()?;

        let mut packs = BTreeSet::new();
        let mut scenario_ids = BTreeSet::new();
        for scenario in &self.scenarios {
            if !scenario_ids.insert(&scenario.id) {
                return Err(ShadowEvalFailure::InvalidManifest(format!(
                    "duplicate scenario id `{}`",
                    scenario.id
                )));
            }
            if !packs.insert(scenario.pack.as_str()) {
                return Err(ShadowEvalFailure::InvalidManifest(format!(
                    "duplicate pack `{}`",
                    scenario.pack
                )));
            }
            let mut classes = BTreeSet::new();
            for case in &scenario.cases {
                if !classes.insert(case.class) {
                    return Err(ShadowEvalFailure::InvalidManifest(format!(
                        "{} contains duplicate {:?} case",
                        scenario.pack, case.class
                    )));
                }
                validate_fixture_path(&case.fixture)?;
            }
            let required = BTreeSet::from([ScenarioClass::Normal, ScenarioClass::Fault, ScenarioClass::Missing]);
            if classes != required {
                return Err(ShadowEvalFailure::InvalidManifest(format!(
                    "{} must contain exactly normal, fault, and missing cases",
                    scenario.pack
                )));
            }
        }

        let expected = WAVE_A_PACKS.into_iter().collect::<BTreeSet<_>>();
        if packs != expected {
            return Err(ShadowEvalFailure::InvalidManifest(format!(
                "Wave A pack surface mismatch: expected {expected:?}, found {packs:?}"
            )));
        }
        Ok(())
    }
}

/// Loads and validates a versioned shadow manifest.
///
/// # Errors
///
/// Returns a redacted I/O, YAML, schema, policy, or coverage error.
pub(super) fn load_shadow_manifest(path: &Path) -> Result<ShadowManifest, ShadowEvalFailure> {
    let raw = fs::read_to_string(path).map_err(|source| ShadowEvalFailure::Io {
        _path: path.to_path_buf(),
        source,
    })?;
    let manifest = serde_yaml::from_str::<ShadowManifest>(&raw).map_err(ShadowEvalFailure::ManifestDecode)?;
    manifest.validate_inner()?;
    Ok(manifest)
}

fn validate_fixture_path(path: &Path) -> Result<(), ShadowEvalFailure> {
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(ShadowEvalFailure::InvalidManifest(format!(
            "fixture path `{}` must be a non-empty relative path without traversal",
            path.display()
        )));
    }
    Ok(())
}
