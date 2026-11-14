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

//! Deterministic, read-only diagnostic packs and their execution engine.

mod confidence;
mod engine;
mod investigation;
pub mod packs;
mod registry;
mod types;

pub use confidence::ConfidenceInputs;
pub use confidence::calculate_confidence;
pub use engine::DiagnosticContext;
pub use engine::DiagnosticEngine;
pub use investigation::{
    InvestigationAggregateDecision, InvestigationExplanationPolicy, InvestigationPlanDecision,
    InvestigationPlannedCollection, InvestigationPlannedEvaluation, InvestigationPlannedExplanation,
    InvestigationPlanningInput, InvestigationReadBinding, InvestigationReservationPolicy, aggregate_investigation,
    investigation_conclusion_evidence_available, investigation_findings, investigation_requirements,
    investigation_rule_inputs, plan_investigation,
};
pub use packs::active_pack_ids;
pub use packs::full_pack_ids;
pub use packs::full_registry;
pub use packs::qualification_baseline_pack_ids;
pub use packs::wave_a_registry;
pub use packs::wave_b_registry;
pub use packs::wave_c_registry;
pub use registry::DiagnosticPackRegistry;
pub use registry::DiagnosticRegistryRejection;
pub use types::ConfidenceBand;
pub use types::ConfidenceScore;
pub use types::DIAGNOSTIC_OUTPUT_SCHEMA_FAMILY;
pub use types::DIAGNOSTIC_OUTPUT_SCHEMA_MAJOR;
pub use types::DIAGNOSTIC_OUTPUT_SCHEMA_MINOR;
pub use types::DiagnosticFinding;
pub use types::DiagnosticPack;
pub use types::DiagnosticReport;
pub use types::DiagnosticStatus;
pub use types::EvidenceRequirement;
pub use types::FindingOutcome;
pub use types::FollowUpQuery;
pub use types::PackVersion;
pub use types::RuleEvidence;
pub use types::RuleMatch;
pub use types::Severity;
