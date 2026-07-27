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

use super::super::DiagnosticContext;
use super::super::DiagnosticError;
use super::super::DiagnosticPack;
use super::super::EvidenceRequirement;
use super::super::FindingOutcome;
use super::super::FollowUpQuery;
use super::super::PackVersion;
use super::super::RuleMatch;
use super::super::Severity;
use super::common;

const REQUIRED: &[EvidenceRequirement] = &[EvidenceRequirement {
    key: "deployment-diff",
    source: "kubernetes",
    resource_prefix: "deployment-drift/",
    purpose: "Body-free desired/live image, feature, config, Secret metadata, RBAC, PDB, PVC, replica, and topology \
              diff",
}];

const OPTIONAL: &[EvidenceRequirement] = &[EvidenceRequirement {
    key: "runtime-build",
    source: "runtime",
    resource_prefix: "build-info/",
    purpose: "Runtime confirmation of image revision and compiled features",
}];

const RULES: &[&str] = &[
    "DEPLOYMENT_IMAGE_FEATURE_DRIFT",
    "DEPLOYMENT_CONFIG_DRIFT",
    "DEPLOYMENT_SECURITY_DRIFT",
    "DEPLOYMENT_AVAILABILITY_DRIFT",
    "DEPLOYMENT_STORAGE_DRIFT",
    "DEPLOYMENT_TOPOLOGY_DRIFT",
    "DEPLOYMENT_IN_SYNC",
    "DEPLOYMENT_DRIFT_EVIDENCE_INCOMPLETE",
];

const FOLLOW_UP: &[FollowUpQuery] = &[FollowUpQuery {
    source: "runtime",
    resource_template: "build-info/{component}",
    reason: "Confirm live image revision and compiled feature set without reading Secret values",
}];

const DRIFT_FIELDS: &[&str] = &[
    "image_drift",
    "feature_drift",
    "config_drift",
    "secret_metadata_drift",
    "rbac_drift",
    "pdb_drift",
    "pvc_drift",
    "replica_drift",
    "topology_drift",
];

/// Desired/live deployment drift pack.
#[derive(Debug)]
pub struct DeploymentDriftV1;

impl DiagnosticPack for DeploymentDriftV1 {
    fn id(&self) -> &'static str {
        "deployment-drift"
    }

    fn version(&self) -> PackVersion {
        PackVersion::new(1, 0, 0)
    }

    fn applicable_components(&self) -> &'static [&'static str] {
        &[
            "broker",
            "nameserver",
            "controller",
            "proxy",
            "mcp",
            "sre",
            "kubernetes",
        ]
    }

    fn required_evidence(&self) -> &'static [EvidenceRequirement] {
        REQUIRED
    }

    fn optional_evidence(&self) -> &'static [EvidenceRequirement] {
        OPTIONAL
    }

    fn rule_codes(&self) -> &'static [&'static str] {
        RULES
    }

    fn follow_up_queries(&self) -> &'static [FollowUpQuery] {
        FOLLOW_UP
    }

    fn evaluate(&self, context: &DiagnosticContext<'_>) -> Result<Vec<RuleMatch>, DiagnosticError> {
        if !context.is_available("deployment-diff") {
            return Ok(Vec::new());
        }
        if DRIFT_FIELDS
            .iter()
            .any(|field| context.bool("deployment-diff", field).is_none())
        {
            return Ok(common::incomplete(
                context,
                "deployment-diff",
                "DEPLOYMENT_DRIFT_EVIDENCE_INCOMPLETE",
                DRIFT_FIELDS,
            )
            .into_iter()
            .collect());
        }

        let mut findings = Vec::new();
        if is_drift(context, "image_drift") || is_drift(context, "feature_drift") {
            findings.extend(
                common::conclusion(
                    context,
                    "deployment-diff",
                    "DEPLOYMENT_IMAGE_FEATURE_DRIFT",
                    "Live image or compiled features differ from the desired deployment",
                    Severity::Critical,
                    FindingOutcome::Fault,
                    "The body-free desired/live diff reports image or feature drift",
                )
                .map(|finding| finding.with_matched_signals(2)),
            );
        }
        if is_drift(context, "config_drift") {
            findings.extend(common::conclusion(
                context,
                "deployment-diff",
                "DEPLOYMENT_CONFIG_DRIFT",
                "Live non-secret configuration differs from the desired deployment",
                Severity::Warning,
                FindingOutcome::Fault,
                "The desired/live diff reports sanitized configuration drift",
            ));
        }
        if is_drift(context, "secret_metadata_drift") || is_drift(context, "rbac_drift") {
            findings.extend(
                common::conclusion(
                    context,
                    "deployment-diff",
                    "DEPLOYMENT_SECURITY_DRIFT",
                    "Secret metadata or RBAC policy differs from the desired deployment",
                    Severity::Critical,
                    FindingOutcome::Fault,
                    "The diff reports metadata or policy drift without exposing Secret values",
                )
                .map(|finding| finding.with_matched_signals(2)),
            );
        }
        if is_drift(context, "pdb_drift") || is_drift(context, "replica_drift") {
            findings.extend(
                common::conclusion(
                    context,
                    "deployment-diff",
                    "DEPLOYMENT_AVAILABILITY_DRIFT",
                    "PDB or replica count differs from the desired availability posture",
                    Severity::Critical,
                    FindingOutcome::Fault,
                    "The desired/live diff reports availability control drift",
                )
                .map(|finding| finding.with_matched_signals(2)),
            );
        }
        if is_drift(context, "pvc_drift") {
            findings.extend(common::conclusion(
                context,
                "deployment-diff",
                "DEPLOYMENT_STORAGE_DRIFT",
                "PVC configuration differs from the desired durable storage posture",
                Severity::Critical,
                FindingOutcome::Fault,
                "The desired/live diff reports PVC drift",
            ));
        }
        if is_drift(context, "topology_drift") {
            findings.extend(common::conclusion(
                context,
                "deployment-diff",
                "DEPLOYMENT_TOPOLOGY_DRIFT",
                "Live placement or topology differs from the desired deployment",
                Severity::Warning,
                FindingOutcome::Fault,
                "The desired/live diff reports topology drift",
            ));
        }

        if findings.is_empty() {
            findings.extend(
                common::conclusion(
                    context,
                    "deployment-diff",
                    "DEPLOYMENT_IN_SYNC",
                    "Image, features, configuration, security, availability, storage, and topology match",
                    Severity::Info,
                    FindingOutcome::Healthy,
                    "All nine sanitized desired/live drift flags are false",
                )
                .map(|finding| finding.with_matched_signals(9)),
            );
        }
        Ok(findings)
    }
}

fn is_drift(context: &DiagnosticContext<'_>, field: &str) -> bool {
    context.bool("deployment-diff", field) == Some(true)
}
