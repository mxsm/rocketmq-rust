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
    key: "topology",
    source: "topology",
    resource_prefix: "asset-graph/",
    purpose: "Versioned component, Topic, Queue, Broker, client, and dependency graph",
}];

const OPTIONAL: &[EvidenceRequirement] = &[
    EvidenceRequirement {
        key: "kubernetes",
        source: "kubernetes",
        resource_prefix: "live-resources/",
        purpose: "Kubernetes workload readiness and ownership edges",
    },
    EvidenceRequirement {
        key: "client-connections",
        source: "admin-query",
        resource_prefix: "client-connections/",
        purpose: "Producer and consumer connection edges",
    },
];

const RULES: &[&str] = &[
    "TOPOLOGY_BROKEN_DEPENDENCY",
    "TOPOLOGY_ORPHAN_ASSET",
    "TOPOLOGY_CHANGED",
    "TOPOLOGY_WORKLOAD_UNREADY",
    "TOPOLOGY_CLIENT_DISCONNECTED",
    "TOPOLOGY_HEALTHY",
    "TOPOLOGY_EVIDENCE_INCOMPLETE",
];

const FOLLOW_UP: &[FollowUpQuery] = &[
    FollowUpQuery {
        source: "kubernetes",
        resource_template: "live-resources/{namespace}",
        reason: "Confirm workload ownership and readiness for topology nodes",
    },
    FollowUpQuery {
        source: "admin-query",
        resource_template: "client-connections/{cluster}",
        reason: "Confirm live client-to-broker edges",
    },
];

/// Asset and dependency graph consistency pack.
#[derive(Debug)]
pub struct ClusterTopologyV1;

impl DiagnosticPack for ClusterTopologyV1 {
    fn id(&self) -> &'static str {
        "cluster-topology"
    }

    fn version(&self) -> PackVersion {
        PackVersion::new(1, 0, 0)
    }

    fn applicable_components(&self) -> &'static [&'static str] {
        &["broker", "nameserver", "controller", "proxy", "client", "kubernetes"]
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
        if !context.is_available("topology") {
            return Ok(Vec::new());
        }
        let broken_edges = context.number("topology", "broken_edges");
        let orphan_assets = context.number("topology", "orphan_assets");
        let topology_changed = context.bool("topology", "topology_changed");
        if broken_edges.is_none() || orphan_assets.is_none() || topology_changed.is_none() {
            return Ok(common::incomplete(
                context,
                "topology",
                "TOPOLOGY_EVIDENCE_INCOMPLETE",
                &["broken_edges", "orphan_assets", "topology_changed"],
            )
            .into_iter()
            .collect());
        }

        let mut findings = Vec::new();
        if broken_edges.is_some_and(|value| value > 0.0) {
            findings.extend(common::conclusion(
                context,
                "topology",
                "TOPOLOGY_BROKEN_DEPENDENCY",
                "One or more topology dependency edges point to an absent or unhealthy asset",
                Severity::Critical,
                FindingOutcome::Fault,
                "The topology snapshot reports broken dependency edges",
            ));
        }
        if orphan_assets.is_some_and(|value| value > 0.0) {
            findings.extend(common::conclusion(
                context,
                "topology",
                "TOPOLOGY_ORPHAN_ASSET",
                "Assets are present without an expected owner or RocketMQ dependency",
                Severity::Warning,
                FindingOutcome::Fault,
                "The topology snapshot reports orphan assets",
            ));
        }
        if topology_changed == Some(true) {
            findings.extend(common::conclusion(
                context,
                "topology",
                "TOPOLOGY_CHANGED",
                "The live RocketMQ dependency graph differs from the previous snapshot",
                Severity::Warning,
                FindingOutcome::Fault,
                "The topology snapshot marks a versioned graph change",
            ));
        }
        if context
            .number("kubernetes", "unready_workloads")
            .is_some_and(|value| value > 0.0)
        {
            findings.extend(common::conclusion(
                context,
                "kubernetes",
                "TOPOLOGY_WORKLOAD_UNREADY",
                "Kubernetes workloads backing topology nodes are not ready",
                Severity::Critical,
                FindingOutcome::Fault,
                "The Kubernetes snapshot reports unready RocketMQ workloads",
            ));
        }
        if context
            .number("client-connections", "disconnected_clients")
            .is_some_and(|value| value > 0.0)
        {
            findings.extend(common::conclusion(
                context,
                "client-connections",
                "TOPOLOGY_CLIENT_DISCONNECTED",
                "Expected producer or consumer connection edges are absent",
                Severity::Warning,
                FindingOutcome::Fault,
                "The connection snapshot reports disconnected clients",
            ));
        }

        if findings.is_empty() {
            findings.extend(common::conclusion(
                context,
                "topology",
                "TOPOLOGY_HEALTHY",
                "No broken dependency, orphan asset, or topology drift was detected",
                Severity::Info,
                FindingOutcome::Healthy,
                "The complete topology snapshot reports no structural anomaly",
            ));
        }
        Ok(findings)
    }
}
