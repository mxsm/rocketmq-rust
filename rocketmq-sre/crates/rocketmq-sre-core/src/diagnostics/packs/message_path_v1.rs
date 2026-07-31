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

use rocketmq_sre_contracts::EvidenceContent;
use rocketmq_sre_contracts::EvidenceSnapshot;

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
    key: "message-metadata",
    source: "admin-query",
    resource_prefix: "message-metadata/",
    purpose: "Pseudonymized IDs/keys, queue/offset, and send-to-ack stage metadata",
}];

const OPTIONAL: &[EvidenceRequirement] = &[
    EvidenceRequirement {
        key: "trace",
        source: "tempo",
        resource_prefix: "message-trace/",
        purpose: "Trace metadata without message content",
    },
    EvidenceRequirement {
        key: "topic-route",
        source: "rocketmq-mcp",
        resource_prefix: "topic-route/",
        purpose: "Topic route metadata for the route segment",
    },
];

const RULES: &[&str] = &[
    "MESSAGE_SEND_STAGE_MISSING",
    "MESSAGE_ROUTE_STAGE_MISSING",
    "MESSAGE_STORE_STAGE_MISSING",
    "MESSAGE_DELIVERY_STAGE_MISSING",
    "MESSAGE_ACK_STAGE_MISSING",
    "MESSAGE_TRANSACTION_STATUS_UNKNOWN",
    "MESSAGE_PATH_COMPLETE",
    "MESSAGE_PATH_EVIDENCE_INCOMPLETE",
];

const FOLLOW_UP: &[FollowUpQuery] = &[
    FollowUpQuery {
        source: "tempo",
        resource_template: "message-trace/{trace_id_hash}",
        reason: "Correlate missing path stages using trace metadata only",
    },
    FollowUpQuery {
        source: "rocketmq-mcp",
        resource_template: "topic-route/{topic}",
        reason: "Confirm the route stage without requesting message content",
    },
];

const FORBIDDEN_JSON_KEYS: &[&str] = &[
    "\"body\":",
    "\"message_body\":",
    "\"messagebody\":",
    "\"payload\":",
    "\"body_base64\":",
    "\"message_body_base64\":",
];

/// Message journey pack restricted to pseudonymized metadata.
#[derive(Debug)]
pub struct MessagePathV1;

impl DiagnosticPack for MessagePathV1 {
    fn id(&self) -> &'static str {
        "message-path"
    }

    fn version(&self) -> PackVersion {
        PackVersion::new(1, 0, 0)
    }

    fn applicable_components(&self) -> &'static [&'static str] {
        &["producer", "nameserver", "broker", "store", "consumer"]
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

    fn validate_evidence(&self, evidence: &[EvidenceSnapshot]) -> Result<(), DiagnosticError> {
        for snapshot in evidence.iter().filter(|snapshot| {
            (snapshot.source == "admin-query" && snapshot.resource.starts_with("message-metadata/"))
                || (snapshot.source == "tempo" && snapshot.resource.starts_with("message-trace/"))
        }) {
            let EvidenceContent::Inline(content) = &snapshot.content else {
                return Err(DiagnosticError::MessageMetadataReferenceRejected {
                    evidence_id: snapshot.evidence_id,
                });
            };
            let compact = content.to_string().to_ascii_lowercase();
            if FORBIDDEN_JSON_KEYS.iter().any(|key| compact.contains(key)) {
                return Err(DiagnosticError::MessageBodyRejected {
                    evidence_id: snapshot.evidence_id,
                });
            }
        }
        Ok(())
    }

    fn evaluate(&self, context: &DiagnosticContext<'_>) -> Result<Vec<RuleMatch>, DiagnosticError> {
        if !context.is_available("message-metadata") {
            return Ok(Vec::new());
        }
        let stages = [
            ("send", context.bool("message-metadata", "stages.send")),
            ("route", context.bool("message-metadata", "stages.route")),
            ("store", context.bool("message-metadata", "stages.store")),
            ("deliver", context.bool("message-metadata", "stages.deliver")),
            ("ack", context.bool("message-metadata", "stages.ack")),
        ];
        if stages.iter().any(|(_, value)| value.is_none()) {
            return Ok(common::incomplete(
                context,
                "message-metadata",
                "MESSAGE_PATH_EVIDENCE_INCOMPLETE",
                &[
                    "stages.send",
                    "stages.route",
                    "stages.store",
                    "stages.deliver",
                    "stages.ack",
                ],
            )
            .into_iter()
            .collect());
        }

        let mut findings = Vec::new();
        let stage_rules = [
            (
                "send",
                "MESSAGE_SEND_STAGE_MISSING",
                "No pseudonymized metadata confirms the producer send stage",
            ),
            (
                "route",
                "MESSAGE_ROUTE_STAGE_MISSING",
                "No route metadata connects the send stage to a Broker",
            ),
            (
                "store",
                "MESSAGE_STORE_STAGE_MISSING",
                "No queue/offset metadata confirms that the Broker stored the message",
            ),
            (
                "deliver",
                "MESSAGE_DELIVERY_STAGE_MISSING",
                "No metadata confirms delivery from Store to a consumer",
            ),
            (
                "ack",
                "MESSAGE_ACK_STAGE_MISSING",
                "No metadata confirms the final acknowledgement stage",
            ),
        ];
        for (stage, reason_code, root_cause) in stage_rules {
            if context.bool("message-metadata", &format!("stages.{stage}")) == Some(false) {
                findings.extend(common::conclusion(
                    context,
                    "message-metadata",
                    reason_code,
                    root_cause,
                    Severity::Critical,
                    FindingOutcome::Fault,
                    "The body-free message metadata marks this path stage absent",
                ));
            }
        }
        if context.text("message-metadata", "transaction_status") == Some("unknown") {
            findings.extend(common::conclusion(
                context,
                "message-metadata",
                "MESSAGE_TRANSACTION_STATUS_UNKNOWN",
                "Transaction status metadata is unavailable or unresolved",
                Severity::Warning,
                FindingOutcome::Fault,
                "The transaction metadata field is explicitly unknown",
            ));
        }

        if findings.is_empty() {
            findings.extend(
                common::conclusion(
                    context,
                    "message-metadata",
                    "MESSAGE_PATH_COMPLETE",
                    "Pseudonymized metadata confirms send, route, store, deliver, and acknowledgement",
                    Severity::Info,
                    FindingOutcome::Healthy,
                    "All five body-free message path stages are present",
                )
                .map(|finding| finding.with_matched_signals(5)),
            );
        }
        Ok(findings)
    }
}
