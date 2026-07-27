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
use super::super::FindingOutcome;
use super::super::RuleMatch;
use super::super::Severity;

pub(super) fn conclusion(
    context: &DiagnosticContext<'_>,
    requirement: &str,
    reason_code: &'static str,
    root_cause: &'static str,
    severity: Severity,
    outcome: FindingOutcome,
    rationale: &'static str,
) -> Option<RuleMatch> {
    context
        .cite(requirement, rationale)
        .map(|evidence| RuleMatch::new(reason_code, root_cause, severity, outcome).with_support(evidence))
}

pub(super) fn incomplete(
    context: &DiagnosticContext<'_>,
    requirement: &str,
    reason_code: &'static str,
    missing_fields: &[&str],
) -> Option<RuleMatch> {
    let mut finding = conclusion(
        context,
        requirement,
        reason_code,
        "Required fields are absent from the available evidence snapshot",
        Severity::Warning,
        FindingOutcome::Inconclusive,
        "The snapshot exists but does not contain every field required by this pack",
    )?;
    for field in missing_fields {
        finding = finding.with_missing(format!("{requirement}.{field}"));
    }
    Some(finding)
}
