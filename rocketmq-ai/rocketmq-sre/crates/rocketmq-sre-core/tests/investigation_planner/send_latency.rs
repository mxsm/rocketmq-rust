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

use super::*;

#[test]
fn latency_partial_proof_retains_local_findings_without_strong_quality_or_clock_only_progress() {
    let registry = full_registry().unwrap();
    let pack = registry.resolve("send-latency.v2").unwrap();
    let reference = InvestigationPackRef {
        pack_id: pack.qualified_id(),
        pack_version: pack.version().to_string(),
    };
    let (_, mut ctx, mut run) = fixture();
    let content: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/diagnostics/send-latency-window.v1.json"
    ))
    .unwrap();
    let observed: DateTime<Utc> = content["collected_at"].as_str().unwrap().parse().unwrap();
    let scope = InvestigationRunScope {
        time_range: TimeRange::new(
            content["window"]["requested_start"].as_str().unwrap().parse().unwrap(),
            content["window"]["requested_end"].as_str().unwrap().parse().unwrap(),
        )
        .unwrap(),
        ..run.initial_scope.clone()
    };
    run.initial_scope = scope.clone();
    run.effective_scope = scope.clone();
    ctx.authorized_scope = scope;
    run.created_at = observed;
    run.updated_at = observed;
    run.deadline = observed + Duration::minutes(15);
    run.initial_packs = vec![reference.clone()];
    run.effective_packs = run.initial_packs.clone();
    ctx.allowed_packs = run.initial_packs.clone();
    ctx.allowed_requirements = vec![InvestigationRequirementRef {
        pack: reference.clone(),
        requirement_id: "send-latency".to_owned(),
    }];
    let mut content = content;
    content["scope"]["tenant_id"] = json!(run.initial_scope.tenant_id);
    content["scope"]["cluster_id"] = json!(run.initial_scope.cluster_id);
    content["policy"]["scope"]["cluster_id"] = json!(run.initial_scope.cluster_id);
    let mut sample = EvidenceSnapshot::capture(
        EvidenceQuery {
            query_id: QueryId::new(),
            correlation_id: CorrelationId::new(),
            tenant_id: run.initial_scope.tenant_id,
            cluster_id: run.initial_scope.cluster_id,
            source: "prometheus".to_owned(),
            resource: "send-latency/orders".to_owned(),
            time_range: run.effective_scope.time_range.clone(),
        },
        current_evidence_schema(),
        observed,
        EvidenceContent::Inline(content),
    )
    .unwrap();
    sample.freshness_seconds = 1;
    sample.exposure = EvidenceExposure::Synthetic;
    // A forged outer Available/complete declaration must not grant full-path proof.
    sample.partial = false;
    sample.coverage = CoverageStatus::Available;
    let bound = [InvestigationReadBinding {
        requirement: ctx.allowed_requirements[0].clone(),
        source: sample.source.clone(),
        resource: sample.resource.clone(),
        scope: run.effective_scope.clone(),
    }];
    let InvestigationPlanDecision::Collect(read) = plan_investigation(&registry, input(&run, &bound, &[])).unwrap()
    else {
        panic!("initial canonical read");
    };
    collect(&mut ctx, &mut run, read, &sample);
    let qualities = investigation_requirements(
        &registry,
        &reference,
        &input(&run, &bound, std::slice::from_ref(&sample)),
    )
    .unwrap();
    assert_eq!(qualities[0].quality, DiagnosticEvidenceQuality::Partial);
    assert!(
        qualities[0]
            .missing_fields
            .iter()
            .any(|field| field.contains("correlated_segment_population"))
    );
    let selected = investigation_rule_inputs(
        &registry,
        &reference,
        &input(&run, &bound, std::slice::from_ref(&sample)),
    )
    .unwrap();
    assert_eq!(selected.len(), 1);
    let report = DiagnosticEngine::new(full_registry().unwrap())
        .evaluate("send-latency.v2", &selected)
        .unwrap();
    assert_eq!(report.status, DiagnosticStatus::Inconclusive);
    assert!(
        report
            .findings
            .iter()
            .any(|finding| finding.reason_code == "BROKER_SEND_LATENCY_ESTIMATE_EXCEEDS_POLICY")
    );
    let InvestigationPlanDecision::Evaluate(evaluation) =
        plan_investigation(&registry, input(&run, &bound, std::slice::from_ref(&sample))).unwrap()
    else {
        panic!("save local observation");
    };
    evaluate(&ctx, &mut run, evaluation);
    let mut refreshed = sample.clone();
    refreshed.evidence_id = EvidenceId::new();
    refreshed.observed_at += Duration::milliseconds(1);
    if let EvidenceContent::Inline(content) = &mut refreshed.content {
        content["collected_at"] = json!(refreshed.observed_at.to_rfc3339());
        content["segments"][4]["availability"] = json!("unknown");
    }
    refreshed.content_hash = refreshed.compute_content_hash().unwrap();
    proof(&mut ctx, &run, &refreshed);
    steer(&ctx, &mut run, vec![reference.clone()], vec![refreshed.evidence_id]);
    for (age, selected_count) in [(299, 1), (301, 0)] {
        let evidence = [refreshed.clone()];
        let mut at = input(&run, &bound, &evidence);
        at.as_of_unix_ms = (refreshed.observed_at + Duration::seconds(age - 1)).timestamp_millis();
        assert_eq!(
            investigation_rule_inputs(&registry, &reference, &at).unwrap().len(),
            selected_count
        );
        assert_eq!(
            plan_investigation(&registry, at).unwrap(),
            InvestigationPlanDecision::Stop {
                reason: InvestigationStopReason::NoProgress
            }
        );
    }
    for invalid in ["scope", "hash", "scrape"] {
        let mut sample = refreshed.clone();
        if let EvidenceContent::Inline(content) = &mut sample.content {
            match invalid {
                "scope" => content["scope"]["topic"] = json!("foreign"),
                "scrape" => content["raw_histograms"][0]["buckets"][0]["samples"][0][0] = json!(-1),
                _ => {}
            }
        }
        sample.content_hash = if invalid == "hash" {
            "invalid".to_owned()
        } else {
            sample.compute_content_hash().unwrap()
        };
        assert!(
            investigation_rule_inputs(&registry, &reference, &input(&run, &bound, &[sample]))
                .unwrap()
                .is_empty()
        );
    }
    if let EvidenceContent::Inline(content) = &mut refreshed.content {
        content["policy"]["p99_millis"] = json!(300);
    }
    refreshed.content_hash = refreshed.compute_content_hash().unwrap();
    assert!(matches!(
        plan_investigation(&registry, input(&run, &bound, &[refreshed])).unwrap(),
        InvestigationPlanDecision::Evaluate(_)
    ));
}
