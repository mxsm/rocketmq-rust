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
fn store_pressure_capture_refresh_is_not_progress_but_effective_policy_change_is() {
    let registry = full_registry().unwrap();
    let pack = registry.resolve("store-pressure.v2").unwrap();
    let reference = InvestigationPackRef {
        pack_id: pack.qualified_id(),
        pack_version: pack.version().to_string(),
    };
    let (_, mut ctx, mut run) = fixture();
    run.initial_packs = vec![reference.clone()];
    run.effective_packs = run.initial_packs.clone();
    ctx.allowed_packs = run.initial_packs.clone();
    ctx.allowed_requirements = vec![InvestigationRequirementRef {
        pack: reference.clone(),
        requirement_id: "store-pressure".to_owned(),
    }];
    let mut observation: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/diagnostics/store-pressure-observation.v1.json"
    ))
    .unwrap();
    observation["observed_at_millis"] = json!((run.updated_at - Duration::seconds(2)).timestamp_millis());
    let mut first = snapshot(&run, true);
    first.observed_at -= Duration::seconds(1);
    first.source = "admin-query".to_owned();
    first.resource = "store-pressure/broker-a".to_owned();
    first.coverage = CoverageStatus::Partial;
    first.content = EvidenceContent::Inline(json!({"schema_version":"rocketmq.sre-diagnostic-source.v1",
        "profile":"store_pressure","collected_at":first.observed_at.to_rfc3339(),"observed_brokers":1,
        "brokers":[{"broker_name":"broker-a","broker_id":0,"store_pressure":observation}]}));
    first.content_hash = first.compute_content_hash().unwrap();
    let bound = [InvestigationReadBinding {
        requirement: ctx.allowed_requirements[0].clone(),
        source: first.source.clone(),
        resource: first.resource.clone(),
        scope: run.effective_scope.clone(),
    }];
    let InvestigationPlanDecision::Collect(read) = plan_investigation(&registry, input(&run, &bound, &[])).unwrap()
    else {
        panic!("the shared read precedes its evaluation");
    };
    collect(&mut ctx, &mut run, read, &first);
    let InvestigationPlanDecision::Evaluate(planned) =
        plan_investigation(&registry, input(&run, &bound, std::slice::from_ref(&first))).unwrap()
    else {
        panic!("initial real observations need a saved evaluation");
    };
    evaluate(&ctx, &mut run, planned);
    let mut refreshed = first.clone();
    refreshed.evidence_id = EvidenceId::new();
    refreshed.observed_at += Duration::milliseconds(1);
    if let EvidenceContent::Inline(content) = &mut refreshed.content {
        content["collected_at"] = json!(refreshed.observed_at.to_rfc3339());
        content["brokers"][0]["store_pressure"]["observed_at_millis"] =
            json!((refreshed.observed_at - Duration::seconds(1)).timestamp_millis());
        content["brokers"][0]["observation_status"] = json!("instantaneous");
        content["brokers"][0]["signals"] = json!([]);
    }
    refreshed.content_hash = refreshed.compute_content_hash().unwrap();
    assert_ne!(first.content_hash, refreshed.content_hash);
    proof(&mut ctx, &run, &refreshed);
    let packs = run.effective_packs.clone();
    steer(&ctx, &mut run, packs, vec![refreshed.evidence_id]);
    assert_eq!(
        plan_investigation(&registry, input(&run, &bound, std::slice::from_ref(&refreshed))).unwrap(),
        InvestigationPlanDecision::Stop {
            reason: InvestigationStopReason::NoProgress,
        }
    );
    for (native_age_seconds, expected_inputs) in [(299, 1), (301, 0)] {
        let observations = [refreshed.clone()];
        let mut aged = input(&run, &bound, &observations);
        aged.as_of_unix_ms =
            (refreshed.observed_at - Duration::seconds(1) + Duration::seconds(native_age_seconds)).timestamp_millis();
        assert_eq!(
            investigation_rule_inputs(&registry, &reference, &aged).unwrap().len(),
            expected_inputs
        );
        assert_eq!(
            plan_investigation(&registry, aged).unwrap(),
            InvestigationPlanDecision::Stop {
                reason: InvestigationStopReason::NoProgress,
            }
        );
    }
    for (section, field, value) in [
        ("policy", "disk_max_used_ratio", json!(0.10)),
        ("runtime", "sync_flush_timeout_total", json!(38)),
        ("runtime", "dispatch_behind_bytes", serde_json::Value::Null),
    ] {
        let mut changed = refreshed.clone();
        if let EvidenceContent::Inline(content) = &mut changed.content {
            content["brokers"][0]["store_pressure"][section][field] = value;
        }
        changed.content_hash = changed.compute_content_hash().unwrap();
        assert!(matches!(
            plan_investigation(&registry, input(&run, &bound, &[changed])).unwrap(),
            InvestigationPlanDecision::Evaluate(_)
        ));
    }
}

#[test]
fn store_pressure_saved_complete_flag_does_not_supply_missing_progress_proof() {
    let registry = full_registry().unwrap();
    let pack = registry.resolve("store-pressure.v2").unwrap();
    let reference = InvestigationPackRef {
        pack_id: pack.qualified_id(),
        pack_version: pack.version().to_string(),
    };
    let (_, mut ctx, mut run) = fixture();
    run.initial_packs = vec![reference.clone()];
    run.effective_packs = run.initial_packs.clone();
    ctx.allowed_packs = run.initial_packs.clone();
    ctx.allowed_requirements = vec![InvestigationRequirementRef {
        pack: reference.clone(),
        requirement_id: "store-pressure".to_owned(),
    }];
    let mut observation: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/diagnostics/store-pressure-observation.v1.json"
    ))
    .unwrap();
    observation["observed_at_millis"] = json!(run.updated_at.timestamp_millis());
    let mut sample = snapshot(&run, false);
    sample.source = "admin-query".to_owned();
    sample.resource = "store-pressure/broker-a".to_owned();
    sample.content = EvidenceContent::Inline(json!({"schema_version":"rocketmq.sre-diagnostic-source.v1",
        "profile":"store_pressure","observed_brokers":1,
        "brokers":[{"broker_name":"broker-a","broker_id":0,"store_pressure":observation}]}));
    sample.content_hash = sample.compute_content_hash().unwrap();
    proof(&mut ctx, &run, &sample);
    steer(&ctx, &mut run, vec![reference.clone()], vec![sample.evidence_id]);
    let bound = [InvestigationReadBinding {
        requirement: ctx.allowed_requirements[0].clone(),
        source: sample.source.clone(),
        resource: sample.resource.clone(),
        scope: run.effective_scope.clone(),
    }];
    let evidence = [sample];
    let qualities = investigation_requirements(&registry, &reference, &input(&run, &bound, &evidence)).unwrap();
    assert_eq!(qualities[0].quality, DiagnosticEvidenceQuality::Partial);
    assert_eq!(qualities[0].evidence_ids, vec![evidence[0].evidence_id]);
    assert!(
        qualities[0]
            .missing_fields
            .iter()
            .any(|fact| fact.contains("monotonic_progress"))
    );
    let selected = investigation_rule_inputs(&registry, &reference, &input(&run, &bound, &evidence)).unwrap();
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].evidence_id, evidence[0].evidence_id);
    let mut stale = evidence[0].clone();
    stale.freshness_seconds = 301;
    assert!(
        investigation_rule_inputs(&registry, &reference, &input(&run, &bound, &[stale]))
            .unwrap()
            .is_empty()
    );
    let mut malformed = evidence[0].clone();
    if let EvidenceContent::Inline(content) = &mut malformed.content {
        content["brokers"][0]["store_pressure"]["disk"]["commit_log_max_used_ratio"] = json!(-1);
    }
    malformed.content_hash = malformed.compute_content_hash().unwrap();
    assert!(
        investigation_rule_inputs(&registry, &reference, &input(&run, &bound, &[malformed]))
            .unwrap()
            .is_empty()
    );
    let mut bad_hash = evidence[0].clone();
    bad_hash.content_hash = "invalid".to_owned();
    assert!(
        investigation_rule_inputs(&registry, &reference, &input(&run, &bound, &[bad_hash]))
            .unwrap()
            .is_empty()
    );
    let report = DiagnosticEngine::new(registry)
        .evaluate("store-pressure.v2", &evidence)
        .unwrap();
    assert_eq!(report.status, DiagnosticStatus::Inconclusive);
    assert!(
        report
            .findings
            .iter()
            .any(|finding| finding.reason_code == "STORE_DISK_WARNING_RATIO_EXCEEDED")
    );
}

#[test]
fn compiled_readable_packs_expose_their_actual_rule_field_types() {
    let registry = full_registry().unwrap();
    let cases = [
        (
            "broker-health.v1",
            "broker-metrics",
            json!({
                "ready":true, "request_error_rate_percent":0, "active_connections":2,
                "cpu_percent":20, "rss_bytes":1024, "disk_used_percent":30,
                "flush_latency_ms":2, "dispatch_backlog":0, "ha_lag_bytes":0
            }),
            "ready",
        ),
        (
            "broker-health.v1",
            "broker-runtime",
            json!({"broker_up":false}),
            "broker_up",
        ),
        (
            "consumer-runtime.v1",
            "runtime",
            json!({
                "connected":true, "running_info_available":true, "rebalance_in_progress":false,
                "unassigned_queues":0, "process_rt_ms":1, "paused":false,
                "commit_failures":0, "seek_operations":0, "client_version_supported":true
            }),
            "connected",
        ),
        (
            "consumer-lag.v2",
            "consumer-runtime",
            json!({"paused":false,"connected_clients":2}),
            "connected_clients",
        ),
        (
            "consumer-lag.v2",
            "broker-store",
            json!({"broker_ready":true,"store_healthy":true}),
            "store_healthy",
        ),
        (
            "producer-connectivity.v1",
            "topic-route",
            json!({"route_available":false}),
            "route_available",
        ),
    ];
    for (id, key, content, corrupted_field) in cases {
        let (_, mut ctx, mut run) = fixture();
        let pack = registry.active_packs().find(|pack| pack.qualified_id() == id).unwrap();
        let reference = InvestigationPackRef {
            pack_id: pack.qualified_id(),
            pack_version: pack.version().to_string(),
        };
        run.initial_packs = vec![reference.clone()];
        run.effective_packs = run.initial_packs.clone();
        ctx.allowed_packs = run.initial_packs.clone();
        ctx.allowed_requirements = pack
            .required_evidence()
            .iter()
            .chain(pack.optional_evidence())
            .map(|requirement| InvestigationRequirementRef {
                pack: reference.clone(),
                requirement_id: requirement.key.to_owned(),
            })
            .collect();
        let requirement = pack
            .required_evidence()
            .iter()
            .chain(pack.optional_evidence())
            .find(|requirement| requirement.key == key)
            .unwrap();
        let mut sample = snapshot(&run, false);
        sample.source = requirement.source.to_owned();
        sample.resource = format!("{}fixture-target", requirement.resource_prefix);
        sample.content = EvidenceContent::Inline(content);
        sample.content_hash = sample.compute_content_hash().unwrap();
        proof(&mut ctx, &run, &sample);
        steer(&ctx, &mut run, vec![reference.clone()], vec![sample.evidence_id]);
        let bound = [InvestigationReadBinding {
            requirement: InvestigationRequirementRef {
                pack: reference.clone(),
                requirement_id: key.to_owned(),
            },
            source: sample.source.clone(),
            resource: sample.resource.clone(),
            scope: run.effective_scope.clone(),
        }];
        let evidence = [sample.clone()];
        let quality = investigation_requirements(&registry, &reference, &input(&run, &bound, &evidence)).unwrap();
        let slot = quality.iter().find(|quality| quality.id == key).unwrap();
        assert_eq!(
            slot.quality,
            DiagnosticEvidenceQuality::Available,
            "{id}/{key}: {slot:?}"
        );
        let mut expired = sample.clone();
        expired.freshness_seconds = pack.max_evidence_freshness_seconds() + 1;
        let evidence = [expired];
        let quality = investigation_requirements(&registry, &reference, &input(&run, &bound, &evidence)).unwrap();
        assert_eq!(
            quality.iter().find(|quality| quality.id == key).unwrap().quality,
            DiagnosticEvidenceQuality::Stale
        );
        let EvidenceContent::Inline(content) = &mut sample.content else {
            unreachable!()
        };
        content[corrupted_field] = serde_json::Value::Null;
        sample.content_hash = sample.compute_content_hash().unwrap();
        let evidence = [sample];
        let quality = investigation_requirements(&registry, &reference, &input(&run, &bound, &evidence)).unwrap();
        let slot = quality.iter().find(|quality| quality.id == key).unwrap();
        assert_eq!(slot.quality, DiagnosticEvidenceQuality::Partial, "{id}/{key}");
        assert!(slot.missing_fields.iter().any(|field| field == corrupted_field));
    }
}
