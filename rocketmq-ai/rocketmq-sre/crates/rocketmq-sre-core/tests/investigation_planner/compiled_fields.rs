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
