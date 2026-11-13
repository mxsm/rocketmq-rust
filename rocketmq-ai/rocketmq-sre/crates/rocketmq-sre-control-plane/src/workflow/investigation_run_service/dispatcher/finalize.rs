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

impl RunDispatcher {
    pub(super) async fn finalize(
        &self,
        service: &InvestigationRunService,
        lease: &RunLease,
        run: &InvestigationRun,
        stop_reason: Option<InvestigationStopReason>,
        bindings: &[InvestigationReadBinding],
        evidence: &[EvidenceSnapshot],
    ) -> Result<(), ControlPlaneRequestFailure> {
        let delegation = service.repository.investigation_run_delegation(run.id).await?;
        let auth = delegation.read_auth(run)?;
        let context = self.context(service, auth, run, &[]).await?;
        let worker = RunWorkerContext {
            auth,
            context: &context,
            fence: &lease.fence,
        };
        let input = self.planning_input(run, bindings, evidence);
        let mut saved = Vec::new();
        let mut qualified = stop_reason.is_none();
        let mut missing = Vec::new();
        for pack in &run.effective_packs {
            let current = investigation_requirements(&service.catalog, pack, &input).map_err(contract_failure)?;
            missing.extend(
                current
                    .iter()
                    .filter(|requirement| {
                        requirement.required && requirement.quality != DiagnosticEvidenceQuality::Available
                    })
                    .map(|requirement| InvestigationRequirementRef {
                        pack: pack.clone(),
                        requirement_id: requirement.id.clone(),
                    }),
            );
            let Some(result) = run.current_pack_result(pack) else {
                qualified = false;
                continue;
            };
            let revision = service
                .repository
                .investigation_run_revision(auth, run.id, result.run_revision_id)
                .await?;
            if revision.pack_result().map_err(contract_failure)? != *result {
                return Err(contract_failure(InvestigationRunFailure::ResultConflict));
            }
            qualified &= conclusion_evidence_available(&revision, &current);
            for requirement in current.iter().filter(|requirement| {
                requirement.quality != DiagnosticEvidenceQuality::Available
                    && requirement.evidence_ids.iter().any(|id| {
                        revision.findings.iter().any(|finding| {
                            finding.supporting_evidence_ids.contains(id) || finding.counter_evidence_ids.contains(id)
                        })
                    })
            }) {
                let reference = InvestigationRequirementRef {
                    pack: pack.clone(),
                    requirement_id: requirement.id.clone(),
                };
                if !missing.contains(&reference) {
                    missing.push(reference);
                }
            }
            saved.push(revision);
        }
        let reason = stop_reason.unwrap_or(InvestigationStopReason::EvidenceInsufficient);
        if saved.is_empty() {
            service
                .repository
                .investigation_run_stop_planned(&worker, run.id, run.revision, reason)
                .await?;
            return Ok(());
        }
        let faults = saved
            .iter()
            .filter(|revision| revision.status == InvestigationDiagnosisStatus::Fault)
            .collect::<Vec<_>>();
        let basis = if faults.len() == 1 { faults[0] } else { &saved[0] };
        let mut considered = saved
            .iter()
            .flat_map(|revision| revision.evidence_ids.iter())
            .filter(|id| run.evidence_ids.contains(id))
            .copied()
            .collect::<Vec<_>>();
        considered.sort();
        considered.dedup();
        let mut conclusion = HypothesisAssessmentConclusion::Inconclusive {
            reason,
            considered_evidence_ids: considered.clone(),
            missing_requirements: missing.clone(),
        };
        if qualified && saved.len() == run.effective_packs.len() {
            if faults.len() == 1 {
                let findings = basis
                    .findings
                    .iter()
                    .filter(|finding| finding.outcome == InvestigationDiagnosisStatus::Fault)
                    .collect::<Vec<_>>();
                if findings.len() == 1 && !findings[0].supporting_evidence_ids.is_empty() {
                    conclusion = HypothesisAssessmentConclusion::RootCause {
                        statement: findings[0].root_cause.clone(),
                        supporting_evidence_ids: findings[0].supporting_evidence_ids.clone(),
                        counter_evidence_ids: findings[0].counter_evidence_ids.clone(),
                    };
                }
            } else if faults.is_empty()
                && saved
                    .iter()
                    .all(|revision| revision.status == InvestigationDiagnosisStatus::Healthy)
            {
                let mut supporting = basis
                    .findings
                    .iter()
                    .flat_map(|finding| finding.supporting_evidence_ids.iter().copied())
                    .collect::<Vec<_>>();
                supporting.sort();
                supporting.dedup();
                if !supporting.is_empty() {
                    conclusion = HypothesisAssessmentConclusion::Healthy {
                        supporting_evidence_ids: supporting,
                    };
                }
            }
        }
        let mut assessment = HypothesisAssessment {
            schema_version: INVESTIGATION_RUN_SCHEMA_VERSION.to_owned(),
            id: HypothesisAssessmentId::new(),
            run_id: run.id,
            scope: run.effective_scope.clone(),
            control_epoch: run.control_epoch,
            diagnosis_revision_id: None,
            run_revision_id: Some(basis.id),
            conclusion,
            summary:
                "Read-only advisory assessment of saved deterministic pack results; no execution authority is granted."
                    .to_owned(),
            created_at: Utc::now(),
        };
        if run.validate_planned_conclusion(&assessment, basis).is_err() {
            assessment.conclusion = HypothesisAssessmentConclusion::Inconclusive {
                reason,
                considered_evidence_ids: considered,
                missing_requirements: missing,
            };
        }
        service
            .repository
            .investigation_run_finish_planned(&worker, run.id, run.revision, &assessment)
            .await?;
        Ok(())
    }
}

fn conclusion_evidence_available(
    revision: &SavedInvestigationRunRevision,
    current: &[DiagnosticRequirementQuality],
) -> bool {
    let required = current
        .iter()
        .filter(|requirement| requirement.required)
        .collect::<Vec<_>>();
    let available = current
        .iter()
        .filter(|requirement| requirement.quality == DiagnosticEvidenceQuality::Available)
        .flat_map(|requirement| requirement.evidence_ids.iter())
        .collect::<std::collections::BTreeSet<_>>();
    !required.is_empty()
        && required.iter().all(|requirement| {
            requirement.quality == DiagnosticEvidenceQuality::Available
                && requirement
                    .evidence_ids
                    .iter()
                    .all(|id| revision.evidence_ids.contains(id))
        })
        && revision
            .findings
            .iter()
            .flat_map(|finding| {
                finding
                    .supporting_evidence_ids
                    .iter()
                    .chain(&finding.counter_evidence_ids)
            })
            .all(|id| available.contains(id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_expired_optional_rule_support_cannot_survive_the_final_freshness_check() {
        let at = chrono::DateTime::parse_from_rfc3339("2026-11-12T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let scope = InvestigationRunScope {
            tenant_id: TenantId::new(),
            cluster_id: ClusterId::new(),
            investigation_id: InvestigationId::new(),
            resource: Some(InvestigationResource {
                kind: ResourceKind::Broker,
                key: "broker-a".to_owned(),
            }),
            time_range: TimeRange::new(at - chrono::Duration::minutes(10), at).unwrap(),
        };
        let pack = InvestigationPackRef {
            pack_id: "broker-health.v1".to_owned(),
            pack_version: "1.0.0".to_owned(),
        };
        let snapshot = |source: &str, resource: &str, observed_at, content| {
            EvidenceSnapshot::capture(
                EvidenceQuery {
                    query_id: QueryId::new(),
                    correlation_id: CorrelationId::new(),
                    tenant_id: scope.tenant_id,
                    cluster_id: scope.cluster_id,
                    source: source.to_owned(),
                    resource: resource.to_owned(),
                    time_range: scope.time_range.clone(),
                },
                current_evidence_schema(),
                observed_at,
                EvidenceContent::Inline(content),
            )
            .unwrap()
        };
        let metrics = snapshot(
            "prometheus",
            "broker-health/broker-a",
            at,
            serde_json::json!({"ready":true,"request_error_rate_percent":0,"active_connections":10,"cpu_percent":10,"rss_bytes":1024,"disk_used_percent":10,"flush_latency_ms":1,"dispatch_backlog":0,"ha_lag_bytes":0}),
        );
        let runtime = snapshot(
            "rocketmq-mcp",
            "broker-runtime/broker-a",
            at - chrono::Duration::seconds(299),
            serde_json::json!({"broker_up":false}),
        );
        let evidence = vec![metrics.clone(), runtime.clone()];
        let reference = |id: &str| InvestigationRequirementRef {
            pack: pack.clone(),
            requirement_id: id.to_owned(),
        };
        let context = InvestigationRunContext {
            actor_subject: "synthetic-test".to_owned(),
            permission: InvestigationRunPermission::Control,
            authorized_scope: scope.clone(),
            allowed_packs: vec![pack.clone()],
            allowed_requirements: vec![
                reference("broker-metrics"),
                reference("broker-runtime"),
                reference("connections"),
            ],
            visible_evidence: evidence
                .iter()
                .map(|e| InvestigationEvidenceContext {
                    evidence_id: e.evidence_id,
                    scope: scope.clone(),
                })
                .collect(),
        };
        let request = CreateInvestigationRunRequest {
            schema_version: INVESTIGATION_RUN_SCHEMA_VERSION.to_owned(),
            run_id: InvestigationRunId::new(),
            scope: scope.clone(),
            packs: vec![pack.clone()],
            budget: InvestigationBudget {
                limit: InvestigationBudgetCost {
                    read_queries: 12,
                    model_calls: 8,
                    evidence_items: 16,
                    output_bytes: 4194304,
                },
                max_steps: 32,
                max_in_flight: 2,
                max_attempts_per_step: 3,
                max_query_retries: 2,
            },
            deadline: at + chrono::Duration::minutes(10),
            idempotency_key: "synthetic-freshness".to_owned(),
        };
        let mut run = InvestigationRun::create(&context, &request, None, at).unwrap().run;
        run.start(&context, run.revision, at).unwrap();
        run.apply_command(
            &context,
            &InvestigationCommandRequest {
                schema_version: INVESTIGATION_RUN_SCHEMA_VERSION.to_owned(),
                command_id: InvestigationCommandId::new(),
                run_id: run.id,
                expected_revision: run.revision,
                idempotency_key: "import-saved-fixture".to_owned(),
                reason: "synthetic saved context".to_owned(),
                command: InvestigationCommand::Steer {
                    refinement: InvestigationRefinement {
                        scope: scope.clone(),
                        packs: vec![pack.clone()],
                        evidence_ids: evidence.iter().map(|e| e.evidence_id).collect(),
                    },
                },
            },
            None,
            at,
        )
        .unwrap();
        let bindings = vec![
            InvestigationReadBinding {
                requirement: reference("broker-metrics"),
                source: "prometheus".to_owned(),
                resource: metrics.resource.clone(),
                scope: scope.clone(),
            },
            InvestigationReadBinding {
                requirement: reference("broker-runtime"),
                source: "rocketmq-mcp".to_owned(),
                resource: runtime.resource.clone(),
                scope: scope.clone(),
            },
        ];
        let registry = full_registry().unwrap();
        let input = |as_of: chrono::DateTime<Utc>| InvestigationPlanningInput {
            run: &run,
            bindings: &bindings,
            evidence: &evidence,
            as_of_unix_ms: as_of.timestamp_millis(),
            explanation: InvestigationExplanationPolicy::RuleOnly,
            reservations: InvestigationReservationPolicy {
                collection_output_bytes: 524288,
                evaluation_output_bytes: 65536,
                explanation_output_bytes: 65536,
            },
        };
        let before = investigation_requirements(&registry, &pack, &input(at)).unwrap();
        let engine = DiagnosticEngine::new(full_registry().unwrap());
        let report = engine
            .evaluate_version("broker-health", PackVersion::new(1, 0, 0), &evidence)
            .unwrap();
        assert_eq!(report.status, DiagnosticStatus::Fault);
        let mut saved = SavedInvestigationRunRevision {
            schema_version: INVESTIGATION_RUN_REVISION_SCHEMA_VERSION.to_owned(),
            id: InvestigationRunRevisionId::new(),
            run_id: run.id,
            scope: scope.clone(),
            control_epoch: run.control_epoch,
            pack: pack.clone(),
            step_id: InvestigationStepId::new(),
            attempt_id: StepAttemptId::new(),
            status: InvestigationDiagnosisStatus::Fault,
            partial: false,
            evidence_ids: evidence.iter().map(|e| e.evidence_id).collect(),
            requirements: before.clone(),
            findings: investigation_findings(&report),
            input_fingerprint: canonical_sha256(&scope).unwrap(),
            created_at: at,
        };
        assert!(conclusion_evidence_available(&saved, &before));
        let after = investigation_requirements(&registry, &pack, &input(at + chrono::Duration::seconds(2))).unwrap();
        assert_eq!(
            after.iter().find(|q| q.id == "broker-metrics").unwrap().quality,
            DiagnosticEvidenceQuality::Available
        );
        assert_eq!(
            after.iter().find(|q| q.id == "broker-runtime").unwrap().quality,
            DiagnosticEvidenceQuality::Stale
        );
        assert!(!conclusion_evidence_available(&saved, &after));
        // An unrelated missing optional does not block a healthy rule supported
        // entirely by the still-qualified required metric snapshot.
        let healthy = engine
            .evaluate_version("broker-health", PackVersion::new(1, 0, 0), &[metrics])
            .unwrap();
        assert_eq!(healthy.status, DiagnosticStatus::Healthy);
        saved.findings = investigation_findings(&healthy);
        saved.status = InvestigationDiagnosisStatus::Healthy;
        assert!(conclusion_evidence_available(&saved, &after));
    }
}
