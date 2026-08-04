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
use std::time::Duration;

use chrono::Utc;
use reqwest::RequestBuilder;
use reqwest::Response;
use reqwest::StatusCode;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::EvidenceSnapshot;
use serde_json::Value;
use serde_json::json;
use sqlx::Row;
use sqlx::postgres::PgPoolOptions;

use super::fixture::MaterializedPackScenario;
use super::fixture::generated_manifest;
use super::fixture::materialize_pack_scenario;
use super::fixture::validate_safe_value;
use super::model::DiagnosticQualificationError;
use super::model::DiagnosticQualificationReport;
use super::model::LiveQualificationConfig;
use super::model::QUALIFICATION_PACK_COUNT;
use super::model::QUALIFICATION_REPORT_SCHEMA;
use super::model::QUALIFICATION_SCENARIO_COUNT;
use super::model::QualificationExpectation;
use super::model::QualificationScenario;
use super::model::QualifiedPackScenarioResult;

const QUALIFICATION_SUBJECT: &str = "rocketmq-sre-diagnostic-qualification";
const MAX_PERSISTED_EVIDENCE_BYTES: usize = 64 * 1024;
const MAX_CITED_EVIDENCE: usize = 200;

struct LiveClient<'a> {
    http: reqwest::Client,
    config: &'a LiveQualificationConfig,
    cluster_scope: String,
}

struct PersistedPackRun {
    pack_id: String,
    output: Value,
    partial: bool,
}

struct QualificationJob {
    pack_id: String,
    inspection_template: String,
    scenario: QualificationScenario,
    cluster_id: ClusterId,
}

fn qualification_jobs() -> Result<Vec<QualificationJob>, DiagnosticQualificationError> {
    let manifest = generated_manifest()?;
    let mut jobs = Vec::with_capacity(QUALIFICATION_PACK_COUNT * QUALIFICATION_SCENARIO_COUNT);
    for pack in manifest.packs {
        for expectation in pack.scenarios {
            jobs.push(QualificationJob {
                pack_id: pack.id.clone(),
                inspection_template: pack.inspection_template.clone(),
                scenario: expectation.scenario,
                cluster_id: ClusterId::new(),
            });
        }
    }
    if jobs.len() != QUALIFICATION_PACK_COUNT * QUALIFICATION_SCENARIO_COUNT {
        return Err(DiagnosticQualificationError::InvalidManifest(
            "qualification job cardinality must be 32 packs by 3 isolated scenarios".to_owned(),
        ));
    }
    Ok(jobs)
}

/// Exercises all built-in packs through the running Control Plane and verifies
/// their persisted PostgreSQL results without enabling a model provider.
pub async fn run_live_qualification(
    config: &LiveQualificationConfig,
) -> Result<DiagnosticQualificationReport, DiagnosticQualificationError> {
    validate_config(config)?;
    let started_at = Utc::now();
    let jobs = qualification_jobs()?;
    let client = LiveClient {
        http: reqwest::Client::builder().timeout(Duration::from_secs(60)).build()?,
        config,
        cluster_scope: jobs
            .iter()
            .map(|job| job.cluster_id.to_string())
            .collect::<Vec<_>>()
            .join(","),
    };
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&config.database_url)
        .await?;
    let mut results = Vec::with_capacity(QUALIFICATION_PACK_COUNT * QUALIFICATION_SCENARIO_COUNT);
    let mut first_evidence = None;
    let mut schema_drift_rejected = false;

    for job in &jobs {
        client.onboard(&job.pack_id, job.scenario, job.cluster_id).await?;
        let incident_id = client
            .create_evidence_container(&job.pack_id, job.scenario, job.cluster_id)
            .await?;
        let materialized =
            materialize_pack_scenario(&job.pack_id, job.scenario, config.tenant_id, job.cluster_id, Utc::now())?;
        if !schema_drift_rejected && let Some(snapshot) = materialized.evidence.first() {
            schema_drift_rejected = client
                .assert_schema_drift_rejected(incident_id, snapshot.clone())
                .await?;
        }
        for snapshot in &materialized.evidence {
            let persisted = client.persist_evidence(incident_id, snapshot).await?;
            first_evidence.get_or_insert((persisted.evidence_id, job.cluster_id));
        }
        client.run_inspection(job.cluster_id, &job.inspection_template).await?;
        let pack_runs = load_pack_runs(&pool, config.tenant_id, job.cluster_id).await?;
        results.push(
            validate_pack_run_result(
                &client,
                &job.pack_id,
                job.scenario,
                job.cluster_id,
                &materialized,
                pack_runs,
            )
            .await?,
        );
    }

    let (evidence_id, evidence_cluster) = first_evidence.ok_or_else(|| {
        DiagnosticQualificationError::Assertion("normal and fault scenarios persisted no Evidence".to_owned())
    })?;
    let alternate_cluster = jobs
        .iter()
        .map(|job| job.cluster_id)
        .find(|cluster_id| *cluster_id != evidence_cluster)
        .ok_or_else(|| {
            DiagnosticQualificationError::Assertion("cross-cluster test requires two clusters".to_owned())
        })?;
    let cross_cluster_access_rejected = client
        .assert_cross_cluster_rejected(evidence_id, alternate_cluster)
        .await?;
    let model_provider_network_calls = count_for_tenant(&pool, "model_invocations", config.tenant_id).await?;
    let execution_records = count_for_tenant(&pool, "executions", config.tenant_id).await?;
    pool.close().await;

    results.sort_by(|left, right| {
        left.scenario
            .cmp(&right.scenario)
            .then_with(|| left.pack_id.cmp(&right.pack_id))
    });
    if results.len() != QUALIFICATION_PACK_COUNT * QUALIFICATION_SCENARIO_COUNT
        || model_provider_network_calls != 0
        || execution_records != 0
        || !cross_cluster_access_rejected
        || !schema_drift_rejected
    {
        return Err(DiagnosticQualificationError::Assertion(
            "global mutation-zero, model-zero, scope, schema, or cardinality invariant failed".to_owned(),
        ));
    }

    Ok(DiagnosticQualificationReport {
        schema_version: QUALIFICATION_REPORT_SCHEMA.to_owned(),
        revision: config.revision.clone(),
        environment: config.environment.clone(),
        database: "PostgreSQL 17 (Docker)".to_owned(),
        started_at,
        finished_at: Utc::now(),
        status: "passed".to_owned(),
        operating_mode: "rules_only".to_owned(),
        pack_count: QUALIFICATION_PACK_COUNT,
        scenario_count: QUALIFICATION_SCENARIO_COUNT,
        pack_scenario_count: results.len(),
        model_provider_network_calls,
        target_mutation_calls: 0,
        execution_records,
        cross_cluster_access_rejected,
        schema_drift_rejected,
        results,
    })
}

impl LiveClient<'_> {
    async fn onboard(
        &self,
        pack_id: &str,
        scenario: QualificationScenario,
        cluster_id: ClusterId,
    ) -> Result<(), DiagnosticQualificationError> {
        let external_pack_id = pack_id.replace('.', "-");
        let request = json!({
            "cluster_id": cluster_id,
            "tenant_id": self.config.tenant_id,
            "external_cluster_key": format!(
                "diagnostic-qualification-{external_pack_id}-{}",
                scenario.as_str()
            ),
            "environment": "qualification",
            "region": "docker-local",
            "rocketmq_version": "read-only-fixture",
            "deployment_mode": "qualification",
            "owner": "rocketmq-sre",
            "actor_subject": QUALIFICATION_SUBJECT,
        });
        let response = self
            .authorized(self.http.post(self.public_endpoint("/v1/clusters/onboard")))
            .json(&request)
            .send()
            .await?;
        success_json(response, "cluster onboarding").await?;
        Ok(())
    }

    async fn create_evidence_container(
        &self,
        pack_id: &str,
        scenario: QualificationScenario,
        cluster_id: ClusterId,
    ) -> Result<rocketmq_sre_contracts::IncidentId, DiagnosticQualificationError> {
        let response = self
            .authorized(self.http.post(self.public_endpoint("/v1/incidents")))
            .json(&json!({
                "cluster_id": cluster_id,
                "title": format!("Diagnostic qualification {pack_id} {} evidence", scenario.as_str()),
                "resource": format!("qualification/{pack_id}/{}", scenario.as_str()),
                "symptom_family": "diagnostic-qualification",
            }))
            .send()
            .await?;
        let value = success_json(response, "incident creation").await?;
        serde_json::from_value(
            value.pointer("/incident/id").cloned().ok_or_else(|| {
                DiagnosticQualificationError::Assertion("incident response omitted its ID".to_owned())
            })?,
        )
        .map_err(DiagnosticQualificationError::from)
    }

    async fn persist_evidence(
        &self,
        incident_id: rocketmq_sre_contracts::IncidentId,
        snapshot: &EvidenceSnapshot,
    ) -> Result<EvidenceSnapshot, DiagnosticQualificationError> {
        let response = self
            .authorized(self.http.post(self.connector_endpoint("/internal/v1/evidence")))
            .json(&json!({
                "investigation_id": null,
                "incident_id": incident_id,
                "evidence": snapshot,
            }))
            .send()
            .await?;
        let value = success_json(response, "Evidence persistence").await?;
        let persisted: EvidenceSnapshot = serde_json::from_value(value)?;
        persisted.verify_content_hash().map_err(|error| {
            DiagnosticQualificationError::Assertion(format!("persisted Evidence hash failed: {error}"))
        })?;
        Ok(persisted)
    }

    async fn run_inspection(&self, cluster_id: ClusterId, template: &str) -> Result<(), DiagnosticQualificationError> {
        let response = self
            .authorized(self.http.post(self.public_endpoint("/v1/inspections")))
            .json(&json!({
                "cluster_id": cluster_id,
                "template": template,
                "schedule": null,
            }))
            .send()
            .await?;
        let value = success_json(response, "inspection execution").await?;
        let status = value.pointer("/run/status").and_then(Value::as_str).unwrap_or_default();
        if !matches!(status, "completed" | "needs_evidence") {
            return Err(DiagnosticQualificationError::Assertion(format!(
                "inspection `{template}` ended in unexpected status `{status}`"
            )));
        }
        Ok(())
    }

    async fn assert_schema_drift_rejected(
        &self,
        incident_id: rocketmq_sre_contracts::IncidentId,
        mut snapshot: EvidenceSnapshot,
    ) -> Result<bool, DiagnosticQualificationError> {
        snapshot.schema.major = snapshot.schema.major.saturating_add(99);
        snapshot.content_hash = snapshot
            .compute_content_hash()
            .map_err(|error| DiagnosticQualificationError::Assertion(error.to_string()))?;
        let response = self
            .authorized(self.http.post(self.connector_endpoint("/internal/v1/evidence")))
            .json(&json!({
                "investigation_id": null,
                "incident_id": incident_id,
                "evidence": snapshot,
            }))
            .send()
            .await?;
        error_code_is(response, StatusCode::BAD_REQUEST, "unsupported_schema_major").await
    }

    async fn assert_cross_cluster_rejected(
        &self,
        evidence_id: EvidenceId,
        authorized_cluster: ClusterId,
    ) -> Result<bool, DiagnosticQualificationError> {
        let response = self
            .authorized_for_clusters(
                self.http
                    .get(self.public_endpoint(&format!("/v1/evidence/{evidence_id}"))),
                &authorized_cluster.to_string(),
            )
            .send()
            .await?;
        error_code_is(response, StatusCode::FORBIDDEN, "cluster_not_allowed").await
    }

    async fn read_evidence(
        &self,
        evidence_id: EvidenceId,
        cluster_id: ClusterId,
    ) -> Result<(), DiagnosticQualificationError> {
        let response = self
            .authorized(
                self.http
                    .get(self.public_endpoint(&format!("/v1/evidence/{evidence_id}"))),
            )
            .send()
            .await?;
        let value = success_json(response, "cited Evidence lookup").await?;
        if serde_json::to_vec(&value)?.len() > MAX_PERSISTED_EVIDENCE_BYTES {
            return Err(DiagnosticQualificationError::Assertion(format!(
                "cited Evidence `{evidence_id}` exceeded the response bound"
            )));
        }
        validate_safe_value(&value).map_err(|field| {
            DiagnosticQualificationError::Assertion(format!(
                "cited Evidence `{evidence_id}` exposed forbidden field or value `{field}`"
            ))
        })?;
        let snapshot: EvidenceSnapshot = serde_json::from_value(value)?;
        if snapshot.evidence_id != evidence_id
            || snapshot.tenant_id != self.config.tenant_id
            || snapshot.cluster_id != cluster_id
        {
            return Err(DiagnosticQualificationError::Assertion(format!(
                "cited Evidence `{evidence_id}` crossed its persisted scope"
            )));
        }
        snapshot.verify_content_hash().map_err(|error| {
            DiagnosticQualificationError::Assertion(format!("cited Evidence `{evidence_id}` hash failed: {error}"))
        })
    }

    fn authorized(&self, request: RequestBuilder) -> RequestBuilder {
        self.authorized_for_clusters(request, &self.cluster_scope)
    }

    fn authorized_for_clusters(&self, request: RequestBuilder, clusters: &str) -> RequestBuilder {
        request
            .bearer_auth(&self.config.token)
            .header("x-rocketmq-tenant", self.config.tenant_id.to_string())
            .header("x-rocketmq-clusters", clusters)
            .header("x-rocketmq-subject", QUALIFICATION_SUBJECT)
    }

    fn public_endpoint(&self, path: &str) -> String {
        format!("{}{}", self.config.public_url.trim_end_matches('/'), path)
    }

    fn connector_endpoint(&self, path: &str) -> String {
        format!("{}{}", self.config.connector_url.trim_end_matches('/'), path)
    }
}

async fn load_pack_runs(
    pool: &sqlx::PgPool,
    tenant_id: rocketmq_sre_contracts::TenantId,
    cluster_id: ClusterId,
) -> Result<Vec<PersistedPackRun>, DiagnosticQualificationError> {
    let rows = sqlx::query(
        "SELECT pack_id, output, partial
         FROM diagnostic_pack_runs
         WHERE tenant_id = $1 AND cluster_id = $2 AND inspection_run_id IS NOT NULL
         ORDER BY completed_at, id",
    )
    .bind(tenant_id.as_uuid())
    .bind(cluster_id.as_uuid())
    .fetch_all(pool)
    .await?;
    rows.iter()
        .map(|row| {
            Ok(PersistedPackRun {
                pack_id: row.try_get("pack_id")?,
                output: row.try_get("output")?,
                partial: row.try_get("partial")?,
            })
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()
        .map_err(DiagnosticQualificationError::from)
}

async fn validate_pack_run_result(
    client: &LiveClient<'_>,
    pack_id: &str,
    scenario: QualificationScenario,
    cluster_id: ClusterId,
    materialized: &MaterializedPackScenario,
    pack_runs: Vec<PersistedPackRun>,
) -> Result<QualifiedPackScenarioResult, DiagnosticQualificationError> {
    let matching = pack_runs
        .into_iter()
        .filter(|run| run.pack_id == pack_id)
        .collect::<Vec<_>>();
    if matching.len() != 1 {
        return Err(DiagnosticQualificationError::Assertion(format!(
            "pack `{pack_id}` scenario `{}` persisted {} target results instead of one",
            scenario.as_str(),
            matching.len()
        )));
    }
    let run = &matching[0];
    validate_pack_run(pack_id, &materialized.expected, run)?;
    let cited = cited_evidence_ids(&run.output)?;
    if cited.len() > MAX_CITED_EVIDENCE {
        return Err(DiagnosticQualificationError::Assertion(format!(
            "pack `{pack_id}` exceeded the {MAX_CITED_EVIDENCE}-citation bound"
        )));
    }
    for evidence_id in &cited {
        client.read_evidence(*evidence_id, cluster_id).await?;
    }
    Ok(QualifiedPackScenarioResult {
        pack_id: pack_id.to_owned(),
        scenario,
        status: materialized.expected.expected_status.clone(),
        reason_codes: materialized.expected.expected_reason_codes.clone(),
        cited_evidence_count: cited.len(),
        persisted_run_count: matching.len(),
        partial: materialized.expected.partial,
        execution_eligible: false,
    })
}

fn validate_pack_run(
    pack_id: &str,
    expectation: &QualificationExpectation,
    run: &PersistedPackRun,
) -> Result<(String, Vec<String>, bool), DiagnosticQualificationError> {
    let status = run.output.get("status").and_then(Value::as_str).unwrap_or_default();
    let execution_eligible = run
        .output
        .get("execution_eligible")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let mut reason_codes = run
        .output
        .get("findings")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|finding| finding.get("reason_code").and_then(Value::as_str))
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    reason_codes.sort();
    if status != expectation.expected_status
        || reason_codes != expectation.expected_reason_codes
        || run.partial != expectation.partial
        || execution_eligible != expectation.execution_eligible
    {
        return Err(DiagnosticQualificationError::Assertion(format!(
            "pack `{pack_id}` result drifted: status `{status}` vs `{}`, reasons {:?} vs {:?}, partial `{}` vs `{}`, \
             execution_eligible `{execution_eligible}` vs `{}`",
            expectation.expected_status,
            reason_codes,
            expectation.expected_reason_codes,
            run.partial,
            expectation.partial,
            expectation.execution_eligible
        )));
    }
    Ok((status.to_owned(), reason_codes, execution_eligible))
}

fn cited_evidence_ids(output: &Value) -> Result<BTreeSet<EvidenceId>, DiagnosticQualificationError> {
    let mut ids = BTreeSet::new();
    for finding in output.get("findings").and_then(Value::as_array).into_iter().flatten() {
        for field in ["supporting_evidence", "counter_evidence"] {
            for citation in finding.get(field).and_then(Value::as_array).into_iter().flatten() {
                let id = citation.get("evidence_id").cloned().ok_or_else(|| {
                    DiagnosticQualificationError::Assertion("diagnostic citation omitted evidence_id".to_owned())
                })?;
                ids.insert(serde_json::from_value(id)?);
            }
        }
    }
    Ok(ids)
}

async fn count_for_tenant(
    pool: &sqlx::PgPool,
    table: &str,
    tenant_id: rocketmq_sre_contracts::TenantId,
) -> Result<u64, DiagnosticQualificationError> {
    let query = match table {
        "model_invocations" => "SELECT COUNT(*) AS count FROM model_invocations WHERE tenant_id = $1",
        "executions" => "SELECT COUNT(*) AS count FROM executions WHERE tenant_id = $1",
        _ => {
            return Err(DiagnosticQualificationError::Assertion(
                "qualification attempted an unbounded database query".to_owned(),
            ));
        }
    };
    let count: i64 = sqlx::query(query)
        .bind(tenant_id.as_uuid())
        .fetch_one(pool)
        .await?
        .try_get("count")?;
    u64::try_from(count).map_err(|_| {
        DiagnosticQualificationError::Assertion(format!("table `{table}` returned a negative record count"))
    })
}

async fn success_json(response: Response, operation: &str) -> Result<Value, DiagnosticQualificationError> {
    let status = response.status();
    let value = response.json::<Value>().await?;
    if !status.is_success() {
        let code = value.get("code").and_then(Value::as_str).unwrap_or("unknown_error");
        return Err(DiagnosticQualificationError::Assertion(format!(
            "{operation} failed with HTTP {status} and code `{code}`"
        )));
    }
    Ok(value)
}

async fn error_code_is(
    response: Response,
    expected_status: StatusCode,
    expected_code: &str,
) -> Result<bool, DiagnosticQualificationError> {
    let status = response.status();
    let value = response.json::<Value>().await?;
    let code = value.get("code").and_then(Value::as_str);
    Ok(status == expected_status && code == Some(expected_code))
}

fn validate_config(config: &LiveQualificationConfig) -> Result<(), DiagnosticQualificationError> {
    if config.public_url.trim().is_empty()
        || config.connector_url.trim().is_empty()
        || config.database_url.trim().is_empty()
        || config.token.trim().is_empty()
        || config.revision.trim().is_empty()
        || config.environment.trim().is_empty()
    {
        return Err(DiagnosticQualificationError::InvalidManifest(
            "live qualification configuration contains an empty required value".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn citation_without_evidence_id_fails_closed() {
        let output = json!({
            "findings": [{
                "supporting_evidence": [{}],
                "counter_evidence": [],
            }],
        });

        assert!(matches!(
            cited_evidence_ids(&output),
            Err(DiagnosticQualificationError::Assertion(message))
                if message == "diagnostic citation omitted evidence_id"
        ));
    }
}
