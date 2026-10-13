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

//! The health diagnosis of one Broker: every instance that carries its name.
//!
//! A Broker name stands for a master and its replicas. The diagnosis asks whether they answer,
//! whether they are ready, whether their stores accept writes, whether the replicas keep up, and
//! whether they are configured alike. A rule draws one conclusion from the evidence it names and
//! is silent without it.

use serde_json::json;

use crate::model::diagnosis::ConfidenceBand;
use crate::model::diagnosis::DiagnosisReport;
use crate::model::diagnosis::EvidenceKind;
use crate::model::diagnosis::EvidenceSnapshot;
use crate::model::diagnosis::MetricWatchItem;
use crate::model::diagnosis::Recommendation;
use crate::model::diagnosis::Severity;
use crate::service::diagnosis_rules::conclusion;
use crate::service::diagnosis_rules::Finding;
use crate::service::diagnosis_service;
use crate::service::diagnosis_service::HealthFinding;
use crate::service::diagnosis_service::HealthReport;
use crate::service::diagnosis_snapshot;
use crate::tools::broker_tools::BackgroundIndexRebuildState;
use crate::tools::broker_tools::BrokerDiagnosticsOutput;
use crate::tools::broker_tools::BrokerDiagnosticsRow;
use crate::tools::broker_tools::DescribeBrokerOutput;
use crate::tools::config_tools::BrokerConfigSummaryOutput;
use crate::tools::diagnosis_tools::DiagnoseBrokerHealthArgs;
use crate::tools::executor::NotFoundEntity;
use crate::tools::executor::ToolFailure;
use crate::tools::infrastructure_tools::GetHaStatusOutput;

pub(crate) const BROKER_HEALTH_EVIDENCE_VERSION: &str = "rocketmq-mcp.evidence.broker-health.v1";
pub(crate) const BROKER_HEALTH_RULES_VERSION: &str = "rocketmq-mcp.rules.broker-health.v1";

/// Identifiers of the evidence items of a Broker health diagnosis.
pub(crate) mod evidence_id {
    pub(crate) const BROKER_SUMMARY: &str = "broker_summary";
    pub(crate) const BROKER_DIAGNOSTICS: &str = "broker_diagnostics";
    pub(crate) const HA_STATUS: &str = "ha_status";
    pub(crate) const BROKER_CONFIG: &str = "broker_config";
}

use evidence_id::BROKER_CONFIG;
use evidence_id::BROKER_DIAGNOSTICS;
use evidence_id::BROKER_SUMMARY;
use evidence_id::HA_STATUS;

/// The id a master registers under.
const MASTER_ID: u64 = 0;

/// Commit log bytes that are not yet dispatched to the consume queues from which a store counts
/// as behind. Consumers cannot see a message before it is dispatched.
const DISPATCH_BEHIND_BYTES_LIMIT: i64 = 256 * 1024 * 1024;

/// How long a write may wait for its replicas before it counts as stalled: the time after which a
/// synchronous master gives up on a replica by default.
const SYNC_REPLICATION_WAIT_LIMIT_MILLIS: u64 = 5_000;

/// What each evidence item other than the Broker rows adds to the confidence of a report, in
/// thousandths.
const CONFIDENCE_WEIGHTS: &[(&str, u16)] = &[(BROKER_DIAGNOSTICS, 250), (HA_STATUS, 100), (BROKER_CONFIG, 100)];

/// The evidence one Broker health diagnosis is evaluated on.
///
/// `summary` says which instances exist and whether they answer. Without it the health is unknown.
/// A part that could not be read is an error here and missing evidence in the report.
pub(crate) struct BrokerHealthEvidence {
    pub summary: Result<DescribeBrokerOutput, ToolFailure>,
    pub diagnostics: Result<BrokerDiagnosticsOutput, ToolFailure>,
    pub ha: Result<GetHaStatusOutput, ToolFailure>,
    pub config: Result<BrokerConfigSummaryOutput, ToolFailure>,
}

impl BrokerHealthEvidence {
    fn snapshot(&self, args: &DiagnoseBrokerHealthArgs) -> EvidenceSnapshot {
        let query_hash = diagnosis_snapshot::query_hash(&[&args.cluster, &args.broker_name]);
        let items = vec![
            diagnosis_snapshot::item(
                BROKER_SUMMARY,
                "rocketmq_describe_broker",
                EvidenceKind::BrokerSummary,
                &query_hash,
                &self.summary,
            ),
            diagnosis_snapshot::item(
                BROKER_DIAGNOSTICS,
                "rocketmq_get_broker_diagnostics",
                EvidenceKind::BrokerDiagnostics,
                &query_hash,
                &self.diagnostics,
            ),
            diagnosis_snapshot::item(
                HA_STATUS,
                "rocketmq_get_ha_status",
                EvidenceKind::HaStatus,
                &query_hash,
                &self.ha,
            ),
            diagnosis_snapshot::item(
                BROKER_CONFIG,
                "rocketmq_get_broker_config_summary",
                EvidenceKind::BrokerConfig,
                &query_hash,
                &self.config,
            ),
        ];
        diagnosis_snapshot::snapshot(
            BROKER_HEALTH_EVIDENCE_VERSION,
            query_hash,
            &args.cluster,
            json!({ "broker_name": args.broker_name }),
            items,
        )
    }
}

/// What the rules read. `summary` is always there: no rule runs without it.
struct RuleInput<'a> {
    broker_name: &'a str,
    summary: &'a DescribeBrokerOutput,
    diagnostics: Option<&'a BrokerDiagnosticsOutput>,
    ha: Option<&'a GetHaStatusOutput>,
    config: Option<&'a BrokerConfigSummaryOutput>,
}

impl RuleInput<'_> {
    /// The diagnostics rows of the instances that `matches`.
    fn diagnosed(&self, matches: impl Fn(&BrokerDiagnosticsRow) -> bool) -> Vec<&BrokerDiagnosticsRow> {
        self.diagnostics
            .map(|diagnostics| diagnostics.brokers.iter().filter(|row| matches(row)).collect())
            .unwrap_or_default()
    }

    fn replica_count(&self) -> usize {
        self.summary
            .brokers
            .iter()
            .filter(|row| row.broker_id != MASTER_ID)
            .count()
    }
}

type Rule = fn(&RuleInput<'_>) -> Option<HealthFinding>;

/// The rule table. The identifier of a rule is the `rule_id` of the finding it returns; it is part
/// of the report contract and never changes once released.
const RULES: &[Rule] = &[
    master_missing,
    broker_instance_inactive,
    broker_not_ready,
    store_not_writeable,
    store_under_pressure,
    dispatch_behind,
    sync_replication_stalled,
    no_in_sync_replica,
    replica_not_in_sync,
    recovery_failed,
    index_rebuild_failed,
    config_inconsistent,
];

/// Evaluates the rules on `evidence` and writes the report.
pub(crate) fn evaluate(
    args: &DiagnoseBrokerHealthArgs,
    evidence: BrokerHealthEvidence,
    policy_profile: &str,
) -> DiagnosisReport {
    let snapshot = evidence.snapshot(args);
    let findings = match evidence.summary.as_ref() {
        Ok(summary) => {
            let input = RuleInput {
                broker_name: &args.broker_name,
                summary,
                diagnostics: evidence.diagnostics.as_ref().ok(),
                ha: evidence.ha.as_ref().ok(),
                config: evidence.config.as_ref().ok(),
            };
            RULES.iter().filter_map(|rule| rule(&input)).collect()
        }
        Err(_) => Vec::new(),
    };
    let instances = evidence.summary.as_ref().map_or(0, |summary| summary.brokers.len());
    let mut report = diagnosis_service::build_health_report(HealthReport {
        report_type: "broker_health",
        rules_version: BROKER_HEALTH_RULES_VERSION,
        target: json!({ "broker_name": args.broker_name }),
        subject: format!("Broker {}", args.broker_name),
        policy_profile,
        missing_primary: evidence.summary.is_err().then_some("Broker"),
        healthy_summary: format!("{instances} instances answer and no rule found a problem."),
        confidence: match &evidence.summary {
            Ok(_) => diagnosis_service::weighted_confidence(&snapshot, 500, CONFIDENCE_WEIGHTS),
            Err(_) => 0.20,
        },
        findings,
        risks: vec![
            "A Broker hosts queues of many Topics; restarting or reconfiguring it affects all of them.".to_string(),
            "Removing a master without an in-sync replica loses the messages that were not replicated.".to_string(),
        ],
        follow_up_metrics: follow_up_metrics(),
        snapshot,
    });
    if evidence.summary.as_ref().err().and_then(ToolFailure::not_found_entity) == Some(NotFoundEntity::Broker) {
        report_absent_broker(args, &mut report);
    }
    report
}

fn follow_up_metrics() -> Vec<MetricWatchItem> {
    let watch = |name: &str, reason: &str| MetricWatchItem {
        name: name.to_string(),
        target: "broker".to_string(),
        reason: reason.to_string(),
    };
    vec![
        watch("broker_active", "Shows whether every instance answers."),
        watch("dispatch_behind_bytes", "Shows whether the store keeps up with writes."),
        watch(
            "in_sync_slave_count",
            "Shows whether the replicas keep up with the master.",
        ),
    ]
}

/// Replaces "evidence is missing" with the definite conclusion: there is no such Broker.
fn report_absent_broker(args: &DiagnoseBrokerHealthArgs, report: &mut DiagnosisReport) {
    const ABSENT_BROKER_CONFIDENCE: f32 = 0.90;

    let entity = NotFoundEntity::Broker;
    report.summary = format!(
        "Broker {} health is Unknown because the {} does not exist in the selected cluster.",
        args.broker_name,
        entity.label()
    );
    report.confidence = ABSENT_BROKER_CONFIDENCE;
    report.confidence_band = ConfidenceBand::High;
    report.root_causes = vec![diagnosis_service::conclude(
        conclusion::TARGET_NOT_FOUND,
        BROKER_SUMMARY,
        &format!("The {} does not exist in the selected cluster", entity.label()),
        ABSENT_BROKER_CONFIDENCE,
        &format!(
            "rocketmq_describe_broker reported that {} {} was not found.",
            entity.label(),
            args.broker_name
        ),
    )];
    report.recommendations = vec![Recommendation {
        action: entity.suggestion().to_string(),
        priority: "high".to_string(),
        rationale: format!("The {} was not found, so there is nothing to diagnose.", entity.label()),
        risk: "None. Listing names is read-only.".to_string(),
        verification: "Run the diagnosis again with the confirmed name.".to_string(),
    }];
}

fn finding(
    rule_id: &'static str,
    severity: Severity,
    area: &str,
    evidence_refs: &[&'static str],
    cause: String,
    reasoning: String,
    recommendation: Recommendation,
) -> HealthFinding {
    HealthFinding {
        rule_id,
        severity,
        area: area.to_string(),
        finding: Finding {
            cause,
            // A rule reads a state the Broker reports about itself, so what fires is established.
            confidence: 0.90,
            reasoning,
            evidence_refs: evidence_refs.to_vec(),
            stops_consumption: false,
            recommendation,
        },
    }
}

fn recommend(action: &str, priority: &str, rationale: &str, risk: &str, verification: &str) -> Recommendation {
    Recommendation {
        action: action.to_string(),
        priority: priority.to_string(),
        rationale: rationale.to_string(),
        risk: risk.to_string(),
        verification: verification.to_string(),
    }
}

/// Writes instance ids as `0, 1, 2`.
fn ids(ids: impl Iterator<Item = u64>) -> String {
    ids.map(|id| id.to_string()).collect::<Vec<_>>().join(", ")
}

fn master_missing(input: &RuleInput<'_>) -> Option<HealthFinding> {
    if input.summary.brokers.iter().any(|row| row.broker_id == MASTER_ID) {
        return None;
    }
    Some(finding(
        "master_missing",
        Severity::Critical,
        "availability",
        &[BROKER_SUMMARY],
        "The Broker has no master".to_string(),
        format!(
            "No instance of {} is registered with id 0; the registered ids are {}.",
            input.broker_name,
            ids(input.summary.brokers.iter().map(|row| row.broker_id))
        ),
        recommend(
            "Find out why the master is gone: its process, its host, and its registration with the NameServer.",
            "high",
            "Producers write to the master only, so the queues of this Broker accept no messages.",
            "Starting an old master after a replica took over can lose what the replica accepted since.",
            "rocketmq_describe_broker lists an instance with id 0.",
        ),
    ))
}

fn broker_instance_inactive(input: &RuleInput<'_>) -> Option<HealthFinding> {
    let inactive = input
        .summary
        .brokers
        .iter()
        .filter(|row| !row.broker_active)
        .map(|row| row.broker_id)
        .collect::<Vec<_>>();
    if inactive.is_empty() {
        return None;
    }
    let master_down = inactive.contains(&MASTER_ID);
    Some(finding(
        "broker_instance_inactive",
        if master_down {
            Severity::Critical
        } else {
            Severity::Medium
        },
        "availability",
        &[BROKER_SUMMARY],
        if master_down {
            "The master does not answer".to_string()
        } else {
            "A replica does not answer".to_string()
        },
        format!(
            "{} of {} instances of {} are registered but did not answer: id {}.",
            inactive.len(),
            input.summary.brokers.len(),
            input.broker_name,
            ids(inactive.iter().copied())
        ),
        recommend(
            "Check the process and the host of the instance, and the network path from this server to it.",
            "high",
            "An instance that does not answer serves no client, or this server cannot reach it.",
            "A restart drops the connections of every client of the instance.",
            "rocketmq_describe_broker reports broker_active=true for every instance.",
        ),
    ))
}

fn broker_not_ready(input: &RuleInput<'_>) -> Option<HealthFinding> {
    let not_ready = input.diagnosed(|row| {
        row.readiness
            .as_ref()
            .is_some_and(|readiness| !readiness.ready || !readiness.active || readiness.shutdown)
    });
    if not_ready.is_empty() {
        return None;
    }
    let master_affected = not_ready.iter().any(|row| row.broker_id == MASTER_ID);
    Some(finding(
        "broker_not_ready",
        if master_affected {
            Severity::Critical
        } else {
            Severity::High
        },
        "readiness",
        &[BROKER_DIAGNOSTICS],
        "A Broker instance is not ready to serve".to_string(),
        format!(
            "Instance id {} of {} reports that it is not ready, not active, or shutting down.",
            ids(not_ready.iter().map(|row| row.broker_id)),
            input.broker_name
        ),
        recommend(
            "Read the readiness and recovery sections of rocketmq_get_broker_diagnostics for the instance.",
            "high",
            "An instance that is not ready rejects or does not receive client requests.",
            "An instance that is still recovering becomes ready on its own; interrupting it restarts the recovery.",
            "rocketmq_get_broker_diagnostics reports readiness.ready=true.",
        ),
    ))
}

fn store_not_writeable(input: &RuleInput<'_>) -> Option<HealthFinding> {
    let failing = input.diagnosed(|row| {
        row.store_health
            .as_ref()
            .is_some_and(|store| !store.writeable || store.last_flush_error || store.shutdown)
    });
    if failing.is_empty() {
        return None;
    }
    Some(finding(
        "store_not_writeable",
        Severity::Critical,
        "store",
        &[BROKER_DIAGNOSTICS],
        "The message store does not accept writes".to_string(),
        format!(
            "Instance id {} of {} reports a store that is not writeable, failed its last flush, or is shut down.",
            ids(failing.iter().map(|row| row.broker_id)),
            input.broker_name
        ),
        recommend(
            "Check the disk of the instance: free space, I/O errors, and the store paths.",
            "high",
            "A store that cannot write or flush rejects messages, and unflushed messages are lost on a crash.",
            "Freeing space by deleting store files loses the messages in them.",
            "rocketmq_get_broker_diagnostics reports store_health.writeable=true and last_flush_error=false.",
        ),
    ))
}

fn store_under_pressure(input: &RuleInput<'_>) -> Option<HealthFinding> {
    let pressed = input.diagnosed(|row| {
        row.store_health
            .as_ref()
            .is_some_and(|store| store.os_page_cache_busy || store.transient_store_pool_deficient)
    });
    if pressed.is_empty() {
        return None;
    }
    Some(finding(
        "store_under_pressure",
        Severity::Medium,
        "store",
        &[BROKER_DIAGNOSTICS],
        "The message store is under write pressure".to_string(),
        format!(
            "Instance id {} of {} reports a busy page cache or a transient store pool that ran out of buffers.",
            ids(pressed.iter().map(|row| row.broker_id)),
            input.broker_name
        ),
        recommend(
            "Compare the inbound TPS of the Broker with its disk throughput, and look for a slow disk.",
            "medium",
            "A store that cannot keep up makes the Broker reject sends as busy.",
            "Raising flow-control limits lets memory fill up instead.",
            "rocketmq_get_broker_diagnostics reports os_page_cache_busy=false.",
        ),
    ))
}

fn dispatch_behind(input: &RuleInput<'_>) -> Option<HealthFinding> {
    let behind = input.diagnosed(|row| {
        row.store_health
            .as_ref()
            .is_some_and(|store| store.dispatch_behind_bytes >= DISPATCH_BEHIND_BYTES_LIMIT)
    });
    let worst = behind
        .iter()
        .filter_map(|row| row.store_health.as_ref().map(|store| store.dispatch_behind_bytes))
        .max()?;
    Some(finding(
        "dispatch_behind",
        Severity::Medium,
        "store",
        &[BROKER_DIAGNOSTICS],
        "The store is behind in building its consume queues".to_string(),
        format!(
            "Instance id {} of {} has up to {worst} bytes of commit log that are not dispatched yet; the limit is \
             {DISPATCH_BEHIND_BYTES_LIMIT}.",
            ids(behind.iter().map(|row| row.broker_id)),
            input.broker_name
        ),
        recommend(
            "Check the disk and CPU load of the instance; dispatch runs on one thread.",
            "medium",
            "Consumers see a message only after it is dispatched, so stored messages are delayed.",
            "None. Watching the figure is read-only.",
            "rocketmq_get_broker_diagnostics reports a falling dispatch_behind_bytes.",
        ),
    ))
}

fn sync_replication_stalled(input: &RuleInput<'_>) -> Option<HealthFinding> {
    let from_store = input
        .diagnosed(|row| {
            row.store_health.as_ref().is_some_and(|store| {
                store.ha_pending_request_count > 0
                    && store.ha_pending_oldest_wait_millis >= SYNC_REPLICATION_WAIT_LIMIT_MILLIS
            })
        })
        .iter()
        .filter_map(|row| row.store_health.as_ref())
        .map(|store| store.ha_pending_oldest_wait_millis)
        .max();
    let from_ha = input
        .ha
        .into_iter()
        .flat_map(|ha| &ha.brokers)
        .filter(|broker| {
            broker.pending_group_transfer_request_count > 0
                && broker.pending_group_transfer_oldest_wait_millis >= SYNC_REPLICATION_WAIT_LIMIT_MILLIS
        })
        .map(|broker| broker.pending_group_transfer_oldest_wait_millis)
        .max();
    let waited = from_store.max(from_ha)?;
    let mut evidence_refs = Vec::new();
    if from_store.is_some() {
        evidence_refs.push(BROKER_DIAGNOSTICS);
    }
    if from_ha.is_some() {
        evidence_refs.push(HA_STATUS);
    }
    Some(finding(
        "sync_replication_stalled",
        Severity::High,
        "replication",
        &evidence_refs,
        "Writes wait for replicas that do not acknowledge".to_string(),
        format!(
            "The oldest write of {} has waited {waited} ms for a replica; a synchronous master gives up after \
             {SYNC_REPLICATION_WAIT_LIMIT_MILLIS} ms.",
            input.broker_name
        ),
        recommend(
            "Check the replicas of the Broker and the network between them and the master with \
             rocketmq_get_ha_status.",
            "high",
            "Producers of a synchronous master receive a timeout for every write that a replica does not confirm.",
            "Switching the master to asynchronous replication trades the timeouts for possible message loss.",
            "rocketmq_get_ha_status reports no pending transfer request.",
        ),
    ))
}

fn no_in_sync_replica(input: &RuleInput<'_>) -> Option<HealthFinding> {
    let master = input
        .ha?
        .brokers
        .iter()
        .find(|broker| broker.broker_name == input.broker_name && broker.broker_id == MASTER_ID)?;
    let replicas = input.replica_count().max(master.connections.len());
    if replicas == 0 || master.in_sync_slave_count > 0 {
        return None;
    }
    Some(finding(
        "no_in_sync_replica",
        Severity::High,
        "replication",
        &[HA_STATUS, BROKER_SUMMARY],
        "No replica is in sync with the master".to_string(),
        format!(
            "Replicas of {}: {replicas} known, {} connected to the master, 0 in sync.",
            input.broker_name,
            master.connections.len()
        ),
        recommend(
            "Check that the replicas run and can reach the master, then watch them catch up.",
            "high",
            "Without an in-sync replica, losing the master loses the messages that were not replicated.",
            "A replica that is far behind loads the master while it catches up.",
            "rocketmq_get_ha_status reports in_sync_slave_count above zero.",
        ),
    ))
}

fn replica_not_in_sync(input: &RuleInput<'_>) -> Option<HealthFinding> {
    let master = input
        .ha?
        .brokers
        .iter()
        .find(|broker| broker.broker_name == input.broker_name && broker.broker_id == MASTER_ID)?;
    let lagging = master
        .connections
        .iter()
        .filter(|connection| !connection.in_sync)
        .collect::<Vec<_>>();
    // With no replica in sync at all, `no_in_sync_replica` is the conclusion.
    if lagging.is_empty() || master.in_sync_slave_count == 0 {
        return None;
    }
    Some(finding(
        "replica_not_in_sync",
        Severity::Medium,
        "replication",
        &[HA_STATUS],
        "A replica is behind the master".to_string(),
        format!(
            "Replica id {} of {} is not in sync; the furthest is {} bytes behind.",
            ids(lagging.iter().map(|connection| connection.replica.broker_id)),
            input.broker_name,
            lagging
                .iter()
                .map(|connection| connection.diff)
                .max()
                .unwrap_or_default()
        ),
        recommend(
            "Check the load and the network of the replica that is behind.",
            "medium",
            "A replica that is behind cannot take over without losing what it has not received.",
            "None. Watching the replica is read-only.",
            "rocketmq_get_ha_status reports in_sync=true for every replica.",
        ),
    ))
}

fn recovery_failed(input: &RuleInput<'_>) -> Option<HealthFinding> {
    let failed = input.diagnosed(|row| {
        row.recovery
            .as_ref()
            .is_some_and(|recovery| recovery.failed_phase_count.is_some_and(|count| count > 0))
    });
    if failed.is_empty() {
        return None;
    }
    Some(finding(
        "recovery_failed",
        Severity::High,
        "recovery",
        &[BROKER_DIAGNOSTICS],
        "The last store recovery did not complete".to_string(),
        format!(
            "Instance id {} of {} reports failed phases in its last recovery.",
            ids(failed.iter().map(|row| row.broker_id)),
            input.broker_name
        ),
        recommend(
            "Read the recovery section of rocketmq_get_broker_diagnostics and the Broker log of that start.",
            "high",
            "A store that did not recover completely may serve fewer messages than it holds.",
            "Restarting to recover again repeats the same scan and the same downtime.",
            "rocketmq_get_broker_diagnostics reports failed_phase_count=0 after the next start.",
        ),
    ))
}

fn index_rebuild_failed(input: &RuleInput<'_>) -> Option<HealthFinding> {
    let failed = input.diagnosed(|row| {
        row.background_index_rebuild
            .as_ref()
            .is_some_and(|rebuild| rebuild.state == BackgroundIndexRebuildState::Failed)
    });
    if failed.is_empty() {
        return None;
    }
    Some(finding(
        "index_rebuild_failed",
        Severity::Medium,
        "recovery",
        &[BROKER_DIAGNOSTICS],
        "The background index rebuild failed".to_string(),
        format!(
            "Instance id {} of {} reports its background index rebuild as failed.",
            ids(failed.iter().map(|row| row.broker_id)),
            input.broker_name
        ),
        recommend(
            "Read the background_index_rebuild section of rocketmq_get_broker_diagnostics and the Broker log.",
            "medium",
            "Until the index is rebuilt, lookups by key or id miss the messages it does not cover.",
            "None. Reading the state is read-only.",
            "rocketmq_get_broker_diagnostics reports the rebuild as completed.",
        ),
    ))
}

fn config_inconsistent(input: &RuleInput<'_>) -> Option<HealthFinding> {
    let rows = &input.config?.brokers;
    let first = rows.first()?;
    let mut fields = Vec::new();
    if rows
        .iter()
        .any(|row| row.send_message_thread_pool_nums != first.send_message_thread_pool_nums)
    {
        fields.push("send_message_thread_pool_nums");
    }
    if rows
        .iter()
        .any(|row| row.pull_message_thread_pool_nums != first.pull_message_thread_pool_nums)
    {
        fields.push("pull_message_thread_pool_nums");
    }
    if rows
        .iter()
        .any(|row| row.flush_delay_offset_interval_ms != first.flush_delay_offset_interval_ms)
    {
        fields.push("flush_delay_offset_interval_ms");
    }
    if rows
        .iter()
        .any(|row| row.max_client_event_count != first.max_client_event_count)
    {
        fields.push("max_client_event_count");
    }
    if fields.is_empty() {
        return None;
    }
    Some(finding(
        "config_inconsistent",
        Severity::Low,
        "configuration",
        &[BROKER_CONFIG],
        "The instances of the Broker are configured differently".to_string(),
        format!(
            "The {} instances of {} differ in {}.",
            rows.len(),
            input.broker_name,
            fields.join(", ")
        ),
        recommend(
            "Compare the instances with rocketmq_get_broker_config_summary and decide which value is intended. Plan \
             a change with a dry run of rocketmq_patch_broker_config on the control MCP server.",
            "low",
            "A replica that is configured differently behaves differently once it takes over.",
            "Some settings differ between a master and its replicas on purpose.",
            "rocketmq_get_broker_config_summary reports the same values for every instance.",
        ),
    ))
}

#[cfg(test)]
pub(crate) mod fixtures {
    use super::*;
    use crate::tools::broker_tools::AuthSecurityDiagnostics;
    use crate::tools::broker_tools::BrokerDiagnosticsCoverage;
    use crate::tools::broker_tools::BrokerReadinessDiagnostics;
    use crate::tools::broker_tools::HaDiagnostics;
    use crate::tools::broker_tools::RocksDbMaintenanceDiagnostics;
    use crate::tools::broker_tools::StoreHealthDiagnostics;
    use crate::tools::broker_tools::SyncFlushDiagnostics;
    use crate::tools::broker_tools::TieredDispatchDiagnostics;
    use crate::tools::cluster_tools::BrokerSummary;
    use crate::tools::config_tools::BrokerConfigSummaryRow;
    use crate::tools::infrastructure_tools::BrokerHaObservation;
    use crate::tools::infrastructure_tools::HaConnectionObservation;
    use crate::tools::infrastructure_tools::LogicalBrokerInstance;

    pub(crate) const BROKER: &str = "broker-a";

    pub(crate) fn args() -> DiagnoseBrokerHealthArgs {
        DiagnoseBrokerHealthArgs {
            cluster: "local-dev".to_string(),
            broker_name: BROKER.to_string(),
        }
    }

    pub(crate) fn instance(broker_id: u64) -> BrokerSummary {
        BrokerSummary {
            cluster: "local-dev".to_string(),
            broker_name: BROKER.to_string(),
            broker_id,
            broker_addr: "127.0.0.1:10911".to_string(),
            version: "V5_3_0".to_string(),
            in_tps: "10.0".to_string(),
            out_tps: "10.0".to_string(),
            timer_progress: "0".to_string(),
            page_cache_lock_time_millis: "0".to_string(),
            hour: "0".to_string(),
            space: "0".to_string(),
            broker_active: true,
        }
    }

    pub(crate) fn summary(instances: Vec<BrokerSummary>) -> DescribeBrokerOutput {
        DescribeBrokerOutput {
            cluster: "local-dev".to_string(),
            namesrv_addr: "127.0.0.1:9876".to_string(),
            broker_name: BROKER.to_string(),
            brokers: instances,
            generated_at: "2026-10-10T12:00:00.000Z".to_string(),
        }
    }

    /// The diagnostics of a ready instance with a healthy store.
    pub(crate) fn diagnosed(broker_id: u64) -> BrokerDiagnosticsRow {
        BrokerDiagnosticsRow {
            broker_name: BROKER.to_string(),
            broker_id,
            observed_at_millis: Some(1),
            coverage: BrokerDiagnosticsCoverage::Available,
            readiness: Some(BrokerReadinessDiagnostics {
                ready: true,
                active: true,
                shutdown: false,
                registration_accepting: true,
                registration_configured: true,
            }),
            config: None,
            store_health: Some(StoreHealthDiagnostics {
                writeable: true,
                last_flush_error: false,
                os_page_cache_busy: false,
                transient_store_pool_deficient: false,
                dispatch_behind_bytes: 0,
                shutdown: false,
                ha_pending_request_count: 0,
                ha_pending_oldest_wait_millis: 0,
                sync_flush: SyncFlushDiagnostics {
                    queue_depth: 0,
                    timeout_total: 0,
                    oldest_wait_millis: 0,
                },
            }),
            ha: HaDiagnostics {
                supported: false,
                role: None,
                master_epoch: None,
                sync_state_set_epoch: None,
                sync_state_set_size: None,
                max_replica_lag_bytes: None,
                ack_policy: None,
                required_ack_count: None,
                decision_code: None,
            },
            recovery: None,
            background_index_rebuild: None,
            rocksdb: RocksDbMaintenanceDiagnostics {
                supported: false,
                maintenance_running: None,
                message_maintenance_running: None,
            },
            tiered: TieredDispatchDiagnostics {
                configured: false,
                dispatch_ready: None,
                minimum_pinned_wal_segment: None,
            },
            auth: AuthSecurityDiagnostics {
                supported: false,
                authentication_enabled: None,
                authorization_enabled: None,
                acl_file_watch_enabled: None,
                acl_generation: None,
                acl_reload_attempts: None,
                acl_reload_successes: None,
                acl_reload_failures: None,
                acl_reload_skipped: None,
                credential_rotation_supported: false,
            },
            warnings: Vec::new(),
        }
    }

    pub(crate) fn diagnostics(rows: Vec<BrokerDiagnosticsRow>) -> BrokerDiagnosticsOutput {
        BrokerDiagnosticsOutput {
            cluster: "local-dev".to_string(),
            broker_name: BROKER.to_string(),
            diagnostics_schema_version: "rocketmq-mcp.broker-diagnostics.v1".to_string(),
            observed_at_millis: 1,
            brokers: rows,
            unavailable_brokers: 0,
        }
    }

    /// The master's view of its replicas: one connection per entry, `(replica id, in sync, bytes behind)`.
    pub(crate) fn ha(replicas: &[(u64, bool, i64)]) -> GetHaStatusOutput {
        GetHaStatusOutput {
            cluster: "local-dev".to_string(),
            brokers: vec![BrokerHaObservation {
                broker_name: BROKER.to_string(),
                broker_id: 0,
                master_commit_log_max_offset: 1_000_000,
                in_sync_slave_count: replicas.iter().filter(|(_, in_sync, _)| *in_sync).count() as u32,
                pending_group_transfer_request_count: 0,
                pending_group_transfer_oldest_wait_millis: 0,
                group_transfer_ack_notify_count: 0,
                connections: replicas
                    .iter()
                    .map(|(broker_id, in_sync, diff)| HaConnectionObservation {
                        replica: LogicalBrokerInstance {
                            broker_name: BROKER.to_string(),
                            broker_id: *broker_id,
                        },
                        slave_ack_offset: 1_000_000,
                        diff: *diff,
                        in_sync: *in_sync,
                        transferred_bytes_per_second: 1_024,
                        transfer_from_where: 0,
                    })
                    .collect(),
            }],
            controller_sync_states: Vec::new(),
        }
    }

    pub(crate) fn config(send_threads: &[u32]) -> BrokerConfigSummaryOutput {
        BrokerConfigSummaryOutput {
            cluster: "local-dev".to_string(),
            broker_name: BROKER.to_string(),
            brokers: send_threads
                .iter()
                .zip(0_u64..)
                .map(|(threads, broker_id)| BrokerConfigSummaryRow {
                    broker_name: BROKER.to_string(),
                    broker_id,
                    generation: 1,
                    send_message_thread_pool_nums: Some(*threads),
                    pull_message_thread_pool_nums: Some(32),
                    flush_delay_offset_interval_ms: Some(10_000),
                    max_client_event_count: Some(100),
                })
                .collect(),
        }
    }

    /// Complete evidence of a healthy master with one replica in sync.
    pub(crate) fn evidence() -> BrokerHealthEvidence {
        BrokerHealthEvidence {
            summary: Ok(summary(vec![instance(0), instance(1)])),
            diagnostics: Ok(diagnostics(vec![diagnosed(0), diagnosed(1)])),
            ha: Ok(ha(&[(1, true, 0)])),
            config: Ok(config(&[16, 16])),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fixtures::diagnosed;
    use super::fixtures::instance;
    use super::*;
    use crate::model::diagnosis::EvidenceStatus;
    use crate::tools::broker_tools::BackgroundIndexRebuildDiagnostics;
    use crate::tools::broker_tools::RecoveryDiagnostics;
    use crate::tools::executor::ToolExecutionError;

    fn diagnose(evidence: BrokerHealthEvidence) -> DiagnosisReport {
        evaluate(&fixtures::args(), evidence, "production-default")
    }

    fn rule_ids(report: &DiagnosisReport) -> Vec<&str> {
        report.root_causes.iter().map(|cause| cause.rule_id.as_str()).collect()
    }

    /// The report of healthy evidence whose master diagnostics were changed by `adjust`.
    fn with_master(adjust: impl Fn(&mut BrokerDiagnosticsRow)) -> DiagnosisReport {
        let mut master = diagnosed(0);
        adjust(&mut master);
        let mut evidence = fixtures::evidence();
        evidence.diagnostics = Ok(fixtures::diagnostics(vec![master, diagnosed(1)]));
        diagnose(evidence)
    }

    fn unavailable<T>() -> Result<T, ToolFailure> {
        Err(ToolFailure::Operational(ToolExecutionError::Backend(None)))
    }

    #[test]
    fn a_broker_whose_instances_answer_and_agree_is_healthy() {
        let report = diagnose(fixtures::evidence());

        assert_eq!(report.report_type, "broker_health");
        assert_eq!(report.rules_version, BROKER_HEALTH_RULES_VERSION);
        assert_eq!(report.evidence_version, BROKER_HEALTH_EVIDENCE_VERSION);
        assert_eq!(report.severity, Severity::Healthy);
        assert!(!report.partial);
        assert_eq!(report.confidence, 0.95);
        assert!(report.root_causes.is_empty());
        assert_eq!(
            report.summary,
            "Broker broker-a is Healthy: 2 instances answer and no rule found a problem."
        );
        assert_eq!(
            report.evidence_refs,
            ["broker_summary", "broker_diagnostics", "ha_status", "broker_config"]
        );
        assert_eq!(report.recommendations[0].action, "Continue monitoring.");
    }

    #[test]
    fn rules_that_fire_together_have_distinct_identifiers() {
        // Evidence built to trip as many rules as can hold at once.
        let mut master = diagnosed(0);
        let store = master.store_health.as_mut().unwrap();
        store.writeable = false;
        store.os_page_cache_busy = true;
        store.dispatch_behind_bytes = DISPATCH_BEHIND_BYTES_LIMIT;
        store.ha_pending_request_count = 1;
        store.ha_pending_oldest_wait_millis = SYNC_REPLICATION_WAIT_LIMIT_MILLIS;
        master.readiness.as_mut().unwrap().ready = false;
        let mut evidence = fixtures::evidence();
        evidence.diagnostics = Ok(fixtures::diagnostics(vec![master]));
        let mut replica = instance(1);
        replica.broker_active = false;
        evidence.summary = Ok(fixtures::summary(vec![instance(0), replica]));
        evidence.ha = Ok(fixtures::ha(&[(1, false, 4_096)]));
        evidence.config = Ok(fixtures::config(&[16, 8]));

        let report = diagnose(evidence);

        let mut ids = rule_ids(&report);
        let fired = ids.len();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), fired);
        assert!(fired >= 8, "{ids:?}");
        assert_eq!(report.severity, Severity::Critical);
    }

    #[test]
    fn master_missing_fires_when_only_replicas_are_registered() {
        let mut evidence = fixtures::evidence();
        evidence.summary = Ok(fixtures::summary(vec![instance(1), instance(2)]));

        let report = diagnose(evidence);

        assert_eq!(report.severity, Severity::Critical);
        assert_eq!(rule_ids(&report)[0], "master_missing");
        assert!(report.root_causes[0].reasoning.contains("the registered ids are 1, 2"));
        assert_eq!(report.root_causes[0].evidence_refs, ["broker_summary"]);
        assert_eq!(report.impacts[0].area, "availability");
    }

    #[test]
    fn broker_instance_inactive_is_critical_for_the_master_and_medium_for_a_replica() {
        let report_with = |inactive_id: u64| {
            let mut evidence = fixtures::evidence();
            let rows = [0, 1]
                .map(|id| {
                    let mut row = instance(id);
                    row.broker_active = id != inactive_id;
                    row
                })
                .to_vec();
            evidence.summary = Ok(fixtures::summary(rows));
            diagnose(evidence)
        };

        let master = report_with(0);
        assert_eq!(master.severity, Severity::Critical);
        assert_eq!(rule_ids(&master), ["broker_instance_inactive"]);
        assert_eq!(master.root_causes[0].cause, "The master does not answer");

        let replica = report_with(1);
        assert_eq!(replica.severity, Severity::Medium);
        assert_eq!(replica.root_causes[0].cause, "A replica does not answer");
        assert!(replica.root_causes[0].reasoning.contains("1 of 2 instances"));
    }

    #[test]
    fn broker_not_ready_fires_for_an_instance_that_reports_itself_not_ready() {
        let report = with_master(|row| row.readiness.as_mut().unwrap().ready = false);
        assert_eq!(rule_ids(&report), ["broker_not_ready"]);
        assert_eq!(report.severity, Severity::Critical);
        assert_eq!(report.root_causes[0].evidence_refs, ["broker_diagnostics"]);

        let shutting_down = with_master(|row| row.readiness.as_mut().unwrap().shutdown = true);
        assert_eq!(rule_ids(&shutting_down), ["broker_not_ready"]);
    }

    #[test]
    fn store_not_writeable_fires_for_a_store_that_rejects_writes_or_failed_a_flush() {
        let rejecting = with_master(|row| row.store_health.as_mut().unwrap().writeable = false);
        assert_eq!(rule_ids(&rejecting), ["store_not_writeable"]);
        assert_eq!(rejecting.severity, Severity::Critical);
        assert_eq!(rejecting.impacts[0].area, "store");

        let flush_failed = with_master(|row| row.store_health.as_mut().unwrap().last_flush_error = true);
        assert_eq!(rule_ids(&flush_failed), ["store_not_writeable"]);
    }

    #[test]
    fn store_under_pressure_fires_for_a_busy_page_cache() {
        let report = with_master(|row| row.store_health.as_mut().unwrap().os_page_cache_busy = true);

        assert_eq!(rule_ids(&report), ["store_under_pressure"]);
        assert_eq!(report.severity, Severity::Medium);
    }

    #[test]
    fn dispatch_behind_fires_from_its_limit() {
        let behind =
            |bytes: i64| with_master(move |row| row.store_health.as_mut().unwrap().dispatch_behind_bytes = bytes);

        assert!(rule_ids(&behind(DISPATCH_BEHIND_BYTES_LIMIT - 1)).is_empty());
        let report = behind(DISPATCH_BEHIND_BYTES_LIMIT);
        assert_eq!(rule_ids(&report), ["dispatch_behind"]);
        assert!(report.root_causes[0].reasoning.contains("268435456 bytes"));
    }

    #[test]
    fn sync_replication_stalled_fires_when_a_write_waits_longer_than_the_sync_timeout() {
        let waiting = |millis: u64| {
            with_master(move |row| {
                let store = row.store_health.as_mut().unwrap();
                store.ha_pending_request_count = 3;
                store.ha_pending_oldest_wait_millis = millis;
            })
        };
        assert!(rule_ids(&waiting(4_999)).is_empty());
        let report = waiting(5_000);
        assert_eq!(rule_ids(&report), ["sync_replication_stalled"]);
        assert_eq!(report.severity, Severity::High);
        assert_eq!(report.root_causes[0].evidence_refs, ["broker_diagnostics"]);

        // The master's replication view shows the same wait without the store diagnostics.
        let mut evidence = fixtures::evidence();
        evidence.diagnostics = unavailable();
        let mut ha = fixtures::ha(&[(1, true, 0)]);
        ha.brokers[0].pending_group_transfer_request_count = 1;
        ha.brokers[0].pending_group_transfer_oldest_wait_millis = 9_000;
        evidence.ha = Ok(ha);
        let report = diagnose(evidence);
        assert_eq!(rule_ids(&report), ["sync_replication_stalled"]);
        assert_eq!(report.root_causes[0].evidence_refs, ["ha_status"]);
        assert!(report.root_causes[0].reasoning.contains("waited 9000 ms"));
    }

    #[test]
    fn no_in_sync_replica_fires_when_the_replicas_exist_and_none_keeps_up() {
        let mut evidence = fixtures::evidence();
        evidence.ha = Ok(fixtures::ha(&[(1, false, 8_192)]));

        let report = diagnose(evidence);

        assert_eq!(rule_ids(&report), ["no_in_sync_replica"]);
        assert_eq!(report.severity, Severity::High);
        assert_eq!(
            report.root_causes[0].reasoning,
            "Replicas of broker-a: 1 known, 1 connected to the master, 0 in sync."
        );

        // A replica that is registered but not connected is not in sync either.
        let mut evidence = fixtures::evidence();
        evidence.ha = Ok(fixtures::ha(&[]));
        assert_eq!(rule_ids(&diagnose(evidence)), ["no_in_sync_replica"]);

        // A master without replicas has none to be in sync with.
        let mut evidence = fixtures::evidence();
        evidence.summary = Ok(fixtures::summary(vec![instance(0)]));
        evidence.diagnostics = Ok(fixtures::diagnostics(vec![diagnosed(0)]));
        evidence.ha = Ok(fixtures::ha(&[]));
        evidence.config = Ok(fixtures::config(&[16]));
        assert!(rule_ids(&diagnose(evidence)).is_empty());
    }

    #[test]
    fn replica_not_in_sync_fires_when_one_replica_is_behind_and_another_keeps_up() {
        let mut evidence = fixtures::evidence();
        evidence.summary = Ok(fixtures::summary(vec![instance(0), instance(1), instance(2)]));
        evidence.ha = Ok(fixtures::ha(&[(1, true, 0), (2, false, 65_536)]));

        let report = diagnose(evidence);

        assert_eq!(rule_ids(&report), ["replica_not_in_sync"]);
        assert_eq!(report.severity, Severity::Medium);
        assert!(report.root_causes[0]
            .reasoning
            .contains("Replica id 2 of broker-a is not in sync; the furthest is 65536 bytes behind."));
    }

    #[test]
    fn recovery_failed_fires_for_failed_recovery_phases() {
        let report = with_master(|row| {
            row.recovery = Some(RecoveryDiagnostics {
                available: true,
                total_duration_millis: Some(1_000),
                phase_count: Some(4),
                failed_phase_count: Some(1),
                fallback_phase_count: Some(0),
                fallback_reason_present: Some(false),
                scanned_bytes: Some(1),
                recovered_messages: Some(1),
                invalid_messages: Some(0),
                truncated_files: Some(0),
                index_files_removed: Some(0),
                index_files_rebuilt: Some(0),
            });
        });

        assert_eq!(rule_ids(&report), ["recovery_failed"]);
        assert_eq!(report.severity, Severity::High);
    }

    #[test]
    fn index_rebuild_failed_fires_for_a_failed_background_rebuild() {
        let report = with_master(|row| {
            row.background_index_rebuild = Some(BackgroundIndexRebuildDiagnostics {
                state: BackgroundIndexRebuildState::Failed,
                effective_enabled: true,
                gray_mode: false,
                current_safe_offset: 0,
                target_offset: 10,
                backlog_bytes: 10,
                rebuilt_bytes: 0,
                rebuilt_messages: 0,
                failure_count: 3,
                bytes_per_second: 0,
            });
        });

        assert_eq!(rule_ids(&report), ["index_rebuild_failed"]);
        assert_eq!(report.severity, Severity::Medium);
    }

    #[test]
    fn config_inconsistent_names_the_fields_that_differ() {
        let mut evidence = fixtures::evidence();
        evidence.config = Ok(fixtures::config(&[16, 8]));

        let report = diagnose(evidence);

        assert_eq!(rule_ids(&report), ["config_inconsistent"]);
        assert_eq!(report.severity, Severity::Low);
        assert_eq!(
            report.root_causes[0].reasoning,
            "The 2 instances of broker-a differ in send_message_thread_pool_nums."
        );
    }

    #[test]
    fn findings_are_listed_worst_first_and_the_summary_names_the_worst() {
        let mut evidence = fixtures::evidence();
        evidence.config = Ok(fixtures::config(&[16, 8]));
        evidence.ha = Ok(fixtures::ha(&[(1, false, 8_192)]));
        let mut master = diagnosed(0);
        master.store_health.as_mut().unwrap().os_page_cache_busy = true;
        evidence.diagnostics = Ok(fixtures::diagnostics(vec![master, diagnosed(1)]));

        let report = diagnose(evidence);

        assert_eq!(
            rule_ids(&report),
            ["no_in_sync_replica", "store_under_pressure", "config_inconsistent"]
        );
        assert_eq!(report.severity, Severity::High);
        assert_eq!(
            report.summary,
            "Broker broker-a is High: No replica is in sync with the master, and 2 more findings."
        );
        assert_eq!(report.impacts.len(), 3);
        assert_eq!(report.recommendations.len(), 3);
    }

    #[test]
    fn a_failed_evidence_source_silences_its_rules_and_leaves_a_partial_report() {
        let mut evidence = fixtures::evidence();
        evidence.diagnostics = unavailable();
        evidence.ha = Err(ToolFailure::Rejected(crate::tools::executor::ToolRejection::TimedOut {
            timeout_ms: 5_000,
        }));

        let report = diagnose(evidence);

        assert!(report.partial);
        assert_eq!(report.missing_evidence, ["broker_diagnostics", "ha_status"]);
        assert_eq!(report.severity, Severity::Healthy, "the instances answer and agree");
        assert_eq!(report.confidence, 0.6);
        assert!(report.root_causes.is_empty());
        let status = |id: &str| {
            report
                .evidence_snapshot
                .items
                .iter()
                .find(|item| item.id == id)
                .unwrap()
                .status
        };
        assert_eq!(status("ha_status"), EvidenceStatus::Timeout);
    }

    #[test]
    fn the_health_is_unknown_without_the_broker_rows() {
        let mut evidence = fixtures::evidence();
        evidence.summary = unavailable();

        let report = diagnose(evidence);

        assert_eq!(report.severity, Severity::Unknown);
        assert_eq!(
            report.summary,
            "Broker broker-a health is Unknown because Broker evidence is missing."
        );
        assert!(report.root_causes.is_empty(), "no rule runs without the instances");
        assert_eq!(report.confidence, 0.20);
        assert_eq!(report.recommendations[0].action, "Collect the evidence again.");
    }

    #[test]
    fn a_broker_that_does_not_exist_is_reported_as_such() {
        let mut evidence = fixtures::evidence();
        evidence.summary = Err(ToolFailure::not_found(NotFoundEntity::Broker));

        let report = diagnose(evidence);

        assert_eq!(report.severity, Severity::Unknown);
        assert_eq!(rule_ids(&report), ["target_not_found"]);
        assert!(report.summary.contains("the broker does not exist"));
        assert_eq!(report.confidence_band, ConfidenceBand::High);
    }
}
