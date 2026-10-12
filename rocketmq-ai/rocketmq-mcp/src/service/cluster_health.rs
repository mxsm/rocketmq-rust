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

//! The health of a cluster at a glance: which Brokers to look at first.
//!
//! It reads only what one request to the NameServer and one to each master tell: which instances
//! are registered, which answer, and whether the replicas keep up. It does not look inside a
//! Broker; a finding names the Brokers to hand to the Broker health diagnosis.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use serde::Serialize;
use serde_json::json;

use crate::model::diagnosis::DiagnosisReport;
use crate::model::diagnosis::EvidenceKind;
use crate::model::diagnosis::EvidenceSnapshot;
use crate::model::diagnosis::MetricWatchItem;
use crate::model::diagnosis::Recommendation;
use crate::model::diagnosis::Severity;
use crate::service::diagnosis_evidence::LISTED_BROKER_LIMIT;
use crate::service::diagnosis_rules::Finding;
use crate::service::diagnosis_service;
use crate::service::diagnosis_service::HealthFinding;
use crate::service::diagnosis_service::HealthReport;
use crate::service::diagnosis_snapshot;
use crate::tools::cluster_tools::BrokerSummary;
use crate::tools::diagnosis_tools::GetClusterHealthArgs;
use crate::tools::executor::ToolFailure;
use crate::tools::infrastructure_tools::GetHaStatusOutput;

pub(crate) const CLUSTER_HEALTH_EVIDENCE_VERSION: &str = "rocketmq-mcp.evidence.cluster-health.v1";
pub(crate) const CLUSTER_HEALTH_RULES_VERSION: &str = "rocketmq-mcp.rules.cluster-health.v1";

const BROKER_INVENTORY: &str = "broker_inventory";
const HA_STATUS: &str = "ha_status";

/// The id a master registers under.
const MASTER_ID: u64 = 0;

/// What the replication evidence adds to the confidence of a report, in thousandths.
const CONFIDENCE_WEIGHTS: &[(&str, u16)] = &[(HA_STATUS, 350)];

/// The first [`LISTED_BROKER_LIMIT`] names of a sorted set.
fn listed(names: &BTreeSet<String>) -> Vec<String> {
    names.iter().take(LISTED_BROKER_LIMIT).cloned().collect()
}

/// The Broker instances a cluster has registered, counted.
///
/// A Broker name stands for a master and its replicas. Each list holds at most
/// [`LISTED_BROKER_LIMIT`] names; the count next to it covers every Broker.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct BrokerInventory {
    pub cluster: String,
    pub broker_count: usize,
    pub instance_count: usize,
    pub master_count: usize,
    pub replica_count: usize,
    /// Instances that are registered but did not answer.
    pub inactive_instance_count: usize,
    pub inactive_master_count: usize,
    pub inactive_brokers: Vec<String>,
    pub brokers_without_master_count: usize,
    pub brokers_without_master: Vec<String>,
    pub masters_without_replica_count: usize,
    pub masters_without_replica: Vec<String>,
    /// The Broker versions in use, most instances first.
    pub versions: Vec<VersionCount>,
    pub generated_at: String,
    /// How many replicas each Broker name has registered.
    #[serde(skip)]
    replicas: BTreeMap<String, usize>,
}

/// How many instances run one Broker version.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct VersionCount {
    pub version: String,
    pub instance_count: usize,
}

impl BrokerInventory {
    pub(crate) fn from_rows(cluster: &str, rows: &[BrokerSummary], generated_at: String) -> Self {
        let mut names = BTreeSet::new();
        let mut with_master = BTreeSet::new();
        let mut inactive = BTreeSet::new();
        let mut replicas = BTreeMap::<String, usize>::new();
        let mut versions = BTreeMap::<&str, usize>::new();
        let mut inactive_master_count = 0;
        for row in rows {
            names.insert(row.broker_name.clone());
            *versions.entry(row.version.as_str()).or_default() += 1;
            if row.broker_id == MASTER_ID {
                with_master.insert(row.broker_name.clone());
            } else {
                *replicas.entry(row.broker_name.clone()).or_default() += 1;
            }
            if !row.broker_active {
                inactive.insert(row.broker_name.clone());
                inactive_master_count += usize::from(row.broker_id == MASTER_ID);
            }
        }
        let without_master = names.difference(&with_master).cloned().collect::<BTreeSet<_>>();
        let without_replica = with_master
            .iter()
            .filter(|name| !replicas.contains_key(*name))
            .cloned()
            .collect::<BTreeSet<_>>();
        let mut versions = versions
            .into_iter()
            .map(|(version, instance_count)| VersionCount {
                version: version.to_string(),
                instance_count,
            })
            .collect::<Vec<_>>();
        // The sort is stable, so the version order of the map breaks ties.
        versions.sort_by_key(|version| std::cmp::Reverse(version.instance_count));
        Self {
            cluster: cluster.to_string(),
            broker_count: names.len(),
            instance_count: rows.len(),
            master_count: with_master.len(),
            replica_count: replicas.values().sum(),
            inactive_instance_count: rows.iter().filter(|row| !row.broker_active).count(),
            inactive_master_count,
            inactive_brokers: listed(&inactive),
            brokers_without_master_count: without_master.len(),
            brokers_without_master: listed(&without_master),
            masters_without_replica_count: without_replica.len(),
            masters_without_replica: listed(&without_replica),
            versions,
            generated_at,
            replicas,
        }
    }

    fn replicas_of(&self, broker_name: &str) -> usize {
        self.replicas.get(broker_name).copied().unwrap_or_default()
    }
}

/// How the replicas of a cluster keep up with their masters, counted over every master that
/// reported.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct ReplicationSummary {
    pub cluster: String,
    /// Masters that reported their replication.
    pub master_count: usize,
    /// Masters that have replicas, none of them in sync.
    pub masters_without_in_sync_replica_count: usize,
    pub masters_without_in_sync_replica: Vec<String>,
    /// Masters with a replica in sync and another one behind.
    pub masters_with_lagging_replica_count: usize,
    pub masters_with_lagging_replica: Vec<String>,
    /// How far the furthest replica is behind its master, in bytes.
    pub max_replica_lag_bytes: i64,
    pub generated_at: String,
}

impl ReplicationSummary {
    /// Summarizes the replication view of the masters. A master has replicas when one is
    /// connected to it or, with the `inventory` at hand, when one is registered under its name.
    pub(crate) fn from_ha(ha: &GetHaStatusOutput, inventory: Option<&BrokerInventory>, generated_at: String) -> Self {
        let mut without_in_sync = BTreeSet::new();
        let mut with_lagging = BTreeSet::new();
        let mut max_replica_lag_bytes = 0;
        for master in ha.brokers.iter().filter(|broker| broker.broker_id == MASTER_ID) {
            let registered = inventory.map_or(0, |inventory| inventory.replicas_of(&master.broker_name));
            let lagging = master.connections.iter().filter(|connection| !connection.in_sync);
            max_replica_lag_bytes = lagging
                .clone()
                .map(|connection| connection.diff)
                .fold(max_replica_lag_bytes, i64::max);
            if master.in_sync_slave_count == 0 {
                if registered.max(master.connections.len()) > 0 {
                    without_in_sync.insert(master.broker_name.clone());
                }
            } else if lagging.count() > 0 {
                with_lagging.insert(master.broker_name.clone());
            }
        }
        Self {
            cluster: ha.cluster.clone(),
            master_count: ha.brokers.iter().filter(|broker| broker.broker_id == MASTER_ID).count(),
            masters_without_in_sync_replica_count: without_in_sync.len(),
            masters_without_in_sync_replica: listed(&without_in_sync),
            masters_with_lagging_replica_count: with_lagging.len(),
            masters_with_lagging_replica: listed(&with_lagging),
            max_replica_lag_bytes,
            generated_at,
        }
    }
}

/// The evidence one cluster health check is evaluated on. Without the inventory the health is
/// unknown; without the replication summary its rules stay silent.
pub(crate) struct ClusterHealthEvidence {
    pub inventory: Result<BrokerInventory, ToolFailure>,
    pub replication: Result<ReplicationSummary, ToolFailure>,
}

impl ClusterHealthEvidence {
    fn snapshot(&self, args: &GetClusterHealthArgs) -> EvidenceSnapshot {
        let query_hash = diagnosis_snapshot::query_hash(&[&args.cluster]);
        let items = vec![
            diagnosis_snapshot::item(
                BROKER_INVENTORY,
                "rocketmq_list_brokers",
                EvidenceKind::BrokerInventory,
                &query_hash,
                &self.inventory,
            ),
            diagnosis_snapshot::item(
                HA_STATUS,
                "rocketmq_get_ha_status",
                EvidenceKind::HaStatus,
                &query_hash,
                &self.replication,
            ),
        ];
        diagnosis_snapshot::snapshot(
            CLUSTER_HEALTH_EVIDENCE_VERSION,
            query_hash,
            &args.cluster,
            json!({}),
            items,
        )
    }
}

struct RuleInput<'a> {
    inventory: &'a BrokerInventory,
    replication: Option<&'a ReplicationSummary>,
}

type Rule = fn(&RuleInput<'_>) -> Option<HealthFinding>;

/// The rule table. The identifier of a rule is the `rule_id` of the finding it returns; it is part
/// of the report contract and never changes once released.
const RULES: &[Rule] = &[
    no_broker_registered,
    master_missing,
    broker_instance_inactive,
    no_in_sync_replica,
    replica_not_in_sync,
    master_without_replica,
    version_skew,
];

/// Evaluates the rules on `evidence` and writes the report.
pub(crate) fn evaluate(
    args: &GetClusterHealthArgs,
    evidence: ClusterHealthEvidence,
    policy_profile: &str,
) -> DiagnosisReport {
    let snapshot = evidence.snapshot(args);
    let findings = match evidence.inventory.as_ref() {
        Ok(inventory) => {
            let input = RuleInput {
                inventory,
                replication: evidence.replication.as_ref().ok(),
            };
            RULES.iter().filter_map(|rule| rule(&input)).collect()
        }
        Err(_) => Vec::new(),
    };
    let healthy_summary = evidence
        .inventory
        .as_ref()
        .map(|inventory| {
            format!(
                "{} instances of {} Brokers answer and no rule found a problem.",
                inventory.instance_count, inventory.broker_count
            )
        })
        .unwrap_or_default();
    diagnosis_service::build_health_report(HealthReport {
        report_type: "cluster_health",
        rules_version: CLUSTER_HEALTH_RULES_VERSION,
        target: json!({}),
        subject: format!("Cluster {}", args.cluster),
        policy_profile,
        missing_primary: evidence.inventory.is_err().then_some("Broker inventory"),
        healthy_summary,
        confidence: match &evidence.inventory {
            Ok(_) => diagnosis_service::weighted_confidence(&snapshot, 600, CONFIDENCE_WEIGHTS),
            Err(_) => 0.20,
        },
        findings,
        risks: vec![
            "This check does not look inside a Broker: an instance that answers can still be unhealthy.".to_string(),
        ],
        follow_up_metrics: vec![
            MetricWatchItem {
                name: "inactive_instance_count".to_string(),
                target: "cluster".to_string(),
                reason: "Shows whether every registered instance answers.".to_string(),
            },
            MetricWatchItem {
                name: "masters_without_in_sync_replica_count".to_string(),
                target: "cluster".to_string(),
                reason: "Shows whether every master can be lost without losing messages.".to_string(),
            },
        ],
        snapshot,
    })
}

fn finding(
    rule_id: &'static str,
    severity: Severity,
    area: &str,
    evidence: &'static str,
    cause: &str,
    reasoning: String,
    recommendation: Recommendation,
) -> HealthFinding {
    HealthFinding {
        rule_id,
        severity,
        area: area.to_string(),
        finding: Finding {
            cause: cause.to_string(),
            // A rule counts what the cluster reports about itself, so what fires is established.
            confidence: 0.90,
            reasoning,
            evidence_refs: vec![evidence],
            stops_consumption: false,
            recommendation,
        },
    }
}

/// Recommends the Broker health diagnosis for the Brokers a finding names.
fn diagnose_brokers(priority: &str, rationale: &str) -> Recommendation {
    Recommendation {
        action: "Run rocketmq_diagnose_broker_health for each Broker the finding names.".to_string(),
        priority: priority.to_string(),
        rationale: rationale.to_string(),
        risk: "None. The diagnosis is read-only.".to_string(),
        verification: "The next cluster health check no longer names the Broker.".to_string(),
    }
}

/// Writes up to [`LISTED_BROKER_LIMIT`] names and says how many more there are.
fn names(listed: &[String], total: usize) -> String {
    let omitted = total.saturating_sub(listed.len());
    if omitted == 0 {
        listed.join(", ")
    } else {
        format!("{} and {omitted} more", listed.join(", "))
    }
}

fn no_broker_registered(input: &RuleInput<'_>) -> Option<HealthFinding> {
    (input.inventory.instance_count == 0).then(|| {
        finding(
            "no_broker_registered",
            Severity::Critical,
            "availability",
            BROKER_INVENTORY,
            "No Broker is registered in the cluster",
            "The NameServer lists no Broker instance for the cluster.".to_string(),
            Recommendation {
                action: "Check that the Brokers run and register with the NameServer this server is configured for."
                    .to_string(),
                priority: "high".to_string(),
                rationale: "Without a Broker the cluster accepts and delivers no message.".to_string(),
                risk: "None. Checking is read-only.".to_string(),
                verification: "rocketmq_list_brokers lists the Brokers.".to_string(),
            },
        )
    })
}

fn master_missing(input: &RuleInput<'_>) -> Option<HealthFinding> {
    let inventory = input.inventory;
    (inventory.brokers_without_master_count > 0).then(|| {
        finding(
            "master_missing",
            Severity::Critical,
            "availability",
            BROKER_INVENTORY,
            "A Broker has no master",
            format!(
                "{} of {} Brokers have no instance registered with id 0: {}.",
                inventory.brokers_without_master_count,
                inventory.broker_count,
                names(
                    &inventory.brokers_without_master,
                    inventory.brokers_without_master_count
                )
            ),
            diagnose_brokers(
                "high",
                "Producers write to the master only, so the queues of such a Broker accept no messages.",
            ),
        )
    })
}

fn broker_instance_inactive(input: &RuleInput<'_>) -> Option<HealthFinding> {
    let inventory = input.inventory;
    (inventory.inactive_instance_count > 0).then(|| {
        let master_down = inventory.inactive_master_count > 0;
        finding(
            "broker_instance_inactive",
            if master_down {
                Severity::Critical
            } else {
                Severity::Medium
            },
            "availability",
            BROKER_INVENTORY,
            if master_down {
                "A master does not answer"
            } else {
                "A replica does not answer"
            },
            format!(
                "{} of {} instances are registered but did not answer, {} of them masters. Brokers: {}.",
                inventory.inactive_instance_count,
                inventory.instance_count,
                inventory.inactive_master_count,
                names(&inventory.inactive_brokers, inventory.inactive_brokers.len())
            ),
            diagnose_brokers(
                "high",
                "An instance that does not answer serves no client, or this server cannot reach it.",
            ),
        )
    })
}

fn no_in_sync_replica(input: &RuleInput<'_>) -> Option<HealthFinding> {
    let replication = input.replication?;
    (replication.masters_without_in_sync_replica_count > 0).then(|| {
        finding(
            "no_in_sync_replica",
            Severity::High,
            "replication",
            HA_STATUS,
            "A master has no replica in sync",
            format!(
                "{} of {} masters have replicas and none of them in sync: {}.",
                replication.masters_without_in_sync_replica_count,
                replication.master_count,
                names(
                    &replication.masters_without_in_sync_replica,
                    replication.masters_without_in_sync_replica_count
                )
            ),
            diagnose_brokers(
                "high",
                "Losing such a master loses the messages that were not replicated.",
            ),
        )
    })
}

fn replica_not_in_sync(input: &RuleInput<'_>) -> Option<HealthFinding> {
    let replication = input.replication?;
    (replication.masters_with_lagging_replica_count > 0).then(|| {
        finding(
            "replica_not_in_sync",
            Severity::Medium,
            "replication",
            HA_STATUS,
            "A replica is behind its master",
            format!(
                "{} of {} masters have a replica that is not in sync, the furthest {} bytes behind: {}.",
                replication.masters_with_lagging_replica_count,
                replication.master_count,
                replication.max_replica_lag_bytes,
                names(
                    &replication.masters_with_lagging_replica,
                    replication.masters_with_lagging_replica_count
                )
            ),
            diagnose_brokers(
                "medium",
                "A replica that is behind cannot take over without losing what it has not received.",
            ),
        )
    })
}

fn master_without_replica(input: &RuleInput<'_>) -> Option<HealthFinding> {
    let inventory = input.inventory;
    (inventory.masters_without_replica_count > 0).then(|| {
        finding(
            "master_without_replica",
            Severity::Low,
            "redundancy",
            BROKER_INVENTORY,
            "A master has no replica",
            format!(
                "{} of {} masters have no replica registered: {}.",
                inventory.masters_without_replica_count,
                inventory.master_count,
                names(
                    &inventory.masters_without_replica,
                    inventory.masters_without_replica_count
                )
            ),
            Recommendation {
                action: "Decide whether those Brokers are meant to run without a replica.".to_string(),
                priority: "low".to_string(),
                rationale: "Losing a master without a replica makes its queues unavailable and can lose their \
                            messages."
                    .to_string(),
                risk: "A development cluster often runs without replicas on purpose.".to_string(),
                verification: "rocketmq_list_brokers lists a replica for each master that needs one.".to_string(),
            },
        )
    })
}

fn version_skew(input: &RuleInput<'_>) -> Option<HealthFinding> {
    let versions = &input.inventory.versions;
    (versions.len() > 1).then(|| {
        finding(
            "version_skew",
            Severity::Low,
            "configuration",
            BROKER_INVENTORY,
            "The Brokers run different versions",
            format!(
                "{} versions are in use: {}.",
                versions.len(),
                versions
                    .iter()
                    .take(LISTED_BROKER_LIMIT)
                    .map(|version| format!("{} on {} instances", version.version, version.instance_count))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Recommendation {
                action: "Check whether an upgrade is in progress; if not, plan to bring the Brokers to one version."
                    .to_string(),
                priority: "low".to_string(),
                rationale: "Instances of different versions can behave differently for the same request.".to_string(),
                risk: "A rolling upgrade passes through this state on purpose.".to_string(),
                verification: "rocketmq_list_brokers reports one version.".to_string(),
            },
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::diagnosis::EvidenceStatus;
    use crate::service::broker_health_diagnosis::fixtures::ha;
    use crate::tools::executor::ToolExecutionError;
    use crate::tools::infrastructure_tools::BrokerHaObservation;

    const NOW: &str = "2026-10-10T12:00:00.000Z";

    fn args() -> GetClusterHealthArgs {
        GetClusterHealthArgs {
            cluster: "local-dev".to_string(),
        }
    }

    fn instance(broker_name: &str, broker_id: u64) -> BrokerSummary {
        BrokerSummary {
            cluster: "local-dev".to_string(),
            broker_name: broker_name.to_string(),
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

    /// Two Brokers, each a master with one replica.
    fn rows() -> Vec<BrokerSummary> {
        vec![
            instance("broker-a", 0),
            instance("broker-a", 1),
            instance("broker-b", 0),
            instance("broker-b", 1),
        ]
    }

    /// The replication view of `broker-a` and `broker-b`: one connection per entry of each,
    /// `(replica id, in sync, bytes behind)`.
    fn replication(broker_a: &[(u64, bool, i64)], broker_b: &[(u64, bool, i64)]) -> GetHaStatusOutput {
        let master = |broker_name: &str, replicas: &[(u64, bool, i64)]| BrokerHaObservation {
            broker_name: broker_name.to_string(),
            ..ha(replicas).brokers.remove(0)
        };
        GetHaStatusOutput {
            cluster: "local-dev".to_string(),
            brokers: vec![master("broker-a", broker_a), master("broker-b", broker_b)],
            controller_sync_states: Vec::new(),
        }
    }

    fn diagnose(rows: &[BrokerSummary], ha: Option<&GetHaStatusOutput>) -> DiagnosisReport {
        let inventory = BrokerInventory::from_rows("local-dev", rows, NOW.to_string());
        let replication = match ha {
            Some(ha) => Ok(ReplicationSummary::from_ha(ha, Some(&inventory), NOW.to_string())),
            None => Err(ToolFailure::Operational(ToolExecutionError::Backend(None))),
        };
        evaluate(
            &args(),
            ClusterHealthEvidence {
                inventory: Ok(inventory),
                replication,
            },
            "production-default",
        )
    }

    fn rule_ids(report: &DiagnosisReport) -> Vec<&str> {
        report.root_causes.iter().map(|cause| cause.rule_id.as_str()).collect()
    }

    fn in_sync() -> GetHaStatusOutput {
        replication(&[(1, true, 0)], &[(1, true, 0)])
    }

    #[test]
    fn a_cluster_whose_instances_answer_and_replicate_is_healthy() {
        let report = diagnose(&rows(), Some(&in_sync()));

        assert_eq!(report.report_type, "cluster_health");
        assert_eq!(report.rules_version, CLUSTER_HEALTH_RULES_VERSION);
        assert_eq!(report.evidence_version, CLUSTER_HEALTH_EVIDENCE_VERSION);
        assert_eq!(report.severity, Severity::Healthy);
        assert!(!report.partial);
        assert_eq!(report.confidence, 0.95);
        assert_eq!(
            report.summary,
            "Cluster local-dev is Healthy: 4 instances of 2 Brokers answer and no rule found a problem."
        );
        assert_eq!(report.evidence_refs, ["broker_inventory", "ha_status"]);
    }

    #[test]
    fn the_inventory_counts_instances_and_lists_a_bounded_number_of_names() {
        let mut rows = (0..25)
            .map(|index| instance(&format!("broker-{index:02}"), 0))
            .collect::<Vec<_>>();
        rows.push(instance("broker-00", 1));
        rows[3].broker_active = false;
        rows[4].version = "V5_2_0".to_string();

        let inventory = BrokerInventory::from_rows("local-dev", &rows, NOW.to_string());

        assert_eq!(inventory.broker_count, 25);
        assert_eq!(inventory.instance_count, 26);
        assert_eq!(inventory.master_count, 25);
        assert_eq!(inventory.replica_count, 1);
        assert_eq!(inventory.inactive_instance_count, 1);
        assert_eq!(inventory.inactive_master_count, 1);
        assert_eq!(inventory.inactive_brokers, ["broker-03"]);
        assert_eq!(inventory.masters_without_replica_count, 24);
        assert_eq!(inventory.masters_without_replica.len(), LISTED_BROKER_LIMIT);
        assert_eq!(inventory.masters_without_replica[0], "broker-01");
        assert_eq!(
            inventory.versions,
            [
                VersionCount {
                    version: "V5_3_0".to_string(),
                    instance_count: 25
                },
                VersionCount {
                    version: "V5_2_0".to_string(),
                    instance_count: 1
                },
            ]
        );
        let serialized = serde_json::to_value(&inventory).unwrap();
        assert!(
            serialized.get("replicas").is_none(),
            "the per-Broker map is not published"
        );
    }

    #[test]
    fn no_broker_registered_fires_for_an_empty_cluster() {
        let report = diagnose(&[], None);

        assert_eq!(rule_ids(&report), ["no_broker_registered"]);
        assert_eq!(report.severity, Severity::Critical);
    }

    #[test]
    fn master_missing_names_the_brokers_without_a_master() {
        let rows = vec![
            instance("broker-a", 0),
            instance("broker-b", 1),
            instance("broker-b", 2),
        ];

        let report = diagnose(&rows, None);

        assert_eq!(rule_ids(&report)[0], "master_missing");
        assert_eq!(report.severity, Severity::Critical);
        assert_eq!(
            report.root_causes[0].reasoning,
            "1 of 2 Brokers have no instance registered with id 0: broker-b."
        );
        assert!(report.recommendations[0]
            .action
            .contains("rocketmq_diagnose_broker_health"));
    }

    #[test]
    fn broker_instance_inactive_is_critical_for_a_master_and_medium_for_a_replica() {
        let with_inactive = |index: usize| {
            let mut rows = rows();
            rows[index].broker_active = false;
            diagnose(&rows, Some(&in_sync()))
        };

        let master = with_inactive(0);
        assert_eq!(rule_ids(&master), ["broker_instance_inactive"]);
        assert_eq!(master.severity, Severity::Critical);
        assert_eq!(master.root_causes[0].cause, "A master does not answer");

        let replica = with_inactive(3);
        assert_eq!(replica.severity, Severity::Medium);
        assert_eq!(
            replica.root_causes[0].reasoning,
            "1 of 4 instances are registered but did not answer, 0 of them masters. Brokers: broker-b."
        );
    }

    #[test]
    fn no_in_sync_replica_fires_for_a_master_whose_replicas_all_lag_or_are_not_connected() {
        // broker-a: its replica is connected and behind. broker-b: its replica is not connected.
        let report = diagnose(&rows(), Some(&replication(&[(1, false, 4_096)], &[])));

        assert_eq!(rule_ids(&report), ["no_in_sync_replica"]);
        assert_eq!(report.severity, Severity::High);
        assert_eq!(
            report.root_causes[0].reasoning,
            "2 of 2 masters have replicas and none of them in sync: broker-a, broker-b."
        );
        assert_eq!(report.root_causes[0].evidence_refs, ["ha_status"]);
    }

    #[test]
    fn replica_not_in_sync_fires_for_a_master_with_one_replica_in_sync_and_one_behind() {
        let mut rows = rows();
        rows.push(instance("broker-a", 2));

        let report = diagnose(
            &rows,
            Some(&replication(&[(1, true, 0), (2, false, 65_536)], &[(1, true, 0)])),
        );

        assert_eq!(rule_ids(&report), ["replica_not_in_sync"]);
        assert_eq!(report.severity, Severity::Medium);
        assert_eq!(
            report.root_causes[0].reasoning,
            "1 of 2 masters have a replica that is not in sync, the furthest 65536 bytes behind: broker-a."
        );
    }

    #[test]
    fn master_without_replica_and_version_skew_are_low_findings() {
        let mut rows = vec![instance("broker-a", 0), instance("broker-b", 0)];
        rows[1].version = "V5_2_0".to_string();

        let report = diagnose(&rows, Some(&replication(&[], &[])));

        assert_eq!(rule_ids(&report), ["master_without_replica", "version_skew"]);
        assert_eq!(report.severity, Severity::Low);
        assert_eq!(
            report.root_causes[1].reasoning,
            "2 versions are in use: V5_2_0 on 1 instances, V5_3_0 on 1 instances."
        );
    }

    #[test]
    fn a_failed_replication_read_silences_its_rules_and_leaves_a_partial_report() {
        let report = diagnose(&rows(), None);

        assert!(report.partial);
        assert_eq!(report.missing_evidence, ["ha_status"]);
        assert_eq!(report.severity, Severity::Healthy);
        assert_eq!(report.confidence, 0.6);
        assert_eq!(
            report
                .evidence_snapshot
                .items
                .iter()
                .find(|item| item.id == "ha_status")
                .unwrap()
                .status,
            EvidenceStatus::Unavailable
        );
    }

    #[test]
    fn the_health_is_unknown_without_the_inventory() {
        let report = evaluate(
            &args(),
            ClusterHealthEvidence {
                inventory: Err(ToolFailure::Operational(ToolExecutionError::Backend(None))),
                replication: Err(ToolFailure::Operational(ToolExecutionError::Backend(None))),
            },
            "production-default",
        );

        assert_eq!(report.severity, Severity::Unknown);
        assert_eq!(
            report.summary,
            "Cluster local-dev health is Unknown because Broker inventory evidence is missing."
        );
        assert!(report.root_causes.is_empty());
        assert_eq!(report.confidence, 0.20);
    }
}
