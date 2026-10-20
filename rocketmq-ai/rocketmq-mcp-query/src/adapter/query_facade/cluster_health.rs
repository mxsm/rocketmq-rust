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

//! The cluster health check: the registered Broker instances and how their replicas keep up,
//! read from one session inside one request budget.

use std::time::Duration;

use super::diagnosis_support::note;
use super::diagnosis_support::wrap_report;
use super::diagnosis_support::SupportingBudget;
use super::AdminSession;
use super::AdminSessionFactory;
use super::QueryFacade;
use crate::adapter::admin_session::ResolvedCluster;
use crate::model::contract::observed_at;
use crate::model::contract::QueryCompleteness;
use crate::model::contract::QueryPayload;
use crate::model::contract::QueryResult;
use crate::model::contract::QuerySource;
use crate::model::diagnosis::DiagnosisReport;
use crate::service::cluster_health;
use crate::service::cluster_health::BrokerInventory;
use crate::service::cluster_health::ClusterHealthEvidence;
use crate::service::cluster_health::ReplicationSummary;
use crate::tools::diagnosis_tools::GetClusterHealthArgs;
use crate::tools::executor::ToolFailure;
use crate::tools::executor::ToolRejection;

impl<F> QueryFacade<F>
where
    F: AdminSessionFactory,
{
    pub(crate) async fn cluster_health(
        &self,
        args: GetClusterHealthArgs,
    ) -> Result<QueryResult<DiagnosisReport>, ToolFailure> {
        let cluster = self.resolve_cluster(&args.cluster)?;
        let key = self.cache_key("cluster_health", &cluster.name, "");
        let ttl = Duration::from_millis(self.config.cache.broker_metrics_ttl_ms);
        let policy_profile = self.config.diagnosis.consumer_lag_policy_profile.clone();
        self.cache
            .get_or_try_init_cancellable(
                key,
                ttl,
                &self.control.cancellation,
                || ToolFailure::Rejected(ToolRejection::Cancelled),
                move || async {
                    let budget = SupportingBudget::new(&self.control);
                    self.run_workflow(cluster, move |session, cluster| {
                        Box::pin(check_in_session(session, cluster, args, policy_profile, budget))
                    })
                    .await
                },
            )
            .await
    }
}

/// Collects the evidence and evaluates the rules on it.
///
/// The Broker rows are read first and may take the whole request budget: without them the health
/// is unknown. The replication view of the masters has the supporting budget. Neither read fails
/// the check.
async fn check_in_session<S>(
    session: &mut S,
    cluster: &ResolvedCluster,
    args: GetClusterHealthArgs,
    policy_profile: String,
    mut budget: SupportingBudget,
) -> Result<QueryPayload<DiagnosisReport>, ToolFailure>
where
    S: AdminSession,
{
    let mut completeness = QueryCompleteness::default();

    let inventory = note(
        &mut completeness,
        QuerySource::BrokerRuntime,
        &cluster.name,
        session.broker_rows().await,
    )
    .map(|rows| BrokerInventory::from_rows(&cluster.name, &rows.data, observed_at()));
    // No Broker name selects every master the cluster has registered.
    let replication = note(
        &mut completeness,
        QuerySource::BrokerHaRuntime,
        &cluster.name,
        budget.read(session.ha_status(&[], false, &[])).await,
    )
    .map(|ha| {
        let mut summary = ReplicationSummary::from_ha(&ha.data, inventory.as_ref().ok(), observed_at());
        // The source names the physical cluster; the report names the configured one.
        summary.cluster.clone_from(&cluster.name);
        summary
    });

    let report = cluster_health::evaluate(&args, ClusterHealthEvidence { inventory, replication }, &policy_profile);
    Ok(wrap_report(completeness, report))
}
